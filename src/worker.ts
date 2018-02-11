import * as uuid from 'uuid';
import * as WS from 'ws';
import * as M from './message';
import { Serializer } from './serializer';
import { Logger, LogLevel } from './logger';
import { JobId, JobAttr, JobFailed } from './job';
import { spinlock, mergeOpt, SeqLock } from './util';
import { Runner } from './runner';
import { Signal } from './signal';

export interface JobHandle<T = any> {
  job:      JobAttr<T>;
  progress: (progress: any) => void;
}

export const enum _WorkerId {}
export type WorkerId = _WorkerId & string; // pseudo nominal typing

export interface ExecData {
  jobid:   JobId;
  started: Date;
  retries: number; // acts as seq for instances of the same job
}

export type ExecStrategy = 'exec'|'execquiet'; // TODO: cancel, abort

export const WORKER_DEFS = {
  log:     LogLevel.warn,
  name:    'worker',
  host:    'localhost',
  port:    3000,
  wss:     false,
  waitms:  500,
  retryms: 5000,
  retryx:  0
};

export type WorkerOpt = typeof WORKER_DEFS;

export class Worker {
  readonly up          = new Signal();
  readonly down        = new Signal<Error|null>();
  readonly jobstart    = new Signal();
  readonly jobprogress = new Signal<any>();
  readonly jobfinish   = new Signal<{ resumed: boolean }>();

  readonly id: WorkerId;
  readonly name: string;
  private ws: WS|null = null;
  private opening = false;
  private closing = false;
  private closingerr: Error|null = null;
  private retrytimer: NodeJS.Timer|null = null;
  private retries = 0;
  private exec: {
    data:     ExecData;
    runner:   Runner<any, any>;
    canAbort: boolean;
  }|null = null;
  private replay: M.Finish|null = null;    // sent on ready
  private strategy: ExecStrategy = 'exec'; // handling strategy for current job
  private readonly opt: WorkerOpt;
  private readonly log = new Logger(Worker.name);

  constructor(opt?: Partial<WorkerOpt>) {
    this.opt = mergeOpt(WORKER_DEFS, opt) as WorkerOpt;
    this.id = uuid.v4() as WorkerId;
    this.name = this.opt.name;
    this.logLevel = this.opt.log;
  }

  get report() {
    return {
      alive:   !!this.ws,
      closing: this.closing,
      working: !!this.exec
    };
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  start() {
    if (this.opening) {
      this.log.warn('broker is opening'); // WorkerEvent.start emitted later
      return this;
    }
    else if (this.closing) {
      this.closing = false;
    }
    else if (this.ws) {
      this.log.warn('worker is already running');
      this.up.event();
      return this;
    }

    this.opening = true;

    this.ws = this.startClient();

    return this;
  }

  stop(force = false) {
    if (this.retrytimer) {
      this.log.info('cancel reconnect timer');
      clearTimeout(this.retrytimer);
      this.retrytimer = null;
    }

    if (this.closing) {
      this.log.warn('worker is closing'); // WorkerEvent.stop emitted later
      return this;
    }
    else if (this.opening) {
      this.opening = false;
    }
    else if (!this.ws) {
      this.log.warn('worker is not running');
      this.down.event(null);
      return this;
    }

    this.log.warn(`shutting down - ${force ? 'forced' : 'graceful'}`);

    this.closing = true;
    this.closingerr = null;

    this.sendMsg(M.Code.meta).catch(() => {});

    spinlock({
      ms:   this.opt.waitms,
      test: () => force || !this.exec,
      pre:  () => this.log.info('waiting on job to finish…')
    })
    .catch((err) => {
      this.log.error('timeout elapsed, closing now', err);

      this.closingerr = err;
    })
    .then(() => {
      if (this.ws) {
        this.ws.terminate();
        this.ws = null;
      }

      this.closing = false;

      this.down.event(this.closingerr);
    })
    .catch(() => {
      // for linter
    });

    return this;
  }

  abort() {
    if (this.exec) {
      if (this.exec.canAbort) {
        this.exec.runner.abort();
      }
      else {
        throw new Error('job cannot be aborted');
      }
    }
    else {
      throw new Error('worker has no job');
    }
  }

  async pingBroker() {
    const start = Date.now();

    if (await this.sendMsg(M.Code.ping)) {
      return Date.now() - start;
    }
    else {
      return 'unreachable';
    }
  }

  private startClient() {
    this.log.debug('client starting');

    const ws = new WS(
      `${this.opt.wss ? 'wss' : 'ws'}://${this.opt.host}:${this.opt.port}`
    );

    ws.on('open', async () => {
      this.log.debug(`socket open ${this.name}`);

      this.opening = false;
      this.retries = 0;

      if (this.exec) {
        const resume: M.Resume = {
          id:   this.id,
          name: this.name,
          job:  this.exec.data
        };

        await this.sendMsg(M.Code.resume, resume);
      }
      else {
        const ready: M.Ready = {
          id:     this.id,
          name:   this.name,
          replay: this.replay
        };

        if (await this.sendMsg(M.Code.ready, ready)) {
          // only replay once
          this.replay = null;
        }
      }
    });

    ws.on('message', async (raw) => {
      return this.handleMsg(Serializer.unpack(raw as string) as M.Msg);
    });

    ws.on('close', (code, reason) => {
      this.log.error(`socket close ${code} ${reason}`);

      if (this.ws) {
        this.ws.terminate();
        this.ws = null;
      }

      this.down.event(null);

      if (!this.closing) {
        this.log.info('reconnecting');

        this.retrytimer = setTimeout(() => {
          const retriesLeft = (
            !this.opt.retryx || (this.opt.retryx > this.retries)
          );

          if (retriesLeft) {
            this.log.info(`connecting (attempt ${++this.retries})`);

            this.ws = this.startClient();
          }
          else {
            this.log.info('skip reconnecting');

            this.opening = false;

            if (!retriesLeft) {
              this.log.warn('no retries left, stopping');
              this.stop();
            }
          }
        }, this.opt.retryms);
      }
    });

    ws.on('error', (err) => {
      this.log.error('socket error', err);
    });

    return ws;
  }

  private async handleMsg(msg: M.Msg) {
    const seq = new SeqLock(false); // process requests sequentially

    switch (msg.code) {
      case M.Code.meta:
        return seq.run(async () => {
          this.log.debug(`${this.name} got meta`);
        });

      case M.Code.ping:
        return seq.run(async () => {
          this.log.debug(`${this.name} got ping`);

          await this.sendMsg(M.Code.pong);
        });

      case M.Code.pong:
        return seq.run(async () => {
          this.log.debug(`${this.name} got pong`);
        });

      case M.Code.readyok:
        return seq.run(async () => {
          const m = msg.data as M.ReadyOk;

          this.strategy = m.strategy;

          this.log.debug(`${this.name} got readyok (strategy: ${m.strategy})`);

          this.up.event();
        });

      case M.Code.assign:
        return seq.run(async () => {
          if (this.exec) {
            this.log.error('worker already has a job', msg);
            return;
          }

          const assign = msg.data as M.Assign;

          await this.run(assign.job);
        });

      case M.Code.abort:
        return seq.run(async () => {
          if (!this.exec) {
            this.log.error('worker does not have a job', msg);
            return;
          }
          else if (!this.exec.canAbort) {
            this.log.error('worker cannot abort job', msg);
            return;
          }

          this.abort();
        });

      default:
        return seq.run(async () => {
          this.log.error('unknown broker message', msg);
        });
    }
  }

  private async run<T, U>(j: JobAttr<T>) {
    this.log.info(`worker ${this.name} starting job ${j.name}`);

    const exec: ExecData = {
      jobid:   j.id,
      started: new Date(),
      retries: j.retries
    };

    this.exec = {
      data:     exec,
      runner:   new Runner(),
      canAbort: !!j.sandbox
    };

    this.jobstart.event();

    const start: M.Start = { job: exec };

    await this.sendMsg(M.Code.start, start);

    let result: U|JobFailed;
    let status: 'done'|'failed';

    try {
      result = await this.exec.runner.run({
        job: j,
        progress: async (progress) => {
          this.jobprogress.event(progress);

          const prog: M.Progress = { job: exec, progress };

          await this.sendMsg(M.Code.progress, prog);
        }
      });

      status = 'done';

      this.log.debug('job finished ok');
    }
    catch (err) {
      result = err as JobFailed;

      status = 'failed';

      this.log.error('job error', err);
    }

    const resumed = !!this.exec;

    this.exec = null;
    this.strategy = 'exec';

    this.jobfinish.event({ resumed }); // TODO: remove this prop

    const finish: M.Finish = { job: exec, status, result };

    await this.sendMsg(M.Code.finish, finish);
  }

  private trySaveReplay(code: M.Code, data: M.Data) {
    if (code === M.Code.finish) {
      this.log.warn('save message for replay', code);

      this.replay = data as M.Finish;
    }
    else {
      this.log.debug('drop message', code);
    }
  }

  private async sendMsg(code: M.Code, data: M.Data = {}) {
    return new Promise<boolean>((resolve) => {
      const doSwap = (
        this.strategy === 'execquiet' &&
        (
          code === M.Code.start ||
          code === M.Code.progress
        )
      );

      const msg: M.Msg = doSwap ?
        // send meta/noop instead, will discard results
        { code: M.Code.meta, data: {}, closing: this.closing } :
        { code, data, closing: this.closing };

      if (!this.ws) {
        this.trySaveReplay(code, data);

        return resolve(false);
      }

      this.ws.send(Serializer.pack(msg), (err) => {
        if (err) {
          this.log.error('socket write err', err.message);

          // save actual message being sent instead of original
          this.trySaveReplay(msg.code, msg.data);

          return resolve(false);
        }
        else {
          return resolve(true);
        }
      });
    });
  }
}
