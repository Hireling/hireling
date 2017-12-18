import { EventEmitter } from 'events';
import * as uuid from 'uuid';
import * as WS from 'ws';
import * as M from './message';
import { Serializer } from './serializer';
import { Logger, LogLevel } from './logger';
import { JobId, JobAttr } from './job';
import { spinlock, mergeOpt, NoopHandler } from './util';

export interface JobHandle<T = any> {
  job:      JobAttr<T>;
  progress: (progress: number) => void;
}

export const enum _WorkerId {}
export type WorkerId = _WorkerId & string; // pseudo nominal typing

export interface ResumeData {
  id: JobId
}

export type ExecStrategy = 'exec'|'execquiet'; // TODO: cancel, abort

export type JobContext = (job: JobHandle) => Promise<any>;

const noopjob: JobContext = async () => {};

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

export const enum WorkerEvent {
  start       = 'start',
  stop        = 'stop',
  jobstart    = 'jobstart',
  jobprogress = 'jobprogress',
  jobfinish   = 'jobfinish'
}

// tslint:disable:unified-signatures
export declare interface Worker {
  on(e: WorkerEvent.start|'start', fn: NoopHandler): this;
  on(e: WorkerEvent.stop|'stop', fn: NoopHandler): this;
  on(e: WorkerEvent.jobstart|'jobstart', fn: NoopHandler): this;
  on(e: WorkerEvent.jobprogress|'jobprogress', fn: NoopHandler): this;
  on(e: WorkerEvent.jobfinish|'jobfinish', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export class Worker extends EventEmitter {
  readonly id: WorkerId;
  readonly name: string;
  private ws: WS|null;
  private ctx: JobContext;
  private opening = false;
  private closing = false;
  private closingerr: Error|null = null;
  private retrytimer: NodeJS.Timer|null = null;
  private retries = 0;
  private job: JobAttr|null = null;
  private resuming = false;
  private replay: M.Finish|null = null;   // sent on ready
  private strategy: ExecStrategy = 'exec'; // handling strategy for current job
  private readonly opt: WorkerOpt;
  private readonly log = new Logger(Worker.name);

  constructor(opt?: Partial<WorkerOpt>, ctx: JobContext = noopjob) {
    super();

    this.opt = mergeOpt(WORKER_DEFS, opt) as WorkerOpt;
    this.id = uuid.v4() as WorkerId;
    this.name = this.opt.name;
    this.ctx  = ctx;
    this.logLevel = this.opt.log;
  }

  get report() {
    return {
      alive:   !!this.ws,
      closing: this.closing,
      working: !!this.job
    };
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  setContext(ctx: JobContext) {
    this.ctx = ctx;

    return this;
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
      this.event(WorkerEvent.start);
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
      this.event(WorkerEvent.stop);
      return this;
    }

    this.log.warn(`shutting down - ${force ? 'forced' : 'graceful'}`);

    this.closing = true;
    this.closingerr = null;

    this.sendMsg(M.Code.meta).catch(() => {});

    spinlock({
      ms:   this.opt.waitms,
      test: () => force || !this.job,
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

      this.event(WorkerEvent.stop, this.closingerr);
    })
    .catch(() => {
      // for linter
    });

    return this;
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

      const data: M.Ready = {
        id:     this.id,
        name:   this.name,
        replay: this.replay,
        resume: this.job ? { id: this.job.id } : null
      };

      if (await this.sendMsg(M.Code.ready, data)) {
        // only replay once
        this.replay = null;
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

      this.event(WorkerEvent.stop);

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

  private async executeTask(msg: M.Add) {
    const job = msg.job;

    this.job = job;

    this.log.info(`worker ${this.name} starting job ${job.name}`);

    this.event(WorkerEvent.jobstart);

    const start: M.Start = { jobid: job.id };

    await this.sendMsg(M.Code.start, start);

    let finish: M.Finish;

    try {
      // execute and forward progress events
      const result = await this.ctx({
        job,
        progress: async (progress) => {
          const prog: M.Progress = { jobid: job.id, progress };

          this.event(WorkerEvent.jobprogress, prog);

          await this.sendMsg(M.Code.progress, prog);
        }
      });

      finish = {
        jobid:  job.id,
        status: 'done',
        result
      };

      this.log.debug('job finished ok');
    }
    catch (err) {
      this.log.error('job error', err);

      finish = {
        jobid:  job.id,
        status: 'failed',
        result: String(err)
      };
    }

    const wasResumed = this.resuming;

    this.job = null;
    this.strategy = 'exec';
    this.resuming = false;

    this.event(WorkerEvent.jobfinish, { resumed: wasResumed });

    await this.sendMsg(M.Code.finish, finish);
  }

  private async handleMsg(msg: M.Msg) {
    switch (msg.code) {
      case M.Code.meta:
        this.log.debug(`${this.name} got meta`);
      break;

      case M.Code.ping:
        this.log.debug(`${this.name} got ping`);

        await this.sendMsg(M.Code.pong);
      break;

      case M.Code.pong:
        this.log.debug(`${this.name} got pong`);
      break;

      case M.Code.readyok:
        const m = msg.data as M.ReadyOk;

        this.strategy = m.strategy;
        this.resuming = !!this.job;

        this.log.debug(`${this.name} got readyok (strategy: ${m.strategy})`);

        this.event(WorkerEvent.start);
      break;

      case M.Code.add:
        await this.executeTask(msg.data as M.Add);
      break;

      default:
        this.log.error('unkown broker message', msg);
      break;
    }
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
      if (!this.ws) {
        this.trySaveReplay(code, data);
        return resolve(false);
      }

      const doSwap = (
        this.strategy === 'execquiet' &&
        (
          code === M.Code.start ||
          code === M.Code.progress ||
          code === M.Code.finish
        )
      );

      const msg: M.Msg = doSwap ?
        // send meta/noop instead, will discard results
        { code: M.Code.meta, data: {}, closing: this.closing } :
        { code, data, closing: this.closing };

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

  private event(e: WorkerEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
