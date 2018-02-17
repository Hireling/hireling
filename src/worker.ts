import * as uuid from 'uuid';
import * as M from './message';
import { Logger, LogLevel } from './logger';
import { spinlock, TopPartial, mergeOpt, SeqLock } from './util';
import { Signal } from './signal';
import { Client, CLIENT_DEFS } from './client';
import { JobId, JobAttr, EFail } from './job';
import { Runner } from './runner';

export interface JobHandle<I = any> {
  job:      JobAttr<I>;
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
  closems: 5000,
  waitms:  500,
  client:  CLIENT_DEFS
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
  private client: Client;
  private closing = false;
  private exec: {
    ex:       ExecData;
    runner:   Runner;
    canAbort: boolean;
  }|null = null;
  private replay: M.Finish|null = null;    // sent on ready
  private strategy: ExecStrategy = 'exec'; // handling strategy for current job
  private readonly opt: WorkerOpt;
  private readonly log = new Logger(Worker.name);

  constructor(opt?: TopPartial<WorkerOpt>) {
    this.opt = mergeOpt(WORKER_DEFS, opt) as WorkerOpt;
    this.id = uuid.v4() as WorkerId;
    this.name = this.opt.name;
    this.logLevel = this.opt.log;
    this.client = this.makeClient();
  }

  get report() {
    return {
      up:      this.client.report.up,
      working: !!this.exec
    };
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  start() {
    this.log.info('starting');

    this.client.start();

    return this;
  }

  stop(force = false) {
    if (this.closing) {
      this.log.warn('already closing');
      return this;
    }

    this.log.warn(`shutting down - ${force ? 'forced' : 'graceful'}`);

    this.closing = true;

    this.msgClient({ code: M.Code.meta }).catch(() => {});

    spinlock({
      ms:    this.opt.waitms,
      errms: this.opt.closems,
      // forced, stoped executing, or down while waiting
      test:  () => force || !this.exec || !this.client.report.up,
      pre:   () => this.log.info('waiting on job to finish…')
    })
    .then(() => {
      this.log.warn('stopping');

      this.closing = false;

      this.client.stop();
    })
    .catch((err) => {
      this.log.error('timeout elapsed, stopping now', err);

      this.closing = false;

      this.client.stop();
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

    if (await this.msgClient({ code: M.Code.ping })) {
      return Date.now() - start;
    }
    else {
      return 'unreachable';
    }
  }

  private makeClient() {
    const seq = new SeqLock(false); // process requests sequentially

    const client = new Client(this.opt.client);

    client.up.on(seq.cb(async () => {
      this.log.debug('client connected');

      if (this.exec) {
        await this.msgClient({
          code: M.Code.resume,
          id:   this.id,
          name: this.name,
          ex:   this.exec.ex
        });
      }
      else {
        await this.msgClient({
          code:   M.Code.ready,
          id:     this.id,
          name:   this.name,
          replay: this.replay
        });
      }
    }));

    client.down.on(seq.cb(async (err) => {
      this.log.debug('client disconnected');

      this.down.event(err);
    }));

    client.meta.on(seq.cb(async () => {
      this.log.debug(`${this.name} got meta`);
    }));

    client.ping.on(seq.cb(async () => {
      this.log.debug(`${this.name} got ping`);

      await this.msgClient({ code: M.Code.pong });
    }));

    client.pong.on(seq.cb(async () => {
      this.log.debug(`${this.name} got pong`);
    }));

    client.readyok.on(seq.cb(async (msg) => {
      this.strategy = msg.strat;
      this.replay = null; // broker received replay

      this.log.debug(`${this.name} got readyok (strategy: ${msg.strat})`);

      this.up.event();
    }));

    client.assign.on(seq.cb(async (msg) => {
      if (this.exec) {
        this.log.error('job already assigned', msg);
        return;
      }

      await this.run(msg.job);
    }));

    client.abort.on(seq.cb(async (msg) => {
      if (!this.exec) {
        this.log.error('no job to abort', msg);
        return;
      }
      else if (!this.exec.canAbort) {
        this.log.error('job cannot be aborted', msg);
        return;
      }

      this.abort();
    }));

    return client;
  }

  private async run<I, O>(j: JobAttr<I>) {
    this.log.info(`${this.name} starting job ${j.name}`);

    const ex: ExecData = {
      jobid:   j.id,
      started: new Date(),
      retries: j.retries
    };

    this.exec = {
      ex,
      runner:   new Runner(),
      canAbort: !!j.sandbox
    };

    this.jobstart.event();

    await this.msgClient({ code: M.Code.start, ex });

    let result: O|EFail;
    let status: 'done'|'failed';

    try {
      result = await this.exec.runner.run({
        job: j,
        progress: async (progress) => {
          this.jobprogress.event(progress);

          await this.msgClient({ code: M.Code.progress, ex, progress });
        }
      });

      status = 'done';

      this.log.debug('job finished ok');
    }
    catch (err) {
      result = err as EFail;

      status = 'failed';

      this.log.error('job error', err);
    }

    const resumed = !!this.exec;

    this.exec = null;
    this.strategy = 'exec';

    this.jobfinish.event({ resumed }); // TODO: remove this prop

    await this.msgClient({ code: M.Code.finish, ex, status, result });
  }

  private async msgClient(msg: M.Msg) {
    const doSwap = (
      this.strategy === 'execquiet' &&
      (
        msg.code === M.Code.start ||
        msg.code === M.Code.progress
      )
    );

    // send meta/noop, results will be discarded
    const swapMsg: M.Msg = doSwap ?
      { closing: this.closing, code: M.Code.meta } :
      { closing: this.closing, ...msg };

    if (await this.client.sendMsg(swapMsg)) {
      return true;
    }
    else {
      if (swapMsg.code === M.Code.finish) {
        this.log.warn('save message for replay', swapMsg.code);

        this.replay = swapMsg;
      }
      else {
        this.log.debug('drop message', swapMsg.code);
      }

      return false;
    }
  }
}
