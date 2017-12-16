import { EventEmitter } from 'events';
import * as uuid from 'uuid';
import * as M from './message';
import { WorkerId } from './worker';
import { Logger, LogLevel } from './logger';
import {
  Job, JobStatus, JobMeta, JobData, JobEvent, JobAttr, JobStrategy, JobId
} from './job';
import { spinlock, TopPartial, mergeOpt, NoopHandler } from './util';
import { Db, DbEvent, MemoryEngine } from './db';
import { Server, ServerEvent, SERVER_DEFS } from './server';
import { Remote, RemoteEvent } from './remote';

export interface JobCreate<T = any> {
  name?:     string;
  expirems?: number; // max life in ms from time of creation
  stallms?:  number; // max time interval in ms between worker updates
  data?:     JobData<T>;
}

interface Replay {
  workerid: WorkerId;
  data:     M.Finish;
}

const GC_DEFS = {
  interval: 30000,                  // in ms, 0 to gc after each job completion
  statuses: ['done'] as JobStatus[] // statuses to gc
};

export const BROKER_DEFS = {
  log:     LogLevel.warn,
  closems: 5000,
  waitms:  500,
  gc:      GC_DEFS as typeof GC_DEFS|null,
  reapms:  5000,
  manual:  false, // manual job flow control
  server:  SERVER_DEFS,
  db:      {
    log:     LogLevel.warn,
    engine:  MemoryEngine as new (...args: any[]) => Db,
    retryms: 5000,
    retryx:  0,
    dbc:     {} as any
  }
};

export type BrokerOpt = typeof BROKER_DEFS;

export const enum BrokerEvent {
  start      = 'start',
  stop       = 'stop',
  error      = 'error',
  workerjoin = 'workerjoin',
  workerpart = 'workerpart',
  jobdone    = 'jobdone',
  full       = 'full',       // all workers busy
  drain      = 'drain'       // queue drained, no jobs remain
}

// tslint:disable:unified-signatures
export declare interface Broker {
  on(e: BrokerEvent.start|'start', fn: NoopHandler): this;
  on(e: BrokerEvent.stop|'stop', fn: NoopHandler): this;
  on(e: BrokerEvent.error|'error', fn: NoopHandler): this;
  on(e: BrokerEvent.workerjoin|'workerjoin', fn: NoopHandler): this;
  on(e: BrokerEvent.workerpart|'workerpart', fn: NoopHandler): this;
  on(e: BrokerEvent.jobdone|'jobdone', fn: NoopHandler): this;
  on(e: BrokerEvent.full|'full', fn: NoopHandler): this;
  on(e: BrokerEvent.drain|'drain', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export class Broker extends EventEmitter {
  private server: Server;
  private opening = false;
  private closing = false;
  private closingerr: Error|null = null;
  private retrytimer: NodeJS.Timer|null = null;
  private retries = 0;
  private serveropen = false;
  private dbopen = false;
  private gctimer: NodeJS.Timer|null = null;
  private reaptimer: NodeJS.Timer|null = null;
  private replaying = false;                     // lock vs. concurrent replays
  private readonly replays: Replay[] = [];       // queue for db-failed replays
  private readonly db: Db;
  private readonly jobs = new Map<JobId, Job>(); // handles with client events
  private readonly workers: Remote[] = [];
  private readonly opt: BrokerOpt;
  private readonly log = new Logger(Broker.name);

  constructor(opt?: TopPartial<BrokerOpt>) {
    super();

    this.opt = mergeOpt(BROKER_DEFS, opt) as BrokerOpt;
    this.logLevel = this.opt.log;

    this.db = this.makeDb();
    this.server = this.makeServer();
  }

  get report() {
    return {
      alive:   this.dbopen && this.serveropen,
      jobs:    this.jobs.size,
      workers: this.workers.length
    };
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  start() {
    if (this.opening) {
      this.log.warn('broker is opening'); // BrokerEvent.start emitted later
      return this;
    }
    else if (this.closing) {
      this.closing = false;
    }
    else if (this.dbopen && this.serveropen) {
      this.log.warn('broker is already running');
      this.event(BrokerEvent.start);
      return this;
    }

    this.opening = true;

    if (!this.dbopen) {
      this.db.open();
    }
    if (!this.serveropen) {
      this.server.start();
    }

    return this;
  }

  stop(force = false) {
    if (this.retrytimer) {
      this.log.info('cancel reconnect timer');

      clearTimeout(this.retrytimer);

      this.retrytimer = null;
    }

    if (this.closing) {
      this.log.warn('broker is closing'); // BrokerEvent.stop emitted later
      return this;
    }
    else if (this.opening) {
      this.opening = false;
    }
    else if (!this.dbopen && !this.serveropen) {
      this.log.warn('broker is not running');
      this.event(BrokerEvent.stop);
      return this;
    }

    this.log.warn(`shutting down - ${force ? 'forced' : 'graceful'}`);

    this.closing = true;
    this.closingerr = null;

    spinlock({
      ms:    this.opt.waitms,
      errms: this.opt.closems,
      test:  () => force || !this.workers.some(w => !!w.job),
      pre:   () => {
        const num = this.workers.filter(w => !!w.job).length;

        this.log.info(`waiting on ${num} workers…`);
      }
    })
    .catch((err) => {
      this.log.error('timeout elapsed, closing now', err);

      this.closingerr = err;
    })
    .then(() => {
      if (this.serveropen) {
        this.server.stop();
      }
      if (this.dbopen) {
        this.db.close(force);
      }
    })
    .catch(() => {
      // for linter
    });

    return this;
  }

  async pingWorkers() {
    const start = Date.now();
    const bad: 'unreachable' = 'unreachable';

    return Promise.all(
      this.workers.map(async (w) => ({
        id:   w.id,
        name: w.name,
        ms:   await w.sendMsg(M.Code.ping) ? (Date.now() - start) : bad
      }))
    );
  }

  async createJob<T>(opt: JobCreate<T> = {}) {
    if (this.closing) {
      throw new Error('broker is closing');
    }
    else if (!this.dbopen) {
      throw new Error('broker is not ready');
    }

    let job: Job<T>|null;

    if (this.serveropen) {
      const worker = this.workers.find(w => !w.job && !w.locked && !w.closing);

      if (worker) {
        // lock available worker, assign job now
        worker.locked = true;

        job = await this.addJob(opt, worker.id, 'processing');

        if (job) {
          await this.assignJob(worker, job);
        }

        worker.locked = false;
      }
      else {
        this.event(BrokerEvent.full);
        this.log.info('no workers available for assignment');

        // no worker available, assign job later
        job = await this.addJob(opt, null, 'ready');
      }
    }
    else {
      // socket server down, assign job later
      job = await this.addJob(opt, null, 'ready');
    }

    if (!job) {
      throw new Error('could not create job');
    }

    this.log.debug(`added job ${job.id} (${job.status})`);

    return job;
  }

  async tryAssignJob() {
    if (this.closing) {
      this.log.warn('skip job assign: broker is closing');
      return false;
    }
    else if (!this.serveropen || !this.dbopen) {
      this.log.warn('skip job assign: broker is not ready');
      return false;
    }

    const worker = this.workers.find(w => !w.job && !w.locked && !w.closing);

    if (!worker) {
      this.event(BrokerEvent.full);
      this.log.info('no workers available for assignment');
      return false;
    }

    let assigned = false;

    worker.locked = true;

    const job = await this.reserveJob(worker);

    if (job) {
      assigned = await this.assignJob(worker, job);
    }
    else {
      this.event(BrokerEvent.drain);
    }

    worker.locked = false;

    return assigned;
  }

  async clearJobs() {
    return this.db.clear();
  }

  private async tryReplay(replay: Replay) {
    try {
      const { workerid, data } = replay;

      const job = await this.getJobById(data.jobid);

      if (!job) {
        // replay job finished more than once
        this.log.warn('replay job missing (reaper)', data.jobid);
      }
      else if (job.workerid != workerid) {
        this.log.error('replay job reassigned (reaper)', data.jobid);

        // TODO: cancel/abort replay job double still ongoing
      }
      else {
        return this.finishJob(job.workerid, job, data);
      }
    }
    catch (err) {
      this.replays.push(replay);

      this.log.error('replay fetch error, queued for later', err);

      return false;
    }

    return true;
  }

  private async tryResume(worker: Remote, meta: JobMeta) {
    let strategy: JobStrategy;

    try {
      const job = await this.getJobById(meta.id);

      if (!job) {
        // resume job already picked up by reaper and completed
        this.log.error('resume job missing (reaper)', meta.id);

        strategy = 'execquiet';
      }
      else if (job.workerid != worker.id) {
        this.log.error('resume job reassigned (reaper)', meta.id);

        // TODO: cancel/abort ongoing job executing again concurrently

        strategy = 'execquiet';
      }
      else {
        // re-associate with original worker
        worker.job = job;

        this.log.warn('job resumed', meta.id);

        strategy = 'exec';
      }
    }
    catch (err) {
      this.log.error('resume fetch error', err);

      strategy = 'execquiet';
    }

    return strategy;
  }

  private startGC() {
    const gc = this.opt.gc;

    if (!gc) {
      this.log.warn('gc disabled');
    }
    else if (gc.interval > 0) {
      if (this.gctimer) {
        this.log.error('gc already running');
        return;
      }

      this.log.warn(`gc set to run every ${gc.interval} ms (${gc.statuses})`);

      this.gctimer = setInterval(() => this.gc(gc.statuses), gc.interval);

      this.gctimer.unref();
    }
    else {
      this.log.warn(`gc set to run after job completion (${gc.statuses})`);
    }
  }

  private gc(statuses: JobStatus[]) {
    setTimeout(async () => {
      try {
        let removed = 0;

        for (const s of statuses) {
          removed += await this.db.removeByStatus(s);
        }

        this.log.debug(`gc removed ${removed} jobs ${statuses}`);
      }
      catch (err) {
        this.log.error('gc err', err);
      }
    }, 0)
    .unref();
  }

  private gcById(id: JobId) {
    setTimeout(async () => {
      try {
        await this.db.removeById(id);
      }
      catch (err) {
        this.log.error('gc err', err);
      }
    }, 0)
    .unref();
  }

  private stopGC() {
    if (!this.gctimer) {
      this.log.debug('gc not running, skip stop');
      return;
    }

    clearInterval(this.gctimer);

    this.gctimer = null;

    this.log.warn('gc stopped');
  }

  private startReaper() {
    if (this.reaptimer) {
      this.log.error('reaper already started');
      return;
    }

    this.reaptimer = setInterval(async () => this.reap(), this.opt.reapms);

    this.reaptimer.unref();

    this.log.warn('reaper started');
  }

  private async reap() {
    try {
      const e = await this.db.refreshExpired();
      const s = await this.db.refreshStalled();

      this.log.debug(`reaped ${e + s} jobs (${e} expired, ${s} stalled)`);
    }
    catch (err) {
      this.log.error('reap error', err);
    }
  }

  private stopReaper() {
    if (!this.reaptimer) {
      this.log.debug('reaper not running, skip stop');
      return;
    }

    clearInterval(this.reaptimer);
    this.reaptimer = null;

    this.log.warn('reaper stopped');
  }

  private makeDb() {
    const { engine: DbEngine, dbc } = this.opt.db;
    const db = new DbEngine(dbc);

    db.logLevel = this.opt.db.log;

    db.on(DbEvent.open, () => {
      this.log.debug('db open');

      if (this.dbopen) {
        this.log.error('ignore duplicate db open event');
        return;
      }

      this.dbopen = true;
      this.retries = 0;

      if (!this.replaying) {
        if (this.replays.length > 0) {
          this.replaying = true;

          setTimeout(async () => {
            const replays = this.replays.slice(); // snapshot
            this.replays.length = 0;              // truncate

            let ok = 0;

            for (const r of replays) {
              if (await this.tryReplay(r)) {
                ok++;
              }
            }

            const requeued = replays.length - ok;

            this.log.warn(`replay: ${ok} ok, ${requeued} re-queued`);

            this.replaying = false;

            if (requeued === 0) {
              // if any jobs requeued, db failed, reap next time
              // let replays finish before reaping
              this.startReaper();
            }
          }, 0);
        }
        else {
          this.startReaper();
        }
      }

      this.startGC();

      if (this.opening) {
        if (this.serveropen) {
          this.opening = false;

          this.event(BrokerEvent.start);
        }
      }
      else {
        this.event(BrokerEvent.start);
      }
    });

    db.on(DbEvent.close, () => {
      this.log.debug('db close');

      this.dbopen = false;

      this.stopGC();
      this.stopReaper();

      if (this.closing) {
        if (!this.serveropen) {
          this.closing = false;

          this.event(BrokerEvent.stop, this.closingerr);
        }
      }
      else {
        this.event(BrokerEvent.stop, this.closingerr);

        this.log.info('reconnecting');

        this.retrytimer = setTimeout(() => {
          const retriesLeft = (
            !this.opt.db.retryx || (this.opt.db.retryx > this.retries)
          );

          if (retriesLeft) {
            this.log.info(`connecting (attempt ${++this.retries})`);
            this.db.open();
          }
          else {
            this.log.info('skip reconnecting');

            this.opening = false;

            if (!retriesLeft) {
              this.log.warn('no retries left, stopping');
              this.stop();
            }
          }
        }, this.opt.db.retryms);
      }
    });

    return db;
  }

  private makeServer() {
    const server = new Server(this.opt.server);

    server.on(ServerEvent.start, () => {
      this.log.debug('server start');

      if (this.serveropen) {
        this.log.error('ignore duplicate server start event');
        return;
      }

      this.serveropen = true;

      if (this.opening && this.dbopen) {
        this.opening = false;

        this.event(BrokerEvent.start);
      }
    });

    server.on(ServerEvent.stop, () => {
      this.log.debug('server stop');

      if (!this.serveropen) {
        this.log.error('ignore duplicate server stop event');
        return;
      }

      this.serveropen = false;

      if (this.closing && !this.dbopen) {
        this.closing = false;

        this.event(BrokerEvent.stop, this.closingerr);
      }
    });

    server.on(ServerEvent.error, (err: Error) => {
      this.log.debug('server error', err);

      this.serveropen = false;

      this.event(BrokerEvent.error, err);
    });

    server.on(ServerEvent.workerconnect, (worker: Remote) => {
      this.receiveWorker(worker);
    });

    server.on(ServerEvent.workerdisconnect, (worker: Remote) => {
      this.workers.splice(this.workers.indexOf(worker), 1);

      this.event(BrokerEvent.workerpart);
    });

    return server;
  }

  private receiveWorker(worker: Remote) {
    worker.on(RemoteEvent.meta, () => {
      this.log.debug(`meta from ${worker.name}`);
    });

    worker.on(RemoteEvent.ping, async () => {
      this.log.debug(`ping from ${worker.name}`);

      await worker.sendMsg(M.Code.pong);
    });

    worker.on(RemoteEvent.pong, () => {
      this.log.debug(`pong from ${worker.name}`);
    });

    worker.on(RemoteEvent.ready, async (msg: M.Ready) => {
      let strategy: JobStrategy;

      if (msg.replay) {
        await this.tryReplay({
          workerid: worker.id,
          data:     msg.replay
        });

        // replay only occurs once, nothing more for worker to do
        strategy = 'exec';
      }
      else if (msg.resume) {
        // send follow-up strategy to worker
        strategy = await this.tryResume(worker, msg.resume);
      }
      else {
        // normal startup
        strategy = 'exec';
      }

      this.log.info(`worker ${worker.name} is ready (${strategy})`);

      this.workers.push(worker);

      this.event(BrokerEvent.workerjoin);

      const readyOk: M.ReadyOk = { strategy };

      await worker.sendMsg(M.Code.readyok, readyOk);

      if (!this.opt.manual) {
        await this.tryAssignJob();
      }
    });

    worker.on(RemoteEvent.start, () => {
      if (!worker.job) {
        this.log.error('missing job (start)', worker.name);
        return;
      }

      worker.job.event(JobEvent.start);
    });

    worker.on(RemoteEvent.progress, async (msg: M.Progress) => {
      if (!worker.job) {
        this.log.error('missing job (progress)', worker.name);
        return;
      }

      if (worker.job.stallms) {
        try {
          await this.db.updateById(msg.jobid, {
            stalls: new Date(Date.now() + worker.job.stallms)
          });
        }
        catch (err) {
          this.log.error('error updating progress, job may stall', err);
        }
      }

      worker.job.event(JobEvent.progress, msg.progress);
    });

    worker.on(RemoteEvent.finish, async (msg: M.Finish) => {
      if (!worker.job) {
        this.log.error('missing job (finish)', worker.name);
        return;
      }

      await this.finishJob(worker.id, worker.job, msg);

      worker.job = null; // release worker even if update failed

      if (!this.opt.manual) {
        await this.tryAssignJob();
      }
    });
  }

  private async assignJob(worker: Remote, job: Job) {
    if (this.closing) {
      this.log.warn('skip job assign: broker is closing');
      return false;
    }
    else if (!this.serveropen || !this.dbopen) {
      this.log.warn('skip job assign: broker is not ready');
      return false;
    }
    else if (worker.job) {
      this.log.error('skip job assign: worker already has a job');
      return false;
    }
    else if (worker.closing) {
      this.log.warn('skip job assign:, worker closing');
      return false;
    }

    const add: M.Add = {
      job: Job.toJobAttr(job)
    };

    if (!(await worker.sendMsg(M.Code.add, add))) {
      this.log.warn('rolling back job assignment');

      if (!(await this.updateJob(job, 'ready'))) {
        // job stalled, will be resolved by reaper
        this.log.error('could not roll back job assignment');
      }

      return false;
    }

    worker.job = job;

    this.log.debug(`assigned job ${job.name} => ${worker.name}`);

    return true;
  }

  private async finishJob(wId: WorkerId, job: Job, msg: M.Finish) {
    if (await this.updateJob(job, msg.status)) {
      this.log.warn(`job ${job.name} finished`);

      this.event(BrokerEvent.jobdone);

      if (msg.status === 'failed') {
        job.event(JobEvent.fail, msg.result);
      }
      else if (msg.status === 'done') {
        job.event(JobEvent.done, msg.result);
      }

      return true;
    }
    else {
      this.replays.push({ workerid: wId, data: msg });

      this.log.error('finish failed, queued for later', msg.jobid);

      return false;
    }
  }

  private addJobHandle(j: JobAttr) {
    const job = new Job(j);

    // store job handle with client-attached events
    this.jobs.set(job.id, job);

    return job;
  }

  private removeJobHandle(job: Job) {
    this.jobs.delete(job.id);

    this.log.debug('freed job handle', job.id, this.jobs.size);
  }

  private async getJobById(id: JobId) {
    const j = await this.db.getById(id);

    if (j) {
      const job = this.jobs.get(j.id);

      return job || this.addJobHandle(j);
    }

    return null;
  }

  private async reserveJob(worker: Remote) {
    try {
      const j = await this.db.atomicFindReady(worker.id);

      if (!j) {
        return null;
      }

      // try to restore job with client events
      const job = this.jobs.get(j.id);

      if (job) {
        // update in-memory status to match update
        job.status = 'processing';
      }

      return job || this.addJobHandle(j);
    }
    catch (err) {
      this.log.error('could not reserve job', err);

      return null;
    }
  }

  private async addJob(opt: JobCreate, wId: WorkerId|null, status: JobStatus) {
    const id = uuid.v4() as JobId;
    const now = Date.now();

    const job = this.addJobHandle({
      id,
      workerid: wId,
      name:     opt.name || `job-${id}`,
      created:  new Date(now),
      expires:  opt.expirems ? new Date(now + opt.expirems) : null,
      expirems: opt.expirems || null,
      stalls:   opt.stallms ? new Date(now + opt.stallms) : null,
      stallms:  opt.stallms || null,
      status,
      attempts: 0,
      data:     opt.data || {}
    });

    try {
      await this.db.add(Job.toJobAttr(job));

      return job;
    }
    catch (err) {
      this.log.error('error saving job', err);

      this.removeJobHandle(job); // cleanup

      return null;
    }
  }

  private async updateJob(job: Job, newStatus: JobStatus) {
    this.log.info(`update job ${job.name} ${job.status} => ${newStatus}`);

    if (job.status === newStatus) {
      this.log.error('job status is the same');
      return false;
    }

    try {
      await this.db.updateById(job.id, { status: newStatus });
    }
    catch (err) {
      this.log.error('error updating job', err);
      return false;
    }

    // update in-memory status to match update
    job.status = newStatus;

    if (job.status === 'done') {
      this.removeJobHandle(job);

      if (this.opt.gc && this.opt.gc.interval === 0) {
        this.gcById(job.id);
      }
    }

    return true;
  }

  private event(e: BrokerEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
