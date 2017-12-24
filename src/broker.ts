import { EventEmitter } from 'events';
import * as uuid from 'uuid';
import * as M from './message';
import { WorkerId, ExecStrategy, ExecData } from './worker';
import { Logger, LogLevel } from './logger';
import { Job, JobStatus, JobEvent, JobAttr, JobId } from './job';
import { spinlock, TopPartial, mergeOpt, NoopHandler, SeqLock } from './util';
import { Db, DbEvent, MemoryEngine } from './db';
import { Server, ServerEvent, SERVER_DEFS } from './server';
import { Remote, RemoteEvent } from './remote';

export interface JobCreate<T = any> {
  name?:     string;
  expirems?: number; // max life in ms from time of creation
  stallms?:  number; // max time allowed between worker updates
  retryx?:   number; // number of times to rety upon failure
  data?:     T;
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
  gc:      GC_DEFS as typeof GC_DEFS|null, // null disables gc
  manual:  false,                          // manual job flow control
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
  on(e: BrokerEvent.full|'full', fn: NoopHandler): this;
  on(e: BrokerEvent.drain|'drain', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export class Broker extends EventEmitter {
  private opening = false;
  private closing = false;
  private closingerr: Error|null = null;
  private retrytimer: NodeJS.Timer|null = null;
  private retries = 0;
  private serveropen = false;
  private dbopen = false;
  private gctimer: NodeJS.Timer|null = null;
  private readonly replays: Replay[] = [];       // queue for db-failed replays
  private readonly server: Server;
  private readonly db: Db;
  private readonly jobs = new Map<JobId, Job>(); // handles with client events
  private readonly workers: Remote[] = [];
  private readonly opt: BrokerOpt;
  private readonly log = new Logger(Broker.name);
  private readonly logJob = new Logger(Job.name); // shared instance

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
    this.logJob.level = val;
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

  async clearJobs() {
    this.jobs.forEach(job => this.removeJobHandle(job));
    // this.jobs.clear();

    await this.db.clear();
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
      const worker = this.workers
        .find(w => !w.job && !w.lock && !w.lockStart && !w.closing);

      if (worker) {
        // lock available worker, assign job now
        worker.lock = true;

        job = await this.addJob(opt, 'processing', worker.id);

        if (job) {
          await this.assignJob(worker, job);
        }

        worker.lock = false;
      }
      else {
        this.event(BrokerEvent.full);
        this.log.info('no workers available for assignment');

        // no worker available, assign job later
        job = await this.addJob(opt, 'ready', null);
      }
    }
    else {
      // socket server down, assign job later
      job = await this.addJob(opt, 'ready', null);
    }

    if (!job) {
      throw new Error('could not create job');
    }

    this.log.debug(`added job ${job.attr.id} (${job.attr.status})`);

    return job;
  }

  async tryAssignJob() {
    const worker = this.workers
      .find(w => !w.job && !w.lock && !w.lockStart && !w.closing);

    if (!worker) {
      this.event(BrokerEvent.full);
      this.log.info('no workers available for assignment');
      return false;
    }

    return this.assignToWorker(worker);
  }

  private async assignToWorker(worker: Remote) {
    if (this.closing) {
      this.log.warn('skip job assign: broker is closing');
      return false;
    }
    else if (!this.serveropen || !this.dbopen) {
      this.log.warn('skip job assign: broker is not ready');
      return false;
    }

    let assigned = false;

    worker.lock = true;

    const job = await this.reserveJob(worker);

    if (job) {
      assigned = await this.assignJob(worker, job);
    }
    else {
      this.event(BrokerEvent.drain);
    }

    worker.lock = false;

    return assigned;
  }

  private async replay(replay: Replay) {
    try {
      const { workerid, data } = replay;

      const job = await this.getJobById(data.job.jobid);

      if (!job) {
        this.log.warn('replay job already finished', data.job.jobid);
      }
      else if (!job.attr.workerid) {
        // recover job before it is reassigned
        await this.db.updateById(job.attr.id, { status: replay.data.status });
      }
      else if (job.attr.workerid !== workerid) {
        this.log.error('replay job reassigned', job.attr.id);
        // TODO: cancel/abort replay job double still ongoing
      }
      else if (job.attr.retries !== replay.data.job.retries) {
        this.log.error('replay job different instance', job.attr.id);
        // TODO: cancel/abort replay job double still ongoing
      }
      else {
        return this.finishJob(job.attr.workerid, job, data);
      }
    }
    catch (err) {
      this.replays.push(replay);
      this.log.error('replay fetch error, queued for later', err);
      return false;
    }

    return true;
  }

  private async reclaim(worker: Remote, ex: ExecData, e: RemoteEvent) {
    try {
      const job = await this.getJobById(ex.jobid);

      if (!job) {
        return null;
      }
      else if (!job.attr.workerid) {
        // TODO: job can be recovered here
        return null;
      }
      else if (job.attr.workerid !== worker.id) {
        return null;
      }
      else if (job.attr.retries !== ex.retries) {
        return null;
      }
      else {
        return job;
      }
    }
    catch (err) {
      this.log.error(`reclaim fetch error (${e})`, worker.name);
      return null;
    }
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

      this.gctimer = setInterval(async () => this.gc(gc.statuses), gc.interval);

      this.gctimer.unref();
    }
    else {
      this.log.warn(`gc set to run after job completion (${gc.statuses})`);
    }
  }

  private async gc(statuses: JobStatus[]) {
    let removed = 0;

    for (const s of statuses) {
      try {
        removed += await this.db.remove({ status: s });

        this.jobs.forEach(job => {
          if (job.attr.status === s) {
            this.removeJobHandle(job);
          }
        });
      }
      catch (err) {
        this.log.error(`gc err (${s})`, err);
      }
    }

    this.log.debug(`gc removed ${removed} jobs ${statuses}`);
  }

  private async gcSingle(job: Job) {
    try {
      this.removeJobHandle(job);

      await this.db.removeById(job.attr.id);
    }
    catch (err) {
      this.log.error('gc err', err);
    }
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

  private async expireJob(job: Job) {
    this.log.warn('job expired', job.attr.id, job.attr.status);

    if (job.attr.status === 'processing') {
      const update: Partial<JobAttr> = (Job.canRetry(job.attr)) ? {
        status:   'ready', // eligible for re-queue
        workerid: null,    // TODO: allow ongoing job recovery
        expires:  null,
        retries:  job.attr.retries + 1
      } : {
        status: 'failed'
      };

      if (job.attr.stalls !== null && Job.canRetry(job.attr)) {
        update.stalls = null;
      }

      if (await this.updateJob(job, update)) {
        job.stopTimers();
      }
    }
    else {
      this.log.warn('skip expire, job is no longer processing');
    }
  }

  private async stallJob(job: Job) {
    this.log.warn('job stalled', job.attr.id, job.attr.status);

    if (job.attr.status === 'processing') {
      const update: Partial<JobAttr> = (Job.canRetry(job.attr)) ? {
        status:   'ready', // eligible for re-queue
        workerid: null,    // TODO: allow ongoing job recovery
        stalls:   null,
        retries:  job.attr.retries + 1
      } : {
        status: 'failed'
      };

      if (job.attr.expires !== null && Job.canRetry(job.attr)) {
        update.expires = null;
      }

      if (await this.updateJob(job, update)) {
        job.stopTimers();
      }
    }
    else {
      this.log.warn('skip stall, job is no longer processing');
    }
  }

  private makeDb() {
    const { engine: DbEngine, dbc } = this.opt.db;
    const db = new DbEngine(dbc);

    db.logLevel = this.opt.db.log;

    db.on(DbEvent.open, async () => {
      this.log.debug('db open');

      if (this.dbopen) {
        this.log.error('ignore duplicate db open event');
        return;
      }

      try {
        // resync active jobs and timers
        const jobs = await this.db.get({ status: 'processing' });

        jobs.forEach(j => this.addJobHandle(j));

        this.log.warn(`loaded ${jobs.length} active jobs`);
      }
      catch (err) {
        // abort db start
        return;
      }

      for (const r of this.replays.slice()) {
        if (await this.replay(r)) {
          this.replays.splice(this.replays.indexOf(r), 1);
        }
        else {
          // abort replays and db start
          return;
        }
      }

      this.startGC();

      this.dbopen = true;
      this.retries = 0;

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
    const seq = new SeqLock(true); // process requests sequentially

    worker.on(RemoteEvent.meta, async () => {
      return seq.push(async () => {
        this.log.debug(`meta from ${worker.name}`);
      });
    });

    worker.on(RemoteEvent.ping, async () => {
      return seq.push(async () => {
        this.log.debug(`ping from ${worker.name}`);

        await worker.sendMsg(M.Code.pong);
      });
    });

    worker.on(RemoteEvent.pong, async () => {
      return seq.push(async () => {
        this.log.debug(`pong from ${worker.name}`);
      });
    });

    worker.on(RemoteEvent.ready, async (msg: M.Ready) => {
      return seq.push(async () => {
        if (msg.replay) {
          // replay only occurs once, nothing more for worker to do
          await this.replay({ workerid: worker.id, data: msg.replay });
        }

        worker.lockStart = false; // crash recovery
        worker.job = null; // crash recovery

        const strat: ExecStrategy = 'exec';
        const readyOk: M.ReadyOk = { strategy: strat };

        if (await worker.sendMsg(M.Code.readyok, readyOk)) {
          this.log.info(`worker ${worker.name} ready (${strat})`);

          this.workers.push(worker);

          this.event(BrokerEvent.workerjoin);

          if (!this.opt.manual) {
            await this.assignToWorker(worker);
          }
        }
      });
    });

    worker.on(RemoteEvent.resume, async (msg: M.Resume) => {
      return seq.push(async () => {
        const job = await this.reclaim(worker, msg.job, RemoteEvent.resume);

        if (job) {
          // job recovered, update timers relative to original start
          await this.recoverTimers(job, msg.job);
        }

        // lock worker if job was not reclaimed (in lieu of job assignment)
        worker.lockStart = !job;
        worker.job = job;

        const strat: ExecStrategy = job ? 'exec' : 'execquiet';
        const readyOk: M.ReadyOk = { strategy: strat };

        if (await worker.sendMsg(M.Code.readyok, readyOk)) {
          this.log.info(`worker ${worker.name} resume (${strat})`);

          this.workers.push(worker);

          this.event(BrokerEvent.workerjoin);
        }
      });
    });

    worker.on(RemoteEvent.start, async (msg: M.Start) => {
      return seq.push(async () => {
        const job = await this.reclaim(worker, msg.job, RemoteEvent.start);

        if (!job) {
          return;
        }

        await this.recoverTimers(job, msg.job);

        job.event(JobEvent.start);
      });
    });

    worker.on(RemoteEvent.progress, async (msg: M.Progress) => {
      return seq.push(async () => {
        const job = await this.reclaim(worker, msg.job, RemoteEvent.progress);

        if (!job) {
          return;
        }

        await this.recoverTimers(job, msg.job);

        job.event(JobEvent.progress, msg.progress);
      });
    });

    worker.on(RemoteEvent.finish, async (msg: M.Finish) => {
      return seq.push(async () => {
        const job = await this.reclaim(worker, msg.job, RemoteEvent.finish);

        if (worker.lockStart) {
          this.log.warn('unlocking worker resume');
          // always unlock worker, even when discarding job result
          worker.lockStart = false;
        }

        if (!job) {
          return;
        }

        // await this.recoverTimers(job, msg.job);

        await this.finishJob(worker.id, job, msg);

        worker.job = null; // release worker even if update failed

        if (!this.opt.manual) {
          await this.assignToWorker(worker);
        }
      });
    });
  }

  private async recoverTimers(job: Job, execData: ExecData) {
    const update: { expires?: Date; stalls?:  Date } = {};
    const startTs = execData.started.getTime();
    const lastProgTs = job.attr.stalls && job.attr.stalls.getTime() || 0;
    const progTs = Date.now();

    if (job.attr.expirems && !job.attr.expires) {
      update.expires = new Date(startTs + job.attr.expirems);
    }

    if (job.attr.stallms && lastProgTs !== progTs) {
      update.stalls = new Date(progTs + job.attr.stallms);
    }

    if (Object.keys(update).length > 0) {
      if (await this.updateJob(job, update)) {
        job.startTimers(execData.started);
      }
      else {
        this.log.error('timer update error, job may expire/stall', job.attr.id);
      }
    }
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

    const add: M.Assign = {
      job: job.attr
    };

    if (await worker.sendMsg(M.Code.assign, add)) {
      worker.job = job;

      this.log.debug(`assigned job ${job.attr.name} => ${worker.name}`);

      return true;
    }
    else {
      this.log.warn('rolling back job assignment');

      if (!(await this.updateJob(job, { workerid: null, status: 'ready' }))) {
        // job may expire/stall, will be resolved on next db start
        this.log.error('could not roll back job assignment');
      }

      return false;
    }
  }

  private async finishJob(wId: WorkerId, job: Job, msg: M.Finish) {
    const update: Partial<JobAttr> = {
      status: msg.status
    };

    if (msg.status === 'failed' && Job.canRetry(job.attr)) {
      update.status = 'ready'; // re-queue job to retry
      update.retries = job.attr.retries + 1;
    }

    if (job.attr.expires) {
      update.expires = null;
    }
    if (job.attr.stalls) {
      update.stalls = null;
    }

    job.stopTimers();

    if (await this.updateJob(job, update)) {
      const reason = (job.attr.retryx > 0) ?
        ` (retry ${job.attr.retries} of ${job.attr.retryx})` : '';

      this.log.warn(`job ${job.attr.name} => ${update.status}${reason}`);

      if (update.status === 'done') {
        job.event(JobEvent.done, msg.result);
      }
      else if (update.status === 'failed') {
        job.event(JobEvent.fail, msg.result);
      }
      else if (update.status === 'ready') {
        job.event(JobEvent.retry, msg.result);
      }

      return true;
    }
    else {
      this.replays.push({ workerid: wId, data: msg });

      this.log.error('finish failed, queued for later', msg.job.jobid);

      return false;
    }
  }

  private addJobHandle(j: JobAttr) {
    this.log.debug('create job handle', j.id);

    const job = new Job(j, this.logJob);

    // store job handle with client-attached events
    this.jobs.set(job.attr.id, job);

    job.on('expire', async () => this.expireJob(job));

    job.on('stall', async () => this.stallJob(job));

    return job;
  }

  private removeJobHandle(job: Job) {
    job.stopTimers();

    this.jobs.delete(job.attr.id);

    this.log.debug('freed job handle', job.attr.id);
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
      const j = await this.db.reserve(worker.id);

      if (!j) {
        return null;
      }

      // try to restore job with client events
      const job = this.jobs.get(j.id);

      if (job) {
        // sync mem
        job.attr.workerid = worker.id;
        job.attr.status = 'processing';
      }

      return job || this.addJobHandle(j);
    }
    catch (err) {
      this.log.error('could not reserve job', err);

      return null;
    }
  }

  private async addJob(opt: JobCreate, status: JobStatus, wId: WorkerId|null) {
    const id = uuid.v4() as JobId;

    const job = this.addJobHandle({
      id,
      workerid: wId,
      name:     opt.name || `job-${id}`,
      created:  new Date(),
      expires:  null, // set on job start
      expirems: opt.expirems || null,
      stalls:   null, // set on job start
      stallms:  opt.stallms || null,
      status,
      retryx:   opt.retryx || 0,
      retries:  0,
      data:     opt.data || {}
    });

    try {
      await this.db.add(job.attr);

      return job;
    }
    catch (err) {
      this.log.error('error saving job', err);

      this.removeJobHandle(job); // sync mem

      return null;
    }
  }

  private async updateJob(job: Job, update: Partial<JobAttr>) {
    const j = job.attr;

    this.log.info(`update job ${j.name}`, update);

    for (const k in update) {
      if ((j as any)[k] === (update as any)[k]) {
        this.log.error(`job update key '${k}' is the same (${(j as any)[k]})`);
      }
    }

    try {
      await this.db.updateById(j.id, update);
    }
    catch (err) {
      this.log.error('error updating job', update, err);
      return false;
    }

    Object.assign(j, update); // sync mem

    if (j.status === 'done') {
      if (this.opt.gc && this.opt.gc.interval === 0) {
        await this.gcSingle(job);
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
