import * as uuid from 'uuid';
import * as M from './message';
import { WorkerId, ExecStrategy, ExecData } from './worker';
import { Logger, LogLevel } from './logger';
import { Job, JobStatus, JobAttr, JobId } from './job';
import { spinlock, TopPartial, mergeOpt, SeqLock } from './util';
import { Db, MemoryEngine } from './db';
import { Server, SERVER_DEFS } from './server';
import { Remote } from './remote';
import { Signal } from './signal';

export interface JobCreate<T = any> {
  name?:     string;
  expirems?: number;
  stallms?:  number;
  retryx?:   number;
  sandbox?:  boolean;
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

export class Broker {
  readonly up         = new Signal();
  readonly down       = new Signal<Error|null>();
  readonly error      = new Signal<Error>();
  readonly workerjoin = new Signal();
  readonly workerpart = new Signal();
  readonly full       = new Signal();
  readonly drain      = new Signal();

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
      this.up.event();
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
      this.down.event(null);
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
    await this.db.clear();

    this.jobs.forEach(job => this.removeJobHandle(job));
    // this.jobs.clear();
  }

  async createJob<T = any>(opt: JobCreate<T> = {}) {
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
        this.full.event();
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
      this.full.event();
      this.log.info('no workers available for assignment');
      return false;
    }

    return this.reserveJob(worker);
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
      await this.db.removeById(job.attr.id);

      this.removeJobHandle(job);
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
      const update: Partial<JobAttr> = job.canRetry() ? {
        status:   'ready', // eligible for re-queue
        workerid: null,    // TODO: allow ongoing job recovery
        expires:  null,
        retries:  job.attr.retries + 1
      } : {
        status: 'failed'
      };

      if (job.attr.stalls !== null && job.canRetry()) {
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
      const update: Partial<JobAttr> = job.canRetry() ? {
        status:   'ready', // eligible for re-queue
        workerid: null,    // TODO: allow ongoing job recovery
        stalls:   null,
        retries:  job.attr.retries + 1
      } : {
        status: 'failed'
      };

      if (job.attr.expires !== null && job.canRetry()) {
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

    db.up.on(async () => {
      this.log.debug('db open');

      if (this.dbopen) {
        this.log.error('ignore duplicate db open event');
        return;
      }

      for (const r of this.replays.slice()) {
        try {
          const job = await this.resumeJob(r.workerid, r.data.job, 'replay');

          if (job) {
            await this.finishJob(r.workerid, job, r.data);
          }

          this.replays.splice(this.replays.indexOf(r), 1);
        }
        catch (err) {
          return; // abort db start
        }
      }

      try {
        // resync active jobs and timers
        const jobs = await this.db.get({ status: 'processing' });

        jobs.forEach(j => {
          const job = this.addJobHandle(j);

          // use created date as fallback
          job.syncTimers(job.attr.created);
        });

        this.log.warn(`loaded ${jobs.length} active jobs`);
      }
      catch (err) {
        return; // abort db start
      }

      this.startGC();

      this.dbopen = true;
      this.retries = 0;

      if (this.opening) {
        if (this.serveropen) {
          this.opening = false;

          this.up.event();
        }
      }
      else {
        this.up.event();
      }
    });

    db.down.on(() => {
      this.log.debug('db close');

      this.dbopen = false;

      this.stopGC();

      if (this.closing) {
        if (!this.serveropen) {
          this.closing = false;

          this.down.event(this.closingerr);
        }
      }
      else {
        this.down.event(this.closingerr);

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

    server.up.on(() => {
      this.log.debug('server start');

      if (this.serveropen) {
        this.log.error('ignore duplicate server start event');
        return;
      }

      this.serveropen = true;

      if (this.opening && this.dbopen) {
        this.opening = false;

        this.up.event();
      }
    });

    server.down.on(() => {
      this.log.debug('server stop');

      if (!this.serveropen) {
        this.log.error('ignore duplicate server stop event');
        return;
      }

      this.serveropen = false;

      if (this.closing && !this.dbopen) {
        this.closing = false;

        this.down.event(this.closingerr);
      }
    });

    server.error.on((err) => {
      this.log.debug('server error', err);

      this.serveropen = false;

      this.error.event(err);
    });

    server.workerup.on((worker) => {
      this.receiveWorker(worker);
    });

    server.workerdown.on((worker) => {
      this.workers.splice(this.workers.indexOf(worker), 1);

      this.workerpart.event();
    });

    return server;
  }

  private receiveWorker(worker: Remote) {
    const seq = new SeqLock(true); // process requests sequentially

    worker.meta.on(async () => {
      return seq.run(async () => {
        this.log.debug(`meta from ${worker.name}`);
      });
    });

    worker.ping.on(async () => {
      return seq.run(async () => {
        this.log.debug(`ping from ${worker.name}`);

        await worker.sendMsg(M.Code.pong);
      });
    });

    worker.pong.on(async () => {
      return seq.run(async () => {
        this.log.debug(`pong from ${worker.name}`);
      });
    });

    worker.ready.on(async (msg) => {
      return seq.run(async () => {
        const replay = msg.replay;

        if (replay) {
          try {
            const job = await this.resumeJob(worker.id, replay.job, 'ready');

            if (job) {
              await this.finishJob(worker.id, job, replay);
            }
          }
          catch (err) {
            this.replays.push({ workerid: worker.id, data: replay });
          }
        }

        // crash recovery
        worker.lockStart = false;
        worker.job = null;

        const strat: ExecStrategy = 'exec';
        const readyOk: M.ReadyOk = { strategy: strat };

        if (await worker.sendMsg(M.Code.readyok, readyOk)) {
          this.log.info(`worker ${worker.name} ready (${strat})`);

          this.workers.push(worker);

          this.workerjoin.event();

          if (!this.opt.manual) {
            await this.reserveJob(worker);
          }
        }
      });
    });

    worker.resume.on(async (msg) => {
      return seq.run(async () => {
        const job = await this.resumeJob(worker.id, msg.job, 'resume');

        // lock worker if active job unrecoverable (in lieu of job assignment)
        worker.lockStart = !job;
        worker.job = job;

        if (job) {
          const update = job.syncTimers(msg.job.started);

          if (update) {
            await this.updateJob(job, update);
          }
        }

        const strat: ExecStrategy = job ? 'exec' : 'execquiet';
        const readyOk: M.ReadyOk = { strategy: strat };

        if (await worker.sendMsg(M.Code.readyok, readyOk)) {
          this.log.info(`worker ${worker.name} resume (${strat})`);

          this.workers.push(worker);

          this.workerjoin.event();
        }
      });
    });

    worker.start.on(async (msg) => {
      return seq.run(async () => {
        const job = await this.resumeJob(worker.id, msg.job, 'start');

        if (!job) {
          return;
        }

        const update = job.syncTimers(msg.job.started);

        if (update) {
          await this.updateJob(job, update);
        }

        job.start.event();
      });
    });

    worker.progress.on(async (msg) => {
      return seq.run(async () => {
        const job = await this.resumeJob(worker.id, msg.job, 'progress');

        if (!job) {
          return;
        }

        const update = job.syncTimers(msg.job.started);

        if (update) {
          await this.updateJob(job, update);
        }

        job.progress.event(msg.progress);
      });
    });

    worker.finish.on(async (msg) => {
      return seq.run(async () => {
        const job = await this.resumeJob(worker.id, msg.job, 'finish');

        if (worker.lockStart) {
          this.log.warn('unlocking worker resume');
          // always unlock worker, even when discarding job result
          worker.lockStart = false;
        }

        if (!job) {
          return;
        }

        await this.finishJob(worker.id, job, msg);

        worker.job = null; // release worker even if update failed

        if (!this.opt.manual) {
          await this.reserveJob(worker);
        }
      });
    });
  }

  private async resumeJob(wId: WorkerId, ex: ExecData, tag: string) {
    const { jobid, retries } = ex;

    const job = this.jobs.get(jobid);

    const j = job && job.attr || await this.db.getById(jobid);

    if (!j) {
      this.log.warn(`${tag} job not found`, jobid);
    }
    else if (!j.workerid) {
      this.log.error(`${tag} job unassigned`, j.id);
      // TODO: lock and recover
    }
    else if (j.workerid !== wId) {
      this.log.error(`${tag} job reassigned`, j.id);
      // TODO: cancel/abort
    }
    else if (j.retries !== retries) {
      this.log.error(`${tag} job has a new instance`, j.id);
      // TODO: cancel/abort
    }
    else if (j.status !== 'processing') {
      this.log.error(`${tag} job no longer processing`, j.id);
      // TODO: lock and recover
    }
    else {
      return job || this.addJobHandle(j);
    }

    return null;
  }

  private async reserveJob(worker: Remote) {
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

    try {
      const j = await this.db.reserve(worker.id);

      if (j) {
        let job = this.jobs.get(j.id);

        if (job) {
          // sync mem
          job.attr.workerid = worker.id;
          job.attr.status = 'processing';
        }
        else {
          job = this.addJobHandle(j);
        }

        assigned = await this.assignJob(worker, job);
      }
      else {
        this.drain.event();
      }
    }
    catch (err) {
      this.log.error('could not reserve job', err);
    }

    worker.lock = false;

    return assigned;
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

    if (msg.status === 'failed' && job.canRetry()) {
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
        job.done.event(msg.result);
      }
      else if (update.status === 'failed') {
        job.fail.event(msg.result);
      }
      else if (update.status === 'ready') {
        job.retry.event(msg.result);
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

    this.jobs.set(job.attr.id, job);

    job.expire.on(async () => this.expireJob(job));

    job.stall.on(async () => this.stallJob(job));

    return job;
  }

  private removeJobHandle(job: Job) {
    job.stopTimers();

    this.jobs.delete(job.attr.id);

    this.log.debug('freed job handle', job.attr.id);
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
      sandbox:  opt.sandbox || false,
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
}
