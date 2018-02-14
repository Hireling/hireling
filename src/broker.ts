import * as uuid from 'uuid';
import * as M from './message';
import { Logger, LogLevel } from './logger';
import { spinlock, TopPartial, mergeOpt, SeqLock } from './util';
import { Signal } from './signal';
import { Server, SERVER_DEFS } from './server';
import { JobId, JobAttr, Job, JobStatus } from './job';
import { WorkerId, ExecStrategy, ExecData } from './worker';
import { Db, MemoryEngine } from './db';
import { Remote } from './remote';
import { CtxArg, Ctx } from './ctx';

export interface JobCreate<I = any, O = any> {
  name?:     string;
  expirems?: number;
  stallms?:  number;
  retryx?:   number;
  sandbox?:  boolean;
  data?:     I;
  ctx?:      CtxArg<I, O>;
}

interface FinishData {
  workerid: WorkerId;
  ex:       ExecData;
  status:   JobStatus;
  result:   any;
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
  private readonly replays: FinishData[] = [];    // queue for db-failed replays
  private readonly server: Server;
  private readonly db: Db;
  private readonly jobs = new Map<JobId, Job>();  // handles with client events
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
      this.log.warn('broker is opening'); // up emitted later
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
      this.log.warn('broker is closing'); // down emitted later
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
        ms:   await w.sendMsg({ code: M.Code.ping }) ? Date.now() - start : bad
      }))
    );
  }

  async clearJobs() {
    await this.db.clear();

    this.jobs.forEach(job => this.removeJobHandle(job));
    // this.jobs.clear();
  }

  async createJob<I = any, O = any>(opt: JobCreate<I, O> = {}) {
    if (this.closing) {
      throw new Error('broker is closing');
    }
    else if (!this.dbopen) {
      throw new Error('broker is not ready');
    }

    let job: Job<I, O>|null;

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
          const job = await this.resumeJob(r.workerid, r.ex, 'replay');

          if (job) {
            await this.finishJob(job, r);
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
          if (!this.jobs.get(j.id)) {
            const job = this.addJobHandle(j);

            // use created date as fallback
            job.syncTimers(j.created);
          }
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

    worker.meta.on(seq.cb(async () => {
      this.log.debug(`meta from ${worker.name}`);
    }));

    worker.ping.on(seq.cb(async () => {
      this.log.debug(`ping from ${worker.name}`);

      await worker.sendMsg({ code: M.Code.pong });
    }));

    worker.pong.on(seq.cb(async () => {
      this.log.debug(`pong from ${worker.name}`);
    }));

    worker.ready.on(seq.cb(async (msg) => {
      if (msg.replay) {
        const { ex, result, status } = msg.replay;
        const data: FinishData = { workerid: worker.id, ex, result, status };

        try {
          const job = await this.resumeJob(worker.id, ex, 'ready');

          if (job) {
            await this.finishJob(job, data);
          }
        }
        catch (err) {
          this.replays.push(data);
        }
      }

      // crash recovery
      worker.lockStart = false;
      worker.job = null;

      const strat: ExecStrategy = 'exec';

      if (await worker.sendMsg({ code: M.Code.readyok, strat })) {
        this.log.info(`worker ${worker.name} ready (${strat})`);

        this.workers.push(worker);

        this.workerjoin.event();

        if (!this.opt.manual) {
          await this.reserveJob(worker);
        }
      }
    }));

    worker.resume.on(seq.cb(async (msg) => {
      const job = await this.resumeJob(worker.id, msg.ex, 'resume');

      // lock worker if active job unrecoverable (in lieu of job assignment)
      worker.lockStart = !job;
      worker.job = job;

      if (job) {
        const update = job.syncTimers(msg.ex.started);

        if (update) {
          await this.updateJob(job, update);
        }
      }

      const strat: ExecStrategy = job ? 'exec' : 'execquiet';

      if (await worker.sendMsg({ code: M.Code.readyok, strat })) {
        this.log.info(`worker ${worker.name} resume (${strat})`);

        this.workers.push(worker);

        this.workerjoin.event();
      }
    }));

    worker.start.on(seq.cb(async (msg) => {
      const job = await this.resumeJob(worker.id, msg.ex, 'start');

      if (!job) {
        return;
      }

      const update = job.syncTimers(msg.ex.started);

      if (update) {
        await this.updateJob(job, update);
      }

      job.start.event();
    }));

    worker.progress.on(seq.cb(async (msg) => {
      const job = await this.resumeJob(worker.id, msg.ex, 'progress');

      if (!job) {
        return;
      }

      const update = job.syncTimers(msg.ex.started);

      if (update) {
        await this.updateJob(job, update);
      }

      job.progress.event({ progress: msg.progress });
    }));

    worker.finish.on(seq.cb(async (msg) => {
      const job = await this.resumeJob(worker.id, msg.ex, 'finish');

      if (worker.lockStart) {
        this.log.warn('unlocking worker resume');
        // always unlock worker, even when discarding job result
        worker.lockStart = false;
      }

      if (!job) {
        return;
      }

      await this.finishJob(job, {
        workerid: worker.id,
        ex:       msg.ex,
        result:   msg.result,
        status:   msg.status
      });

      worker.job = null; // release worker even if update failed

      if (!this.opt.manual) {
        await this.reserveJob(worker);
      }
    }));
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

    if (await worker.sendMsg({ code: M.Code.assign, job: job.attr })) {
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

  private async finishJob(job: Job, msg: FinishData) {
    const { status, result } = msg;
    const update: Partial<JobAttr> = { status };

    if (status === 'failed' && job.canRetry()) {
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
        job.done.event({ result });
      }
      else if (update.status === 'failed') {
        job.fail.event(result);
      }
      else if (update.status === 'ready') {
        job.retry.event(result);
      }

      return true;
    }
    else {
      this.replays.push(msg);

      this.log.error('finish failed, queued for later', job.attr.id);

      return false;
    }
  }

  private addJobHandle<I>(j: JobAttr<I>) {
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
    try {
      const id = uuid.v4() as JobId;
      const cfg = Ctx.fromArg(opt.ctx || null);
      const j: JobAttr = {
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
        sandbox:  opt.sandbox === undefined ? null : opt.sandbox,
        data:     opt.data,
        ctx:      cfg ? cfg.ctx.toString() : null,
        ctxkind:  cfg ? cfg.ctxkind : null
      };

      await this.db.add(j);

      return this.addJobHandle(j);
    }
    catch (err) {
      this.log.error('error saving job', err);

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
