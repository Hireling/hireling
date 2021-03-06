import { JobAttr, JobStatus, JobId } from './job';
import { WorkerId } from './worker';
import { Logger, LogLevel } from './logger';
import { Signal } from './signal';
// @ts-ignore ignore unused for declaration emit
import { CtxKind } from './ctx';

export abstract class Db {
  readonly up   = new Signal();
  readonly down = new Signal<Error|null>();

  protected readonly log = new Logger(Db.name);

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  abstract open(): void;

  abstract close(force?: boolean): void;

  abstract add(job: JobAttr): Promise<void>;

  abstract getById(id: JobId): Promise<JobAttr|null>;

  abstract get(query: Partial<JobAttr>): Promise<JobAttr[]>;

  abstract reserve(wId: WorkerId): Promise<JobAttr|null>;

  abstract updateById(id: JobId, values: Partial<JobAttr>): Promise<void>;

  abstract removeById(id: JobId): Promise<boolean>;

  abstract remove(query: Partial<JobAttr>): Promise<number>;

  abstract clear(): Promise<number>;
}

export class MemoryEngine extends Db {
  private readonly jobs: JobAttr[] = [];

  constructor() {
    super();

    this.log.debug('db created (memory)');
  }

  open() {
    this.log.debug('opened');

    this.up.event();
  }

  close(force = false) {
    this.log.warn(`closing - ${force ? 'forced' : 'graceful'}`);

    this.down.event(null);
  }

  async add(job: JobAttr) {
    this.log.debug(`add job ${job.id}`);

    this.jobs.push({ ...job });
  }

  async getById(id: JobId) {
    this.log.debug('get job by id');

    const job = this.jobs.find(j => j.id === id);

    return job ? { ...job } : null;
  }

  async get(query: Partial<JobAttr>) {
    this.log.debug('search jobs');

    return this.jobs
      .filter(j =>
        Object.entries(query).every(([k, v]) => (j as any)[k] === v)
      )
      .map(j => ({ ...j }));
  }

  async reserve(wId: WorkerId) {
    this.log.debug(`atomic reserve job ${wId}`);

    const from: JobStatus = 'ready';
    const to: JobStatus = 'processing';

    const job = this.jobs.find(j => j.status === from);

    if (job) {
      job.status = to;
      job.workerid = wId;
    }

    return job ? { ...job } : null;
  }

  async updateById(id: JobId, values: Partial<JobAttr>) {
    this.log.debug('update job');

    const job = this.jobs.find(j => j.id === id);

    if (job) {
      Object.entries(values).forEach(([k, v]) => (job as any)[k] = v);
    }
  }

  async removeById(id: JobId) {
    this.log.debug('remove job by id');

    for (let i = this.jobs.length - 1; i >= 0; i--) {
      if (this.jobs[i].id === id) {
        this.jobs.splice(i, 1);
        return true;
      }
    }

    return false;
  }

  async remove(query: Partial<JobAttr>) {
    this.log.debug('remove jobs');

    const matches = this.jobs.filter(j =>
      Object.entries(query).every(([k, v]) => (j as any)[k] === v)
    );

    for (const m of matches) {
      this.jobs.splice(this.jobs.indexOf(m), 1);
    }

    return matches.length;
  }

  async clear() {
    const removed = this.jobs.length;

    this.jobs.length = 0; // truncate

    return removed;
  }
}
