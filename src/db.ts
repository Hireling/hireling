import { EventEmitter } from 'events';
import { JobAttr, JobStatus, JobId } from './job';
import { WorkerId } from './worker';
import { Logger, LogLevel } from './logger';
import { NoopHandler } from './util';

export const enum DbEvent {
  open  = 'open',
  close = 'close'
}

// tslint:disable:unified-signatures
export declare interface Db {
  on(e: DbEvent.open|'open', fn: NoopHandler): this;
  on(e: DbEvent.close|'close', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export abstract class Db extends EventEmitter {
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

  protected event(e: DbEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}

export class MemoryEngine extends Db {
  private readonly jobs: JobAttr[] = [];

  constructor() {
    super();

    this.log.debug('db created (memory)');
  }

  open() {
    this.log.debug('opened');

    this.event(DbEvent.open);
  }

  close(force = false) {
    this.log.warn(`closing - ${force ? 'forced' : 'graceful'}`);

    this.event(DbEvent.close);
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
      // @ts-ignore ignore implicit any type
      .filter(j => Object.entries(query).every(([k, v]) => job[k] === v))
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
      // @ts-ignore ignore implicit any type
      Object.entries(values).forEach(([k, v]) => job[k] = v);
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

    const matches = this.jobs
      // @ts-ignore ignore implicit any type
      .filter(j => Object.entries(query).every(([k, v]) => job[k] === v));

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
