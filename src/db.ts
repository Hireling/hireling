import { EventEmitter } from 'events';
import { JobAttr, JobStatus, JobId } from './job';
import { WorkerId } from './worker';
import { Logger, LogLevel } from './logger';

export const enum DbEvent {
  open  = 'open',
  close = 'close'
}

export declare interface Db {
  on(event: DbEvent, fn: (...args: any[]) => void): this;
}

export abstract class Db extends EventEmitter {
  protected readonly log = new Logger(Db.name);

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  abstract open(): void;

  abstract close(force?: boolean): void;

  abstract clear(): Promise<number>;

  abstract add(job: JobAttr): Promise<void>;

  abstract getById(id: JobId): Promise<JobAttr|null>;

  abstract atomicFindReady(wId: WorkerId): Promise<JobAttr|null>;

  abstract updateById(id: JobId, values: Partial<JobAttr>): Promise<void>;

  abstract removeById(id: JobId): Promise<boolean>;

  abstract removeByStatus(status: JobStatus): Promise<number>;

  abstract refreshExpired(): Promise<number>;

  abstract refreshStalled(): Promise<number>;

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

  async clear() {
    const removed  = this.jobs.length;

    this.jobs.length = 0; // truncate

    return removed;
  }

  async add(job: JobAttr) {
    this.log.debug(`add job ${job.id}`);

    this.jobs.push({ ...job });
  }

  async getById(id: JobId) {
    this.log.debug('get job by id');

    return this.jobs.find(j => j.id === id) || null;
  }

  async atomicFindReady(wId: WorkerId) {
    this.log.debug(`atomic find update job ${wId}`);

    const from: JobStatus = 'ready';
    const to: JobStatus  = 'processing';

    const job = this.jobs.find(j => j.status === from);

    if (job) {
      job.status = to;
      job.workerid = wId;
    }

    return job || null;
  }

  async updateById(id: JobId, values: Partial<JobAttr>) {
    this.log.debug('update job');

    const job = await this.getById(id);

    if (job) {
      Object.entries(values).forEach(([k, v]) => {
        // @ts-ignore ignore implicit any type
        if (job[k] != v) {
          // @ts-ignore ignore implicit any type
          job[k] = v;
        }
      });
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

  async removeByStatus(status: JobStatus) {
    this.log.debug(`remove jobs by status [${status}]`);

    let removed = 0;

    for (let i = this.jobs.length - 1; i >= 0; i--) {
      if (this.jobs[i].status === status) {
        this.jobs.splice(i, 1);
        removed++;
      }
    }

    return removed;
  }

  async refreshExpired() {
    this.log.debug('refresh expired jobs');

    const now = Date.now();
    let updated = 0;

    this.jobs
      .filter(j =>
        j.status === 'processing' &&
        j.expirems && j.expires && j.expires.getTime() < now
      )
      .forEach(j => {
        j.status = 'ready';
        j.workerid = null;
        j.expires = new Date(now + j.expirems!);
        j.attempts++;

        updated++;
      });

    return updated;
  }

  async refreshStalled() {
    this.log.debug('refresh stalled jobs');

    const now = Date.now();
    let updated = 0;

    this.jobs
      .filter(j =>
        j.status === 'processing' &&
        j.stallms && j.stalls && j.stalls.getTime() < now
      )
      .forEach(j => {
        j.status = 'ready';
        j.workerid = null;
        j.stalls = new Date(now + j.stallms!);
        j.attempts++;

        updated++;
      });

    return updated;
  }
}
