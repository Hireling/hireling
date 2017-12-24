import { EventEmitter } from 'events';
import { WorkerId } from './worker';
import { NoopHandler } from './util';
import { Logger } from './logger';

export const enum _JobId {}
export type JobId = _JobId & string; // pseudo nominal typing

export type JobStatus = 'ready'|'processing'|'done'|'failed';

export interface JobAttr<T = any> {
  id:       JobId;
  workerid: WorkerId|null;
  name:     string;
  created:  Date;        // absolute
  expires:  Date|null;   // absolute
  expirems: number|null; // relative
  stalls:   Date|null;   // absolute
  stallms:  number|null; // relative
  status:   JobStatus;
  retryx:   number;      // number of times to rety
  retries:  number;      // current attempt count, also acts as seq number
  data:     T;           // user-supplied data
}

export const enum JobEvent {
  start    = 'start',
  progress = 'progress',
  done     = 'done',
  fail     = 'fail',
  expire   = 'expire',
  stall    = 'stall',
  retry    = 'retry'
}

// tslint:disable:unified-signatures
export declare interface Job<T> {
  on(e: JobEvent.start|'start', fn: NoopHandler): this;
  on(e: JobEvent.progress|'progress', fn: NoopHandler): this;
  on(e: JobEvent.done|'done', fn: NoopHandler): this;
  on(e: JobEvent.fail|'fail', fn: NoopHandler): this;
  on(e: JobEvent.expire|'expire', fn: NoopHandler): this;
  on(e: JobEvent.stall|'stall', fn: NoopHandler): this;
  on(e: JobEvent.retry|'retry', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export class Job<T = any> extends EventEmitter {
  readonly attr: JobAttr<T>;
  private expireTimer: NodeJS.Timer|null;
  private stallTimer: NodeJS.Timer|null;
  private readonly log: Logger;

  constructor(j: JobAttr<T>, log: Logger) {
    super();

    this.attr = j;
    this.log = log;

    const now = Date.now();

    if (this.attr.status === 'processing') {
      // recover timers
      if (this.attr.expirems && this.attr.expires) {
        this.log.debug('recover expire timer', this.attr.id);

        this.expireTimer = setTimeout(() => {
          this.event(JobEvent.expire);
        }, this.attr.expires.getTime() - now);

        this.expireTimer.unref();
      }

      if (this.attr.stallms && this.attr.stalls) {
        this.log.debug('recover stall timer', this.attr.id);

        this.stallTimer = setTimeout(() => {
          this.event(JobEvent.stall);
        }, this.attr.stalls.getTime() - now);

        this.stallTimer.unref();
      }
    }
  }

  startTimers(startedAt: Date) {
    if (this.attr.expirems && !this.attr.expires) {
      if (this.expireTimer) {
        this.log.error('expire timer already exists', this.attr.id);
      }
      else {
        const expireDate = new Date(startedAt.getTime() + this.attr.expirems);
        const offsetms = expireDate.getTime() - Date.now();

        this.expireTimer = setTimeout(() => {
          this.event(JobEvent.expire);
        }, offsetms);

        this.expireTimer.unref();
      }
    }

    if (this.attr.stallms) {
      if (this.stallTimer) {
        this.log.debug('refresh stall timer', this.attr.id);
        clearTimeout(this.stallTimer);
        this.stallTimer = null;
      }

      this.stallTimer = setTimeout(() => {
        this.event(JobEvent.stall);
      }, this.attr.stallms);

      this.stallTimer.unref();
    }
  }

  stopTimers() {
    this.log.debug('destroy timers');

    if (this.expireTimer) {
      this.log.debug('clear expire timer', this.attr.id);
      clearTimeout(this.expireTimer);
      this.expireTimer = null;
    }

    if (this.stallTimer) {
      this.log.debug('clear stall timer', this.attr.id);
      clearTimeout(this.stallTimer);
      this.stallTimer = null;
    }
  }

  static canRetry(j: JobAttr) {
    return j.retryx > 0 && j.retries < j.retryx;
  }

  async toPromise() {
    return new Promise<any>((resolve, reject) => {
      this.once('done', resolve);
      this.once('fail', reject);
    });
  }

  event(e: JobEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
