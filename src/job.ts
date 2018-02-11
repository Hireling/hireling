import { WorkerId } from './worker';
import { Logger } from './logger';
import { Signal } from './signal';
import { CtxKind } from './ctx';

export const enum _JobId {}
export type JobId = _JobId & string; // pseudo nominal typing

export type JobStatus = 'ready'|'processing'|'done'|'failed';

export interface JobProgress {
  progress: any;
}

export interface JobDone<O = any> {
  result: O;
}

export interface JobFailed {
  reason: 'error'|'aborted'|'exited';
  msg:    string;
}

export interface JobAttr<I = any> {
  id:       JobId;
  workerid: WorkerId|null;
  name:     string;
  created:  Date;         // absolute
  expires:  Date|null;    // absolute
  expirems: number|null;  // relative, max life from time of creation
  stalls:   Date|null;    // absolute
  stallms:  number|null;  // relative, max time between worker updates
  status:   JobStatus;
  retryx:   number;       // number of times to rety upon failure
  retries:  number;       // current attempt count, also acts as seq number
  sandbox:  boolean|null; // run this job inside a sandbox
  data:     I;            // user-supplied data
  ctx:      string|null;  // user-supplied execution context, stringified
  ctxkind:  CtxKind|null; // to restore ctx after stringify/parse
}

export class Job<I = any, O = any> {
  readonly start    = new Signal();
  readonly progress = new Signal<JobProgress>();
  readonly done     = new Signal<JobDone<O>>();
  readonly fail     = new Signal<JobFailed>();
  readonly expire   = new Signal();
  readonly stall    = new Signal();
  readonly retry    = new Signal();

  readonly attr: JobAttr<I>;
  private expireTimer: NodeJS.Timer|null = null;
  private stallTimer: NodeJS.Timer|null = null;
  private readonly log: Logger;

  constructor(j: JobAttr<I>, log: Logger) {
    this.attr = j;
    this.log = log;
  }

  syncTimers(startAt: Date) {
    const update: { expires?: Date; stalls?: Date } = {};
    const now = Date.now();

    if (!this.expireTimer && this.attr.expirems) {
      const ms = this.attr.expires ?
        this.attr.expires.getTime() - now :
        startAt.getTime() + this.attr.expirems - now;

      // may have negative timeout (already elapsed)
      this.expireTimer = setTimeout(() => this.expire.emit(), ms);
      this.expireTimer.unref();

      this.log.debug(`set expire timer (${ms}ms)`, this.attr.id);

      update.expires = new Date(now + ms);
    }

    if (this.attr.stallms) {
      if (this.stallTimer) {
        this.log.debug('refresh stall timer', this.attr.id);
        clearTimeout(this.stallTimer);
        this.stallTimer = null;
      }

      const ms = this.attr.stallms;

      this.stallTimer = setTimeout(() => this.stall.emit(), ms);
      this.stallTimer.unref();

      this.log.debug(`set stall timer (${ms}ms)`, this.attr.id);

      update.stalls = new Date(now + ms);
    }

    return Object.keys(update).length > 0 ? update : null;
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

  canRetry() {
    return this.attr.retryx > 0 && this.attr.retries < this.attr.retryx;
  }

  async toPromise() {
    return new Promise<JobDone<O>>((resolve, reject) => {
      this.done.once(resolve);
      this.fail.once(reject);
    });
  }
}
