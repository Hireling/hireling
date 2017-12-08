import { EventEmitter } from 'events';
import { WorkerId } from './worker';
import { NoopHandler } from './util';

export const enum _JobId {}
export type JobId = _JobId & string; // pseudo nominal typing

export type JobStatus = 'ready'|'processing'|'done'|'failed';
export type JobData = object; // user-supplied data

export interface JobMeta {
  id:       JobId;
  workerid: WorkerId|null;
  name:     string;
  created:  Date;        // absolute
  expires:  Date|null;   // absolute
  expirems: number|null; // relative
  stalls:   Date|null;   // absolute
  stallms:  number|null; // relative
  status:   JobStatus;
  attempts: number;
}

export interface JobAttr extends JobMeta {
  data: JobData;
}

export type JobStrategy = 'exec'|'execquiet'; // TODO: cancel, abort

export const enum JobEvent {
  start    = 'start',
  progress = 'progress',
  done     = 'done',
  fail     = 'fail'
}

// tslint:disable:unified-signatures
export declare interface Job {
  on(e: JobEvent.start|'start', fn: NoopHandler): this;
  on(e: JobEvent.progress|'progress', fn: NoopHandler): this;
  on(e: JobEvent.done|'done', fn: NoopHandler): this;
  on(e: JobEvent.fail|'fail', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export class Job extends EventEmitter implements JobAttr {
  readonly id: JobId;
  readonly workerid: WorkerId|null;
  readonly name: string;
  readonly created: Date;
  expires: Date|null;
  readonly expirems: number;
  stalls: Date|null;
  readonly stallms: number|null;
  status: JobStatus;
  attempts: number;
  readonly data: JobData;

  constructor(vals: JobAttr) {
    super();

    Object.assign(this, vals);
  }

  static toJobAttr(job: Job): JobAttr {
    return {
      id:       job.id,
      workerid: job.workerid,
      name:     job.name,
      created:  job.created,
      expires:  job.expires,
      expirems: job.expirems,
      stalls:   job.stalls,
      stallms:  job.stallms,
      status:   job.status,
      attempts: job.attempts,
      data:     job.data
    };
  }

  event(e: JobEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
