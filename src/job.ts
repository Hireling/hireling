import { EventEmitter } from 'events';
import { WorkerId } from './worker';
import { NoopHandler } from './util';

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
  attempts: number;
  data:     T;           // user-supplied data
}

export const enum JobEvent {
  start    = 'start',
  progress = 'progress',
  done     = 'done',
  fail     = 'fail'
}

// tslint:disable:unified-signatures
export declare interface Job<T> {
  on(e: JobEvent.start|'start', fn: NoopHandler): this;
  on(e: JobEvent.progress|'progress', fn: NoopHandler): this;
  on(e: JobEvent.done|'done', fn: NoopHandler): this;
  on(e: JobEvent.fail|'fail', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

export class Job<T = any> extends EventEmitter {
  readonly attr: JobAttr<T>;

  constructor(val: JobAttr<T>) {
    super();

    this.attr = val;
  }

  event(e: JobEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
