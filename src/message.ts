import { JobStatus, JobAttr, JobId } from './job';
import { WorkerId, ExecStrategy, ResumeData } from './worker';

export const enum Code {
  // common
  meta = 1, // send meta, no response
  ping,     // request pong
  pong,     // respond to ping

  // broker => worker
  readyok,  // broker has registered worker
  add,      // job assigned to worker

  // worker => broker
  ready,    // ready to accept work
  start,    // received job
  progress, // making progress
  finish,   // finished job (done, fail)
}

// envelope
export interface Msg {
  readonly code:     Code;
  readonly data:     Data;
  readonly closing?: boolean;
}

// payload
export interface Data {}

export interface Add extends Data {
  readonly job: JobAttr;
}

export interface ReadyOk extends Data {
  readonly strategy: ExecStrategy; // follow-up instructions from broker
}

export interface Ready extends Data {
  readonly id:      WorkerId;
  readonly name:    string;
  readonly replay?: Finish|null;     // result if finished while disconnected
  readonly resume?: ResumeData|null; // request to resume active job
}

export interface Start extends Data {
  readonly jobid: JobId;
}

export interface Progress extends Data {
  readonly jobid:    JobId;
  readonly progress: number;
}

export interface Finish extends Data {
  readonly jobid:  JobId;
  readonly status: JobStatus;
  readonly result: any;
}
