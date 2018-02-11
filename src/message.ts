import { JobStatus, JobAttr } from './job';
import { WorkerId, ExecStrategy, ExecData } from './worker';

export const enum Code {
  // common
  meta = 1, // send meta, one-way
  ping,     // request pong
  pong,     // respond to ping

  // broker => worker
  readyok,  // broker has registered worker
  assign,   // notify worker of job assignment
  abort,    // abort current job

  // worker => broker
  ready,    // connected, ready to accept work
  resume,   // connected, job already in progress
  start,    // job starting
  progress, // job progress (from user)
  finish,   // job finished (done, fail)
}

// envelope
export interface Msg {
  readonly code:     Code;
  readonly data:     Data;
  readonly closing?: boolean;
}

// payload
export interface Data {}

export interface Assign extends Data {
  readonly job: JobAttr;
}

export interface ReadyOk extends Data {
  readonly strategy: ExecStrategy; // follow-up instructions from broker
}

export interface Ready extends Data {
  readonly id:     WorkerId;
  readonly name:   string;
  readonly replay: Finish|null; // replay job result obtained while d/c
}

export interface Resume extends Data {
  readonly id:   WorkerId;
  readonly name: string;
  readonly job:  ExecData; // resume active job, upgrades to replay
}

export interface Start extends Data {
  readonly job: ExecData;
}

export interface Progress extends Data {
  readonly job:      ExecData;
  readonly progress: any; // job-generated
}

export interface Finish extends Data {
  readonly job:    ExecData;
  readonly status: JobStatus;
  readonly result: any; // job-generated
}
