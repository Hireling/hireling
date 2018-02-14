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
  finish    // job finished (done, fail)
}

export interface BaseMsg {
  readonly closing?: boolean;
}

export interface Meta extends BaseMsg {
  readonly code: Code.meta;
}

export interface Ping extends BaseMsg {
  readonly code: Code.ping;
}

export interface Pong extends BaseMsg {
  readonly code: Code.pong;
}

export interface ReadyOk extends BaseMsg {
  readonly code:  Code.readyok;
  readonly strat: ExecStrategy; // follow-up instructions from broker
}

export interface Assign extends BaseMsg {
  readonly code: Code.assign;
  readonly job:  JobAttr;
}

export interface Abort extends BaseMsg {
  readonly code: Code.abort;
}

export interface Ready extends BaseMsg {
  readonly code:   Code.ready;
  readonly id:     WorkerId;
  readonly name:   string;
  readonly replay: Finish|null; // replay job result obtained while d/c
}

export interface Resume extends BaseMsg {
  readonly code: Code.resume;
  readonly id:   WorkerId;
  readonly name: string;
  readonly ex:   ExecData; // resume active job, upgrades to replay
}

export interface Start extends BaseMsg {
  readonly code: Code.start;
  readonly ex:   ExecData;
}

export interface Progress extends BaseMsg {
  readonly code:     Code.progress;
  readonly ex:       ExecData;
  readonly progress: any; // job-generated
}

export interface Finish extends BaseMsg {
  readonly code:   Code.finish;
  readonly ex:     ExecData;
  readonly status: JobStatus;
  readonly result: any; // job-generated
}

export type Msg =
  Meta|Ping|Pong|ReadyOk|Assign|Abort|Ready|Resume|Start|Progress|Finish;
