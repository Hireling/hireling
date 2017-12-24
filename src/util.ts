import { EventEmitter } from 'events';
import { Broker, BrokerEvent } from './broker';
import { Worker, WorkerEvent } from './worker';
import { Job, JobEvent } from './job';

export type TopPartial<T> = {
  [P in keyof T]?: Partial<T[P]>;
};

// TODO: remove this
export type NoopHandler = (...args: any[]) => void;

// merge options recursively, ignore undefined keys
export function mergeOpt(tgt: object = {}, src: object = {}) {
  const def = { ...tgt };

  for (const [key, val] of Object.entries(src)) {
    if (Object.prototype.toString.call(val) === '[object Object]') {
      if (!(key in tgt)) {
        Object.assign(def, { [key]: val });
      }
      else {
        (def as any)[key] = mergeOpt((tgt as any)[key], val);
      }
    }
    else if (val !== undefined) {
      Object.assign(def, { [key]: val });
    }
  }

  return def;
}

export async function ewait(tgt: Broker, e: BrokerEvent): Promise<any[]>;
export async function ewait(tgt: Worker, e: WorkerEvent): Promise<any[]>;
export async function ewait(tgt: Job, e: JobEvent): Promise<any[]>;
export async function ewait(tgt: EventEmitter, e: string) {
  return new Promise<any[]>((resolve) => tgt.once(e, (...args) => resolve(args)));
}

export async function fnwait(ms: number, fn = () => undefined as any) {
  return new Promise((resolve) => setTimeout(() => resolve(fn()), ms));
}

export interface SpinlockOpt {
  ms?:    number;        // post-spin delay, in ms
  errms?: number;        // time after which to stop and throw, in ms
  test:   () => boolean; // spin until this predicate evaluates to true
  pre?:   () => void;    // fn to execute before every spin
}

export async function spinlock(opt: SpinlockOpt) {
  const { ms, errms, test, pre } = opt;

  const start = Date.now();

  while (!test()) {
    if (pre) {
      pre();
    }

    if (errms !== undefined && (Date.now() - start > errms)) {
      throw new Error('spinlock timeout');
    }

    await new Promise(resolve => setTimeout(resolve, ms || 0));
  }

  return 'spinlock done';
}

export type AsyncCb = (...args: any[]) => Promise<void>;

export class SeqLock {
  private lock = false;
  private cbs: AsyncCb[] = [];
  private tick = false;

  constructor(tick = false) {
    this.tick = tick;
  }

  async push(fn: AsyncCb) {
    this.cbs.push(fn);

    return this.run();
  }

  private async run(): Promise<void> {
    if (this.lock) {
      return;
    }

    const cb = this.cbs.shift();

    if (!cb) {
      return;
    }

    this.lock = true;

    try {
      await cb();
    }
    catch (err) {
      console.log('async queue err', err);
    }

    this.lock = false;

    return (this.tick) ? setImmediate(async () => this.run()) : this.run();
  }
}
