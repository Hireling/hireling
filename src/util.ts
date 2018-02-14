import { Signal } from './signal';

export type TopPartial<T> = {
  [P in keyof T]?: Partial<T[P]>;
};

// merge options recursively, ignore undefined values
export const mergeOpt = (tgt: object = {}, src: object = {}) => {
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
};

export const swait = async <T>(e: Signal<T>) => new Promise<T>(e.once.bind(e));

export const fnwait = async (ms: number, fn = () => undefined as any) =>
  new Promise(resolve => setTimeout(() => resolve(fn()), ms));

export interface SpinlockOpt {
  ms?:    number;        // post-spin delay, in ms
  errms?: number;        // time after which to stop and throw, in ms
  test:   () => boolean; // spin until this predicate evaluates to true
  pre?:   () => void;    // fn to execute before every spin
}

export const spinlock = async (opt: SpinlockOpt) => {
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
};

export type CbWrap = (...args: any[]) => Promise<void>;
export type AsyncCb<T = any> = (arg: T) => Promise<void>;

export class SeqLock {
  private lock = false;
  private cbs: CbWrap[] = [];
  private tick = false;

  constructor(tick: boolean) {
    this.tick = tick;
  }

  async run(fn: AsyncCb) {
    this.cbs.push(fn);

    return this.turn();
  }

  cb<T>(fn: AsyncCb<T>) {
    return (arg: T) => {
      this.cbs.push(async () => fn(arg));

      this.turn().catch(SeqLock.asyncErr);
    };
  }

  private async turn(): Promise<void> {
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
      SeqLock.asyncErr(err);
    }

    this.lock = false;

    return (this.tick) ? setImmediate(async () => this.turn()) : this.turn();
  }

  private static asyncErr(err: any) {
    console.log('async queue err', err);

    setImmediate(() => { throw err; });
  }
}
