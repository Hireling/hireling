import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker } from '../src/broker';
import { Worker, JobContext } from '../src/worker';
import { swait } from '../src/util';
import { brokerCfg, workerCfg } from './fixture/cfg';

let broker: Broker;
let worker: Worker;

const complexDataIn = {
  a: 'b',
  b: new Date(),
  c: {
    aa: `a'\`"`,
    ' "b\ ': [1, null, true, false]
  },
  d: [1, Infinity, -Infinity, undefined],
  e: '',
  f: () => true,
  g: Symbol('g')
};

const complexDataOut = {
  a: 'b',
  b: new Date(),
  c: {
    aa: `a'\`"`,
    ' "b\ ': [1, null, true, false]
  },
  d: [1, Infinity, -Infinity, undefined],
  e: '',
  f: null, // stripped
  g: null  // stripped
};

const nanData = {
  a: NaN
};

const echoWork: JobContext = async jh => jh.job.data;

@TestFixture()
export class MessageTest {
  @AsyncTeardownFixture
  async teardownFixture() {
    broker = new Broker(brokerCfg);

    broker.start();

    await swait(broker.up);

    await broker.clearJobs();

    broker.stop();

    await swait(broker.down);
  }

  @AsyncSetup
  async setup() {
    broker = new Broker(brokerCfg);

    broker.start();

    await swait(broker.up);

    await broker.clearJobs();

    worker = new Worker(workerCfg, echoWork).start();

    await swait(broker.drain);
  }

  @AsyncTeardown
  async teardown() {
    worker.stop();

    await swait(worker.down);

    broker.stop();

    await swait(broker.down);
  }

  @AsyncTest()
  async differentOutput() {
    const job = await broker.createJob({ data: { a: 'b' } });

    const result = await swait(job.done);

    Expect(result).not.toEqual({ a: 'c' });
  }

  @AsyncTest()
  async complexWorkData() {
    const job = await broker.createJob({ data: complexDataIn });

    const result = await swait(job.done);

    Expect(result).toEqual(complexDataOut);
  }

  @AsyncTest()
  async nanPassthrough() {
    const job = await broker.createJob({ data: nanData });

    const result = await swait(job.done);

    Expect(Number.isNaN(result.a)).toBe(true);
  }

  @AsyncTest()
  async dateKeepsMillis() {
    const dateToStr = '2000-01-01T01:01:01.111Z';

    const job = await broker.createJob({
      data: { a: new Date(dateToStr) }
    });

    const result = await swait(job.done);

    Expect((result.a as Date).toISOString()).toBe(dateToStr);
  }
}
