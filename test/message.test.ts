import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker, BrokerEvent } from '../src/broker';
import { Worker, WorkerEvent, JobContext } from '../src/worker';
import { JobEvent } from '../src/job';
import { ewait } from '../src/util';
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

    await ewait(broker, BrokerEvent.start);

    await broker.clearJobs();

    broker.stop();

    await ewait(broker, BrokerEvent.stop);
  }

  @AsyncSetup
  async setup() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    await broker.clearJobs();

    worker = new Worker(workerCfg, echoWork).start();

    await ewait(broker, BrokerEvent.drain);
  }

  @AsyncTeardown
  async teardown() {
    worker.stop();

    await ewait(worker, WorkerEvent.stop);

    broker.stop();

    await ewait(broker, BrokerEvent.stop);
  }

  @AsyncTest()
  async differentOutput() {
    const job = await broker.createJob({ data: { a: 'b' } });

    const [result] = await ewait(job, JobEvent.done);

    Expect(result).not.toEqual({ a: 'c' });
  }

  @AsyncTest()
  async complexWorkData() {
    const job = await broker.createJob({ data: complexDataIn });

    const [result] = await ewait(job, JobEvent.done);

    Expect(result).toEqual(complexDataOut);
  }

  @AsyncTest()
  async nanPassthrough() {
    const job = await broker.createJob({ data: nanData });

    const [result] = await ewait(job, JobEvent.done);

    Expect(Number.isNaN(result.a)).toBe(true);
  }

  @AsyncTest()
  async dateKeepsMillis() {
    const dateToStr = '2000-01-01T01:01:01.111Z';

    const job = await broker.createJob({
      data: { a: new Date(dateToStr) }
    });

    const [result] = await ewait(job, JobEvent.done);

    Expect((result.a as Date).toISOString()).toBe(dateToStr);
  }
}
