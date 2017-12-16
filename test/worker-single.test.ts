import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect, SpyOn, Timeout
} from 'alsatian';
import { Broker, BrokerEvent } from '../src/broker';
import { Worker, WorkerEvent, JobContext } from '../src/worker';
import { JobEvent } from '../src/job';
import { ewait, fnwait } from '../src/util';
import { brokerCfg, workerCfg } from './cfg.test';

let broker: Broker;
let worker: Worker;

const echoWork: JobContext = async jh => jh.job.data;

@TestFixture()
export class BrokerTest {
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
  async connectWorker() {
    Expect(broker.report.workers).toBe(1);
  }

  @AsyncTest()
  async stopWithWorker() {
    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    Expect(broker.report.workers).toBe(0);
  }

  @AsyncTest()
  async resumeWhileRunning() {
    Expect(() => worker.start()).not.toThrow();
  }

  @AsyncTest()
  async stopWhileStopping() {
    // event not awaited to catch intermediate state
    worker.stop();

    Expect(() => worker.stop()).not.toThrow();

    await ewait(worker, WorkerEvent.stop);
  }

  @AsyncTest()
  async pingOk() {
    const ms = await worker.pingBroker();

    await fnwait(20);

    Expect(ms).not.toBe('unreachable');
  }

  @AsyncTest()
  async brokerPingOk() {
    const pings = await broker.pingWorkers();

    await fnwait(20);

    Expect(pings.length).toBe(1);
  }

  @AsyncTest()
  async pingUnreachable() {
    broker.stop();

    await ewait(worker, WorkerEvent.stop);

    const ms = await worker.pingBroker();

    await fnwait(20);

    Expect(ms).toBe('unreachable');
  }

  @AsyncTest()
  async brokerPingClosed() {
    worker.stop();

    await ewait(broker, BrokerEvent.workerpart);

    const pings = await broker.pingWorkers();

    await fnwait(20);

    Expect(pings.length).toBe(0);
  }

  @AsyncTest()
  async stopResumeWithWorker() {
    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    broker.start();

    await ewait(broker, BrokerEvent.drain);

    Expect(broker.report.workers).toBe(1);
  }

  @AsyncTest()
  async workerSimpleJob1() {
    const job = await broker.createJob({ data: { a: 'b' } });

    const [result] = await ewait(job, JobEvent.done);

    Expect(result).toEqual({ a: 'b' });
  }

  @AsyncTest()
  async workerSimpleJob2() {
    const job = await broker.createJob({ data: { a: 'b' } });

    const [result] = await ewait(job, JobEvent.done);

    Expect(result).not.toEqual({ a: 'c' });
  }

  @Timeout(1000)
  @AsyncTest()
  async stopWithBusyWorker() {
    worker.setContext(async (job) => {
      job.progress(50);
      await fnwait(200);
    });

    await broker.createJob();

    await ewait(worker, WorkerEvent.jobstart);

    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    Expect(worker.report.working).toBe(false);
  }

  @Timeout(1000)
  @AsyncTest()
  async forceStopWithBusyWorker() {
    worker.setContext(async (job) => {
      job.progress(50);
      await fnwait(100);
    });

    await broker.createJob();

    await ewait(worker, WorkerEvent.jobstart);

    broker.stop(true);

    await ewait(broker, BrokerEvent.stop);

    Expect(worker.report.working).toBe(true);

    await ewait(worker, WorkerEvent.jobfinish);
  }

  @Timeout(1000)
  @AsyncTest()
  async stopTimeoutWithBusyWorker() {
    worker.setContext(async (job) => {
      job.progress(50);
      await fnwait(600); // timeout spinlock
    });

    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    broker = new Broker({ ...brokerCfg, closems: 1 });

    broker.start();

    await ewait(broker, BrokerEvent.drain);

    await broker.createJob();

    await ewait(worker, WorkerEvent.jobprogress);

    broker.stop();

    const [result] = await ewait(broker, BrokerEvent.stop);

    Expect(result instanceof Error).toBe(true);
  }

  @AsyncTest()
  async backToBackJobs() {
    worker.setContext(async (job) => {
      job.progress(10);
    });

    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    const j1 = await broker.createJob();

    j1.on(JobEvent.progress, spy.onProgress);

    const j2 = await broker.createJob();

    j2.on(JobEvent.progress, spy.onProgress);

    await Promise.all([
      ewait(j1, JobEvent.done),
      ewait(j2, JobEvent.done)
    ]);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(2);
  }

  @AsyncTest()
  async jobAfterConnect() {
    worker.stop();

    await ewait(worker, WorkerEvent.stop);

    const job = await broker.createJob();

    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    job.on(JobEvent.progress, spy.onProgress);

    worker = new Worker(workerCfg, async (jh) => {
      jh.progress(10);
    }).start();

    await ewait(job, JobEvent.done);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobProgress() {
    worker.setContext(async (job) => {
      job.progress(30);

      await new Promise((resolve) => setTimeout(resolve, 100));

      job.progress(100);
    });

    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    const job = await broker.createJob();

    job.on(JobEvent.progress, spy.onProgress);

    await ewait(job, JobEvent.done);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(2);
  }

  @AsyncTest()
  async jobError() {
    worker.setContext(async (job) => {
      job.progress(30);

      throw new Error();
    });

    const spy = { onFail() {} };
    SpyOn(spy, 'onFail');

    const job = await broker.createJob();

    job.on(JobEvent.fail, spy.onFail);

    await ewait(job, JobEvent.fail);

    Expect(spy.onFail).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobReplay() {
    worker.setContext(async (job) => {
      job.progress(33);
      await fnwait(50);
      job.progress(66);
      await fnwait(50);
      job.progress(100);
    });

    const job = await broker.createJob();

    await ewait(job, JobEvent.progress);

    broker.stop(true);

    // allow worker to save message for replay
    await ewait(worker, WorkerEvent.jobfinish);

    const spy = { onDone() {} };
    SpyOn(spy, 'onDone');

    job.on(JobEvent.done, spy.onDone);

    broker.start();

    await ewait(worker, WorkerEvent.start);

    Expect(spy.onDone).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobResume() {
    worker.setContext(async (job) => {
      job.progress(33);
      await fnwait(100);
      job.progress(66);
      await fnwait(100);
      job.progress(100);
    });

    const job = await broker.createJob();

    await ewait(job, JobEvent.progress);

    broker.stop(true);

    await ewait(broker, BrokerEvent.stop);

    broker.start();

    const [result] = await ewait(worker, WorkerEvent.jobfinish);

    Expect(result.resumed).toBe(true);
  }
}
