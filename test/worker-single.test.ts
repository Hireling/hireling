import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect, SpyOn, Timeout
} from 'alsatian';
import { Broker } from '../src/broker';
import { Worker, JobContext } from '../src/worker';
import { fnwait, swait } from '../src/util';
import { brokerCfg, workerCfg } from './fixture/cfg';

let broker: Broker;
let worker: Worker;

const echoWork: JobContext = async jh => jh.job.data;

@TestFixture()
export class BrokerTest {
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
  async connectWorker() {
    Expect(broker.report.workers).toBe(1);
  }

  @AsyncTest()
  async stopWithWorker() {
    broker.stop();

    await swait(broker.down);

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

    await swait(worker.down);
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

    await swait(worker.down);

    const ms = await worker.pingBroker();

    await fnwait(20);

    Expect(ms).toBe('unreachable');
  }

  @AsyncTest()
  async brokerPingClosed() {
    worker.stop();

    await swait(broker.workerpart);

    const pings = await broker.pingWorkers();

    await fnwait(20);

    Expect(pings.length).toBe(0);
  }

  @AsyncTest()
  async stopResumeWithWorker() {
    broker.stop();

    await swait(broker.down);

    broker.start();

    await swait(broker.drain);

    Expect(broker.report.workers).toBe(1);
  }

  @AsyncTest()
  async workerSimpleJob1() {
    const job = await broker.createJob({ data: { a: 'b' } });

    const result = await swait(job.done);

    Expect(result).toEqual({ a: 'b' });
  }

  @AsyncTest()
  async workerSimpleJob2() {
    const job = await broker.createJob({ data: { a: 'b' } });

    const result = await swait(job.done);

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

    await swait(worker.jobprogress);

    broker.stop();

    await swait(broker.down);

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

    await swait(worker.jobprogress);

    broker.stop(true);

    await swait(broker.down);

    Expect(worker.report.working).toBe(true);

    await swait(worker.jobfinish);
  }

  @Timeout(1000)
  @AsyncTest()
  async stopTimeoutWithBusyWorker() {
    worker.setContext(async (job) => {
      job.progress(50);
      await fnwait(600); // timeout spinlock
    });

    broker.stop();

    await swait(broker.down);

    broker = new Broker({ ...brokerCfg, closems: 1 });

    broker.start();

    await swait(broker.drain);

    await broker.createJob();

    await swait(worker.jobprogress);

    broker.stop();

    const result = await swait(broker.down);

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

    j1.progress.on(spy.onProgress);

    const j2 = await broker.createJob();

    j2.progress.on(spy.onProgress);

    await Promise.all([
      swait(j1.done),
      swait(j2.done)
    ]);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(2);
  }

  @AsyncTest()
  async jobAfterConnect() {
    worker.stop();

    await swait(worker.down);

    const job = await broker.createJob();

    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    job.progress.on(spy.onProgress);

    worker = new Worker(workerCfg, async (jh) => {
      jh.progress(10);
    }).start();

    await swait(job.done);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobProgress() {
    worker.setContext(async (job) => {
      job.progress(30);

      await fnwait(100);

      job.progress(100);
    });

    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    const job = await broker.createJob();

    job.progress.on(spy.onProgress);

    await swait(job.done);

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

    job.fail.on(spy.onFail);

    await swait(job.fail);

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

    await swait(job.progress);

    broker.stop(true);

    // allow worker to save message for replay
    await swait(worker.jobfinish);

    const spy = { onDone() {} };
    SpyOn(spy, 'onDone');

    job.done.on(spy.onDone);

    broker.start();

    await swait(worker.up);

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

    await swait(job.progress);

    broker.stop(true);

    await swait(broker.down);

    broker.start();

    const result = await swait(worker.jobfinish);

    Expect(result.resumed).toBe(true);
  }
}
