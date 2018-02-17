import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect, SpyOn, Timeout
} from 'alsatian';
import { Broker } from '../src/broker';
import { Worker } from '../src/worker';
import { fnwait, swait } from '../src/util';
import { brokerCfg, workerCfg } from './fixture/cfg';
import { CtxFn } from '../src/ctx';

let broker: Broker;
let worker: Worker;

@TestFixture()
export class BrokerTest {
  @AsyncTeardownFixture
  async teardownFixture() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    await broker.clearJobs();

    broker.stop();

    await swait(broker.down);
  }

  @AsyncSetup
  async setup() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    await broker.clearJobs();

    worker = new Worker(workerCfg).start();

    await swait(broker.drain);
  }

  @AsyncTeardown
  async teardown() {
    if (worker.report.up) {
      worker.stop(true);

      await swait(worker.down);
    }

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
    worker.start();

    await swait(broker.drain);

    Expect(broker.report.workers).toBe(1);
  }

  @AsyncTest()
  async workerSimpleJob1() {
    const job = await broker.createJob({
      data: { a: 'b' },
      ctx:  async jh => jh.job.data
    });

    const { result } = await swait(job.done);

    Expect(result).toEqual({ a: 'b' });
  }

  @AsyncTest()
  async workerSimpleJob2() {
    const job = await broker.createJob({
      data: { a: 'b' },
      ctx:  async jh => jh.job.data
    });

    const { result } = await swait(job.done);

    Expect(result).not.toEqual({ a: 'c' });
  }

  @Timeout(1000)
  @AsyncTest()
  async stopWithBusyWorker() {
    await broker.createJob({
      ctx: async (jh) => {
        jh.progress(50);
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    });

    await swait(worker.jobprogress);

    broker.stop();

    await swait(broker.down);

    Expect(worker.report.working).toBe(false);
  }

  @Timeout(1000)
  @AsyncTest()
  async forceStopWithBusyWorker() {
    await broker.createJob({
      ctx: async (jh) => {
        jh.progress(50);
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    });

    await swait(worker.jobprogress);

    broker.stop(true);

    await swait(broker.down);

    Expect(worker.report.working).toBe(true);
  }

  @Timeout(1000)
  @AsyncTest()
  async stopTimeoutWithBusyWorker() {
    broker.stop();

    await swait(broker.down);

    broker = new Broker({ ...brokerCfg, closems: 1 }).start();

    worker.start();

    await swait(broker.drain);

    await broker.createJob({
      ctx: async (jh) => {
        jh.progress(50);
        await new Promise(resolve => setTimeout(resolve, 600)); // cause timeout
      }
    });

    await swait(worker.jobprogress);

    broker.stop();

    const result = await swait(broker.down);

    Expect(result instanceof Error).toBe(true);
  }

  @AsyncTest()
  async backToBackJobs() {
    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    const ctx: CtxFn = async (jh) => {
      jh.progress(10);
    };

    const j1 = await broker.createJob({ ctx });

    j1.progress.on(spy.onProgress);

    const j2 = await broker.createJob({ ctx });

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

    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(10);
      }
    });

    job.progress.on(spy.onProgress);

    worker = new Worker(workerCfg).start();

    await swait(job.done);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobProgress() {
    const spy = { onProgress() {} };
    SpyOn(spy, 'onProgress');

    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(30);
        await new Promise(resolve => setTimeout(resolve, 100));
        jh.progress(100);
      }
    });

    job.progress.on(spy.onProgress);

    await swait(job.done);

    Expect(spy.onProgress).toHaveBeenCalled().exactly(2);
  }

  @AsyncTest()
  async jobError() {
    const spy = { onFail() {} };
    SpyOn(spy, 'onFail');

    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(30);

        throw new Error('an error');
      }
    });

    job.fail.on(spy.onFail);

    const result = await swait(job.fail);

    Expect(result.msg).toBe('an error');
    Expect(spy.onFail).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobReplay() {
    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(33);
        await new Promise(resolve => setTimeout(resolve, 50));
        jh.progress(66);
        await new Promise(resolve => setTimeout(resolve, 50));
        jh.progress(100);
      }
    });

    await swait(job.progress);

    broker.stop(true);

    // allow worker to save message for replay
    await swait(worker.jobfinish);

    const spy = { onDone() {} };
    SpyOn(spy, 'onDone');

    job.done.on(spy.onDone);

    broker.start();
    worker.start();

    await swait(worker.up);

    Expect(spy.onDone).toHaveBeenCalled().exactly(1);
  }

  @Timeout(1000)
  @AsyncTest()
  async jobResume() {
    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(33);
        await new Promise(resolve => setTimeout(resolve, 100));
        jh.progress(66);
        await new Promise(resolve => setTimeout(resolve, 100));
        jh.progress(100);
      }
    });

    await swait(job.progress);

    broker.stop(true);

    await swait(broker.down);

    broker.start();

    const result = await swait(worker.jobfinish);

    Expect(result.resumed).toBe(true);
  }
}
