import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker } from '../src/broker';
import { Worker, JobContext } from '../src/worker';
import { swait } from '../src/util';
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
  async jobSandboxPathOk() {
    worker.setContext(`${__dirname}/fixture/ctx-ok`);

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    const result = await swait(job.done);

    Expect(result).toBe(555);
  }

  @AsyncTest()
  async jobSandboxPathFail() {
    worker.setContext(`${__dirname}/fixture/ctx-err`);

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    const result = await swait(job.fail);

    Expect(result).toBe('an error');
  }

  @AsyncTest()
  async jobSandboxPathExternalFn() {
    worker.setContext(`${__dirname}/fixture/ctx-fn`);

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    const result = await swait(job.done);

    Expect(result).toBe(555);
  }

  @AsyncTest()
  async jobSandboxPathInvalid() {
    worker.setContext(`${__dirname}/fixture/ctx-undefined`);

    const job = await broker.createJob({ sandbox: true });

    const result = await swait(job.fail);

    Expect(/^Cannot find module/.test(result)).toBe(true);
  }

  @AsyncTest()
  async jobSandboxScriptOk() {
    worker.setContext(async (jh) => {
      jh.progress(55);

      return 23;
    });

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    const result = await swait(job.done);

    Expect(result).toBe(23);
  }

  @AsyncTest()
  async jobSandboxScriptFail() {
    worker.setContext(async (jh) => {
      jh.progress(55);

      throw new Error('an error');
    });

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    const result = await swait(job.fail);

    Expect(result).toBe('an error');
  }

  @AsyncTest()
  async jobSandboxScriptExit() {
    worker.setContext(async (jh) => {
      jh.progress(55);

      process.exit();
    });

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    const result = await swait(job.fail);

    Expect(result).toBe('exited');
  }

  @AsyncTest()
  async jobSandboxScriptAbort() {
    worker.setContext(async (jh) => {
      jh.progress(55);

      // NOTE: no fnwait in sandbox with callback ctx
      await new Promise(resolve => setTimeout(resolve, 500)); // delay for abort
    });

    const job = await broker.createJob({ sandbox: true });

    await swait(job.progress);

    worker.abort();

    const result = await swait(job.fail);

    Expect(result).toBe('aborted');
  }
}
