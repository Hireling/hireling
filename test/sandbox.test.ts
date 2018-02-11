import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker } from '../src/broker';
import { Worker } from '../src/worker';
import { swait } from '../src/util';
import { brokerCfg, workerCfg } from './fixture/cfg';

let broker: Broker;
let worker: Worker;

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

    worker = new Worker(workerCfg).start();

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
    const job = await broker.createJob({
      ctx:     `${__dirname}/fixture/ctx-ok`,
      sandbox: true
    });

    await swait(job.progress);

    const { result } = await swait(job.done);

    Expect(result).toBe(555);
  }

  @AsyncTest()
  async jobSandboxPathFail() {
    const job = await broker.createJob({
      ctx:     `${__dirname}/fixture/ctx-err`,
      sandbox: true
    });

    await swait(job.progress);

    const result = await swait(job.fail);

    Expect(result.msg).toBe('an error');
  }

  @AsyncTest()
  async jobSandboxPathExternalFn() {
    const job = await broker.createJob({
      ctx:     `${__dirname}/fixture/ctx-fn`,
      sandbox: true
    });

    await swait(job.progress);

    const { result } = await swait(job.done);

    Expect(result).toBe(555);
  }

  @AsyncTest()
  async jobSandboxPathInvalid() {
    const job = await broker.createJob({
      ctx:     `${__dirname}/fixture/ctx-undefined`,
      sandbox: true
    });

    const result = await swait(job.fail);

    Expect(/^Cannot find module/.test(result.msg)).toBe(true);
  }

  @AsyncTest()
  async jobSandboxScriptOk() {
    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(55);

        return 23;
      },
      sandbox: true
    });

    await swait(job.progress);

    const { result } = await swait(job.done);

    Expect(result).toBe(23);
  }

  @AsyncTest()
  async jobSandboxScriptFail() {
    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(55);

        throw new Error('an error');
      },
      sandbox: true
    });

    await swait(job.progress);

    const result = await swait(job.fail);

    Expect(result.msg).toBe('an error');
  }

  @AsyncTest()
  async jobSandboxScriptExit() {
    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(55);

        process.exit(1);
      },
      sandbox: true
    });

    const result = await swait(job.fail);

    Expect(result.reason).toBe('exited');
  }

  @AsyncTest()
  async jobSandboxScriptAbort() {
    const job = await broker.createJob({
      ctx: async (jh) => {
        jh.progress(55);
        await new Promise(resolve => setTimeout(resolve, 100));
      },
      sandbox: true
    });

    await swait(job.progress);

    worker.abort();

    const result = await swait(job.fail);

    Expect(result.reason).toBe('aborted');
  }
}
