import {
  TestFixture, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Worker, WorkerEvent, JobContext } from '../src/worker';
import { ewait } from '../src/util';
import { workerCfg } from './cfg.test';

let worker: Worker;

const echoWork: JobContext = async (job) => job.data;

@TestFixture()
export class WorkerTest {
  @AsyncTeardown
  async teardown() {
    worker.stop();

    await ewait(worker, WorkerEvent.stop);
  }

  @AsyncTest()
  async startErr() {
    worker = new Worker(workerCfg, echoWork).start();

    await ewait(worker, WorkerEvent.stop);

    Expect(worker.report.alive).toBe(false);
  }

  @AsyncTest()
  async stop() {
    worker = new Worker(workerCfg, echoWork).start();

    await ewait(worker, WorkerEvent.stop);

    worker.stop();

    await ewait(worker, WorkerEvent.stop);

    Expect(worker.report.alive).toBe(false);
  }

  @AsyncTest()
  async resumeErr() {
    worker = new Worker(workerCfg, echoWork).start();

    await ewait(worker, WorkerEvent.stop);

    worker.start();

    await ewait(worker, WorkerEvent.stop);

    worker.stop();

    await ewait(worker, WorkerEvent.stop);

    Expect(worker.report.alive).toBe(false);
  }

  @AsyncTest()
  async forceClose() {
    worker = new Worker(workerCfg, echoWork).start();

    await ewait(worker, WorkerEvent.stop);

    worker.stop(true);

    await ewait(worker, WorkerEvent.stop);

    Expect(worker.report.alive).toBe(false);
  }
}
