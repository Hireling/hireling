import {
  TestFixture, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Worker } from '../src/worker';
import { swait } from '../src/util';
import { workerCfg } from './fixture/cfg';

let worker: Worker;

@TestFixture()
export class WorkerTest {
  @AsyncTeardown
  async teardown() {
    worker.stop();

    await swait(worker.down);
  }

  @AsyncTest()
  async startErr() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    Expect(worker.report.alive).toBe(false);
  }

  @AsyncTest()
  async stop() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    worker.stop();

    await swait(worker.down);

    Expect(worker.report.alive).toBe(false);
  }

  @AsyncTest()
  async resumeErr() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    worker.start();

    await swait(worker.down);

    worker.stop();

    await swait(worker.down);

    Expect(worker.report.alive).toBe(false);
  }

  @AsyncTest()
  async forceClose() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    worker.stop(true);

    await swait(worker.down);

    Expect(worker.report.alive).toBe(false);
  }
}
