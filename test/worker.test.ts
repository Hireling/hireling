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
    if (worker.report.up) {
      worker.stop(true);

      await swait(worker.down);
    }
  }

  @AsyncTest()
  async startErr() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    Expect(worker.report.up).toBe(false);
  }

  @AsyncTest()
  async stop() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    worker.stop();

    Expect(worker.report.up).toBe(false);
  }

  @AsyncTest()
  async resumeErr() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    worker.start();

    await swait(worker.down);

    worker.stop();

    Expect(worker.report.up).toBe(false);
  }

  @AsyncTest()
  async forceClose() {
    worker = new Worker(workerCfg).start();

    await swait(worker.down);

    worker.stop(true);

    Expect(worker.report.up).toBe(false);
  }
}
