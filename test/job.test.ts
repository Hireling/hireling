import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker } from '../src/broker';
import { swait } from '../src/util';
import { brokerCfg } from './fixture/cfg';

let broker: Broker;

@TestFixture()
export class JobTest {
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
  }

  @AsyncTeardown
  async teardown() {
    broker.stop();

    await swait(broker.down);
  }

  @AsyncTest()
  async addJobs() {
    await broker.createJob();
    await broker.createJob();
    await broker.createJob();
    await broker.createJob();

    Expect(broker.report.jobs).toBe(4);
  }

  @AsyncTest()
  async addJobErrorOnClosing() {
    // event not awaited to catch intermediate state
    broker.stop();

    await Expect(async () => broker.createJob()).toThrowAsync();

    await swait(broker.down);
  }

  @AsyncTest()
  async addJobErrorOnClosed() {
    broker.stop();

    await swait(broker.down);

    await Expect(async () => broker.createJob()).toThrowAsync();
  }

  @AsyncTest()
  async addJobAfterResume() {
    broker.stop();

    await swait(broker.down);

    broker.start();

    await swait(broker.up);

    await broker.createJob();

    Expect(broker.report.jobs).toBe(1);
  }
}
