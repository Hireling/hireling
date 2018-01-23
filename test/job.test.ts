import {
  TestFixture, AsyncTeardownFixture, AsyncSetup, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker, BrokerEvent } from '../src/broker';
import { ewait } from '../src/util';
import { brokerCfg } from './fixture/cfg';

let broker: Broker;

@TestFixture()
export class JobTest {
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
  }

  @AsyncTeardown
  async teardown() {
    broker.stop();

    await ewait(broker, BrokerEvent.stop);
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

    await ewait(broker, BrokerEvent.stop);
  }

  @AsyncTest()
  async addJobErrorOnClosed() {
    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    await Expect(async () => broker.createJob()).toThrowAsync();
  }

  @AsyncTest()
  async addJobAfterResume() {
    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    await broker.createJob();

    Expect(broker.report.jobs).toBe(1);
  }
}
