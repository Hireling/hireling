import {
  TestFixture, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker, BrokerEvent } from '../src/broker';
import { ewait } from '../src/util';
import { brokerCfg } from './cfg.test';

let broker: Broker;

@TestFixture()
export class BrokerTest {
  @AsyncTeardown
  async teardown() {
    broker.stop();

    await ewait(broker, BrokerEvent.stop);
  }

  @AsyncTest()
  async start() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    Expect(broker.report.alive).toBe(true);
  }

  @AsyncTest()
  async close() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    Expect(broker.report.alive).toBe(false);
  }

  @AsyncTest()
  async restart() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    broker.stop();

    await ewait(broker, BrokerEvent.stop);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    Expect(broker.report.alive).toBe(true);
  }

  @AsyncTest()
  async forceClose() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    broker.stop(true);

    await ewait(broker, BrokerEvent.stop);

    Expect(broker.report.alive).toBe(false);
  }

  @AsyncTest()
  async startWhileRunning() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    Expect(() => broker.start()).not.toThrow();
  }

  @AsyncTest()
  async stopWhileStopping() {
    broker = new Broker(brokerCfg);

    broker.start();

    await ewait(broker, BrokerEvent.start);

    // event not awaited to catch intermediate state
    broker.stop();

    Expect(() => broker.stop()).not.toThrow();

    await ewait(broker, BrokerEvent.stop);
  }

  @AsyncTest()
  async stopWhileStopped() {
    broker = new Broker(brokerCfg);

    Expect(() => broker.stop()).not.toThrow();
  }
}
