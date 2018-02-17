import {
  TestFixture, AsyncTeardown, AsyncTest, Expect
} from 'alsatian';
import { Broker } from '../src/broker';
import { swait } from '../src/util';
import { brokerCfg } from './fixture/cfg';

let broker: Broker;

@TestFixture()
export class BrokerTest {
  @AsyncTeardown
  async teardown() {
    broker.stop();

    await swait(broker.down);
  }

  @AsyncTest()
  async start() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    Expect(broker.report.alive).toBe(true);
  }

  @AsyncTest()
  async close() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    broker.stop();

    await swait(broker.down);

    Expect(broker.report.alive).toBe(false);
  }

  @AsyncTest()
  async restart() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    broker.stop();

    await swait(broker.down);

    broker.start();

    await swait(broker.up);

    Expect(broker.report.alive).toBe(true);
  }

  @AsyncTest()
  async forceClose() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    broker.stop(true);

    await swait(broker.down);

    Expect(broker.report.alive).toBe(false);
  }

  @AsyncTest()
  async startWhileRunning() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    Expect(() => broker.start()).not.toThrow();
  }

  @AsyncTest()
  async stopWhileStopping() {
    broker = new Broker(brokerCfg).start();

    await swait(broker.up);

    // event not awaited to catch intermediate state
    broker.stop();

    Expect(() => broker.stop()).not.toThrow();

    await swait(broker.down);
  }

  @AsyncTest()
  async stopWhileStopped() {
    broker = new Broker(brokerCfg);

    Expect(() => broker.stop()).not.toThrow();
  }
}
