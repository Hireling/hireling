import { BrokerOpt } from '../../src/broker';
import { WorkerOpt } from '../../src/worker';
import { LogLevel } from '../../src/logger';
import { TopPartial } from '../../src/util';

const TEST_HOST = '127.0.0.1';
const TEST_PORT = 3005;

export const brokerCfg: TopPartial<BrokerOpt> = {
  log:    LogLevel.none,
  waitms: 250,
  gc:     null,
  server: {
    log:  LogLevel.none,
    host: TEST_HOST,
    port: TEST_PORT
  },
  db: {
    log:    LogLevel.none,
    retryx: 0
  }
};

export const workerCfg: TopPartial<WorkerOpt> = {
  log:     LogLevel.none,
  name:    'test-hireling',
  waitms:  50,
  client: {
    log:     LogLevel.none,
    host:    TEST_HOST,
    port:    TEST_PORT,
    retryx:  0
  }
};

process.on('unhandledRejection', (err) => {
  console.log('TEST unhandledRejection', err);
});

process.on('uncaughtException', (err) => {
  console.log('TEST uncaughtException', err);

  process.exit(1);
});
