import { BrokerOpt } from '../src/broker';
import { WorkerOpt } from '../src/worker';
import { LogLevel } from '../src/logger';
import { TopPartial } from '../src/util';

const TEST_HOST = '127.0.0.1';
const TEST_PORT = 3005;

export const brokerCfg: TopPartial<BrokerOpt> = {
  log: LogLevel.none,
  waitms: 250,
  gc:  null,
  server: {
    log:  LogLevel.none,
    host: TEST_HOST,
    port: TEST_PORT
  },
  db: {
    log: LogLevel.none
  }
};

export const workerCfg: TopPartial<WorkerOpt> = {
  host:    TEST_HOST,
  port:    TEST_PORT,
  name:    'test-hireling',
  log:     LogLevel.none,
  retryms: 50, // avoid timing out tests
  waitms:  250,
  retryx:  0
};

process.on('unhandledRejection', (err) => {
  console.log('TEST unhandledRejection', err);
});

process.on('uncaughtException', (err) => {
  console.log('TEST uncaughtException', err);

  process.exit(1);
});
