import {
  TestFixture, AsyncTest, Expect, SpyOn
} from 'alsatian';
import { Logger, LogLevel } from '../src/logger';

// HACK: spy on private member for test coverage
const INTERNALS = 'printargs';

@TestFixture()
export class LoggerTest {
  @AsyncTest()
  async logInternals() {
    const logger = new Logger('Test', true);

    logger.level = LogLevel.debug;

    SpyOn(logger, INTERNALS);

    logger.debug('test', { a: 'b' });
    logger.info('test', { a: 'b' });
    logger.warn('test', { a: 'b' });
    logger.error('test', { a: 'b' });

    (Expect(logger[INTERNALS]) as any).toHaveBeenCalled().exactly(4);
  }

  @AsyncTest()
  async loggerLevels() {
    const logger = new Logger('Test', true);

    logger.level = LogLevel.warn;

    SpyOn(logger, INTERNALS);

    logger.debug('test', { a: 'b' });
    logger.info('test', { a: 'b' });
    logger.warn('test', { a: 'b' });
    logger.error('test', { a: 'b' });

    (Expect(logger[INTERNALS]) as any).toHaveBeenCalled().exactly(2);
  }
}
