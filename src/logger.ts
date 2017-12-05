import * as util from 'util';

export const enum LogLevel {
  none  = 'none',
  debug = 'debug',
  info  = 'info',
  warn  = 'warn',
  error = 'error'
}

const logLevelVal = {
  [LogLevel.none]:  0,
  [LogLevel.debug]: 1,
  [LogLevel.info]:  2,
  [LogLevel.warn]:  3,
  [LogLevel.error]: 4
};

export class Logger {
  private prefix: string;
  private stream = process.stdout;
  level: LogLevel = LogLevel.warn;

  constructor(prefix: string, test = false) {
    this.prefix = `[${Logger.color(prefix, 'bold')}]`;

    if (test) {
      (this.stream as any) = {
        write() {} // mock writable stream
      };
    }
  }

  private doPrint(printLevel: LogLevel) {
    return (
      this.level != LogLevel.none &&
      logLevelVal[this.level] <= logLevelVal[printLevel]
    );
  }

  debug(...args: any[]) {
    if (this.doPrint(LogLevel.debug)) {
      this.printargs(args, 'cyan');
    }
  }

  info(...args: any[]) {
    if (this.doPrint(LogLevel.info)) {
      this.printargs(args, 'white');
    }
  }

  warn(...args: any[]) {
    if (this.doPrint(LogLevel.warn)) {
      this.printargs(args, 'yellow');
    }
  }

  error(...args: any[]) {
    if (this.doPrint(LogLevel.error)) {
      this.printargs(args, 'red');
    }
  }

  private static color(txt: string, reqColor: string) {
    const WHITE = [37, 39];
    const color = util.inspect.colors[reqColor] || WHITE;

    return `\u001b[${color[0]}m${txt}\u001b[${color[1]}m`;
  }

  private printargs(args: any[], color: string) {
    this.stream.write(`${this.prefix} `);

    args.forEach(a => {
      this.stream.write(typeof a === 'string' ?
        `${Logger.color(a, color)} ` :
        `${util.inspect(a, { colors: true })} `
      );
    });

    this.stream.write('\n');
  }
}
