import { fork, ChildProcess } from 'child_process';
import { JobContext, JobHandle } from './worker';

export class Runner {
  cproc: ChildProcess;
  _abort = false;
  private readonly ctx: JobContext|string;
  private readonly sandbox: boolean;

  constructor(ctx: JobContext|string, sandbox: boolean) {
    this.ctx = ctx;
    this.sandbox = sandbox;
  }

  async run(jh: JobHandle) {
    if (this.sandbox) {
      if (typeof this.ctx === 'string') {
        // load path in sandbox
        return this.runInSandbox(jh, this.ctx, true);
      }
      else {
        // run stringified code in sandbox
        return this.runInSandbox(jh, this.ctx.toString(), false);
      }
    }
    else {
      if (typeof this.ctx === 'string') {
        // load path in current context
        const ctx = require(this.ctx).ctx as JobContext;

        return ctx(jh);
      }
      else {
        // run code in current context
        return this.ctx(jh);
      }
    }
  }

  abort() {
    this._abort = true;

    this.cproc.kill();
  }

  private async runInSandbox(jh: JobHandle, ctx: string, isPath: boolean) {
    return new Promise<any>((resolve, reject) => {
      const cproc = fork(`${__dirname}/sandbox`);
      let resolved = false;

      this.cproc = cproc;

      cproc.on('message', (msg) => {
        if (msg.progress) {
          jh.progress(msg.progress); // pipe to worker
        }
        else if (msg.error) {
          return reject(msg.error);
        }
        else if (msg.result) {
          return resolve(msg.result);
        }
      });

      cproc.on('disconnect', () => {
        console.log('child disconnect');
      });

      cproc.on('exit', () => {
        console.log('child exit');

        // if (!resolved) {
        //   resolved = true;
        //   return resolve('done ok');
        // }
      });

      cproc.on('error', (err) => {
        console.log('child error', err);

        if (!resolved) {
          resolved = true;
          return reject(err);
        }
      });

      cproc.on('close', () => {
        console.log('child close');

        return this._abort ? reject('aborted') : resolve('x');
      });

      cproc.send({ jh, ctx, isPath });
    });
  }
}
