import { fork, ChildProcess } from 'child_process';
import { JobContext, JobHandle } from './worker';
import { StringifiedArgs, CprocMsg } from './sandbox';

export class Runner {
  cproc: ChildProcess|null = null;
  aborted = false;
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

  abort(signal: string|undefined = undefined) {
    if (this.cproc) {
      this.aborted = true;

      this.cproc.kill(signal);
    }
    else {
      throw new Error('job not executing in sandbox');
    }
  }

  private async runInSandbox(jh: JobHandle, ctx: string, isPath: boolean) {
    return new Promise<any>((resolve, reject) => {
      let resolved = false;

      const cproc = fork(`${__dirname}/sandbox`);

      this.cproc = cproc;

      cproc.on('message', (msg) => {
        if (msg.code === CprocMsg.progress) {
          jh.progress(msg.progress); // pipe to worker
        }
        else if (msg.code === CprocMsg.error) {
          if (!resolved) {
            resolved = true;

            return reject(msg.error);
          }
        }
        else if (msg.code === CprocMsg.result) {
          if (!resolved) {
            resolved = true;

            return resolve(msg.result);
          }
        }
        else {
          if (!resolved) {
            resolved = true;

            return reject(`unknown msg ${msg.code}`);
          }
        }
      });

      cproc.on('exit', () => {
        if (!resolved) {
          resolved = true;

          return reject(this.aborted ? 'aborted' : 'exited');
        }
      });

      cproc.on('error', (err) => {
        if (!resolved) {
          resolved = true;

          return reject(err ? (err.message || String(err)) : 'sandbox error');
        }
      });

      const msg: StringifiedArgs = { jh, ctx, isPath };

      cproc.send(msg);
    });
  }
}
