import { fork, ChildProcess } from 'child_process';
import { JobHandle } from './worker';
import { ProcMsg } from './sandbox';
import { EFail } from './job';
import { Ctx } from './ctx';

export const errStr = (err: any, def = '') =>
  err ? (err.message as string || String(err)) : def;

export class Runner<I = any, O = any> {
  private cproc: ChildProcess|null = null;
  private aborted = false;

  async run(jh: JobHandle<I>) {
    const { job } = jh;

    if (!job.ctx || !job.ctxkind) {
      const newErr: EFail = {
        reason: 'error',
        msg:    'no run context'
      };

      throw newErr;
    }
    else if (job.sandbox) {
      return this.runInSandbox(jh);
    }
    else {
      return this.runInProcess(jh);
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

  private async runInSandbox(jh: JobHandle<I>) {
    return new Promise<O>((resolve, reject) => {
      let resolved = false;

      const cproc = fork(`${__dirname}/sandbox`);

      this.cproc = cproc;

      cproc.on('message', (msg: ProcMsg) => {
        // tslint:disable-next-line:switch-default
        switch (msg.code) {
          case 'progress':
            jh.progress(msg.progress); // pipe to worker
            break;

          case 'done':
            if (!resolved) {
              resolved = true;

              return resolve(msg.result);
            }
            break;

          case 'failed':
            if (!resolved) {
              resolved = true;

              const rejection: EFail = {
                reason: 'error',
                msg:    msg.msg
              };

              return reject(rejection);
            }
            break;
        }
      });

      cproc.on('exit', (code, signal) => {
        if (!resolved) {
          resolved = true;

          const rejection: EFail = {
            reason: this.aborted ? 'aborted' : 'exited',
            msg:    errStr(code || signal, 'aborted/exited')
          };

          return reject(rejection);
        }
      });

      cproc.on('error', (err) => {
        if (!resolved) {
          resolved = true;

          const rejection: EFail = {
            reason: 'error',
            msg:    errStr(err, 'cproc error')
          };

          return reject(rejection);
        }
      });

      cproc.send(jh.job);
    });
  }

  private async runInProcess(jh: JobHandle<I>) {
    try {
      const ctxFn = Ctx.toFn<I, O>(jh.job.ctx, jh.job.ctxkind);

      // capture errors here
      const result = await ctxFn(jh);

      return result;
    }
    catch (err) {
      // wrap as job error, hide stack
      const newErr: EFail = {
        reason: 'error',
        msg:    errStr(err, 'runner error')
      };

      throw newErr;
    }
  }
}
