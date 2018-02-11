import { errStr } from './runner';
import { JobAttr } from './job';
import { Ctx } from './ctx';

export interface ProcProg { code: 'progress'; progress: any; }
export interface ProcDone { code: 'done'; result: any; }
export interface ProcFail { code: 'failed'; msg: string; }
export type ProcMsg = ProcProg|ProcDone|ProcFail;

const msgRunner = (msg: ProcMsg) => process.send!(msg);

process.on('message', async (j: JobAttr) => {
  try {
    const ctxFn = Ctx.toFn(j.ctx, j.ctxkind);

    const result = await ctxFn({
      job: j,
      progress(progress) {
        msgRunner({ code: 'progress', progress });
      }
    });

    msgRunner({ code: 'done', result });
  }
  catch (err) {
    msgRunner({ code: 'failed', msg:  errStr(err, 'sandbox error') });
  }
});

process.on('unhandledRejection', (err) => {
  console.log('SANDBOX unhandledRejection', err);
});

process.on('uncaughtException', (err) => {
  console.log('SANDBOX uncaughtException', err);

  process.exit(1);
});
