import * as path from 'path';
import Module = require('module');
import { JobContext, JobHandle } from './worker';

interface StringifiedArgs {
  jh:     JobHandle;
  ctx:    string;
  isPath: boolean;
}

process.on('message', async (opts: StringifiedArgs) => {
  try {
    const { jh, ctx: _ctx, isPath } = opts;

    // re-wire progress hook
    jh.progress = (arg) => {
      process.send!({ progress: arg });
    };

    let ctx: JobContext;

    if (isPath) {
      ctx = require(_ctx).ctx as JobContext;
    }
    else {
      const filename = '';
      const parent = module.parent;
      const m = new Module(filename, parent || undefined);

      m.filename = filename;
      m.paths = (Module as any)._nodeModulePaths(path.dirname(filename));

      (m as any)._compile(`exports.ctx = ${_ctx};`, filename);

      if (parent) {
        parent.children.splice(parent.children.indexOf(m), 1);
      }

      // require.cache[filename] = m;

      ctx = m.exports.ctx;
    }

    const result = await ctx(jh);

    process.send!({ result });
  }
  catch (err) {
    process.send!({ error: err.message });
  }
});

process.on('unhandledRejection', (err) => {
  console.log('SANDBOX unhandledRejection', err);
});

process.on('uncaughtException', (err) => {
  console.log('SANDBOX uncaughtException', err);

  process.exit(1);
});
