import * as path from 'path';
import Module = require('module');
import { JobHandle } from './worker';

export const enum CtxKind {
  path = 'path',
  src  = 'src',
}

export type CtxFn<I = any, O = any> = (jh: JobHandle<I>) => Promise<O>;

export interface SrcCtx<I, O> {
  ctx:     CtxFn<I, O>|string;
  ctxkind: CtxKind.src;
}

export interface PathCtx {
  ctx:     string;
  ctxkind: CtxKind.path;
}

export type RunCtx<I, O> = SrcCtx<I, O>|PathCtx;

export type CtxArg<I = any, O = any> = RunCtx<I, O>|CtxFn<I, O>|string;

export class Ctx {
  static fromArg<I, O>(arg: CtxArg<I, O>|null) {
    if (!arg) {
      return null;
    }
    else if (typeof arg === 'string' || typeof arg === 'function') {
      const ctx: RunCtx<I, O> = {
        ctx:     arg,
        ctxkind: (typeof arg === 'string' ? CtxKind.path : CtxKind.src) as any
      };

      return ctx;
    }
    else {
      return arg;
    }
  }

  static fromSrc<I, O>(src: string, filename = '') {
    const parent = module.parent;

    const m = new Module(filename, parent || undefined);

    m.filename = filename;
    m.paths = (Module as any)._nodeModulePaths(path.dirname(filename));
    (m as any)._compile(`exports.ctx = ${src};`, filename);

    if (parent) {
      parent.children.splice(parent.children.indexOf(m), 1);
    }

    return m.exports.ctx as CtxFn<I, O>;
  }

  static toFn<I, O>(ctx: string|null, ctxkind: CtxKind|null): CtxFn<I, O> {
    if (!ctx || !ctxkind) {
      throw new Error('no run context');
    }
    else if (ctxkind === CtxKind.path) {
      return require(ctx).ctx;
    }
    else if (ctxkind === CtxKind.src) {
      return Ctx.fromSrc(ctx);
    }
    else {
      throw new Error('unknown context kind');
    }
  }
}
