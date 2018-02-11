import * as path from 'path';
import Module = require('module');
import { JobHandle } from './worker';

export const enum CtxKind {
  path = 'path',
  src  = 'src',
}

export type CtxFn<T = any, U = any> = (jh: JobHandle<T>) => Promise<U>;

export interface SrcCtx<T, U> {
  ctx:     CtxFn<T, U>|string;
  ctxkind: CtxKind.src;
}

export interface PathCtx {
  ctx:     string;
  ctxkind: CtxKind.path;
}

export type RunCtx<T, U> = SrcCtx<T, U>|PathCtx;

export type CtxArg<T = any, U = any> = RunCtx<T, U>|CtxFn<T, U>|string;

export class Ctx {
  static fromArg<T, U>(arg: CtxArg<T, U>|null) {
    if (!arg) {
      return null;
    }
    else if (typeof arg === 'string' || typeof arg === 'function') {
      const ctx: RunCtx<T, U> = {
        ctx:     arg,
        ctxkind: (typeof arg === 'string' ? CtxKind.path : CtxKind.src) as any
      };

      return ctx;
    }
    else {
      return arg;
    }
  }

  static fromSrc<T, U>(src: string, filename = '') {
    const parent = module.parent;

    const m = new Module(filename, parent || undefined);

    m.filename = filename;
    m.paths = (Module as any)._nodeModulePaths(path.dirname(filename));
    (m as any)._compile(`exports.ctx = ${src};`, filename);

    if (parent) {
      parent.children.splice(parent.children.indexOf(m), 1);
    }

    return m.exports.ctx as CtxFn<T, U>;
  }

  static toFn<T, U>(ctx: string|null, ctxkind: CtxKind|null): CtxFn<T, U> {
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
