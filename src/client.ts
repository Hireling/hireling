import * as WS from 'ws';
import * as M from './message';
import { Serializer } from './serializer';
import { Logger, LogLevel } from './logger';
import { TopPartial, mergeOpt } from './util';
import { Signal } from './signal';

export const CLIENT_DEFS = {
  log:     LogLevel.warn,
  host:    'localhost',
  port:    3000,
  wss:     false,
  retryms: 5000,
  retryx:  Infinity
};

export type ClientOpt = typeof CLIENT_DEFS;

export class Client {
  readonly up      = new Signal();
  readonly down    = new Signal<Error|null>();
  readonly meta    = new Signal<M.Meta>();
  readonly ping    = new Signal<M.Ping>();
  readonly pong    = new Signal<M.Pong>();
  readonly readyok = new Signal<M.ReadyOk>();
  readonly assign  = new Signal<M.Assign>();
  readonly abort   = new Signal<M.Abort>();

  private retrytimer: NodeJS.Timer|null = null;
  private retries: number;
  private ws: WS|null = null;
  private readonly opt: ClientOpt;
  private readonly log = new Logger(Client.name);

  constructor(opt?: TopPartial<ClientOpt>) {
    this.opt = mergeOpt(CLIENT_DEFS, opt) as ClientOpt;
    this.logLevel = this.opt.log;
    this.retries = this.opt.retryx;
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  get report() {
    return {
      up: !!this.ws && this.ws.readyState === this.ws.OPEN
    };
  }

  start() {
    if (this.ws) {
      this.log.warn('already started');
      return;
    }

    this.cancelRetry();
    this.retries = this.opt.retryx;

    this.ws = this.startClient();
  }

  stop() {
    this.cancelRetry();
    this.retries = -1;

    if (this.ws) {
      this.ws.terminate();
      this.ws = null;

      this.log.warn('stopped');
    }
    else {
      this.log.warn('already stopped');
    }
  }

  private startClient() {
    const { wss, host, port } = this.opt;
    const url = `${wss ? 'wss' : 'ws'}://${host}:${port}`;

    this.log.debug(`connecting to ${url}…`);

    const ws = new WS(url);

    ws.on('open', () => {
      this.log.warn(`connected to ${url}`);

      this.up.emit();
    });

    ws.on('message', (raw) => {
      const msg = Serializer.unpack(raw as string) as M.Msg;

      switch (msg.code) {
        case M.Code.meta: this.meta.emit(msg); break;
        case M.Code.ping: this.ping.emit(msg); break;
        case M.Code.pong: this.pong.emit(msg); break;
        case M.Code.readyok: this.readyok.emit(msg); break;
        case M.Code.assign: this.assign.emit(msg); break;
        case M.Code.abort: this.abort.emit(msg); break;
        default: this.log.error('unknown message', msg); break;
      }
    });

    ws.on('close', (code, reason) => {
      this.log.error('socket close', code, reason);

      this.ws = null;

      this.down.emit(null);

      this.retry();
    });

    ws.on('error', (err) => {
      this.log.error('socket error', err);

      this.down.emit(err);

      this.ws = null;
    });

    return ws;
  }

  private retry() {
    if (this.retries === -1) {
      this.log.debug('stop requested, abort reconnect');
      return;
    }
    else if (this.retries === 0) {
      this.log.debug('no retries left, abort reconnect');
      return;
    }

    this.log.info('reconnecting');

    this.retrytimer = setTimeout(() => {
      this.retrytimer = null;

      this.log.info(`retrying (${--this.retries}/${this.opt.retryx})`);

      this.ws = this.startClient();
    }, this.opt.retryms);
  }

  private cancelRetry() {
    if (!this.retrytimer) {
      return;
    }

    this.log.info('cancel reconnect timer');
    clearTimeout(this.retrytimer);
    this.retrytimer = null;
  }

  async sendMsg(msg: M.Msg) {
    return new Promise<boolean>((resolve) => {
      if (!this.ws) {
        return resolve(false);
      }

      this.ws.send(Serializer.pack(msg), (err) => {
        if (err) {
          this.log.error('socket write err', err.message);

          return resolve(false);
        }
        else {
          return resolve(true);
        }
      });
    });
  }
}
