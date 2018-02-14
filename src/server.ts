import * as WS from 'ws';
import * as M from './message';
import { Serializer } from './serializer';
import { Logger, LogLevel } from './logger';
import { TopPartial, mergeOpt } from './util';
import { Remote } from './remote';
import { Signal } from './signal';

export const SERVER_DEFS = {
  log:  LogLevel.warn,
  host: '127.0.0.1',
  port: 3000
};

export type ServerOpt = typeof SERVER_DEFS;

export class Server {
  readonly up         = new Signal();
  readonly down       = new Signal<Error|null>();
  readonly error      = new Signal<Error>();
  readonly workerup   = new Signal<Remote>();
  readonly workerdown = new Signal<Remote>();

  private server: WS.Server|null = null;
  private readonly opt: ServerOpt;
  private readonly log = new Logger(Server.name);
  private readonly logRemote = new Logger(Remote.name); // shared instance

  constructor(opt?: TopPartial<ServerOpt>) {
    this.opt = mergeOpt(SERVER_DEFS, opt) as ServerOpt;
    this.logLevel = this.opt.log;
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
    this.logRemote.level = val;
  }

  start() {
    if (this.server) {
      throw new Error('server is running');
    }

    this.server = this.startServer();
  }

  stop() {
    if (!this.server) {
      throw new Error('server not started');
    }

    this.server.close((err) => {
      this.server = null;

      this.log.warn('server stopped', err);

      this.down.emit(err || null);
    });
  }

  private startServer() {
    const server = new WS.Server({
      host: this.opt.host,
      port: this.opt.port
    });

    server.on('listening', () => {
      const { host, port } = server.options;

      this.log.warn(`server up on ${host}:${port}`);

      this.up.emit();
    });

    server.on('connection', (ws) => {
      this.log.warn('worker connected');

      let worker: Remote;

      ws.on('message', (raw) => {
        const msg = Serializer.unpack(raw as string) as M.Msg;

        if (!worker) {
          switch (msg.code) {
            case M.Code.ready:
              worker = new Remote(msg.id, msg.name, ws, this.logRemote);

              // allow broker to attach worker events
              this.workerup.emit(worker);

              worker.ready.emit(msg);
              break;

            case M.Code.resume:
              worker = new Remote(msg.id, msg.name, ws, this.logRemote);

              // allow broker to attach worker events
              this.workerup.emit(worker);

              worker.resume.emit(msg);
              break;

            default:
              this.log.error('unknown socket message', msg);
              break;
          }
        }
        else {
          if (msg.closing) {
            this.log.info(`worker ${worker.name} sent closing`);

            worker.closing = true;
          }

          switch (msg.code) {
            case M.Code.meta:
              worker.meta.emit(msg);
              break;

            case M.Code.ping:
              worker.ping.emit(msg);
              break;

            case M.Code.pong:
              worker.pong.emit(msg);
              break;

            case M.Code.start:
              worker.start.emit(msg);
              break;

            case M.Code.progress:
              worker.progress.emit(msg);
              break;

            case M.Code.finish:
              worker.finish.emit(msg);
              break;

            default:
              this.log.error('unknown worker message', msg);
              break;
          }
        }
      });

      ws.on('close', (code, reason) => {
        this.log.error('socket close', code, reason);

        if (worker) {
          this.workerdown.emit(worker);
        }
      });

      ws.on('error', (err) => {
        this.log.error('socket error', err);
      });
    });

    server.on('error', (err) => {
      this.log.error('server error', err);

      this.error.emit(err);
    });

    return server;
  }
}
