import * as WS from 'ws';
import * as M from './message';
import { Serializer } from './serializer';
import { Logger, LogLevel } from './logger';
import { TopPartial, mergeOpt } from './util';
import { Remote } from './remote';
import { Signal } from './signal';

export const SERVER_DEFS = {
  host: '127.0.0.1',
  port: 3000,
  log:  LogLevel.warn
};

export type ServerOpt = typeof SERVER_DEFS;

export class Server {
  readonly up: Signal = new Signal();
  readonly down: Signal<Error|null> = new Signal();
  readonly error: Signal<Error> = new Signal();
  readonly workerup: Signal<Remote> = new Signal();
  readonly workerdown: Signal<Remote> = new Signal();

  private server: WS.Server;
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
    this.server = this.startServer();
  }

  stop() {
    this.server.close((err) => {
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
              const ready = msg.data as M.Ready;

              worker = new Remote(ready.id, ready.name, ws, this.logRemote);

              // allow broker to attach worker events
              this.workerup.emit(worker);

              worker.ready.emit(ready);
            break;

            case M.Code.resume:
              const resume = msg.data as M.Resume;

              worker = new Remote(resume.id, resume.name, ws, this.logRemote);

              // allow broker to attach worker events
              this.workerup.emit(worker);

              worker.resume.emit(resume);
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
              worker.meta.emit(msg.data);
            break;

            case M.Code.ping:
              worker.ping.emit(msg.data);
            break;

            case M.Code.pong:
              worker.pong.emit(msg.data);
            break;

            case M.Code.start:
              worker.start.emit(msg.data as M.Start);
            break;

            case M.Code.progress:
              worker.progress.emit(msg.data as M.Progress);
            break;

            case M.Code.finish:
              worker.finish.emit(msg.data as M.Finish);
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
