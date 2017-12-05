import { EventEmitter } from 'events';
import * as WS from 'ws';
import * as M from './message';
import { Serializer } from './serializer';
import { Logger, LogLevel } from './logger';
import { TopPartial, mergeOpt } from './util';
import { Remote, RemoteEvent } from './remote';

export const SERVER_DEFS = {
  host: '127.0.0.1',
  port: 3000,
  log:  LogLevel.warn
};

export type ServerOpt = typeof SERVER_DEFS;

export const enum ServerEvent {
  start            = 'start',
  stop             = 'stop',
  error            = 'error',
  workerconnect    = 'workerconnect',
  workerdisconnect = 'workerdisconnect'
}

export declare interface Server {
  on(event: ServerEvent, fn: (...args: any[]) => void): this;
}

export class Server extends EventEmitter {
  private server: WS.Server;
  private readonly opt: ServerOpt;
  private readonly log = new Logger(Server.name);

  constructor(opt?: TopPartial<ServerOpt>) {
    super();

    this.opt = mergeOpt(SERVER_DEFS, opt) as ServerOpt;
    this.logLevel = this.opt.log;
  }

  set logLevel(val: LogLevel) {
    this.log.level = val;
  }

  start() {
    this.server = this.startServer();
  }

  stop() {
    this.server.close((err) => {
      this.log.warn('server stopped', err);

      this.event(ServerEvent.stop, err);
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

      this.event(ServerEvent.start);
    });

    server.on('connection', (ws) => {
      this.log.warn('worker connected');

      let worker: Remote;

      ws.on('message', async (raw) => {
        const msg = Serializer.unpack(raw as string) as M.Msg;

        if (!worker) {
          switch (msg.code) {
            case M.Code.ready:
              const readyMsg = msg.data as M.Ready;

              // associate worker object with socket
              worker = new Remote(readyMsg.id, readyMsg.name, ws);

              // allow broker to attach worker events
              this.event(ServerEvent.workerconnect, worker, readyMsg);

              worker.event(RemoteEvent.ready, msg.data);
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
              worker.event(RemoteEvent.meta, msg.data);
            break;

            case M.Code.ping:
              worker.event(RemoteEvent.ping);
            break;

            case M.Code.pong:
              worker.event(RemoteEvent.pong);
            break;

            case M.Code.start:
              worker.event(RemoteEvent.start);
            break;

            case M.Code.progress:
              worker.event(RemoteEvent.progress, msg.data);
            break;

            case M.Code.finish:
              worker.event(RemoteEvent.finish, msg.data);
            break;

            default:
              this.log.error('unknown worker message', msg);
            break;
          }
        }
      });

      ws.on('error', (err) => {
        this.log.error('socket error', err);
      });

      ws.on('close', (code, reason) => {
        this.log.error('socket close', code, reason);

        if (worker) {
          this.event(ServerEvent.workerdisconnect, worker);
        }
      });
    });

    server.on('error', (err) => {
      this.log.error('server error', err);

      this.event(ServerEvent.error, err);
    });

    return server;
  }

  private event(e: ServerEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
