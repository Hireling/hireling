import { EventEmitter } from 'events';
import * as WS from 'ws';
import * as M from './message';
import { WorkerId } from './worker';
import { Serializer } from './serializer';
import { Job } from './job';
import { NoopHandler } from './util';

export const enum RemoteEvent {
  meta     = 'meta',
  ping     = 'ping',
  pong     = 'pong',
  ready    = 'ready',
  resume   = 'resume',
  replay   = 'replay',
  start    = 'start',
  progress = 'progress',
  finish   = 'finish',
}

// tslint:disable:unified-signatures
export declare interface Remote {
  on(e: RemoteEvent.meta|'meta', fn: NoopHandler): this;
  on(e: RemoteEvent.ping|'ping', fn: NoopHandler): this;
  on(e: RemoteEvent.pong|'pong', fn: NoopHandler): this;
  on(e: RemoteEvent.ready|'ready', fn: NoopHandler): this;
  on(e: RemoteEvent.resume|'resume', fn: NoopHandler): this;
  on(e: RemoteEvent.replay|'replay', fn: NoopHandler): this;
  on(e: RemoteEvent.start|'start', fn: NoopHandler): this;
  on(e: RemoteEvent.progress|'progress', fn: NoopHandler): this;
  on(e: RemoteEvent.finish|'finish', fn: NoopHandler): this;
}
// tslint:enable:unified-signatures

// broker's view of a remote worker and socket
export class Remote extends EventEmitter {
  readonly id: WorkerId;
  readonly name: string;
  private readonly ws: WS;
  job: Job|null = null;
  locked = false;  // hold worker for job assignment
  closing = false; // worker intends to close

  constructor(id: WorkerId, name: string, ws: WS) {
    super();

    this.id = id;
    this.name = name;
    this.ws = ws;
  }

  async sendMsg(code: M.Code, data: M.Data = {}) {
    return new Promise<boolean>((resolve) => {
      this.ws.send(Serializer.pack({ code, data }), (err) => {
        if (err) {
          console.log('socket write err', err.message);

          this.closing = true;

          return resolve(false);
        }
        else {
          return resolve(true);
        }
      });
    });
  }

  event(e: RemoteEvent, ...args: any[]) {
    setImmediate(() => {
      this.emit(e, ...args);
    });
  }
}
