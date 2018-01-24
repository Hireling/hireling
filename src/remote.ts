import * as WS from 'ws';
import * as M from './message';
import { WorkerId } from './worker';
import { Serializer } from './serializer';
import { Job } from './job';
import { Logger } from './logger';
import { Signal } from './signal';

// broker's view of a remote worker and socket
export class Remote {
  readonly meta: Signal<M.Data> = new Signal();
  readonly ping: Signal = new Signal();
  readonly pong: Signal = new Signal();
  readonly ready: Signal<M.Ready> = new Signal();
  readonly resume: Signal<M.Resume> = new Signal();
  readonly start: Signal<M.Start> = new Signal();
  readonly progress: Signal<M.Progress> = new Signal();
  readonly finish: Signal<M.Finish> = new Signal();

  readonly id: WorkerId;
  readonly name: string;
  private readonly ws: WS;
  private readonly log: Logger;
  job: Job|null = null;
  lock = false;      // lock worker for job assignment
  lockStart = false; // lock new worker with unknown job state
  closing = false;   // worker intends to close

  constructor(id: WorkerId, name: string, ws: WS, log: Logger) {
    this.id = id;
    this.name = name;
    this.ws = ws;
    this.log = log;
  }

  async sendMsg(code: M.Code, data: M.Data = {}) {
    return new Promise<boolean>((resolve) => {
      this.ws.send(Serializer.pack({ code, data }), (err) => {
        if (err) {
          this.log.error('socket write err', err.message);

          this.closing = true;

          return resolve(false);
        }
        else {
          return resolve(true);
        }
      });
    });
  }
}
