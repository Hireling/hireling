import * as WS from 'ws';
import * as M from './message';
import { WorkerId } from './worker';
import { Serializer } from './serializer';
import { Job } from './job';
import { Logger } from './logger';
import { Signal } from './signal';

// broker's view of a remote worker and socket
export class Remote {
  readonly meta     = new Signal<M.Meta>();
  readonly ping     = new Signal<M.Ping>();
  readonly pong     = new Signal<M.Pong>();
  readonly ready    = new Signal<M.Ready>();
  readonly resume   = new Signal<M.Resume>();
  readonly start    = new Signal<M.Start>();
  readonly progress = new Signal<M.Progress>();
  readonly finish   = new Signal<M.Finish>();

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

  async sendMsg(msg: M.Msg) {
    return new Promise<boolean>((resolve) => {
      this.ws.send(Serializer.pack(msg), (err) => {
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
