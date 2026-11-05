/**
 * The port broker (election.ts "Reaching the owner"): runs in a SharedWorker shared by every
 * context of the origin. It keeps one registration per database and, for each `connect`, creates
 * a MessageChannel and hands one end to the owner and the other to the client. After that the
 * broker is off the path.
 *
 * - It checks the host lock (`navigator.locks.query()`) before handing out a port: a registration
 *   is live only while the context it names (`lockHolder`) holds the host lock. A dead one is
 *   dropped, never used (`noOwner`, or `unreachable` while a successor that has not registered yet
 *   holds the lock). A held host lock with no live registration here is `unreachable`: the owner
 *   registered with another broker (another build's script) or is still starting.
 * - Every registration is announced to every connected context (`registered`), so waiting clients
 *   reconnect at once.
 * - It takes no locks itself.
 */
import { BROKER_PROTOCOL, hostLockName, type BrokerMessage } from "./election.js";

/** The slice of `navigator.locks` the broker uses. */
export interface LockQuery {
  query(): Promise<{
    readonly held?: ReadonlyArray<{ readonly name?: string; readonly clientId?: string }>;
  }>;
}

interface Registration {
  readonly ownerId: string;
  readonly protocol: number;
  readonly port: MessagePort;
  readonly lockHolder: string | null;
}

export class Broker {
  readonly #locks: LockQuery;
  readonly #owners = new Map<string, Registration>();
  readonly #ports = new Set<MessagePort>();

  constructor(locks: LockQuery) {
    this.#locks = locks;
  }

  /** Serves a newly connected context's port. */
  accept(port: MessagePort): void {
    this.#ports.add(port);
    port.onmessage = (event: MessageEvent) => {
      void this.#receive(port, event.data as BrokerMessage);
    };
  }

  /** Who holds the host lock: `null` when nobody does, `""` when the platform hides the id. */
  async #hostHolder(db: string): Promise<string | null> {
    const state = await this.#locks.query();
    const name = hostLockName(db);
    const held = (state.held ?? []).find((lock) => lock.name === name);
    return held === undefined ? null : (held.clientId ?? "");
  }

  async #receive(port: MessagePort, message: BrokerMessage): Promise<void> {
    if (!("v" in message) || message.v !== BROKER_PROTOCOL) return;
    switch (message.t) {
      case "ping":
        port.postMessage({ t: "pong", nonce: message.nonce } satisfies BrokerMessage);
        return;
      case "register":
        this.#owners.set(message.db, {
          ownerId: message.ownerId,
          protocol: message.protocol,
          port,
          lockHolder: message.lockHolder,
        });
        for (const other of this.#ports)
          other.postMessage({
            t: "registered",
            db: message.db,
            ownerId: message.ownerId,
          } satisfies BrokerMessage);
        return;
      case "unregister":
        if (this.#owners.get(message.db)?.ownerId === message.ownerId)
          this.#owners.delete(message.db);
        this.#ports.delete(port);
        return;
      case "connect": {
        const owner = this.#owners.get(message.db);
        const holder = await this.#hostHolder(message.db);
        const held = holder !== null;
        // The registered owner is live only while it, not some other context, holds the host lock
        // (a successor starting up holds it before it registers).
        const live =
          owner !== undefined &&
          held &&
          (owner.lockHolder === null || holder === "" || owner.lockHolder === holder);
        if (live && this.#owners.get(message.db) === owner) {
          const channel = new MessageChannel();
          owner.port.postMessage(
            {
              t: "client",
              clientId: message.clientId,
              port: channel.port1,
            } satisfies BrokerMessage,
            [channel.port1],
          );
          port.postMessage(
            {
              t: "port",
              nonce: message.nonce,
              ownerId: owner.ownerId,
              port: channel.port2,
            } satisfies BrokerMessage,
            [channel.port2],
          );
          return;
        }
        if (owner !== undefined && !live) this.#owners.delete(message.db);
        port.postMessage({
          t: held ? "unreachable" : "noOwner",
          nonce: message.nonce,
        } satisfies BrokerMessage);
        return;
      }
      default:
        return;
    }
  }
}

/** The SharedWorker entry: `serveBroker(self, navigator.locks)`. */
export function serveBroker(
  scope: { onconnect: ((event: MessageEvent) => void) | null },
  locks: LockQuery,
): Broker {
  const broker = new Broker(locks);
  scope.onconnect = (event: MessageEvent) => {
    const port = event.ports[0];
    if (port !== undefined) broker.accept(port);
  };
  return broker;
}
