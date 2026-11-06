/**
 * A broker SharedWorker that can be replaced under its ports, as WebKit 26.6 does on a
 * cross-origin-isolated page when the page that created the SharedWorker closes
 * (owner/webkit-sharedworker.md): a new `Broker` instance with empty state, every port reattached
 * to it (the new instance accepts each again and announces its id), and the messages sent while the
 * switch runs dropped. Replies the old instance was still preparing are dropped too: it is gone.
 *
 * - `accept(port)`: as `Broker.accept`, for the broker's end of a port.
 * - `restart({ dropMs })`: the current instance dies now; messages arriving in the next `dropMs`
 *   go nowhere; then a new instance takes every port.
 * - `duplicateNext(t)`: the next message of kind `t` is delivered twice to the current instance
 *   (a client retry that the same instance also saw).
 */
import { Broker, type LockQuery } from "../owner/broker.js";

interface Attached {
  /** What the current instance sees as the port. */
  view: MessagePort;
  readonly port: MessagePort;
}

export class RestartingBroker {
  readonly #locks: LockQuery;
  #broker: Broker | null;
  readonly #ports: Attached[] = [];
  #duplicate: string | null = null;
  /** Instances so far (1 before any restart). */
  instances = 1;
  /** Messages dropped while no instance was there, by kind. */
  readonly dropped = new Map<string, number>();
  /** Replies an instance posted after it died (lost with it). */
  lostReplies = 0;
  /** At each restart: what the dying instance held (registrations, waiting ports). */
  readonly restarts: Array<{ readonly registered: number; readonly waiting: number }> = [];
  #pastDuplicates = 0;
  /** Messages delivered twice by `duplicateNext`. */
  duplicated = 0;

  constructor(locks: LockQuery) {
    this.#locks = locks;
    this.#broker = new Broker(locks);
  }

  /** The current instance (null while a restart runs). */
  get current(): Broker | null {
    return this.#broker;
  }

  accept(port: MessagePort): void {
    const attached: Attached = { port, view: port };
    this.#ports.push(attached);
    // One router for the port's whole life: it delivers to the instance of the moment.
    port.onmessage = (event: MessageEvent) => {
      const view = attached.view as unknown as {
        onmessage: ((event: MessageEvent) => void) | null;
      };
      if (this.#broker === null) {
        const kind = String((event.data as { t?: string }).t);
        this.dropped.set(kind, (this.dropped.get(kind) ?? 0) + 1);
        return;
      }
      view.onmessage?.(event);
      const kind = (event.data as { t?: string }).t;
      if (kind !== undefined && kind === this.#duplicate) {
        this.#duplicate = null;
        this.duplicated++;
        view.onmessage?.(event);
      }
    };
    if (this.#broker !== null) this.#attach(attached, this.#broker);
  }

  /** Replaces the instance: `dropMs` of lost messages, then a new, empty one takes every port. */
  restart({ dropMs }: { readonly dropMs: number }): Promise<void> {
    const dying = this.#broker;
    if (dying !== null) {
      this.restarts.push({ registered: dying.registrations.size, waiting: dying.heldPorts });
      this.#pastDuplicates += dying.duplicates;
    }
    this.#broker = null;
    return new Promise((resolve) => {
      setTimeout(() => {
        const broker = new Broker(this.#locks);
        this.#broker = broker;
        this.instances++;
        for (const attached of this.#ports) this.#attach(attached, broker);
        resolve();
      }, dropMs);
    });
  }

  /** Duplicate requests every instance so far received. */
  get duplicates(): number {
    return this.#pastDuplicates + (this.#broker?.duplicates ?? 0);
  }

  duplicateNext(kind: string): void {
    this.#duplicate = kind;
  }

  /** Gives `broker` its own view of the port: posts from an instance that died go nowhere. */
  #attach(attached: Attached, broker: Broker): void {
    const { port } = attached;
    const view = {
      onmessage: null as ((event: MessageEvent) => void) | null,
      postMessage: (message: unknown, transfer?: Transferable[]) => {
        if (this.#broker !== broker) {
          this.lostReplies++;
          return;
        }
        port.postMessage(message, transfer ?? []);
      },
      addEventListener: () => undefined,
      close: () => {
        port.close();
      },
    };
    attached.view = view as unknown as MessagePort;
    broker.accept(attached.view);
  }
}
