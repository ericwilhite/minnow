/**
 * The client side of election (election.ts): keeps one `ProtocolClient` connected to the
 * database's owner, electing this context when there is none.
 *
 * ## Connecting
 * 1. Ask the broker for a port. `port`: connect (the first time) or reconnect (resending what is
 *    unanswered).
 * 2. `noOwner`: if this context may host (`canHost`), queue for the host lock; on the grant, start
 *    an owner (a page spawns the owner worker; a dedicated worker hosts in place) and connect to it
 *    over a MessageChannel. A `registered` announcement from the broker while queued means another
 *    context won: the queued request is aborted and step 1 runs again. A context that may not host
 *    waits for `registered`.
 * 3. `unreachable` (a live host registered with another broker, or still starting): ask on the
 *    skew channel who owns, supersede it if it is older, then retry.
 * 4. Refused for `version`: a newer client asks the older owner to step aside (`supersede`) and
 *    retries; an older client is superseded (`onSuperseded`) and stops.
 *
 * ## Losing the owner
 * - While requests are in flight it watches the host lock, which the owner itself holds: a grant
 *   means the owner died (a closed tab, a terminated or crashed worker, this page's own worker
 *   included). The connector reconnects, electing itself if it may, and the client resends.
 * - `stepDown` from the owner: reconnect, or stop when superseded.
 *
 * ## Liveness
 * The client's liveness lock is held while it has requests in flight or pins (open transactions,
 * cursors), plus `LIVENESS_IDLE_RELEASE_MS` of idle time; holding it is announced with
 * `liveness { held: true }` and its release with `liveness { held: false }` first.
 *
 * ## Lifecycle
 * The host's page relays its lifecycle events here (`lifecycle`): `freeze`/`pagehide` terminate
 * the owner at once and release the host lock; `hidden` flushes (desktop) or steps down (mobile).
 */
import type { HeldLock } from "../storage/fence.js";
import type { ProtocolClient } from "./client.js";
import {
  BROKER_PROTOCOL,
  LIVENESS_IDLE_RELEASE_MS,
  canHost,
  clientLockName,
  compareProtocol,
  hostLockName,
  lifecycleAction,
  skewChannelName,
  type BrokerMessage,
  type HostContext,
  type LifecycleEvent,
  type SkewSignal,
} from "./election.js";
import { ConnectionRefusedError, OwnerUnreachableError } from "./errors.js";
import type { OwnerHostInit } from "./host.js";
import type { StepDown } from "./protocol.js";
import { watchLock, webLocks, type WebLockManager } from "./web-locks.js";

/** A started owner, as the connector controls it. */
export interface OwnerHandle {
  /** Resolves with the owner id once it serves; rejects if it could not start. */
  readonly ready: Promise<string>;
  /** Resolves when the owner stopped (stepped down, superseded, lock lost, terminated). */
  readonly stopped: Promise<StepDown["reason"]>;
  /** Relays a lifecycle event (the owner decides: flush, step down, terminate). */
  lifecycle(event: LifecycleEvent): void;
  /** Stops it now, synchronously as far as the platform allows. */
  terminate(): void;
}

export type StartOwner = (init: Omit<OwnerHostInit, "t">) => OwnerHandle;

export interface ConnectorOptions {
  readonly database: string;
  readonly client: ProtocolClient;
  readonly locks: WebLockManager;
  /** A fresh port to the broker SharedWorker, or null where there is none. */
  readonly openBroker: (() => MessagePort) | null;
  /** Starts an owner here, or null where this context never hosts. */
  readonly startOwner: StartOwner | null;
  readonly context: () => HostContext;
  /** Owner settings when this context hosts (structured-clone data). */
  readonly settings?: Readonly<Record<string, unknown>>;
  readonly build?: string;
  /** Called once when a newer build owns the database: this context must reload. */
  readonly onSuperseded?: () => void;
  /** How long to wait for a `registered` announcement before trying again. */
  readonly retryMs?: number;
}

export type ConnectorStatus = "connecting" | "connected" | "superseded" | "closed";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export class OwnerConnector {
  readonly #options: ConnectorOptions;
  readonly #client: ProtocolClient;
  #status: ConnectorStatus = "connecting";
  #broker: MessagePort | null = null;
  #nonce = 0;
  readonly #brokerReplies = new Map<number, (message: BrokerMessage) => void>();
  #registered: Array<() => void> = [];
  #hosting: { readonly handle: OwnerHandle; readonly lock: HeldLock } | null = null;
  #ownerProtocol: number | null = null;
  #connecting: Promise<void> | null = null;
  #everConnected = false;
  #watch: { readonly gone: AbortSignal; stop(): void } | null = null;
  #watchIdle: ReturnType<typeof setTimeout> | null = null;
  #liveness: {
    lock: HeldLock | null;
    acquiring: boolean;
    idleTimer: ReturnType<typeof setTimeout> | null;
  } = {
    lock: null,
    acquiring: false,
    idleTimer: null,
  };

  constructor(options: ConnectorOptions) {
    this.#options = options;
    this.#client = options.client;
    // The lock `#updateLiveness` holds is the one the owner must watch.
    this.#client.livenessLock = clientLockName(options.database, options.client.clientId);
    this.#client.onStepDown = (reason) => {
      this.#ownerLeft(reason);
    };
    this.#client.onActivity = () => {
      this.#activity();
    };
  }

  get status(): ConnectorStatus {
    return this.#status;
  }

  /** Whether this context runs the owner. */
  get hosting(): boolean {
    return this.#hosting !== null;
  }

  /** Connects (electing this context if needed). Idempotent while a connection attempt runs. */
  start(): Promise<void> {
    this.#connecting ??= this.#connect().finally(() => {
      this.#connecting = null;
    });
    return this.#connecting;
  }

  /** Relays a page lifecycle event to the owner this context hosts. */
  lifecycle(event: LifecycleEvent): void {
    const hosting = this.#hosting;
    if (hosting === null) return;
    const action = lifecycleAction(event, this.#options.context().platform);
    if (action === "terminate") {
      this.#hosting = null;
      hosting.handle.terminate();
      hosting.lock.release();
      return;
    }
    hosting.handle.lifecycle(event);
  }

  close(): void {
    if (this.#status === "closed") return;
    this.#status = "closed";
    this.#watch?.stop();
    if (this.#watchIdle !== null) clearTimeout(this.#watchIdle);
    this.#releaseLiveness();
    const hosting = this.#hosting;
    this.#hosting = null;
    this.#client.close();
    if (hosting !== null) {
      hosting.handle.terminate();
      hosting.lock.release();
    }
    this.#broker?.close();
  }

  // ---- connecting ----

  async #connect(): Promise<void> {
    const retryMs = this.#options.retryMs ?? 1_000;
    for (;;) {
      if (this.#status === "closed" || this.#status === "superseded") return;
      this.#status = "connecting";
      const answer =
        this.#options.openBroker === null ? ({ t: "noOwner" } as const) : await this.#ask();
      if (answer.t === "port") {
        if (await this.#attach(answer.port)) return;
        continue;
      }
      if (answer.t === "unreachable") {
        await this.#settleSkew();
        await this.#nextRegistration(retryMs);
        continue;
      }
      // No live owner.
      if (this.#options.startOwner === null || !canHost(this.#options.context())) {
        if (this.#options.openBroker === null)
          throw new OwnerUnreachableError("this context cannot host and has no broker");
        await this.#nextRegistration(retryMs);
        continue;
      }
      if (await this.#host()) return;
    }
  }

  /** Connects the client on `port`; false when it must try again (skew handled). */
  async #attach(port: MessagePort): Promise<boolean> {
    try {
      const welcome = this.#everConnected
        ? await this.#client.reconnect(port)
        : await this.#client.connect(port);
      this.#everConnected = true;
      this.#ownerProtocol = welcome.protocol;
      this.#status = "connected";
      this.#activity();
      return true;
    } catch (error) {
      if (!(error instanceof ConnectionRefusedError)) throw error;
      if (error.reason === "version") {
        if (compareProtocol(this.#client.protocol, error.ownerProtocol) === "clientNewer") {
          this.#supersedeOlderOwner();
          await this.#nextRegistration(this.#options.retryMs ?? 1_000);
          return false;
        }
        this.#superseded();
        return true;
      }
      if (error.reason === "superseded") {
        this.#superseded();
        return true;
      }
      // steppingDown or staleGeneration: try again.
      await sleep(10);
      return false;
    }
  }

  /** Queues for the host lock; on the grant starts an owner and connects. False: someone else won. */
  async #host(): Promise<boolean> {
    const database = this.#options.database;
    const waiting = new AbortController();
    const onRegistered = () => {
      waiting.abort();
    };
    this.#registered.push(onRegistered);
    let lock: HeldLock;
    try {
      lock = await webLocks(this.#options.locks).request(hostLockName(database), {
        signal: waiting.signal,
      });
    } catch {
      return false; // another context registered while we queued
    } finally {
      this.#registered = this.#registered.filter((wake) => wake !== onRegistered);
    }
    const start = this.#options.startOwner;
    if (start === null || this.#status === "closed") {
      lock.release();
      return false;
    }
    const channel = new MessageChannel();
    const handle = start({
      database,
      build: this.#options.build ?? "",
      clientPort: channel.port2,
      brokerPort: this.#options.openBroker?.() ?? null,
      settings: { ...this.#options.settings, protocol: this.#client.protocol },
    });
    try {
      await handle.ready;
    } catch {
      handle.terminate();
      lock.release();
      await sleep(50);
      return false;
    }
    this.#hosting = { handle, lock };
    void handle.stopped.then((reason) => {
      if (this.#hosting?.handle !== handle) return;
      this.#hosting = null;
      lock.release();
      this.#ownerLeft(reason);
    });
    return this.#attach(channel.port1);
  }

  // ---- broker ----

  #brokerPort(): MessagePort {
    if (this.#broker !== null) return this.#broker;
    const open = this.#options.openBroker;
    if (open === null) throw new OwnerUnreachableError("no broker");
    const port = open();
    port.onmessage = (event: MessageEvent) => {
      const message = event.data as BrokerMessage;
      if (message.t === "registered") {
        if (message.db !== this.#options.database) return;
        for (const wake of this.#registered.splice(0)) wake();
        return;
      }
      if ("nonce" in message) {
        const reply = this.#brokerReplies.get(message.nonce);
        this.#brokerReplies.delete(message.nonce);
        reply?.(message);
      }
    };
    this.#broker = port;
    return port;
  }

  #ask(): Promise<
    | { readonly t: "port"; readonly port: MessagePort }
    | { readonly t: "noOwner" }
    | { readonly t: "unreachable" }
  > {
    const port = this.#brokerPort();
    const nonce = ++this.#nonce;
    return new Promise((resolve) => {
      this.#brokerReplies.set(nonce, (message) => {
        if (message.t === "port") resolve({ t: "port", port: message.port });
        else if (message.t === "unreachable") resolve({ t: "unreachable" });
        else resolve({ t: "noOwner" });
      });
      port.postMessage({
        t: "connect",
        v: BROKER_PROTOCOL,
        db: this.#options.database,
        clientId: this.#client.clientId,
        nonce,
      } satisfies BrokerMessage);
    });
  }

  /** Resolves on the next owner registration, or after `ms`. */
  #nextRegistration(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(done, ms);
      function done() {
        clearTimeout(timer);
        resolve();
      }
      this.#registered.push(done);
    });
  }

  // ---- skew ----

  #supersedeOlderOwner(): void {
    if (typeof BroadcastChannel === "undefined") return;
    const channel = new BroadcastChannel(skewChannelName(this.#options.database));
    channel.postMessage({ t: "supersede", protocol: this.#client.protocol } satisfies SkewSignal);
    channel.close();
  }

  /** Asks who owns; supersedes an older owner, or is superseded by a newer one. */
  async #settleSkew(): Promise<void> {
    if (typeof BroadcastChannel === "undefined") return;
    const channel = new BroadcastChannel(skewChannelName(this.#options.database));
    const owner = await new Promise<number | null>((resolve) => {
      const timer = setTimeout(() => {
        resolve(null);
      }, 200);
      channel.onmessage = (event: MessageEvent) => {
        const signal = event.data as SkewSignal;
        if (signal.t !== "owner") return;
        clearTimeout(timer);
        resolve(signal.protocol);
      };
      channel.postMessage({ t: "whoOwns", protocol: this.#client.protocol } satisfies SkewSignal);
    });
    channel.close();
    if (owner === null) return;
    const relation = compareProtocol(this.#client.protocol, owner);
    if (relation === "clientNewer") this.#supersedeOlderOwner();
    else if (relation === "clientOlder") this.#superseded();
  }

  #superseded(): void {
    if (this.#status === "superseded") return;
    this.#status = "superseded";
    this.#watch?.stop();
    this.#options.onSuperseded?.();
  }

  // ---- losing the owner ----

  #ownerLeft(reason: StepDown["reason"]): void {
    if (this.#status === "closed" || this.#status === "superseded") return;
    // An owner of our own protocol stepping aside for a newer build: we are the older build.
    if (reason === "superseded" && this.#ownerProtocol === this.#client.protocol) {
      this.#superseded();
      return;
    }
    this.#status = "connecting";
    void this.start().catch(() => undefined);
  }

  #activity(): void {
    this.#updateWatch();
    this.#updateLiveness();
  }

  /**
   * Watch the host lock while requests are in flight. The owner holds it, so a grant means the
   * owner died: another context's, or this page's own worker.
   */
  #updateWatch(): void {
    const needs = this.#status === "connected" && this.#client.pendingCount > 0;
    if (needs) {
      if (this.#watchIdle !== null) clearTimeout(this.#watchIdle);
      this.#watchIdle = null;
      if (this.#watch !== null) return;
      const watch = watchLock(this.#options.locks, hostLockName(this.#options.database));
      this.#watch = watch;
      watch.gone.addEventListener(
        "abort",
        () => {
          if (this.#watch !== watch) return;
          this.#watch = null;
          const hosting = this.#hosting;
          if (hosting !== null) {
            this.#hosting = null;
            hosting.handle.terminate();
            hosting.lock.release();
          }
          this.#ownerLeft("closing");
        },
        { once: true },
      );
      return;
    }
    // Kept for `LIVENESS_IDLE_RELEASE_MS` after the last request: a request loop pays one lock
    // request, not one per request (lock calls cross processes).
    if (this.#watch === null || this.#watchIdle !== null) return;
    this.#watchIdle = setTimeout(() => {
      this.#watchIdle = null;
      if (this.#client.pendingCount > 0) return;
      this.#watch?.stop();
      this.#watch = null;
    }, LIVENESS_IDLE_RELEASE_MS);
  }

  #updateLiveness(): void {
    const state = this.#liveness;
    const busy = this.#client.pendingCount > 0 || this.#client.hasPins;
    if (busy) {
      if (state.idleTimer !== null) clearTimeout(state.idleTimer);
      state.idleTimer = null;
      if (state.lock === null && !state.acquiring) {
        state.acquiring = true;
        void webLocks(this.#options.locks)
          .request(clientLockName(this.#options.database, this.#client.clientId), {})
          .then((lock) => {
            state.acquiring = false;
            if (this.#status === "closed") {
              lock.release();
              return;
            }
            state.lock = lock;
            this.#client.liveness(true);
          })
          .catch(() => {
            state.acquiring = false;
          });
      }
      return;
    }
    if (state.lock !== null && state.idleTimer === null)
      state.idleTimer = setTimeout(() => {
        state.idleTimer = null;
        if (this.#client.pendingCount === 0 && !this.#client.hasPins) this.#releaseLiveness();
      }, LIVENESS_IDLE_RELEASE_MS);
  }

  #releaseLiveness(): void {
    const state = this.#liveness;
    if (state.idleTimer !== null) clearTimeout(state.idleTimer);
    state.idleTimer = null;
    if (state.lock === null) return;
    this.#client.liveness(false);
    state.lock.release();
    state.lock = null;
  }
}

/**
 * `StartOwner` for a page: spawns the owner worker (`createWorker`), sends it `minnow:host` and
 * relays lifecycle events; terminating it frees its handles (5–40 ms, storage probe).
 */
export function workerOwner(
  createWorker: () => Worker,
  platform: HostContext["platform"],
): StartOwner {
  return (init) => {
    const worker = createWorker();
    let resolveReady: (ownerId: string) => void = () => undefined;
    let rejectReady: (error: Error) => void = () => undefined;
    let resolveStopped: (reason: StepDown["reason"]) => void = () => undefined;
    const ready = new Promise<string>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    ready.catch(() => undefined);
    const stopped = new Promise<StepDown["reason"]>((resolve) => {
      resolveStopped = resolve;
    });
    worker.onmessage = (event: MessageEvent) => {
      const status = event.data as { t: string; ownerId?: string; reason?: StepDown["reason"] };
      if (status.t === "minnow:ready") resolveReady(status.ownerId ?? "");
      else if (status.t === "minnow:failed")
        rejectReady(new Error("The owner worker could not start"));
      else if (status.t === "minnow:stopped") {
        resolveStopped(status.reason ?? "closing");
        worker.terminate();
      }
    };
    worker.onerror = () => {
      rejectReady(new Error("The owner worker failed"));
      resolveStopped("closing");
    };
    const transfer: Transferable[] = [init.clientPort];
    if (init.brokerPort !== null) transfer.push(init.brokerPort);
    worker.postMessage({ ...init, t: "minnow:host" } satisfies OwnerHostInit, transfer);
    return {
      ready,
      stopped,
      lifecycle: (event) => {
        worker.postMessage({ t: "minnow:lifecycle", event, platform });
      },
      terminate: () => {
        worker.terminate();
        resolveStopped("pagehide");
      },
    };
  };
}

/** Wires a page's lifecycle events to the connector (returns a function that unwires them). */
export function relayPageLifecycle(
  connector: OwnerConnector,
  target: Window & typeof globalThis,
): () => void {
  const freeze = () => {
    connector.lifecycle("freeze");
  };
  const pagehide = () => {
    connector.lifecycle("pagehide");
  };
  const visibility = () => {
    connector.lifecycle(target.document.visibilityState === "hidden" ? "hidden" : "visible");
  };
  target.addEventListener("freeze", freeze);
  target.addEventListener("pagehide", pagehide);
  target.document.addEventListener("visibilitychange", visibility);
  return () => {
    target.removeEventListener("freeze", freeze);
    target.removeEventListener("pagehide", pagehide);
    target.document.removeEventListener("visibilitychange", visibility);
  };
}
