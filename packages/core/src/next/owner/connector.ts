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
 * ## The handshake
 * Bounded and watched: it is abandoned when the host lock is granted to a watch (the owner died
 * before it answered) or after `HANDSHAKE_TIMEOUT_MS`, and the connector starts again from step 1.
 * A welcomed connection re-announces the liveness lock before its resends (client.ts).
 *
 * ## Losing the owner
 * - While requests are in flight it watches the host lock, which the owner itself holds: a grant
 *   means the owner died (a closed tab, a terminated or crashed worker, this page's own worker
 *   included). The connector reconnects, electing itself if it may, and the client resends.
 * - `stepDown` from the owner: reconnect, or stop when superseded.
 * - A watch only sees an owner's absence, not its replacement: a client whose owner died while
 *   it was idle (no watch) keeps a dead port, and its next request's watch queues behind the
 *   successor's live host lock. So a new owner announces itself on the database's
 *   BroadcastChannel and a connected client of another owner reconnects at once, idle or not.
 * - Backstop (a missed announcement, a hung port): with requests in flight and nothing heard for
 *   `SILENCE_DEADLINE_MS`, the connector compares the host lock's holder with its owner's
 *   (`Welcome.lockHolder`). The same: alive and only slow (throttled, or a WebKit owner running a
 *   long statement sends no `alive`); the count restarts. Otherwise: reconnect and resend.
 *
 * ## Liveness
 * The client's liveness lock is held while it has requests in flight or pins (open transactions,
 * cursors), plus `LIVENESS_IDLE_RELEASE_MS` of idle time; holding it is announced with
 * `liveness { held: true }` and its release with `liveness { held: false }` first.
 *
 * ## Lifecycle
 * The host's page relays its lifecycle events here (`lifecycle`): `freeze`/`pagehide` terminate
 * the owner at once and release the host lock; `hidden` flushes (desktop) or steps down (mobile).
 *
 * ## Closing
 * `close()` ends everything this context started, wherever it is: a queued host-lock request is
 * withdrawn, an owner still starting is terminated and its host lock released, a liveness grant
 * that arrives later is released at once.
 */
import type { HeldLock } from "../storage/fence.js";
import type { ProtocolClient } from "./client.js";
import {
  BROKER_PROTOCOL,
  HANDSHAKE_TIMEOUT_MS,
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
import { ClientClosedError, ConnectionRefusedError, OwnerUnreachableError } from "./errors.js";
import type { OwnerHostInit } from "./host.js";
import { SILENCE_DEADLINE_MS, type StepDown } from "./protocol.js";
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
  /** Bound on the handshake (default `HANDSHAKE_TIMEOUT_MS`). */
  readonly handshakeTimeoutMs?: number;
  /** The silence deadline (default protocol.ts `SILENCE_DEADLINE_MS`; shorter in tests). */
  readonly silenceDeadlineMs?: number;
  /** Listen for owner announcements (default true; tests turn it off to reach the backstop). */
  readonly announcements?: boolean;
}

export type ConnectorStatus = "connecting" | "connected" | "superseded" | "closed";

/** An owner this context is starting: the host-lock wait, then the lock and the handle. */
interface Starting {
  readonly waiting: AbortController;
  handle: OwnerHandle | null;
  lock: HeldLock | null;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export class OwnerConnector {
  readonly #options: ConnectorOptions;
  readonly #client: ProtocolClient;
  #status: ConnectorStatus = "connecting";
  #broker: MessagePort | null = null;
  #nonce = 0;
  readonly #brokerReplies = new Map<
    number,
    { readonly reply: (message: BrokerMessage) => void; readonly fail: (error: Error) => void }
  >();
  /** Rejected by `close()`: every wait in startup and discovery races it, so none outlives close. */
  readonly #closedWait: Promise<never>;
  #rejectClosed: (error: Error) => void = () => undefined;
  #registered: Array<() => void> = [];
  #hosting: { readonly handle: OwnerHandle; readonly lock: HeldLock } | null = null;
  #silenceTimer: ReturnType<typeof setTimeout> | null = null;
  /** Owner announcements on the database's channel (host.ts), or null without BroadcastChannel. */
  readonly #announcements: BroadcastChannel | null;
  /** An owner this context is starting (not yet ready), or the host-lock wait before it. */
  #starting: Starting | null = null;
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
    this.#announcements =
      options.announcements === false || typeof BroadcastChannel === "undefined"
        ? null
        : new BroadcastChannel(skewChannelName(options.database));
    if (this.#announcements !== null)
      this.#announcements.onmessage = (event: MessageEvent) => {
        this.#announced(event.data as SkewSignal);
      };
    this.#closedWait = new Promise<never>((_, reject) => {
      this.#rejectClosed = reject;
    });
    this.#closedWait.catch(() => undefined);
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
    // `freeze` and `pagehide` terminate on every platform. Decide without calling out first:
    // Firefox can abandon a closing page's handler midway (observed inside `context()`), and the
    // `hidden` that follows must then find nothing to flush, not a live owner.
    const action =
      event === "freeze" || event === "pagehide"
        ? "terminate"
        : lifecycleAction(event, this.#options.context().platform);
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
    this.#announcements?.close();
    if (this.#silenceTimer !== null) clearTimeout(this.#silenceTimer);
    this.#silenceTimer = null;
    const starting = this.#starting;
    this.#starting = null;
    if (starting !== null) {
      starting.waiting.abort();
      starting.handle?.terminate();
      starting.lock?.release();
    }
    this.#watch?.stop();
    if (this.#watchIdle !== null) clearTimeout(this.#watchIdle);
    this.#releaseLiveness();
    // Settle every wait of startup and discovery: start() rejects with ClientClosedError.
    this.#rejectClosed(new ClientClosedError());
    for (const { fail } of this.#brokerReplies.values()) fail(new ClientClosedError());
    this.#brokerReplies.clear();
    for (const wake of this.#registered.splice(0)) wake();
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

  /** `promise`, or ClientClosedError as soon as `close()` runs. */
  #untilClosed<T>(promise: Promise<T>): Promise<T> {
    return Promise.race([promise, this.#closedWait]);
  }

  #throwIfClosed(): void {
    if (this.#status === "closed") throw new ClientClosedError();
  }

  async #connect(): Promise<void> {
    const retryMs = this.#options.retryMs ?? 1_000;
    for (;;) {
      this.#throwIfClosed();
      if (this.#status === "superseded") return;
      this.#status = "connecting";
      const answer =
        this.#options.openBroker === null
          ? ({ t: "noOwner" } as const)
          : await this.#untilClosed(this.#ask());
      if (answer.t === "port") {
        if (await this.#attach(answer.port)) return;
        continue;
      }
      if (answer.t === "unreachable") {
        await this.#untilClosed(this.#settleSkew());
        await this.#untilClosed(this.#nextRegistration(retryMs));
        continue;
      }
      // No live owner.
      if (this.#options.startOwner === null || !canHost(this.#options.context())) {
        if (this.#options.openBroker === null)
          throw new OwnerUnreachableError("this context cannot host and has no broker");
        await this.#untilClosed(this.#nextRegistration(retryMs));
        continue;
      }
      if (await this.#host()) return;
    }
  }

  /** Connects the client on `port`; false when it must try again (skew handled). */
  async #attach(port: MessagePort): Promise<boolean> {
    // The owner holds the host lock: a grant to this watch means it died mid-handshake.
    const abandon = new AbortController();
    const watch = watchLock(this.#options.locks, hostLockName(this.#options.database));
    const onGone = () => {
      abandon.abort();
    };
    watch.gone.addEventListener("abort", onGone, { once: true });
    const timer = setTimeout(onGone, this.#options.handshakeTimeoutMs ?? HANDSHAKE_TIMEOUT_MS);
    const done = () => {
      clearTimeout(timer);
      watch.gone.removeEventListener("abort", onGone);
      watch.stop();
    };
    try {
      const welcome = this.#everConnected
        ? await this.#client.reconnect(port, abandon.signal)
        : await this.#client.connect(port, abandon.signal);
      done();
      this.#throwIfClosed();
      this.#everConnected = true;
      this.#ownerProtocol = welcome.protocol;
      this.#status = "connected";
      this.#activity();
      return true;
    } catch (error) {
      done();
      this.#throwIfClosed();
      // The owner died or never answered: start again (a successor is elected).
      if (error instanceof OwnerUnreachableError) return false;
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
      await this.#untilClosed(sleep(10));
      return false;
    }
  }

  /** Queues for the host lock; on the grant starts an owner and connects. False: someone else won. */
  async #host(): Promise<boolean> {
    const database = this.#options.database;
    if (this.#status === "closed") return false;
    const waiting = new AbortController();
    const starting: Starting = {
      waiting,
      handle: null,
      lock: null,
    };
    this.#starting = starting;
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
      if (this.#starting === starting) this.#starting = null;
      return false; // another context registered while we queued, or closed
    } finally {
      this.#registered = this.#registered.filter((wake) => wake !== onRegistered);
    }
    const start = this.#options.startOwner;
    if (start === null || this.#starting !== starting) {
      lock.release();
      return false;
    }
    starting.lock = lock;
    const channel = new MessageChannel();
    const handle = start({
      database,
      build: this.#options.build ?? "",
      clientPort: channel.port2,
      brokerPort: this.#options.openBroker?.() ?? null,
      settings: { ...this.#options.settings, protocol: this.#client.protocol },
    });
    starting.handle = handle;
    try {
      // Raced with close(): an owner that never reports ready must not keep start() pending.
      await this.#untilClosed(handle.ready);
    } catch {
      // close() already terminated it and released the lock if it took this start over.
      if (this.#starting === starting) {
        this.#starting = null;
        handle.terminate();
        lock.release();
      }
      this.#throwIfClosed();
      await this.#untilClosed(sleep(50));
      return false;
    }
    // Closed while it started: `close()` already terminated it and released the lock.
    if (this.#starting !== starting) return false;
    this.#starting = null;
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
        const pending = this.#brokerReplies.get(message.nonce);
        this.#brokerReplies.delete(message.nonce);
        pending?.reply(message);
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
    return new Promise((resolve, reject) => {
      this.#brokerReplies.set(nonce, {
        reply: (message) => {
          if (message.t === "port") resolve({ t: "port", port: message.port });
          else if (message.t === "unreachable") resolve({ t: "unreachable" });
          else resolve({ t: "noOwner" });
        },
        fail: reject,
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
    this.#updateSilence();
  }

  /** An owner announced itself (host.ts). Not ours: ours was replaced; reconnect and resend. */
  #announced(signal: SkewSignal): void {
    if (signal.t !== "owner" || this.#status !== "connected") return;
    const current = this.#client.ownerId;
    if (current === null || signal.ownerId === current) return;
    this.#ownerLeft("closing");
  }

  /**
   * The backstop for an announcement missed or never sent: while requests are in flight, a
   * silence of `SILENCE_DEADLINE_MS` makes the connector ask who holds the host lock. Its owner's
   * holder (`Welcome.lockHolder`): alive, only slow; the count restarts. Anyone else, nobody, or a
   * platform that hides holders: the owner is gone; reconnect and resend.
   */
  #updateSilence(): void {
    const needs = this.#status === "connected" && this.#client.pendingCount > 0;
    if (!needs) {
      if (this.#silenceTimer !== null) clearTimeout(this.#silenceTimer);
      this.#silenceTimer = null;
      return;
    }
    if (this.#silenceTimer === null) this.#armSilence();
  }

  #armSilence(): void {
    const deadline = this.#options.silenceDeadlineMs ?? SILENCE_DEADLINE_MS;
    const wait = Math.max(0, this.#client.lastHeardAt + deadline - Date.now());
    this.#silenceTimer = setTimeout(() => {
      void this.#checkSilence();
    }, wait);
  }

  async #checkSilence(): Promise<void> {
    if (this.#status !== "connected" || this.#client.pendingCount === 0) {
      this.#silenceTimer = null;
      return;
    }
    const deadline = this.#options.silenceDeadlineMs ?? SILENCE_DEADLINE_MS;
    if (Date.now() - this.#client.lastHeardAt < deadline) {
      this.#armSilence();
      return;
    }
    const owner = this.#client.ownerLockHolder;
    const holder = await this.#hostHolder();
    // The connection may have changed while the query ran.
    if (!this.#isConnected()) {
      this.#silenceTimer = null;
      return;
    }
    if (owner !== null && owner !== "" && holder === owner) {
      // The owner still holds the host lock: alive, only slow (throttled, busy).
      this.#client.heard();
      this.#armSilence();
      return;
    }
    this.#silenceTimer = null;
    this.#ownerLeft("closing");
  }

  #isConnected(): boolean {
    return this.#status === "connected";
  }

  /** Who holds the host lock: a client id, `""` when hidden, null when free or unknown. */
  async #hostHolder(): Promise<string | null> {
    const query = this.#options.locks.query?.bind(this.#options.locks);
    if (query === undefined) return null;
    try {
      const state = await query();
      const held = (state.held ?? []).find(
        (lock) => lock.name === hostLockName(this.#options.database),
      );
      return held === undefined ? null : (held.clientId ?? "");
    } catch {
      return null;
    }
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
            // Granted late, after the client went idle: start the idle release now.
            this.#updateLiveness();
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
        // Terminated before it reported ready: it never will.
        rejectReady(new Error("The owner worker was terminated before it was ready"));
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
  // Page Lifecycle: `freeze` fires on the document, `pagehide` on the window.
  target.document.addEventListener("freeze", freeze);
  target.addEventListener("pagehide", pagehide);
  target.document.addEventListener("visibilitychange", visibility);
  return () => {
    target.document.removeEventListener("freeze", freeze);
    target.removeEventListener("pagehide", pagehide);
    target.document.removeEventListener("visibilitychange", visibility);
  };
}
