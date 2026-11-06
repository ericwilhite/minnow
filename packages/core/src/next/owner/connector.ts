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
 *   successor's live host lock. So a new owner announces itself, with its fence epoch, on the
 *   database's BroadcastChannel. A connected client acts on an epoch above its owner's, once the
 *   host lock confirms its owner no longer holds it, and reconnects at once, idle or not.
 * - Rediscovery that finds the very owner it is still connected to resumes that connection: a
 *   new generation would end the old one and roll its transactions back.
 * - Backstop (a missed announcement, a hung port): with requests in flight and nothing heard for
 *   `SILENCE_DEADLINE_MS`, the connector probes the connection (`ping`, answered by `alive` at
 *   once). Answered: alive and only slow (throttled, or a WebKit owner running a long statement
 *   sends no `alive` of its own); the count restarts. Unanswered: it compares the host lock's
 *   holder with its owner's (`Welcome.lockHolder`). Another or none: reconnect and resend. Hidden:
 *   rediscover (the same owner resumes). The same: wait. Each unanswered probe doubles the next
 *   deadline (× 1, 2, 4), and after `SILENT_PROBES_LIMIT` of them the connection is dead
 *   whatever the lock or the broker says (a live owner is not a live connection): a new
 *   generation, resolved through the ledger. Any message from the owner resets the count.
 *
 * ## Liveness
 * The client's liveness lock is held while it has requests in flight or pins (open transactions,
 * cursors), plus `LIVENESS_IDLE_RELEASE_MS` of idle time; holding it is announced with
 * `liveness { held: true }` and its release with `liveness { held: false }` first.
 *
 * ## Lifecycle
 * The host's page relays its lifecycle events here (`lifecycle`): `freeze`/`pagehide` start a
 * bounded owner step-down before termination; `hidden` flushes (desktop) or steps down (mobile).
 *
 * ## Closing
 * `close()` ends everything this context started, wherever it is: a queued host-lock request is
 * withdrawn, an owner still starting is stepped down before termination and host-lock release,
 * and a liveness grant that arrives later is released at once.
 */
import type { HeldLock } from "../storage/fence.js";
import type { ProtocolClient } from "./client.js";
import {
  BROKER_PROTOCOL,
  BROKER_REQUEST_BOUND_MS,
  BROKER_REQUEST_TIMEOUT_MS,
  HANDSHAKE_TIMEOUT_MS,
  LIVENESS_IDLE_RELEASE_MS,
  STEP_DOWN_BOUND_MS,
  canHost,
  clientLockName,
  compareProtocol,
  hostLockName,
  openChannel,
  skewChannelName,
  type BrokerMessage,
  type HostContext,
  type LifecycleEvent,
  type SkewSignal,
} from "./election.js";
import { ClientClosedError, ConnectionRefusedError, OwnerUnreachableError } from "./errors.js";
import type { OwnerHostCommand, OwnerHostInit, OwnerHostStatus } from "./host.js";
import {
  ALIVE_INTERVAL_MS,
  DEFAULT_UNRESPONSIVE_AFTER_MS,
  SILENCE_DEADLINE_MS,
  SILENT_PROBES_LIMIT,
  type StepDown,
} from "./protocol.js";
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
  /**
   * Steps it down gracefully, then stops it (DECISIONS.md "Step-down"): the owner stops starting
   * storage work, lets what is in flight finish, closes its session and releases its lease; only
   * then is the worker terminated, or after `STEP_DOWN_BOUND_MS` regardless. Without it (an owner
   * that cannot step down), `terminate`.
   */
  stepDown?(reason: StepDown["reason"]): Promise<void>;
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
  /** One broker request attempt's timeout (default election.ts `BROKER_REQUEST_TIMEOUT_MS`). */
  readonly brokerTimeoutMs?: number;
  /** Whole discovery/startup attempt bound, also bounding each ask (default 15 s). */
  readonly brokerBoundMs?: number;
  /** Called when connecting gives up (`status` becomes `unreachable`); see `ConnectorStatus`. */
  readonly onUnreachable?: (error: OwnerUnreachableError) => void;
  /** How long `unreachable` may last before waiting requests fail (protocol.ts default). */
  readonly unresponsiveAfterMs?: number;
}

/**
 * - `connecting` has one whole-attempt deadline, `brokerBoundMs` (15 s by default), covering
 *   discovery, repeated negative replies, failed hellos, queued election and worker startup.
 *   Individual broker retries back off within that budget; a reply does not renew it. Expiry
 *   cancels outstanding asks, hellos and startup and rejects start() with OwnerUnreachableError.
 * - `unreachable`: an attempt failed or this context cannot host. onUnreachable is called.
 *   Requests/activity or owner/broker announcements can retry. Waiting SQL has one additional
 *   unresponsiveAfterMs deadline (10 s by default) from the first failure since a successful
 *   connection; retries never renew it. At that deadline waiting requests fail with
 *   OwnerUnreachableError: none only for SQL never posted, unknown for sent unanswered SQL.
 *   Thus a continuously disconnected request waits at most one attempt budget plus that grace,
 *   conditional on event-loop execution. New calls after give-up fail immediately with none.
 *   A successful connection or close cancels the grace. Connected execution is not a query timeout.
 * - `superseded`: pending SQL fails immediately with the same sent/never-sent effect rule;
 *   reloading is required. Reporting a reload prompt must not leave queued calls pending.
 */
export type ConnectorStatus = "connecting" | "connected" | "unreachable" | "superseded" | "closed";

/** An owner this context is starting: the host-lock wait, then the lock and the handle. */
interface Starting {
  readonly waiting: AbortController;
  handle: OwnerHandle | null;
  lock: HeldLock | null;
}

/** AbortSignal.reason is untyped at the platform boundary; preserve typed owner/close errors. */
function attemptFailure(signal: AbortSignal | undefined): Error {
  const reason: unknown = signal?.reason;
  return reason instanceof Error ? reason : new OwnerUnreachableError("connection attempt stopped");
}

export class OwnerConnector {
  readonly #options: ConnectorOptions;
  readonly #client: ProtocolClient;
  #status: ConnectorStatus = "connecting";
  #broker: MessagePort | null = null;
  /** Earlier broker ports, kept open until no request may still be answered on them. */
  readonly #oldBrokers: MessagePort[] = [];
  /** The broker instance id last announced on any of our ports. */
  #brokerInstance: string | null = null;
  #nonce = 0;
  readonly #brokerReplies = new Map<
    number,
    {
      readonly reply: (message: BrokerMessage) => void;
      readonly fail: (error: Error) => void;
      /** Sends the request again now on `port` (a replaced broker), its timer untouched. */
      readonly resend: (port: MessagePort) => void;
    }
  >();
  /** Rejected by `close()`: every wait in startup and discovery races it, so none outlives close. */
  readonly #closedWait: Promise<never>;
  #rejectClosed: (error: Error) => void = () => undefined;
  #registered: Array<() => void> = [];
  #hosting: { readonly handle: OwnerHandle; readonly lock: HeldLock } | null = null;
  #silenceTimer: ReturnType<typeof setTimeout> | null = null;
  /** The unresponsive deadline, armed when connecting first gives up, until a connection. */
  #unresponsiveTimer: ReturnType<typeof setTimeout> | null = null;
  /**
   * The silence backstop's state for the current connection. `rounds`: probes unanswered since the
   * owner last spoke (at `messageAt`, the client's `lastMessageAt`). `roundAt`: when the last
   * round ended, the earliest point the next deadline counts from. `probeAt`: a `ping` awaiting
   * its answer.
   */
  #silence = { rounds: 0, messageAt: 0, roundAt: 0, probeAt: null as number | null };
  /** The next connection must be a new generation, never a resume (the old one is dead). */
  #fresh = false;
  /** Owner announcements on the database's channel (host.ts), or null without BroadcastChannel. */
  readonly #announcements: BroadcastChannel | null;
  /** An owner this context is starting (not yet ready), or the host-lock wait before it. */
  #starting: Starting | null = null;
  #ownerProtocol: number | null = null;
  #connecting: Promise<void> | null = null;
  #attempt: AbortController | null = null;
  #attemptWait: Promise<never> | null = null;
  #attemptTimer: ReturnType<typeof setTimeout> | null = null;
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
      options.announcements === false ? null : openChannel(skewChannelName(options.database));
    if (this.#announcements !== null)
      this.#announcements.onmessage = (event: MessageEvent) => {
        void this.#announced(event.data as SkewSignal);
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
    if (this.#connecting !== null) return this.#connecting;
    const attempt = new AbortController();
    this.#attempt = attempt;
    this.#attemptWait = new Promise<never>((_, reject) => {
      attempt.signal.addEventListener("abort", () => reject(attemptFailure(attempt.signal)), {
        once: true,
      });
    });
    this.#attemptWait.catch(() => undefined);
    const ms = this.#options.brokerBoundMs ?? BROKER_REQUEST_BOUND_MS;
    const timer = setTimeout(
      () =>
        attempt.abort(
          new OwnerUnreachableError(`discovery and startup did not finish within ${String(ms)} ms`),
        ),
      ms,
    );
    this.#attemptTimer = timer;
    const previousHosting = this.#hosting;
    this.#connecting = this.#connect()
      .catch((error: unknown) => {
        const starting = this.#starting;
        this.#starting = null;
        if (starting !== null) {
          starting.waiting.abort();
          if (starting.handle !== null && starting.lock !== null)
            this.#stepDown({ handle: starting.handle, lock: starting.lock }, "closing");
          else starting.lock?.release();
        }
        if (this.#hosting !== null && this.#hosting !== previousHosting) {
          const hosting = this.#hosting;
          this.#hosting = null;
          this.#stepDown(hosting, "closing");
        }
        if (this.#status === "connecting") {
          const failure =
            error instanceof OwnerUnreachableError
              ? error
              : new OwnerUnreachableError(
                  error instanceof Error ? error.message : "discovery or startup failed",
                );
          this.#status = "unreachable";
          this.#armUnresponsive();
          this.#options.onUnreachable?.(failure);
          throw failure;
        }
        throw error;
      })
      .finally(() => {
        clearTimeout(timer);
        if (this.#attemptTimer === timer) this.#attemptTimer = null;
        this.#attempt = null;
        this.#attemptWait = null;
        this.#connecting = null;
      });
    return this.#connecting;
  }

  /**
   * The unresponsive deadline: armed once, at the first `unreachable` since the last connection;
   * retries in between do not restart it. A connection clears it (`#attach`), and so does close.
   */
  #armUnresponsive(): void {
    if (this.#unresponsiveTimer !== null || this.#client.gaveUp) return;
    const ms = this.#options.unresponsiveAfterMs ?? DEFAULT_UNRESPONSIVE_AFTER_MS;
    this.#unresponsiveTimer = setTimeout(() => {
      this.#unresponsiveTimer = null;
      if (this.#status === "closed" || this.#status === "connected") return;
      this.#client.giveUp(`no database owner was reachable for ${String(ms)} ms`);
    }, ms);
  }

  #disarmUnresponsive(): void {
    if (this.#unresponsiveTimer !== null) clearTimeout(this.#unresponsiveTimer);
    this.#unresponsiveTimer = null;
  }

  /** Connects again if connecting gave up (`unreachable`): something says it may work now. */
  #retryIfUnreachable(): void {
    if (this.#status === "unreachable") void this.start().catch(() => undefined);
  }

  /** Relays a page lifecycle event to the owner this context hosts. */
  lifecycle(event: LifecycleEvent): void {
    if (event === "freeze" || event === "pagehide") {
      // A worker can already own storage while its ready message is still pending. Retire the
      // pending election synchronously, then let that worker release before terminating it.
      const starting = this.#starting;
      this.#starting = null;
      starting?.waiting.abort();
      if (starting !== null)
        this.#attempt?.abort(new OwnerUnreachableError(`owner startup interrupted by ${event}`));
      let hosting = this.#hosting;
      if (hosting === null && starting !== null) {
        const { handle, lock } = starting;
        if (handle !== null && lock !== null) hosting = { handle, lock };
      }
      this.#hosting = null;
      if (hosting !== null) this.#stepDown(hosting, event);
      else starting?.lock?.release();
      return;
    }
    const hosting = this.#hosting;
    if (hosting === null) return;
    hosting.handle.lifecycle(event);
  }

  /**
   * Stops a hosted owner: gracefully and bounded where the handle can (WebKit leaves IndexedDB
   * wedged for the browser session when a worker dies with transactions in flight: DECISIONS.md
   * "Step-down"), else at once. The page's own host lock goes after.
   */
  #stepDown(
    hosting: { readonly handle: OwnerHandle; readonly lock: HeldLock },
    reason: StepDown["reason"],
  ): void {
    const stepping = hosting.handle.stepDown?.(reason);
    if (stepping === undefined) {
      hosting.handle.terminate();
      hosting.lock.release();
      return;
    }
    void stepping.then(
      () => hosting.lock.release(),
      (error: unknown) => {
        console.error("Owner step-down failed; reload this page", error);
        hosting.handle.terminate();
        hosting.lock.release();
      },
    );
  }

  close(): void {
    if (this.#status === "closed") return;
    this.#status = "closed";
    if (this.#attemptTimer !== null) clearTimeout(this.#attemptTimer);
    this.#attemptTimer = null;
    this.#attempt?.abort(new ClientClosedError());
    this.#announcements?.close();
    if (this.#silenceTimer !== null) clearTimeout(this.#silenceTimer);
    this.#silenceTimer = null;
    this.#disarmUnresponsive();
    const starting = this.#starting;
    this.#starting = null;
    if (starting !== null) {
      starting.waiting.abort();
      if (starting.handle !== null && starting.lock !== null)
        this.#stepDown({ handle: starting.handle, lock: starting.lock }, "closing");
      else starting.lock?.release();
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
    if (hosting !== null) this.#stepDown(hosting, "closing");
    this.#broker?.close();
    for (const port of this.#oldBrokers.splice(0)) port.close();
  }

  // ---- connecting ----

  /** `promise`, or ClientClosedError as soon as `close()` runs. */
  #untilClosed<T>(promise: Promise<T>): Promise<T> {
    return Promise.race([
      promise,
      this.#closedWait,
      ...(this.#attemptWait === null ? [] : [this.#attemptWait]),
    ]);
  }

  #throwIfClosed(): void {
    if (this.#status === "closed") throw new ClientClosedError();
    if (this.#attempt?.signal.aborted) throw attemptFailure(this.#attempt.signal);
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
        // The same live owner we are still connected to (D1 b): resume, never a new generation,
        // which would end the old one and roll its transactions back.
        if (
          answer.ownerId === this.#client.ownerId &&
          this.#client.connected &&
          !this.#client.gaveUp &&
          !this.#fresh
        ) {
          answer.port.postMessage({ t: "bye" });
          answer.port.close();
          this.#status = "connected";
          this.#disarmUnresponsive();
          // The next deadline counts from now, with the backoff the rounds have reached: a resume
          // is not an answer (B5-p2d review 2, D2).
          this.#silence.roundAt = Date.now();
          this.#activity();
          return;
        }
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
    const signal = this.#attempt?.signal;
    signal?.addEventListener("abort", onGone, { once: true });
    if (signal?.aborted) onGone();
    const timer = setTimeout(onGone, this.#options.handshakeTimeoutMs ?? HANDSHAKE_TIMEOUT_MS);
    const done = () => {
      clearTimeout(timer);
      watch.gone.removeEventListener("abort", onGone);
      signal?.removeEventListener("abort", onGone);
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
      this.#fresh = false;
      this.#disarmUnresponsive();
      this.#silence = {
        rounds: 0,
        messageAt: this.#client.lastMessageAt,
        roundAt: 0,
        probeAt: null,
      };
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
      await this.#untilClosed(this.#nextRegistration(10));
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
    const signal = this.#attempt?.signal;
    signal?.addEventListener("abort", onRegistered, { once: true });
    if (signal?.aborted) onRegistered();
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
      signal?.removeEventListener("abort", onRegistered);
    }
    const start = this.#options.startOwner;
    if (start === null || this.#starting !== starting || signal?.aborted) {
      lock.release();
      this.#throwIfClosed();
      return false;
    }
    starting.lock = lock;
    const channel = new MessageChannel();
    let handle: OwnerHandle;
    try {
      handle = start({
        database,
        build: this.#options.build ?? "",
        clientPort: channel.port2,
        brokerPort: this.#options.openBroker?.() ?? null,
        settings: { ...this.#options.settings, protocol: this.#client.protocol },
      });
    } catch (error) {
      channel.port1.close();
      channel.port2.close();
      throw error; // start() releases the pending election and arms the waiting-SQL deadline.
    }
    starting.handle = handle;
    try {
      // Raced with close(): an owner that never reports ready must not keep start() pending.
      await this.#untilClosed(handle.ready);
    } catch {
      // close()/lifecycle() already took responsibility for release if they retired this start.
      if (this.#starting === starting) {
        this.#starting = null;
        this.#stepDown({ handle, lock }, "closing");
      }
      channel.port1.close();
      this.#throwIfClosed();
      await this.#untilClosed(this.#nextRegistration(50));
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
    return this.#broker ?? this.#freshBrokerPort();
  }

  /** Opens a new broker port and makes it the current one; the old one stays open until idle. */
  #freshBrokerPort(): MessagePort {
    const open = this.#options.openBroker;
    if (open === null) throw new OwnerUnreachableError("no broker");
    const port = open();
    port.onmessage = (event: MessageEvent) => {
      this.#brokerMessage(port, event.data as BrokerMessage);
    };
    if (this.#broker !== null) this.#oldBrokers.push(this.#broker);
    this.#broker = port;
    return port;
  }

  #brokerMessage(port: MessagePort, message: BrokerMessage): void {
    if (this.#status === "closed") return;
    if (message.t === "instance") {
      // Every request still unanswered goes again to the instance that announced itself here: a
      // port opened while a broker was being replaced may have had its request dropped, and
      // its first announcement is the new instance's, so no id change would show it. The
      // broker answers a request id once, so the repeat costs one message.
      for (const pending of this.#brokerReplies.values()) pending.resend(port);
      const previous = this.#brokerInstance;
      this.#brokerInstance = message.instance;
      if (previous === null || previous === message.instance) return;
      // A replaced broker also lost every registration and everyone waiting for one.
      for (const wake of this.#registered.splice(0)) wake();
      this.#retryIfUnreachable();
      return;
    }
    if (message.t === "registered") {
      if (message.db !== this.#options.database) return;
      for (const wake of this.#registered.splice(0)) wake();
      this.#retryIfUnreachable();
      return;
    }
    if ("nonce" in message) {
      const pending = this.#brokerReplies.get(message.nonce);
      if (pending !== undefined) {
        pending.reply(message);
        return;
      }
      // A second answer to a request already answered: its connection is not wanted.
      if (message.t === "port") {
        message.port.postMessage({ t: "bye" });
        message.port.close();
      }
    }
  }

  /**
   * Asks the broker for the owner. Bounded: each attempt waits `brokerTimeoutMs`, doubling; the
   * first retry goes on the same port, later ones on fresh ports; past `brokerBoundMs` it fails
   * with `OwnerUnreachableError`. Every attempt carries the same request id, so the broker
   * answers it once. Every timer ends with the request (answer, failure, `close()`).
   */
  #ask(): Promise<
    | { readonly t: "port"; readonly port: MessagePort; readonly ownerId: string }
    | { readonly t: "noOwner" }
    | { readonly t: "unreachable" }
  > {
    const nonce = ++this.#nonce;
    const timeoutMs = this.#options.brokerTimeoutMs ?? BROKER_REQUEST_TIMEOUT_MS;
    const boundMs = this.#options.brokerBoundMs ?? BROKER_REQUEST_BOUND_MS;
    const request = {
      t: "connect",
      v: BROKER_PROTOCOL,
      db: this.#options.database,
      clientId: this.#client.clientId,
      nonce,
    } satisfies BrokerMessage;
    return new Promise((resolve, reject) => {
      let attempt = 0;
      // When the next attempt is due, from the first one: the schedule, not the clock, decides
      // how many attempts there are (a timer may fire a millisecond early or late).
      let plannedAt = 0;
      let timer: ReturnType<typeof setTimeout> | null = null;
      const signal = this.#attempt?.signal;
      const onAbort = () => {
        finish();
        reject(attemptFailure(signal));
      };
      const finish = () => {
        signal?.removeEventListener("abort", onAbort);
        if (timer !== null) clearTimeout(timer);
        timer = null;
        this.#brokerReplies.delete(nonce);
        // Old ports may still carry an answer only while some request is unanswered.
        if (this.#brokerReplies.size === 0)
          for (const old of this.#oldBrokers.splice(0)) old.close();
      };
      const attemptNow = () => {
        timer = null;
        if (this.#status === "closed") {
          finish();
          reject(new ClientClosedError());
          return;
        }
        if (attempt > 0 && plannedAt >= boundMs) {
          finish();
          reject(
            new OwnerUnreachableError(
              `the broker did not answer within ${String(boundMs)} ms (${String(attempt)} attempts)`,
            ),
          );
          return;
        }
        attempt++;
        let port: MessagePort;
        try {
          port = attempt <= 2 ? this.#brokerPort() : this.#freshBrokerPort();
        } catch (error) {
          finish();
          reject(error instanceof Error ? error : new Error(String(error)));
          return;
        }
        port.postMessage(request);
        const wait = Math.min(timeoutMs * 2 ** (attempt - 1), boundMs - plannedAt);
        plannedAt += wait;
        timer = setTimeout(attemptNow, Math.max(0, wait));
      };
      this.#brokerReplies.set(nonce, {
        reply: (message) => {
          finish();
          if (message.t === "port")
            resolve({ t: "port", port: message.port, ownerId: message.ownerId });
          else if (message.t === "unreachable") resolve({ t: "unreachable" });
          else resolve({ t: "noOwner" });
        },
        fail: (error) => {
          finish();
          reject(error);
        },
        resend: (port) => {
          port.postMessage(request);
        },
      });
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
      else attemptNow();
    });
  }

  /** Resolves on the next owner registration, or after `ms`. */
  #nextRegistration(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const signal = this.#attempt?.signal;
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", done);
        this.#registered = this.#registered.filter((wake) => wake !== done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.#registered.push(done);
      signal?.addEventListener("abort", done, { once: true });
      if (signal?.aborted) done();
    });
  }

  // ---- skew ----

  #supersedeOlderOwner(): void {
    const channel = openChannel(skewChannelName(this.#options.database));
    if (channel === null) return;
    channel.postMessage({ t: "supersede", protocol: this.#client.protocol } satisfies SkewSignal);
    channel.close();
  }

  /** Asks who owns; supersedes an older owner, or is superseded by a newer one. */
  async #settleSkew(): Promise<void> {
    const signal = this.#attempt?.signal;
    if (signal === undefined) return;
    this.#throwIfClosed();
    const channel = openChannel(skewChannelName(this.#options.database));
    if (channel === null) return;
    const owner = await new Promise<number | null>((resolve) => {
      let finished = false;
      const finish = (owner: number | null) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        signal.removeEventListener("abort", cancel);
        channel.close();
        resolve(owner);
      };
      const cancel = () => finish(null);
      const timer = setTimeout(cancel, 200);
      // close() aborts the attempt synchronously. Cancelling its outer race alone would leave
      // this timer and channel alive until the discovery timeout.
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) {
        cancel();
        return;
      }
      channel.onmessage = (event: MessageEvent) => {
        const message = event.data as SkewSignal;
        if (message.t === "owner") finish(message.protocol);
      };
      channel.postMessage({ t: "whoOwns", protocol: this.#client.protocol } satisfies SkewSignal);
    });
    if (owner === null || signal.aborted || this.#status === "closed") return;
    const relation = compareProtocol(this.#client.protocol, owner);
    if (relation === "clientNewer") this.#supersedeOlderOwner();
    else if (relation === "clientOlder") this.#superseded();
  }

  #superseded(): void {
    if (this.#status === "superseded") return;
    this.#status = "superseded";
    this.#watch?.stop();
    if (this.#silenceTimer !== null) clearTimeout(this.#silenceTimer);
    this.#silenceTimer = null;
    this.#disarmUnresponsive();
    // A reload prompt is not completion of queued SQL. Preserve sent/never-sent effects even
    // when no compatible connection can ever be made by this client version.
    this.#client.giveUp("a newer database owner superseded this client; reload this page");
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
    // A new request while connecting had given up: try again (bounded, as always).
    this.#retryIfUnreachable();
    this.#updateWatch();
    this.#updateLiveness();
    this.#updateSilence();
  }

  /** An owner announced itself (host.ts). Not ours: ours was replaced; reconnect and resend. */
  /**
   * An owner announced itself (host.ts). A hint, never proof (B5-p2d review 1, D1): only an epoch
   * above our owner's counts, and then only once the host lock confirms our owner no longer holds
   * it. A stale, repeated or forged announcement leaves a live connection, and its transactions,
   * alone.
   */
  async #announced(signal: SkewSignal): Promise<void> {
    if (signal.t === "owner") this.#retryIfUnreachable();
    if (signal.t !== "owner" || !this.#isConnected()) return;
    const current = this.#client.ownerId;
    if (current === null || signal.ownerId === current) return;
    if ((signal.epoch ?? 0) <= this.#client.ownerEpoch) return;
    const generation = this.#client.generation;
    const owner = this.#client.ownerLockHolder;
    const holder = await this.#hostHolder();
    if (!this.#isConnected() || this.#client.generation !== generation) return;
    // Our owner still holds the host lock: the announcement is stale or forged.
    if (owner !== null && owner !== "" && holder === owner) return;
    this.#ownerLeft("closing");
  }

  /**
   * The backstop for an announcement missed or never sent, or a connection that ended without a
   * word: while requests are in flight, a silence past the deadline sends a `ping`; an unanswered
   * one asks who holds the host lock. See "Losing the owner" for the outcomes and the backoff.
   */
  #updateSilence(): void {
    const needs = this.#status === "connected" && this.#client.pendingCount > 0;
    if (!needs) {
      if (this.#silenceTimer !== null) clearTimeout(this.#silenceTimer);
      this.#silenceTimer = null;
      this.#silence.probeAt = null;
      return;
    }
    if (this.#silenceTimer === null) this.#armSilence(this.#silenceDueAt() - Date.now());
  }

  #silenceDeadlineMs(): number {
    return this.#options.silenceDeadlineMs ?? SILENCE_DEADLINE_MS;
  }

  /** The deadline, doubled per unanswered probe (× 1, 2, 4, capped), from the later baseline. */
  #silenceDueAt(): number {
    const { rounds, roundAt } = this.#silence;
    const from = Math.max(this.#client.lastHeardAt, roundAt);
    return from + this.#silenceDeadlineMs() * 2 ** Math.min(rounds, 2);
  }

  #armSilence(wait: number): void {
    this.#silenceTimer = setTimeout(
      () => {
        void this.#checkSilence();
      },
      Math.max(0, wait),
    );
  }

  async #checkSilence(): Promise<void> {
    this.#silenceTimer = null;
    if (this.#status !== "connected" || this.#client.pendingCount === 0) {
      this.#silence.probeAt = null;
      return;
    }
    // The owner spoke since the count started (a probe answered included): start again.
    if (this.#client.lastMessageAt !== this.#silence.messageAt) {
      this.#silence = {
        rounds: 0,
        messageAt: this.#client.lastMessageAt,
        roundAt: 0,
        probeAt: null,
      };
    }
    const state = this.#silence;
    const now = Date.now();
    if (state.probeAt === null) {
      const due = this.#silenceDueAt();
      if (now < due) {
        this.#armSilence(due - now);
        return;
      }
      // Silent past the deadline: is the connection still there?
      state.probeAt = now;
      this.#client.ping();
      this.#armSilence(Math.min(this.#silenceDeadlineMs(), ALIVE_INTERVAL_MS));
      return;
    }
    // The probe went unanswered.
    state.probeAt = null;
    state.rounds++;
    state.roundAt = now;
    if (state.rounds >= SILENT_PROBES_LIMIT) {
      // Dead, whoever holds the lock: a new generation, never a resume.
      this.#fresh = true;
      this.#ownerLeft("closing");
      return;
    }
    const owner = this.#client.ownerLockHolder;
    const generation = this.#client.generation;
    const holder = await this.#hostHolder();
    // The connection may have changed while the query ran (D1 d): same connection only.
    if (!this.#isConnected() || this.#client.generation !== generation) return;
    if (owner !== null && owner !== "" && holder === owner) {
      // The owner still holds the host lock: alive; wait, backing off.
      this.#updateSilence();
      return;
    }
    // Another holder, none, or hidden: rediscover (the same owner resumes, with the backoff).
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
    let finished = false;
    let stepping: Promise<void> | null = null;
    worker.onmessage = (event: MessageEvent) => {
      const status = event.data as OwnerHostStatus;
      switch (status.t) {
        case "minnow:ready":
          resolveReady(status.ownerId);
          break;
        case "minnow:failed":
          rejectReady(new Error("The owner worker could not start"));
          break;
        case "minnow:stopped":
          if (finished) return;
          finished = true;
          if (status.reloadSuggested)
            console.error("Owner release was unsettled; reload this page");
          rejectReady(new Error("The owner stopped before it was ready"));
          resolveStopped(status.reason);
          worker.terminate();
      }
    };
    worker.onerror = () => {
      if (!finished) worker.terminate();
      finished = true;
      rejectReady(new Error("The owner worker failed"));
      resolveStopped("closing");
    };
    const transfer: Transferable[] = [init.clientPort];
    if (init.brokerPort !== null) transfer.push(init.brokerPort);
    worker.postMessage({ ...init, t: "minnow:host" } satisfies OwnerHostInit, transfer);
    void stopped.then(() => {
      finished = true;
    });
    const terminate = () => {
      if (finished) return;
      finished = true;
      worker.terminate();
      // Terminated before it reported ready: it never will.
      rejectReady(new Error("The owner worker was terminated before it was ready"));
      resolveStopped("pagehide");
    };
    return {
      ready,
      stopped,
      lifecycle: (event) => {
        worker.postMessage({ t: "minnow:lifecycle", event, platform });
      },
      terminate,
      stepDown: (reason) => {
        if (stepping !== null) return stepping;
        if (finished) return Promise.resolve();
        stepping = (async () => {
          // The worker drains, closes, releases and reports `minnow:stopped` (then terminated
          // above); a worker that does not answer in time is terminated anyway.
          worker.postMessage({ t: "minnow:stepDown", reason } satisfies OwnerHostCommand);
          let bound: ReturnType<typeof setTimeout> | undefined;
          const outcome = await Promise.race([
            stopped.then(() => "stopped" as const),
            new Promise<"deadline">((resolve) => {
              bound = setTimeout(() => resolve("deadline"), STEP_DOWN_BOUND_MS);
            }),
          ]);
          clearTimeout(bound);
          if (outcome === "deadline") {
            console.error("Owner step-down did not settle; reload this page");
            terminate();
          }
        })();
        return stepping;
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
