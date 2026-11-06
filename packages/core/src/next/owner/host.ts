/**
 * The owner runtime: what runs inside the owner worker (a dedicated worker a page started, or a
 * dedicated worker hosting in place). It takes the fence, opens the engine, serves clients and
 * steps down (election.ts).
 *
 * ## Startup (rulings R13, R16.6)
 * 1. `acquireFenced`: the fence lock `minnow:<db>`, then the engine's sync handles with bounded
 *    retry (the lock frees before a dead owner's handles).
 * 2. `factory.open(fence)`: store session → extent space → write path (one log scan and replay)
 *    → spill sweep. With stand-ins it loads their durable file.
 * 3. The host lock, stolen from the page that won the election: from now on the owner holds it.
 * 4. The protocol server; the host page's own client connects on `clientPort`.
 * 5. Arm registration with the broker (its instance announcement starts the acknowledged retry
 *    loop); answer on the skew channel, and send an `owner` announcement there,
 *    so clients of a previous owner reconnect at once. The registration is repeated whenever the
 *    broker port announces an instance id other than the last one seen (a replaced broker starts
 *    empty, election.ts "Reaching the owner"), but only while this owner holds both its locks and
 *    has not stopped: a stale owner never registers again. Each send awaits an acknowledgement
 *    for that instance; retries back off within 15 s. Terminal failure is reported and steps the
 *    owner down. Repeated same-instance announcements cannot reset this budget.
 * 6. `minnow:ready` to the page (local service is ready; broker confirmation is asynchronous).
 *
 * A newer owner may steal the fence lock while startup awaits (steps 2 and 3, and the lock query
 * that names the holder for the broker). After every await the host checks; a lost fence tears
 * everything down (handles, locks, engine) and startup fails with `OwnershipLostError` before
 * anything is served. Steps 4 to 6 run with no await between them.
 *
 * ## Stepping down
 * - `terminate` (`freeze`, `pagehide`, a stolen lock): synchronous — the server ends every
 *   connection (requests past the commit boundary complete in memory but their replies go
 *   nowhere), the fence's handles close, the lock is released. A page host also terminates the
 *   worker. Relaxed commits acknowledged since the last flush may be lost, as on power loss.
 * - `stepDown` (`hidden` on mobile, superseded by a newer build): the server tells every client
 *   (`stepDown`), the engine flushes and closes, then the fence closes and the lock is released.
 * - `flush` (`hidden` elsewhere): `engine.flushPending()`; ownership stays.
 * - A stolen fence lock closes the handles at once (storage probe: the steal alone does not stop a
 *   hung owner; only closing the handle does).
 */
import { OwnershipLostError } from "../storage/contract.js";
import { acquireFenced, type HeldLock } from "../storage/fence.js";
import {
  BROKER_PROTOCOL,
  BROKER_REQUEST_TIMEOUT_MS,
  BROKER_REQUEST_BOUND_MS,
  hostLockName,
  lifecycleAction,
  openChannel,
  ownerLockName,
  randomId,
  skewChannelName,
  type BrokerMessage,
  type LifecycleEvent,
  type Platform,
  type SkewSignal,
} from "./election.js";
import type { OwnerEngine } from "./engine.js";
import { OwnerUnreachableError, toWireError } from "./errors.js";
import { PROTOCOL_VERSION, type StepDown } from "./protocol.js";
import { OwnerServer } from "./server.js";
import { watchLock, webLocks, type WebLockManager } from "./web-locks.js";

/** The first message a page posts to the owner worker. */
export interface OwnerHostInit {
  readonly t: "minnow:host";
  readonly database: string;
  readonly build: string;
  /** The host page's own client connection. */
  readonly clientPort: MessagePort;
  /** A port to the broker SharedWorker, or null without one. */
  readonly brokerPort: MessagePort | null;
  /** Engine settings (durability, idle timeout; stand-in knobs in tests). Structured-clone data. */
  readonly settings: Readonly<Record<string, unknown>>;
}

/** Messages the host posts to its page. */
export type OwnerHostStatus =
  | { readonly t: "minnow:ready"; readonly ownerId: string }
  | { readonly t: "minnow:failed"; readonly error: ReturnType<typeof toWireError> }
  | { readonly t: "minnow:stopped"; readonly reason: StepDown["reason"] };

/** Messages the page posts to its owner worker after `minnow:host`. */
export type OwnerHostCommand =
  | { readonly t: "minnow:lifecycle"; readonly event: LifecycleEvent; readonly platform: Platform }
  | { readonly t: "minnow:stepDown" };

/** How the host opens the engine on the fence; the composition root supplies it. */
export interface OwnerEngineFactory<F> {
  /** Opens the fence: the store's sync handles. Busy errors are retried by `acquireFenced`. */
  openFence(database: string): F | Promise<F>;
  closeFence(fence: F): void;
  /** Opens the engine on the fence (recovery runs here). */
  open(fence: F, settings: Readonly<Record<string, unknown>>): OwnerEngine | Promise<OwnerEngine>;
  /**
   * The fence's epoch: strictly increasing across acquisitions of one database (storage/contract.ts
   * `OwnerLease.epoch`). Announced to clients, which act only on a newer one. Absent: 1.
   */
  epochOf?(fence: F): number;
}

export interface OwnerHost {
  readonly ownerId: string;
  readonly server: OwnerServer;
  readonly engine: OwnerEngine;
  /** Confirmation/failure for the current broker instance; null instance when no broker is used. */
  readonly registration: {
    readonly instance: string | null;
    readonly confirmed: boolean;
    readonly error: OwnerUnreachableError | null;
  };
  /** Handles a lifecycle event the page relayed. */
  lifecycle(event: LifecycleEvent, platform: Platform): void;
  /** Steps down; `graceful` flushes and closes the engine first. */
  stop(reason: StepDown["reason"], graceful: boolean): Promise<void>;
}

export interface OwnerHostDeps {
  readonly locks: WebLockManager;
  /** Posts a status message to the page (`self.postMessage` in the worker). */
  readonly post: (status: OwnerHostStatus) => void;
  /** Bound for taking the fence (default `acquireFenced`'s). */
  readonly acquireTimeoutMs?: number;
  /** Registration retry interval and whole per-instance budget (default 1 s and 15 s). */
  readonly registrationTimeoutMs?: number;
  readonly registrationBoundMs?: number;
}

export async function startOwnerHost<F>(
  init: OwnerHostInit,
  factory: OwnerEngineFactory<F>,
  deps: OwnerHostDeps,
): Promise<OwnerHost> {
  const database = init.database;
  const protocol =
    typeof init.settings.protocol === "number" ? init.settings.protocol : PROTOCOL_VERSION;
  let acquired: Awaited<ReturnType<typeof acquireFenced<F>>>;
  let engine: OwnerEngine;
  try {
    acquired = await acquireFenced(
      webLocks(deps.locks),
      ownerLockName(database),
      () => factory.openFence(database),
      (fence) => {
        factory.closeFence(fence);
      },
      deps.acquireTimeoutMs === undefined ? {} : { timeoutMs: deps.acquireTimeoutMs },
    );
  } catch (error) {
    deps.post({ t: "minnow:failed", error: toWireError(error) });
    throw error;
  }
  const { lock, fence } = acquired;
  // Startup awaits three times (engine open, host-lock steal, lock query). A newer owner may steal
  // the fence lock during any of them: after each, check, and if it was lost tear everything
  // down before serving anything (the handles first: only closing them stops this owner).
  let engineOpened: OwnerEngine | null = null;
  let hostLock: HeldLock | null = null;
  const abandon = (error: unknown): never => {
    factory.closeFence(fence);
    lock.release();
    hostLock?.release();
    if (engineOpened !== null) void engineOpened.close().catch(() => undefined);
    deps.post({ t: "minnow:failed", error: toWireError(error) });
    throw error;
  };
  const checkFence = () => {
    if (lock.lost.aborted) abandon(new OwnershipLostError(0));
  };
  try {
    engine = await factory.open(fence, init.settings);
  } catch (error) {
    return abandon(error);
  }
  engineOpened = engine;
  checkFence();

  // The host lock moves to the owner itself: the page held it only to serialize the election.
  // Held here, any death of the owner (terminate, crash, closed tab) frees it at once, so waiting
  // clients elect a successor (election.ts "Locks").
  try {
    hostLock = await webLocks(deps.locks).request(hostLockName(database), { steal: true });
  } catch (error) {
    return abandon(error);
  }
  checkFence();

  // The context id the platform gives this owner's host lock, for the broker's liveness check.
  const lockHolder =
    (await deps.locks.query?.().then(
      (state) =>
        (state.held ?? []).find((held) => held.name === hostLockName(database))?.clientId ?? null,
      () => null,
    )) ?? null;
  checkFence();
  const heldHostLock = hostLock;

  // No await from here to `minnow:ready`: the lock checks above cover the whole startup.
  const ownerId = randomId();
  const epoch = factory.epochOf?.(fence) ?? 1;
  const idle = init.settings.transactionIdleTimeoutMs;
  const server = new OwnerServer(engine, {
    ownerId,
    epoch,
    build: init.build,
    protocol,
    ...(typeof idle === "number" ? { transactionIdleTimeoutMs: idle } : {}),
    ...(typeof init.settings.aliveIntervalMs === "number"
      ? { aliveIntervalMs: init.settings.aliveIntervalMs }
      : {}),
    watchLiveness: (name) => watchLock(deps.locks, name),
    lockHolder,
  });
  server.accept(init.clientPort);

  let stopped: Promise<void> | null = null;
  const broker = init.brokerPort;
  let instance: string | null = null;
  let confirmedInstance: string | null = null;
  let registrationError: OwnerUnreachableError | null = null;
  let registrationTimer: ReturnType<typeof setTimeout> | null = null;
  let registrationDeadline: ReturnType<typeof setTimeout> | null = null;
  const cancelRegistration = () => {
    if (registrationTimer !== null) clearTimeout(registrationTimer);
    registrationTimer = null;
    if (registrationDeadline !== null) clearTimeout(registrationDeadline);
    registrationDeadline = null;
  };
  if (broker !== null) {
    const fenced = () => stopped === null && !lock.lost.aborted && !heldHostLock.lost.aborted;
    const beginRegistration = (target: string) => {
      cancelRegistration();
      instance = target;
      confirmedInstance = null;
      registrationError = null;
      const timeout = Math.max(1, deps.registrationTimeoutMs ?? BROKER_REQUEST_TIMEOUT_MS);
      const bound = Math.max(1, deps.registrationBoundMs ?? BROKER_REQUEST_BOUND_MS);
      let attempts = 0;
      const fail = () => {
        if (instance !== target || confirmedInstance === target || stopped !== null) return;
        if (!fenced()) {
          void stop("lockLost", false);
          return;
        }
        cancelRegistration();
        registrationError = new OwnerUnreachableError(
          `broker ${target} did not acknowledge registration within ${String(bound)} ms; reload if it is an older broker`,
        );
        console.error("Owner broker registration failed", registrationError);
        deps.post({ t: "minnow:failed", error: toWireError(registrationError) });
        // Do not strand a live, unreachable owner holding both locks after terminal failure.
        void stop("closing", true);
      };
      // Independent of retries and repeated announcements, including a lost acknowledgement.
      registrationDeadline = setTimeout(fail, bound);
      const attempt = () => {
        registrationTimer = null;
        if (instance !== target || confirmedInstance === target || stopped !== null) return;
        // No await separates either fence check from register, on EVERY retry.
        if (!fenced()) {
          void stop("lockLost", false);
          return;
        }
        const wait = Math.min(timeout * 2 ** attempts++, bound);
        registrationTimer = setTimeout(attempt, wait);
        broker.postMessage({
          t: "register",
          v: BROKER_PROTOCOL,
          instance: target,
          db: database,
          ownerId,
          protocol,
          build: init.build,
          lockHolder,
        } satisfies BrokerMessage);
      };
      attempt();
    };
    broker.onmessage = (event: MessageEvent) => {
      const message = event.data as BrokerMessage;
      if (message.t === "client") {
        if (stopped === null) server.accept(message.port);
        else message.port.close();
      } else if (message.t === "instance" && message.instance !== instance) {
        if (fenced()) beginRegistration(message.instance);
      } else if (
        message.t === "registrationAck" &&
        message.instance === instance &&
        message.db === database &&
        message.ownerId === ownerId &&
        fenced()
      ) {
        confirmedInstance = message.instance;
        cancelRegistration();
      }
    };
    // The instance announcement precedes registration; an unacknowledged send proves nothing.
  }

  const skew = openChannel(skewChannelName(database));

  const release = (reason: StepDown["reason"]) => {
    factory.closeFence(fence);
    lock.release();
    heldHostLock.release();
    skew?.close();
    if (broker !== null) {
      broker.postMessage({
        t: "unregister",
        v: BROKER_PROTOCOL,
        db: database,
        ownerId,
      } satisfies BrokerMessage);
      broker.close();
    }
    deps.post({ t: "minnow:stopped", reason });
  };
  const stop = (reason: StepDown["reason"], graceful: boolean): Promise<void> => {
    if (stopped !== null) return stopped;
    cancelRegistration();
    server.stepDown(reason);
    if (!graceful) {
      // Synchronous: the handles must be closed before the page is suspended or the lock stolen.
      release(reason);
      stopped = Promise.resolve();
      return stopped;
    }
    stopped = engine
      .close()
      .catch(() => undefined)
      .then(() => {
        release(reason);
      });
    return stopped;
  };

  if (skew !== null)
    skew.onmessage = (event: MessageEvent) => {
      const signal = event.data as SkewSignal;
      if (signal.t === "whoOwns")
        skew.postMessage({
          t: "owner",
          protocol,
          build: init.build,
          ownerId,
          epoch,
        } satisfies SkewSignal);
      else if (signal.t === "supersede" && signal.protocol > protocol)
        void stop("superseded", true);
    };

  lock.lost.addEventListener("abort", () => void stop("lockLost", false), { once: true });
  heldHostLock.lost.addEventListener("abort", () => void stop("lockLost", false), { once: true });

  // Announce this owner, now registered: a client connected to a previous owner (idle, its port
  // dead without a word) learns of the change at once and reconnects (connector.ts "Losing the
  // owner").
  skew?.postMessage({
    t: "owner",
    protocol,
    build: init.build,
    ownerId,
    epoch,
  } satisfies SkewSignal);

  const host: OwnerHost = {
    ownerId,
    server,
    engine,
    get registration() {
      return {
        instance,
        confirmed: stopped === null && instance !== null && confirmedInstance === instance,
        error: registrationError,
      };
    },
    lifecycle(event, platform) {
      const action = lifecycleAction(event, platform);
      if (action === "flush") void engine.flushPending();
      else if (action === "stepDown") void stop("hidden", true);
      else if (action === "terminate") void stop(event === "freeze" ? "freeze" : "pagehide", false);
    },
    stop,
  };
  deps.post({ t: "minnow:ready", ownerId });
  return host;
}

/**
 * The owner worker's entry: waits for `minnow:host`, starts the host, then relays the page's
 * lifecycle commands. `scope` is the worker's global scope.
 */
export function serveOwnerWorker<F>(
  scope: {
    onmessage: ((event: MessageEvent) => void) | null;
    postMessage(message: unknown): void;
  },
  factory: OwnerEngineFactory<F>,
  locks: WebLockManager,
): void {
  let host: Promise<OwnerHost> | null = null;
  scope.onmessage = (event: MessageEvent) => {
    const message = event.data as OwnerHostInit | OwnerHostCommand;
    if (message.t === "minnow:host") {
      host = startOwnerHost(message, factory, {
        locks,
        post: (status) => {
          scope.postMessage(status);
        },
      });
      host.catch(() => undefined);
      return;
    }
    void host?.then((started) => {
      if (message.t === "minnow:lifecycle") started.lifecycle(message.event, message.platform);
      else void started.stop("closing", true);
    });
  };
}
