/**
 * Who owns a database, how clients reach the owner, when the owner steps down, and how builds of
 * different protocol versions settle who stays. Platform facts are from the storage probe of
 * 2026-11-05 (DECISIONS.md "Ownership protocol facts").
 *
 * ## Locks (all Web Locks, exclusive)
 * | Name                         | Held by                                     | While                        |
 * | ---------------------------- | ------------------------------------------- | ---------------------------- |
 * | `minnow:<db>`                | the owner worker (`storage/fence.ts`)       | it holds the store's handles |
 * | `minnow:<db>:host`           | the context that started the owner worker   | that worker runs             |
 * | `minnow:<db>:client:<id>`    | a client                                    | it has pins or `live` requests |
 *
 * - The fence lock is taken inside the owner worker so it frees when the worker dies. The handle,
 *   not the lock, is the fence: the lock frees 2 ms–2 s before the handle, so the successor opens
 *   with bounded retry (`acquireFenced`), and a frozen owner blocks takeover until it thaws or is
 *   discarded (`OwnerUnresponsiveError`).
 * - The host lock is the election. It is requested (queued, never `ifAvailable`) only by a client
 *   that is eligible to host (`canHost`) and currently needs an owner: it has requests in flight or
 *   is connecting. So the queue is empty when no one is waiting (pages keep bfcache), a dead host
 *   is replaced by the first waiter at once (no silence deadline), and a live host's lock blocks a
 *   waiting page, which exempts it from Chrome's Energy Saver freezing exactly when it matters.
 *   A waiter whose work completes aborts its request.
 * - The liveness lock is held only while the client has snapshot pins (cursors, transactions) or
 *   requests sent with `live: true`, plus `LIVENESS_IDLE_RELEASE_MS` of idle hysteresis so a
 *   request loop does not pay a lock round trip per request. A permanent lock would cost Firefox
 *   bfcache, and Safari drops locks on bfcache suspension anyway. The owner watches a client's
 *   lock (a queued request) only while it holds that client's pins or live requests; the grant
 *   means the client is gone: the owner releases at once, cancels its live requests and drops its
 *   pins. A client back from bfcache expects `SessionLostError` and reconnects.
 *
 * ## Reaching the owner: the broker
 * - BroadcastChannel cannot carry a MessagePort (probe), so a SharedWorker brokers ports
 *   (`Broker`). It holds a registry of owners and, for each connect, creates a MessageChannel and
 *   transfers one end to the owner and one to the client. Round trip through a brokered port:
 *   ~55–65 µs Chromium, ~140 µs Firefox, ~280 µs WebKit (p95 520 µs).
 * - The broker checks the host lock with `navigator.locks.query()` before handing out a port, so a
 *   registration whose host died is never used. It takes no locks itself.
 * - The broker script has no imports and its own `BROKER_PROTOCOL`, so builds that share it share
 *   one SharedWorker (same script URL). The owner tab's own client skips the broker: it talks to
 *   its owner worker over a MessageChannel it creates.
 * - Without SharedWorker (old Chrome Android) a context can host but cannot reach another host:
 *   `OwnerUnreachableError`. An app may supply its own `Broker` (a service worker works, same
 *   round trip, probe table 3).
 *
 * ## Step-down (`lifecycleAction`)
 * - `freeze` and `pagehide`: the host terminates its owner worker at once. Terminating frees the
 *   handle in 5–40 ms; a page in bfcache would otherwise keep it (Firefox, verified) or lose only
 *   the lock (Safari). Strict commits are durable before they are acknowledged; relaxed commits
 *   acknowledged within the idle-flush window may be lost, as on power loss. Clients resend
 *   unanswered requests to the next owner (exactly-once through the ledger).
 * - `hidden` on mobile (where hidden pages are suspended): graceful step-down: stop admitting,
 *   finish or cancel running statements, flush, send `stepDown`, close the handles, terminate.
 * - Nothing on `hidden` elsewhere: a handover costs a close, an open (Firefox ~12 ms each), up to
 *   70 ms of lock-to-handle lag and a rebuild of in-memory state, so it is not paid per tab switch.
 * - A host returning (`pageshow`, `resume`, `visible`) does nothing: it is a client again and
 *   elects lazily on its next request.
 *
 * ## Version skew (`compareProtocol`)
 * - The owner refuses a `hello` from another protocol version.
 * - A newer client asks the owner to step aside (`SkewSignal` `supersede` on the BroadcastChannel
 *   `minnow:<db>`). The owner steps down with reason `superseded`; its clients (the older build)
 *   are superseded and reload, at most once a minute (0.19's `whenSuperseded`). The newer client
 *   then elects itself.
 * - An older client is superseded at once and never hosts again in that page.
 * - A newer client whose broker cannot see a live owner (the older build uses another broker
 *   script) finds it by asking on the BroadcastChannel (`whoOwns`), then sends `supersede`.
 */

/** The broker message format version, independent of `PROTOCOL_VERSION`. */
export const BROKER_PROTOCOL = 1;

/** Idle time after which a client releases its liveness lock. */
export const LIVENESS_IDLE_RELEASE_MS = 1_000;
/** A live subscription whose update is unacknowledged this long is dropped (`liveEnded`). */
export const LIVE_UNACKED_DROP_MS = 60_000;

/** Lock and channel names. `db` is the database name the app opened. */
export const ownerLockName = (db: string): string => `minnow:${db}`;
export const hostLockName = (db: string): string => `minnow:${db}:host`;
export const clientLockName = (db: string, clientId: string): string =>
  `minnow:${db}:client:${clientId}`;
export const skewChannelName = (db: string): string => `minnow:${db}`;

// --- Broker ------------------------------------------------------------------------------------

/** A client's view of the broker. */
export interface Broker {
  /**
   * A port to the live owner of `db`, or `noOwner` when no live host is registered (the caller
   * may elect itself), or `unreachable` when a live host exists but registered elsewhere (skew, or
   * a broker of another script URL).
   */
  connect(
    db: string,
    clientId: string,
  ): Promise<
    | { readonly kind: "port"; readonly port: MessagePort; readonly ownerId: string }
    | { readonly kind: "noOwner" }
    | { readonly kind: "unreachable" }
  >;
  /** Called whenever an owner registers for `db`. Returns an unsubscribe function. */
  onOwnerRegistered(db: string, listener: (ownerId: string) => void): () => void;
}

/** The owner worker's view of the broker. */
export interface BrokerRegistration {
  /** Registers this owner. Each connect delivers a fresh port. */
  register(
    db: string,
    owner: { readonly ownerId: string; readonly protocol: number; readonly build: string },
    onClient: (port: MessagePort, clientId: string) => void,
  ): Promise<void>;
  /** Removes the registration (step-down); later connects get `noOwner` until a new owner. */
  unregister(db: string, ownerId: string): void;
}

/** Messages on a port to the broker SharedWorker. */
export type BrokerMessage =
  | {
      readonly t: "connect";
      readonly v: number;
      readonly db: string;
      readonly clientId: string;
      readonly nonce: number;
    }
  | {
      readonly t: "register";
      readonly v: number;
      readonly db: string;
      readonly ownerId: string;
      readonly protocol: number;
      readonly build: string;
    }
  | { readonly t: "unregister"; readonly v: number; readonly db: string; readonly ownerId: string }
  /** Broker → client (the port is in the transfer list). */
  | {
      readonly t: "port";
      readonly nonce: number;
      readonly ownerId: string;
      readonly port: MessagePort;
    }
  | { readonly t: "noOwner" | "unreachable"; readonly nonce: number }
  /** Broker → owner: a new client connection. */
  | { readonly t: "client"; readonly clientId: string; readonly port: MessagePort }
  /** Broker → every connected context. */
  | { readonly t: "registered"; readonly db: string; readonly ownerId: string };

// --- Skew --------------------------------------------------------------------------------------

/** Messages on the BroadcastChannel `skewChannelName(db)`. */
export type SkewSignal =
  | { readonly t: "whoOwns"; readonly protocol: number }
  | {
      readonly t: "owner";
      readonly protocol: number;
      readonly build: string;
      readonly ownerId: string;
    }
  /** From a newer client: owners of an older protocol step aside. */
  | { readonly t: "supersede"; readonly protocol: number };

export function compareProtocol(
  client: number,
  owner: number,
): "same" | "clientNewer" | "clientOlder" {
  if (client === owner) return "same";
  return client > owner ? "clientNewer" : "clientOlder";
}

// --- Lifecycle ---------------------------------------------------------------------------------

/** Page lifecycle events. Workers get none (probe table 4); the host page relays them. */
export type LifecycleEvent = "freeze" | "resume" | "pagehide" | "pageshow" | "hidden" | "visible";

export interface Platform {
  /** Hidden pages get suspended (iOS, Android). */
  readonly mobile: boolean;
}

/**
 * What the host does with its owner worker on `event`:
 * - `terminate`: `worker.terminate()` now, inside the event handler, then release the host lock.
 * - `stepDown`: graceful step-down (see the module comment), then terminate.
 * - `none`.
 */
export function lifecycleAction(
  event: LifecycleEvent,
  platform: Platform,
): "terminate" | "stepDown" | "none" {
  switch (event) {
    case "freeze":
    case "pagehide":
      return "terminate";
    case "hidden":
      return platform.mobile ? "stepDown" : "none";
    case "resume":
    case "pageshow":
    case "visible":
      return "none";
  }
}

export interface HostContext {
  /** A page can start and terminate an owner worker and sees lifecycle events; a worker cannot. */
  readonly kind: "page" | "worker";
  readonly visible: boolean;
  readonly platform: Platform;
  /** This page was superseded by a newer build; it never hosts again. */
  readonly superseded: boolean;
}

/**
 * Whether a context may request the host lock. Workers do not host (no lifecycle events, and
 * nested workers need Safari 16.4); a hidden mobile page would be suspended while hosting.
 */
export function canHost(context: HostContext): boolean {
  if (context.superseded || context.kind !== "page") return false;
  return context.visible || !context.platform.mobile;
}
