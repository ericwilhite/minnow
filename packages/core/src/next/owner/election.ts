/**
 * Who owns a database, how clients reach the owner, when the owner steps down, and how builds of
 * different protocol versions settle who stays. Platform facts are from the storage probe of
 * 2026-11-05 (DECISIONS.md "Ownership protocol facts").
 *
 * ## Locks (all Web Locks, exclusive)
 * | Name                         | Held by                                     | While                        |
 * | ---------------------------- | ------------------------------------------- | ---------------------------- |
 * | `minnow:<db>`                | the owner worker (`storage/fence.ts`)       | it holds the store's handles |
 * | `minnow:<db>:host`           | the owner (stolen from the page that won)    | the owner runs               |
 * | `minnow:<db>:client:<id>`    | a client                                    | it has pins or `live` requests |
 *
 * - Hosts (`canHost`): a page, which starts a dedicated owner worker (the package's owner entry),
 *   or a dedicated worker, which runs the owner **in place** (sync access handles exist only in
 *   dedicated workers). SharedWorkers never host; they only broker.
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
 * - A broker instance can be replaced under its ports: WebKit 26.6 restarts a SharedWorker on a
 *   cross-origin-isolated page when the page that created it closes, with empty state, the other
 *   pages' ports reattached, and the messages sent during the switch dropped (owner/webkit-sharedworker.md).
 *   So each instance has a random id it announces on every port it accepts (`instance`); an owner
 *   that sees a new id registers again (only while it still holds its locks), and a client sends
 *   every unanswered request again to each instance that announces itself on its ports. Every client request is bounded: `BROKER_REQUEST_TIMEOUT_MS`, retried
 *   with backoff on the same port once, then on fresh ports, up to `BROKER_REQUEST_BOUND_MS`; past
 *   that, `OwnerUnreachableError`. A retry carries the same request id (`nonce`), and the broker
 *   answers a request id once: a duplicate never makes a second connection.
 * - The broker script has no imports and its own `BROKER_PROTOCOL`, so builds that share it share
 *   one SharedWorker (same script URL). The owner tab's own client skips the broker: it talks to
 *   its owner worker over a MessageChannel it creates.
 * - Without SharedWorker (old Chrome Android) a context can host but cannot reach another host:
 *   `OwnerUnreachableError`. An app may supply its own `Broker` (a service worker works, same
 *   round trip, probe table 3).
 *
 * ## Step-down (`lifecycleAction`)
 * Workers get no lifecycle events (probe table 4). A page host sees them itself; a worker hosting
 * in place gets them relayed by the page that created it (`LifecycleRelay`, a message the client
 * package sends for the app). Without a relay an in-place host never steps down early: it holds
 * the store until its worker ends, documented as the cost of hosting without a relay.
 * - `hidden` on desktop: `flush`: the owner calls `engine.flushPending()` (one flush, ~4 ms), so a
 *   later freeze or pagehide loses at most relaxed commits acknowledged after the page was hidden
 *   (ruling R9). Strict commits are durable before they are acknowledged and lose nothing. No
 *   handover: it costs a close, an open (Firefox ~12 ms each), up to 70 ms of lock-to-handle lag
 *   and a rebuild of in-memory state, so it is not paid per tab switch.
 * - `hidden` on mobile (where hidden pages are suspended): graceful step-down: stop admitting,
 *   finish or cancel running statements up to the commit boundary, flush, send `stepDown`, close
 *   the handles, release the locks, terminate.
 * - `freeze` and `pagehide`: start bounded step-down in the event handler, then terminate the
 *   worker after release (or at STEP_DOWN_BOUND_MS). A page in bfcache would otherwise
 *   keep it (Firefox, verified) or lose only the lock (Safari). An in-place host, on the relayed
 *   event, follows the same release protocol. Abrupt destruction may leave no time for it; relaxed
 *   commits acknowledged since the last flush may then be lost, as on power loss. Clients resend
 *   unanswered requests to the next owner (exactly-once through the ledger).
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

/** One attempt of a client's broker request waits this long before it is sent again. */
export const BROKER_REQUEST_TIMEOUT_MS = 1_000;
/**
 * A client's broker request, retried with doubling timeouts (1, 2, 4, 8 s), fails with
 * `OwnerUnreachableError` after this long.
 */
export const BROKER_REQUEST_BOUND_MS = 15_000;

/** A random 64-bit id, hex (owner ids, broker instance ids). */
export const randomId = (): string => {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
};

/** The broker message format version, independent of `PROTOCOL_VERSION`. */
export const BROKER_PROTOCOL = 1;

/**
 * How long a page waits for its owner worker to step down gracefully (drain, close its session,
 * release its lease; DECISIONS.md "Step-down") before it terminates the worker anyway. The storage
 * backend drains for up to 500 ms and gives aborted transactions up to 1 s to settle. Allow
 * another 500 ms for owner work/session close and scheduling margin; never cut release short.
 */
export const STEP_DOWN_BOUND_MS = 3_000;
/** Maximum wait for owner tasks and engine close before proceeding to lease release. */
export const STEP_DOWN_DRAIN_MS = 500;
/** How long a client waits for `welcome` before abandoning the connection and starting over. */
export const HANDSHAKE_TIMEOUT_MS = 5_000;
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

/**
 * The database's BroadcastChannel, or null where there is none: no `BroadcastChannel`, or a
 * constructor that throws (restricted contexts). Without it, announcements and skew signals are
 * off and clients rely on the silence deadline.
 */
export function openChannel(name: string): BroadcastChannel | null {
  if (typeof BroadcastChannel === "undefined") return null;
  try {
    return new BroadcastChannel(name);
  } catch {
    return null;
  }
}

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
      /**
       * The request id, unique per client: every retry of one request carries the same one, and
       * the broker answers it once (a duplicate makes no second connection).
       */
      readonly nonce: number;
    }
  | {
      readonly t: "register";
      /** Target instance; absent only for legacy senders that do not await acknowledgement. */
      readonly instance?: string;
      readonly v: number;
      readonly db: string;
      readonly ownerId: string;
      readonly protocol: number;
      readonly build: string;
      /**
       * The Web Locks client id holding the host lock (from `navigator.locks.query()`), or null
       * where the platform does not report it: the broker hands out this registration only while
       * that very holder still holds the lock.
       */
      readonly lockHolder: string | null;
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
  /** Broker → owner: registration was installed by this exact instance. */
  | {
      readonly t: "registrationAck";
      readonly db: string;
      readonly ownerId: string;
      readonly instance: string;
    }
  /** Broker → every connected context. */
  | { readonly t: "registered"; readonly db: string; readonly ownerId: string }
  /**
   * Broker → every port it accepts, first: the instance's random id. A new id on a port means the
   * broker was replaced and lost its registrations and pending requests.
   */
  | { readonly t: "instance"; readonly instance: string }
  /** Diagnostics: a bare round trip to the broker (the latency lane's platform baseline). */
  | { readonly t: "ping"; readonly v: number; readonly nonce: number }
  | { readonly t: "pong"; readonly nonce: number };

// --- Skew --------------------------------------------------------------------------------------

/** Messages on the BroadcastChannel `skewChannelName(db)`. */
export type SkewSignal =
  | { readonly t: "whoOwns"; readonly protocol: number }
  | {
      readonly t: "owner";
      readonly protocol: number;
      readonly build: string;
      readonly ownerId: string;
      /**
       * The owner's fence epoch (storage/contract.ts `OwnerLease.epoch`: strictly increasing
       * across acquisitions of one database). A client acts only on an epoch above its own
       * owner's; an older build that sends none is treated as epoch 0.
       */
      readonly epoch?: number;
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

/** Page lifecycle events. Workers get none (probe table 4); the page relays them. */
export type LifecycleEvent = "freeze" | "resume" | "pagehide" | "pageshow" | "hidden" | "visible";

/** The message a page posts to a dedicated worker hosting in place, for each lifecycle event. */
export interface LifecycleRelay {
  readonly t: "minnow:lifecycle";
  readonly event: LifecycleEvent;
  readonly platform: Platform;
}

export interface Platform {
  /** Hidden pages get suspended (iOS, Android). */
  readonly mobile: boolean;
}

/**
 * What the host does on `event` (module comment, "Step-down"):
 * - `terminate`: begin the bounded release/termination protocol inside the event handler.
 * - `stepDown`: graceful step-down, then terminate.
 * - `flush`: `engine.flushPending()`; keep owning.
 * - `none`.
 */
export function lifecycleAction(
  event: LifecycleEvent,
  platform: Platform,
): "terminate" | "stepDown" | "flush" | "none" {
  switch (event) {
    case "freeze":
    case "pagehide":
      return "terminate";
    case "hidden":
      return platform.mobile ? "stepDown" : "flush";
    case "resume":
    case "pageshow":
    case "visible":
      return "none";
  }
}

export interface HostContext {
  /**
   * `page`: starts an owner worker. `dedicatedWorker`: hosts in place. `sharedWorker` and
   * `serviceWorker`: never host (no sync access handles).
   */
  readonly kind: "page" | "dedicatedWorker" | "sharedWorker" | "serviceWorker";
  /** The page's visibility (for a dedicated worker: as last relayed; true if never relayed). */
  readonly visible: boolean;
  readonly platform: Platform;
  /** This page was superseded by a newer build; it never hosts again. */
  readonly superseded: boolean;
}

/**
 * Whether a context may request the host lock: pages and dedicated workers (ruling R9), unless
 * superseded, and not while hidden on mobile, where the host would be suspended.
 */
export function canHost(context: HostContext): boolean {
  if (context.superseded) return false;
  if (context.kind !== "page" && context.kind !== "dedicatedWorker") return false;
  return context.visible || !context.platform.mobile;
}
