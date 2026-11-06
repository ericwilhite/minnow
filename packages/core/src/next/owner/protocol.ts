/**
 * The owner/client wire protocol: every message that crosses the MessagePort between a client
 * (any tab or worker) and the database's one owner worker.
 *
 * ## Topology
 * - One owner worker per database holds the storage fence (election.ts). Every other context is a
 *   client. The owner tab's own client uses the same protocol over a MessageChannel to its worker;
 *   a dedicated worker hosting in place talks to itself without a port.
 * - A client in another tab reaches the owner through a port handed out by the broker
 *   (election.ts `Broker`). After the handout the broker is off the path: the port connects the
 *   client straight to the owner worker.
 * - There is no SharedArrayBuffer. Result frames are **transferred** (frame.ts); batch-write input
 *   is cloned so the client can resend it (ruling R9).
 *
 * ## Connection
 * 1. The client sends `hello`. The owner answers `welcome` or `refused`. Nothing else is sent
 *    before `welcome`; requests made earlier wait in the client, not in the owner.
 * 2. A connection is one port. Sessions, cursors, prepared statements and subscriptions belong to
 *    the connection and die with it. A new port is a new connection, even to the same owner.
 * 3. There is no close event on a MessagePort (removed from the spec, 2026-10). A connection ends
 *    by `bye` (client), `stepDown` (owner), the owner seeing the client's liveness lock released,
 *    or the client's silence deadline (`OwnerUnresponsiveError`).
 * 4. When a connection ends, the owner cancels its running statements (except past the commit
 *    boundary, below), rolls back its sessions, drops its pins and releases every result frame it
 *    still has outstanding.
 *
 * ## Requests and replies
 * - Every request carries a `RequestHeader` with a per-client sequence `seq`. It is the ledger
 *   sequence (format/ledger.ts) and the request's id on the wire: replies, frames, cancellation
 *   and releases name it.
 * - Every request gets **exactly one terminal reply** (`done`, `error`, `prepared`, `value`),
 *   possibly preceded by `rows` messages. After its terminal reply a seq is never mentioned again
 *   by the owner.
 * - Signals (`cancel`, `release`, `liveness`, `liveAck`, `unprepare`, `bye`) have no seq and no
 *   reply.
 * - The owner may answer requests out of order. Requests on one session run in arrival order.
 *
 * ## Result frames and the window (ruling R3)
 * - The owner pulls frames from the executor's result stream (exec/result.ts `ResultStream`) and
 *   posts each one as soon as it has it; frames already queued go out together in one message.
 *   The last frames ride in the terminal `done`, so a small result is one message.
 * - A frame stays charged to the owner until the client sends `release { target, through }` for
 *   it. The client releases a cursor frame when the caller takes it from the iterator, and a
 *   buffered result's frames as they arrive. Unreleased bytes reach the request's `windowBytes`
 *   and the executor pauses: a slow consumer costs one window, never a growing buffer.
 * - RETURNING rows (ruling R2) are delivered only once the write is committed: for an autocommit
 *   write, after its commit is acknowledged per durability mode (strict: durable; relaxed:
 *   applied). Until then the executor keeps them in a charged, spillable spool and offers no
 *   frames; if the commit fails they are discarded and the client gets only the error. Inside an
 *   explicit transaction they flow when the statement has executed, before COMMIT, as in
 *   PostgreSQL (ruling R11). A plain SELECT streams at once.
 * - Under strict durability a frame is posted only once `writePath.durable(frame.requiresLsn)`
 *   holds (no wait in the common case: a read snapshot sees only durable commits).
 * - A stream whose request is not `live` (below) and whose frames stay unreleased for
 *   `ABANDONED_STREAM_MS` is treated as abandoned: cancelled, frames released, `SessionLostError`.
 *
 * ## Cancellation and its acknowledgement
 * - `cancel { target }` asks the owner to stop request `target`. Its acknowledgement is the
 *   target's terminal reply, which always follows:
 *   - `error` with code `MINNOW_CANCELLED`: stopped, with no effect.
 *   - its normal reply: it finished first.
 * - **The commit boundary** (ruling R9) is the owner's call to the write path's `commit()`.
 *   Cancelling before it aborts the transaction; from that call on the request completes and its
 *   reply reports the commit. Explicit `COMMIT` and autocommit writes are the same here.
 * - `cancel { target, close: true }` ends a cursor or subscription early. Its terminal reply is
 *   `done` with `summary.closedEarly`, never an error.
 * - A cancel for a seq the owner has already answered, or never saw, is ignored.
 *
 * ## Exactly-once (format/ledger.ts)
 * - The owner admits every request through `RequestLedger.admit` before running it; `sentAtMs`
 *   and `resolvedThrough` feed it, and the receiver enforces the retry window. The client keeps
 *   `seq − resolvedThrough ≤ MAX_OUTSTANDING_REQUESTS` (sequence.ts).
 * - What a retry gets depends on the request (outcomes.ts `ledgerAction` decides; rulings R9,
 *   R12, R16.1, R16.2):
 *   - **Commits are exactly-once** (an autocommit write or DDL, `COMMIT`): the write path records
 *     the committed outcome once durable (strict) or applied (relaxed); a retry gets it.
 *   - **Statements inside an open transaction** that change it (DML, DDL, savepoints, `BEGIN`,
 *     and any statement that fails, since failing aborts the transaction) get a session-scoped,
 *     in-memory `statement` outcome, keyed by (client, engine session, generation); a duplicate
 *     gets it back instead of running again. The session's end (`endSession`) clears that
 *     session's outcomes only.
 *   - **Everything else is at-least-once**: reads, standalone queries including
 *     `SELECT nextval(…)` (a lost value is a sequence gap, which PostgreSQL allows), queries
 *     inside a transaction (re-running reads the same transaction state), `ROLLBACK`, prepares and
 *     subscriptions are `settle`d: no outcome, and a retry runs again.
 *   - **Failures** of requests that would have committed are recorded by the owner (in memory);
 *     a retry gets the same error.
 * - The ledger identity is derived by the owner (ruling R18.2), never sent: `sessionId` is the
 *   request's session (`String(session)`), `generation` the request's, and `scope` comes from the
 *   routing table (outcomes.ts `ledgerScope`): `transaction` for a statement inside a transaction
 *   the client opened, `standalone` for everything else (autocommit writes and DDL, COMMIT,
 *   queries). A resent request that is not resendable is admitted with `transaction` scope, so an
 *   ended generation answers `sessionLost`.
 * - A connection is one **generation** of its sessions: the client numbers its connections
 *   (`Hello.generation`), and when one ends the owner ends each of its sessions in the ledger. In-transaction
 *   statements of an ended generation are dropped with its rolled-back transaction, and their
 *   late completions are not recorded; a commit already past the boundary still records.
 * - After a reconnect, the client resends every request it has no terminal reply for, unchanged
 *   (same seq, same `sentAtMs`, same cloned input), except those it already saw frames of (it
 *   fails those itself).
 * - The owner answers a recorded duplicate from the ledger: a committed write gets `done` with
 *   `summary.replayed: true` and the original counts. RETURNING rows are not replayed: the client
 *   rejects with `CommittedWithoutResultError` if the caller asked for them.
 * - `resendable` says whether a request means the same thing on a fresh connection (sequence.ts
 *   rules). A resent request (`retry`) that the ledger admits as first sight and that is not
 *   resendable is refused with `SessionLostError` and never executed. So a transaction's
 *   statements never run outside it, and a COMMIT whose fate is unknown resolves from the ledger:
 *   committed, or definitely not.
 * - A session with an open transaction belongs to its connection generation (B5-p2b reviews 1
 *   and 2). What a request does to its session's transaction comes from authority: a prepared
 *   statement's kind from this owner's `prepared` reply (`statementKind`), SQL text from the
 *   client's classifier, which follows the front end's lexer (sequence.ts
 *   `classifyStatementText`; multi-statement text is `unknown`). After a reconnect, in send order:
 *   a request in flight whose kind is `begin` or `unknown` is refused and never resent, and its
 *   session is lost (session 0 included); `commit` is resent and the ledger answers it
 *   (committed: the session ends); `rollback` ends the session; anything else on a lost session
 *   is refused. Until the client ends a lost session (ROLLBACK, ABORT), every new request on it
 *   fails with `SessionLostError("transaction")` in the client, unsent: nothing meant for the lost
 *   transaction runs as autocommit on the new owner.
 * - `SessionLostError.effect` says whether "nothing took effect" is proven (`none`: never sent,
 *   ran only inside a transaction that rolled back, or no commit in the ledger) or not
 *   (`unknown`: a multi-statement text, a cursor whose frames were handed out, a result whose
 *   statement passed its commit point). Only `none` claims nothing took effect. *
 * ## Idle transactions (ruling R16.7)
 * - The owner rolls back a transaction left idle for `transactionIdleTimeoutMs` (default 30 s, as
 *   in 0.19): no request of its session running and none arriving. The timer starts when the
 *   session's last running request ends, and is cleared by any request on that session or any
 *   `release` of one of its streams.
 * - On expiry the owner rolls the transaction back, which releases the writer slot (or the
 *   read-only pin), and sends `sessionAborted`. Until the client ends the transaction, every
 *   request on that session except `ROLLBACK` fails with `TransactionIdleTimeoutError` (SQLSTATE
 *   25P03); `COMMIT` fails the same way and ends it; `ROLLBACK` succeeds and ends it. So statements
 *   meant for the transaction never run in autocommit.
 *
 * ## Liveness (election.ts)
 * - `live: true` promises that the sender held its liveness lock when it sent the request and
 *   keeps holding it until the terminal reply. The owner watches that lock only while it has
 *   such requests or pins of the client; a grant ends the connection (above).
 * - Requests are never delayed to take the lock: the client sends `live: false` until the lock is
 *   granted, then sends `liveness { held: true }`, which makes its outstanding requests and pins
 *   watched; before releasing the lock it sends `liveness { held: false }`. Opening a
 *   cursor or a transaction (they pin a snapshot) waits for the lock.
 * - A held lock is declared again on every new connection, right after `welcome` and before any
 *   resend: an owner watches only what its own connection was told.
 * - While a client has requests in flight the owner sends `alive` at least every
 *   `ALIVE_INTERVAL_MS`. The client's deadline counts silence, not wall time.
 *
 * ## Version skew
 * - `PROTOCOL_VERSION` changes with any change to these messages or to frame.ts. The owner refuses
 *   a `hello` with another version (`refused { reason: "version" }`).
 * - A newer client asks the owner to step aside (election.ts `SkewSignal`); an older client is
 *   superseded and reloads (0.19's `whenSuperseded` behaviour, engine/client.ts).
 *
 * ## Latency (ruling R6)
 * Targets for a warm primary-key read from another tab, measured at the client from `query()` to
 * its resolution, in the browser perf lane:
 *
 * | Engine   | Uncontended p50 | Under a concurrent analytic query, p95 | Owner-side work |
 * | -------- | --------------- | -------------------------------------- | --------------- |
 * | Chromium | ≤ 0.25 ms       | ≤ 5 ms                                 | ≤ 10 µs         |
 * | Firefox  | ≤ 0.4 ms        | ≤ 5 ms                                 | ≤ 10 µs         |
 * | WebKit   | ≤ 0.6 ms        | ≤ 5 ms                                 | ≤ 10 µs         |
 *
 * The bare round trip is ~55–65 µs, ~140 µs and ~280 µs (probe), so the path adds nothing it can
 * avoid:
 * - **Client:** `query()` issues the seq and posts synchronously: no await before `postMessage`,
 *   no lock wait (`live: false` is fine), no SQL parsing (prepared statements send an id), no
 *   per-request timer (one shared silence timer).
 * - **Owner:** the port's message handler runs the whole read synchronously: envelope check,
 *   `admit`, the plan-cache hit, a snapshot pin, the executor's synchronous point-read path when
 *   the key index and page cache hit (`EngineSession.runNow`), `settle`, unpin, and one
 *   `postMessage` of `done` with the frame inline. No promise, microtask or timer on the hit path;
 *   a miss falls back to the async run. Strict durability adds no wait: a read snapshot sees only
 *   durable commits.
 * - **Reply:** one message; `columns` omitted when the client already has them for that prepared
 *   statement and catalog version.
 * - **Under load:** the executor yields at most every `YIELD_INTERVAL_MS` (4 ms, B4), and the
 *   owner handles each message as its own task, so a point read waits at most one slice.
 * - On WebKit a busy main thread in the owner's page delays messages to its worker (probe 7):
 *   outside these targets, documented.
 */
import type { PhysicalType } from "../types.js";
import type { WireError } from "./errors.js";
import type { FramePayload, WireInputColumn } from "./frame.js";

/** A session's transaction state as the owner reports it. */
export type TransactionState = "idle" | "open" | "failed" | "timedOut";

/** Default `transactionIdleTimeoutMs` (0.19's default). */
export const DEFAULT_TRANSACTION_IDLE_TIMEOUT_MS = 30_000;

/** Bumped by every change to a message in this file or in frame.ts. */
export const PROTOCOL_VERSION = 2;

/** The owner sends `alive` at least this often while a client has requests in flight. */
export const ALIVE_INTERVAL_MS = 1_000;
/** Default silence after which a client reports `OwnerUnresponsiveError`. */
export const DEFAULT_UNRESPONSIVE_AFTER_MS = 10_000;
/** Default result window: two frames of ~1 MiB, equal to the executor's `RESULT_WINDOW_BYTES`. */
export const DEFAULT_RESULT_WINDOW_BYTES = 2 * 1024 * 1024;
/** A non-live stream with frames unreleased this long is abandoned. */
export const ABANDONED_STREAM_MS = 30_000;

// --- Values ------------------------------------------------------------------------------------

/**
 * A parameter or input value as it crosses the port (structured clone). DECISIONS.md types:
 * int8 takes `bigint` or a safe-integer `number`; TIMESTAMP takes ISO/PostgreSQL text or a `Date`
 * (exact to the ms); NUMERIC takes text or a number; JSON takes text; arrays are JS arrays.
 * The owner converts each value to its slot's type and fails with a bind error naming the
 * parameter or input row, never by re-planning.
 */
export type WireValue =
  null | boolean | number | bigint | string | Date | Uint8Array | readonly WireValue[];

// --- Client → owner ----------------------------------------------------------------------------

export interface Hello {
  readonly t: "hello";
  readonly protocol: number;
  /** The app build (bundle hash or version string); diagnostic only, never compared. */
  readonly build: string;
  /** Hex of the client's 16 random bytes; constant for the page's lifetime (format/ledger.ts). */
  readonly clientId: string;
  /** The name of this client's liveness lock (election.ts `clientLockName`). */
  readonly livenessLock: string;
  /** The owner this client last talked to, if any (diagnostics and step-down accounting). */
  readonly previousOwnerId: string | null;
  /**
   * This connection's generation: 1 for the client's first connection, +1 for each reconnect
   * (format/ledger.ts `SessionRef.generation`). A hello from a newer generation ends the client's
   * older connections to this owner.
   */
  readonly generation: number;
}

export interface RequestHeader {
  /** Per-client, increasing in send order; ≥ 1. The ledger sequence. */
  readonly seq: number;
  /** Client clock when first sent; unchanged on retry. */
  readonly sentAtMs: number;
  /** Highest S such that the client holds the terminal reply of every request ≤ S. */
  readonly resolvedThrough: number;
  /** The sender holds its liveness lock until this request's terminal reply. */
  readonly live: boolean;
  /** Means the same on any connection (see "Exactly-once"). */
  readonly resendable: boolean;
  /** Resent after a reconnect; the first send may or may not have reached an owner. */
  readonly retry: boolean;
  /** The generation of the connection the request was first sent on (≤ the current one). */
  readonly generation: number;
}

/**
 * The session a request runs in. Session 0 is the connection's default session: SQL `BEGIN`,
 * `COMMIT` and `ROLLBACK` text act on it, as on a PostgreSQL connection (the Kysely driver relies
 * on this). Sessions ≥ 1 are opened by `begin` with a client-chosen id, so a transaction's
 * statements can be pipelined behind its `begin` without waiting for a reply.
 */
export type SessionId = number;

/** How the rows of a statement come back. */
export interface ResultRequest {
  /**
   * Unreleased frame bytes the stream may hold before the executor pauses (the executor's
   * `resultWindowBytes`). `DEFAULT_RESULT_WINDOW_BYTES` unless the caller asked for more prefetch.
   */
  readonly windowBytes: number;
  /** Upper bound on rows per frame, if the caller wants smaller frames than the byte target. */
  readonly frameRows: number | null;
  /** Statement returns rows the caller wants (SELECT, or RETURNING). False drops them unsent. */
  readonly wantRows: boolean;
}

/** Runs one SQL statement, by text or by prepared id. */
export interface StatementRequest extends RequestHeader {
  readonly t: "statement";
  readonly session: SessionId;
  /** SQL text, or null when `prepared` names a statement prepared on this connection. */
  readonly text: string | null;
  readonly prepared: number | null;
  /** Catalog version of the columns the client already holds for `prepared`, or −1. */
  readonly knownColumnsVersion: number;
  readonly params: readonly WireValue[];
  readonly result: ResultRequest;
}

/** Plans `text` and names it `id` (client-chosen, unique on the connection). */
export interface PrepareRequest extends RequestHeader {
  readonly t: "prepare";
  readonly id: number;
  readonly text: string;
}

/**
 * Opens session `session` (client-chosen, ≥ 1) and its transaction. A read-write transaction
 * waits for the write path's single writer slot (B3, FIFO) and holds it until commit or
 * rollback; `readOnly` only pins a snapshot.
 */
export interface BeginRequest extends RequestHeader {
  readonly t: "begin";
  readonly session: SessionId;
  readonly readOnly: boolean;
}

export interface EndRequest extends RequestHeader {
  readonly t: "commit" | "rollback";
  readonly session: SessionId;
}

/**
 * A batch write without SQL: rows as cloned columns (frame.ts; the client keeps them to resend).
 * One transaction unless it runs inside an open session transaction. Counts come back in
 * `done.summary.counts`.
 */
export interface WriteRequest extends RequestHeader {
  readonly t: "write";
  readonly session: SessionId;
  readonly table: string;
  readonly op: "insert" | "upsert" | "update" | "delete";
  /**
   * Input column names, parallel to `columns`. For `delete`, the primary key columns only. For
   * `update`, the primary key columns plus the columns to set.
   */
  readonly names: readonly string[];
  readonly columns: readonly WireInputColumn[];
  readonly rowCount: number;
  /** INSERT … ON CONFLICT DO NOTHING semantics for `insert`; ignored otherwise. */
  readonly ignoreConflicts: boolean;
  /** Columns to return for each written row, or null. */
  readonly returning: readonly string[] | null;
  readonly result: ResultRequest;
}

/**
 * Subscribes to a query: the first reply is its result (`rows`/`done`, kept open), then `live`
 * whenever a commit may have changed it. No pin is held between updates: each update is a fresh
 * execution on the newest snapshot. Ends with `done` (after `cancel { close: true }`) or `error`.
 */
export interface SubscribeRequest extends RequestHeader {
  readonly t: "subscribe";
  readonly text: string;
  readonly params: readonly WireValue[];
}

/** Catalog and administration calls whose reply is a structured-clone `value`. */
export interface AdminRequest extends RequestHeader {
  readonly t: "admin";
  readonly op: AdminOp;
  readonly args: readonly WireValue[];
}

/**
 * The administrative surface (devtools, Kysely introspection). Each op's argument and value shape
 * is fixed by its client method in api.ts. Snapshot export streams through `rows` frames of bytes.
 */
export type AdminOp =
  | "catalog"
  | "storageStats"
  | "memoryStats"
  | "checkIntegrity"
  | "exportSnapshot"
  | "importSnapshot";

export type Request =
  | StatementRequest
  | PrepareRequest
  | BeginRequest
  | EndRequest
  | WriteRequest
  | SubscribeRequest
  | AdminRequest;

export interface Cancel {
  readonly t: "cancel";
  readonly target: number;
  /** End a cursor or subscription normally instead of failing it. */
  readonly close: boolean;
}

/**
 * The client has taken every frame of request `target` with `sequence ≤ through`: the owner calls
 * their `release()`, which returns their bytes to the window. Cumulative and idempotent.
 */
export interface Release {
  readonly t: "release";
  readonly target: number;
  readonly through: number;
}

/**
 * `held: true`: the client now holds its liveness lock, so its outstanding requests and its pins
 * (open transactions, cursors) may be watched. `held: false`, sent before it releases the lock
 * (no pins, nothing in flight): stop watching.
 */
export interface Liveness {
  readonly t: "liveness";
  readonly held: boolean;
}

/**
 * The client has taken live update `version` of subscription `target`: its frames are released
 * and one more update may follow.
 */
export interface LiveAck {
  readonly t: "liveAck";
  readonly target: number;
  readonly version: number;
}

export interface Unprepare {
  readonly t: "unprepare";
  readonly id: number;
}

/** The client is closing this connection: drop its sessions, cursors, pins and subscriptions. */
export interface Bye {
  readonly t: "bye";
}

export type Signal = Cancel | Release | Liveness | LiveAck | Unprepare | Bye;

export type ClientMessage = Hello | Request | Signal;

// --- Owner → client ----------------------------------------------------------------------------

export interface OwnerLimits {
  /** format/ledger.ts `MAX_OUTSTANDING_REQUESTS`. */
  readonly maxOutstanding: number;
  /** format/ledger.ts `CLIENT_RETRY_WINDOW_MS`. */
  readonly retryWindowMs: number;
  /** Target bytes of one frame; a single larger row is a frame of one. */
  readonly frameBytes: number;
  readonly aliveIntervalMs: number;
}

export interface Welcome {
  readonly t: "welcome";
  readonly protocol: number;
  readonly build: string;
  /** Random per owner worker; changes on every election. */
  readonly ownerId: string;
  /** The storage fence epoch this owner holds. */
  readonly epoch: number;
  readonly durability: "strict" | "relaxed";
  readonly limits: OwnerLimits;
}

export interface Refused {
  readonly t: "refused";
  /**
   * - `version`: protocol mismatch; `ownerProtocol` says which side is newer.
   * - `steppingDown`: this owner is leaving; reconnect through the broker.
   * - `superseded`: a newer build owns (or is taking) the database; this client must reload.
   * - `staleGeneration`: this client already connected with this generation or a newer one; the
   *   hello belongs to an ended connection.
   */
  readonly reason: "version" | "steppingDown" | "superseded" | "staleGeneration";
  readonly ownerProtocol: number;
  readonly ownerBuild: string;
}

/** Output column metadata, sent once per statement (or omitted, see `knownColumnsVersion`). */
export interface WireColumnMeta {
  readonly name: string;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  /** The SQL type name as PostgreSQL prints it (`numeric(10,2)`, `timestamp with time zone`). */
  readonly sqlType: string;
  /**
   * How row access renders an int64-family column: `bigint` (D2, every BIGINT) or `number`
   * (COUNT, and SUM over int2/int4, which are exact-or-RangeError by construction).
   */
  readonly render: "bigint" | "number" | null;
}

export interface StatementSummary {
  /** PostgreSQL command tag word: `SELECT`, `INSERT`, `CREATE TABLE`, `BEGIN`, … */
  readonly command: string;
  /** Rows returned (SELECT/RETURNING) or affected (INSERT/UPDATE/DELETE/MERGE), else null. */
  readonly rowCount: number | null;
  /** Per-table affected rows of a committed write; the ledger's `counts`. */
  readonly counts: Readonly<Record<string, number>> | null;
  /** The commit's log sequence number, when this request committed. */
  readonly lsn: number | null;
  /** This reply comes from the ledger: the request had already committed. No rows follow. */
  readonly replayed: boolean;
  /** A cursor or subscription ended by `cancel { close: true }`. */
  readonly closedEarly: boolean;
  /** The session's transaction state after the request, like PostgreSQL's ReadyForQuery. */
  /**
   * The session's transaction state after the request, like PostgreSQL's ReadyForQuery: `failed`
   * after an error inside it (25P02 until it ends), `timedOut` after the idle timeout.
   */
  readonly transaction: TransactionState;
  readonly catalogVersion: number;
}

/** Frames of request `seq`'s rows, in sequence order (all those the stream had queued). */
export interface Rows {
  readonly t: "rows";
  readonly seq: number;
  /** Present on the first message of a result unless the client already has them. */
  readonly columns: readonly WireColumnMeta[] | null;
  readonly frames: readonly FramePayload[];
}

export interface Done {
  readonly t: "done";
  readonly seq: number;
  readonly columns: readonly WireColumnMeta[] | null;
  /** The last frames, inline (often the only one: a small result is this one message). */
  readonly frames: readonly FramePayload[];
  readonly summary: StatementSummary;
}

export interface Prepared {
  readonly t: "prepared";
  readonly seq: number;
  readonly id: number;
  readonly columns: readonly WireColumnMeta[];
  readonly parameters: readonly PhysicalType[];
  readonly catalogVersion: number;
  /** The statement may open, end or change a transaction (sequence.ts `isResendable`). */
  readonly controlsTransaction: boolean;
  /**
   * What the statement does to its session's transaction, from the owner's front end: the
   * authority the client uses for a prepared statement after a reconnect (sequence.ts
   * "Statement kinds"). `savepoint` covers SAVEPOINT, RELEASE and ROLLBACK TO.
   */
  readonly statementKind: PreparedStatementKind;
  /** A BEGIN that opens a read-only transaction. */
  readonly readOnly: boolean;
}

/** A statement's effect on its session's transaction, as the owner's front end classified it. */
export type PreparedStatementKind = "begin" | "commit" | "rollback" | "savepoint" | "other";

export interface Value {
  readonly t: "value";
  readonly seq: number;
  readonly value: unknown;
}

export interface Failure {
  readonly t: "error";
  readonly seq: number;
  readonly error: WireError;
}

/**
 * A new result for subscription `seq`, sent only after the previous version was acknowledged
 * (`liveAck`, which also releases its frames); changes in between coalesce. Unacknowledged subscriptions are dropped after
 * `LIVE_UNACKED_DROP_MS` (election.ts) with `liveEnded`; a client that wakes later resubscribes.
 */
export interface Live {
  readonly t: "live";
  readonly seq: number;
  readonly version: number;
  readonly columns: readonly WireColumnMeta[] | null;
  readonly frames: readonly FramePayload[];
}

export interface LiveEnded {
  readonly t: "liveEnded";
  readonly seq: number;
  readonly reason: "unacknowledged" | "catalogChanged" | "unknown";
}

/** The owner is working on this client's requests (sent while any is in flight). */
export interface Alive {
  readonly t: "alive";
}

/** The owner is leaving. Requests without a terminal reply will be resent to the next owner. */
/**
 * The owner rolled back session `session`'s transaction on its own: idle past
 * `transactionIdleTimeoutMs`. Requests on it fail with `TransactionIdleTimeoutError` until the
 * client ends it.
 */
export interface SessionAborted {
  readonly t: "sessionAborted";
  readonly session: SessionId;
  readonly reason: "idleTimeout";
}

export interface StepDown {
  readonly t: "stepDown";
  readonly reason: "freeze" | "pagehide" | "hidden" | "superseded" | "closing" | "lockLost";
}

/** A failure that belongs to no request (maintenance, coordination), for `onOwnerError`. */
export interface Diagnostic {
  readonly t: "diagnostic";
  readonly kind: "uncaught" | "unhandledRejection" | "maintenance" | "coordination";
  readonly context: string;
  readonly error: WireError;
}

export type Reply = Rows | Done | Prepared | Value | Failure;

export type OwnerMessage =
  Welcome | Refused | Reply | Live | LiveEnded | Alive | SessionAborted | StepDown | Diagnostic;

/** Whether `message` is the last message about its seq. */
export function isTerminal(message: OwnerMessage): boolean {
  return (
    message.t === "done" ||
    message.t === "error" ||
    message.t === "prepared" ||
    message.t === "value"
  );
}

// --- Parsing -----------------------------------------------------------------------------------

/** A message that is not a protocol message or breaks its shape: the connection is closed. */
export class ProtocolError extends Error {
  readonly code = "MINNOW_PROTOCOL";
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

const CLIENT_KINDS = new Set<string>([
  "hello",
  "statement",
  "prepare",
  "begin",
  "commit",
  "rollback",
  "write",
  "subscribe",
  "admin",
  "cancel",
  "release",
  "liveness",
  "liveAck",
  "unprepare",
  "bye",
]);

const HEADER_KINDS = new Set<string>([
  "statement",
  "prepare",
  "begin",
  "commit",
  "rollback",
  "write",
  "subscribe",
  "admin",
]);

const isSeq = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 1;

/**
 * Checks the envelope of an incoming client message: its kind, and for requests the header. The
 * owner runs it on every message before anything else; payload fields are checked where they are
 * used (a bad parameter is a bind error on that request, not a protocol error).
 */
export function parseClientMessage(value: unknown): ClientMessage {
  if (typeof value !== "object" || value === null) throw new ProtocolError("Not a message");
  const kind = (value as { t?: unknown }).t;
  if (typeof kind !== "string" || !CLIENT_KINDS.has(kind))
    throw new ProtocolError(`Unknown message kind ${String(kind)}`);
  if (HEADER_KINDS.has(kind)) {
    const header = value as Partial<RequestHeader>;
    if (!isSeq(header.seq)) throw new ProtocolError("Request seq must be a positive integer");
    if (typeof header.sentAtMs !== "number" || !Number.isFinite(header.sentAtMs))
      throw new ProtocolError("Request sentAtMs must be a finite number");
    if (
      typeof header.resolvedThrough !== "number" ||
      !Number.isSafeInteger(header.resolvedThrough) ||
      header.resolvedThrough < 0 ||
      header.resolvedThrough >= header.seq
    )
      throw new ProtocolError("Request resolvedThrough must be in [0, seq)");
    if (
      typeof header.generation !== "number" ||
      !Number.isSafeInteger(header.generation) ||
      header.generation < 1
    )
      throw new ProtocolError("Request generation must be a positive integer");
    if (
      typeof header.live !== "boolean" ||
      typeof header.resendable !== "boolean" ||
      typeof header.retry !== "boolean"
    )
      throw new ProtocolError("Request live, resendable and retry must be booleans");
  }
  if (kind === "hello" && (value as Partial<Hello>).protocol === undefined)
    throw new ProtocolError("Hello without a protocol version");
  return value as ClientMessage;
}
