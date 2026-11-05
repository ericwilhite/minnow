/**
 * The owner/client wire protocol: every message that crosses the MessagePort between a client
 * (any tab or worker) and the database's one owner worker.
 *
 * ## Topology
 * - One owner worker per database holds the storage fence (election.ts). Every other context is a
 *   client. The owner tab's own client uses the same protocol over a MessageChannel to its worker.
 * - A client in another tab reaches the owner through a port handed out by the broker
 *   (election.ts `Broker`). The port carries nothing but these messages.
 * - There is no SharedArrayBuffer. Result and input buffers are **transferred** (frame.ts), never
 *   cloned: a 64 KiB clone costs 0.6–0.9 ms and 1 MiB 4–7 ms (storage probe, table 3).
 *
 * ## Connection
 * 1. The client sends `hello`. The owner answers `welcome` or `refused`. Nothing else is sent
 *    before `welcome`; requests sent early are queued by the client, not by the owner.
 * 2. A connection is one port. Sessions, cursors, prepared statements and subscriptions belong to
 *    the connection and die with it. A new port is a new connection, even to the same owner.
 * 3. There is no close event on a MessagePort (removed from the spec, 2026-10). A connection ends
 *    by `bye` (client), `stepDown` (owner), or the client's silence deadline (`OwnerUnresponsive`).
 *
 * ## Requests and replies
 * - Every request carries a `RequestHeader` with a per-client sequence `seq`. It is the ledger
 *   sequence (format/ledger.ts) and the request's id on the wire: replies, frames, cancellation
 *   and credits name it.
 * - Every request gets **exactly one terminal reply** (`done`, `error`, `prepared`, `value`),
 *   possibly preceded by `rows` frames. After its terminal reply a seq is never mentioned again.
 * - Signals (`cancel`, `credit`, `liveAck`, `unprepare`, `bye`) have no seq and no reply.
 * - The owner may answer requests out of order. Requests on one session run in arrival order.
 *
 * ## Cancellation and its acknowledgement
 * - `cancel { target }` asks the owner to stop request `target`. Its acknowledgement is the
 *   target's terminal reply, which always follows:
 *   - `error` with code `MINNOW_CANCELLED`: stopped, with no effect.
 *   - its normal reply: it finished first. A write's reply then means it committed.
 * - `cancel { target, close: true }` ends a cursor or subscription early. Its terminal reply is
 *   `done` with `summary.closedEarly`, never an error.
 * - A commit that has reached the group-commit flush cannot be stopped; cancel then waits for it.
 * - A cancel for a seq the owner has already answered, or never saw, is ignored.
 *
 * ## Exactly-once (format/ledger.ts)
 * - `sentAtMs` and `resolvedThrough` feed `RequestLedger.admit`; the receiver enforces the retry
 *   window. The client keeps `seq − resolvedThrough ≤ MAX_OUTSTANDING_REQUESTS` (sequence.ts).
 * - After a reconnect, the client resends every request it has no terminal reply for, unchanged
 *   (same seq, same `sentAtMs`), except those it already saw frames of (it fails those itself).
 * - The owner answers a recorded duplicate from the ledger: a committed write gets `done` with
 *   `summary.replayed: true` and the original counts. RETURNING rows are not replayed: the client
 *   rejects with `CommittedWithoutResultError` if the caller asked for them.
 * - `standalone` says whether a request means the same thing on a fresh connection (sequence.ts
 *   rules). A resent request (`retry`) that the ledger admits as first sight and that is not
 *   standalone is refused with `SessionLostError` and never executed. So a
 *   transaction's statements never run outside it, and a COMMIT whose fate is unknown resolves
 *   from the ledger: committed, or definitely not.
 *
 * ## Liveness (election.ts)
 * - `live: true` promises that the sender held its liveness lock when it sent the request and
 *   keeps holding it until the terminal reply. Only then does the owner watch that lock and drop
 *   the request's work (and the client's pins) when it is released.
 * - Requests are never delayed to take the lock: the client sends `live: false` until the lock is
 *   granted. Opening a cursor or a transaction (which pin a snapshot) waits for the lock.
 * - While a client has requests in flight the owner sends `alive` at least every
 *   `ALIVE_INTERVAL_MS`. The client's deadline counts silence, not wall time.
 *
 * ## Version skew
 * - `PROTOCOL_VERSION` changes with any change to these messages or to frame.ts. The owner refuses
 *   a `hello` with another version (`refused { reason: "version" }`).
 * - A newer client asks the owner to step aside (election.ts `SkewSignal`); an older client is
 *   superseded and reloads (0.19's `whenSuperseded` behaviour, engine/client.ts).
 *
 * ## Performance contract (cross-tab PK read ≤ 0.25 ms)
 * - A small result is one message each way: the request, then a `done` carrying the column
 *   metadata and the only frame inline.
 * - Prepared statements send an id, not text. A reply omits `columns` when the client already has
 *   them for that statement at that catalog version.
 * - Messages are plain objects of numbers, strings and typed arrays: no JSON, no class instances,
 *   nothing whose clone cost grows with the result.
 */
import type { PhysicalType } from "../types.js";
import type { WireError } from "./errors.js";
import type { ResultFrame, WireInputColumn } from "./frame.js";

/** Bumped by every change to a message in this file or in frame.ts. */
export const PROTOCOL_VERSION = 1;

/** The owner sends `alive` at least this often while a client has requests in flight. */
export const ALIVE_INTERVAL_MS = 1_000;
/** Default silence after which a client reports `OwnerUnresponsiveError`. */
export const DEFAULT_UNRESPONSIVE_AFTER_MS = 10_000;
/** Frames a cursor may have in flight before the client grants more, by default. */
export const DEFAULT_CURSOR_CREDITS = 2;
/** `credits` value meaning "send every frame": buffered results. */
export const UNLIMITED_CREDITS = -1;

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
  readonly standalone: boolean;
  /** Resent after a reconnect; the first send may or may not have reached an owner. */
  readonly retry: boolean;
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
   * Frames the owner may send before the client grants more (`credit`): ≥ 1, since a `done`
   * carrying a frame consumes a credit too; or `UNLIMITED_CREDITS` for a buffered result.
   */
  readonly credits: number;
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

/** Opens session `session` (client-chosen, ≥ 1) and its transaction. */
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
 * A batch write without SQL: rows as transferred columns (frame.ts). One transaction unless it
 * runs inside an open session transaction. Counts come back in `done.summary.counts`.
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

/** Lets the owner send `frames` more frames of request `target`. */
export interface Credit {
  readonly t: "credit";
  readonly target: number;
  readonly frames: number;
}

/** The client has applied live update `version` of subscription `target`: one more may follow. */
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

export type Signal = Cancel | Credit | LiveAck | Unprepare | Bye;

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
   */
  readonly reason: "version" | "steppingDown" | "superseded";
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
  readonly transaction: "idle" | "open" | "failed";
  readonly catalogVersion: number;
}

/** A non-terminal frame of request `seq`'s rows. Consumes one credit. */
export interface Rows {
  readonly t: "rows";
  readonly seq: number;
  /** Present on the first message of a result unless the client already has them. */
  readonly columns: readonly WireColumnMeta[] | null;
  readonly frame: ResultFrame;
}

export interface Done {
  readonly t: "done";
  readonly seq: number;
  readonly columns: readonly WireColumnMeta[] | null;
  /** The last (or only) frame, inline: a small result is this one message. */
  readonly frame: ResultFrame | null;
  readonly summary: StatementSummary;
}

export interface Prepared {
  readonly t: "prepared";
  readonly seq: number;
  readonly id: number;
  readonly columns: readonly WireColumnMeta[];
  readonly parameters: readonly PhysicalType[];
  readonly catalogVersion: number;
  /** The statement may open, end or change a transaction (sequence.ts `isStandalone`). */
  readonly controlsTransaction: boolean;
}

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
 * (`liveAck`); changes in between coalesce. Unacknowledged subscriptions are dropped after
 * `LIVE_UNACKED_DROP_MS` (election.ts) with `liveEnded`; a client that wakes later resubscribes.
 */
export interface Live {
  readonly t: "live";
  readonly seq: number;
  readonly version: number;
  readonly columns: readonly WireColumnMeta[] | null;
  readonly frames: readonly ResultFrame[];
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
  Welcome | Refused | Reply | Live | LiveEnded | Alive | StepDown | Diagnostic;

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
  "credit",
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
      typeof header.live !== "boolean" ||
      typeof header.standalone !== "boolean" ||
      typeof header.retry !== "boolean"
    )
      throw new ProtocolError("Request live, standalone and retry must be booleans");
  }
  if (kind === "hello" && (value as Partial<Hello>).protocol === undefined)
    throw new ProtocolError("Hello without a protocol version");
  return value as ClientMessage;
}
