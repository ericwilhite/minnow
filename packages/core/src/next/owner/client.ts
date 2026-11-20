import type { RekeyEffect } from "../access/changes.js";
import {
  SnapshotClient,
  type SnapshotExportOptions,
  type SnapshotImportOptions,
} from "./snapshot-api.js";
/**
 * The protocol client: one client's side of protocol.ts over a MessagePort — the handshake,
 * sequencing and the outstanding window (sequence.ts), resendability, frame releases, stream
 * dictionaries, cancellation, idle-timeout and step-down events, and reconnecting with resends.
 * The public API (api.ts) is built on it in increment 3; Node tests drive it over a
 * MessageChannel.
 *
 * ## Releases
 * A buffered result releases its frames as they arrive, a cursor a frame when the caller takes it.
 * Releases ride on the next request (`releases`), or go alone after `RELEASE_DELAY_MS` or once
 * half the window is unreleased (ruling R22.2). Either way the owner's window, not this
 * client, bounds what is in flight. Explicit exports use cursor credit timing, but return their
 * native buffers only when the consumer calls the frame handle's release().
 *
 * ## Reconnect
 * `reconnect(port)` opens the next generation and resends, unchanged, every request without a
 * terminal reply, except streams whose frames it already handed out (those fail with
 * `SessionLostError`). The ledger answers duplicates; a non-resendable request the new owner has
 * not seen fails with `SessionLostError` and never runs. `connect(port)`, the first connection,
 * sends the same way every request made before it (while `connecting`), in issue order, with
 * the same request ids. Neither sends again a request already posted on the new connection
 * during its handshake.
 *
 * ## Giving up (connector.ts "unresponsive")
 * `giveUp(detail)`: no owner was reachable for `DEFAULT_UNRESPONSIVE_AFTER_MS`. Every waiting
 * sent classified mutation fails with `OutcomeUnknownError` and keeps its wire seq for late
 * terminal collection. Other requests use `OwnerUnreachableError`: effect `none` if never sent,
 * `unknown` if sent and unanswered. Unknown mutation results are discarded, not buffered. A sent BEGIN or unknown statement
 * leaves its session lost, as on a reconnect. New requests fail at once (`none`) until the next
 * connection.
 *
 * ## Sessions bound to their generation
 * A session with an open transaction (any state but `idle`) belongs to the connection generation
 * it ran on. Each request knows its kind (sequence.ts "Statement kinds": typed messages, the
 * owner's `statementKind` for prepared statements, `classifyStatementText` for SQL); one that may
 * open, end or change a transaction is never resendable. On a reconnect every sent, unanswered
 * request goes back unchanged, in send order, and the new owner's ledger resolves it (protocol.ts
 * "Exactly-once"): the client decides nothing but "never sent" (`none`) and "frames handed out"
 * (`unknown`). A session open at the loss, or with a BEGIN or unknown request unanswered, is
 * lost: every new request on it fails with `SessionLostError("transaction")` without being
 * sent, until the owner confirms an explicit ROLLBACK ended it. Every failure, early or late,
 * reaches a cursor's consumer as well as its promise (`#refuse`, `#fail`).
 *
 * ## Handshake
 * `connect` and `reconnect` take an optional `AbortSignal`: the connector aborts a handshake whose
 * owner died or did not answer within `HANDSHAKE_TIMEOUT_MS`. A welcomed connection re-announces
 * the liveness lock (`liveness { held: true }`) before anything else is sent on it.
 */
import { SessionLostError, DatabaseReplacedError } from "../errors.js";
import type { PhysicalType } from "../types.js";
import { decodeUtf8 } from "../vector/varlen.js";
import {
  ClientClosedError,
  CancelledError,
  ConnectionRefusedError,
  OwnerUnreachableError,
  OutcomeUnknownError,
  OwnerUnresponsiveError,
  RequestExpiredError,
  fromWireError,
  ownerErrorRegistry,
} from "./errors.js";
import type { ErrorRegistry } from "../../worker-protocol/index.js";
import {
  StreamDictionaries,
  payloadBytes,
  type FramePayload,
  type WireFlat,
  type WireVector,
} from "./frame.js";
import {
  DEFAULT_RESULT_WINDOW_BYTES,
  DEFAULT_UNRESPONSIVE_AFTER_MS,
  RELEASE_DELAY_MS,
  LEASE_RELEASE_FAILURE_CONTEXT,
  PROTOCOL_VERSION,
  ProtocolError,
  type OwnerMessage,
  type PreparedStatementKind,
  type Request,
  type StatementSummary,
  type StepDown,
  type TransactionState,
  type Welcome,
  type WireColumnMeta,
  type WireValue,
} from "./protocol.js";
import {
  RequestSequencer,
  classifyStatementText,
  isResendable,
  type StatementKind,
} from "./sequence.js";
import { recyclingFrame } from "./recycling-frame.js";

/** Dedicated views remain valid until release(), including across cancellation/reconnect.
 * release() is idempotent and detaches the frame's typed views permanently. Dictionaries have
 * independent compact storage, shared by this stream's handles, and are not relinquished here.
 */
export interface ExportFrame {
  readonly payload: FramePayload;
  readonly dictionaries: StreamDictionaries;
  release(): void;
}

export interface ExportStream {
  readonly seq: number;
  readonly columns: Promise<readonly WireColumnMeta[]>;
  readonly frames: AsyncIterableIterator<ExportFrame>;
  readonly done: Promise<StatementSummary>;
  /** Stop iteration normally; only frames not yet handed to the consumer are relinquished. */
  close(): void;
  cancel(): void;
}

export interface ClientResult {
  readonly columns: readonly WireColumnMeta[];
  readonly frames: readonly FramePayload[];
  readonly summary: StatementSummary;
  /** Already-applied dictionaries; ordinary result buffers are never relinquished. */
  readonly dictionaries: StreamDictionaries;
  /** PREPARE's input types (not returned for ordinary statements). */
  readonly parameters?: readonly PhysicalType[];
  /** Structured administrative reply, including null/false/zero. */
  readonly value?: unknown;
  /** Decoded rows (test helper: bool, int32, int64, float64, text and dictionary text). */
  rows(): unknown[][];
}

export interface ProtocolClientOptions {
  readonly clientId?: string;
  /** The protocol this client speaks; `PROTOCOL_VERSION` (other values are for skew tests). */
  readonly protocol?: number;
  readonly build?: string;
  readonly livenessLock?: string;
  readonly now?: () => number;
  /** Client-only domain prototypes; copied once. Owner error identities cannot be overridden. */
  readonly errorRegistry?: ErrorRegistry;
}

/** Facade-owned effect classification; a statement in an explicit transaction is still
 * provisional. Cancellation is acknowledged by the owner's terminal reply, never locally. */
export interface ProtocolCallOptions {
  readonly signal?: AbortSignal;
  readonly mutation?: boolean;
}

/** Caller outcome and retained-input lifetime differ after OutcomeUnknown. */
export interface ProtocolTask {
  readonly seq: number;
  readonly promise: Promise<ClientResult>;
  /** This client will never resend the input again. Release input leases here, not on
   * caller rejection. Also resolves on proven never-sent refusal and irreversible close;
   * retirement alone is NOT proof of commitment. */
  readonly retired: Promise<void>;
}

/** Internal bridge for B6's lossless decoder/external store. Delivery is backpressured until
 * the consumer's promise settles. Resolve after atomic decode/install; reject before installation
 * to preserve the last accepted watermark. Downstream listener errors must be isolated separately.
 * Generation changes cancel old deliveries without replaying SQL.
 */
export interface ClientLiveOptions {
  onResult(
    result: ClientResult,
    version: number,
    generation: number,
    delivery: { readonly reset: boolean; readonly keyChanges: readonly RekeyEffect[] },
  ): void | Promise<void>;
  onError(error: Error): void | Promise<void>;
  onLoading?(): void;
  /** Terminal identity reset: discard rows/diffs and cancel decoding immediately, even while
   * an onResult promise is pending. A new database requires an explicit client reopen. */
  onReset?(reason: "databaseReplaced"): void;
}
export interface ClientLiveHandle {
  refresh(): void;
  close(): void;
}
type QueuedLiveDelivery =
  | {
      readonly kind: "result";
      readonly result: ClientResult;
      readonly version: number;
      readonly token: number;
      readonly delivery: { readonly reset: boolean; readonly keyChanges: readonly RekeyEffect[] };
    }
  | {
      readonly kind: "error";
      readonly error: Error;
      readonly version: number | null;
      readonly token: number;
    };

interface ClientLiveState {
  readonly sql: string;
  params: readonly WireValue[];
  readonly options: ClientLiveOptions;
  delivering: boolean;
  initialErrorHandled: boolean;
  queued: QueuedLiveDelivery | null;
  seq: number;
  generation: number;
  token: number;
  closed: boolean;
}
const EMPTY_LIVE_PARAMS: readonly WireValue[] = Object.freeze([]);

/** A request body: the header is the client's. */
type Body = Request extends infer R
  ? R extends Request
    ? Omit<
        R,
        "seq" | "sentAtMs" | "resolvedThrough" | "live" | "resendable" | "retry" | "generation"
      >
    : never
  : never;

interface Pending {
  readonly seq: number;
  readonly sentAtMs: number;
  /**
   * The connection generation it belongs to (the ledger keys it by this): the one it was first
   * sent on. A request issued while no connection was open takes the next one's.
   */
  generation: number;
  readonly resendable: boolean;
  readonly body: Body;
  /** What it does to its session's transaction (null: not a session request). */
  readonly kind: StatementKind | null;
  /** It may open, end or change a transaction: `kind` is neither `other` nor null. */
  readonly control: boolean;
  readonly session: number | null;
  columns: readonly WireColumnMeta[] | null;
  readonly frames: FramePayload[];
  /** Reset when the request is resent: a resent stream starts again at frame 0. */
  dictionaries: StreamDictionaries;
  /** Frames already handed to a cursor consumer: the request cannot be resent. */
  handedOut: boolean;
  /** Posted on a connection at least once (else it provably never reached an owner). */
  sent: boolean;
  /** The generation of the connection it was last posted on (0: never posted). */
  sentOn: number;
  readonly cursor: CursorQueue | null;
  signal: AbortSignal | undefined;
  readonly mutation: boolean;
  cancelCleanup: (() => void) | null;
  cancelRequested: boolean;
  /** Caller got unknown; retain the wire identity, but no longer retain result payloads. */
  abandoned: boolean;
  readonly resolve: (result: ClientResult) => void;
  readonly reject: (error: unknown) => void;
  readonly retire: () => void;
}

/** Frames waiting for a cursor consumer. Metadata can arrive before a result ends. */
class CursorQueue {
  readonly frames: FramePayload[] = [];
  done: StatementSummary | null = null;
  error: Error | null = null;
  wake: (() => void) | null = null;
  stopped = false;
  dictionaries = new StreamDictionaries();
  readonly columns: Promise<readonly WireColumnMeta[]>;
  resolveColumns: (columns: readonly WireColumnMeta[]) => void = () => undefined;
  rejectColumns: (error: Error) => void = () => undefined;
  constructor() {
    this.columns = new Promise((resolve, reject) => {
      this.resolveColumns = resolve;
      this.rejectColumns = reject;
    });
    this.columns.catch(() => undefined);
  }
}

class ExportQueue extends CursorQueue {
  readonly handles = new WeakMap<FramePayload, ExportFrame>();
}

export interface PreparedInfo {
  readonly id: number;
  readonly columns: readonly WireColumnMeta[];
  readonly parameters: readonly PhysicalType[];
  readonly catalogVersion: number;
}
export interface OrdinaryCursor {
  readonly seq: number;
  readonly columns: Promise<readonly WireColumnMeta[]>;
  readonly done: Promise<StatementSummary>;
  readonly frames: AsyncIterableIterator<FramePayload>;
  /** Stable immutable entry prefix for a Result retained after this cursor closes. */
  snapshotDictionaries(): StreamDictionaries;
  close(): void;
}

const randomClientId = (): string => {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
};

function valueAt(column: WireVector, row: number, dictionaries: StreamDictionaries): unknown {
  if (column.form === "constant") return flatValue(column.value, 0);
  if (column.form === "dictionary") {
    if (column.validity !== null && ((column.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
      return null;
    const { entries, index } = dictionaries.locate(column.dictionary, column.codes[row] ?? 0);
    return flatValue(entries, index);
  }
  return flatValue(column, row);
}

function flatValue(column: WireFlat, row: number): unknown {
  if (column.validity !== null && ((column.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
    return null;
  const data = column.data;
  switch (data.layout) {
    case "bool":
      return data.values[row] === 1;
    case "i32":
    case "f64":
    case "f32":
      return data.values[row];
    case "int64":
      return data.values.repr === "safe" ? data.values.values[row] : data.values.values[row];
    case "varlen":
      return decodeUtf8(data, row);
    default:
      return `<${data.layout}>`;
  }
}

function makeResult(
  columns: readonly WireColumnMeta[],
  frames: readonly FramePayload[],
  summary: StatementSummary,
  dictionaries: StreamDictionaries,
  extra?: Pick<Partial<ClientResult>, "parameters" | "value">,
): ClientResult {
  return {
    columns,
    frames,
    summary,
    dictionaries,
    ...extra,
    rows: () => {
      const rows: unknown[][] = [];
      for (const frame of frames)
        for (let row = 0; row < frame.rowCount; row++)
          rows.push(frame.columns.map((column) => valueAt(column, row, dictionaries)));
      return rows;
    },
  };
}

export class ProtocolClient {
  readonly clientId: string;
  readonly #build: string;
  /**
   * The liveness lock this client holds and announces in `hello` (election.ts `clientLockName`).
   * The connector sets it before connecting; it must name the lock actually held, or the owner
   * watches a free lock and ends the connection.
   */
  livenessLock: string;
  readonly #now: () => number;
  readonly #errors: ErrorRegistry;
  readonly protocol: number;
  readonly #sequencer = new RequestSequencer();
  /** Each session's transaction state, from replies: a non-idle session pins a snapshot. */
  readonly #sessions = new Map<number, TransactionState>();
  /**
   * Why each session is lost, as the sequence numbers of its loss causes (B5-p2d review 1, B1): 0
   * for a transaction open when a connection was lost, else an unanswered BEGIN or unknown
   * request. A boundary the owner confirms left the session idle (a ROLLBACK, a COMMIT answered
   * from the ledger) discharges only the causes before it; the session is lost while any cause
   * remains. So a replayed COMMIT does not clear a later lost BEGIN.
   */
  readonly #lostCauses = new Map<number, number[]>();
  /** Whether the connector holds the liveness lock (re-announced on every new connection). */
  #livenessHeld = false;
  #cursors = 0;
  readonly #pending = new Map<number, Pending>();
  readonly #live = new Set<ClientLiveState>();
  readonly #liveBySeq = new Map<number, ClientLiveState>();
  readonly #session0 = { transaction: "idle" as TransactionState, unresolvedControl: 0 };
  #port: MessagePort | null = null;
  #generation = 0;
  #ownerId: string | null = null;
  #ownerLockHolder: string | null = null;
  #ownerEpoch = 0;
  #lastHeardAt = 0;
  #lastMessageAt = 0;
  /** Why the client gave up on reaching an owner (`giveUp`), until the next connection. */
  #unreachable: string | null = null;
  #welcome: { resolve: (welcome: Welcome) => void; reject: (error: Error) => void } | null = null;
  #prepared = 0;
  /** Prepared id → its statement kind, as the owner's front end classified it (`Prepared`). */
  readonly #preparedKinds = new Map<number, PreparedStatementKind>();
  /** Releases waiting for a request to ride on, by target. */
  readonly #releases = new Map<number, { through: number; bytes: number }>();
  #releaseTimer: ReturnType<typeof setTimeout> | null = null;
  #closed = false;
  #databaseId: bigint | null = null;
  #replaced: DatabaseReplacedError | null = null;
  onSessionAborted: ((session: number) => void) | null = null;
  /** Called whenever requests in flight or pins change (the connector's liveness and watcher). */
  onActivity: (() => void) | null = null;
  onStepDown: ((reason: StepDown["reason"]) => void) | null = null;
  /** Stops reconnecting: replacement requires a fresh client, never transparent replay. */
  onDatabaseReplaced: ((error: DatabaseReplacedError) => void) | null = null;
  onOwnerError: ((event: { kind: string; context: string; error: Error }) => void) | null = null;

  constructor(options: ProtocolClientOptions = {}) {
    this.clientId = options.clientId ?? randomClientId();
    this.#build = options.build ?? "test";
    this.livenessLock = options.livenessLock ?? `client:${this.clientId}`;
    this.#now = options.now ?? Date.now;
    this.protocol = options.protocol ?? PROTOCOL_VERSION;
    const errors = new Map(options.errorRegistry);
    for (const [name, constructor] of ownerErrorRegistry) errors.set(name, constructor);
    this.#errors = errors;
  }

  /** Requests sent and not yet answered. */
  get pendingCount(): number {
    return this.#pending.size;
  }

  /** Retained transaction-state entries (ordinary diagnostics, not live subscriptions). */
  get retainedSessionCount(): number {
    return this.#sessions.size;
  }
  get retainedPreparedCount(): number {
    return this.#preparedKinds.size;
  }
  get liveCount(): number {
    return this.#live.size;
  }

  /** Whether the owner holds pins for this client: an open transaction or an open cursor. */
  get hasPins(): boolean {
    if (this.#cursors > 0 || this.#live.size > 0) return true;
    for (const state of this.#sessions.values()) if (state !== "idle") return true;
    return false;
  }

  /** Whether a connection is open (welcomed and not lost). */
  get connected(): boolean {
    return this.#port !== null && this.#welcome === null;
  }

  get generation(): number {
    return this.#generation;
  }

  /** The current owner's id (`Welcome.ownerId`), or null before the first welcome. */
  get ownerId(): string | null {
    return this.#ownerId;
  }

  /** The current owner's fence epoch (`Welcome.epoch`), 0 before the first welcome. */
  get ownerEpoch(): number {
    return this.#ownerEpoch;
  }

  /** The Web Locks client id holding the current owner's host lock, if the owner said. */
  get ownerLockHolder(): string | null {
    return this.#ownerLockHolder;
  }

  /**
   * When the client last heard from its owner (any message), or when its requests came into
   * flight if later (`now()` clock): the connector's silence deadline (protocol.ts
   * `SILENCE_DEADLINE_MS`) counts from here.
   */
  get lastHeardAt(): number {
    return this.#lastHeardAt;
  }

  /** When the last message from an owner arrived (`now()` clock), 0 before any. */
  get lastMessageAt(): number {
    return this.#lastMessageAt;
  }

  /** Asks the owner to answer `alive` on the current connection (protocol.ts `Ping`). */
  ping(): void {
    if (this.connected) this.#port?.postMessage({ t: "ping" });
  }

  get resolvedThrough(): number {
    return this.#sequencer.resolvedThrough;
  }

  /** Opens the first connection. */
  async connect(port: MessagePort, signal?: AbortSignal): Promise<Welcome> {
    const welcome = await this.#open(port, signal);
    this.#unreachable = null;
    // Requests made while connecting were never sent: they go now, as a reconnect's would.
    for (const live of this.#live) live.generation = this.#generation;
    this.#resendPending(true);
    return welcome;
  }

  /** Sessions a reconnect lost, refused until ended (see "Sessions bound to their generation"). */
  get lostSessions(): ReadonlySet<number> {
    return new Set(this.#lostCauses.keys());
  }

  #isLost(session: number): boolean {
    return this.#lostCauses.has(session);
  }

  #addCause(session: number, seq: number): void {
    const causes = this.#lostCauses.get(session);
    if (causes === undefined) this.#lostCauses.set(session, [seq]);
    else if (!causes.includes(seq)) causes.push(seq);
  }

  /** The owner confirmed request `seq` left `session` idle: the causes before it are discharged. */
  #discharge(session: number, seq: number): void {
    const causes = this.#lostCauses.get(session);
    if (causes === undefined) return;
    const left = causes.filter((cause) => cause > seq);
    if (left.length === 0) this.#lostCauses.delete(session);
    else this.#lostCauses.set(session, left);
  }

  /** Opens the next generation on `port` and resends unanswered requests. */
  async reconnect(port: MessagePort, signal?: AbortSignal): Promise<Welcome> {
    const welcome = await this.#open(port, signal);
    // Every session with a transaction open on the old generation is lost with it: a cause before
    // every unanswered request.
    for (const [session, state] of this.#sessions) if (state !== "idle") this.#addCause(session, 0);
    if (this.#session0.transaction !== "idle") this.#addCause(0, 0);
    for (const session of this.#lostCauses.keys()) this.#sessions.set(session, "idle");
    this.#session0.transaction = "idle";
    this.#unreachable = null;
    this.#restartLives();
    this.#resendPending(false);
    return welcome;
  }

  /**
   * Sends every request without a terminal reply on the new connection (`connect`, `reconnect`):
   * once, in issue order, with its original request id. A request already posted on this very
   * connection (issued during its handshake) is not sent again.
   */
  #resendPending(first: boolean): void {
    // In send order. Nothing is decided here: every request that was sent goes back to the new
    // owner unchanged, whose ledger answers it (a recorded outcome, or a refusal saying what its
    // absence proves). A request that depended on the old connection is not resendable, so the
    // new owner never runs it.
    for (const pending of [...this.#pending.values()].sort((a, b) => a.seq - b.seq)) {
      if (pending.sentOn === this.#generation) continue;
      const { session, kind } = pending;
      // An unanswered BEGIN (or unknown text) may have opened a transaction there: a cause at its
      // place in the sequence, discharged only by a confirmed end after it. On the first
      // connection only one actually sent (on a handshake that failed) can have: one made while
      // connecting and never sent opened nothing, and goes now like any other.
      if (session !== null && (kind === "begin" || kind === "unknown") && (!first || pending.sent))
        this.#addCause(session, pending.seq);
      if (!pending.sent && session !== null && this.#isLost(session) && kind !== "rollback") {
        // Never sent, so nothing took effect; on the new owner it would run outside the lost
        // transaction.
        this.#fail(pending, new SessionLostError("transaction", pending.seq, "none"));
        continue;
      }
      if (pending.handedOut) {
        // Frames already reached the caller, so it cannot start again. An autocommit write's
        // RETURNING frames come only after its commit: the effect is not known to be none.
        this.#fail(pending, new SessionLostError("cursor", pending.seq, "unknown"));
        continue;
      }
      pending.frames.length = 0;
      pending.dictionaries = new StreamDictionaries();
      if (pending.cursor !== null) pending.cursor.dictionaries = pending.dictionaries;
      pending.columns = null;
      this.#send(pending, pending.sent);
    }
  }

  /**
   * No owner was reachable for the unresponsive deadline (connector.ts): every waiting request
   * sent classified mutation fails with `OutcomeUnknownError` without settling its sequence.
   * Other requests retain `OwnerUnreachableError`, effect `none` if never sent, `unknown` if sent. A sent BEGIN or unknown statement leaves its session lost (a cause at its place, as
   * on a reconnect). New requests fail at once until the next connection.
   */
  giveUp(
    detail: string,
    cause: Error = new OwnerUnresponsiveError(DEFAULT_UNRESPONSIVE_AFTER_MS),
  ): void {
    if (this.#closed) return;
    this.#unreachable = detail;
    for (const pending of [...this.#pending.values()].sort((a, b) => a.seq - b.seq)) {
      const effect = pending.sent ? "unknown" : "none";
      if (
        pending.sent &&
        pending.session !== null &&
        (pending.kind === "begin" || pending.kind === "unknown")
      )
        this.#addCause(pending.session, pending.seq);
      if (pending.sent && pending.mutation) {
        if (!pending.abandoned) {
          pending.abandoned = true;
          pending.frames.length = 0; // Buffered frames already returned their owner credit.
          pending.columns = null;
          pending.dictionaries = new StreamDictionaries();
          const error = new OutcomeUnknownError("unresponsive", { cause });
          if (pending.cursor !== null) {
            if (pending.cursor instanceof ExportQueue) {
              const window =
                "result" in pending.body
                  ? pending.body.result.windowBytes
                  : DEFAULT_RESULT_WINDOW_BYTES;
              this.#discardExport(
                pending.cursor,
                pending.seq,
                window,
                pending.sentOn === this.#generation,
              );
              pending.cursor.rejectColumns(error);
            } else {
              this.#releaseFrames(pending, pending.cursor.frames);
              pending.cursor.frames.length = 0;
            }
            pending.cursor.error = error;
            pending.cursor.wake?.();
          }
          pending.reject(error);
          queueMicrotask(() => this.onActivity?.());
        }
      } else
        this.#fail(pending, new OwnerUnreachableError(detail, { sequence: pending.seq, effect }));
    }
  }

  /** Whether the client gave up (`giveUp`) and has not connected since. */
  get gaveUp(): boolean {
    return this.#unreachable !== null;
  }

  #open(port: MessagePort, signal?: AbortSignal): Promise<Welcome> {
    if (this.#replaced !== null) {
      port.close();
      return Promise.reject(this.#replaced);
    }
    // Releases name the old connection's streams, which ended with it.
    this.#takeReleases();
    this.#port?.close();
    this.#port = port;
    this.#generation++;
    port.onmessage = (event: MessageEvent) => {
      if (this.#port !== port) return;
      this.#receive(event.data as OwnerMessage);
    };
    return new Promise<Welcome>((resolve, reject) => {
      const onAbort = () => {
        if (this.#port !== port || this.#welcome === null) return;
        this.#welcome = null;
        this.#port = null;
        port.close();
        reject(new OwnerUnreachableError("the owner did not answer the handshake"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
      this.#welcome = {
        resolve: (welcome) => {
          signal?.removeEventListener("abort", onAbort);
          resolve(welcome);
        },
        reject: (error) => {
          signal?.removeEventListener("abort", onAbort);
          reject(error);
        },
      };
      if (signal?.aborted === true) {
        onAbort();
        return;
      }
      port.postMessage({
        t: "hello",
        databaseId: this.#databaseId,
        protocol: this.protocol,
        build: this.#build,
        clientId: this.clientId,
        livenessLock: this.livenessLock,
        previousOwnerId: this.#ownerId,
        generation: this.#generation,
      });
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const live of this.#live) {
      live.closed = true;
      live.token++;
      live.queued = null;
      live.params = EMPTY_LIVE_PARAMS;
    }
    this.#live.clear();
    this.#liveBySeq.clear();
    // Pending releases and their timer go: `bye` makes the owner release every frame anyway.
    this.#takeReleases();
    this.#port?.postMessage({ t: "bye" });
    this.#port?.close();
    const welcome = this.#welcome;
    this.#welcome = null;
    welcome?.reject(new ClientClosedError());
    for (const pending of [...this.#pending.values()]) this.#fail(pending, new ClientClosedError());
    this.#sessions.clear();
    this.#lostCauses.clear();
    this.#preparedKinds.clear();
    this.#session0.transaction = "idle";
  }

  // ---- live subscriptions (fresh reads, separate from mutation resends) ----

  subscribeLive(
    sql: string,
    params: readonly WireValue[],
    options: ClientLiveOptions,
  ): ClientLiveHandle {
    if (this.#closed) throw new ClientClosedError();
    if (this.#replaced !== null) throw this.#replaced;
    const state: ClientLiveState = {
      sql,
      params: structuredClone(params),
      options,
      delivering: false,
      initialErrorHandled: false,
      queued: null,
      seq: -1,
      generation: this.#generation,
      token: 0,
      closed: false,
    };
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- parameter getters can close the client during structuredClone.
    if (this.#closed) throw new ClientClosedError();
    this.#live.add(state);
    this.#startLive(state);
    this.onActivity?.();
    return {
      refresh: () => {
        if (!state.closed) {
          this.#retireLive(state);
          this.#startLive(state);
        }
      },
      close: () => {
        if (!state.closed) {
          state.closed = true;
          this.#retireLive(state);
          state.params = EMPTY_LIVE_PARAMS;
          this.#live.delete(state);
          this.onActivity?.();
        }
      },
    };
  }
  #retireLive(state: ClientLiveState): void {
    state.token++;
    state.queued = null;
    this.#liveBySeq.delete(state.seq);
    const pending = this.#pending.get(state.seq);
    if (pending !== undefined) this.#fail(pending, new SessionLostError("result"));
    if (state.generation === this.#generation)
      this.#port?.postMessage({ t: "cancel", target: state.seq, close: true });
  }
  #startLive(state: ClientLiveState): void {
    if (this.#replaced !== null) {
      this.#liveError(state, this.#replaced);
      return;
    }
    const token = ++state.token;
    const generation = this.#generation;
    state.initialErrorHandled = false;
    state.generation = generation;
    try {
      state.options.onLoading?.();
    } catch {
      /* Consumer isolation. */
    }
    // Loading may close, refresh or reconnect synchronously. Only its current job registers.
    if (state.closed || this.#closed || state.token !== token || generation !== this.#generation)
      return;
    const request = this.#request({ t: "subscribe", text: state.sql, params: state.params });
    state.seq = request.seq;
    this.#liveBySeq.set(state.seq, state);
    void request.promise.catch((error: unknown) => {
      if (
        !state.closed &&
        state.token === token &&
        generation === this.#generation &&
        !state.initialErrorHandled
      )
        this.#liveError(state, error);
    });
  }
  #restartLives(): void {
    for (const state of this.#live) {
      this.#retireLive(state);
      this.#startLive(state);
    }
  }
  #liveError(state: ClientLiveState, error: unknown, version: number | null = null): void {
    this.#queueLive(state, {
      kind: "error",
      error: error instanceof Error ? error : new Error(String(error)),
      version,
      token: state.token,
    });
  }
  #deliverLive(
    state: ClientLiveState,
    result: ClientResult,
    version: number,
    delivery: { readonly reset: boolean; readonly keyChanges: readonly RekeyEffect[] },
  ): void {
    this.#queueLive(state, { kind: "result", result, version, delivery, token: state.token });
  }
  #queueLive(state: ClientLiveState, job: QueuedLiveDelivery): void {
    if (state.closed || job.token !== state.token) return;
    if (state.delivering) {
      state.queued = job;
      return;
    }
    const generation = this.#generation;
    state.delivering = true;
    let accepted = false;
    void Promise.resolve()
      .then(() => {
        if (state.closed || job.token !== state.token) return;
        if (job.kind === "error") return state.options.onError(job.error);
        return state.options.onResult(job.result, job.version, generation, job.delivery);
      })
      .then(
        () => {
          accepted = job.kind === "result";
        },
        (error: unknown) => {
          // A failed error listener is isolated; it must not recursively enqueue another error.
          if (job.kind === "result" && !state.closed && state.token === job.token)
            this.#liveError(state, error);
        },
      )
      .finally(() => {
        state.delivering = false;
        if (
          job.version !== null &&
          !state.closed &&
          job.token === state.token &&
          generation === this.#generation
        )
          this.#port?.postMessage({
            t: "liveAck",
            target: state.seq,
            version: job.version,
            accepted,
          });
        const queued = state.queued;
        state.queued = null;
        if (!state.closed && queued?.token === state.token) this.#queueLive(state, queued);
      });
  }

  // ---- requests ----

  query(
    sql: string,
    params: readonly WireValue[] = [],
    options: {
      readonly session?: number;
      readonly windowBytes?: number;
      readonly wantRows?: boolean;
      readonly signal?: AbortSignal;
      readonly mutation?: boolean;
    } = {},
  ): Promise<ClientResult> {
    return this.#request(
      {
        t: "statement",
        session: options.session ?? 0,
        text: sql,
        prepared: null,
        knownColumnsVersion: -1,
        params,
        result: {
          windowBytes: options.windowBytes ?? DEFAULT_RESULT_WINDOW_BYTES,
          frameRows: null,
          wantRows: options.wantRows ?? true,
        },
      },
      null,
      options,
    ).promise;
  }

  prepare(sql: string, options: ProtocolCallOptions = {}): Promise<PreparedInfo> {
    return this.prepareTask(sql, undefined, options).promise;
  }

  /** Same opaque request, with scoped metadata and terminal-aware cancellation. */
  prepareTask(
    sql: string,
    session?: number,
    options: ProtocolCallOptions = {},
  ): {
    readonly seq: number;
    readonly promise: Promise<PreparedInfo>;
    readonly retired: Promise<void>;
  } {
    const id = ++this.#prepared;
    const task = this.#request(
      {
        t: "prepare",
        id,
        text: sql,
        ...(session === undefined ? {} : { session }),
      },
      null,
      options,
    );
    return {
      seq: task.seq,
      retired: task.retired,
      promise: task.promise.then((result) => ({
        id,
        columns: result.columns,
        parameters: result.parameters ?? [],
        catalogVersion: result.summary.catalogVersion,
      })),
    };
  }

  /** Releases a connection-scoped prepared handle. It cannot survive a reconnect. */
  unprepare(id: number): void {
    this.#preparedKinds.delete(id);
    if (!this.#closed) this.#port?.postMessage({ t: "unprepare", id });
  }

  execute(
    id: number,
    params: readonly WireValue[] = [],
    session = 0,
    knownColumnsVersion = -1,
    options: ProtocolCallOptions = {},
  ): Promise<ClientResult> {
    return this.#request(
      {
        t: "statement",
        session,
        text: null,
        prepared: id,
        knownColumnsVersion,
        params,
        result: { windowBytes: DEFAULT_RESULT_WINDOW_BYTES, frameRows: null, wantRows: true },
      },
      null,
      options,
    ).promise;
  }

  begin(
    session: number,
    readOnly = false,
    options: ProtocolCallOptions = {},
  ): Promise<ClientResult> {
    return this.#request({ t: "begin", session, readOnly }, null, options).promise;
  }

  commit(session: number, options: ProtocolCallOptions = {}): Promise<ClientResult> {
    return this.#request({ t: "commit", session }, null, { ...options, mutation: true }).promise;
  }

  rollback(session: number, options: ProtocolCallOptions = {}): Promise<ClientResult> {
    return this.#request({ t: "rollback", session }, null, options).promise;
  }

  /** An ended callback never reuses its positive session id. Forget local recovery state
   * only after all requests/cursors settled; default-session loss protection is unchanged. */
  retireSession(session: number): void {
    if (!Number.isSafeInteger(session) || session <= 0)
      throw new TypeError("Only a callback session may retire");
    for (const pending of this.#pending.values())
      if (pending.session === session)
        throw new TypeError("A callback session still has pending requests");
    this.#sessions.delete(session);
    this.#lostCauses.delete(session);
  }

  #snapshotClient: SnapshotClient | undefined;
  #snapshots(): SnapshotClient {
    return (this.#snapshotClient ??= new SnapshotClient(
      async (op, args, signal) => {
        signal?.throwIfAborted();
        const { seq, promise } = this.#request({ t: "admin", op, args });
        const abort = () => this.cancel(seq);
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) abort();
        try {
          return (await promise).value;
        } catch (error) {
          if (signal?.aborted && error instanceof CancelledError) throw signal.reason;
          throw error;
        } finally {
          signal?.removeEventListener("abort", abort);
        }
      },
      () => {
        this.#cursors++;
        this.onActivity?.();
        return () => {
          this.#cursors--;
          this.onActivity?.();
        };
      },
    ));
  }
  exportSnapshot(options?: SnapshotExportOptions): Promise<Uint8Array> {
    return this.#snapshots().exportSnapshot(options);
  }
  exportSnapshotStream(options?: SnapshotExportOptions): AsyncGenerator<Uint8Array, void, void> {
    return this.#snapshots().exportSnapshotStream(options);
  }
  importSnapshot(bytes: Uint8Array, options?: SnapshotImportOptions): Promise<void> {
    return this.#snapshots().importSnapshot(bytes, options);
  }
  importSnapshotStream(
    source: AsyncIterable<Uint8Array>,
    options?: SnapshotImportOptions,
  ): Promise<void> {
    return this.#snapshots().importSnapshotStream(source, options);
  }

  /** Sends a request and returns its seq with its promise (for cancellation and retries in tests). */
  send(body: Body, options: ProtocolCallOptions = {}): ProtocolTask {
    return this.#request(body, null, options);
  }

  /** A cursor: frames released as they are taken. */
  cursor(
    sql: string,
    params: readonly WireValue[] = [],
    options: {
      readonly windowBytes?: number;
      readonly session?: number;
      readonly frameRows?: number;
      readonly queryOnly?: boolean;
      readonly prepared?: number;
    } = {},
  ): OrdinaryCursor {
    const queue = new CursorQueue();
    const { seq, promise } = this.#request(
      {
        t: "statement",
        session: options.session ?? 0,
        text: sql,
        prepared: options.prepared ?? null,
        ...(options.queryOnly === undefined ? {} : { queryOnly: options.queryOnly }),
        knownColumnsVersion: -1,
        params,
        result: {
          windowBytes: options.windowBytes ?? DEFAULT_RESULT_WINDOW_BYTES,
          frameRows: options.frameRows ?? null,
          wantRows: true,
        },
      },
      queue,
    );
    const done = promise.then((result) => result.summary);
    done.catch(() => undefined);
    const stop = () => {
      if (queue.stopped) return;
      queue.stopped = true;
      queue.frames.length = 0;
      this.cancel(seq, true);
      queue.dictionaries = new StreamDictionaries();
      queue.wake?.();
    };
    const window = options.windowBytes ?? DEFAULT_RESULT_WINDOW_BYTES;
    const release = (frame: FramePayload) => {
      this.#queueRelease(seq, frame.sequence, payloadBytes(frame), window);
    };
    const frames: AsyncIterableIterator<FramePayload> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next: async (): Promise<IteratorResult<FramePayload>> => {
        for (;;) {
          if (queue.stopped) return { value: undefined, done: true };
          const frame = queue.frames.shift();
          if (frame !== undefined) {
            release(frame);
            return { value: frame, done: false };
          }
          if (queue.error !== null) throw queue.error;
          if (queue.done !== null) return { value: undefined, done: true };
          await new Promise<void>((resolve) => {
            queue.wake = resolve;
          });
        }
      },
      return: async () => {
        stop();
        await done.catch(() => undefined);
        return { value: undefined, done: true };
      },
    };
    return {
      seq,
      frames,
      columns: queue.columns,
      done,
      snapshotDictionaries: () => queue.dictionaries.snapshot(),
      close: stop,
    };
  }

  cancel(seq: number, close = false): void {
    if (seq < 0) return; // Refused before a sequence was allocated.
    const pending = this.#pending.get(seq);
    if (pending && !close) pending.cancelRequested = true;
    if (pending !== undefined && !pending.sent) {
      if (!close) this.#fail(pending, new CancelledError());
      else {
        this.#settle(seq);
        const summary: StatementSummary = {
          ...emptySummary(),
          closedEarly: true,
          transaction:
            pending.session === 0
              ? this.#session0.transaction
              : pending.session === null
                ? "idle"
                : (this.#sessions.get(pending.session) ?? "idle"),
        };
        if (pending.cursor) {
          pending.cursor.done = summary;
          pending.cursor.resolveColumns([]);
          pending.cursor.wake?.();
        }
        pending.resolve(makeResult([], [], summary, pending.dictionaries));
      }
      return;
    }
    this.#port?.postMessage({ t: "cancel", target: seq, close });
  }

  /** Facade adapter for an already-issued task. The SAME pending request owns its listener. */
  attachSignal(seq: number, signal: AbortSignal): void {
    const pending = this.#pending.get(seq);
    if (pending) this.#bindSignal(pending, signal);
  }

  #bindSignal(pending: Pending, signal: AbortSignal): void {
    if (pending.signal && pending.signal !== signal)
      throw new TypeError("A request already has a different cancellation signal");
    if (pending.cancelCleanup) return;
    pending.signal = signal;
    const abort = () => {
      if (this.#pending.has(pending.seq)) this.cancel(pending.seq);
    };
    signal.addEventListener("abort", abort, { once: true });
    pending.cancelCleanup = () => signal.removeEventListener("abort", abort);
    if (signal.aborted) abort();
  }

  /** Explicit columnar streaming export. Credits return on handout, buffers only on release. */
  exportStream(
    sql: string,
    params: readonly WireValue[] = [],
    options: { readonly windowBytes?: number; readonly session?: number } = {},
  ): ExportStream {
    const queue = new ExportQueue();
    const window = options.windowBytes ?? DEFAULT_RESULT_WINDOW_BYTES;
    const { seq, promise } = this.#request(
      {
        t: "statement",
        session: options.session ?? 0,
        text: sql,
        prepared: null,
        knownColumnsVersion: -1,
        params,
        result: { windowBytes: window, frameRows: null, wantRows: true, recycle: true },
      },
      queue,
    );
    const done = promise.then((result) => result.summary);
    done.catch(() => undefined);
    const stop = (cancel: boolean) => {
      queue.stopped = true;
      this.#discardExport(queue, seq, window);
      if (cancel) queue.error = new CancelledError();
      const pending = this.#pending.get(seq);
      if (pending !== undefined && !pending.sent) {
        this.#fail(pending, new CancelledError());
        return;
      }
      this.cancel(seq, !cancel);
      queue.wake?.();
    };
    let pulling = false;
    const frames: AsyncIterableIterator<ExportFrame> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next: async (): Promise<IteratorResult<ExportFrame>> => {
        if (pulling) throw new TypeError("Only one export next() may be pending");
        pulling = true;
        try {
          for (;;) {
            const frame = queue.frames.shift();
            if (frame !== undefined) {
              const handle = queue.handles.get(frame);
              if (!handle) throw new Error("Missing export frame handle");
              queue.handles.delete(frame);
              this.#queueRelease(seq, frame.sequence, payloadBytes(frame), window);
              return { value: handle, done: false };
            }
            if (queue.error !== null) throw queue.error;
            if (queue.stopped || queue.done !== null) return { value: undefined, done: true };
            await new Promise<void>((resolve) => {
              queue.wake = resolve;
            });
          }
        } finally {
          pulling = false;
        }
      },
      return: () => {
        stop(false);
        return Promise.resolve({ value: undefined, done: true });
      },
    };
    return {
      seq,
      columns: queue.columns,
      frames,
      done,
      close: () => stop(false),
      cancel: () => stop(true),
    };
  }

  #discardExport(queue: ExportQueue, seq: number, window: number, credit = true): void {
    for (const frame of queue.frames.splice(0)) {
      const bytes = payloadBytes(frame);
      queue.handles.get(frame)?.release();
      queue.handles.delete(frame);
      if (credit) this.#queueRelease(seq, frame.sequence, bytes, window);
    }
  }

  #exportFrame(
    frame: FramePayload,
    ids: readonly number[],
    dictionaries: StreamDictionaries,
  ): ExportFrame {
    const port = this.#port,
      generation = this.#generation;
    const handle = recyclingFrame(frame, ids, (buffers, transfer) => {
      // A capability belongs to the connection that issued it. Never route it through reconnect.
      if (
        !this.#closed &&
        port !== null &&
        this.#port === port &&
        this.#generation === generation
      ) {
        try {
          port.postMessage({ t: "returnBuffers", through: frame.sequence, buffers }, transfer);
          return;
        } catch {
          /* A lost return is disposal, never a reason to wait or replay on another owner. */
        }
      }
      const live = transfer.filter((buffer) => buffer.byteLength > 0);
      if (live.length > 0) structuredClone(null, { transfer: live });
    });
    return { ...handle, dictionaries };
  }

  /**
   * Declares the liveness lock held, or about to be released (protocol.ts `liveness`). Remembered:
   * a held lock is announced again on every new connection, before its resends.
   */
  liveness(held: boolean): void {
    this.#livenessHeld = held;
    if (this.connected) this.#port?.postMessage({ t: "liveness", held });
  }

  /** What `body` does to its session's transaction (sequence.ts "Statement kinds"). */
  #kindOf(body: Body): StatementKind | null {
    switch (body.t) {
      case "begin":
      case "commit":
      case "rollback":
        return body.t;
      case "write":
        return "other";
      case "statement":
        // Prepared: the owner's front end said; an id it never answered for is unknown.
        if (body.text === null)
          return body.prepared === null
            ? "unknown"
            : (this.#preparedKinds.get(body.prepared) ?? "unknown");
        return classifyStatementText(body.text);
      default:
        return null;
    }
  }

  #request(
    body: Body,
    cursor: CursorQueue | null = null,
    options: ProtocolCallOptions = {},
  ): ProtocolTask {
    if (options.signal?.aborted)
      return this.#refuse(new CancelledError(), cursor, options.signal.reason);
    if (this.#replaced !== null) return this.#refuse(this.#replaced, cursor);
    if (this.#closed) return this.#refuse(new ClientClosedError(), cursor);
    if (this.#unreachable !== null) {
      // Gave up on reaching an owner: never sent, so nothing takes effect. Still activity: the
      // connector tries to connect again.
      const refused = this.#refuse(
        new OwnerUnreachableError(this.#unreachable, { effect: "none" }),
        cursor,
      );
      this.onActivity?.();
      return refused;
    }
    const session = "session" in body ? (body.session ?? null) : null;
    const kind = this.#kindOf(body);
    const control = kind !== null && kind !== "other";
    // Never sent, so nothing took effect: on the new owner it would run outside the lost
    // transaction (autocommit). Only an explicit end (ROLLBACK, ABORT) goes through, and the
    // session stays lost until the owner's reply confirms it.
    if (session !== null && this.#isLost(session) && kind !== "rollback")
      return this.#refuse(new SessionLostError("transaction", undefined, "none"), cursor);
    const seq = this.#sequencer.issue();
    if (seq === null)
      return this.#refuse(new RangeError("Outstanding request window is full"), cursor);
    // A request that may open, end or change a transaction never means the same on a fresh
    // connection: it is never resendable, so a new owner refuses it rather than running it.
    const resendable = !control && isResendable(body, this.#session0);
    let resolve: (result: ClientResult) => void = () => undefined;
    let reject: (error: unknown) => void = () => undefined;
    const promise = new Promise<ClientResult>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    let retire: () => void = () => undefined;
    const retired = new Promise<void>((resolve) => {
      retire = resolve;
    });
    const pending: Pending = {
      seq,
      sentAtMs: this.#now(),
      generation: this.#generation,
      resendable,
      body,
      kind,
      control,
      session,
      columns: null,
      frames: [],
      dictionaries: new StreamDictionaries(),
      handedOut: false,
      sent: false,
      sentOn: 0,
      cursor,
      signal: undefined,
      mutation:
        body.t === "write" ||
        kind === "commit" ||
        (body.t === "statement" &&
          options.mutation === true &&
          (kind === "other" || kind === "unknown")),
      cancelCleanup: null,
      cancelRequested: false,
      abandoned: false,
      resolve,
      reject,
      retire,
    };
    if (control && session === 0) this.#session0.unresolvedControl++;
    // Silence is counted from when requests came into flight, not from an idle stretch before.
    if (this.#pending.size === 0) this.#lastHeardAt = this.#now();
    this.#pending.set(seq, pending);
    if (cursor !== null) {
      cursor.dictionaries = pending.dictionaries;
      this.#cursors++;
      const closeCursor = () => {
        this.#cursors--;
        this.onActivity?.();
      };
      promise.then(closeCursor, closeCursor);
    }
    if (options.signal) this.#bindSignal(pending, options.signal);
    if (this.#pending.has(seq)) this.#send(pending, false);
    this.onActivity?.();
    return { seq, promise, retired };
  }

  #send(pending: Pending, retry: boolean): void {
    if (this.#port !== null) {
      // First sent now: it belongs to this connection (generation 0 means none was ever open).
      if (!pending.sent) pending.generation = this.#generation;
      pending.sent = true;
      pending.sentOn = this.#generation;
    }
    const releases = this.#takeReleases();
    this.#port?.postMessage({
      ...pending.body,
      ...(releases === null ? {} : { releases }),
      seq: pending.seq,
      sentAtMs: pending.sentAtMs,
      resolvedThrough: this.#sequencer.resolvedThrough,
      live: false,
      resendable: pending.resendable,
      retry,
      generation: pending.generation,
    });
    if ((pending.cancelRequested || pending.signal?.aborted) && this.#pending.has(pending.seq))
      this.cancel(pending.seq);
  }

  // ---- releases (ruling R22.2) ----

  /**
   * Queues the release of `target`'s frames through `through`. It rides on the next request; with
   * none within `RELEASE_DELAY_MS`, or once the unreleased bytes reach half the window, it goes
   * alone.
   */
  #queueRelease(target: number, through: number, bytes: number, window: number): void {
    if (this.#closed) return;
    const queued = this.#releases.get(target);
    const next = {
      through: Math.max(queued?.through ?? -1, through),
      bytes: (queued?.bytes ?? 0) + bytes,
    };
    this.#releases.set(target, next);
    if (next.bytes * 2 >= window) {
      this.#flushReleases();
      return;
    }
    this.#releaseTimer ??= setTimeout(() => {
      this.#releaseTimer = null;
      this.#flushReleases();
    }, RELEASE_DELAY_MS);
  }

  #takeReleases(): Array<{ target: number; through: number }> | null {
    if (this.#releases.size === 0) return null;
    if (this.#releaseTimer !== null) clearTimeout(this.#releaseTimer);
    this.#releaseTimer = null;
    const releases = [...this.#releases].map(([target, { through }]) => ({ target, through }));
    this.#releases.clear();
    return releases;
  }

  #flushReleases(): void {
    for (const release of this.#takeReleases() ?? [])
      this.#port?.postMessage({ t: "release", target: release.target, through: release.through });
  }

  #settle(seq: number): void {
    const pending = this.#pending.get(seq);
    if (pending === undefined) return;
    this.#pending.delete(seq);
    pending.cancelCleanup?.();
    pending.cancelCleanup = null;
    this.#sequencer.resolve(seq);
    if (pending.control && pending.session === 0) this.#session0.unresolvedControl--;
    pending.retire();
    queueMicrotask(() => this.onActivity?.());
  }

  /** No frame payload or dictionary survives a caller's unknown outcome. */
  #releaseFrames(pending: Pending, frames: readonly FramePayload[]): void {
    const last = frames.at(-1);
    if (!last) return;
    let bytes = 0;
    for (const frame of frames) bytes += payloadBytes(frame);
    const window =
      "result" in pending.body ? pending.body.result.windowBytes : DEFAULT_RESULT_WINDOW_BYTES;
    this.#queueRelease(pending.seq, last.sequence, bytes, window);
  }

  #acceptSummary(pending: Pending, summary: StatementSummary): void {
    // Only an authoritative terminal boundary discharges earlier lost-session causes.
    if (
      pending.session !== null &&
      (pending.kind === "commit" || pending.kind === "rollback") &&
      summary.transaction === "idle"
    )
      this.#discharge(pending.session, pending.seq);
    if (pending.session === 0) this.#session0.transaction = summary.transaction;
    if (pending.session !== null) {
      if (
        (pending.body.t === "commit" || pending.body.t === "rollback") &&
        pending.body.retire === true &&
        summary.transaction === "idle"
      )
        this.#sessions.delete(pending.session);
      else this.#sessions.set(pending.session, summary.transaction);
    }
  }

  /**
   * A request refused before it was issued (closed, a lost session, a full window): its promise
   * rejects, and so does a cursor's queue, which its consumer waits on.
   */
  #refuse(error: Error, cursor: CursorQueue | null, reason: unknown = error): ProtocolTask {
    if (cursor !== null) {
      cursor.error = error;
      cursor.rejectColumns(error);
      cursor.wake?.();
    }
    // AbortSignal.reason may be any value; preserve it as the cancellation contract requires.
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
    return { seq: -1, promise: Promise.reject(reason), retired: Promise.resolve() };
  }

  /**
   * Fails a request: settles it, then rejects both its promise and its cursor queue (a cursor's
   * consumer waits on the queue, not the promise). Every failure goes through here: an error
   * reply, a broken frame, a reconnect, `close()`.
   */
  #fail(pending: Pending, error: Error): void {
    const reason: unknown =
      pending.signal?.aborted && "code" in error && error.code === "MINNOW_CANCELLED"
        ? pending.signal.reason
        : error;
    this.#settle(pending.seq);
    if (pending.abandoned) return; // The caller's original unknown outcome stays terminal.
    if (pending.cursor !== null) {
      if (pending.cursor instanceof ExportQueue) {
        const window =
          "result" in pending.body ? pending.body.result.windowBytes : DEFAULT_RESULT_WINDOW_BYTES;
        this.#discardExport(
          pending.cursor,
          pending.seq,
          window,
          pending.sentOn === this.#generation,
        );
      }
      pending.cursor.rejectColumns(error);
      pending.cursor.error = error;
      pending.cursor.wake?.();
    }
    pending.reject(reason);
  }

  // ---- incoming ----

  #markDatabaseReplaced(): DatabaseReplacedError {
    if (this.#replaced !== null) return this.#replaced;
    const error = new DatabaseReplacedError();
    this.#replaced = error;
    this.#liveBySeq.clear();
    // No replacement registration can enter this Set: subscribeLive refuses the sticky
    // identity error before capture. Remove before callbacks, which may close other handles.
    for (const live of this.#live) {
      this.#live.delete(live);
      live.token++;
      live.queued = null;
      live.params = EMPTY_LIVE_PARAMS;
      try {
        live.options.onReset?.("databaseReplaced");
      } catch {
        /* Reset callbacks do not own replacement or typed-error delivery. */
      }
      this.#liveError(live, error);
    }
    this.#welcome?.reject(error);
    this.#welcome = null;
    for (const pending of [...this.#pending.values()]) this.#fail(pending, error);
    this.#takeReleases();
    const port = this.#port;
    this.#port = null;
    try {
      port?.postMessage({ t: "bye" });
    } finally {
      port?.close();
    }
    this.onDatabaseReplaced?.(error);
    this.onActivity?.();
    return error;
  }

  #receive(message: OwnerMessage): void {
    this.#lastHeardAt = this.#now();
    this.#lastMessageAt = this.#lastHeardAt;
    switch (message.t) {
      case "welcome": {
        if (
          typeof message.databaseId !== "bigint" ||
          message.databaseId <= 0n ||
          message.databaseId >> 128n !== 0n
        ) {
          this.#welcome?.reject(new ProtocolError("Invalid owner database identity"));
          this.#welcome = null;
          return;
        }
        if (this.#databaseId !== null && this.#databaseId !== message.databaseId) {
          this.#markDatabaseReplaced();
          return;
        }
        this.#databaseId = message.databaseId;
        this.#ownerId = message.ownerId;
        this.#ownerLockHolder = message.lockHolder;
        this.#ownerEpoch = message.epoch;
        const welcome = this.#welcome;
        this.#welcome = null;
        // Before any resend or new request: the owner watches the lock only once announced.
        if (this.#livenessHeld) this.#port?.postMessage({ t: "liveness", held: true });
        welcome?.resolve(message);
        return;
      }
      case "connectionReset":
        this.giveUp("snapshot import ended this connection; reconnect before continuing");
        this.#port?.close();
        this.#port = null;
        this.onStepDown?.("closing");
        return;
      case "databaseReplaced":
        this.#markDatabaseReplaced();
        return;
      case "refused":
        if (message.reason === "databaseReplaced") {
          this.#markDatabaseReplaced();
          return;
        }
        this.#welcome?.reject(new ConnectionRefusedError(message.reason, message.ownerProtocol));
        this.#welcome = null;
        return;
      case "live": {
        const state = this.#liveBySeq.get(message.seq);
        if (state?.generation !== this.#generation) return;
        try {
          const dictionaries = new StreamDictionaries();
          for (const frame of message.frames) dictionaries.apply(frame);
          const summary = {
            ...emptySummary(),
            command: "SELECT",
            lsn: message.lsn ?? null,
            rowCount: message.frames.reduce((n, frame) => n + frame.rowCount, 0),
          };
          this.#deliverLive(
            state,
            makeResult(message.columns ?? [], message.frames, summary, dictionaries),
            message.version,
            { reset: message.reset ?? false, keyChanges: message.keyChanges ?? [] },
          );
        } catch (error) {
          this.#liveError(state, error, message.version);
        }
        return;
      }
      case "liveEnded": {
        const state = this.#liveBySeq.get(message.seq);
        if (state !== undefined && !state.closed) {
          this.#retireLive(state);
          this.#startLive(state);
        }
        return;
      }
      case "rows":
      case "done": {
        const pending = this.#pending.get(message.seq);
        if (pending === undefined) return;
        if (pending.abandoned) {
          this.#releaseFrames(pending, message.frames);
          if (message.t === "done") {
            this.#acceptSummary(pending, message.summary);
            this.#settle(message.seq);
          }
          return;
        }
        if (message.columns !== null) pending.columns = message.columns;
        try {
          if (pending.cursor instanceof ExportQueue) {
            const queue = pending.cursor;
            if (message.frames.length !== (message.bufferIds?.length ?? 0))
              throw new RangeError("Missing export frame capabilities");
            for (let i = 0; i < message.frames.length; i++) {
              const frame = message.frames[i];
              const ids = message.bufferIds?.[i];
              if (frame === undefined || ids === undefined)
                throw new RangeError("Missing export frame");
              pending.dictionaries.apply(frame, true);
              queue.handles.set(frame, this.#exportFrame(frame, ids, pending.dictionaries));
              queue.frames.push(frame);
            }
            if (pending.columns !== null || message.t === "done")
              queue.resolveColumns(pending.columns ?? []);
            if (queue.stopped) {
              const window =
                "result" in pending.body
                  ? pending.body.result.windowBytes
                  : DEFAULT_RESULT_WINDOW_BYTES;
              this.#discardExport(queue, pending.seq, window);
            }
          } else for (const frame of message.frames) pending.dictionaries.apply(frame);
        } catch (error) {
          // A frame out of order or a broken delta: fail this request, never the handler.
          this.cancel(message.seq);
          this.#fail(pending, error instanceof Error ? error : new Error(String(error)));
          return;
        }
        if (pending.cursor !== null && (pending.columns !== null || message.t === "done")) {
          pending.cursor.resolveColumns(pending.columns ?? []);
          // Metadata has escaped too: a reconnect cannot silently change this cursor's schema.
          pending.handedOut = true;
        }
        const last = message.frames.at(-1);
        if (pending.cursor !== null) {
          if (!(pending.cursor instanceof ExportQueue) && !pending.cursor.stopped)
            for (const frame of message.frames) pending.cursor.frames.push(frame);
          if (message.frames.length > 0) pending.handedOut = true;
        } else {
          for (const frame of message.frames) pending.frames.push(frame);
          // Buffered: released on receipt, riding on the next request when one follows soon.
          if (last !== undefined) {
            let bytes = 0;
            for (const frame of message.frames) bytes += payloadBytes(frame);
            const window =
              "result" in pending.body
                ? pending.body.result.windowBytes
                : DEFAULT_RESULT_WINDOW_BYTES;
            this.#queueRelease(message.seq, last.sequence, bytes, window);
          }
        }
        if (message.t === "done") {
          // The owner confirmed this boundary left the session idle (a COMMIT answered from the
          // ledger, a ROLLBACK that succeeded): the loss causes before it are discharged, not the
          // ones after it. Never on send.
          this.#acceptSummary(pending, message.summary);
          this.#settle(message.seq);
          const result = makeResult(
            pending.columns ?? [],
            pending.frames,
            message.summary,
            pending.dictionaries,
          );
          if (pending.cursor !== null) pending.cursor.done = message.summary;
          pending.cursor?.wake?.();
          pending.resolve(result);
          if (message.liveVersion !== undefined) {
            const state = this.#liveBySeq.get(message.seq);
            if (state !== undefined)
              this.#deliverLive(state, result, message.liveVersion, {
                reset: message.liveReset ?? true,
                keyChanges: message.keyChanges ?? [],
              });
          }
        } else pending.cursor?.wake?.();
        return;
      }
      case "prepared": {
        const pending = this.#pending.get(message.seq);
        if (pending === undefined) return;
        this.#preparedKinds.set(message.id, message.statementKind);
        this.#settle(message.seq);
        pending.resolve(
          makeResult(
            message.columns,
            [],
            {
              command: "PREPARE",
              rowCount: null,
              counts: null,
              lsn: null,
              replayed: false,
              closedEarly: false,
              transaction: "idle",
              catalogVersion: message.catalogVersion,
            },
            pending.dictionaries,
            { parameters: message.parameters },
          ),
        );
        return;
      }
      case "value":
      case "error": {
        const live = this.#liveBySeq.get(message.seq);
        if (message.t === "error" && live !== undefined && message.liveVersion !== undefined) {
          if (live.generation !== this.#generation) return;
          const error = fromWireError(message.error, ownerErrorRegistry);
          const pending = this.#pending.get(message.seq);
          if (pending !== undefined) {
            live.initialErrorHandled = true;
            this.#fail(pending, error);
          }
          this.#liveError(live, error, message.liveVersion);
          return;
        }
        if (message.t === "error" && live !== undefined && !this.#pending.has(message.seq)) {
          this.#liveError(live, fromWireError(message.error, ownerErrorRegistry));
          return;
        }
        const pending = this.#pending.get(message.seq);
        if (pending === undefined) return;
        if (message.t === "error") {
          const error = fromWireError(message.error, this.#errors);
          if (error instanceof DatabaseReplacedError) this.#markDatabaseReplaced();
          else
            this.#fail(
              pending,
              pending.mutation && pending.sent && error instanceof RequestExpiredError
                ? new OutcomeUnknownError("expired", { cause: error })
                : error,
            );
          return;
        }
        this.#settle(message.seq);
        pending.resolve(
          makeResult([], [], emptySummary(), pending.dictionaries, { value: message.value }),
        );
        return;
      }
      case "sessionAborted":
        if (message.session === 0) this.#session0.transaction = "timedOut";
        this.#sessions.set(message.session, "timedOut");
        this.onSessionAborted?.(message.session);
        return;
      case "diagnostic": {
        const error = fromWireError(message.error, this.#errors);
        if (
          message.context === LEASE_RELEASE_FAILURE_CONTEXT &&
          error instanceof SessionLostError
        ) {
          // No pending strict commit can be called successful after an unsettled release. Keep
          // its session's barrier even if its BEGIN had not yet been acknowledged.
          for (const pending of [...this.#pending.values()]) {
            if (pending.session !== null && pending.sent)
              this.#addCause(pending.session, pending.seq);
            this.#fail(pending, new SessionLostError("transaction", pending.seq, "unknown"));
          }
        }
        if (this.onOwnerError !== null)
          this.onOwnerError({ kind: message.kind, context: message.context, error });
        else console.error(message.context, error);
        return;
      }
      case "stepDown":
        this.onStepDown?.(message.reason);
        return;
      default:
        return;
    }
  }
}

function emptySummary(): StatementSummary {
  return {
    command: "",
    rowCount: null,
    counts: null,
    lsn: null,
    replayed: false,
    closedEarly: false,
    transaction: "idle",
    catalogVersion: 0,
  };
}
