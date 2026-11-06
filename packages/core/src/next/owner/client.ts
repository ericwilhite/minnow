/**
 * The protocol client: one client's side of protocol.ts over a MessagePort — the handshake,
 * sequencing and the outstanding window (sequence.ts), resendability, frame releases, stream
 * dictionaries, cancellation, idle-timeout and step-down events, and reconnecting with resends.
 * The public API (api.ts) is built on it in increment 3; Node tests drive it over a
 * MessageChannel.
 *
 * ## Releases
 * A buffered result releases each frame as it arrives (`release { through }` once per message);
 * a cursor releases a frame when the caller takes it. Either way the owner's window, not this
 * client, bounds what is in flight.
 *
 * ## Reconnect
 * `reconnect(port)` opens the next generation and resends, unchanged, every request without a
 * terminal reply, except streams whose frames it already handed out (those fail with
 * `SessionLostError`). The ledger answers duplicates; a non-resendable request the new owner has
 * not seen fails with `SessionLostError` and never runs.
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
import { SessionLostError } from "../errors.js";
import { decodeUtf8 } from "../vector/varlen.js";
import {
  ClientClosedError,
  ConnectionRefusedError,
  OwnerUnreachableError,
  fromWireError,
  ownerErrorRegistry,
} from "./errors.js";
import { StreamDictionaries, type FramePayload, type WireFlat, type WireVector } from "./frame.js";
import {
  DEFAULT_RESULT_WINDOW_BYTES,
  PROTOCOL_VERSION,
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

export interface ClientResult {
  readonly columns: readonly WireColumnMeta[];
  readonly frames: readonly FramePayload[];
  readonly summary: StatementSummary;
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
}

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
  readonly generation: number;
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
  readonly cursor: CursorQueue | null;
  readonly resolve: (result: ClientResult) => void;
  readonly reject: (error: Error) => void;
}

/** Frames waiting for a cursor consumer. */
class CursorQueue {
  readonly frames: FramePayload[] = [];
  done: StatementSummary | null = null;
  error: Error | null = null;
  wake: (() => void) | null = null;
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
): ClientResult {
  return {
    columns,
    frames,
    summary,
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
  readonly #session0 = { transaction: "idle" as TransactionState, unresolvedControl: 0 };
  #port: MessagePort | null = null;
  #generation = 0;
  #ownerId: string | null = null;
  #welcome: { resolve: (welcome: Welcome) => void; reject: (error: Error) => void } | null = null;
  #prepared = 0;
  /** Prepared id → its statement kind, as the owner's front end classified it (`Prepared`). */
  readonly #preparedKinds = new Map<number, PreparedStatementKind>();
  #closed = false;
  onSessionAborted: ((session: number) => void) | null = null;
  /** Called whenever requests in flight or pins change (the connector's liveness and watcher). */
  onActivity: (() => void) | null = null;
  onStepDown: ((reason: StepDown["reason"]) => void) | null = null;

  constructor(options: ProtocolClientOptions = {}) {
    this.clientId = options.clientId ?? randomClientId();
    this.#build = options.build ?? "test";
    this.livenessLock = options.livenessLock ?? `client:${this.clientId}`;
    this.#now = options.now ?? Date.now;
    this.protocol = options.protocol ?? PROTOCOL_VERSION;
  }

  /** Requests sent and not yet answered. */
  get pendingCount(): number {
    return this.#pending.size;
  }

  /** Whether the owner holds pins for this client: an open transaction or an open cursor. */
  get hasPins(): boolean {
    if (this.#cursors > 0) return true;
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

  get resolvedThrough(): number {
    return this.#sequencer.resolvedThrough;
  }

  /** Opens the first connection. */
  connect(port: MessagePort, signal?: AbortSignal): Promise<Welcome> {
    return this.#open(port, signal);
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
    // In send order. Nothing is decided here: every request that was sent goes back to the new
    // owner unchanged, whose ledger answers it (a recorded outcome, or a refusal saying what its
    // absence proves). A request that depended on the old connection is not resendable, so the
    // new owner never runs it.
    for (const pending of [...this.#pending.values()].sort((a, b) => a.seq - b.seq)) {
      const { session, kind } = pending;
      // An unanswered BEGIN (or unknown text) may have opened a transaction there: a cause at its
      // place in the sequence, discharged only by a confirmed end after it.
      if (session !== null && (kind === "begin" || kind === "unknown"))
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
      pending.columns = null;
      this.#send(pending, pending.sent);
    }
    return welcome;
  }

  #open(port: MessagePort, signal?: AbortSignal): Promise<Welcome> {
    this.#port?.close();
    this.#port = port;
    this.#generation++;
    port.onmessage = (event: MessageEvent) => {
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
    this.#port?.postMessage({ t: "bye" });
    this.#port?.close();
    const welcome = this.#welcome;
    this.#welcome = null;
    welcome?.reject(new ClientClosedError());
    for (const pending of [...this.#pending.values()]) this.#fail(pending, new ClientClosedError());
  }

  // ---- requests ----

  query(
    sql: string,
    params: readonly WireValue[] = [],
    options: {
      readonly session?: number;
      readonly windowBytes?: number;
      readonly wantRows?: boolean;
    } = {},
  ): Promise<ClientResult> {
    return this.#request({
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
    }).promise;
  }

  async prepare(
    sql: string,
  ): Promise<{ readonly id: number; readonly columns: readonly WireColumnMeta[] }> {
    const id = ++this.#prepared;
    const result = await this.#request({ t: "prepare", id, text: sql }).promise;
    return { id, columns: result.columns };
  }

  execute(
    id: number,
    params: readonly WireValue[] = [],
    session = 0,
    knownColumnsVersion = -1,
  ): Promise<ClientResult> {
    return this.#request({
      t: "statement",
      session,
      text: null,
      prepared: id,
      knownColumnsVersion,
      params,
      result: { windowBytes: DEFAULT_RESULT_WINDOW_BYTES, frameRows: null, wantRows: true },
    }).promise;
  }

  begin(session: number, readOnly = false): Promise<ClientResult> {
    return this.#request({ t: "begin", session, readOnly }).promise;
  }

  commit(session: number): Promise<ClientResult> {
    return this.#request({ t: "commit", session }).promise;
  }

  rollback(session: number): Promise<ClientResult> {
    return this.#request({ t: "rollback", session }).promise;
  }

  /** Sends a request and returns its seq with its promise (for cancellation and retries in tests). */
  send(body: Body): { readonly seq: number; readonly promise: Promise<ClientResult> } {
    return this.#request(body);
  }

  /** A cursor: frames released as they are taken. */
  cursor(
    sql: string,
    params: readonly WireValue[] = [],
    options: { readonly windowBytes?: number; readonly session?: number } = {},
  ): { readonly seq: number; readonly frames: AsyncIterableIterator<FramePayload>; close(): void } {
    const queue = new CursorQueue();
    const { seq, promise } = this.#request(
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
          wantRows: true,
        },
      },
      queue,
    );
    promise.catch(() => undefined);
    const port = () => this.#port;
    const frames: AsyncIterableIterator<FramePayload> = {
      [Symbol.asyncIterator]() {
        return this;
      },
      next: async (): Promise<IteratorResult<FramePayload>> => {
        for (;;) {
          const frame = queue.frames.shift();
          if (frame !== undefined) {
            port()?.postMessage({ t: "release", target: seq, through: frame.sequence });
            return { value: frame, done: false };
          }
          if (queue.error !== null) throw queue.error;
          if (queue.done !== null) return { value: undefined, done: true };
          await new Promise<void>((resolve) => {
            queue.wake = resolve;
          });
        }
      },
    };
    return {
      seq,
      frames,
      close: () => {
        this.cancel(seq, true);
      },
    };
  }

  cancel(seq: number, close = false): void {
    this.#port?.postMessage({ t: "cancel", target: seq, close });
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
  ): { readonly seq: number; readonly promise: Promise<ClientResult> } {
    if (this.#closed) return this.#refuse(new ClientClosedError(), cursor);
    const session = "session" in body ? body.session : null;
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
    let reject: (error: Error) => void = () => undefined;
    const promise = new Promise<ClientResult>((res, rej) => {
      resolve = res;
      reject = rej;
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
      cursor,
      resolve,
      reject,
    };
    if (control && session === 0) this.#session0.unresolvedControl++;
    this.#pending.set(seq, pending);
    if (cursor !== null) {
      this.#cursors++;
      const closeCursor = () => {
        this.#cursors--;
        this.onActivity?.();
      };
      promise.then(closeCursor, closeCursor);
    }
    this.#send(pending, false);
    this.onActivity?.();
    return { seq, promise };
  }

  #send(pending: Pending, retry: boolean): void {
    if (this.#port !== null) pending.sent = true;
    this.#port?.postMessage({
      ...pending.body,
      seq: pending.seq,
      sentAtMs: pending.sentAtMs,
      resolvedThrough: this.#sequencer.resolvedThrough,
      live: false,
      resendable: pending.resendable,
      retry,
      generation: pending.generation,
    });
  }

  #settle(seq: number): void {
    const pending = this.#pending.get(seq);
    if (pending === undefined) return;
    this.#pending.delete(seq);
    this.#sequencer.resolve(seq);
    if (pending.control && pending.session === 0) this.#session0.unresolvedControl--;
    queueMicrotask(() => this.onActivity?.());
  }

  /**
   * A request refused before it was issued (closed, a lost session, a full window): its promise
   * rejects, and so does a cursor's queue, which its consumer waits on.
   */
  #refuse(
    error: Error,
    cursor: CursorQueue | null,
  ): { readonly seq: number; readonly promise: Promise<ClientResult> } {
    if (cursor !== null) {
      cursor.error = error;
      cursor.wake?.();
    }
    return { seq: -1, promise: Promise.reject(error) };
  }

  /**
   * Fails a request: settles it, then rejects both its promise and its cursor queue (a cursor's
   * consumer waits on the queue, not the promise). Every failure goes through here: an error
   * reply, a broken frame, a reconnect, `close()`.
   */
  #fail(pending: Pending, error: Error): void {
    this.#settle(pending.seq);
    if (pending.cursor !== null) {
      pending.cursor.error = error;
      pending.cursor.wake?.();
    }
    pending.reject(error);
  }

  // ---- incoming ----

  #receive(message: OwnerMessage): void {
    switch (message.t) {
      case "welcome": {
        this.#ownerId = message.ownerId;
        const welcome = this.#welcome;
        this.#welcome = null;
        // Before any resend or new request: the owner watches the lock only once announced.
        if (this.#livenessHeld) this.#port?.postMessage({ t: "liveness", held: true });
        welcome?.resolve(message);
        return;
      }
      case "refused":
        this.#welcome?.reject(new ConnectionRefusedError(message.reason, message.ownerProtocol));
        this.#welcome = null;
        return;
      case "rows":
      case "done": {
        const pending = this.#pending.get(message.seq);
        if (pending === undefined) return;
        if (message.columns !== null) pending.columns = message.columns;
        try {
          for (const frame of message.frames) pending.dictionaries.apply(frame);
        } catch (error) {
          // A frame out of order or a broken delta: fail this request, never the handler.
          this.cancel(message.seq);
          this.#fail(pending, error instanceof Error ? error : new Error(String(error)));
          return;
        }
        const last = message.frames.at(-1);
        if (pending.cursor !== null) {
          pending.cursor.frames.push(...message.frames);
          if (message.frames.length > 0) pending.handedOut = true;
        } else {
          pending.frames.push(...message.frames);
          // Buffered: release on receipt.
          if (last !== undefined)
            this.#port?.postMessage({ t: "release", target: message.seq, through: last.sequence });
        }
        if (message.t === "done") {
          // The owner confirmed this boundary left the session idle (a COMMIT answered from the
          // ledger, a ROLLBACK that succeeded): the loss causes before it are discharged, not the
          // ones after it. Never on send.
          if (
            pending.session !== null &&
            (pending.kind === "commit" || pending.kind === "rollback") &&
            message.summary.transaction === "idle"
          )
            this.#discharge(pending.session, pending.seq);
          if (pending.session === 0) this.#session0.transaction = message.summary.transaction;
          if (pending.session !== null)
            this.#sessions.set(pending.session, message.summary.transaction);
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
          ),
        );
        return;
      }
      case "value":
      case "error": {
        const pending = this.#pending.get(message.seq);
        if (pending === undefined) return;
        if (message.t === "error") {
          this.#fail(pending, fromWireError(message.error, ownerErrorRegistry));
          return;
        }
        this.#settle(message.seq);
        pending.resolve(makeResult([], [], emptySummary(), pending.dictionaries));
        return;
      }
      case "sessionAborted":
        if (message.session === 0) this.#session0.transaction = "timedOut";
        this.#sessions.set(message.session, "timedOut");
        this.onSessionAborted?.(message.session);
        return;
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
