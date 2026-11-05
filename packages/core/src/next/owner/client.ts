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
 * A session with an open transaction (any state but `idle`, or a BEGIN unanswered) belongs to the
 * connection generation it ran on. A reconnect loses it: every request on it then fails with
 * `SessionLostError("transaction")` without being sent, so nothing can fall through to
 * autocommit on the new owner, until the client ends the session explicitly (`rollback`, or a
 * ROLLBACK / ABORT statement). Requests that were in flight on it fail the same way, except a
 * COMMIT: it is resent, and the ledger answers it if it committed (ending the session).
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
  type Request,
  type StatementSummary,
  type StepDown,
  type TransactionState,
  type Welcome,
  type WireColumnMeta,
  type WireValue,
} from "./protocol.js";
import { RequestSequencer, isResendable, mayControlTransaction } from "./sequence.js";

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
  readonly control: boolean;
  readonly session: number | null;
  columns: readonly WireColumnMeta[] | null;
  readonly frames: FramePayload[];
  /** Reset when the request is resent: a resent stream starts again at frame 0. */
  dictionaries: StreamDictionaries;
  /** Frames already handed to a cursor consumer: the request cannot be resent. */
  handedOut: boolean;
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
  /** Sessions whose transaction a reconnect lost: refused until ended explicitly. */
  readonly #lostSessions = new Set<number>();
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
  /** Prepared id → whether it may control a transaction (protocol.ts `Prepared`). */
  readonly #preparedControl = new Map<number, boolean>();
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
    return this.#lostSessions;
  }

  /** Opens the next generation on `port` and resends unanswered requests. */
  async reconnect(port: MessagePort, signal?: AbortSignal): Promise<Welcome> {
    const welcome = await this.#open(port, signal);
    // Every session with a transaction open on the old generation is lost with it.
    for (const [session, state] of this.#sessions)
      if (state !== "idle") this.#lostSessions.add(session);
    if (this.#session0.transaction !== "idle") this.#lostSessions.add(0);
    for (const pending of this.#pending.values())
      if (pending.session !== null && pending.body.t === "begin")
        this.#lostSessions.add(pending.session);
    for (const session of this.#lostSessions) this.#sessions.set(session, "idle");
    this.#session0.transaction = "idle";
    for (const pending of [...this.#pending.values()].sort((a, b) => a.seq - b.seq)) {
      if (pending.handedOut) {
        this.#settle(pending.seq);
        pending.reject(new SessionLostError("cursor", pending.seq));
        continue;
      }
      if (
        pending.session !== null &&
        this.#lostSessions.has(pending.session) &&
        sessionEffect(pending.body) !== "commit"
      ) {
        if (sessionEffect(pending.body) === "end") {
          // The ROLLBACK the client already asked for: the transaction is gone either way.
          this.#lostSessions.delete(pending.session);
          this.#send(pending, true);
          continue;
        }
        this.#settle(pending.seq);
        pending.reject(new SessionLostError("transaction", pending.seq));
        continue;
      }
      pending.frames.length = 0;
      pending.dictionaries = new StreamDictionaries();
      pending.columns = null;
      this.#send(pending, true);
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
    for (const pending of this.#pending.values()) pending.reject(new ClientClosedError());
    this.#pending.clear();
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
      mayControlTransaction(sql),
    ).promise;
  }

  async prepare(
    sql: string,
  ): Promise<{ readonly id: number; readonly columns: readonly WireColumnMeta[] }> {
    const id = ++this.#prepared;
    const result = await this.#request({ t: "prepare", id, text: sql }, false).promise;
    return { id, columns: result.columns };
  }

  execute(
    id: number,
    params: readonly WireValue[] = [],
    session = 0,
    knownColumnsVersion = -1,
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
      // A prepared statement may be BEGIN or COMMIT: the owner said so at prepare time; unknown
      // is treated as control (it only costs resendability).
      this.#preparedControl.get(id) ?? true,
    ).promise;
  }

  begin(session: number, readOnly = false): Promise<ClientResult> {
    return this.#request({ t: "begin", session, readOnly }, true).promise;
  }

  commit(session: number): Promise<ClientResult> {
    return this.#request({ t: "commit", session }, true).promise;
  }

  rollback(session: number): Promise<ClientResult> {
    return this.#request({ t: "rollback", session }, true).promise;
  }

  /** Sends a request and returns its seq with its promise (for cancellation and retries in tests). */
  send(
    body: Body,
    control = false,
  ): { readonly seq: number; readonly promise: Promise<ClientResult> } {
    return this.#request(body, control);
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
      false,
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

  #request(
    body: Body,
    control: boolean,
    cursor: CursorQueue | null = null,
  ): { readonly seq: number; readonly promise: Promise<ClientResult> } {
    if (this.#closed) return { seq: -1, promise: Promise.reject(new ClientClosedError()) };
    const session = "session" in body ? body.session : null;
    if (session !== null && this.#lostSessions.has(session)) {
      // Never sent: on the new owner it would run outside the lost transaction (autocommit).
      if (sessionEffect(body) !== "end")
        return { seq: -1, promise: Promise.reject(new SessionLostError("transaction")) };
      this.#lostSessions.delete(session);
    }
    const seq = this.#sequencer.issue();
    if (seq === null)
      return {
        seq: -1,
        promise: Promise.reject(new RangeError("Outstanding request window is full")),
      };
    const resendable = isResendable(body, this.#session0);
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
      control,
      session,
      columns: null,
      frames: [],
      dictionaries: new StreamDictionaries(),
      handedOut: false,
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
          this.#settle(message.seq);
          const failure = error instanceof Error ? error : new Error(String(error));
          if (pending.cursor !== null) {
            pending.cursor.error = failure;
            pending.cursor.wake?.();
          }
          pending.reject(failure);
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
          // A COMMIT resent after a reconnect, answered (from the ledger): the session ended.
          if (pending.session !== null && sessionEffect(pending.body) === "commit")
            this.#lostSessions.delete(pending.session);
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
        this.#preparedControl.set(message.id, message.controlsTransaction);
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
        this.#settle(message.seq);
        if (message.t === "error") {
          const error = fromWireError(message.error, ownerErrorRegistry);
          if (pending.cursor !== null) {
            pending.cursor.error = error;
            pending.cursor.wake?.();
          }
          pending.reject(error);
        } else pending.resolve(makeResult([], [], emptySummary(), pending.dictionaries));
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

/** What a request does to its session's transaction, as far as reconnect handling needs. */
function sessionEffect(body: Body): "commit" | "end" | null {
  if (body.t === "commit") return "commit";
  if (body.t === "rollback") return "end";
  if (body.t !== "statement" || body.text === null) return null;
  const match = /^\s*(\w+)(?:\s+(\w+))?\s*;?\s*$/u.exec(body.text);
  const word = match?.[1]?.toLowerCase();
  const next = match?.[2]?.toLowerCase();
  if (word === "commit" || word === "end") return "commit";
  // ROLLBACK and ABORT end the transaction; ROLLBACK TO a savepoint does not.
  if ((word === "rollback" || word === "abort") && next !== "to") return "end";
  return null;
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
