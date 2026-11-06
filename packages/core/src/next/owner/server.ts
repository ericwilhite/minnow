/**
 * The owner's protocol server: connections, request admission and outcomes, per-session ordering,
 * the idle-transaction timeout, result streaming with release windows, cancellation, liveness and
 * error mapping (protocol.ts), over an `OwnerEngine` (engine.ts, composed by compose.ts).
 *
 * ## One task per message, synchronous when it can be
 * Each incoming message is handled inside its `message` event. A request whose session is idle
 * starts at once, in that task; a point read the engine serves synchronously (`runNow`) is
 * admitted, run, settled and answered with one `postMessage` before the handler returns: no
 * promise, microtask or timer (ruling R6). Everything else continues asynchronously.
 *
 * ## Sessions
 * Requests of one session run one at a time, in arrival order (a FIFO per session). A session is
 * created on first use; session 0 is the connection's default session.
 *
 * ## Ledger (outcomes.ts; rulings R9, R12, R16, R18.2)
 * The server derives each request's ledger identity (`ledgerScope`, `ledgerIdentity`) when the
 * request reaches the head of its session's queue, admits it, and once answered applies
 * `ledgerAction`: the write path records commits; the server records failures of would-be commits
 * and session-scoped `statement` outcomes, and settles the rest. A resent request that is not
 * resendable is never executed on a new connection: the ledger answers it, or it fails with
 * `SessionLostError`.
 *
 * ## Results (rulings R3, R16.5)
 * The server drains the executor's stream with `poll()` and posts what was queued in one message,
 * awaiting `next()` only when nothing is ready. Under strict durability a frame waits for
 * `engine.durable(frame.requiresLsn)` first. Posted frames stay charged until the client's
 * `release` covers them, or the connection ends.
 *
 * ## Ending a connection
 * `bye`, a newer generation's `hello` from the same client, the client's liveness lock released
 * (`watchLiveness`), or `stepDown`: running requests are aborted up to the commit boundary, every
 * posted frame is released, every stream cancelled, every session rolled back and ended in the
 * ledger (`endSession`).
 */
import { RequestExpiredError, SessionLostError } from "../errors.js";
import { LIVENESS_IDLE_RELEASE_MS } from "./election.js";
import { beginTask } from "../exec/pipeline.js";
import { QueryCancelledError } from "../exec/errors.js";
import type { ResultFrame, ResultStream } from "../exec/result.js";
import type { Admission, Outcome } from "../format/ledger.js";
import { CLIENT_RETRY_WINDOW_MS, MAX_OUTSTANDING_REQUESTS } from "../format/ledger.js";
import { BATCH_TARGET_BYTES } from "../vector/vector.js";
import type {
  EngineSession,
  OwnerEngine,
  PlannedStatement,
  Run,
  RunContext,
  RunOutcome,
  SyncRun,
} from "./engine.js";
import { IdleTransactionTimer } from "./engine.js";
import {
  CancelledError,
  CommittedWithoutResultError,
  RecordedFailureError,
  toWireError,
} from "./errors.js";
import { transferFor, type FramePayload } from "./frame.js";
import { ledgerAction, ledgerIdentity, ledgerScope, type AnsweredRequest } from "./outcomes.js";
import {
  ABANDONED_STREAM_MS,
  ALIVE_INTERVAL_MS,
  DEFAULT_TRANSACTION_IDLE_TIMEOUT_MS,
  PROTOCOL_VERSION,
  ProtocolError,
  parseClientMessage,
  type ClientMessage,
  type Hello,
  type OwnerMessage,
  type Request,
  type StatementSummary,
  type StepDown,
  type WireColumnMeta,
} from "./protocol.js";

/** The slice of a MessagePort the server needs (a Worker's own scope works too). */
export interface ServerPort {
  postMessage(message: unknown, transfer: Transferable[]): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close?(): void;
}

export interface OwnerServerOptions {
  /** Random per owner worker. */
  readonly ownerId: string;
  /** The storage fence epoch held. */
  readonly epoch: number;
  readonly build: string;
  /** The protocol this owner speaks; `PROTOCOL_VERSION` (other values are for skew tests). */
  readonly protocol?: number;
  readonly now?: () => number;
  readonly transactionIdleTimeoutMs?: number;
  readonly aliveIntervalMs?: number;
  readonly abandonedStreamMs?: number;
  /**
   * Watches a client's liveness lock (election.ts): `gone` aborts when the owner is granted the
   * client's lock, meaning the client released it while holding pins or live requests (it is
   * gone). The browser host passes a Web Locks implementation; absent, liveness is not watched.
   */
  readonly watchLiveness?: (lockName: string) => { readonly gone: AbortSignal; stop(): void };
  /** The Web Locks client id holding this owner's host lock, sent in `welcome` (null: unknown). */
  readonly lockHolder?: string | null;
}

export interface OwnerServerStats {
  readonly connections: number;
  /** Requests answered inside their message event, with no await (ruling R6). */
  readonly synchronous: number;
  readonly asynchronous: number;
  /** Frames posted and not yet released, across connections. */
  readonly outstandingFrames: number;
}

/** A running request's controls. */
interface Running {
  readonly abort: AbortController;
  readonly session: number | null;
  live: boolean;
  /** `cancel { close: true }` was asked: end with `done { closedEarly }`. */
  closeEarly: boolean;
  cancelled: boolean;
  /** Why the owner stopped it, when not the client's cancel: the error the client gets. */
  lost: Error | null;
  /** The run, once started: its `reachedCommit` makes cancellation a no-op. */
  run: Run | null;
}

interface Stream {
  readonly results: ResultStream;
  readonly session: number | null;
  /** Posted, unreleased, in sequence order. */
  readonly posted: ResultFrame[];
  finished: boolean;
  live: boolean;
  lastActivityMs: number;
}

interface SessionSlot {
  readonly id: number;
  engine: EngineSession | null;
  readonly queue: Array<() => void | Promise<void>>;
  busy: boolean;
  readonly timer: IdleTransactionTimer;
  /** Requests running or queued, for the idle timer. */
  pending: number;
}

const hexToBytes = (hex: string): Uint8Array => {
  const bytes = new Uint8Array(hex.length >> 1);
  for (let i = 0; i < bytes.length; i++)
    bytes[i] = Number.parseInt(hex.slice(2 * i, 2 * i + 2), 16);
  return bytes;
};

const errorCode = (error: unknown): string => {
  if (typeof error === "object" && error !== null) {
    const code = (error as { code?: unknown }).code;
    if (typeof code === "string") return code;
    const name = (error as { name?: unknown }).name;
    if (typeof name === "string") return name;
  }
  return "Error";
};

const isPromise = (value: unknown): value is Promise<unknown> =>
  typeof value === "object" &&
  value !== null &&
  typeof (value as { then?: unknown }).then === "function";

export class OwnerServer {
  readonly #engine: OwnerEngine;
  readonly #options: OwnerServerOptions;
  readonly #now: () => number;
  readonly #connections = new Set<Connection>();
  /** Completion of each executing request, by `clientId:seq`, for `inFlight` admissions. */
  readonly #inFlight = new Map<string, Promise<void>>();
  #steppingDown: StepDown["reason"] | null = null;
  synchronous = 0;
  asynchronous = 0;

  constructor(engine: OwnerEngine, options: OwnerServerOptions) {
    this.#engine = engine;
    this.#options = options;
    this.#now = options.now ?? Date.now;
  }

  get engine(): OwnerEngine {
    return this.#engine;
  }

  get options(): OwnerServerOptions {
    return this.#options;
  }

  get steppingDown(): StepDown["reason"] | null {
    return this.#steppingDown;
  }

  now(): number {
    return this.#now();
  }

  get inFlight(): Map<string, Promise<void>> {
    return this.#inFlight;
  }

  get stats(): OwnerServerStats {
    let outstandingFrames = 0;
    for (const connection of this.#connections) outstandingFrames += connection.outstandingFrames;
    return {
      connections: this.#connections.size,
      synchronous: this.synchronous,
      asynchronous: this.asynchronous,
      outstandingFrames,
    };
  }

  /** Serves one client connection on `port`. */
  accept(port: ServerPort): void {
    const connection = new Connection(this, port);
    this.#connections.add(connection);
    port.onmessage = (event: MessageEvent) => {
      // A message is a task entry point: the worker's task clock starts here (ruling R23.5).
      beginTask();
      connection.receive(event.data);
    };
  }

  /** Newest generation seen per client, kept a retry window after its last hello. */
  readonly #generations = new Map<string, { generation: number; atMs: number }>();

  /**
   * Records `generation` of `clientId`; false when it is not newer than one already seen (a stale
   * hello: its requests belong to an ended generation).
   */
  claimGeneration(clientId: string, generation: number): boolean {
    const now = this.#now();
    for (const [id, seen] of this.#generations)
      if (now - seen.atMs > CLIENT_RETRY_WINDOW_MS) this.#generations.delete(id);
    const seen = this.#generations.get(clientId);
    if (seen !== undefined && generation <= seen.generation) return false;
    this.#generations.set(clientId, { generation, atMs: now });
    return true;
  }

  /** A newer generation of `clientId` connected: older connections of it end. */
  supersede(clientId: string, generation: number, except: Connection): void {
    for (const connection of this.#connections)
      if (
        connection !== except &&
        connection.clientId === clientId &&
        connection.generation < generation
      )
        connection.end("reconnected");
  }

  /** Every connection sends `alive` if it is overdue (`Connection.aliveIfOverdue`). */
  aliveIfOverdue(): void {
    for (const connection of this.#connections) connection.aliveIfOverdue();
  }

  dropped(connection: Connection): void {
    this.#connections.delete(connection);
  }

  /** Tells every client the owner is leaving, then ends every connection. */
  stepDown(reason: StepDown["reason"]): void {
    this.#steppingDown = reason;
    for (const connection of [...this.#connections]) {
      connection.post({ t: "stepDown", reason });
      connection.end("stepDown");
    }
  }
}

class Connection {
  readonly #server: OwnerServer;
  readonly #port: ServerPort;
  clientId = "";
  generation = 0;
  #clientBytes: Uint8Array = new Uint8Array(16);
  #livenessLock = "";
  #state: "handshake" | "open" | "closed" = "handshake";
  readonly #sessions = new Map<number, SessionSlot>();
  readonly #prepared = new Map<number, { readonly text: string; planned: PlannedStatement }>();
  readonly #running = new Map<number, Running>();
  /** Requests waiting behind their session, by seq: a cancel marks them. */
  readonly #queued = new Map<number, { cancelled: boolean; close: boolean }>();
  readonly #streams = new Map<number, Stream>();
  #aliveTimer: ReturnType<typeof setInterval> | null = null;
  #lastPostAt = 0;
  #sweepTimer: ReturnType<typeof setInterval> | null = null;
  #watch: { readonly gone: AbortSignal; stop(): void } | null = null;
  #watchIdle: ReturnType<typeof setTimeout> | null = null;
  #livenessHeld = false;

  constructor(server: OwnerServer, port: ServerPort) {
    this.#server = server;
    this.#port = port;
  }

  get outstandingFrames(): number {
    let count = 0;
    for (const stream of this.#streams.values()) count += stream.posted.length;
    return count;
  }

  /**
   * Posts to the client. Result buffers are transferred, except below `TRANSFER_MIN_BYTES` in all,
   * where cloning is cheaper (ruling R22.1); either way the frames stay charged until released.
   */
  post(message: OwnerMessage, transfer: ArrayBuffer[] = []): void {
    if (this.#state === "closed") return;
    this.#lastPostAt = this.#server.now();
    this.#port.postMessage(message, transfer.length === 0 ? transfer : transferFor(transfer));
  }

  /**
   * Sends `alive` now if this connection has requests running and has been sent nothing for an
   * alive interval. Called on the request path (any incoming message), not only by the interval
   * timer: a busy owner whose timers do not run (WebKit, while a long statement yields through
   * message tasks) still says it is alive whenever it handles anything.
   */
  aliveIfOverdue(): void {
    if (this.#state !== "open" || this.#running.size === 0) return;
    const interval = this.#server.options.aliveIntervalMs ?? ALIVE_INTERVAL_MS;
    if (this.#server.now() - this.#lastPostAt >= interval) this.post({ t: "alive" });
  }

  // ---- incoming ----

  receive(data: unknown): void {
    if (this.#state === "closed") return;
    this.#server.aliveIfOverdue();
    let message: ClientMessage;
    try {
      message = parseClientMessage(data);
    } catch (error) {
      this.#protocolFailure(data, error);
      return;
    }
    if (this.#state === "handshake") {
      // A client that found it already had a connection to this owner leaves without one.
      if (message.t === "bye") {
        this.end("bye");
        return;
      }
      if (message.t !== "hello") {
        this.#protocolFailure(data, new ProtocolError("The first message must be hello"));
        return;
      }
      this.#hello(message);
      return;
    }
    switch (message.t) {
      case "hello":
        this.#protocolFailure(data, new ProtocolError("hello after welcome"));
        return;
      case "cancel":
        this.#cancel(message.target, message.close);
        return;
      case "release":
        this.#release(message.target, message.through);
        return;
      case "liveness":
        this.#livenessHeld = message.held;
        for (const running of this.#running.values()) running.live = message.held;
        for (const stream of this.#streams.values()) stream.live = message.held;
        this.#watchIfNeeded();
        return;
      case "liveAck":
        return;
      case "ping":
        this.post({ t: "alive" });
        return;
      case "unprepare":
        this.#prepared.delete(message.id);
        return;
      case "bye":
        this.end("bye");
        return;
      default:
        // Releases riding on the request first (ruling R22.2): they free window for it.
        if (message.releases !== undefined)
          for (const release of message.releases) this.#release(release.target, release.through);
        this.#request(message);
    }
  }

  #protocolFailure(data: unknown, error: unknown): void {
    const seq =
      typeof data === "object" && data !== null ? (data as { seq?: unknown }).seq : undefined;
    if (typeof seq === "number") this.post({ t: "error", seq, error: toWireError(error) });
    else
      this.post({
        t: "diagnostic",
        kind: "coordination",
        context: "protocol",
        error: toWireError(error),
      });
  }

  #hello(hello: Hello): void {
    const server = this.#server;
    const protocol = server.options.protocol ?? PROTOCOL_VERSION;
    if (hello.protocol !== protocol) {
      this.post({
        t: "refused",
        reason: "version",
        ownerProtocol: protocol,
        ownerBuild: server.options.build,
      });
      return;
    }
    if (server.steppingDown !== null) {
      this.post({
        t: "refused",
        reason: server.steppingDown === "superseded" ? "superseded" : "steppingDown",
        ownerProtocol: protocol,
        ownerBuild: server.options.build,
      });
      return;
    }
    if (typeof hello.clientId !== "string" || !/^[0-9a-f]{32}$/u.test(hello.clientId)) {
      this.#protocolFailure(hello, new ProtocolError("clientId must be 32 hex digits"));
      return;
    }
    if (!Number.isSafeInteger(hello.generation) || hello.generation < 1) {
      this.#protocolFailure(hello, new ProtocolError("generation must be a positive integer"));
      return;
    }
    if (!server.claimGeneration(hello.clientId, hello.generation)) {
      // A newer connection of this client exists (or existed): this one is stale.
      this.post({
        t: "refused",
        reason: "staleGeneration",
        ownerProtocol: PROTOCOL_VERSION,
        ownerBuild: server.options.build,
      });
      return;
    }
    this.clientId = hello.clientId;
    this.#clientBytes = hexToBytes(hello.clientId);
    this.generation = hello.generation;
    this.#livenessLock = hello.livenessLock;
    this.#state = "open";
    server.supersede(this.clientId, this.generation, this);
    this.post({
      t: "welcome",
      protocol,
      build: server.options.build,
      ownerId: server.options.ownerId,
      epoch: server.options.epoch,
      durability: server.engine.durability,
      limits: {
        maxOutstanding: MAX_OUTSTANDING_REQUESTS,
        retryWindowMs: CLIENT_RETRY_WINDOW_MS,
        frameBytes: BATCH_TARGET_BYTES,
        aliveIntervalMs: server.options.aliveIntervalMs ?? ALIVE_INTERVAL_MS,
      },
      lockHolder: server.options.lockHolder ?? null,
    });
  }

  // ---- sessions ----

  #slot(id: number): SessionSlot {
    let slot = this.#sessions.get(id);
    if (slot === undefined) {
      const timeoutMs =
        this.#server.options.transactionIdleTimeoutMs ?? DEFAULT_TRANSACTION_IDLE_TIMEOUT_MS;
      const created: SessionSlot = {
        id,
        engine: null,
        queue: [],
        busy: false,
        pending: 0,
        timer: new IdleTransactionTimer(timeoutMs, () => {
          created.engine?.expire();
          this.post({ t: "sessionAborted", session: id, reason: "idleTimeout" });
        }),
      };
      slot = created;
      this.#sessions.set(id, slot);
    }
    return slot;
  }

  #engineSession(slot: SessionSlot): EngineSession {
    slot.engine ??= this.#server.engine.openSession(
      this.clientId,
      this.generation,
      String(slot.id),
    );
    return slot.engine;
  }

  /** Runs `task` when `slot` is free: at once (synchronously) if idle. */
  #enqueue(slot: SessionSlot, task: () => void | Promise<void>): void {
    slot.pending++;
    slot.timer.start();
    if (slot.busy) {
      slot.queue.push(task);
      return;
    }
    this.#runTask(slot, task);
  }

  /**
   * Runs `task`, then every task queued behind it that finishes synchronously, until one goes
   * asynchronous (its completion continues the queue) or the queue is empty. A synchronous answer
   * (a refusal, a replay) must not stall the requests waiting behind it.
   */
  #runTask(slot: SessionSlot, task: () => void | Promise<void>): void {
    let current: (() => void | Promise<void>) | undefined = task;
    while (current !== undefined) {
      let result: void | Promise<void>;
      try {
        result = current();
      } catch {
        result = undefined;
      }
      if (isPromise(result)) {
        slot.busy = true;
        result.then(
          () => {
            this.#taskDone(slot);
          },
          () => {
            this.#taskDone(slot);
          },
        );
        return;
      }
      this.#afterTask(slot);
      current = this.#state === "closed" ? undefined : slot.queue.shift();
    }
  }

  #taskDone(slot: SessionSlot): void {
    slot.busy = false;
    this.#afterTask(slot);
    const next = slot.queue.shift();
    if (next !== undefined && this.#state !== "closed") this.#runTask(slot, next);
    else if (this.#state === "closed") this.#closeSession(slot);
  }

  #afterTask(slot: SessionSlot): void {
    slot.pending--;
    const state = slot.engine?.transaction ?? "idle";
    slot.timer.setOpen(state === "open" || state === "failed");
    slot.timer.end();
    this.#watchIfNeeded();
  }

  // ---- requests ----

  #request(request: Request): void {
    const session = "session" in request ? request.session : null;
    if (session !== null && (!Number.isSafeInteger(session) || session < 0)) {
      this.post({
        t: "error",
        seq: request.seq,
        error: toWireError(new ProtocolError("Bad session id")),
      });
      return;
    }
    const slot = this.#slot(session ?? 0);
    if (request.t === "prepare" || request.t === "subscribe" || request.t === "admin") {
      // Session-free requests do not wait behind a session.
      void this.#serveOne(request, null);
      return;
    }
    const queued = { cancelled: false, close: false };
    this.#queued.set(request.seq, queued);
    this.#enqueue(slot, () => {
      this.#queued.delete(request.seq);
      // Cancelled while it waited behind its session: never admitted, never run.
      if (queued.cancelled) {
        this.#answerCancelledBeforeStart(request, slot, queued.close);
        return;
      }
      return this.#serveOne(request, slot);
    });
  }

  #answerCancelledBeforeStart(request: Request, slot: SessionSlot, close: boolean): void {
    if (!close) {
      this.post({ t: "error", seq: request.seq, error: toWireError(new CancelledError()) });
      return;
    }
    this.post({
      t: "done",
      seq: request.seq,
      columns: null,
      frames: [],
      summary: {
        command: "",
        rowCount: null,
        counts: null,
        lsn: null,
        replayed: false,
        closedEarly: true,
        transaction: slot.engine?.transaction ?? "idle",
        catalogVersion: slot.engine?.catalogVersion ?? this.#server.engine.catalogVersion,
      },
    });
  }

  /** Admits and serves one request; returns a promise only if it continues asynchronously. */
  #serveOne(request: Request, slot: SessionSlot | null): void | Promise<void> {
    const server = this.#server;
    const engine = server.engine;
    // Plan first (synchronous, cached): the scope depends on what the statement is.
    const session = slot === null ? null : this.#engineSession(slot);
    let planned: PlannedStatement | null = null;
    let planError: unknown = null;
    if (request.t === "statement" && session !== null) {
      try {
        planned = this.#planStatement(session, request.text, request.prepared);
      } catch (error) {
        planError = error;
      }
    }
    const before = session?.transaction ?? "idle";
    const scope = ledgerScope({
      kind: request.t,
      control: planned?.control ?? null,
      before,
      retry: request.retry,
      resendable: request.resendable,
    });
    const identity = ledgerIdentity(this.clientId, request, slot?.id ?? 0, scope);
    const now = server.now();
    const admission = engine.ledger.admit(identity, now);
    if (admission.kind !== "execute")
      return this.#answerAdmission(request, admission, planned, slot);
    if (request.retry && !request.resendable) {
      // First sight of a request that depended on its old connection: never run it here. What
      // the ledger's silence proves depends on the request. A BEGIN, COMMIT or ROLLBACK leaves
      // nothing durable but a commit, whose outcome the commit itself records (format/ledger.ts
      // "Durability"): absent, it did not commit. A statement inside a transaction records its
      // outcome in memory only, and its transaction may have committed after it: unknown.
      const control =
        request.t === "begin" ||
        request.t === "commit" ||
        request.t === "rollback" ||
        planned?.control === "begin" ||
        planned?.control === "commit" ||
        planned?.control === "rollback";
      engine.ledger.settle(this.clientId, request.seq);
      this.post({
        t: "error",
        seq: request.seq,
        error: toWireError(
          new SessionLostError("transaction", request.seq, control ? "none" : "unknown"),
        ),
      });
      return;
    }
    if (planError !== null) {
      session?.fail();
      this.#finishLedger(request, planned, before, session, null, true, planError);
      this.post({ t: "error", seq: request.seq, error: toWireError(planError) });
      return;
    }
    return this.#execute(request, planned, slot, session, before);
  }

  /** Plans against the session's catalog (a transaction's own DDL included). */
  #planStatement(
    session: EngineSession,
    text: string | null,
    prepared: number | null,
  ): PlannedStatement {
    if (prepared !== null) {
      const entry = this.#prepared.get(prepared);
      if (entry === undefined)
        throw new ProtocolError(`Prepared statement ${String(prepared)} does not exist`);
      // Re-planned against the session's catalog when it differs (the planned entry stays the
      // shared one, so a transaction's private plan never outlives it).
      return entry.planned.catalogVersion === session.catalogVersion
        ? entry.planned
        : session.plan(entry.text);
    }
    if (text === null) throw new ProtocolError("A statement needs text or a prepared id");
    return session.plan(text);
  }

  #answerAdmission(
    request: Request,
    admission: Exclude<Admission, { kind: "execute" }>,
    planned: PlannedStatement | null,
    slot: SessionSlot | null,
  ): void | Promise<void> {
    const seq = request.seq;
    switch (admission.kind) {
      case "stale":
        return; // The client already holds the answer.
      case "expired":
        this.post({
          t: "error",
          seq,
          error: toWireError(new RequestExpiredError(seq, this.#server.now() - request.sentAtMs)),
        });
        return;
      case "window":
        this.post({
          t: "error",
          seq,
          error: toWireError(new ProtocolError("Request outside the outstanding window")),
        });
        return;
      case "sessionLost":
        // A statement of an ended generation's transaction: that transaction may have committed
        // before the generation ended (its statements' outcomes went with it), so not "none".
        this.post({
          t: "error",
          seq,
          error: toWireError(new SessionLostError("transaction", seq, "unknown")),
        });
        return;
      case "inFlight": {
        const original = this.#server.inFlight.get(`${this.clientId}:${String(seq)}`);
        if (original === undefined) {
          this.post({
            t: "error",
            seq,
            error: toWireError(new ProtocolError("Duplicate of an unknown request")),
          });
          return;
        }
        // Wait for the original, then answer from the ledger (or run, if it settled).
        return original.then(() => this.#serveOne(request, slot));
      }
      case "duplicate":
        this.#replay(request, admission.outcome, planned, slot);
        return;
    }
  }

  #replay(
    request: Request,
    outcome: Outcome,
    planned: PlannedStatement | null,
    slot: SessionSlot | null,
  ): void {
    if (
      outcome.kind === "failed" ||
      (outcome.kind === "statement" && outcome.status === "failed")
    ) {
      const code = outcome.kind === "failed" ? outcome.code : (outcome.code ?? "Error");
      this.post({
        t: "error",
        seq: request.seq,
        error: toWireError(new RecordedFailureError(code)),
      });
      return;
    }
    const counts = outcome.counts;
    // Rows the caller asked for are not kept by the ledger: say so, never answer with none.
    const wantedRows =
      (request.t === "statement" &&
        request.result.wantRows &&
        planned !== null &&
        planned.kind === "modify" &&
        planned.columns.length > 0) ||
      (request.t === "write" && request.returning !== null && request.result.wantRows);
    if (wantedRows) {
      this.post({
        t: "error",
        seq: request.seq,
        error: toWireError(new CommittedWithoutResultError(counts)),
      });
      return;
    }
    let rows = 0;
    for (const value of Object.values(counts)) rows += value;
    this.post({
      t: "done",
      seq: request.seq,
      columns: null,
      frames: [],
      summary: {
        command:
          planned?.lowered.command ?? (request.t === "commit" ? "COMMIT" : request.t.toUpperCase()),
        rowCount: rows,
        counts,
        lsn: outcome.kind === "committed" ? outcome.lsn : null,
        replayed: true,
        closedEarly: false,
        transaction: slot?.engine?.transaction ?? "idle",
        catalogVersion: slot?.engine?.catalogVersion ?? this.#server.engine.catalogVersion,
      },
    });
  }

  #context(request: Request, signal: AbortSignal, commits: boolean): RunContext {
    const result = "result" in request ? request.result : null;
    return {
      signal,
      request: commits
        ? { clientId: this.#clientBytes, sequence: request.seq, sentAtMs: request.sentAtMs }
        : null,
      windowBytes: result?.windowBytes ?? 2 * BATCH_TARGET_BYTES,
      frameRows: result?.frameRows ?? null,
    };
  }

  #execute(
    request: Request,
    planned: PlannedStatement | null,
    slot: SessionSlot | null,
    session: EngineSession | null,
    before: AnsweredRequest["before"],
  ): void | Promise<void> {
    const server = this.#server;
    const engine = server.engine;
    const abort = new AbortController();
    const running: Running = {
      abort,
      session: slot?.id ?? null,
      live: request.live || this.#livenessHeld,
      closeEarly: false,
      cancelled: false,
      lost: null,
      run: null,
    };
    this.#running.set(request.seq, running);
    const key = `${this.clientId}:${String(request.seq)}`;
    // Registered only once the request goes asynchronous: a synchronous answer completes before
    // any other message, so no duplicate can find it in flight.
    let settleFlight: (() => void) | null = null;
    const goAsync = () => {
      server.asynchronous++;
      server.inFlight.set(
        key,
        new Promise<void>((resolve) => {
          settleFlight = resolve;
        }),
      );
      this.#updateAlive();
    };
    const finish = () => {
      this.#running.delete(request.seq);
      if (settleFlight !== null) {
        server.inFlight.delete(key);
        settleFlight();
      }
      this.#updateAlive();
    };

    // The commit identity goes to every run that may commit; the composition decides.
    const context = this.#context(request, abort.signal, true);
    let run: Run | SyncRun | null = null;
    const inSession = (): EngineSession => {
      if (session === null) throw new ProtocolError(`${request.t} needs a session`);
      return session;
    };
    try {
      switch (request.t) {
        case "statement": {
          if (planned === null) throw new ProtocolError("A statement without a plan");
          const statement = planned;
          if (statement.pointRead && session !== null) {
            const now = session.runNow(statement, request.params, context);
            if (now !== null && "frames" in now) {
              this.#answerSync(request, statement, now, session, before);
              finish();
              server.synchronous++;
              return;
            }
            if (now !== null) run = now.continued;
          }
          run ??= inSession().run(statement, request.params, context);
          break;
        }
        case "begin":
          run = this.#beginRun(inSession(), request.readOnly, abort.signal);
          break;
        case "commit":
          run = inSession().commit(context);
          break;
        case "rollback":
          inSession().rollback();
          run = {
            results: emptyStreamLike(),
            done: Promise.resolve({ command: "ROLLBACK", rowCount: null, counts: null, lsn: null }),
            reachedCommit: false,
            commits: false,
          };
          break;
        case "write":
          run = inSession().write(
            {
              table: request.table,
              op: request.op,
              names: request.names,
              columns: request.columns,
              rowCount: request.rowCount,
              ignoreConflicts: request.ignoreConflicts,
              returning: request.returning,
            },
            context,
          );
          break;
        case "prepare":
          this.#prepare(request);
          finish();
          return;
        case "admin":
          goAsync();
          return engine
            .admin(request.op, request.args, context)
            .then(
              (value) => {
                engine.ledger.settle(this.clientId, request.seq);
                this.post({ t: "value", seq: request.seq, value });
              },
              (error: unknown) => {
                engine.ledger.settle(this.clientId, request.seq);
                this.post({ t: "error", seq: request.seq, error: toWireError(error) });
              },
            )
            .finally(finish);
        case "subscribe":
          engine.ledger.settle(this.clientId, request.seq);
          this.post({
            t: "error",
            seq: request.seq,
            error: toWireError(
              new TypeError("Live subscriptions are not served by this owner yet"),
            ),
          });
          finish();
          return;
      }
    } catch (error) {
      this.#finishLedger(request, planned, before, session, null, true, error);
      this.post({
        t: "error",
        seq: request.seq,
        error: toWireError(this.#mapError(error, running)),
      });
      finish();
      return;
    }
    goAsync();
    running.run = run;
    return this.#stream(request, planned, run, session, before, running).finally(finish);
  }

  #beginRun(session: EngineSession, readOnly: boolean, signal: AbortSignal): Run {
    return {
      results: emptyStreamLike(),
      done: session.begin(readOnly, signal).then(() => ({
        command: "BEGIN",
        rowCount: null,
        counts: null,
        lsn: null,
      })),
      reachedCommit: false,
      commits: false,
    };
  }

  #prepare(request: Extract<Request, { t: "prepare" }>): void {
    const engine = this.#server.engine;
    engine.ledger.settle(this.clientId, request.seq);
    let planned: PlannedStatement;
    try {
      planned = engine.plan(request.text);
    } catch (error) {
      this.post({ t: "error", seq: request.seq, error: toWireError(error) });
      return;
    }
    this.#prepared.set(request.id, { text: request.text, planned });
    this.post({
      t: "prepared",
      seq: request.seq,
      id: request.id,
      columns: planned.columns,
      parameters: planned.parameters,
      catalogVersion: planned.catalogVersion,
      controlsTransaction: planned.controlsTransaction,
      statementKind:
        planned.control === null
          ? "other"
          : planned.control === "other"
            ? "savepoint"
            : planned.control,
      readOnly: planned.lowered.kind === "transactionControl" && planned.lowered.readOnly,
    });
  }

  #columnsFor(
    request: Request,
    columns: readonly WireColumnMeta[],
    catalogVersion: number,
  ): readonly WireColumnMeta[] | null {
    if (
      request.t === "statement" &&
      request.prepared !== null &&
      request.knownColumnsVersion === catalogVersion
    )
      return null;
    return columns;
  }

  #summary(
    outcome: RunOutcome,
    session: EngineSession | null,
    closedEarly: boolean,
  ): StatementSummary {
    return {
      command: outcome.command,
      rowCount: outcome.rowCount,
      counts: outcome.counts,
      lsn: outcome.lsn,
      replayed: false,
      closedEarly,
      transaction: session?.transaction ?? "idle",
      catalogVersion: session?.catalogVersion ?? this.#server.engine.catalogVersion,
    };
  }

  /** The synchronous point read: one message, frames registered for release. */
  #answerSync(
    request: Extract<Request, { t: "statement" }>,
    planned: PlannedStatement,
    run: SyncRun,
    session: EngineSession,
    before: AnsweredRequest["before"],
  ): void {
    this.#finishLedger(request, planned, before, session, run.outcome, false, null);
    const frames = request.result.wantRows ? run.frames : [];
    if (!request.result.wantRows) for (const frame of run.frames) frame.release();
    const transfer: ArrayBuffer[] = [];
    const payloads: FramePayload[] = [];
    for (const frame of frames) {
      payloads.push(frame.payload);
      for (const buffer of frame.transfer) transfer.push(buffer);
    }
    if (frames.length > 0)
      this.#streams.set(request.seq, {
        results: noStream,
        session: request.session,
        posted: [...frames],
        finished: true,
        live: request.live || this.#livenessHeld,
        lastActivityMs: this.#server.now(),
      });
    this.post(
      {
        t: "done",
        seq: request.seq,
        columns: this.#columnsFor(request, run.columns, planned.catalogVersion),
        frames: payloads,
        summary: this.#summary(run.outcome, session, false),
      },
      transfer,
    );
    this.#sweepIfNeeded();
  }

  /** Pulls a run's frames, posts them, then the terminal reply; applies the ledger action. */
  async #stream(
    request: Request,
    planned: PlannedStatement | null,
    run: Run,
    session: EngineSession | null,
    before: AnsweredRequest["before"],
    running: Running,
  ): Promise<void> {
    const engine = this.#server.engine;
    run.done.catch(() => undefined);
    const wantRows = "result" in request ? request.result.wantRows : false;
    const columnsMeta = planned?.columns ?? [];
    let columnsSent = false;
    const stream: Stream = {
      results: run.results,
      session: running.session,
      posted: [],
      finished: false,
      live: running.live,
      lastActivityMs: this.#server.now(),
    };
    this.#streams.set(request.seq, stream);
    let pending: ResultFrame[] = [];
    const flush = () => {
      if (pending.length === 0) return;
      if (this.#state === "closed") {
        // Nobody will release them: free them now.
        for (const frame of pending) frame.release();
        pending = [];
        return;
      }
      const transfer: ArrayBuffer[] = [];
      for (const frame of pending) for (const buffer of frame.transfer) transfer.push(buffer);
      const columns = columnsSent
        ? null
        : this.#columnsFor(request, columnsMeta, planned?.catalogVersion ?? -1);
      columnsSent = true;
      this.post(
        { t: "rows", seq: request.seq, columns, frames: pending.map((frame) => frame.payload) },
        transfer,
      );
      stream.posted.push(...pending);
      stream.lastActivityMs = this.#server.now();
      pending = [];
      this.#sweepIfNeeded();
    };
    try {
      for (;;) {
        let frame = run.results.poll();
        if (frame === undefined) {
          flush();
          frame = await run.results.next();
          // The frame path: other clients' connections may be owed an `alive` by now.
          this.#server.aliveIfOverdue();
        }
        if (frame === null) break;
        if (this.#state === "closed") {
          frame.release();
          continue;
        }
        if (!wantRows) {
          frame.release();
          continue;
        }
        const wait = engine.durable(frame.requiresLsn);
        if (wait !== null) {
          flush();
          // Held in `pending` while it waits, so a failed wait releases it with the rest.
          pending.push(frame);
          await wait;
          continue;
        }
        pending.push(frame);
      }
      const outcome = await run.done;
      stream.finished = true;
      this.#finishLedger(request, planned, before, session, outcome, false, null, run);
      this.#terminalDone(
        request,
        planned,
        pending,
        columnsSent,
        columnsMeta,
        outcome,
        session,
        false,
        stream,
      );
    } catch (error) {
      stream.finished = true;
      // Frames already posted stay the client's to release; queued ones were never sent.
      for (const frame of pending) frame.release();
      pending = [];
      if (running.closeEarly) {
        // A cursor closed early: a normal end.
        run.results.cancel();
        this.#finishLedger(request, planned, before, session, null, false, null, run);
        const outcome: RunOutcome = {
          command: planned?.lowered.command ?? "",
          rowCount: null,
          counts: null,
          lsn: null,
        };
        this.#terminalDone(
          request,
          planned,
          [],
          columnsSent,
          columnsMeta,
          outcome,
          session,
          true,
          stream,
        );
        return;
      }
      // The stream fails as a consequence when the run itself failed (a failed commit discards
      // the RETURNING spool): report the run's error, not the stream's.
      let failure: unknown = error;
      await run.done.catch((runError: unknown) => {
        failure = runError;
      });
      this.#finishLedger(request, planned, before, session, null, true, failure, run);
      this.post({
        t: "error",
        seq: request.seq,
        error: toWireError(this.#mapError(failure, running)),
      });
      if (stream.posted.length === 0) this.#streams.delete(request.seq);
    }
  }

  #terminalDone(
    request: Request,
    planned: PlannedStatement | null,
    frames: ResultFrame[],
    columnsSent: boolean,
    columnsMeta: readonly WireColumnMeta[],
    outcome: RunOutcome,
    session: EngineSession | null,
    closedEarly: boolean,
    stream: Stream,
  ): void {
    if (this.#state === "closed") {
      for (const frame of frames) frame.release();
      this.#streams.delete(request.seq);
      return;
    }
    const transfer: ArrayBuffer[] = [];
    for (const frame of frames) for (const buffer of frame.transfer) transfer.push(buffer);
    stream.posted.push(...frames);
    if (stream.posted.length === 0) this.#streams.delete(request.seq);
    // A finished stream with frames outstanding stays watched (live) or swept (not live).
    this.#sweepIfNeeded();
    this.#watchIfNeeded();
    this.post(
      {
        t: "done",
        seq: request.seq,
        columns: columnsSent
          ? null
          : this.#columnsFor(request, columnsMeta, planned?.catalogVersion ?? -1),
        frames: frames.map((frame) => frame.payload),
        summary: this.#summary(outcome, session, closedEarly),
      },
      transfer,
    );
  }

  #mapError(error: unknown, running: Running): unknown {
    if (running.lost !== null) return running.lost;
    if (running.cancelled && (error instanceof QueryCancelledError || running.abort.signal.aborted))
      return new CancelledError();
    return error;
  }

  /** Applies `ledgerAction` to an answered request. */
  #finishLedger(
    request: Request,
    planned: PlannedStatement | null,
    before: AnsweredRequest["before"],
    session: EngineSession | null,
    outcome: RunOutcome | null,
    failed: boolean,
    error: unknown,
    run?: Run,
  ): void {
    const ledger = this.#server.engine.ledger;
    const after = session?.transaction ?? "idle";
    const opened =
      before === "idle" &&
      after !== "idle" &&
      (request.t === "begin" || planned?.control === "begin");
    const action = ledgerAction({
      kind: request.t,
      plan: planned?.kind ?? null,
      control: planned?.control ?? null,
      before,
      opened,
      reachedCommit: run?.reachedCommit ?? false,
      commits: run?.commits ?? false,
      failed,
    });
    const now = this.#server.now();
    switch (action) {
      case "writePath":
        // The write path recorded a commit that wrote; a no-effect commit records nothing.
        if (outcome?.lsn === null) ledger.settle(this.clientId, request.seq);
        return;
      case "failed":
        ledger.record(this.clientId, request.seq, {
          kind: "failed",
          code: errorCode(error),
          recordedAtMs: now,
        });
        return;
      case "statement":
        ledger.record(this.clientId, request.seq, {
          kind: "statement",
          status: failed ? "failed" : "ok",
          counts: outcome?.counts ?? {},
          ...(failed ? { code: errorCode(error) } : {}),
          recordedAtMs: now,
        });
        return;
      case "settle":
        ledger.settle(this.clientId, request.seq);
        return;
    }
  }

  // ---- signals ----

  #cancel(target: number, close: boolean): void {
    const queued = this.#queued.get(target);
    if (queued !== undefined) {
      queued.cancelled = true;
      queued.close ||= close;
      return;
    }
    const running = this.#running.get(target);
    if (running === undefined) {
      // Finished: closing it frees the frames the client will never take.
      const stream = this.#streams.get(target);
      if (stream?.finished === true) {
        for (const frame of stream.posted.splice(0)) frame.release();
        this.#streams.delete(target);
        this.#watchIfNeeded();
      }
      return;
    }
    // Past the commit boundary the target's own reply answers the cancel.
    if (running.run?.reachedCommit === true) return;
    running.cancelled = true;
    if (close) {
      running.closeEarly = true;
      this.#streams.get(target)?.results.cancel(new CancelledError());
      return;
    }
    running.abort.abort(new CancelledError());
    this.#streams.get(target)?.results.cancel(new CancelledError());
  }

  #release(target: number, through: number): void {
    const stream = this.#streams.get(target);
    if (stream === undefined) return;
    stream.lastActivityMs = this.#server.now();
    while (stream.posted.length > 0 && (stream.posted[0]?.payload.sequence ?? Infinity) <= through)
      stream.posted.shift()?.release();
    if (stream.session !== null) this.#sessions.get(stream.session)?.timer.touch();
    if (stream.finished && stream.posted.length === 0) {
      this.#streams.delete(target);
      this.#watchIfNeeded();
    }
  }

  // ---- liveness, heartbeats, abandoned streams ----

  /** Whether this client's liveness matters now: live requests, live frames, open transactions. */
  #livenessNeeded(): boolean {
    for (const running of this.#running.values()) if (running.live) return true;
    // Frames posted to a live client hold the executor's charge: keep watching until released.
    for (const stream of this.#streams.values())
      if (stream.live && stream.posted.length > 0) return true;
    // Only a client that declared its lock held is watched for its sessions: an undeclared lock is
    // not held, and its grant would end a healthy client.
    if (this.#livenessHeld)
      for (const slot of this.#sessions.values())
        if ((slot.engine?.transaction ?? "idle") !== "idle") return true;
    return false;
  }

  /**
   * Starts watching the client's liveness lock when needed, and stops it only after
   * `LIVENESS_IDLE_RELEASE_MS` without need: a request loop pays one lock request, not one per
   * request (lock calls cross processes). A grant ends the connection only if the client still
   * has something to protect; granted while idle, it just ends the watch.
   */
  #watchIfNeeded(): void {
    const watch = this.#server.options.watchLiveness;
    if (watch === undefined || this.#state !== "open" || this.#livenessLock === "") return;
    const needs = this.#livenessNeeded();
    if (needs) {
      if (this.#watchIdle !== null) clearTimeout(this.#watchIdle);
      this.#watchIdle = null;
      if (this.#watch !== null) return;
      const started = watch(this.#livenessLock);
      this.#watch = started;
      started.gone.addEventListener(
        "abort",
        () => {
          if (this.#watch !== started) return;
          this.#watch = null;
          if (this.#livenessNeeded()) this.end("livenessLost");
        },
        { once: true },
      );
      return;
    }
    if (this.#watch === null || this.#watchIdle !== null) return;
    this.#watchIdle = setTimeout(() => {
      this.#watchIdle = null;
      if (this.#livenessNeeded()) return;
      this.#watch?.stop();
      this.#watch = null;
    }, LIVENESS_IDLE_RELEASE_MS);
  }

  #updateAlive(): void {
    const interval = this.#server.options.aliveIntervalMs ?? ALIVE_INTERVAL_MS;
    if (this.#running.size > 0 && this.#aliveTimer === null && this.#state === "open") {
      this.#aliveTimer = setInterval(() => {
        this.post({ t: "alive" });
      }, interval);
    } else if (this.#running.size === 0 && this.#aliveTimer !== null) {
      clearInterval(this.#aliveTimer);
      this.#aliveTimer = null;
    }
  }

  #sweepIfNeeded(): void {
    const limit = this.#server.options.abandonedStreamMs ?? ABANDONED_STREAM_MS;
    const hasNonLive = [...this.#streams.values()].some(
      (stream) => !stream.live && stream.posted.length > 0,
    );
    if (hasNonLive && this.#sweepTimer === null) {
      this.#sweepTimer = setInterval(
        () => {
          this.#sweep(limit);
        },
        Math.max(1, limit / 2),
      );
    } else if (!hasNonLive && this.#sweepTimer !== null) {
      clearInterval(this.#sweepTimer);
      this.#sweepTimer = null;
    }
  }

  #sweep(limit: number): void {
    const now = this.#server.now();
    for (const [seq, stream] of this.#streams) {
      if (stream.live || stream.posted.length === 0 || now - stream.lastActivityMs < limit)
        continue;
      for (const frame of stream.posted.splice(0)) frame.release();
      const running = this.#running.get(seq);
      // "Nothing took effect" only for a run stopped here before its commit point; a statement
      // that finished or committed may have changed data (an autocommit write's RETURNING).
      const before = running !== undefined && running.run?.reachedCommit !== true;
      const lost = new SessionLostError("result", seq, before ? "none" : "unknown");
      stream.results.cancel(lost);
      if (before) {
        running.lost = lost;
        running.abort.abort(running.lost);
      }
      if (stream.finished) this.#streams.delete(seq);
    }
    this.#sweepIfNeeded();
  }

  // ---- ending ----

  end(reason: "bye" | "reconnected" | "livenessLost" | "stepDown"): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    void reason;
    // Requests past the commit boundary complete (their frames are released as they come).
    for (const running of this.#running.values()) {
      if (running.run?.reachedCommit === true) continue;
      running.lost = new SessionLostError("transaction");
      running.abort.abort(running.lost);
    }
    for (const [seq, stream] of this.#streams) {
      for (const frame of stream.posted.splice(0)) frame.release();
      const running = this.#running.get(seq);
      if (running?.run?.reachedCommit !== true)
        stream.results.cancel(
          new SessionLostError("result", seq, running === undefined ? "unknown" : "none"),
        );
    }
    this.#streams.clear();
    this.#prepared.clear();
    // The ledger ends every session's generation now, busy or not: a retry arriving before a busy
    // session's task finishes must already find it ended (format/ledger.ts `endSession`; a late
    // completion is ignored). Only the engine session waits for its task.
    for (const slot of this.#sessions.values()) {
      this.#endGeneration(slot);
      if (!slot.busy) this.#closeSession(slot);
    }
    if (this.#aliveTimer !== null) clearInterval(this.#aliveTimer);
    if (this.#sweepTimer !== null) clearInterval(this.#sweepTimer);
    this.#aliveTimer = null;
    this.#sweepTimer = null;
    this.#watch?.stop();
    this.#watch = null;
    if (this.#watchIdle !== null) clearTimeout(this.#watchIdle);
    this.#watchIdle = null;
    this.#server.dropped(this);
    this.#port.onmessage = null;
    this.#port.close?.();
  }

  /** Rolls back a session of an ended connection and ends its generation in the ledger. */
  #closeSession(slot: SessionSlot): void {
    slot.queue.length = 0;
    slot.timer.dispose();
    slot.engine?.close();
    this.#endGeneration(slot);
    this.#sessions.delete(slot.id);
  }

  /** Ends this connection's generation of `slot`'s session in the ledger (idempotent). */
  #endGeneration(slot: SessionSlot): void {
    if (this.clientId !== "")
      this.#server.engine.ledger.endSession(
        this.clientId,
        String(slot.id),
        this.generation,
        this.#server.now(),
      );
  }
}

/** A finished, empty result stream (no frames). */
const noStream: ResultStream = {
  columns: [],
  poll: () => null,
  next: () => Promise.resolve(null),
  cancel: () => undefined,
};

function emptyStreamLike(): ResultStream {
  return noStream;
}
