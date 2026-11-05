/**
 * Stand-ins for the tracks whose implementations have not landed, so the owner's composition
 * (owner/compose.ts) and protocol server (owner/server.ts) run end to end in Node:
 *
 * - `StandinWritePath` implements B3's `WritePath`: one writer slot (FIFO), transactions over a
 *   copied table state, savepoints, group commit with a controllable flush, strict/relaxed
 *   visibility, the request ledger (committed outcomes recorded once durable / applied), and
 *   sequences.
 * - `StandinExecutor` implements B4's `Executor` for the programs the stand-in planner attaches to
 *   its plans: frames through B4's real `ResultChannel` (window, releases, cancellation), the
 *   RETURNING spool (`atCommit` → `acknowledgeCommit`), synchronous completion for point reads,
 *   asynchronous streaming for `… STREAM BY n` scans.
 * - `StandinPlanner` implements the owner's planner port for a fixed SQL subset (see `lower`).
 *
 * They honour the contracts the composition depends on; they are not models of the real engines
 * (no MVCC beyond copies, no key index, no pages). Replace each with the real track as it lands.
 */
import type { CatalogChange, CatalogView, TableDef } from "../access/catalog.js";
import type { Statement, StatementOptions, TableRules } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import type {
  BeginOptions,
  CommitReceipt,
  CommitRequest,
  PinOptions,
  Savepoint,
  SequenceAccess,
  WritePath,
  WritePathStats,
  WriteTransaction,
} from "../access/write-path.js";
import { TransactionStateError } from "../access/write-path.js";
import { ExecutionError, QueryCancelledError } from "../exec/errors.js";
import type {
  BoundParameters,
  ExecuteRequest,
  Execution,
  ExecutionState,
  ExecutionSummary,
  Executor,
  ParameterValue,
  PreparedPlan,
} from "../exec/executor.js";
import {
  ResultChannel,
  frameByteLength,
  type UnnumberedPayload,
  type WireFlat,
} from "../exec/result.js";
import { RequestLedger } from "../format/ledger.js";
import type { Grant } from "../memory/governor.js";
import type { OutputColumn, PhysicalPlan } from "../ir/ir.js";
import type {
  LoweredModify,
  LoweredStatement,
  PlannerContext,
  StatementPlanner,
  WriteShape,
} from "../owner/planner.js";
import type { WireColumnMeta } from "../owner/protocol.js";
import type { PhysicalType } from "../types.js";
import type { Int64Values } from "../vector/vector.js";

type Value = number | string | bigint | null;
type Row = readonly Value[];

interface TableState {
  readonly def: TableDef;
  /** By primary-key value. Rows are immutable; an update replaces the row. */
  readonly rows: Map<string, Row>;
}

interface State {
  version: number;
  nextTableId: number;
  readonly tables: Map<number, TableState>;
}

const cloneState = (state: State): State => ({
  version: state.version,
  nextTableId: state.nextTableId,
  tables: new Map(
    [...state.tables].map(([id, table]) => [id, { def: table.def, rows: new Map(table.rows) }]),
  ),
});

function catalogOf(state: State): CatalogView {
  const tables = [...state.tables.values()].map((table) => table.def);
  return {
    version: state.version,
    tables,
    sequences: [],
    views: [],
    table: (id) => state.tables.get(id)?.def,
    tableByName: (name) => tables.find((table) => table.name === name),
    sequence: () => undefined,
  };
}

/** A view over a copied state: what the stand-in executor reads. */
class StandinSnapshot {
  released = false;
  readonly rootSequence = 0;
  constructor(
    readonly state: State,
    readonly observedLsn: number,
    readonly kind: "read" | "statement",
    private readonly onRelease: () => void = () => undefined,
  ) {}
  get bound(): number {
    return this.observedLsn;
  }
  get catalog(): CatalogView {
    return catalogOf(this.state);
  }
  release(): void {
    if (this.released) return;
    this.released = true;
    this.onRelease();
  }
}

class StandinStatement {
  readonly view: StandinSnapshot;
  readonly #tx: StandinTransaction;
  readonly #before: State;
  #ended = false;
  wrote = 0;

  constructor(
    tx: StandinTransaction,
    readonly ordinal: number,
    readonly options: StatementOptions,
  ) {
    this.#tx = tx;
    this.#before = cloneState(tx.working);
    this.view = new StandinSnapshot(tx.working, tx.baseLsn, "statement");
  }

  get state(): State {
    return this.#tx.working;
  }

  get catalog(): CatalogView {
    return catalogOf(this.#tx.working);
  }

  table(): never {
    throw new TypeError("The stand-in executor writes through `state`, not table writers");
  }

  changeCatalog(change: CatalogChange): { readonly id: number | null } {
    if (change.kind !== "createTable")
      throw new TypeError(`The stand-in supports createTable only, not ${change.kind}`);
    const state = this.#tx.working;
    if (catalogOf(state).tableByName(change.table.name) !== undefined)
      throw new ExecutionError(
        `relation "${change.table.name}" already exists`,
        "MINNOW_DUPLICATE_TABLE",
        "42P07",
      );
    const id = state.nextTableId++;
    const def: TableDef = {
      id,
      name: change.table.name,
      schemaVersion: 1,
      columns: change.table.columns.map((column, index) => ({ ...column, id: index + 1 })),
      primaryKey: change.table.primaryKey?.[0] ?? null,
      indexes: [],
      foreignKeys: [],
      checks: [],
    };
    state.tables.set(id, { def, rows: new Map() });
    state.version++;
    this.wrote++;
    return { id };
  }

  end(): Promise<void> {
    this.#ended = true;
    if (this.wrote > 0) this.#tx.wrote = true;
    return Promise.resolve();
  }

  rollback(): void {
    if (this.#ended) return;
    this.#ended = true;
    this.#tx.working = this.#before;
  }
}

class StandinTransaction {
  working: State;
  wrote = false;
  state: WriteTransaction["state"] = "open";
  #statements = 0;
  readonly #savepoints: State[] = [];

  constructor(
    private readonly path: StandinWritePath,
    readonly baseLsn: number,
  ) {
    this.working = cloneState(path.applied);
  }

  get catalog(): CatalogView {
    return catalogOf(this.working);
  }

  statement(options: StatementOptions): Statement {
    if (this.state !== "open") throw new TransactionStateError("The transaction is not open");
    return new StandinStatement(this, ++this.#statements, options) as unknown as Statement;
  }

  savepoint(): Savepoint {
    this.#savepoints.push(cloneState(this.working));
    return { statement: this.#savepoints.length - 1 };
  }

  rollbackTo(savepoint: Savepoint): void {
    const state = this.#savepoints[savepoint.statement];
    if (state === undefined) throw new TransactionStateError("Unknown savepoint");
    this.working = cloneState(state);
    this.#savepoints.length = savepoint.statement + 1;
  }

  release(savepoint: Savepoint): void {
    this.#savepoints.length = savepoint.statement;
  }

  commit(request: CommitRequest | null): Promise<CommitReceipt> {
    if (this.state !== "open")
      return Promise.reject(new TransactionStateError("The transaction is not open"));
    this.state = "committing";
    return Promise.resolve(this.path.commit(this, request));
  }

  rollback(): void {
    if (this.state !== "open") return;
    this.state = "rolledBack";
    this.path.releaseSlot(this);
  }
}

export interface StandinWritePathOptions {
  readonly durability?: "strict" | "relaxed";
  /** Flush automatically after each commit (a timer of `flushDelayMs`). Default true. */
  readonly autoFlush?: boolean;
  readonly flushDelayMs?: number;
  readonly now?: () => number;
}

export class StandinWritePath implements WritePath {
  readonly durability: "strict" | "relaxed";
  readonly ledger = new RequestLedger();
  readonly failure: Error | null = null;
  appliedLsn = 0;
  durableLsn = 0;
  applied: State = { version: 1, nextTableId: 1, tables: new Map() };
  /** The state readers see under strict durability: as of the last flush. */
  #durable: State = cloneState(this.applied);
  readonly #autoFlush: boolean;
  readonly #flushDelayMs: number;
  readonly #now: () => number;
  #slot: StandinTransaction | null = null;
  readonly #waiters: Array<{
    resolve: (tx: StandinTransaction) => void;
    reject: (error: unknown) => void;
    signal?: AbortSignal;
  }> = [];
  readonly #durableWaiters: Array<{ lsn: number; resolve: () => void }> = [];
  readonly #pendingOutcomes: Array<{ lsn: number; request: CommitRequest }> = [];
  #flushTimer: ReturnType<typeof setTimeout> | null = null;
  readonly #sequenceValues = new Map<number, bigint>();
  pins = 0;
  flushes = 0;

  constructor(options: StandinWritePathOptions = {}) {
    this.durability = options.durability ?? "strict";
    this.#autoFlush = options.autoFlush ?? true;
    this.#flushDelayMs = options.flushDelayMs ?? 0;
    this.#now = options.now ?? Date.now;
  }

  get catalog(): CatalogView {
    return catalogOf(this.applied);
  }

  /** Whether a transaction holds the writer slot. */
  get writerBusy(): boolean {
    return this.#slot !== null;
  }

  readonly sequences: SequenceAccess = {
    next: (sequenceId: number, count: number, out: Int64Values, offset: number): number => {
      let value = this.#sequenceValues.get(sequenceId) ?? 0n;
      for (let i = 0; i < count; i++) {
        value++;
        if (out.repr === "safe") out.values[offset + i] = Number(value);
        else out.values[offset + i] = value;
      }
      this.#sequenceValues.set(sequenceId, value);
      // A block frame of its own (ruling R4): durable with the next flush.
      const lsn = ++this.appliedLsn;
      this.#scheduleFlush();
      return lsn;
    },
    set: (sequenceId: number, value: bigint): number => {
      this.#sequenceValues.set(sequenceId, value);
      const lsn = ++this.appliedLsn;
      this.#scheduleFlush();
      return lsn;
    },
  };

  pin(options: PinOptions = {}): Snapshot {
    const strict = this.durability === "strict" && (options.atLeastLsn ?? 0) <= this.durableLsn;
    this.pins++;
    const state = strict ? this.#durable : this.applied;
    return new StandinSnapshot(state, strict ? this.durableLsn : this.appliedLsn, "read", () => {
      this.pins--;
    }) as unknown as Snapshot;
  }

  begin(options: BeginOptions = {}): Promise<WriteTransaction> {
    const signal = options.signal;
    if (signal?.aborted === true) return Promise.reject(signal.reason as Error);
    if (this.#slot === null) {
      const tx = new StandinTransaction(this, this.appliedLsn);
      this.#slot = tx;
      return Promise.resolve(tx as unknown as WriteTransaction);
    }
    return new Promise<WriteTransaction>((resolve, reject) => {
      const waiter = {
        resolve: (tx: StandinTransaction) => {
          resolve(tx);
        },
        reject,
        ...(signal === undefined ? {} : { signal }),
      };
      this.#waiters.push(waiter);
      signal?.addEventListener(
        "abort",
        () => {
          const index = this.#waiters.indexOf(waiter);
          if (index >= 0) {
            this.#waiters.splice(index, 1);
            reject(
              signal.reason instanceof Error ? signal.reason : new Error(String(signal.reason)),
            );
          }
        },
        { once: true },
      );
    });
  }

  releaseSlot(tx: StandinTransaction): void {
    if (this.#slot !== tx) return;
    this.#slot = null;
    const next = this.#waiters.shift();
    if (next !== undefined) {
      const granted = new StandinTransaction(this, this.appliedLsn);
      this.#slot = granted;
      next.resolve(granted);
    }
  }

  commit(tx: StandinTransaction, request: CommitRequest | null): CommitReceipt {
    const wrote = tx.wrote || (request !== null && tx.wrote);
    let lsn = tx.baseLsn;
    if (wrote) {
      lsn = ++this.appliedLsn;
      this.applied = tx.working;
    }
    tx.state = "committed";
    this.releaseSlot(tx);
    if (!wrote) return { wrote: false, lsn, durable: Promise.resolve() };
    if (request !== null) {
      if (this.durability === "relaxed") this.#record(lsn, request);
      else this.#pendingOutcomes.push({ lsn, request });
    }
    const durable = new Promise<void>((resolve) => {
      this.#durableWaiters.push({ lsn, resolve });
    });
    this.#scheduleFlush();
    return { wrote: true, lsn, durable };
  }

  #record(lsn: number, request: CommitRequest): void {
    const clientId = [...request.clientId]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
    this.ledger.record(clientId, request.sequence, {
      kind: "committed",
      lsn,
      counts: request.counts,
      recordedAtMs: this.#now(),
    });
  }

  #scheduleFlush(): void {
    if (!this.#autoFlush || this.#flushTimer !== null) return;
    this.#flushTimer = setTimeout(() => {
      this.#flushTimer = null;
      void this.flush();
    }, this.#flushDelayMs);
  }

  durable(lsn: number): Promise<void> | null {
    if (lsn <= this.durableLsn) return null;
    return new Promise<void>((resolve) => {
      this.#durableWaiters.push({ lsn, resolve });
    });
  }

  /** One group flush: everything applied so far becomes durable. */
  flush(): Promise<void> {
    this.flushes++;
    this.durableLsn = this.appliedLsn;
    this.#durable = this.applied;
    for (const outcome of this.#pendingOutcomes.splice(0))
      this.#record(outcome.lsn, outcome.request);
    const ready = this.#durableWaiters.filter((waiter) => waiter.lsn <= this.durableLsn);
    for (const waiter of ready) {
      this.#durableWaiters.splice(this.#durableWaiters.indexOf(waiter), 1);
      waiter.resolve();
    }
    return Promise.resolve();
  }

  stats(): WritePathStats {
    return {
      appliedLsn: this.appliedLsn,
      durableLsn: this.durableLsn,
      logBytes: 0,
      memtableBytes: 0,
      frozenMemtables: 0,
      frozenMemtableBytes: 0,
      borrowedChunkBytes: 0,
      repointedReaders: 0,
      repointPins: 0,
      levelZeroRuns: 0,
      garbageBytes: 0,
      pins: this.pins,
      oldestPin: null,
      backpressure: false,
    };
  }

  close(): Promise<void> {
    if (this.#flushTimer !== null) clearTimeout(this.#flushTimer);
    return this.flush();
  }

  /** Test helper: rows of a table by name, as committed in memory. */
  rows(table: string): Row[] {
    const def = catalogOf(this.applied).tableByName(table);
    if (def === undefined) return [];
    return [...(this.applied.tables.get(def.id)?.rows.values() ?? [])].sort(
      (a, b) => Number(a[0]) - Number(b[0]),
    );
  }
}

// ---- executor ----

interface ProgramContext {
  readonly params: readonly ParameterValue[];
  /** The view's state (read) and, for DML, the transaction's working state (write). */
  readonly state: State;
  readonly statement: StandinStatement | null;
  readonly sequences: SequenceAccess | null;
  readonly sequenceSession: ExecuteRequest["sequenceSession"];
}

interface ProgramResult {
  readonly rows: Row[];
  readonly affected: number;
  readonly requiresLsn: number;
}

interface Program {
  readonly kind: "query" | "modify";
  readonly columns: readonly OutputColumn[];
  readonly run: (context: ProgramContext) => ProgramResult;
  /** Stream frames of this many rows asynchronously (window and cancellation tests). */
  readonly streamBy: number | null;
}

const programs = new WeakMap<PhysicalPlan, Program>();
const boundValues = new WeakMap<BoundParameters, readonly ParameterValue[]>();

function columnVector(type: PhysicalType, values: readonly Value[]): WireFlat {
  const length = values.length;
  let validity: Uint32Array | null = null;
  values.forEach((value, row) => {
    if (value !== null) return;
    validity ??= new Uint32Array(Math.ceil(length / 32)).fill(0xffffffff);
    validity[row >>> 5] = (validity[row >>> 5] ?? 0) & ~(1 << (row & 31));
  });
  if (type.kind === "text") {
    const encoder = new TextEncoder();
    const parts = values.map((value) => encoder.encode(value === null ? "" : String(value)));
    const offsets = new Int32Array(length + 1);
    parts.forEach((part, row) => {
      offsets[row + 1] = (offsets[row] ?? 0) + part.length;
    });
    const bytes = new Uint8Array(offsets[length] ?? 0);
    parts.forEach((part, row) => {
      bytes.set(part, offsets[row] ?? 0);
    });
    return { form: "flat", length, validity, data: { layout: "varlen", bytes, offsets } };
  }
  if (type.kind === "int64")
    return {
      form: "flat",
      length,
      validity,
      data: {
        layout: "int64",
        values: { repr: "safe", values: Float64Array.from(values, (value) => Number(value ?? 0)) },
      },
    };
  return {
    form: "flat",
    length,
    validity,
    data: { layout: "i32", values: Int32Array.from(values, (value) => Number(value ?? 0)) },
  };
}

function payloadOf(columns: readonly OutputColumn[], rows: readonly Row[]): UnnumberedPayload {
  return {
    rowCount: rows.length,
    dictionaries: [],
    columns: columns.map((column, index) =>
      columnVector(
        column.type,
        rows.map((row) => row[index] ?? null),
      ),
    ),
  };
}

const profile = (synchronous: boolean): ExecutionSummary["profile"] => ({
  operators: [],
  pipelines: 1,
  memoryWaitMs: 0,
  peakHeldBytes: 0,
  synchronous,
});

/**
 * One execution, following B4's contract (exec/executor.ts, exec/result.ts) where the owner can
 * observe it:
 * - frames carry the run's `requiresLsn` (the view's observed LSN and every sequence block it drew
 *   from), so the owner's delivery rule waits for them under strict durability;
 * - a spillable result grant (`governor.tryAdmit`) is charged for every frame from offer to
 *   release, and for the RETURNING spool while it waits for `acknowledgeCommit()`; it closes when
 *   the stream is delivered or cancelled;
 * - `executed` settles when the last frame has been offered (DML: after `statement.end()`),
 *   `delivered` when the stream has ended and every frame was released; a cancel or failure
 *   rejects both;
 * - the spool replays into frames under the window (`ready()`), one frame per `rowsPerFrame`.
 */
class StandinExecution implements Execution {
  readonly results: ResultChannel;
  state: ExecutionState = "running";
  summary: ExecutionSummary | null = null;
  readonly executed: Promise<ExecutionSummary>;
  readonly delivered: Promise<void>;
  readonly #grant: Grant;
  #spool: { readonly rows: Row[]; readonly bytes: number } | null = null;
  #cancelled = false;
  #ended = false;
  #offered = 0;
  #freed = 0;
  #requiresLsn = 0;
  #resolveExecuted: (summary: ExecutionSummary) => void = () => undefined;
  #rejectExecuted: (error: unknown) => void = () => undefined;
  #resolveDelivered: () => void = () => undefined;
  #rejectDelivered: (error: unknown) => void = () => undefined;

  constructor(
    private readonly program: Program,
    private readonly request: ExecuteRequest,
    params: readonly ParameterValue[],
    private readonly acknowledged: () => void = () => undefined,
  ) {
    this.results = new ResultChannel(program.columns, {
      ...(request.resultWindowBytes === undefined
        ? {}
        : { windowBytes: request.resultWindowBytes }),
      onCancel: (reason) => {
        this.cancel(reason);
      },
    });
    const grant = request.governor.tryAdmit({
      label: `query#${String(request.queryId)}/result`,
      minimumBytes: 0,
      spillable: true,
    });
    if (grant === null) throw new TypeError("The stand-in executor needs an uncontended governor");
    this.#grant = grant;
    this.executed = new Promise<ExecutionSummary>((resolve, reject) => {
      this.#resolveExecuted = resolve;
      this.#rejectExecuted = reject;
    });
    this.executed.catch(() => undefined);
    this.delivered = new Promise<void>((resolve, reject) => {
      this.#resolveDelivered = resolve;
      this.#rejectDelivered = reject;
    });
    this.delivered.catch(() => undefined);
    void this.delivered.then(
      () => {
        this.#grant.close();
      },
      () => {
        this.#grant.close();
      },
    );
    const signal = request.signal;
    if (signal !== undefined) {
      const onAbort = () => {
        this.cancel(signal.reason);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      // The listener lives only as long as the run (B4 removes its listeners the same way).
      void this.delivered
        .catch(() => undefined)
        .finally(() => {
          signal.removeEventListener("abort", onAbort);
        });
    }
    this.#start(params);
  }

  #charge(bytes: number): void {
    if (!this.#grant.allocate(bytes) && this.#grant.tryGrow(bytes)) this.#grant.allocate(bytes);
  }

  #start(params: readonly ParameterValue[]): void {
    const request = this.request;
    const statement = request.statement as unknown as StandinStatement | null;
    const view = (request.snapshot ?? statement?.view) as unknown as StandinSnapshot;
    let result: ProgramResult;
    try {
      if (view.released) throw new TypeError("Read through a released snapshot");
      result = this.program.run({
        params,
        state: statement === null ? view.state : statement.state,
        statement,
        sequences: request.sequences,
        sequenceSession: request.sequenceSession,
      });
    } catch (error) {
      this.#fail(error);
      return;
    }
    this.#requiresLsn = Math.max(result.requiresLsn, view.observedLsn);
    const summary: ExecutionSummary = {
      rowCount: this.program.kind === "modify" ? result.affected : result.rows.length,
      counts: { inserted: 0, updated: 0, deleted: 0 },
      requiresLsn: this.#requiresLsn,
      profile: profile(this.program.streamBy === null),
    };
    if (this.program.kind === "modify" && statement !== null) {
      statement.wrote += result.affected;
      const spool = this.request.returning === "atCommit" && this.program.columns.length > 0;
      if (spool) {
        const bytes = frameByteLength(payloadOf(this.program.columns, result.rows));
        this.#charge(bytes);
        this.#spool = { rows: result.rows, bytes };
      }
      // B4 ends the statement before `executed` (deferred checks): asynchronous.
      void statement.end().then(() => {
        if (this.#cancelled) return;
        if (!spool) void this.#emit(result.rows, null);
        this.#executedWith(summary);
      });
      return;
    }
    if (this.program.streamBy !== null) {
      void this.#emit(result.rows, this.program.streamBy).then(() => {
        this.#executedWith(summary);
      });
      return;
    }
    void this.#emit(result.rows, null);
    this.#executedWith(summary);
  }

  #executedWith(summary: ExecutionSummary): void {
    if (this.#cancelled || this.state !== "running") return;
    this.state = "executed";
    this.summary = summary;
    this.#resolveExecuted(summary);
  }

  /** Offers `rows` as frames of `by` rows (all at once when null) under the window, then ends. */
  async #emit(all: readonly Row[], by: number | null): Promise<void> {
    // A plan without output columns (DML without RETURNING) produces no frames.
    const rows = this.program.columns.length === 0 ? [] : all;
    const size = by ?? Math.max(1, rows.length);
    try {
      for (let start = 0; start < rows.length; start += size) {
        if (this.#cancelled) return;
        const payload = payloadOf(this.program.columns, rows.slice(start, start + size));
        const bytes = frameByteLength(payload);
        this.#charge(bytes);
        this.#offered++;
        const pushed = this.results.offer({
          payload,
          requiresLsn: this.#requiresLsn,
          free: () => {
            this.#grant.free(bytes);
            this.#freed++;
            this.#checkDelivered();
          },
        });
        if (pushed === "blocked" && start + size < rows.length) await this.results.ready();
      }
      if (this.#cancelled) return;
      this.results.end();
      this.#ended = true;
      this.#checkDelivered();
    } catch (error) {
      if (!this.#cancelled) this.#fail(error);
    }
  }

  #checkDelivered(): void {
    if (this.#ended && this.#freed === this.#offered && !this.#cancelled) this.#resolveDelivered();
  }

  #fail(error: unknown): void {
    (this.request.statement as unknown as StandinStatement | null)?.rollback();
    this.state = this.#cancelled ? "cancelled" : "failed";
    this.#dropSpool();
    this.results.fail(error);
    this.#rejectExecuted(error);
    this.#rejectDelivered(error);
  }

  #dropSpool(): void {
    if (this.#spool === null) return;
    this.#grant.free(this.#spool.bytes);
    this.#spool = null;
  }

  acknowledgeCommit(): void {
    this.acknowledged();
    const spool = this.#spool;
    if (spool === null) return;
    this.#spool = null;
    // The spool moves into frames: its charge becomes theirs.
    this.#grant.free(spool.bytes);
    void this.#emit(spool.rows, null);
  }

  cancel(reason?: unknown): void {
    if (this.#cancelled) return;
    this.#cancelled = true;
    this.#dropSpool();
    const error = new QueryCancelledError(reason);
    if (this.state === "running") {
      (this.request.statement as unknown as StandinStatement | null)?.rollback();
      this.state = "cancelled";
      this.#rejectExecuted(error);
    }
    this.#rejectDelivered(error);
    this.results.cancel(reason);
  }
}

export class StandinExecutor implements Executor {
  prepared = 0;
  /** Every result channel created, for frame-accounting assertions. */
  readonly channels = new Set<ResultChannel>();
  /** Called on every `acknowledgeCommit()` (completion 2), for ordering assertions. */
  onAcknowledge: (() => void) | null = null;

  /** Frame bytes still charged across every execution (B4's accounting). */
  get outstandingBytes(): number {
    let bytes = 0;
    for (const channel of this.channels) bytes += channel.outstandingBytes;
    return bytes;
  }

  prepare(plan: PhysicalPlan): PreparedPlan {
    const program = programs.get(plan);
    if (program === undefined) throw new TypeError("Not a stand-in plan");
    this.prepared++;
    return {
      plan,
      columns: program.columns,
      kind: program.kind,
      usesSequences: false,
      bind: (values) => {
        if (values.length !== plan.placeholderCount)
          throw new ExecutionError(
            `Expected ${String(plan.placeholderCount)} parameters`,
            "MINNOW_PARAMETER_BIND",
            "08P01",
          );
        const bound: BoundParameters = { slots: [] };
        boundValues.set(bound, values);
        return bound;
      },
      execute: (request) => {
        const execution = new StandinExecution(
          program,
          request,
          boundValues.get(request.parameters) ?? [],
          () => this.onAcknowledge?.(),
        );
        this.channels.add(execution.results);
        return execution;
      },
    };
  }
}

/** Rules are unused by the stand-ins (no CHECK, no generated columns). */
export const standinRules = (): TableRules => ({}) as TableRules;

// ---- planner ----

const INT: PhysicalType = { kind: "int32", bits: 32 };
const TEXT: PhysicalType = { kind: "text" };
const BIGINT: PhysicalType = { kind: "int64" };

const meta = (column: {
  readonly name: string;
  readonly type: PhysicalType;
  readonly nullable: boolean;
}): WireColumnMeta => ({
  name: column.name,
  type: column.type,
  nullable: column.nullable,
  sqlType:
    column.type.kind === "text" ? "text" : column.type.kind === "int64" ? "bigint" : "integer",
  render: column.type.kind === "int64" ? "bigint" : null,
});

function plan(
  columns: readonly OutputColumn[],
  placeholders: number,
  version: number,
  program: Omit<Program, "columns">,
): PhysicalPlan {
  const physical = {
    root: { id: 0, op: "values", rows: [], output: columns },
    columns,
    parameters: Array.from({ length: placeholders }, () => INT),
    placeholderCount: placeholders,
    clockSlot: null,
    initPlans: [],
    catalogVersion: version,
    volatile: false,
  } as unknown as PhysicalPlan;
  programs.set(physical, { ...program, columns });
  return physical;
}

function coerce(type: PhysicalType, value: ParameterValue): Value {
  if (value === null) return null;
  if (type.kind === "text") return String(value);
  if (type.kind === "int64") return typeof value === "bigint" ? Number(value) : Number(value);
  if (typeof value !== "number" || !Number.isInteger(value))
    throw new ExecutionError(
      `invalid input for integer: ${String(value)}`,
      "MINNOW_PARAMETER_BIND",
      "22P02",
    );
  return value;
}

const tableOf = (state: State, name: string): TableState => {
  const def = catalogOf(state).tableByName(name);
  const table = def === undefined ? undefined : state.tables.get(def.id);
  if (table === undefined)
    throw new ExecutionError(
      `relation "${name}" does not exist`,
      "MINNOW_UNDEFINED_TABLE",
      "42P01",
    );
  return table;
};

export class PlanningError extends Error {
  readonly code = "MINNOW_SYNTAX";
  readonly sqlState = "42601";
  constructor(message: string) {
    super(message);
    this.name = "PlanningError";
  }
}

/**
 * The SQL the stand-in plans (case-insensitive, one statement):
 * `CREATE TABLE t (k INT PRIMARY KEY, c INT|TEXT|BIGINT, …)`; `SELECT * FROM t WHERE k = $1`
 * (point read); `SELECT * FROM t [STREAM BY n]`; `SELECT nextval('s')`; `SELECT fail()` (a 22012
 * error); `INSERT INTO t VALUES ($1, …) [RETURNING *]`; `UPDATE t SET c = c + 1 WHERE k = $1
 * [RETURNING *]`; `DELETE FROM t WHERE k = $1`; BEGIN [READ ONLY], START TRANSACTION, COMMIT,
 * END, ROLLBACK, SAVEPOINT x, ROLLBACK TO x, RELEASE x; SHOW x; SET x = y.
 */
export class StandinPlanner implements StatementPlanner {
  lowered = 0;

  lower(text: string, context: PlannerContext): LoweredStatement {
    this.lowered++;
    const catalog = context.catalog;
    const sql = text.trim().replace(/;$/u, "").trim();
    const base = {
      text,
      catalogVersion: catalog.version,
      controlsTransaction: false,
      parameters: [] as PhysicalType[],
    };
    let match: RegExpExecArray | null;

    const control = (
      action: "begin" | "commit" | "rollback" | "savepoint" | "rollbackTo" | "release",
      command: string,
      savepoint: string | null = null,
      readOnly = false,
    ): LoweredStatement => ({
      ...base,
      kind: "transactionControl",
      command,
      controlsTransaction: true,
      action,
      savepoint,
      readOnly,
    });
    if (/^(BEGIN|START TRANSACTION)( READ ONLY)?$/iu.test(sql))
      return control("begin", "BEGIN", null, /READ ONLY/iu.test(sql));
    if (/^(COMMIT|END)$/iu.test(sql)) return control("commit", "COMMIT");
    if (/^(ROLLBACK|ABORT)$/iu.test(sql)) return control("rollback", "ROLLBACK");
    if ((match = /^SAVEPOINT (\w+)$/iu.exec(sql)) !== null)
      return control("savepoint", "SAVEPOINT", match[1] ?? "");
    if ((match = /^ROLLBACK TO (?:SAVEPOINT )?(\w+)$/iu.exec(sql)) !== null)
      return control("rollbackTo", "ROLLBACK", match[1] ?? "");
    if ((match = /^RELEASE (?:SAVEPOINT )?(\w+)$/iu.exec(sql)) !== null)
      return control("release", "RELEASE", match[1] ?? "");
    if ((match = /^SHOW (\w+)$/iu.exec(sql)) !== null)
      return {
        ...base,
        kind: "utility",
        command: "SHOW",
        show: { column: meta({ name: match[1] ?? "", type: TEXT, nullable: false }), value: "UTC" },
      };
    if (/^SET /iu.test(sql)) return { ...base, kind: "utility", command: "SET", show: null };

    if ((match = /^CREATE TABLE (\w+) \((.+)\)$/iu.exec(sql)) !== null) {
      const columns = (match[2] ?? "").split(",").map((part) => part.trim().split(/\s+/u));
      const primary = columns.findIndex((part) => /PRIMARY/iu.test(part.join(" ")));
      return {
        ...base,
        kind: "ddl",
        command: "CREATE TABLE",
        followUp: null,
        change:
          catalog.tableByName(match[1] ?? "") !== undefined
            ? null
            : {
                kind: "createTable",
                table: {
                  name: match[1] ?? "",
                  columns: columns.map((part) => ({
                    name: part[0] ?? "",
                    type: /TEXT/iu.test(part[1] ?? "")
                      ? TEXT
                      : /BIGINT/iu.test(part[1] ?? "")
                        ? BIGINT
                        : INT,
                    declared: {
                      type: "number",
                      declaredTypeName: part[1] ?? "",
                    } as unknown as TableDef["columns"][number]["declared"],
                    nullable: !/PRIMARY/iu.test(part.join(" ")),
                    defaultSql: null,
                    generatedSql: null,
                    identitySequence: null,
                    missing: null,
                  })),
                  primaryKey: primary < 0 ? null : [primary],
                  unique: [],
                  foreignKeys: [],
                  checks: [],
                },
              },
      };
    }

    if (/^SELECT fail\(\)$/iu.test(sql)) {
      const columns = [{ name: "fail", type: INT, nullable: true }];
      return {
        ...base,
        kind: "query",
        command: "SELECT",
        reads: [],
        pointRead: false,
        columns: columns.map(meta),
        plan: plan(columns, 0, catalog.version, {
          kind: "query",
          streamBy: null,
          run: () => {
            throw new ExecutionError("division by zero", "MINNOW_DIVISION_BY_ZERO", "22012");
          },
        }),
      };
    }

    if ((match = /^SELECT nextval\('(\w+)'\)$/iu.exec(sql)) !== null) {
      const name = match[1] ?? "";
      let id = 0;
      for (let i = 0; i < name.length; i++) id += name.charCodeAt(i);
      const columns = [{ name: "nextval", type: BIGINT, nullable: false }];
      return {
        ...base,
        kind: "query",
        command: "SELECT",
        reads: [],
        pointRead: false,
        columns: columns.map(meta),
        plan: plan(columns, 0, catalog.version, {
          kind: "query",
          streamBy: null,
          run: (context) => {
            const out: Int64Values = { repr: "safe", values: new Float64Array(1) };
            const lsn = context.sequences?.next(id, 1, out, 0) ?? 0;
            const value = out.values[0] ?? 0;
            context.sequenceSession?.record(id, BigInt(value));
            return { rows: [[value]], affected: 0, requiresLsn: lsn };
          },
        }),
      };
    }

    const tableDef = (name: string): TableDef => {
      const def = catalog.tableByName(name);
      if (def === undefined)
        throw new ExecutionError(
          `relation "${name}" does not exist`,
          "MINNOW_UNDEFINED_TABLE",
          "42P01",
        );
      return def;
    };
    const outputs = (def: TableDef): OutputColumn[] =>
      def.columns.map((column) => ({
        name: column.name,
        type: column.type,
        nullable: column.nullable,
      }));

    if ((match = /^SELECT \* FROM (\w+) WHERE (\w+) = \$1$/iu.exec(sql)) !== null) {
      const def = tableDef(match[1] ?? "");
      const columns = outputs(def);
      const name = def.name;
      return {
        ...base,
        kind: "query",
        command: "SELECT",
        parameters: [INT],
        reads: [def.id],
        pointRead: true,
        columns: columns.map(meta),
        plan: plan(columns, 1, catalog.version, {
          kind: "query",
          streamBy: null,
          run: (context) => {
            const row = tableOf(context.state, name).rows.get(String(context.params[0]));
            return { rows: row === undefined ? [] : [row], affected: 0, requiresLsn: 0 };
          },
        }),
      };
    }

    if ((match = /^SELECT \* FROM (\w+)(?: STREAM BY (\d+))?$/iu.exec(sql)) !== null) {
      const def = tableDef(match[1] ?? "");
      const columns = outputs(def);
      const name = def.name;
      return {
        ...base,
        kind: "query",
        command: "SELECT",
        reads: [def.id],
        pointRead: false,
        columns: columns.map(meta),
        plan: plan(columns, 0, catalog.version, {
          kind: "query",
          streamBy: match[2] === undefined ? null : Number(match[2]),
          run: (context) => ({
            rows: [...tableOf(context.state, name).rows.values()].sort(
              (a, b) => Number(a[0]) - Number(b[0]),
            ),
            affected: 0,
            requiresLsn: 0,
          }),
        }),
      };
    }

    const modify = (
      def: TableDef,
      returning: boolean,
      placeholders: number,
      command: string,
      run: Program["run"],
    ): LoweredModify => {
      const columns = returning ? outputs(def) : [];
      return {
        ...base,
        kind: "modify",
        command,
        parameters: Array.from({ length: placeholders }, () => INT),
        reads: [def.id],
        writes: [def.id],
        columns: columns.map(meta),
        plan: plan(columns, placeholders, catalog.version, { kind: "modify", streamBy: null, run }),
      };
    };

    if ((match = /^INSERT INTO (\w+) VALUES \((.+)\)( RETURNING \*)?$/iu.exec(sql)) !== null) {
      const def = tableDef(match[1] ?? "");
      const slots = (match[2] ?? "").split(",").map((part) => Number(part.trim().slice(1)) - 1);
      const name = def.name;
      return modify(def, match[3] !== undefined, slots.length, "INSERT", (context) => {
        const table = tableOf(context.state, name);
        const row = def.columns.map((column, index) =>
          coerce(column.type, context.params[slots[index] ?? 0] ?? null),
        );
        const key = String(row[def.primaryKey ?? 0]);
        if (table.rows.has(key))
          throw new ExecutionError(
            `duplicate key value violates unique constraint "${name}_pkey"`,
            "MINNOW_UNIQUE_VIOLATION",
            "23505",
          );
        table.rows.set(key, row);
        return { rows: [row], affected: 1, requiresLsn: 0 };
      });
    }

    if (
      (match = /^UPDATE (\w+) SET (\w+) = (\w+) \+ 1 WHERE (\w+) = \$1( RETURNING \*)?$/iu.exec(
        sql,
      )) !== null
    ) {
      const def = tableDef(match[1] ?? "");
      const column = def.columns.findIndex((candidate) => candidate.name === match?.[2]);
      const name = def.name;
      return modify(def, match[5] !== undefined, 1, "UPDATE", (context) => {
        const table = tableOf(context.state, name);
        const key = String(context.params[0]);
        const row = table.rows.get(key);
        if (row === undefined) return { rows: [], affected: 0, requiresLsn: 0 };
        const next = row.map((value, index) => (index === column ? Number(value) + 1 : value));
        table.rows.set(key, next);
        return { rows: [next], affected: 1, requiresLsn: 0 };
      });
    }

    if ((match = /^DELETE FROM (\w+) WHERE (\w+) = \$1$/iu.exec(sql)) !== null) {
      const def = tableDef(match[1] ?? "");
      const name = def.name;
      return modify(def, false, 1, "DELETE", (context) => {
        const deleted = tableOf(context.state, name).rows.delete(String(context.params[0]));
        return { rows: [], affected: deleted ? 1 : 0, requiresLsn: 0 };
      });
    }

    throw new PlanningError(`syntax error at or near "${sql.split(/\s+/u)[0] ?? ""}"`);
  }

  lowerWrite(shape: WriteShape): LoweredModify {
    throw new TypeError(`The stand-in planner has no batch writes (${shape.op} ${shape.table})`);
  }
}
