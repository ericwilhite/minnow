/**
 * Explicit protocol fixtures, so the owner's composition
 * (owner/compose.ts) and protocol server (owner/server.ts) run end to end over **real lowering**
 * (lower/, B6, on main):
 *
 * - `StandinWritePath` implements B3's `WritePath`: one writer slot (FIFO), transactions over a
 *   copied state, savepoints, group commit with a controllable flush, strict/relaxed visibility,
 *   the request ledger (committed outcomes recorded once durable / applied), sequences, and a
 *   durable state through a persistence port (OPFS or IndexedDB in the browser lane). Its catalog is
 *   lowering's `MemoryCatalog`, replayed from the catalog changes lowering produced.
 * - `StandinExecutor` implements B4's `Executor` by interpreting the plans lowering produces, for
 *   the subset the owner's tests use: `values`, `scan`, `keyLookup`, `project`, `filter`, and
 *   `modify` insert/update/delete with RETURNING; comparisons, AND/OR/NOT, IS NULL, casts,
 *   arithmetic with PostgreSQL's errors, `nextval`/`currval`/`lastval`. Anything else is
 *   `UnsupportedPlanError`. Frames go through B4's real `ResultChannel` (window, releases,
 *   cancellation), with the RETURNING spool and the result grant as B4's contract states.
 * - `withTestStatements` adds `SELECT spin($1)`, the latency lane's analytic load.
 *
 * They honour the contracts the composition depends on; they are not models of the real engines
 * (no MVCC beyond copies, no key index, no pages). `composeEngine` defaults to the real executor;
 * tests using this synthetic write path must explicitly inject its matching synthetic executor.
 * `storage-core.ts` exercises the real executor over the real write path and extent storage.
 */
import {
  CatalogChangeError,
  type CatalogChange,
  type CatalogView,
  type TableDef,
} from "../access/catalog.js";
import { bindParameters } from "../exec/parameters.js";
import type { Statement, StatementOptions } from "../access/modify.js";
import type { Snapshot } from "../access/snapshot.js";
import type {
  BeginOptions,
  CommitReceipt,
  CommitRequest,
  PinOptions,
  Savepoint,
  SequenceAccess,
  TableSummary,
  WritePath,
  WritePathStats,
  WriteTransaction,
} from "../access/write-path.js";
import { TransactionStateError } from "../access/write-path.js";
import {
  ExecutionError,
  ParameterBindError,
  QueryCancelledError,
  UnsupportedPlanError,
} from "../exec/errors.js";
import type {
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
import type { OutputColumn, PhysicalPlan, Rel, ScalarExpr } from "../ir/ir.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import type {
  LoweredQuery,
  LoweredStatement,
  PlannerContext,
  StatementPlanner,
} from "../owner/planner.js";
import type { PhysicalType } from "../types.js";
import { decodeUtf8 } from "../vector/varlen.js";
import type { FlatVector, Int64Values } from "../vector/vector.js";

type Value = boolean | number | bigint | string | null;
/** A table row: one value per column in catalog order. */
type Row = readonly Value[];

/**
 * A database state: the catalog as the list of changes that built it (replayed through lowering's
 * `MemoryCatalog`, which assigns ids as the contract says), and each table's rows by row id (the
 * locator). Rows are immutable; an update replaces the row.
 */
interface State {
  readonly changes: CatalogChange[];
  catalog: CatalogView;
  readonly tables: Map<number, Map<number, Row>>;
  nextRowId: number;
}

const emptyState = (): State => ({
  changes: [],
  catalog: new MemoryCatalog().view,
  tables: new Map(),
  nextRowId: 1,
});

const cloneState = (state: State): State => ({
  changes: [...state.changes],
  catalog: state.catalog,
  tables: new Map([...state.tables].map(([id, rows]) => [id, new Map(rows)])),
  nextRowId: state.nextRowId,
});

const catalogOf = (state: State): CatalogView => state.catalog;

/** Applies one catalog change to `state`: replays the catalog with it, keeps table rows in step. */
function applyChange(state: State, change: CatalogChange): { readonly id: number | null } {
  const memory = new MemoryCatalog();
  for (const earlier of state.changes) memory.apply(earlier);
  const result = memory.apply(change);
  state.changes.push(change);
  state.catalog = memory.view;
  if (change.kind === "createTable" && result.id !== null) state.tables.set(result.id, new Map());
  if (change.kind === "dropTable") state.tables.delete(change.tableId);
  return result;
}

/** JSON with bigints and typed arrays (catalog changes and rows carry both). */
const encode = (value: unknown): string =>
  JSON.stringify(value, (_key, item: unknown) => {
    if (typeof item === "bigint") return { $bigint: String(item) };
    if (ArrayBuffer.isView(item) && !(item instanceof DataView))
      return {
        $typed: item.constructor.name,
        values: Array.from(item as unknown as ArrayLike<number | bigint>, String),
      };
    return item;
  });

const decode = (text: string): unknown =>
  JSON.parse(text, (_key, item: unknown) => {
    if (typeof item !== "object" || item === null) return item;
    if ("$bigint" in item) return BigInt((item as { $bigint: string }).$bigint);
    if ("$typed" in item) {
      const { $typed, values } = item as { $typed: string; values: string[] };
      const constructor = (
        globalThis as unknown as Record<string, { from(v: unknown[]): unknown }>
      )[$typed];
      return constructor?.from(
        values.map((value) => ($typed.startsWith("Big") ? BigInt(value) : Number(value))),
      );
    }
    return item;
  });

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
    let result: { readonly id: number | null };
    try {
      result = applyChange(this.#tx.working, change);
    } catch (error) {
      if (error instanceof CatalogChangeError)
        throw new ExecutionError(error.message, "MINNOW_CATALOG_CHANGE", "42P07", { cause: error });
      throw error;
    }
    this.wrote++;
    return result;
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
    this.path.onStatement?.(options, this.catalog);
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

/**
 * Where a stand-in write path keeps its durable state across owners: the owner worker gives it a
 * sync access handle on one OPFS file (the handle is the fence), Node tests a string. `save` runs
 * at every flush with the whole durable state; `load` once at open.
 */
export interface StandinPersistence {
  load(): string | null;
  /** May be asynchronous (IndexedDB): the flush, and so durability, waits for it. */
  save(text: string): void | Promise<void>;
}

interface Saved {
  readonly lsn: number;
  readonly changes: CatalogChange[];
  readonly nextRowId: number;
  readonly tables: ReadonlyArray<readonly [number, ReadonlyArray<readonly [number, Row]>]>;
  readonly sequences: ReadonlyArray<readonly [number, string]>;
  readonly ledger: ReturnType<RequestLedger["checkpoint"]>;
}

export interface StandinWritePathOptions {
  readonly persistence?: StandinPersistence;
  readonly durability?: "strict" | "relaxed";
  /** Flush automatically after each commit (a timer of `flushDelayMs`). Default true. */
  readonly autoFlush?: boolean;
  readonly flushDelayMs?: number;
  /** Tests: while true, scheduled flushes wait (a commit stays applied, not durable). */
  readonly flushHeld?: () => boolean;
  readonly now?: () => number;
}

export class StandinWritePath implements WritePath {
  readonly durability: "strict" | "relaxed";
  /**
   * Called with each statement's options as it opens (tests): the stand-in enforces no rules, so a
   * test calls `options.rules(tableId)` as B3's write path would.
   */
  onStatement: ((options: StatementOptions, catalog: CatalogView) => void) | null = null;
  ledger: RequestLedger;
  readonly #persistence: StandinPersistence | null;
  readonly failure: Error | null = null;
  appliedLsn = 0;
  durableLsn = 0;
  applied: State = emptyState();
  /** The state readers see under strict durability: as of the last flush. */
  #durable: State = cloneState(this.applied);
  readonly #autoFlush: boolean;
  readonly #flushDelayMs: number;
  readonly #flushHeld: () => boolean;
  #flushing: Promise<void> = Promise.resolve();
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
    this.#flushHeld = options.flushHeld ?? (() => false);
    this.#now = options.now ?? Date.now;
    this.#persistence = options.persistence ?? null;
    this.ledger = new RequestLedger();
    const text = this.#persistence?.load() ?? null;
    if (text !== null && text !== "") {
      // Recovery: the durable state, sequences and committed outcomes of the last flush.
      const saved = decode(text) as Saved;
      const restored = emptyState();
      for (const change of saved.changes) applyChange(restored, change);
      for (const [tableId, rows] of saved.tables) restored.tables.set(tableId, new Map(rows));
      restored.nextRowId = saved.nextRowId;
      this.applied = restored;
      this.#durable = this.applied;
      this.appliedLsn = saved.lsn;
      this.durableLsn = saved.lsn;
      for (const [id, value] of saved.sequences) this.#sequenceValues.set(id, BigInt(value));
      this.ledger = RequestLedger.restore(saved.ledger, this.#now());
    }
  }

  #save(lsn: number): void | Promise<void> {
    if (this.#persistence === null) return;
    const saved: Saved = {
      lsn,
      changes: this.applied.changes,
      nextRowId: this.applied.nextRowId,
      tables: [...this.applied.tables].map(([tableId, rows]) => [tableId, [...rows]] as const),
      sequences: [...this.#sequenceValues].map(([id, value]) => [id, String(value)] as const),
      ledger: this.ledger.checkpoint(),
    };
    return this.#persistence.save(encode(saved));
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
      if (this.#flushHeld()) {
        this.#scheduleFlush();
        return;
      }
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
  /**
   * One group flush: everything applied so far is saved, then durable. Committed outcomes are
   * recorded first so the saved ledger holds them. Flushes run one at a time.
   */
  flush(): Promise<void> {
    this.#flushing = this.#flushing.then(() => this.#flushOnce());
    return this.#flushing;
  }

  async #flushOnce(): Promise<void> {
    this.flushes++;
    const lsn = this.appliedLsn;
    const state = this.applied;
    for (const outcome of this.#pendingOutcomes.splice(0))
      this.#record(outcome.lsn, outcome.request);
    const saving = this.#save(lsn);
    if (saving !== undefined) await saving;
    this.durableLsn = Math.max(this.durableLsn, lsn);
    this.#durable = state;
    const ready = this.#durableWaiters.filter((waiter) => waiter.lsn <= this.durableLsn);
    for (const waiter of ready) {
      this.#durableWaiters.splice(this.#durableWaiters.indexOf(waiter), 1);
      waiter.resolve();
    }
  }

  /**
   * The committed table's row count, with no column summaries: the owner's composition tests need
   * a planning summary, not real statistics (those are the write path's, `write/`).
   */
  statistics(tableId: number): TableSummary | null {
    if (this.applied.catalog.table(tableId) === undefined) return null;
    const rowCount = this.applied.tables.get(tableId)?.size ?? 0;
    return { rowCount, column: () => null };
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

  /** Test helper: rows of a table by name, as committed in memory, in insertion order. */
  rows(table: string): Row[] {
    const def = this.applied.catalog.tableByName(table);
    if (def === undefined) return [];
    return [...(this.applied.tables.get(def.id) ?? new Map<number, Row>())]
      .sort(([a], [b]) => a - b)
      .map(([, row]) => row);
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
  /**
   * Busy CPU work of this many ms before the result, in slices of `YIELD_SLICE_MS` with a task
   * yield between them, as the executor's driver yields (ruling R16.4): an analytic load.
   */
  readonly spinMs?: (params: readonly ParameterValue[]) => number;
}

/** The executor yields at least this often (B4 `YIELD_INTERVAL_MS`). */
export const YIELD_SLICE_MS = 4;

/** A task boundary (a message, not a timer: no clamping). */
function yieldTask(): Promise<void> {
  return new Promise((resolve) => {
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {
      channel.port1.close();
      resolve();
    };
    channel.port2.postMessage(null);
  });
}

const programs = new WeakMap<PhysicalPlan, Program>();

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
  if (type.kind === "int64") {
    // Proven safe when every value is (the client reads numbers); exact bigint otherwise.
    const safe = values.every(
      (value) => value === null || Number.isSafeInteger(Number(value as number | bigint)),
    );
    return {
      form: "flat",
      length,
      validity,
      data: {
        layout: "int64",
        values: safe
          ? { repr: "safe", values: Float64Array.from(values, (value) => Number(value ?? 0)) }
          : {
              repr: "int64",
              values: BigInt64Array.from(values, (value) =>
                BigInt((value as number | bigint | null) ?? 0),
              ),
            },
      },
    };
  }
  if (type.kind === "bool")
    return {
      form: "flat",
      length,
      validity,
      data: {
        layout: "bool",
        values: Uint8Array.from(values, (value) => (value === true ? 1 : 0)),
      },
    };
  if (type.kind === "float64")
    return {
      form: "flat",
      length,
      validity,
      data: { layout: "f64", values: Float64Array.from(values, (value) => Number(value ?? 0)) },
    };
  if (type.kind !== "int32")
    throw new UnsupportedPlanError(`${type.kind} results in the stand-in executor`);
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
    const by = this.program.streamBy;
    // Results larger than a frame stream asynchronously under the window, as B4's do.
    const streams = by !== null && result.rows.length > by;
    const summary: ExecutionSummary = {
      rowCount: this.program.kind === "modify" ? result.affected : result.rows.length,
      counts: { inserted: 0, updated: 0, deleted: 0 },
      requiresLsn: this.#requiresLsn,
      profile: profile(!streams),
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
    const spin = this.program.spinMs?.(params) ?? 0;
    if (spin > 0) {
      void this.#spin(spin, summary, result.rows);
      return;
    }
    if (streams) {
      void this.#emit(result.rows, by).then(() => {
        this.#executedWith(summary);
      });
      return;
    }
    void this.#emit(result.rows, null);
    this.#executedWith(summary);
  }

  async #spin(ms: number, summary: ExecutionSummary, rows: Row[]): Promise<void> {
    const end = performance.now() + ms;
    while (performance.now() < end) {
      if (this.#cancelled) return;
      const slice = Math.min(end, performance.now() + YIELD_SLICE_MS);
      while (performance.now() < slice) {
        // busy: an analytic kernel
      }
      await yieldTask();
    }
    void this.#emit(rows, null);
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

export interface StandinExecutorOptions {
  /** Bench-only count from real write-path statistics; null delegates to the test interpreter. */
  readonly countFromStatistics?: (plan: PhysicalPlan) => number | null;
  /** Results of more rows than this stream asynchronously in frames of this many rows. */
  readonly rowsPerFrame?: number;
}

export class StandinExecutor implements Executor {
  prepared = 0;
  readonly #rowsPerFrame: number | null;
  readonly #countFromStatistics: ((plan: PhysicalPlan) => number | null) | undefined;

  constructor(options: StandinExecutorOptions = {}) {
    this.#rowsPerFrame = options.rowsPerFrame ?? null;
    this.#countFromStatistics = options.countFromStatistics;
  }
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
    const statistics = this.#countFromStatistics?.(plan) ?? null;
    const program: Program =
      statistics === null
        ? (programs.get(plan) ?? {
            kind: plan.root.op === "modify" ? "modify" : "query",
            columns: plan.columns,
            streamBy: this.#rowsPerFrame,
            run: (context) =>
              interpret(plan, {
                params: context.params,
                state: context.state,
                sequences: context.sequences,
                sequenceSession: context.sequenceSession,
                sequenceLsn: 0,
              }),
          })
        : {
            kind: "query",
            columns: plan.columns,
            streamBy: null,
            run: () => ({ rows: [[BigInt(statistics)]], affected: 0, requiresLsn: 0 }),
          };
    this.prepared++;
    return {
      plan,
      columns: program.columns,
      kind: program.kind,
      usesSequences: false,
      bind: (values, grant) => bindParameters(plan, values, grant),
      execute: (request) => {
        if (request.parameters.length !== plan.placeholderCount)
          throw new ExecutionError(
            `Expected ${String(plan.placeholderCount)} parameters`,
            "MINNOW_PARAMETER_BIND",
            "08P01",
          );
        const execution = new StandinExecution(program, request, request.parameters, () =>
          this.onAcknowledge?.(),
        );
        this.channels.add(execution.results);
        return execution;
      },
    };
  }
}

// ---- the interpreter: real plans from lowering ----

/** A row as an operator yields it: one value per output column of the relation. */
type Tuple = Value[];

interface Context {
  readonly params: readonly ParameterValue[];
  readonly state: State;
  readonly sequences: SequenceAccess | null;
  readonly sequenceSession: ExecuteRequest["sequenceSession"];
  /** The largest sequence-block LSN drawn from (the delivery rule). */
  sequenceLsn: number;
}

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

function literalValue(vector: FlatVector): Value {
  if (vector.validity !== null && ((vector.validity[0] ?? 0) & 1) === 0) return null;
  const data = vector.data;
  switch (data.layout) {
    case "bool":
      return data.values[0] === 1;
    case "i32":
    case "f64":
    case "f32":
      return data.values[0] ?? null;
    case "int64":
      return data.values.repr === "safe"
        ? BigInt(data.values.values[0] ?? 0)
        : (data.values.values[0] ?? null);
    case "varlen":
      return decodeUtf8(data, 0);
    default:
      throw new UnsupportedPlanError(`a ${data.layout} literal in the stand-in executor`);
  }
}

/** A client value bound to a slot of `type` (exec `ParameterValue` rules, the subset used). */
function bindValue(type: PhysicalType, value: ParameterValue, slot: number): Value {
  if (value === null) return null;
  const fail = (): never => {
    throw new ParameterBindError(`Parameter $${String(slot + 1)} does not fit ${type.kind}`, slot);
  };
  switch (type.kind) {
    case "int32": {
      const number = typeof value === "bigint" ? Number(value) : value;
      if (typeof number !== "number" || !Number.isInteger(number)) return fail();
      if (number < INT32_MIN || number > INT32_MAX) return fail();
      return number;
    }
    case "int64":
      if (typeof value === "bigint") return value;
      if (typeof value === "number" && Number.isSafeInteger(value)) return BigInt(value);
      return fail();
    case "float64":
    case "float32":
      return typeof value === "number" ? value : fail();
    case "bool":
      return typeof value === "boolean" ? value : fail();
    case "text":
      return typeof value === "string" ? value : fail();
    default:
      throw new UnsupportedPlanError(`a ${type.kind} parameter in the stand-in executor`);
  }
}

function checkInt32(value: number): number {
  if (!Number.isInteger(value) || value < INT32_MIN || value > INT32_MAX)
    throw new ExecutionError("integer out of range", "MINNOW_NUMERIC_OUT_OF_RANGE", "22003");
  return value;
}

function arithmetic(fn: string, type: PhysicalType, left: Value, right: Value): Value {
  if (left === null || right === null) return null;
  if (type.kind === "int64") {
    const a = BigInt(left);
    const b = BigInt(right);
    if ((fn === "div" || fn === "mod") && b === 0n)
      throw new ExecutionError("division by zero", "MINNOW_DIVISION_BY_ZERO", "22012");
    return fn === "add"
      ? a + b
      : fn === "sub"
        ? a - b
        : fn === "mul"
          ? a * b
          : fn === "div"
            ? a / b
            : a % b;
  }
  const a = Number(left);
  const b = Number(right);
  if ((fn === "div" || fn === "mod") && b === 0 && type.kind === "int32")
    throw new ExecutionError("division by zero", "MINNOW_DIVISION_BY_ZERO", "22012");
  const result =
    fn === "add"
      ? a + b
      : fn === "sub"
        ? a - b
        : fn === "mul"
          ? a * b
          : fn === "div"
            ? type.kind === "int32"
              ? Math.trunc(a / b)
              : a / b
            : a % b;
  return type.kind === "int32" ? checkInt32(result) : result;
}

function compareValues(left: Value, right: Value): number {
  if (typeof left === "string" && typeof right === "string")
    return left < right ? -1 : left > right ? 1 : 0;
  const a =
    typeof left === "bigint" || typeof right === "bigint" ? BigInt(left as number) : Number(left);
  const b =
    typeof left === "bigint" || typeof right === "bigint" ? BigInt(right as number) : Number(right);
  return a < b ? -1 : a > b ? 1 : 0;
}

function evaluate(expr: ScalarExpr, row: Tuple, context: Context): Value {
  switch (expr.op) {
    case "column":
      return row[expr.index] ?? null;
    case "literal":
      return literalValue(expr.value);
    case "param":
      return bindValue(expr.type, context.params[expr.slot] ?? null, expr.slot);
    case "cast": {
      const value = evaluate(expr.input, row, context);
      if (value === null) return null;
      switch (expr.type.kind) {
        case "int32":
          return checkInt32(Number(value));
        case "int64":
          return BigInt(value);
        case "float64":
        case "float32":
          return Number(value);
        case "text":
          return String(value);
        case "bool":
          return Boolean(value);
        default:
          throw new UnsupportedPlanError(`a cast to ${expr.type.kind} in the stand-in executor`);
      }
    }
    case "compare": {
      const left = evaluate(expr.left, row, context);
      const right = evaluate(expr.right, row, context);
      if (left === null || right === null) return null;
      const order = compareValues(left, right);
      switch (expr.cmp) {
        case "=":
          return order === 0;
        case "<>":
          return order !== 0;
        case "<":
          return order < 0;
        case "<=":
          return order <= 0;
        case ">":
          return order > 0;
        case ">=":
          return order >= 0;
      }
    }
    // falls through (unreachable: every comparison returns)
    case "and":
    case "or": {
      let unknown = false;
      for (const arg of expr.args) {
        const value = evaluate(arg, row, context);
        if (value === null) unknown = true;
        else if (value === (expr.op === "or")) return value;
      }
      return unknown ? null : expr.op === "and";
    }
    case "not": {
      const value = evaluate(expr.arg, row, context);
      return value === null ? null : !value;
    }
    case "isNull": {
      const isNull = evaluate(expr.arg, row, context) === null;
      return expr.negated ? !isNull : isNull;
    }
    case "call":
      return call(
        expr.fn,
        expr.type,
        expr.args.map((arg) => evaluate(arg, row, context)),
        context,
      );
    default:
      throw new UnsupportedPlanError(`${expr.op} in the stand-in executor`);
  }
}

function call(fn: string, type: PhysicalType, args: readonly Value[], context: Context): Value {
  switch (fn) {
    case "add":
    case "sub":
    case "mul":
    case "div":
    case "mod":
      return arithmetic(fn, type, args[0] ?? null, args[1] ?? null);
    case "neg": {
      const value = args[0] ?? null;
      if (value === null) return null;
      return typeof value === "bigint"
        ? -value
        : type.kind === "int32"
          ? checkInt32(-Number(value))
          : -Number(value);
    }
    case "nextval": {
      const id = Number(args[0]);
      const out: Int64Values = { repr: "int64", values: new BigInt64Array(1) };
      const lsn = context.sequences?.next(id, 1, out, 0) ?? 0;
      context.sequenceLsn = Math.max(context.sequenceLsn, lsn);
      const value = out.values[0] ?? 0n;
      context.sequenceSession?.record(id, value);
      return value;
    }
    case "currval":
      return context.sequenceSession?.current(Number(args[0])) ?? null;
    case "lastval":
      return context.sequenceSession?.last() ?? null;
    default:
      throw new UnsupportedPlanError(`the function ${fn} in the stand-in executor`);
  }
}

const columnIndex = (table: TableDef, columnId: number): number => {
  const index = table.columns.findIndex((column) => column.id === columnId);
  if (index < 0) throw new TypeError(`Column ${String(columnId)} is not in table ${table.name}`);
  return index;
};

function tableOf(context: Context, tableId: number): { def: TableDef; rows: Map<number, Row> } {
  const def = context.state.catalog.table(tableId);
  const rows = context.state.tables.get(tableId);
  if (def === undefined || rows === undefined)
    throw new ExecutionError(
      `relation ${String(tableId)} does not exist`,
      "MINNOW_UNDEFINED_TABLE",
      "42P01",
    );
  return { def, rows };
}

/** The rows of a relation (no pipelines: the stand-in materializes). */
function rowsOf(rel: Rel, context: Context): Tuple[] {
  switch (rel.op) {
    case "values":
      return rel.rows.map((exprs) => exprs.map((expr) => evaluate(expr, [], context)));
    case "project":
      return rowsOf(rel.input, context).map((row) =>
        rel.exprs.map((expr) => evaluate(expr, row, context)),
      );
    case "filter":
      return rowsOf(rel.input, context).filter(
        (row) => evaluate(rel.predicate, row, context) === true,
      );
    case "scan": {
      const { def, rows } = tableOf(context, rel.tableId);
      const positions = rel.columns.map((id) => columnIndex(def, id));
      const out: Tuple[] = [];
      for (const [rowId, row] of [...rows].sort(([a], [b]) => a - b)) {
        const tuple: Tuple = positions.map((position) => row[position] ?? null);
        if (rel.filter !== null && evaluate(rel.filter, tuple, context) !== true) continue;
        if (rel.emitLocator) tuple.push(rowId);
        out.push(tuple);
      }
      return out;
    }
    case "keyLookup": {
      const { def, rows } = tableOf(context, rel.tableId);
      const index = def.indexes.find((candidate) => candidate.id === rel.indexId);
      if (index === undefined)
        throw new TypeError(`Index ${String(rel.indexId)} is not on ${def.name}`);
      const keyPositions = index.columnIds.map((id) => columnIndex(def, id));
      const positions = rel.columns.map((id) => columnIndex(def, id));
      const out: Tuple[] = [];
      const seen = new Set<number>();
      for (const keys of rel.keys) {
        const values = keys.map((key) => evaluate(key, [], context));
        if (values.some((value) => value === null)) continue;
        for (const [rowId, row] of rows) {
          if (seen.has(rowId)) continue;
          if (
            !keyPositions.every(
              (position, i) => compareValues(row[position] ?? null, values[i] ?? null) === 0,
            )
          )
            continue;
          seen.add(rowId);
          const tuple: Tuple = positions.map((position) => row[position] ?? null);
          if (rel.emitLocator === true) tuple.push(rowId);
          out.push(tuple);
        }
      }
      return out;
    }
    default:
      throw new UnsupportedPlanError(`${rel.op} in the stand-in executor`);
  }
}

function checkUnique(def: TableDef, rows: Map<number, Row>, row: Row, self: number | null): void {
  for (const index of def.indexes) {
    if (!index.unique) continue;
    const positions = index.columnIds.map((id) => columnIndex(def, id));
    if (positions.some((position) => row[position] === null)) continue;
    for (const [rowId, other] of rows)
      if (
        rowId !== self &&
        positions.every(
          (position) => compareValues(other[position] ?? null, row[position] ?? null) === 0,
        )
      )
        throw new ExecutionError(
          `duplicate key value violates unique constraint "${index.name}"`,
          "MINNOW_UNIQUE_VIOLATION",
          "23505",
        );
  }
  def.columns.forEach((column, position) => {
    if (!column.nullable && row[position] === null)
      throw new ExecutionError(
        `null value in column "${column.name}" violates not-null constraint`,
        "MINNOW_NOT_NULL_VIOLATION",
        "23502",
      );
  });
}

/** Runs a plan: query rows, or a modify's effect and its RETURNING rows. */
function interpret(plan: PhysicalPlan, context: Context): ProgramResult {
  const root = plan.root;
  if (root.op !== "modify")
    return { rows: rowsOf(root, context), affected: 0, requiresLsn: context.sequenceLsn };
  const { def, rows } = tableOf(context, root.tableId);
  const input = rowsOf(root.input, context);
  const returned: Row[] = [];
  const passthrough = root.passthrough ?? 0;
  const returning = (row: Row, extra: readonly Value[]) => {
    if (root.returning === null) return;
    const full: Tuple = [...row, ...extra];
    returned.push(root.returning.map((expr) => evaluate(expr, full, context)));
  };
  let affected = 0;
  for (const tuple of input) {
    const extra = passthrough === 0 ? [] : tuple.slice(tuple.length - passthrough);
    switch (root.kind) {
      case "insert": {
        const row = tuple.slice(0, def.columns.length);
        checkUnique(def, rows, row, null);
        rows.set(context.state.nextRowId++, row);
        returning(row, extra);
        affected++;
        break;
      }
      case "update": {
        const rowId = Number(tuple[0]);
        const old = rows.get(rowId);
        if (old === undefined) break;
        const row = [...old];
        root.assigned.forEach((columnId, i) => {
          row[columnIndex(def, columnId)] = tuple[1 + i] ?? null;
        });
        checkUnique(def, rows, row, rowId);
        rows.set(rowId, row);
        returning(row, extra);
        affected++;
        break;
      }
      case "delete": {
        const rowId = Number(tuple[0]);
        const old = rows.get(rowId);
        if (old === undefined) break;
        rows.delete(rowId);
        returning(old, extra);
        affected++;
        break;
      }
      default:
        throw new UnsupportedPlanError(`${root.kind} in the stand-in executor`);
    }
  }
  return { rows: returned, affected, requiresLsn: context.sequenceLsn };
}

// ---- test statements ----

/** Statements only the tests know, planned by the stand-ins and run by the stand-in executor. */
const SPIN = /^\s*SELECT\s+spin\(\$1\)\s*;?\s*$/iu;

/**
 * Lowering, plus `SELECT spin($1)`: an analytic load of $1 ms in the owner, working in
 * `YIELD_SLICE_MS` slices with a task yield between them, as the executor's driver must (ruling
 * R16.4). The latency lane measures point reads beside it. Everything else is lowering's.
 */
export function withTestStatements(lowerer: StatementPlanner): StatementPlanner {
  return {
    lower(text: string, context: PlannerContext): LoweredStatement {
      if (!SPIN.test(text)) return lowerer.lower(text, context);
      const INT: PhysicalType = { kind: "int32", bits: 32 };
      const columns: OutputColumn[] = [{ name: "spin", type: INT, nullable: false }];
      const physical = {
        root: { id: 0, op: "values", rows: [[]], output: columns },
        columns,
        parameters: [INT],
        placeholderCount: 1,
        clockSlot: null,
        initPlans: [],
        catalogVersion: context.catalog.version,
        volatile: true,
      } as unknown as PhysicalPlan;
      programs.set(physical, {
        kind: "query",
        columns,
        streamBy: null,
        spinMs: (params) => Number(params[0] ?? 0),
        run: () => ({ rows: [[1]], affected: 0, requiresLsn: 0 }),
      });
      return {
        kind: "query",
        text,
        catalogVersion: context.catalog.version,
        command: "SELECT",
        controlsTransaction: false,
        parameters: [INT],
        plan: physical,
        columns: [
          { ...columns[0], sqlType: "integer", render: null } as LoweredQuery["columns"][number],
        ],
        reads: [],
        basis: [],
        pointRead: false,
      };
    },
    lowerWrite: (shape, context) => lowerer.lowerWrite(shape, context),
    lowerTableRules: (table, catalog) => lowerer.lowerTableRules(table, catalog),
  };
}
