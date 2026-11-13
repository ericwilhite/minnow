import { prepareGroup } from "../page/query-roots.js";
/**
 * The `scan` operator's source (ir/ir.ts): one table of a snapshot (access/snapshot.ts), every
 * segment in row-id order, batches of at most `BATCH_ROWS` rows and about `targetBytes`, its
 * `columns` in output order (plus a locator column when `emitLocator`), filtered by `filter`.
 *
 * ## Group segments: prune → pushed predicates → decode the selected positions
 * - **Prune.** The filter's pushable conjuncts (exec/pushdown.ts) are compiled per column chunk;
 *   a group whose chunk statistics rule one out is skipped without reading a page.
 * - **Select.** Per range of `BATCH_ROWS` positions: the live rows (`deletions().liveRows`), then
 *   each pushed conjunct `select`s qualifying positions on encoded pages (over a whole range,
 *   loading only the pages it cannot rule out) or `refine`s them (loading the pages they touch).
 * - **Decode.** At or above `DENSE_SELECTION` of the range every output column decodes densely
 *   (`decodeRange`, the selection kept; the residual then narrows it); below it only the selected
 *   positions (`gather`, a compact batch). There, with a residual (conjuncts not pushed down), its
 *   columns decode first, the residual narrows the candidates, and only then are the other
 *   columns gathered for the survivors.
 * - **Out-of-line values** (page/large-value.ts) are loaded for exactly what is decoded
 *   (`loadValues` for a dense range, `loadValuesOf` for positions) and released when the batch's
 *   borrow ends; pages before the next range are released as the scan moves on.
 * - **Byte-bounded batches.** A batch's decoded bytes (ColumnTarget.outputBytes: validity, values
 *   or codes, scales, limbs, specials, offsets, varlen bytes at full value lengths; plus
 *   locators) are known from page headers before any value is read; a batch that would pass
 *   `targetBytes` decodes a prefix of its positions and resumes with the rest. One row larger
 *   than the target is a batch of its own.
 * - **Reserved working set.** Before decoding, the scan prepays the batch's working set: the
 *   out-of-line values it leases and every target buffer it grows (`ScanMemory`, which both the
 *   targets and the chunk readers charge, draws on that reserve first). A refusal halves the
 *   batch; for one row the scan borrows a progress loan (ruling R18.3, memory/governor.ts rule 5)
 *   for it, and repays it once its buffers and values are back: when that batch's borrow ends,
 *   or at once when the residual rejected the row (before the scan moves on or borrows again).
 * - A column the group predates reads `ColumnDef.missing` (a constant).
 *
 * ## Memory segments
 * Read through `MemorySegmentScan` (exec/memory-scan.ts: one borrowed chunk at a time, zero copy
 * while resident); the whole filter is the residual there.
 *
 * ## Pacing and ending (ruling R23)
 * - Work that emits nothing (ranges, chunks and groups filtered or pruned away) yields with a task
 *   once `yieldIntervalMs` has passed since the worker's task began (R23.5: the one task clock,
 *   stamped by that yield; nothing here starts a slice, not construction nor a `next()`), so a
 *   long run of rejected rows does not hold the owner.
 * - At the end of the table the scan releases everything it holds (readers, values, buffers,
 *   scratch) and repays a loan. `abort` (a cancellation, an error, a LIMIT) does so at once and
 *   never throws; a `next()` suspended on a read then resolves null: whatever settles after the
 *   abort (a reader opened, a loan served) is closed or repaid, and nothing more is read, decoded
 *   or emitted. While the scan's own read still holds loan-backed bytes, the loan is repaid when
 *   that read settles and the step unwinds. `close` is `abort`, and reports a loan that another
 *   operator's bytes still hold.
 *
 * ## Dedicated terminal output (opt-in)
 * Complete unfiltered groups without locators, deletions or missing columns may decode directly
 * into tight, fresh buffers. Text stays UTF-8 bytes or dictionary codes; dictionary lifetimes
 * and charges extend through retained output, including after source end/abort. SourceItem.transfer
 * marks only these buffers detachable; pages,
 * dictionaries and pooled slabs are never marked. Charges survive detachment until release.
 * Four batch credits and a four-MiB threshold (plus the current batch) bound retained output;
 * a loan-backed output must be released before the next step. Memory segments keep borrowing.
 *
 * ## Borrowing, sync-when-cached
 * A batch's vectors are the scan's own buffers (or a memory segment's lent chunk), valid until the
 * next `next()`: the pipeline asks again only after the batch's borrow ended. Every access call is
 * `MaybePromise`; `next()` returns a value when everything it needed was resident (the generator
 * below runs synchronously until a call actually returns a Promise).
 */
import type { TaskWork } from "../task.js";
import type { MaybePromise } from "../access/key-index.js";
import type { ColumnDef } from "../access/catalog.js";
import {
  MEMORY_CHUNK_ROWS,
  type ChunkOutput,
  type DeletionVector,
  type GroupSegment,
  type MemorySegment,
  type Segment,
  type TableSnapshot,
} from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { Loan } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { LOCATOR_POSITIONS } from "../vector/locator.js";
import { allocateSelection } from "../vector/validity.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  allocateFlat,
  dataBytes,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../vector/vector.js";
import {
  ColumnTarget,
  type DecodeSpec,
  type TargetMemory,
  type OwnedColumn,
} from "./column-target.js";
import { Predicate } from "./filter.js";
import type { CompileContext } from "./kernels/compile.js";
import { MemorySegmentScan } from "./memory-scan.js";
import {
  YIELD_INTERVAL_MS,
  isPromise,
  afterYield,
  clockFor,
  type TaskClock,
  yieldTask,
  type BatchSource,
  type SourceItem,
  type Transfer,
} from "./pipeline.js";
import {
  CompiledFallback,
  pushedColumn,
  splitFilter,
  loadProfiled,
  type PushedColumn,
  type PushedPredicate,
} from "./pushdown.js";
import type { ChunkPages, ChunkReads } from "./scan.js";

/** Fixed staging needed when a scan reaches a resident or re-pointed memory segment.
 * The executor reserves this before keeping other breaker buffers resident. */
export function memoryScanBufferBytes(types: readonly PhysicalType[]): number {
  return (
    types.reduce((sum, type) => sum + dataBytes(type, BATCH_ROWS), 0) +
    types.length * BATCH_ROWS * Int32Array.BYTES_PER_ELEMENT +
    MEMORY_CHUNK_ROWS * Uint16Array.BYTES_PER_ELEMENT
  );
}

/** `xs[i]`, which must exist. */
function at<T>(xs: readonly T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new RangeError(`no element ${String(i)}`);
  return x;
}

/**
 * Byte splitting can leave a compact selection that is still one contiguous range. Preserve
 * that range through sizing, loading and decoding instead of walking its rows per column.
 * Check every position: endpoints alone would accept a selection with interior gaps/duplicates.
 */
function specForRows(rows: Uint16Array, count: number): DecodeSpec {
  const from = rows[0] ?? 0;
  for (let k = 1; k < count; k++) if (rows[k] !== from + k) return { rows, count };
  return count === 0 ? { rows, count } : { from, to: from + count };
}

/** At or above this share of a range's positions selected, decode the range densely (B2's 40 %). */
export const DENSE_SELECTION = 0.4;

/** What the scan counts (diagnostics, tests, benches). */
export interface ScanStats {
  /** Encoded data pages first opened by a chunk reader (including cache hits). */
  pagesRead: number;
  /** Pages directly ruled out by whole-group or encoded page bounds, once per predicate. */
  pagesPruned: number;
  groupsPruned: number;
  groupsRead: number;
  denseBatches: number;
  gatherBatches: number;
  /** Batches a residual narrowed (or emptied). */
  residualBatches: number;
  memoryBatches: number;
  /** Batches cut short by the byte target. */
  splits: number;
  loans: number;
  /** Pushed conjuncts per chunk evaluated on encoded pages (B2), and on decoded values. */
  encodedPredicates: number;
  decodedPredicates: number;
  /** Yields requested directly by this scan; reader-window task yields are not counted here. */
  yields: number;
}

export interface TableScanOptions {
  /** Fresh detachable outputs for terminal unfiltered groups without locators/deletions/missing columns.
   * true flattens dictionaries to UTF-8; "dictionary" preserves codes and retains dictionary charges.
   * At most four batches / four target MiB (plus one oversized row) may be retained.
   * Other sources/plans retain their ordinary borrowed contract.
   */
  readonly transferOutputs?: boolean | "dictionary";
  /** Terminal ownership: allocate dedicated buffers in this result budget from creation.
   * Without it, detachable output stays charged to the scan budget. Refusal shrinks a batch;
   * an output budget refusing even one row is reported (no second grant's progress loan),
   * unless the terminal caller permits ordinary borrowed output with borrowOutputOnRefusal.
   */
  readonly outputBudget?: ByteBudget;
  readonly outputBuffers?: {
    take(bytes: number): ArrayBuffer | null;
    outputSize?(bytes: number): number;
  };
  /** Owning terminal consumer resolves each dictionary's wire form before sizing output. */
  readonly outputDictionary?: (dictionary: FlatVector) => boolean;
  /** Terminal transfer optimization may use ordinary borrowed decode on one-row refusal. */
  readonly borrowOutputOnRefusal?: boolean;
  /** Full unfiltered scans: one-pass coalesced row windows when the reader factory supports it.
   * Opt-in stored-byte target; absent/zero keeps cached per-batch reads.
   * Filtered/deleted groups stay selective. Refusal shrinks the window, then falls back.
   */
  readonly readWindowBytes?: number;
  readonly table: TableSnapshot;
  readonly pages: SpaceReader;
  readonly reads: ChunkReads;
  /** Page leases, dictionaries and decode buffers are charged here (the scan's grant). */
  readonly budget: ByteBudget;
  /** Catalog column ids, in output order. */
  readonly columns: readonly number[];
  /** Over the output columns; null: every live row. */
  readonly filter: ScalarExpr | null;
  readonly emitLocator: boolean;
  /** Kernels compile from here (its arena, parameter slots); `input` is ignored. */
  readonly context: CompileContext;
  /** The pipeline grant's progress loans (R18.3); without them a refused single row fails. */
  readonly loans?: { borrow(bytes: number): Promise<Loan> };
  /**
   * False: pushed conjuncts run on decoded values (the fallback) even where the page format
   * pushes them down (tests compare the two). Default true.
   */
  readonly encodedPushdown?: boolean;
  /** False: no pushdown, dense decode, the whole filter on decoded values (decode-then-filter). */
  readonly pushdown?: boolean;
  readonly targetBytes?: number;
  /** Executor admission includes B3's complete publication-stable memory step. */
  readonly boundedMemory?: boolean;
  /** A streaming consumer's remaining demand, including OFFSET. Bounds emitted rows and
   * payload decode; predicates/deletions inspect vectorized ranges first. Zero ends the source.
   * Update between batches. A breaker or separate filter starts a new domain. Default: full batches. */
  readonly rowsNeeded?: () => number;
  /** Synchronous work between the scan's task yields (default `YIELD_INTERVAL_MS`). */
  readonly yieldIntervalMs?: number;
  /** The clock in ms (default `performance.now`); injectable for tests. */
  readonly now?: () => number;
}

// ── Sync-when-possible steps ────────────────────────────────────────────────────────────────

type Step<T> = Generator<Promise<unknown>, T, unknown>;

/** Thrown into a step resumed after `abort`: it unwinds (closing what it opened) to `next`. */
class ScanAborted extends Error {}
const ABORTED = new ScanAborted("The scan was aborted");

// ── Group segments ──────────────────────────────────────────────────────────────────────────

/** One output column of an open group: its reader and target, or the group predates it. */
interface GroupColumn {
  ownedDictionary?: boolean;
  readonly pages: ChunkPages | null;
  /** The group predates the column: every row is this value (`ColumnDef.missing` or NULL). */
  readonly missing: FlatVector | null;
}

/** An open group segment and where the scan is in it. */
interface OpenGroup {
  owned: boolean;
  retained: number;
  retired: boolean;
  windowRows: number;
  windowEnd: number;
  readonly releasedThrough: number[];
  readonly segment: GroupSegment;
  readonly columns: readonly GroupColumn[];
  readonly pushed: readonly PushedColumn[];
  readonly deletions: DeletionVector | null;
  /** Next range start. */
  from: number;
  /** Positions (in the scan's `#pending`) still to emit from the current range. */
  pendingCount: number;
  pendingFrom: number;
  pendingTo: number;
  /** The pending positions are the whole selection of their range (nothing emitted yet). */
  fresh: boolean;
}

/**
 * Memory charged through the scan's budget: its own buffers (`take`) and, as the `ByteBudget` its
 * chunk readers charge, their page and value leases. `reserve` holds bytes taken ahead of a
 * decode; both kinds of charge draw on it first, so one prepayment covers a decode's whole
 * working set (values leased and the buffers decoded into).
 */
class ScanMemory implements TargetMemory, ByteBudget {
  reserve = 0;
  constructor(
    readonly budget: ByteBudget,
    readonly buffers?: {
      take(bytes: number): ArrayBuffer | null;
      outputSize?(bytes: number): number;
    },
  ) {}
  outputSize = (bytes: number): number => this.buffers?.outputSize?.(bytes) ?? bytes;
  reuse(bytes: number): ArrayBuffer | null {
    return this.buffers?.take(bytes) ?? null;
  }
  allocate(bytes: number): boolean {
    if (bytes <= this.reserve) {
      this.reserve -= bytes;
      return true;
    }
    if (!this.budget.allocate(bytes - this.reserve)) return false;
    this.reserve = 0;
    return true;
  }
  free(bytes: number): void {
    this.budget.free(bytes);
  }
  take(bytes: number): void {
    if (!this.allocate(bytes))
      throw new RangeError(`A ${String(bytes)}-byte scan buffer was refused by its grant`);
  }
  give(bytes: number): void {
    this.free(bytes);
  }
  /** Takes `bytes` ahead of a decode; false when the budget refuses. */
  prepay(bytes: number): boolean {
    if (bytes <= 0) return true;
    if (!this.budget.allocate(bytes)) return false;
    this.reserve += bytes;
    return true;
  }
  /** Returns an unused reserve. */
  settle(): void {
    if (this.reserve > 0) this.budget.free(this.reserve);
    this.reserve = 0;
  }
}

/** The scan's fixed scratch: position lists, selections and (with `emitLocator`) locators. */
function scratchBytes(emitLocator: boolean): number {
  return 5 * BATCH_ROWS * Uint16Array.BYTES_PER_ELEMENT + (emitLocator ? BATCH_ROWS * 8 : 0);
}

export class TableScan implements BatchSource {
  readonly stats: ScanStats = {
    pagesRead: 0,
    pagesPruned: 0,
    groupsPruned: 0,
    groupsRead: 0,
    denseBatches: 0,
    gatherBatches: 0,
    residualBatches: 0,
    memoryBatches: 0,
    splits: 0,
    loans: 0,
    encodedPredicates: 0,
    decodedPredicates: 0,
    yields: 0,
  };
  readonly #options: TableScanOptions;
  readonly #transferOutputs: boolean | "dictionary";
  #ownedBytes = 0;
  #ownedCount = 0;
  readonly #ownedWaiters = new Set<() => void>();
  readonly #defs: readonly ColumnDef[];
  readonly #types: readonly PhysicalType[];
  readonly #pushed: readonly PushedPredicate[];
  readonly #fallbacks: readonly CompiledFallback[];
  readonly #residual: Predicate | null;
  /** Output columns the residual reads (decoded first on the gather path). */
  readonly #residualColumns: ReadonlySet<number>;
  readonly #all: readonly number[];
  readonly #memory: ScanMemory;
  readonly #ownedMemory: ScanMemory;
  readonly #targets: ColumnTarget[];
  readonly #scratchBytes: number;
  readonly #positions: Uint16Array;
  readonly #survivors: Uint16Array;
  /** The open group's positions still to emit (one group is open at a time). */
  readonly #pending: Uint16Array;
  readonly #selection: Selection;
  readonly #live: Selection;
  readonly #locators: Float64Array;
  readonly #locatorType: PhysicalType;
  readonly #placeholder: Vector;
  readonly #target: number;
  readonly #interval: number;
  /** The worker's task clock, or a test's (R23.5): no slice start of its own. */
  readonly #clock: TaskClock;
  readonly #readTask: TaskWork | undefined;
  #segment = 0;
  #group: OpenGroup | null = null;
  #memoryScan: MemorySegmentScan | null = null;
  /** Memory segments' staging (re-pointed chunks decode into it) and selection, charged. */
  #memoryOut: { out: ChunkOutput[]; selection: Selection; bytes: number } | null = null;
  #loan: Loan | null = null;
  readonly #readAbort = new AbortController();
  #aborted = false;
  #released = false;
  /** A step is suspended on a Promise (an access in flight). */
  #suspended = false;

  constructor(options: TableScanOptions) {
    this.#options = options;
    this.#transferOutputs =
      (options.transferOutputs === true || options.transferOutputs === "dictionary") &&
      options.filter === null &&
      !options.emitLocator
        ? options.transferOutputs
        : false;
    const table = options.table;
    this.#defs = options.columns.map((id) => {
      const def = table.columns.find((column) => column.id === id);
      if (def === undefined) throw new RangeError(`Column ${String(id)} is not in the table`);
      return def;
    });
    this.#types = this.#defs.map((def) => def.type);
    this.#all = this.#defs.map((_, c) => c);
    const context: CompileContext = { ...options.context, input: this.#types };
    const split =
      options.pushdown === false
        ? { pushed: [], residual: options.filter }
        : splitFilter(options.filter, this.#types);
    this.#pushed = split.pushed;
    this.#fallbacks = split.pushed.map(
      (pushed) => new CompiledFallback(pushed, at(this.#types, pushed.column), context),
    );
    this.#residual = split.residual === null ? null : new Predicate(split.residual, context);
    this.#residualColumns = new Set(split.residual === null ? [] : columnsOf(split.residual));
    this.#memory = new ScanMemory(options.budget);
    this.#ownedMemory =
      options.outputBudget === undefined || options.outputBudget === options.budget
        ? this.#memory
        : new ScanMemory(options.outputBudget, options.outputBuffers);
    this.#targets = this.#types.map((type) => new ColumnTarget(type, BATCH_ROWS, this.#memory));
    // The scratch is charged before it is built (a refusal throws, holding nothing).
    const scratch = scratchBytes(options.emitLocator);
    this.#memory.take(scratch);
    try {
      this.#positions = new Uint16Array(BATCH_ROWS);
      this.#survivors = new Uint16Array(BATCH_ROWS);
      this.#pending = new Uint16Array(BATCH_ROWS);
      this.#selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
      this.#live = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
      this.#locators = new Float64Array(options.emitLocator ? BATCH_ROWS : 0);
    } catch (error) {
      this.#memory.give(scratch);
      throw error;
    }
    this.#scratchBytes = scratch;
    this.#locatorType = { kind: "locator", tableId: table.tableId };
    this.#placeholder = {
      form: "constant",
      type: { kind: "bool" },
      length: 0,
      value: allocateFlat({ kind: "bool" }, 1),
    };
    this.#target = options.targetBytes ?? BATCH_TARGET_BYTES;
    this.#interval = options.yieldIntervalMs ?? YIELD_INTERVAL_MS;
    this.#clock = clockFor(options.now);
    this.#readTask = Number.isFinite(this.#interval)
      ? { clock: this.#clock, signal: this.#readAbort.signal }
      : undefined;
  }

  /** Charged allocation components; these are not physical RSS attribution. */
  get memoryStats(): {
    reusedDecodeBytes: number;
    ownedOutputBytes: number;
    ownedOutputInQueryBytes: number;
    outputReserveBytes: number;
    scratchBytes: number;
    reserveBytes: number;
  } {
    return {
      reusedDecodeBytes: this.#targets.reduce((n, t) => n + t.chargedBytes, 0),
      ownedOutputBytes: this.#ownedBytes,
      ownedOutputInQueryBytes: this.#ownedMemory === this.#memory ? this.#ownedBytes : 0,
      outputReserveBytes: this.#ownedMemory === this.#memory ? 0 : this.#ownedMemory.reserve,
      scratchBytes: this.#released ? 0 : this.#scratchBytes,
      reserveBytes: this.#memory.reserve,
    };
  }

  next(): MaybePromise<SourceItem | null> {
    // No slice starts here (R23.4, R23.5): a caller resuming at once (a resolved Promise, a
    // microtask) is no task yield; only a yield begins the worker's task.
    return this.#run(this.#next());
  }

  /**
   * Runs a step generator, synchronously until it yields a Promise. After `abort`, a resumed step
   * is thrown `ABORTED` (a rejection, often the abort's own doing, too), so it unwinds through its
   * cleanup and `next` ends with null.
   */
  #run(
    step: Step<SourceItem | null>,
    input?: unknown,
    failure?: { error: unknown },
  ): MaybePromise<SourceItem | null> {
    let result: IteratorResult<Promise<unknown>, SourceItem | null>;
    this.#suspended = false;
    try {
      if (failure !== undefined) result = step.throw(this.#aborted ? ABORTED : failure.error);
      else result = step.next(input);
    } catch (error) {
      if (error !== ABORTED) throw error;
      this.#settleAborted();
      return null;
    }
    if (result.done) {
      if (this.#aborted) this.#settleAborted();
      return result.value;
    }
    this.#suspended = true;
    return result.value.then(
      (value) => this.#run(step, value),
      (error: unknown) => this.#run(step, undefined, { error }),
    );
  }

  /**
   * Waits for `value` only when it is a Promise; resumed after `abort`, disposes of what it
   * resolved to (a reader opened, a loan served) and unwinds.
   */
  *#wait<T>(value: MaybePromise<T>, dispose?: (value: T) => void): Step<T> {
    if (!isPromise(value)) return value;
    const resolved = (yield value) as T;
    if (this.#aborted) {
      dispose?.(resolved);
      throw ABORTED;
    }
    return resolved;
  }

  /**
   * Scan progress that emits nothing (ranges, chunks and groups filtered or pruned away) yields
   * with a task once `yieldIntervalMs` has passed since the task began (rulings R23, R23.5: the
   * worker's task clock, stamped by the yield itself), so a long run of rejected rows does not
   * hold the owner for the whole table. Nothing here starts a slice: not construction, not a
   * `next()`, which a caller may make again at once, in a microtask.
   */
  *#pace(): Step<void> {
    if (!this.#clock.due(this.#interval)) return;
    this.stats.yields += 1;
    yield* this.#wait(yieldTask());
    afterYield(this.#clock);
  }

  #rowsNeeded(): number {
    const rows = this.#options.rowsNeeded?.() ?? BATCH_ROWS;
    if (rows !== Infinity && (!Number.isSafeInteger(rows) || rows < 0))
      throw new RangeError("nonnegative scan row demand required");
    return Math.min(BATCH_ROWS, rows);
  }

  *#next(): Step<SourceItem | null> {
    this.#endBorrow();
    while (
      !this.#aborted &&
      (this.#ownedCount >= 4 ||
        this.#ownedBytes >= 4 * BATCH_TARGET_BYTES ||
        (this.#loan !== null && this.#ownedCount > 0))
    ) {
      yield* this.#wait(
        new Promise<void>((resolve) => {
          this.#ownedWaiters.add(resolve);
        }),
      );
      this.#endBorrow();
    }
    for (;;) {
      if (this.#aborted) return null;
      if (this.#rowsNeeded() === 0) {
        this.abort();
        return null;
      }
      const segment: Segment | undefined = this.#options.table.segments[this.#segment];
      if (segment === undefined) {
        // The end: the last batch's borrow has ended, so everything goes back now.
        this.#release();
        return null;
      }
      // An empty memory tail cannot decode anything. Do not allocate full chunk staging
      // just to discover its end, especially after a stored scan or for a wide empty table.
      if (segment.kind === "memory" && segment.rowCount === 0) {
        this.#segment += 1;
        yield* this.#pace();
        continue;
      }
      const item =
        segment.kind === "group"
          ? yield* this.#fromGroup(segment)
          : yield* this.#fromMemory(segment);
      if (item !== null) return item;
      this.#closeSegment();
      this.#segment += 1;
      yield* this.#pace();
    }
  }

  /**
   * The previous batch's borrow ended, or a candidate the residual rejected is done: release its
   * values, and repay a loan it needed (its buffers go back first) before the scan moves on or
   * borrows again.
   */
  #endBorrow(): void {
    const group = this.#group;
    if (group !== null) for (const column of group.columns) column.pages?.releaseValues();
    this.#memory.settle();
    if (this.#loan !== null) {
      // The oversized buffers go back first, so the loan's bytes are free to repay.
      for (const target of this.#targets) target.shrink();
      try {
        this.#repay();
      } catch {
        // Still allocated elsewhere in the grant: kept, and retried (next batch, `close`).
      }
    }
  }

  /**
   * After an abort, once no step is suspended: a loan whose repayment the abort could not make
   * (its own in-flight read still held loan-backed bytes, given back when that read settled) is
   * repaid now. Still refused: kept for `close`.
   */
  #settleAborted(): void {
    if (this.#loan === null) return;
    try {
      this.#repay();
    } catch {
      // Held elsewhere in the grant: `close` retries and reports it.
    }
  }

  /** Repays an outstanding loan (Loan: throws, changing nothing, while its bytes are in use). */
  #repay(): void {
    const loan = this.#loan;
    if (loan === null) return;
    loan.repay();
    this.#loan = null;
  }

  // ── Memory segments ──

  #memoryBuffers(): { out: ChunkOutput[]; selection: Selection } {
    if (this.#memoryOut !== null) return this.#memoryOut;
    const bytes = memoryScanBufferBytes(this.#types);
    this.#memory.take(bytes);
    try {
      this.#memoryOut = {
        out: this.#types.map((type) => ({
          flat: allocateFlat(type, BATCH_ROWS),
          codes: new Int32Array(BATCH_ROWS),
          varlen: null,
        })),
        selection: allocateSelection(MEMORY_CHUNK_ROWS),
        bytes,
      };
    } catch (error) {
      this.#memory.give(bytes);
      throw error;
    }
    return this.#memoryOut;
  }

  *#fromMemory(segment: MemorySegment): Step<SourceItem | null> {
    if (this.#memoryScan === null) {
      const { out, selection } = this.#memoryBuffers();
      this.#memoryScan = new MemorySegmentScan(
        [segment],
        this.#options.columns,
        out,
        selection,
        this.#options.filter === null
          ? undefined
          : (batch) => this.#memoryPredicate().select(batch),
        this.#options.boundedMemory === true
          ? {
              context: {
                pages: this.#options.pages,
                budget: this.#memory,
                signal: this.#readAbort.signal,
              },
              targetBytes: this.#target,
            }
          : undefined,
      );
    }
    const scan = this.#memoryScan;
    for (;;) {
      const needed = this.#rowsNeeded();
      if (needed === 0) {
        this.abort();
        return null;
      }
      const item = yield* this.#wait(scan.next(needed));
      if (item === null) return null;
      let batch = item.batch;
      if (this.#options.filter !== null && batch.selection?.count === 0) {
        yield* this.#pace();
        continue;
      }
      this.stats.memoryBatches += 1;
      if (this.#options.emitLocator) {
        const base = scan.lastSegment * LOCATOR_POSITIONS + scan.lastStart;
        for (let k = 0; k < batch.length; k++) this.#locators[k] = base + k;
        batch = { ...batch, columns: [...batch.columns, this.#locatorVector(batch.length)] };
      }
      return { batch };
    }
  }

  #memoryFilter: Predicate | null = null;
  /** Memory segments evaluate the whole filter on their (lent) vectors. */
  #memoryPredicate(): Predicate {
    const filter = this.#options.filter;
    if (filter === null) throw new RangeError("No filter to evaluate");
    this.#memoryFilter ??= new Predicate(filter, {
      ...this.#options.context,
      input: this.#types,
    });
    return this.#memoryFilter;
  }

  #locatorVector(length: number): FlatVector {
    return {
      form: "flat",
      type: this.#locatorType,
      length,
      validity: null,
      data: { layout: "f64", values: this.#locators },
    };
  }

  // ── Group segments ──

  *#open(segment: GroupSegment): Step<OpenGroup | null> {
    yield* this.#wait(prepareGroup(segment, this.#options.columns), () => segment.releaseRoot?.());
    const columns: GroupColumn[] = [];
    const opened: ChunkPages[] = [];
    const windowBytes =
      this.#options.filter === null && this.#options.reads.loadTogether
        ? (this.#options.readWindowBytes ?? 0)
        : 0;
    let storedBytes = 0;
    try {
      for (let c = 0; c < this.#defs.length; c++) {
        const def = at(this.#defs, c);
        const chunk = segment.chunk(def.id);
        if (chunk === null) {
          columns.push({ pages: null, missing: def.missing ?? nullOf(def.type) });
          continue;
        }
        storedBytes += chunk.data.length;
        // Its page and value leases are charged through the scan's memory (see `ScanMemory`).
        const pages = yield* this.#wait(
          this.#options.reads.open(this.#options.pages, chunk, {
            budget: this.#memory,
            signal: this.#readAbort.signal,
            ...(windowBytes > 0
              ? { once: true, ...(this.#readTask ? { task: this.#readTask } : {}) }
              : {}),
          }),
          (late) => {
            late.close();
          },
        );
        opened.push(pages);
        columns.push({ pages, missing: null });
      }
      // Prune: a pushed conjunct the chunk's statistics rule out skips the group unread.
      const pushed: PushedColumn[] = [];
      for (let p = 0; p < this.#pushed.length; p++) {
        const predicate = at(this.#pushed, p);
        const column = at(columns, predicate.column);
        if (column.pages === null) {
          // A missing column is one constant: decide the conjunct once.
          const value = column.missing ?? nullOf(at(this.#types, predicate.column));
          if (!this.#missingMatches(p, { form: "constant", type: value.type, length: 1, value })) {
            for (const pages of opened) {
              this.stats.pagesPruned += pages.pagesInRange(0, pages.rowCount)[1];
              pages.close();
            }
            this.stats.groupsPruned += 1;
            segment.releaseRoot?.();
            return null;
          }
          pushed.push(ALWAYS);
          continue;
        }
        const chunk = segment.chunk(this.#defs[predicate.column]?.id ?? -1);
        if (chunk === null) throw new RangeError("Chunk vanished");
        const compiled = pushedColumn(
          column.pages,
          chunk,
          predicate,
          at(this.#fallbacks, p),
          this.#options.encodedPushdown ?? true,
          this.stats,
        );
        // Statistics count the COMPLETE stored group, including an invisible suffix of
        // a re-pointed memory snapshot. Its visible rowCount is only the scan boundary.
        // Synthetic readers may supply only the legacy visible count.
        const stored: { readonly rowCount?: number } = segment.group;
        if (!compiled.mayMatch(stored.rowCount ?? segment.rowCount, chunk.statistics)) {
          for (const pages of opened) {
            this.stats.pagesPruned += pages.pagesInRange(0, pages.rowCount)[1];
            pages.close();
          }
          this.stats.groupsPruned += 1;
          segment.releaseRoot?.();
          return null;
        }
        if (compiled.encoded) this.stats.encodedPredicates += 1;
        else this.stats.decodedPredicates += 1;
        pushed.push(compiled);
      }
      const deletions = yield* this.#wait(segment.deletions());
      this.stats.groupsRead += 1;
      return {
        owned:
          this.#transferOutputs !== false &&
          deletions === null &&
          columns.every((c) => c.pages !== null),
        retained: 0,
        retired: false,
        windowRows:
          windowBytes > 0 && deletions === null
            ? Math.min(
                segment.rowCount,
                Math.max(
                  1,
                  Math.floor(
                    (windowBytes * segment.rowCount) / Math.max(1, storedBytes) / BATCH_ROWS,
                  ),
                ) * BATCH_ROWS,
              )
            : 0,
        windowEnd: 0,
        releasedThrough: columns.map(() => 0),
        segment,
        columns,
        pushed,
        deletions,
        from: 0,
        pendingCount: 0,
        pendingFrom: 0,
        pendingTo: 0,
        fresh: false,
      };
    } catch (error) {
      for (const pages of opened) pages.close();
      segment.releaseRoot?.();
      throw error;
    }
  }

  /** Whether pushed conjunct `p` holds for a missing column's constant. */
  #missingMatches(p: number, value: Vector): boolean {
    const hits = at(this.#fallbacks, p).evaluate(value, 1);
    return hits.count > 0;
  }

  *#fromGroup(segment: GroupSegment): Step<SourceItem | null> {
    if (this.#group?.segment !== segment) {
      const group = yield* this.#open(segment);
      if (group === null) return null;
      this.#group = group;
    }
    const group = this.#group;
    for (;;) {
      if (this.#aborted) return null;
      if (this.#rowsNeeded() === 0) {
        this.abort();
        return null;
      }
      if (group.pendingCount > 0) {
        const item = yield* this.#emitPositions(group, group.pendingFrom, group.pendingTo);
        if (item !== null) return item;
        // Rejected by the residual: its working set ends here (no borrow will end it).
        this.#endBorrow();
        yield* this.#pace();
        continue;
      }
      if (group.from >= segment.rowCount) return null;
      const from = group.from;
      // Vectorize predicates independently of LIMIT selectivity. Bound survivors below,
      // rather than repeatedly opening one-row intervals for a sparse predicate.
      const rangeRows =
        this.#options.filter === null && group.deletions === null ? this.#rowsNeeded() : BATCH_ROWS;
      const to = Math.min(from + rangeRows, segment.rowCount);
      group.from = to;
      this.#releaseBefore(group, from);
      yield* this.#prefetch(group, from);
      // Live rows, then each pushed conjunct, as group positions.
      const positions = this.#positions;
      let count: number;
      let all = false;
      if (group.deletions !== null) {
        group.deletions.liveRows(from, to - from, null, this.#live);
        count = this.#live.count;
        for (let k = 0; k < count; k++) positions[k] = from + (this.#live.rows[k] ?? 0);
      } else {
        count = to - from;
        all = true;
      }
      for (const pushed of group.pushed) {
        if (count === 0) break;
        if (pushed === ALWAYS) continue;
        if (all) {
          yield* this.#wait(pushed.prepare(from, to));
          count = pushed.select(from, to, positions);
          all = false;
        } else {
          yield* this.#wait(pushed.prepareRows(positions, count));
          count = pushed.refine(positions, count);
        }
      }
      if (count > 0) {
        if (all) for (let k = 0; k < count; k++) positions[k] = from + k;
        this.#pending.set(positions.subarray(0, count));
        group.pendingCount = count;
        group.pendingFrom = from;
        group.pendingTo = to;
        group.fresh = true;
        const item = yield* this.#emitPositions(group, from, to);
        if (item !== null) return item;
        // Rejected by the residual: its working set ends here (no borrow will end it).
        this.#endBorrow();
      }
      yield* this.#pace();
    }
  }

  /** Plan once per window, then lease coalesced one-pass slabs across its columns. */
  *#prefetch(group: OpenGroup, from: number): Step<void> {
    if (group.windowRows === 0 || from < group.windowEnd) return;
    for (;;) {
      const end = Math.min(group.segment.rowCount, from + group.windowRows);
      const loads: Array<{ reader: ChunkPages; pages: number[] }> = [];
      for (const column of group.columns) {
        const reader = column.pages;
        if (reader === null) continue;
        const [first, stop] = reader.pagesInRange(from, end);
        const pages = [];
        for (let p = first; p < stop; p++) if (!reader.isLoaded(p)) pages.push(p);
        if (pages.length > 0) loads.push({ reader, pages });
      }
      try {
        if (loads.length > 0)
          yield* this.#wait(this.#options.reads.loadTogether?.(loads), () => {
            if (group.retired && group.retained > 0)
              for (const load of loads) load.reader.release();
          });
        group.windowEnd = end;
        return;
      } catch (error) {
        // Prefetch is optional. A refused window gives back partial loads and shrinks; the
        // ordinary batch path remains the fallback. Corruption/IO/abort errors still propagate.
        if (!(
          error instanceof Error &&
          "code" in error &&
          (error.code === "MINNOW_PAGE_BUDGET" || error.code === "MINNOW_READ_DENIED")
        ))
          throw error;
        for (const column of group.columns) column.pages?.release();
        group.releasedThrough.fill(0);
        if (group.windowRows <= BATCH_ROWS) {
          group.windowRows = 0;
          return;
        }
        group.windowRows = Math.max(
          BATCH_ROWS,
          Math.floor(group.windowRows / 2 / BATCH_ROWS) * BATCH_ROWS,
        );
      }
    }
  }

  /** Pages wholly before `from` are no longer needed. */
  #releaseBefore(group: OpenGroup, from: number): void {
    if (from === 0) return;
    for (const [c, column] of group.columns.entries()) {
      const pages = column.pages;
      if (pages === null) continue;
      const [first] = pages.pagesInRange(from, from + 1);
      const held = group.releasedThrough[c] ?? 0;
      if (first > held) pages.release(Array.from({ length: first - held }, (_, k) => held + k));
      group.releasedThrough[c] = first;
    }
  }

  /**
   * Bytes of the batch decoding `spec` produces: every output column as decoded (validity, codes
   * or values, varlen bytes; ColumnTarget.outputBytes) plus the locators. Pages loaded.
   */
  #dictionaryCodes(group: OpenGroup, c: number): boolean {
    if (this.#transferOutputs !== "dictionary") return false;
    const column = at(group.columns, c),
      dictionary = column.pages?.dictionary;
    if (column.ownedDictionary === undefined && dictionary != null)
      column.ownedDictionary = this.#options.outputDictionary?.(dictionary) ?? true;
    return column.ownedDictionary ?? true;
  }

  #bytesFor(group: OpenGroup, spec: DecodeSpec): number {
    let bytes = this.#options.emitLocator ? 8 * rowsIn(spec) : 0;
    for (let c = 0; c < this.#types.length; c++) {
      const pages = at(group.columns, c).pages;
      if (pages !== null)
        bytes += at(this.#targets, c).outputBytes(
          pages,
          spec,
          !group.owned || this.#dictionaryCodes(group, c),
          group.owned,
          group.owned ? this.#ownedMemory.outputSize : undefined,
        );
    }
    return bytes;
  }

  /** Loads the pages of every output column that `spec` touches (sizing and decoding read them). */
  *#loadPages(group: OpenGroup, spec: DecodeSpec): Step<void> {
    if (group.windowRows > 0 && "from" in spec && spec.to <= group.windowEnd) return;
    for (const column of group.columns) {
      const pages = column.pages;
      if (pages === null) continue;
      if ("from" in spec) {
        const [first, end] = pages.pagesInRange(spec.from, spec.to);
        yield* this.#wait(
          loadProfiled(
            pages,
            Array.from({ length: end - first }, (_, k) => first + k),
            this.stats,
          ),
          () => {
            if (group.retired && group.retained > 0) pages.release();
          },
        );
      } else
        yield* this.#wait(
          loadProfiled(pages, pages.pagesForRows(spec.rows, 0, spec.count), this.stats),
          () => {
            if (group.retired && group.retained > 0) pages.release();
          },
        );
    }
  }

  /** Loads pages, then the out-of-line values of exactly what `spec` decodes. */
  *#load(group: OpenGroup, spec: DecodeSpec, columns: readonly number[]): Step<void> {
    for (const c of columns) {
      const pages = at(group.columns, c).pages;
      if (pages === null) continue;
      if ("from" in spec) {
        if (!(group.windowRows > 0 && spec.to <= group.windowEnd)) {
          const [first, end] = pages.pagesInRange(spec.from, spec.to);
          yield* this.#wait(
            loadProfiled(
              pages,
              Array.from({ length: end - first }, (_, k) => first + k),
              this.stats,
            ),
            () => {
              if (group.retired && group.retained > 0) pages.release();
            },
          );
        }
        yield* this.#wait(pages.loadValues(spec.from, spec.to));
      } else {
        yield* this.#wait(
          loadProfiled(pages, pages.pagesForRows(spec.rows, 0, spec.count), this.stats),
          () => {
            if (group.retired && group.retained > 0) pages.release();
          },
        );
        yield* this.#wait(pages.loadValuesOf(spec.rows, 0, spec.count));
      }
    }
  }

  /**
   * The working set decoding `spec` (every output column) newly takes: the out-of-line values
   * leased for it and every target buffer it grows. Pages loaded.
   */
  #workingSet(group: OpenGroup, spec: DecodeSpec): { input: number; output: number } {
    let input = 0,
      output = 0;
    for (let c = 0; c < this.#types.length; c++) {
      const pages = at(group.columns, c).pages;
      if (pages === null) continue;
      const bytes = group.owned
        ? at(this.#targets, c).outputBytes(
            pages,
            spec,
            this.#dictionaryCodes(group, c),
            true,
            this.#ownedMemory.outputSize,
          )
        : at(this.#targets, c).growthFor(pages, spec);
      if (group.owned && this.#ownedMemory !== this.#memory) output += bytes;
      else input += bytes;
      input +=
        "from" in spec
          ? pages.largeValueBytes(spec.from, spec.to)
          : pages.largeValueBytesOf(spec.rows, 0, spec.count);
    }
    return { input, output };
  }

  /**
   * Reserves the working set of decoding `spec` (`#workingSet`), so the decode cannot be refused
   * halfway. False when the grant refuses a batch of several rows (the caller decodes fewer); for
   * one row, borrows a progress loan for it (R18.3), repaid when the batch's borrow ends.
   */
  *#reserveOutput(bytes: number, spec: DecodeSpec): Step<boolean> {
    if (this.#ownedMemory === this.#memory) return true;
    while (!this.#ownedMemory.prepay(bytes)) {
      if (this.#ownedCount > 0) {
        yield* this.#wait(
          new Promise<void>((resolve) => {
            this.#ownedWaiters.add(resolve);
          }),
        );
        continue;
      }
      if (rowsIn(spec) > 1) return false;
      if (!this.#options.borrowOutputOnRefusal)
        throw new RangeError("The result grant refused a one-row dedicated output");
      return false;
    }
    return true;
  }

  *#prepay(group: OpenGroup, spec: DecodeSpec): Step<boolean> {
    let { input: need, output } = this.#workingSet(group, spec);
    if (!(yield* this.#reserveOutput(output, spec))) {
      if (rowsIn(spec) > 1) return false;
      // Optional dedicated output must preserve the ordinary one-row progress-loan path.
      // All previous owned batches drained above; existing transfer tokens remain valid.
      group.owned = false;
      this.#ownedMemory.settle();
      ({ input: need, output } = this.#workingSet(group, spec));
    }
    if (this.#memory.prepay(need)) return true;
    if (this.#ownedMemory !== this.#memory) this.#ownedMemory.settle();
    if (rowsIn(spec) > 1) return false;
    const loans = this.#options.loans;
    if (loans === undefined)
      throw new RangeError(`A ${String(need)}-byte row was refused by the scan's grant`);
    this.#repay();
    this.#loan = yield* this.#wait(loans.borrow(need), (late) => {
      late.repay();
    });
    this.stats.loans++;
    if (!this.#memory.prepay(need))
      throw new RangeError(`A ${String(need)}-byte row was refused even after a loan`);
    return yield* this.#reserveOutput(output, spec);
  }

  *#decode(
    group: OpenGroup,
    spec: DecodeSpec,
    columns: readonly number[],
    out: Vector[],
    owned?: OwnedColumn[],
  ): Step<void> {
    yield* this.#load(group, spec, columns);
    const length = rowsIn(spec);
    for (const c of columns) {
      const column = at(group.columns, c);
      out[c] =
        column.pages === null
          ? {
              form: "constant",
              type: at(this.#types, c),
              length,
              value: column.missing ?? nullOf(at(this.#types, c)),
            }
          : owned === undefined || !group.owned
            ? at(this.#targets, c).decode(column.pages, spec)
            : (() => {
                const decoded = at(this.#targets, c).decodeOwned(
                  column.pages,
                  spec,
                  this.#dictionaryCodes(group, c),
                  this.#ownedMemory,
                );
                owned.push(decoded);
                return decoded.vector;
              })();
    }
  }

  /**
   * Emits a batch from the pending positions of range `[from, to)`: dense or gathered, byte
   * bounded, the residual applied. Null when the residual left nothing (the caller goes on).
   * What was reserved and not used goes back once the batch is decoded.
   */
  *#emitPositions(group: OpenGroup, from: number, to: number): Step<SourceItem | null> {
    const owned = group.owned ? ([] as OwnedColumn[]) : undefined;
    let published = false;
    try {
      const item = yield* this.#emitReserved(group, from, to, owned);
      if (item === null || owned === undefined || owned.length === 0) return item;
      const buffers = new Set<ArrayBuffer>();
      let bytes = 0;
      for (const column of owned) {
        bytes += column.bytes;
        for (const buffer of column.buffers) buffers.add(buffer);
      }
      const transfer = this.#retainOwned(buffers, bytes, group);
      published = true;
      return { ...item, transfer };
    } finally {
      if (!published && owned !== undefined) {
        const bytes = owned.reduce((sum, column) => sum + column.bytes, 0);
        if (bytes > 0) this.#ownedMemory.give(bytes);
      }
      this.#memory.settle();
      if (this.#ownedMemory !== this.#memory) this.#ownedMemory.settle();
    }
  }

  // One charge/credit per batch, independent of the emit locals. Retired groups have dropped
  // page slabs; only their explicit dictionary/directory lifetime survives in this handle.
  #retainOwned(buffers: ReadonlySet<ArrayBuffer>, bytes: number, group: OpenGroup): Transfer {
    group.retained++;
    this.#ownedBytes += bytes;
    this.#ownedCount++;
    let released = false;
    return {
      detachableBuffers: buffers,
      detachableBudget: this.#ownedMemory.budget,
      detachableCapacity: this.#ownedMemory.outputSize,
      release: () => {
        if (released) return;
        released = true;
        this.#ownedMemory.give(bytes);
        this.#ownedBytes -= bytes;
        this.#ownedCount--;
        group.retained--;
        if (group.retired && group.retained === 0) {
          for (const column of group.columns) column.pages?.close();
          group.segment.releaseRoot?.();
        }
        for (const wake of this.#ownedWaiters) wake();
        this.#ownedWaiters.clear();
        if (this.#aborted) this.#settleAborted();
      },
    };
  }

  *#emitReserved(
    group: OpenGroup,
    from: number,
    to: number,
    owned?: OwnedColumn[],
  ): Step<SourceItem | null> {
    const span = to - from;
    const all = this.#all;
    const count = group.pendingCount;
    const positions = this.#pending;
    // Dense when enough of the range is selected and the whole range fits the byte target.
    const fresh = group.fresh;
    group.fresh = false;
    const wanted = this.#rowsNeeded();
    let dense = fresh && count >= span * DENSE_SELECTION && count <= wanted;
    if (dense) {
      // Sizing reads page headers: the range's pages first (no values yet).
      yield* this.#loadPages(group, { from, to });
      dense =
        this.#bytesFor(group, { from, to }) <= this.#target &&
        (yield* this.#prepay(group, { from, to }));
    }
    const columns: Vector[] = new Array<Vector>(this.#defs.length);
    if (dense) {
      yield* this.#decode(group, { from, to }, all, columns, owned);
      let selection: Selection | null = null;
      if (count < span) {
        const s = this.#selection;
        for (let k = 0; k < count; k++) s.rows[k] = (positions[k] ?? 0) - from;
        s.count = count;
        selection = s;
      }
      group.pendingCount = 0;
      let batch: Batch = { length: span, columns, selection };
      if (this.#residual !== null) {
        const rows = this.#residual.select(batch);
        this.stats.residualBatches += 1;
        if (rows.count === 0) return null;
        batch = { ...batch, selection: rows.count === span ? null : rows };
      }
      this.stats.denseBatches += 1;
      return { batch: this.#withLocators(batch, group, (k) => from + k) };
    }
    let take = this.#residual === null ? Math.min(count, wanted) : count;
    yield* this.#loadPages(group, specForRows(positions, take));
    // Gather: a prefix of the positions that fits the byte target (at least one row), then the
    // grant (halving until it holds the prefix's working set, or one row under a loan).
    for (;;) {
      if (take <= 1) break;
      const bytes = this.#bytesFor(group, specForRows(positions, take));
      if (bytes <= this.#target) break;
      take = Math.max(1, Math.min(take - 1, Math.floor((take * this.#target) / bytes)));
    }
    while (!(yield* this.#prepay(group, specForRows(positions, take))))
      take = Math.max(1, Math.floor(take / 2));
    if (take < count) this.stats.splits += 1;
    // The taken positions move to `#survivors` before `pending` is shifted (below).
    const survivors = this.#survivors;
    survivors.set(positions.subarray(0, take));
    let kept = take;
    let consumed = take;
    if (this.#residual !== null) {
      // Residual columns first, for the candidates; the rest only for the survivors.
      const first = all.filter((c) => this.#residualColumns.has(c));
      const probe: Vector[] = all.map(() => this.#placeholder);
      yield* this.#decode(group, specForRows(survivors, take), first, probe);
      const rows = this.#residual.select({ length: take, columns: probe, selection: null });
      this.stats.residualBatches += 1;
      kept = Math.min(rows.count, wanted);
      if (rows.count > kept) consumed = (rows.rows[kept - 1] ?? 0) + 1;
      for (let k = 0; k < kept; k++) survivors[k] = survivors[rows.rows[k] ?? 0] ?? 0;
    }
    // Consume the taken positions; keep the rest for the next batch.
    positions.copyWithin(0, consumed, count);
    group.pendingCount = count - consumed;
    if (kept === 0) return null;
    yield* this.#decode(group, specForRows(survivors, kept), all, columns, owned);
    this.stats.gatherBatches += 1;
    return {
      batch: this.#withLocators(
        { length: kept, columns, selection: null },
        group,
        (k) => survivors[k] ?? 0,
      ),
    };
  }

  #withLocators(batch: Batch, group: OpenGroup, position: (slot: number) => number): Batch {
    if (!this.#options.emitLocator) return batch;
    const base = group.segment.index * LOCATOR_POSITIONS;
    for (let k = 0; k < batch.length; k++) this.#locators[k] = base + position(k);
    return { ...batch, columns: [...batch.columns, this.#locatorVector(batch.length)] };
  }

  #closeSegment(): void {
    if (this.#group !== null) {
      const group = this.#group;
      group.retired = true;
      for (const column of group.columns) {
        if (group.retained === 0) column.pages?.close();
        else column.pages?.release(); // Dictionary bytes/charge survive retained batches.
      }
      if (group.retained === 0) group.segment.releaseRoot?.();
    }
    this.#group = null;
    this.#memoryScan?.close();
    this.#memoryScan = null;
  }

  /**
   * Gives back everything the scan holds (readers, pages, values, buffers, scratch, staging),
   * then repays an outstanding loan (its bytes are free by then). Once; `#repay` retries.
   */
  #release(): void {
    if (!this.#released) {
      this.#released = true;
      this.#memoryScan?.abort();
      this.#closeSegment();
      this.#memory.settle();
      if (this.#ownedMemory !== this.#memory) this.#ownedMemory.settle();
      for (const target of this.#targets) target.close();
      if (this.#memoryOut !== null) this.#memory.give(this.#memoryOut.bytes);
      this.#memoryOut = null;
      this.#memory.give(this.#scratchBytes);
    }
    // A read still in flight may hold loan-backed bytes until it settles: then the suspended
    // step's unwinding repays (`#settleAborted`).
    this.#settleAborted();
  }

  /**
   * Ends the scan early (cancellation, an error, a LIMIT): everything is released and a loan
   * repaid at once, or, while the scan's own read is in flight, as soon as that read settles; a
   * `next()` still pending then resolves null without reading or decoding more. Never throws.
   */
  abort(): void {
    this.#aborted = true;
    this.#readAbort.abort();
    for (const wake of this.#ownedWaiters) wake();
    this.#ownedWaiters.clear();
    this.#release();
  }

  /**
   * Releases every buffer and reader (after the last batch's borrow ended). A loan that cannot be
   * repaid with nothing of the scan in flight (another operator of the grant holds its bytes)
   * throws here.
   */
  close(): void {
    this.abort();
    if (!this.#suspended) this.#repay();
  }
}

function rowsIn(spec: DecodeSpec): number {
  return "from" in spec ? spec.to - spec.from : spec.count;
}

/** A pushed conjunct decided once for a missing column (it held). */
const ALWAYS: PushedColumn = {
  encoded: false,
  mayMatch: () => true,
  prepare: () => undefined,
  prepareRows: () => undefined,
  select: () => 0,
  refine: (_rows, count) => count,
  selectMask: () => 0,
};

function nullOf(type: PhysicalType): FlatVector {
  const flat = allocateFlat(type, 1);
  return { ...flat, length: 1, validity: new Uint32Array(1) };
}

/** Output columns an expression reads. */
function columnsOf(expr: ScalarExpr): number[] {
  switch (expr.op) {
    case "column":
      return [expr.index];
    case "literal":
    case "param":
      return [];
    case "cast":
      return columnsOf(expr.input);
    case "compare":
    case "distinctFrom":
      return [...columnsOf(expr.left), ...columnsOf(expr.right)];
    case "and":
    case "or":
    case "call":
      return expr.args.flatMap(columnsOf);
    case "not":
      return columnsOf(expr.arg);
    case "isNull":
      return columnsOf(expr.arg);
    case "case":
      return [
        ...expr.branches.flatMap((b) => [...columnsOf(b.when), ...columnsOf(b.then)]),
        ...(expr.otherwise === null ? [] : columnsOf(expr.otherwise)),
      ];
    case "inList":
      return [...columnsOf(expr.arg), ...expr.list.flatMap(columnsOf)];
    case "like":
      return [...columnsOf(expr.arg), ...columnsOf(expr.pattern)];
  }
}
