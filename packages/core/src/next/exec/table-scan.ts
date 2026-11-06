/**
 * The `scan` operator's source (ir/ir.ts): one table of a snapshot (access/snapshot.ts), every
 * segment in row-id order, batches of at most `BATCH_ROWS` rows and about `targetBytes`, its
 * `columns` in output order (plus a locator column when `emitLocator`), filtered by `filter`.
 *
 * ## Group segments: prune → pushed predicates → decode the selected positions
 * - **Prune.** The filter's pushable conjuncts (exec/pushdown.ts) are compiled per column chunk;
 *   a group whose chunk statistics rule one out is skipped without reading a page.
 * - **Select.** Per range of `BATCH_ROWS` positions: the live rows (`deletions().liveRows`), then
 *   each pushed conjunct `select`s (or `refine`s) qualifying positions on encoded pages, loading
 *   only the pages it cannot rule out.
 * - **Decode.** At or above `DENSE_SELECTION` of the range the output columns decode densely
 *   (`decodeRange`, the selection kept); below it only the selected positions (`gather`, a compact
 *   batch). With a residual (conjuncts not pushed down), its columns decode first, the residual
 *   narrows the candidates, and only then are the other columns gathered for the survivors.
 * - **Out-of-line values** (page/large-value.ts) are loaded for exactly what is decoded
 *   (`loadValues` for a dense range, `loadValuesOf` for positions) and released when the batch's
 *   borrow ends; pages before the next range are released as the scan moves on.
 * - **Byte-bounded batches.** Varlen bytes (`varlenBytes` / `gatherBytes`: full value lengths) are
 *   known before any value is read; a batch that would pass `targetBytes` decodes a prefix of its
 *   positions and resumes with the rest. One row larger than the target is a batch of its own; if
 *   the grant refuses even that row's bytes, the scan borrows a progress loan for them (ruling
 *   R18.3, memory/governor.ts rule 5) and repays it once that batch's borrow ends.
 * - A column the group predates reads `ColumnDef.missing` (a constant).
 *
 * ## Memory segments
 * Read through `MemorySegmentScan` (exec/memory-scan.ts: one borrowed chunk at a time, zero copy
 * while resident); the whole filter is the residual there.
 *
 * ## Borrowing, sync-when-cached
 * A batch's vectors are the scan's own buffers (or a memory segment's lent chunk), valid until the
 * next `next()`: the pipeline asks again only after the batch's borrow ended. Every access call is
 * `MaybePromise`; `next()` returns a value when everything it needed was resident (the generator
 * below runs synchronously until a call actually returns a Promise).
 */
import type { MaybePromise } from "../access/key-index.js";
import type { ColumnDef } from "../access/catalog.js";
import type {
  ChunkOutput,
  DeletionVector,
  GroupSegment,
  MemorySegment,
  Segment,
  TableSnapshot,
} from "../access/snapshot.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { Loan } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { LOCATOR_POSITIONS } from "../vector/locator.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  allocateFlat,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../vector/vector.js";
import { ColumnTarget, type DecodeSpec, type TargetMemory } from "./column-target.js";
import { Predicate } from "./filter.js";
import type { CompileContext } from "./kernels/compile.js";
import { MemorySegmentScan } from "./memory-scan.js";
import { isPromise, type BatchSource, type SourceItem } from "./pipeline.js";
import {
  CompiledFallback,
  pushedColumn,
  splitFilter,
  type PushedColumn,
  type PushedPredicate,
} from "./pushdown.js";
import type { ChunkPages, ChunkReads } from "./scan.js";

/** `xs[i]`, which must exist. */
function at<T>(xs: readonly T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new RangeError(`no element ${String(i)}`);
  return x;
}

/** At or above this share of a range's positions selected, decode the range densely (B2's 40 %). */
export const DENSE_SELECTION = 0.4;

/** What the scan counts (diagnostics, tests, benches). */
export interface ScanStats {
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
}

export interface TableScanOptions {
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
}

// ── Sync-when-possible steps ────────────────────────────────────────────────────────────────

type Step<T> = Generator<Promise<unknown>, T, unknown>;

/** Waits for `value` only when it is a Promise. */
function* wait<T>(value: MaybePromise<T>): Step<T> {
  return isPromise(value) ? ((yield value) as T) : value;
}

/** Runs a step generator, synchronously until it yields a Promise. */
function run<T>(step: Step<T>, input?: unknown, failure?: { error: unknown }): MaybePromise<T> {
  const result = failure === undefined ? step.next(input) : step.throw(failure.error);
  if (result.done) return result.value;
  return result.value.then(
    (value) => run(step, value),
    (error: unknown) => run(step, undefined, { error }),
  );
}

// ── Group segments ──────────────────────────────────────────────────────────────────────────

/** One output column of an open group: its reader and target, or the group predates it. */
interface GroupColumn {
  readonly pages: ChunkPages | null;
  /** The group predates the column: every row is this value (`ColumnDef.missing` or NULL). */
  readonly missing: FlatVector | null;
}

/** An open group segment and where the scan is in it. */
interface OpenGroup {
  readonly segment: GroupSegment;
  readonly columns: readonly GroupColumn[];
  readonly pushed: readonly PushedColumn[];
  readonly deletions: DeletionVector | null;
  /** Next range start. */
  from: number;
  /** Positions still to emit from the current range (a byte-bounded batch left them). */
  pending: Uint16Array;
  pendingCount: number;
  pendingFrom: number;
  pendingTo: number;
  /** The pending positions are the whole selection of their range (nothing emitted yet). */
  fresh: boolean;
}

/** Memory charged through the scan's budget; `reserve` holds bytes taken ahead of a decode. */
class ScanMemory implements TargetMemory {
  reserve = 0;
  constructor(readonly budget: ByteBudget) {}
  take(bytes: number): void {
    if (this.reserve >= bytes) {
      this.reserve -= bytes;
      return;
    }
    if (!this.budget.allocate(bytes))
      throw new RangeError(`A ${String(bytes)}-byte scan buffer was refused by its grant`);
  }
  give(bytes: number): void {
    this.budget.free(bytes);
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

export class TableScan implements BatchSource {
  readonly stats: ScanStats = {
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
  };
  readonly #options: TableScanOptions;
  readonly #defs: readonly ColumnDef[];
  readonly #types: readonly PhysicalType[];
  readonly #pushed: readonly PushedPredicate[];
  readonly #fallbacks: readonly CompiledFallback[];
  readonly #residual: Predicate | null;
  /** Output columns the residual reads (decoded first on the gather path). */
  readonly #residualColumns: ReadonlySet<number>;
  readonly #memory: ScanMemory;
  readonly #targets: ColumnTarget[];
  readonly #positions = new Uint16Array(BATCH_ROWS);
  readonly #survivors = new Uint16Array(BATCH_ROWS);
  readonly #selection: Selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
  readonly #live: Selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
  readonly #locators: Float64Array;
  readonly #locatorType: PhysicalType;
  readonly #placeholder: Vector;
  readonly #target: number;
  #segment = 0;
  #group: OpenGroup | null = null;
  #memoryScan: MemorySegmentScan | null = null;
  #memoryOut: ChunkOutput[] | null = null;
  #loan: Loan | null = null;
  #aborted = false;

  constructor(options: TableScanOptions) {
    this.#options = options;
    const table = options.table;
    this.#defs = options.columns.map((id) => {
      const def = table.columns.find((column) => column.id === id);
      if (def === undefined) throw new RangeError(`Column ${String(id)} is not in the table`);
      return def;
    });
    this.#types = this.#defs.map((def) => def.type);
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
    this.#targets = this.#types.map((type) => new ColumnTarget(type, BATCH_ROWS, this.#memory));
    this.#locators = new Float64Array(BATCH_ROWS);
    this.#locatorType = { kind: "locator", tableId: table.tableId };
    this.#placeholder = {
      form: "constant",
      type: { kind: "bool" },
      length: 0,
      value: allocateFlat({ kind: "bool" }, 1),
    };
    this.#target = options.targetBytes ?? BATCH_TARGET_BYTES;
  }

  next(): MaybePromise<SourceItem | null> {
    return run(this.#next());
  }

  *#next(): Step<SourceItem | null> {
    this.#endBorrow();
    for (;;) {
      if (this.#aborted) return null;
      const segment: Segment | undefined = this.#options.table.segments[this.#segment];
      if (segment === undefined) return null;
      const item =
        segment.kind === "group"
          ? yield* this.#fromGroup(segment)
          : yield* this.#fromMemory(segment);
      if (item !== null) return item;
      this.#closeSegment();
      this.#segment += 1;
    }
  }

  /** The previous batch's borrow ended: release its values, repay a loan it needed. */
  #endBorrow(): void {
    const group = this.#group;
    if (group !== null) for (const column of group.columns) column.pages?.releaseValues();
    this.#memory.settle();
    if (this.#loan !== null) {
      // The oversized buffer goes back first, so the loan's bytes are free to repay.
      for (const target of this.#targets) target.shrink();
      this.#loan.repay();
      this.#loan = null;
    }
  }

  // ── Memory segments ──

  *#fromMemory(segment: MemorySegment): Step<SourceItem | null> {
    if (this.#memoryScan === null) {
      this.#memoryOut ??= this.#types.map((type) => ({
        flat: allocateFlat(type, BATCH_ROWS),
        codes: new Int32Array(BATCH_ROWS),
        varlen: null,
      }));
      this.#memoryScan = new MemorySegmentScan([segment], this.#options.columns, this.#memoryOut);
    }
    const scan = this.#memoryScan;
    for (;;) {
      const item = yield* wait(scan.next());
      if (item === null) return null;
      let batch = item.batch;
      if (this.#options.filter !== null) {
        const predicate = this.#memoryPredicate();
        const rows = predicate.select(batch);
        if (rows.count === 0) continue;
        batch = { ...batch, selection: rows };
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
    yield* wait(segment.root());
    const columns: GroupColumn[] = [];
    const opened: ChunkPages[] = [];
    try {
      for (let c = 0; c < this.#defs.length; c++) {
        const def = at(this.#defs, c);
        const chunk = segment.chunk(def.id);
        if (chunk === null) {
          columns.push({ pages: null, missing: def.missing ?? nullOf(def.type) });
          continue;
        }
        const pages = yield* wait(
          this.#options.reads.open(this.#options.pages, chunk, { budget: this.#options.budget }),
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
            for (const pages of opened) pages.close();
            this.stats.groupsPruned += 1;
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
        );
        if (!compiled.mayMatch(segment.rowCount, chunk.statistics)) {
          for (const pages of opened) pages.close();
          this.stats.groupsPruned += 1;
          return null;
        }
        if (compiled.encoded) this.stats.encodedPredicates += 1;
        else this.stats.decodedPredicates += 1;
        pushed.push(compiled);
      }
      const deletions = yield* wait(segment.deletions());
      this.stats.groupsRead += 1;
      return {
        segment,
        columns,
        pushed,
        deletions,
        from: 0,
        pending: new Uint16Array(BATCH_ROWS),
        pendingCount: 0,
        pendingFrom: 0,
        pendingTo: 0,
        fresh: false,
      };
    } catch (error) {
      for (const pages of opened) pages.close();
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
      if (group.pendingCount > 0) {
        const item = yield* this.#emitPositions(group, group.pendingFrom, group.pendingTo);
        if (item !== null) return item;
        continue;
      }
      if (group.from >= segment.rowCount) return null;
      const from = group.from;
      const to = Math.min(from + BATCH_ROWS, segment.rowCount);
      group.from = to;
      this.#releaseBefore(group, from);
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
          yield* wait(pushed.prepare(from, to));
          count = pushed.select(from, to, positions);
          all = false;
        } else {
          yield* wait(pushed.prepareRows(positions, count));
          count = pushed.refine(positions, count);
        }
      }
      if (count === 0) continue;
      if (all) for (let k = 0; k < count; k++) positions[k] = from + k;
      group.pending.set(positions.subarray(0, count));
      group.pendingCount = count;
      group.pendingFrom = from;
      group.pendingTo = to;
      group.fresh = true;
      const item = yield* this.#emitPositions(group, from, to);
      if (item !== null) return item;
    }
  }

  /** Pages wholly before `from` are no longer needed. */
  #releaseBefore(group: OpenGroup, from: number): void {
    if (from === 0) return;
    for (const column of group.columns) {
      const pages = column.pages;
      if (pages === null) continue;
      const [first] = pages.pagesInRange(from, from + 1);
      if (first > 0) pages.release(Array.from({ length: first }, (_, k) => k));
    }
  }

  /** Varlen bytes the given output columns need for `spec`. */
  #bytesFor(group: OpenGroup, spec: DecodeSpec, columns: readonly number[]): number {
    let bytes = 0;
    for (const c of columns) {
      const pages = group.columns[c]?.pages ?? null;
      if (pages !== null) bytes += ColumnTarget.varlenBytesOf(pages, at(this.#types, c), spec);
    }
    return bytes;
  }

  /** Loads pages (and out-of-line values) of `columns` for `spec`. */
  /** Loads the pages of `columns` that `spec` touches (sizing and decoding read them). */
  *#loadPages(group: OpenGroup, spec: DecodeSpec, columns: readonly number[]): Step<void> {
    for (const c of columns) {
      const pages = group.columns[c]?.pages ?? null;
      if (pages === null) continue;
      if ("from" in spec) {
        const [first, end] = pages.pagesInRange(spec.from, spec.to);
        yield* wait(pages.load(Array.from({ length: end - first }, (_, k) => first + k)));
      } else yield* wait(pages.load(pages.pagesForRows(spec.rows, 0, spec.count)));
    }
  }

  /** Loads pages, then the out-of-line values of exactly what `spec` decodes. */
  *#load(group: OpenGroup, spec: DecodeSpec, columns: readonly number[]): Step<void> {
    yield* this.#loadPages(group, spec, columns);
    for (const c of columns) {
      const pages = group.columns[c]?.pages ?? null;
      if (pages === null) continue;
      if ("from" in spec) yield* wait(pages.loadValues(spec.from, spec.to));
      else yield* wait(pages.loadValuesOf(spec.rows, 0, spec.count));
    }
  }

  /** Charges what decoding `spec` into the targets of `columns` will grow, or borrows it. */
  *#prepay(group: OpenGroup, spec: DecodeSpec, columns: readonly number[]): Step<boolean> {
    let growth = 0;
    for (const c of columns) {
      const pages = group.columns[c]?.pages ?? null;
      if (pages === null) continue;
      growth += at(this.#targets, c).growthFor(
        ColumnTarget.varlenBytesOf(pages, at(this.#types, c), spec),
      );
    }
    if (this.#memory.prepay(growth)) return true;
    const rows = "from" in spec ? spec.to - spec.from : spec.count;
    if (rows > 1) return false; // the caller decodes fewer rows
    // One row the grant cannot hold: a progress loan (R18.3), repaid when this batch's borrow ends.
    const loans = this.#options.loans;
    if (loans === undefined)
      throw new RangeError(`A ${String(growth)}-byte row was refused by the scan's grant`);
    this.#loan = yield* wait(loans.borrow(growth));
    this.stats.loans += 1;
    if (!this.#memory.prepay(growth))
      throw new RangeError(`A ${String(growth)}-byte row was refused even after a loan`);
    return true;
  }

  *#decode(
    group: OpenGroup,
    spec: DecodeSpec,
    columns: readonly number[],
    out: Vector[],
  ): Step<void> {
    yield* this.#load(group, spec, columns);
    const length = "from" in spec ? spec.to - spec.from : spec.count;
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
          : at(this.#targets, c).decode(column.pages, spec);
    }
  }

  /**
   * Emits a batch from the pending positions of range `[from, to)`: dense or gathered, byte
   * bounded, the residual applied. Null when the residual left nothing (the caller goes on).
   */
  *#emitPositions(group: OpenGroup, from: number, to: number): Step<SourceItem | null> {
    const span = to - from;
    const all = this.#defs.map((_, c) => c);
    const count = group.pendingCount;
    const positions = group.pending;
    // Dense when enough of the range is selected and the whole range fits the byte target.
    const fresh = group.fresh;
    group.fresh = false;
    let dense = fresh && count >= span * DENSE_SELECTION;
    if (dense) {
      // Sizing reads page headers: the range's pages first (no values yet).
      yield* this.#loadPages(group, { from, to }, all);
      dense =
        this.#bytesFor(group, { from, to }, all) <= this.#target &&
        (yield* this.#prepay(group, { from, to }, all));
    }
    if (!dense) yield* this.#loadPages(group, { rows: positions, count }, all);
    const columns: Vector[] = new Array<Vector>(this.#defs.length);
    if (dense) {
      yield* this.#decode(group, { from, to }, all, columns);
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
    // Gather: a prefix of the positions that fits the byte target (at least one row).
    let take = count;
    while (take > 1 && this.#bytesFor(group, { rows: positions, count: take }, all) > this.#target)
      take = Math.max(1, Math.floor(take / 2));
    while (!(yield* this.#prepay(group, { rows: positions, count: take }, all)))
      take = Math.max(1, Math.floor(take / 2));
    if (take < count) this.stats.splits += 1;
    // The taken positions move to `#survivors` before `pending` is shifted (below).
    const survivors = this.#survivors;
    survivors.set(positions.subarray(0, take));
    let kept = take;
    if (this.#residual !== null) {
      // Residual columns first, for the candidates; the rest only for the survivors.
      const first = all.filter((c) => this.#residualColumns.has(c));
      const probe: Vector[] = all.map(() => this.#placeholder);
      yield* this.#decode(group, { rows: positions, count: take }, first, probe);
      const rows = this.#residual.select({ length: take, columns: probe, selection: null });
      this.stats.residualBatches += 1;
      kept = rows.count;
      for (let k = 0; k < kept; k++) survivors[k] = survivors[rows.rows[k] ?? 0] ?? 0;
    }
    // Consume the taken positions; keep the rest for the next batch.
    positions.copyWithin(0, take, count);
    group.pendingCount = count - take;
    if (kept === 0) return null;
    yield* this.#decode(group, { rows: survivors, count: kept }, all, columns);
    this.stats.gatherBatches += 1;
    const emitted = survivors.slice(0, kept);
    return {
      batch: this.#withLocators(
        { length: kept, columns, selection: null },
        group,
        (k) => emitted[k] ?? 0,
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
    if (this.#group !== null) for (const column of this.#group.columns) column.pages?.close();
    this.#group = null;
    this.#memoryScan = null;
  }

  abort(): void {
    this.#aborted = true;
    this.#memoryScan?.abort();
    this.#closeSegment();
    this.#memory.settle();
    for (const target of this.#targets) target.close();
  }

  /** Releases every buffer and reader (after the last batch's borrow ended). */
  close(): void {
    this.abort();
  }
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
