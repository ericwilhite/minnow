/**
 * The hash join (track B7, increment 1): typed tables, every join kind, residual ON conditions,
 * grace/hybrid spill with recursive repartitioning and chunked heavy hitters, and dynamic join
 * filters for the probe side.
 *
 * ## Wiring (the executor composes it; exec/executor.ts)
 * ```text
 * build pipeline:  right input ─▶ join.build            (an Operator sink)
 * probe pipeline:  left input  ─▶ join.probe(downstream) (an Operator) ─▶ downstream
 * ```
 * The build pipeline must finish before the probe pipeline pushes. Keys are input column
 * indexes: the executor projects key expressions first (as it does for `aggregate.groupBy`).
 * `join.runtimeFilters` is set when the build finishes; the executor hands it to the scans the
 * IR's `publishes` name (exec/join-filter.ts).
 *
 * ## Output (IR `join`: left = probe, right = build)
 * - inner, left, right, full: probe columns ++ build columns; a missing side is null.
 * - semi, anti: probe columns, each probe row at most once.
 * - mark: probe columns ++ one bool: `probe.T IN (build.T where build.C = probe.C)` with SQL's
 *   three-valued result (below). Lowering emits `NOT mark` for NOT IN.
 *
 * ## Semantics (law: PostgreSQL; join.pglite.test.ts compares every kind against it)
 * - A pair matches when every key is equal (`=`: NULL never matches; `nullSafe`: IS NOT
 *   DISTINCT FROM) and the residual is TRUE. FALSE and NULL residuals are both non-matches.
 * - Outer kinds null-extend a row that has no matching pair after the residual; right and full
 *   track matches per build row and emit the unmatched build rows when the probe ends.
 * - Mark (ruling R20.1; `correlated` leading keys C, the rest the tested keys T): the
 *   three-valued OR, over the build rows whose C equals the probe's C (`=`; a null probe C
 *   matches nothing), of `T = T' AND residual`: TRUE if some row gives TRUE, else NULL if some
 *   row gives NULL, else FALSE (so no matching row gives FALSE). `T = T'` is SQL's row
 *   comparison, exact per column: FALSE when some column is provably different, NULL when none
 *   is but one is null — `(1, 2) IN (SELECT 3, NULL)` is FALSE, never a global "has NULL"
 *   shortcut. Without a residual this is `T IN (SELECT T' … WHERE C' = C)`. The mark's `filter`
 *   (ruling R20.5) is the subquery's WHERE: a build row takes part only where it is TRUE.
 *
 * ## Execution
 * - **Build.** Rows are copied into a chunked `ColumnStore` under the join's spillable grant. At
 *   the end the table is built from the exact size (join-table.ts: direct addressing for dense
 *   integer keys, otherwise open addressing with inline key words).
 * - **Probe.** Per batch: canonical keys, one lookup per row, then output. When no build key
 *   repeats and there is no residual, the output is the probe batch itself (borrowed, zero copy)
 *   with a selection, plus build columns gathered aligned to its rows. Otherwise matches expand
 *   into dense pair batches (≤ 2,048 pairs, byte-bounded), the residual runs over them as a
 *   vector predicate, and outer/semi/anti/mark outcomes are folded per probe row.
 * - **Backpressure.** `push` returns `blocked` when the downstream blocks (or a spill write
 *   waits); the batch stays borrowed and the join resumes it inside `ready()`.
 *
 * ## Spill: grant-or-spill, never refuse
 * - When the build store or table is refused memory the join partitions: 16 ways by the top bits
 *   of the level's hash (a mark join cannot partition by T: it uses one partition, processed in
 *   chunks). Every row goes to typed, checksummed spill pages (exec/spill-pages.ts). At the end of
 *   the build, partitions are read back while they fit (hybrid): their probe rows join at once;
 *   the others' probe rows are spilled too.
 * - After the probe, each spilled partition pair is joined by a sub-join one level down, whose
 *   hash bits are independent (join-keys.ts `levelHash`); it partitions again if it overflows.
 * - A partition that does not split (every row in one partition: a heavy-hitter key) or a mark
 *   join is processed in **chunks**: as many build pages as fit form a table, the whole probe run
 *   is replayed against it, per-probe-row state (matched, mark class) survives across chunks, and
 *   a final replay emits unmatched / mark rows. Bounded memory, always terminates.
 * - I/O per external pass ≈ 2 × (spilled build + spilled probe bytes); recursion adds passes.
 *
 * ## Memory
 * Everything the join holds is allocated from its grant. Its admitted minimum
 * (`hashJoinMinimumBytes`) covers the batch scratch, the two output chunks, the page buffer and a
 * spill pool for page reads and partition staging, so spilling never needs memory the grant has
 * no room for. A single value larger than the pool takes a progress loan (memory/governor.ts
 * rule 5).
 *
 * ## Progress with wide values
 * Pages stop at 64 KiB of varlen bytes (a larger row is a page of its own), rows load in row
 * units, and a page whose rows all belong to one sub-partition is forwarded without a copy. A
 * step that the pool and grant cannot hold takes one loan sized for that step (a row and its
 * page; a page, a row, its table and the replay's headroom in chunk mode), repaid when the step's
 * usage has left. In chunk mode one step can hold a build row, a probe page and an output row at
 * once, so at the minimum grant values up to about a quarter of the reserve progress; a larger
 * one is `ValueTooLargeError` (the governor's one size error).
 *
 * ## Limits (this increment)
 * - Runtime filters are published only when the build stays resident (a spilled build publishes
 *   none: correct, just unfiltered).
 * - Mark joins do not partition on C; an oversized mark build runs in chunks.
 * - Chunk mode keeps 2 bits per probe row of the partition in memory (charged).
 */
import type { JoinKind } from "../ir/ir.js";
import type { Grant } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import type { PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../vector/vector.js";
import {
  YIELD_INTERVAL_MS,
  isPromise,
  yieldTask,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import { Slice, SLICE_ROWS, runSliced, runToEnd, type SlicedJob } from "./slices.js";
import { JoinRuntimeFilter, kindPublishes } from "./join-filter.js";
import {
  KEY_STEP_BYTES,
  KeyLayout,
  KeyScratch,
  KeyWork,
  bytesEqual,
  extractKeys,
  extractKeysJob,
  keyNulls,
  keyStepRows,
  levelHash,
} from "./join-keys.js";
import { PageWriter, SpillFiles, SpilledRun, type DecodedPage } from "./spill-pages.js";
import { ColumnStore, nullConstant } from "./column-store.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";
import { JoinTable, chunkKeys } from "./join-table.js";

type MaybePromise<T> = T | Promise<T>;

/** A compiled boolean condition over a batch (the executor's expression kernels satisfy it). */
export interface VectorPredicate {
  /**
   * Writes into `out` the active rows of `batch` (its selection, or 0 … length) where the
   * condition is TRUE — not FALSE, not NULL — ascending, and returns it. Synchronous; does not
   * retain `batch`.
   */
  filter(batch: Batch, out: Selection): Selection;
  /**
   * Like `filter`, for the rows where the condition is NULL. Required for a mark join's residual
   * (its UNKNOWN counts, ruling R20.1); other kinds never call it.
   */
  unknown?(batch: Batch, out: Selection): Selection;
}

export type HashJoinKind = Exclude<JoinKind, "cross">;

export interface HashJoinSpec {
  readonly kind: HashJoinKind;
  readonly probeTypes: readonly PhysicalType[];
  readonly buildTypes: readonly PhysicalType[];
  /** Key columns of the probe (left) input. */
  readonly probeKeys: readonly number[];
  /** Key columns of the build (right) input, pairwise with `probeKeys`. */
  readonly buildKeys: readonly number[];
  /** Per key: IS NOT DISTINCT FROM instead of `=` (never on a mark join's tested keys). */
  readonly nullSafe: readonly boolean[];
  /** Mark joins: how many leading keys are correlation keys (C); the rest are tested (T). */
  readonly correlated?: number;
  /** Remaining ON conditions over probe columns ++ build columns, after the key match. */
  readonly residual: VectorPredicate | null;
  /**
   * Mark joins only (ruling R20.5): the subquery's own WHERE, correlated conditions included. A
   * build row takes part only where it is TRUE (FALSE and NULL exclude it); `residual` is then
   * only the [NOT] IN / quantified comparison, three-valued.
   */
  readonly filter?: VectorPredicate | null;
  /** Runtime filters to publish: filter id and key position (inner, semi and right only). */
  readonly publishes: ReadonlyArray<{ readonly id: number; readonly key: number }>;
}

export interface HashJoinContext {
  /** Spillable; the join allocates its state from it and closes it when done or aborted. */
  readonly grant: Grant;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal?: AbortSignal;
  /** Plan id, for spill page headers and diagnostics. */
  readonly relId?: number;
  /** Test seam: disable direct addressing. */
  readonly direct?: boolean;
}

export interface HashJoinProfile {
  rowsOut: number;
  batchesOut: number;
  buildRows: number;
  probeRows: number;
  spilledBytes: number;
  /** Sub-joins and chunk passes run over spilled partitions. */
  spillPasses: number;
  /** Chunk passes (heavy hitters, mark joins over budget). */
  chunkPasses: number;
  /** Progress loans taken (one bounded step each) and their largest size. */
  loans: number;
  largestLoan: number;
  /** Spill pages appended whole to a sub-partition, without a copy. */
  forwardedPages: number;
  peakHeldBytes: number;
  table: "direct" | "words" | "bytes" | null;
}

const FANOUT_BITS = 4;
const FANOUT = 1 << FANOUT_BITS;
/** Levels whose hash bits stay independent before chunking takes over. */
export const MAX_LEVEL = 6;
const DONE = 0;
const BLOCKED = 1;
type Step = typeof DONE | typeof BLOCKED | Promise<void>;

const TRUE_CLASS = 2;
const NULL_CLASS = 1;
/**
 * A staging page stops at this many varlen bytes (one larger row is a page of its own), so a
 * page read or written is a bounded step; the service batches pages into large writes.
 */
const STAGING_TARGET_BYTES = 64 * 1024;
const STAGING_FIXED_BYTES = 32 * 1024;
/** One page in flight (read or being encoded), at its bounded size. */
const POOL_PAGE_BYTES = 2 * (STAGING_TARGET_BYTES + STAGING_FIXED_BYTES) + 16 * 1024;
const STAGING_VARLEN_BYTES = 4096;

/** Bytes one row of `types` is estimated at (varlen at 36). */
function rowBytes(types: readonly PhysicalType[]): number {
  let bytes = 0;
  for (const type of types) {
    switch (type.kind) {
      case "bool":
        bytes += 2;
        break;
      case "int32":
      case "date":
      case "float32":
        bytes += 5;
        break;
      case "uuid":
      case "interval":
        bytes += 17;
        break;
      case "text":
      case "bytes":
      case "json":
      case "enum":
      case "array":
        bytes += 40;
        break;
      default:
        bytes += 9;
    }
  }
  return Math.max(1, bytes);
}

/** Rows per staging page: a power of two with ≤ 32 KiB of fixed-width data. */
function stagingRows(types: readonly PhysicalType[]): number {
  let rows = 16;
  while (rows < BATCH_ROWS && rows * 2 * rowBytes(types) <= STAGING_FIXED_BYTES) rows *= 2;
  return rows;
}

/** Bytes one staging store of `types` holds before its varlen arenas grow. */
function stagingBytes(types: readonly PhysicalType[]): number {
  return stagingRows(types) * (rowBytes(types) + 8) + STAGING_VARLEN_BYTES * types.length + 64;
}

/** One output chunk of `types` (rows at their estimate, an initial arena per column). */
function outputBytes(types: readonly PhysicalType[]): number {
  return BATCH_ROWS * (rowBytes(types) + 8) + 16 * 1024 * types.length + 1024;
}

/**
 * The pool: every transient buffer of the join (partition staging at its target, one page in
 * flight, the page encoder, the three output chunks) so a spill step never needs memory the
 * grant may refuse.
 */
function poolBytes(spec: HashJoinSpec): number {
  const side = Math.max(stagingBytes(spec.buildTypes), stagingBytes(spec.probeTypes));
  return (
    2 * POOL_PAGE_BYTES +
    FANOUT * (side + STAGING_TARGET_BYTES) +
    outputBytes(spec.probeTypes) +
    2 * outputBytes(spec.buildTypes)
  );
}

/** Shared scratch arrays (charged once). */
const SCRATCH_BYTES =
  BATCH_ROWS * (4 + 4 + 2 + 4 + 1 + 1 + 1 + 4 + 2 * 3) + 4 * (FANOUT + 1) + BATCH_ROWS;

/** The bytes a join needs admitted before it runs (fixed buffers; state grows or spills). */
export function hashJoinMinimumBytes(spec: HashJoinSpec): number {
  const keyTypes = spec.probeKeys
    .map((c) => spec.probeTypes[c])
    .filter((type): type is PhysicalType => type !== undefined);
  const layout = new KeyLayout(keyTypes, spec.nullSafe);
  return KeyScratch.bytes(layout, BATCH_ROWS) + SCRATCH_BYTES + poolBytes(spec);
}

/** State shared by every level of one join: spec, scratch, outputs, spill, downstream. */
class Shared {
  readonly kind: HashJoinKind;
  readonly layout: KeyLayout;
  readonly correlated: number;
  readonly charge: Charge;
  readonly pool: Pool;
  readonly files: SpillFiles;
  readonly pages: PageWriter;
  readonly io: SpillIo;
  readonly scratch: KeyScratch;
  readonly heads = new Int32Array(BATCH_ROWS);
  readonly aligned = new Int32Array(BATCH_ROWS);
  readonly pairProbe = new Uint16Array(BATCH_ROWS);
  readonly pairBuild = new Int32Array(BATCH_ROWS);
  readonly rowState = new Uint8Array(BATCH_ROWS);
  readonly partition = new Uint8Array(BATCH_ROWS);
  readonly order = new Int32Array(BATCH_ROWS);
  readonly counts = new Int32Array(FANOUT + 1);
  readonly resident: Selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
  readonly out: Selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
  readonly passing: Selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
  readonly admitted: Selection = { rows: new Uint16Array(BATCH_ROWS), count: 0 };
  /** Mark joins: a filter or residual must see each pair (no count shortcuts). */
  readonly conditioned: boolean;
  readonly markValidity = new Uint32Array(BATCH_ROWS >>> 5);
  readonly probeOut: ColumnStore;
  readonly buildOut: ColumnStore;
  readonly markOut: FlatVector;
  readonly probeNulls: readonly Vector[];
  readonly buildNulls: readonly Vector[];
  readonly buildColumns: readonly number[];
  /**
   * Build columns gathered for matched rows; the others are key columns equal to their probe key
   * in representation as well as value (integers, dates, timestamps, uuid, raw bytes), so a
   * matched output reuses the probe's vector instead of gathering (`reuse[c]` = probe column).
   */
  readonly gatherColumns: readonly number[];
  readonly reuse: readonly number[];
  readonly buildPart: ColumnStore;
  readonly profile: HashJoinProfile = {
    rowsOut: 0,
    batchesOut: 0,
    buildRows: 0,
    probeRows: 0,
    spilledBytes: 0,
    spillPasses: 0,
    chunkPasses: 0,
    loans: 0,
    largestLoan: 0,
    forwardedPages: 0,
    peakHeldBytes: 0,
    table: null,
  };
  downstream: Operator | null = null;
  aborted: unknown = undefined;
  /** The slice of the current build, probe push or finish work (R23.2). */
  readonly slice = new Slice(YIELD_INTERVAL_MS);
  /** The key-byte budget every hash and comparison spends (review B7-p2c-2). */
  readonly work = new KeyWork();

  constructor(
    readonly spec: HashJoinSpec,
    readonly context: HashJoinContext,
  ) {
    this.kind = spec.kind;
    if (spec.probeKeys.length !== spec.buildKeys.length)
      throw new RangeError("Join keys must pair up");
    const keyTypes = spec.probeKeys.map((c) => {
      const type = spec.probeTypes[c];
      if (type === undefined) throw new RangeError("Probe key column out of range");
      return type;
    });
    for (let i = 0; i < spec.buildKeys.length; i++) {
      const type = spec.buildTypes[spec.buildKeys[i] ?? -1];
      if (type === undefined) throw new RangeError("Build key column out of range");
      if (type.kind !== keyTypes[i]?.kind)
        throw new TypeError("Join keys must have the same type on both sides (lowering casts)");
    }
    this.correlated = spec.kind === "mark" ? (spec.correlated ?? 0) : spec.probeKeys.length;
    if (spec.kind === "mark") {
      if (this.correlated < 0 || this.correlated >= keyTypes.length)
        throw new RangeError("A mark join tests at least one key");
      for (let i = this.correlated; i < keyTypes.length; i++)
        if (spec.nullSafe[i] === true) throw new TypeError("A mark join's tested keys use =");
    }
    if (spec.kind !== "mark" && (spec.filter ?? null) !== null)
      throw new TypeError("Only a mark join has a filter apart from its residual");
    if (spec.kind === "mark" && spec.residual !== null && spec.residual.unknown === undefined)
      throw new TypeError("A mark join's residual must report NULL rows (`unknown`)");
    if (spec.publishes.length > 0 && !kindPublishes(spec.kind))
      throw new TypeError(
        `A ${spec.kind} join keeps its probe misses: it cannot publish runtime filters`,
      );
    this.layout = new KeyLayout(keyTypes, spec.nullSafe);
    this.conditioned = spec.residual !== null || (spec.filter ?? null) !== null;
    this.charge = new Charge(context.grant);
    if (!this.charge.allocate(SCRATCH_BYTES))
      throw new RangeError("A join's minimum must be admitted before it is built");
    this.pool = new Pool(this.charge, poolBytes(spec));
    this.files = new SpillFiles(context.spill, context.queryId, this.charge);
    this.pages = new PageWriter(this.pool, 16 * 1024);
    this.io = new SpillIo(this.pool, this.files, this.pages, context, () => {
      this.checkAborted();
    });
    this.scratch = new KeyScratch(this.layout, BATCH_ROWS, this.charge, this.pool);
    this.probeOut = new ColumnStore(spec.probeTypes, this.pool, {
      targetBytes: BATCH_TARGET_BYTES,
    });
    this.buildOut = new ColumnStore(spec.buildTypes, this.pool, {
      targetBytes: BATCH_TARGET_BYTES,
    });
    this.markOut = {
      form: "flat",
      type: { kind: "bool" },
      length: 0,
      validity: null,
      data: { layout: "bool", values: new Uint8Array(BATCH_ROWS) },
    };
    this.probeNulls = spec.probeTypes.map((type) => nullConstant(type, 0));
    this.buildNulls = spec.buildTypes.map((type) => nullConstant(type, 0));
    this.buildColumns = spec.buildTypes.map((_, c) => c);
    const reuse = spec.buildTypes.map(() => -1);
    spec.buildKeys.forEach((column, i) => {
      const part = this.layout.parts[i];
      if (part === "w1" || part === "w2i" || part === "w4" || part === "raw")
        if ((reuse[column] ?? -1) < 0) reuse[column] = spec.probeKeys[i] ?? -1;
    });
    this.reuse = reuse;
    this.gatherColumns = this.buildColumns.filter((c) => (reuse[c] ?? -1) < 0);
    this.buildPart = new ColumnStore(
      this.gatherColumns.map((c) => columnOf(spec.buildTypes, c)),
      this.pool,
      { targetBytes: BATCH_TARGET_BYTES },
    );
  }

  /** Output build columns for matched rows: gathered ones, and probe key vectors for the rest. */
  matchedBuildColumns(probeColumns: readonly Vector[]): Vector[] {
    const gathered = this.buildPart.vectors();
    let next = 0;
    return this.buildColumns.map((c) => {
      const probe = this.reuse[c] ?? -1;
      return probe >= 0 ? columnOf(probeColumns, probe) : (gathered[next++] as Vector);
    });
  }

  checkAborted(): void {
    if (this.aborted !== undefined)
      throw this.aborted instanceof Error
        ? this.aborted
        : new Error("Aborted", { cause: this.aborted });
    this.context.signal?.throwIfAborted();
  }

  /** Runs a job on a fresh slice: task yields between slices, the abort check at each resumption. */
  sliced<T>(job: SlicedJob<T>): MaybePromise<T> {
    this.slice.reset();
    return runSliced(job, this.slice, () => {
      this.checkAborted();
    });
  }

  /** Between async steps (pages, partitions, rows): a task yield and the abort check when due. */
  pace(): Promise<void> | null {
    if (!this.slice.due()) return null;
    return yieldTask().then(() => {
      this.checkAborted();
      this.slice.reset();
    });
  }

  /** Emits one output batch downstream (nothing for an empty one). */
  emit(batch: Batch): typeof DONE | typeof BLOCKED {
    const downstream = this.downstream;
    if (downstream === null) throw new TypeError("The join's probe has no downstream");
    const rows = batch.selection === null ? batch.length : batch.selection.count;
    if (rows === 0) return DONE;
    this.profile.rowsOut += rows;
    this.profile.batchesOut++;
    return downstream.push(batch) === "blocked" ? BLOCKED : DONE;
  }

  waitDownstream(): Promise<void> {
    return this.downstream?.ready() ?? Promise.resolve();
  }

  /** Room in the pool for one bounded step of `bytes` (see `Pool`). */
  need(bytes: number): Promise<void> {
    return this.io.need(bytes);
  }

  reset(store: ColumnStore): void {
    this.io.reset(store);
  }

  readPage(run: SpilledRun, index: number): Promise<{ bytes: Uint8Array; charged: number }> {
    return this.io.readPage(run, index);
  }

  releasePage(page: { charged: number }): void {
    this.io.releasePage(page);
  }

  decode(
    page: { bytes: Uint8Array; charged: number },
    types: readonly PhysicalType[],
  ): Promise<DecodedPage> {
    return this.io.decode(page, types);
  }

  async flush(
    staging: ColumnStore,
    run: SpilledRun,
    header: { run: number; partition: number },
  ): Promise<void> {
    await this.io.flush(staging, run, header);
    this.profile.spilledBytes = this.files.written;
  }

  async stage(
    staging: ColumnStore,
    run: SpilledRun,
    header: { run: number; partition: number },
    vectors: readonly Vector[],
    ids: ArrayLike<number> | null,
    from: number,
    count: number,
  ): Promise<void> {
    await this.io.stage(staging, run, header, vectors, ids, from, count);
    this.profile.spilledBytes = this.files.written;
  }
}

/** Per-probe-row state across the chunks of one partition (chunk mode). */
interface ChunkState {
  /** Bit per probe ordinal: matched (mark: TRUE). */
  readonly matched: Uint32Array;
  /** Bit per probe ordinal: mark NULL class. */
  readonly nulls: Uint32Array;
  /** Ordinal of the current batch's row 0. */
  base: number;
}

const getBit = (bits: Uint32Array, i: number) => ((bits[i >>> 5] ?? 0) & (1 << (i & 31))) !== 0;
const setBit = (bits: Uint32Array, i: number) => {
  bits[i >>> 5] = (bits[i >>> 5] ?? 0) | (1 << (i & 31));
};

interface Partitions {
  readonly fanout: number;
  readonly build: SpilledRun[];
  readonly probe: SpilledRun[];
  readonly buildStaging: Array<ColumnStore | null>;
  readonly probeStaging: Array<ColumnStore | null>;
  readonly resident: boolean[];
}

/**
 * One hash join over one set of build rows: the whole build (level 0), one spilled partition
 * (level ≥ 1), or one chunk of a partition (chunk mode, `partitionable` false).
 */
class Level {
  readonly store: ColumnStore;
  table: JoinTable | null = null;
  /** Right/full: 1 per build row some probe row matched. */
  matched: Uint8Array | null = null;
  parts: Partitions | null = null;
  // Mark joins: groups by C, T-null per build row, memoised group null counts.
  groups: JoinTable | null = null;
  groupScratch: KeyScratch | null = null;
  tNull: Uint8Array | null = null;
  groupNulls: Int32Array | null = null;
  #extraBytes = 0;
  /** What the table, match flags and mark groups are charged to (see `buildTable`). */
  #tableCharge: ByteCharge;
  #closed = false;

  constructor(
    readonly shared: Shared,
    readonly level: number,
    /** May this level partition 16 ways when it overflows (false: one partition, chunked). */
    readonly partitionable: boolean,
    /** What its store and table are charged to (the pool for a chunk of chunk mode). */
    readonly charge: ByteCharge = shared.charge,
  ) {
    this.store = new ColumnStore(shared.spec.buildTypes, charge);
    this.#tableCharge = charge;
  }

  /** Every build row this level saw (resident or spilled, dropped rows excluded). */
  get totalBuildRows(): number {
    const parts = this.parts;
    if (parts === null) return this.store.rows;
    let rows = 0;
    for (const run of parts.build) rows += run.rows;
    return rows;
  }

  // --- build ---------------------------------------------------------------------------------

  /** Appends build rows; spills (async) when the grant refuses. */
  addBuild(
    vectors: readonly Vector[],
    ids: ArrayLike<number> | null,
    from: number,
    count: number,
  ): MaybePromise<void> {
    this.shared.checkAborted();
    if (this.parts === null) {
      const done = this.store.append(vectors, ids, from, count);
      if (done === count) return;
      return this.#overflow().then(() => this.#route(vectors, ids, from + done, count - done));
    }
    return this.#route(vectors, ids, from, count);
  }

  /**
   * Adds the rows of a spill page read back (`bytes`, decoded as `columns`). When this level is
   * partitioned and the whole page belongs to one partition, the page itself is appended there:
   * no copy, so a page of one wide row costs one page of memory, not three (read, staged,
   * re-encoded).
   */
  async addPage(bytes: Uint8Array, rows: number, columns: readonly Vector[]): Promise<void> {
    this.shared.checkAborted();
    if (this.parts === null) {
      const done = this.store.append(columns, null, 0, rows);
      if (done === rows) return;
      await this.#overflow();
      if (done > 0) {
        await this.#route(columns, null, done, rows - done);
        return;
      }
    }
    if (await this.forwardPage("build", bytes, rows, columns)) return;
    await this.#route(columns, null, 0, rows);
  }

  /**
   * Appends a whole spill page to the one spilled partition all its rows route to (build side;
   * or probe side, when none of its rows is unmatchable). False when the rows spread, some must
   * be dropped or stay, or this level is not partitioned.
   */
  async forwardPage(
    side: "build" | "probe",
    bytes: Uint8Array,
    rows: number,
    columns: readonly Vector[],
  ): Promise<boolean> {
    const parts = this.parts;
    const shared = this.shared;
    if (parts === null || rows === 0 || rows > BATCH_ROWS) return false;
    const keyColumns = side === "build" ? shared.spec.buildKeys : shared.spec.probeKeys;
    const keys = keyColumns.map((c) => columnOf(columns, c));
    await extractAll(shared, keys, null, 0, rows, shared.scratch);
    const mask = this.unmatchableMask();
    for (let k = 0; k < rows; k++) if (((shared.scratch.nulls[k] ?? 0) & mask) !== 0) return false;
    const counts = this.groupByPartition(null, 0, rows, false);
    for (let p = 0; p < parts.fanout; p++) {
      if ((counts[p + 1] ?? 0) - (counts[p] ?? 0) !== rows) continue;
      if (side === "probe" && parts.resident[p] === true) return false;
      const run = side === "build" ? parts.build[p] : parts.probe[p];
      if (run === undefined) return false;
      await shared.files.append(run, bytes, rows);
      shared.profile.spilledBytes = shared.files.written;
      shared.profile.forwardedPages++;
      return true;
    }
    return false;
  }

  /** Converts to partitioned mode, spilling every resident row. */
  async #overflow(): Promise<void> {
    const shared = this.shared;
    const fanout = this.partitionable && shared.kind !== "mark" ? FANOUT : 1;
    this.parts = {
      fanout,
      build: Array.from({ length: fanout }, () => new SpilledRun()),
      probe: Array.from({ length: fanout }, () => new SpilledRun()),
      buildStaging: new Array<ColumnStore | null>(fanout).fill(null),
      probeStaging: new Array<ColumnStore | null>(fanout).fill(null),
      resident: new Array<boolean>(fanout).fill(false),
    };
    // Spill the resident rows chunk by chunk from the end, freeing each chunk as it goes.
    const store = this.store;
    for (let k = store.chunkCount - 1; k >= 0; k--) {
      const vectors = store.types.map((_, c) => store.chunk(c, k));
      const count = vectors[0]?.length ?? 0;
      await this.#route(vectors, null, 0, count);
      store.truncate(k * store.chunkRows);
    }
    this.#dropTable();
  }

  #staging(side: "build" | "probe", p: number): ColumnStore {
    const parts = this.parts;
    if (parts === null) throw new TypeError("Not partitioned");
    const list = side === "build" ? parts.buildStaging : parts.probeStaging;
    const existing = list[p];
    if (existing !== null && existing !== undefined) return existing;
    const types = side === "build" ? this.shared.spec.buildTypes : this.shared.spec.probeTypes;
    const store = new ColumnStore(types, this.shared.pool, {
      chunkRows: stagingRows(types),
      targetBytes: STAGING_TARGET_BYTES,
      varlenBytes: STAGING_VARLEN_BYTES,
    });
    list[p] = store;
    return store;
  }

  /** Keys whose null makes a row unmatchable: every strict key, or C for a mark join. */
  unmatchableMask(): number {
    const shared = this.shared;
    const strict = ~shared.layout.nullSafeMask;
    return shared.kind === "mark" ? strict & ((1 << shared.correlated) - 1) : strict;
  }

  /**
   * Groups rows `ids[from … from + n)` by partition into `shared.order` (counting sort); keys are
   * in `shared.scratch`. Rows in partition 255 are dropped. Returns the partition starts.
   */
  groupByPartition(
    ids: ArrayLike<number> | null,
    from: number,
    n: number,
    dropUnmatchable: boolean,
  ): Int32Array {
    const shared = this.shared;
    const parts = this.parts;
    if (parts === null) throw new TypeError("Not partitioned");
    const counts = shared.counts;
    counts.fill(0);
    const partition = shared.partition;
    const mask = this.unmatchableMask();
    for (let k = 0; k < n; k++) {
      const unmatchable = ((shared.scratch.nulls[k] ?? 0) & mask) !== 0;
      const p = unmatchable
        ? dropUnmatchable
          ? 255
          : 0
        : parts.fanout === 1
          ? 0
          : levelHash(shared.scratch.hashes[k] ?? 0, this.level) >>> (32 - FANOUT_BITS);
      partition[k] = p;
      if (p !== 255) counts[p + 1] = (counts[p + 1] ?? 0) + 1;
    }
    for (let p = 0; p < parts.fanout; p++) counts[p + 1] = (counts[p + 1] ?? 0) + (counts[p] ?? 0);
    const cursor = shared.heads; // reused as the scatter cursor (≤ FANOUT entries)
    for (let p = 0; p < parts.fanout; p++) cursor[p] = counts[p] ?? 0;
    for (let k = 0; k < n; k++) {
      const p = partition[k] ?? 0;
      if (p === 255) continue;
      const at = cursor[p] ?? 0;
      shared.order[at] = ids === null ? from + k : (ids[from + k] ?? 0);
      cursor[p] = at + 1;
    }
    return counts;
  }

  /** Build rows that can never match nor show (inner, left, semi, anti) are dropped when spilling. */
  #dropsUnmatchedBuild(): boolean {
    const kind = this.shared.kind;
    return (
      kind === "inner" || kind === "left" || kind === "semi" || kind === "anti" || kind === "mark"
    );
  }

  /** Routes build rows to partition staging stores, flushing full ones to spill pages. */
  async #route(
    vectors: readonly Vector[],
    ids: ArrayLike<number> | null,
    from: number,
    count: number,
  ): Promise<void> {
    const shared = this.shared;
    const parts = this.parts;
    if (parts === null) throw new TypeError("Not partitioned");
    const keys = shared.spec.buildKeys.map((c) => columnOf(vectors, c));
    for (let at = 0; at < count; at += BATCH_ROWS) {
      shared.checkAborted();
      const n = Math.min(BATCH_ROWS, count - at);
      await extractAll(shared, keys, ids, from + at, n, shared.scratch);
      const counts = this.groupByPartition(ids, from + at, n, this.#dropsUnmatchedBuild());
      // `order` is overwritten by the next group: copy each partition's ids out first.
      const order = shared.order.slice(0, n);
      for (let p = 0; p < parts.fanout; p++) {
        const start = counts[p] ?? 0;
        const end = counts[p + 1] ?? 0;
        const run = parts.build[p];
        if (end > start && run !== undefined)
          await shared.stage(
            this.#staging("build", p),
            run,
            { run: this.level, partition: p },
            vectors,
            order,
            start,
            end - start,
          );
      }
    }
  }

  /** Stages probe rows `rows[start … start + count)` of a spilled partition p. */
  stageProbe(
    p: number,
    columns: readonly Vector[],
    rows: ArrayLike<number>,
    start: number,
    count: number,
  ): Promise<void> {
    const parts = this.parts;
    const run = parts?.probe[p];
    if (run === undefined) throw new RangeError("No such partition");
    return this.shared.stage(
      this.#staging("probe", p),
      run,
      { run: 0x1000 | this.level, partition: p },
      columns,
      rows,
      start,
      count,
    );
  }

  /** Flushes and closes the staging stores of one side. */
  async flushStaging(side: "build" | "probe"): Promise<void> {
    const parts = this.parts;
    if (parts === null) return;
    const list = side === "build" ? parts.buildStaging : parts.probeStaging;
    for (let p = 0; p < parts.fanout; p++) {
      const staging = list[p];
      const run = side === "build" ? parts.build[p] : parts.probe[p];
      if (staging === null || staging === undefined || run === undefined) continue;
      await this.shared.flush(staging, run, {
        run: side === "build" ? this.level : 0x1000 | this.level,
        partition: p,
      });
      staging.close();
      list[p] = null;
    }
  }

  /** Ends the build: builds the table (spilling if refused), reloads partitions that fit. */
  endBuild(): MaybePromise<void> {
    this.shared.checkAborted();
    if (this.parts === null) {
      const built = this.shared.sliced(this.buildTableJob());
      const after = (ok: boolean): MaybePromise<void> =>
        ok ? undefined : this.#overflow().then(() => this.#endPartitionedBuild());
      return isPromise(built) ? built.then(after) : after(built);
    }
    return this.#endPartitionedBuild();
  }

  /** Builds table, match flags and mark groups over the resident store; false if refused. */
  buildTable(charge: ByteCharge = this.charge): boolean {
    return runToEnd(this.buildTableJob(charge, new Slice(Infinity)));
  }

  /** `buildTable` as a sliced job (R23.2). */
  *buildTableJob(
    charge: ByteCharge = this.charge,
    slice: Slice = this.shared.slice,
  ): SlicedJob<boolean> {
    const shared = this.shared;
    this.#dropTable();
    this.#tableCharge = charge;
    const table = yield* JoinTable.buildJob(
      this.store,
      shared.spec.buildKeys,
      shared.layout,
      this.level,
      this.#tableCharge,
      shared.scratch,
      slice,
      { direct: shared.context.direct !== false },
    );
    if (table === null) return false;
    this.table = table;
    shared.profile.table = table.kind;
    if (shared.kind === "right" || shared.kind === "full") {
      if (!this.#tableCharge.allocate(this.store.rows)) return this.#dropTable();
      this.#extraBytes += this.store.rows;
      this.matched = new Uint8Array(this.store.rows);
    }
    if (shared.kind === "mark" && !(yield* this.#buildGroups(slice))) return this.#dropTable();
    return true;
  }

  #dropTable(): false {
    this.table?.close();
    this.table = null;
    this.matched = null;
    this.groups?.close();
    this.groups = null;
    this.groupScratch = null;
    this.tNull = null;
    this.groupNulls = null;
    if (this.#extraBytes > 0) this.#tableCharge.free(this.#extraBytes);
    this.#extraBytes = 0;
    return false;
  }

  /** Mark join: T-null flags, the group table on C, a memo of group null counts. */
  *#buildGroups(slice: Slice): SlicedJob<boolean> {
    const shared = this.shared;
    const rows = this.store.rows;
    const bytes = rows * 5;
    if (!this.#tableCharge.allocate(bytes)) return false;
    this.#extraBytes += bytes;
    this.tNull = new Uint8Array(rows);
    this.groupNulls = new Int32Array(rows).fill(-1);
    const tMask = ~((1 << shared.correlated) - 1);
    for (let k = 0; k < this.store.chunkCount; k++) {
      const keys = chunkKeys(this.store, shared.spec.buildKeys, k);
      const count = this.store.chunk(0, k).length;
      // Only the null masks are needed here: no hashing or encoding of the keys.
      keyNulls(keys, null, 0, count, shared.scratch.nulls);
      const base = k * this.store.chunkRows;
      for (let i = 0; i < count; i++)
        if (((shared.scratch.nulls[i] ?? 0) & tMask) !== 0) this.tNull[base + i] = 1;
      if (slice.due()) yield;
    }
    if (shared.correlated === 0) return true;
    const cLayout = new KeyLayout(
      shared.layout.types.slice(0, shared.correlated),
      shared.layout.nullSafe.slice(0, shared.correlated),
    );
    const scratchBytes = KeyScratch.bytes(cLayout, BATCH_ROWS);
    if (!this.#tableCharge.allocate(scratchBytes)) return false;
    this.#extraBytes += scratchBytes;
    const scratch = new KeyScratch(cLayout, BATCH_ROWS, {
      allocate: () => true,
      free: () => undefined,
    });
    const groups = yield* JoinTable.buildJob(
      this.store,
      shared.spec.buildKeys.slice(0, shared.correlated),
      cLayout,
      this.level,
      this.#tableCharge,
      scratch,
      slice,
      { direct: false },
    );
    if (groups === null) return false;
    this.groups = groups;
    this.groupScratch = scratch;
    return true;
  }

  async #endPartitionedBuild(): Promise<void> {
    const parts = this.parts;
    if (parts === null) return;
    await this.flushStaging("build");
    // Hybrid: read partitions back while they fit, then build one table over them.
    const store = this.store;
    store.truncate(0);
    if (parts.fanout > 1)
      for (let p = 0; p < parts.fanout; p++) {
        const run = parts.build[p];
        if (run === undefined) continue;
        const before = store.rows;
        if (!(await this.#load(run, store))) {
          store.truncate(before);
          break;
        }
        parts.resident[p] = true;
      }
    while (!(await this.shared.sliced(this.buildTableJob()))) {
      // Drop the last resident partition until the table fits.
      let last = -1;
      for (let p = parts.fanout - 1; p >= 0 && last < 0; p--)
        if (parts.resident[p] === true) last = p;
      if (last < 0) {
        // Nothing resident: the (empty) table fits the pool's staging room, idle from now on.
        store.truncate(0);
        if (!this.buildTable(this.shared.pool))
          throw new RangeError("An empty join table could not be allocated");
        break;
      }
      parts.resident[last] = false;
      store.truncate(store.rows - (parts.build[last]?.rows ?? 0));
    }
    for (let p = 0; p < parts.fanout; p++) {
      const run = parts.build[p];
      if (parts.resident[p] === true && run !== undefined) {
        const rows = run.rows;
        await this.shared.files.release(run);
        run.rows = rows;
      }
    }
  }

  /** Reads a run's pages into `store`; false (partly loaded) when memory ran out. */
  async #load(run: SpilledRun, store: ColumnStore): Promise<boolean> {
    const shared = this.shared;
    for (let i = 0; i < run.pages.length; i++) {
      shared.checkAborted();
      const paced = shared.pace();
      if (paced !== null) await paced;
      const page = await shared.readPage(run, i);
      try {
        const { rows, columns } = await shared.decode(page, shared.spec.buildTypes);
        if (store.append(columns, null, 0, rows) < rows) return false;
      } finally {
        shared.releasePage(page);
      }
    }
    return true;
  }

  /** Frees the resident state (keeps spilled runs: the caller releases them). */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#dropTable();
    this.store.close();
    const parts = this.parts;
    if (parts !== null) {
      for (const staging of parts.buildStaging) staging?.close();
      for (const staging of parts.probeStaging) staging?.close();
    }
  }
}

/** Extracts keys, borrowing a progress loan when the `enc` encoder is refused memory. */
async function extractAll(
  shared: Shared,
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  scratch: KeyScratch,
): Promise<void> {
  // Bytes keys hash in byte steps (a task yield when the slice is due); words at once.
  const extract = async (): Promise<number> => {
    const done = runSliced(
      extractKeysJob(keys, ids, from, count, scratch, shared.work, shared.slice),
      shared.slice,
      () => {
        shared.checkAborted();
      },
    );
    return isPromise(done) ? await done : done;
  };
  if ((await extract()) === count) return;
  // The batch's keys are one bounded step (≤ 2,048 keys): room for what the encoder asked.
  await shared.need(2 * scratch.requiredBytes);
  if ((await extract()) !== count) throw new RangeError("Join keys could not be encoded");
}

/**
 * The probe machine: one probe batch against one level, resumable at every output push
 * (`BLOCKED`) and every spill write or loan (a Promise).
 *
 * Phases: 0 route (spilled partitions' rows go to spill) → 1 keys + lookup → 2 zero-copy
 * output, or 3 pair batches (→ 4 mark candidates) → 5 trailing rows (outer misses, semi, anti,
 * mark) → done.
 */
class Prober {
  batch: Batch | null = null;
  level: Level | null = null;
  chunk: ChunkState | null = null;
  #phase = 0;
  #pk = 0;
  #chain = 0;
  /** Pairs carried into the next pair batch (a byte-bounded output took fewer). */
  #carry = 0;
  #groupsLooked = false;
  #candidate = 0;
  #candidateCursor = 0;
  /** A resumable key job of the current phase (hashing, then lookups). */
  #job: SlicedJob<number> | null = null;
  #routeHashed = false;
  /**
   * The current batch's dictionary entries, hashed and looked up once each (charged for the
   * dictionary's size, released when the next batch starts or the join ends).
   */
  #hashMemo: Map<number, readonly [number, number]> | null = null;
  #headMemo: Map<number, number> | null = null;
  #memoBytes = 0;

  constructor(readonly shared: Shared) {}

  /** Frees the batch's dictionary memo. */
  release(): void {
    if (this.#memoBytes > 0) this.shared.charge.free(this.#memoBytes);
    this.#memoBytes = 0;
    this.#hashMemo = null;
    this.#headMemo = null;
    this.#job = null;
  }

  /** The memo for a raw dictionary probe key (null when not one, or not admitted). */
  #memo(vector: Vector | undefined): Map<number, readonly [number, number]> | null {
    if (this.shared.layout.family !== "raw" || vector === undefined || vector.form === "flat")
      return null;
    if (this.#hashMemo !== null) return this.#hashMemo;
    const entries = vector.form === "constant" ? 1 : vector.dictionary.length;
    const bytes = 64 * entries;
    if (!this.shared.charge.allocate(bytes)) return null;
    this.#memoBytes = bytes;
    this.#hashMemo = new Map();
    this.#headMemo = new Map();
    return this.#hashMemo;
  }

  #yieldStep(): Promise<void> {
    return yieldTask().then(() => {
      this.shared.slice.reset();
    });
  }

  start(batch: Batch, level: Level, chunk: ChunkState | null): void {
    this.release();
    this.#routeHashed = false;
    this.batch = batch;
    this.level = level;
    this.chunk = chunk;
    this.#phase = 0;
    this.#pk = 0;
    this.#chain = 0;
    this.#carry = 0;
    this.#groupsLooked = false;
    this.#candidate = 0;
    this.#candidateCursor = 0;
    this.shared.resident.count = 0;
    this.shared.rowState.fill(0, 0, batch.length);
  }

  /** Runs the batch until it is done, blocked downstream, or waiting (a Promise). */
  run(): Step {
    for (;;) {
      if (this.#phase === 9) return DONE;
      const step = this.#step();
      if (step !== DONE) return step;
      // Between steps (each at most one output batch): a task yield once the slice is due;
      // `continue` checks the signal when it resumes.
      if (this.#phase !== 9 && this.shared.slice.due())
        return yieldTask().then(() => {
          this.shared.slice.reset();
        });
    }
  }

  /** Continues after `step` until the batch is done, awaiting whatever it waits on. */
  async continue(step: Step): Promise<void> {
    for (let s = step; s !== DONE; s = this.run()) {
      if (s === BLOCKED) await this.shared.waitDownstream();
      else await s;
      this.shared.checkAborted();
    }
  }

  #step(): Step {
    const level = this.level;
    const batch = this.batch;
    if (level === null || batch === null) {
      this.#phase = 9;
      return DONE;
    }
    switch (this.#phase) {
      case 0:
        return this.#route(batch, level);
      case 1:
        return this.#lookup(batch, level);
      case 2:
        return this.#emitAligned(batch, level);
      case 3:
        return this.#emitPairs(batch, level);
      case 4:
        return this.#markCandidates(batch, level);
      case 5:
        return this.#emitTrailing(batch);
      default:
        this.#phase = 9;
        return DONE;
    }
  }

  /** Phase 0: rows of spilled partitions go to spill; the others become `resident`. */
  #route(batch: Batch, level: Level): Step {
    const shared = this.shared;
    const resident = shared.resident;
    const parts = level.parts;
    const selection = batch.selection;
    const active = selection === null ? batch.length : selection.count;
    this.#phase = 1;
    if (parts === null || parts.resident.every((resident) => resident)) {
      if (selection === null) for (let i = 0; i < active; i++) resident.rows[i] = i;
      else resident.rows.set(selection.rows.subarray(0, active));
      resident.count = active;
      return DONE;
    }
    const keys = shared.spec.probeKeys.map((c) => columnOf(batch.columns, c));
    const ids = selection?.rows ?? null;
    if (!this.#routeHashed) {
      let done: number;
      if (shared.layout.family !== "words") {
        // Bytes keys hash in byte steps; this phase resumes here after each yield.
        this.#job ??= extractKeysJob(
          keys,
          ids,
          0,
          active,
          shared.scratch,
          shared.work,
          shared.slice,
          this.#memo(keys[0]),
        );
        const step = this.#job.next();
        if (step.done !== true) {
          this.#phase = 0;
          return step.value ?? this.#yieldStep();
        }
        this.#job = null;
        done = step.value;
      } else done = extractKeys(keys, ids, 0, active, shared.scratch);
      if (done < active) {
        this.#phase = 0;
        return shared.need(2 * shared.scratch.requiredBytes);
      }
      this.#routeHashed = true;
    }
    const counts = level.groupByPartition(ids, 0, active, false);
    const mask = level.unmatchableMask();
    // Unmatchable rows and resident partitions stay; keep them ascending.
    for (let k = 0; k < active; k++) {
      const row = ids === null ? k : (ids[k] ?? 0);
      const p = shared.partition[k] ?? 0;
      if (((shared.scratch.nulls[k] ?? 0) & mask) !== 0 || parts.resident[p] === true)
        resident.rows[resident.count++] = row;
    }
    const spilled: Array<[number, number, number]> = [];
    for (let p = 0; p < parts.fanout; p++) {
      const start = counts[p] ?? 0;
      const end = counts[p + 1] ?? 0;
      if (end > start && parts.resident[p] !== true) spilled.push([p, start, end]);
    }
    if (spilled.length === 0) return DONE;
    // Unmatchable rows were grouped into partition 0: exclude them from its spill.
    const order = shared.order.slice(0, active);
    const nulls = shared.scratch.nulls;
    const unmatchable = new Set<number>();
    for (let k = 0; k < active; k++)
      if (((nulls[k] ?? 0) & mask) !== 0) unmatchable.add(ids === null ? k : (ids[k] ?? 0));
    return (async () => {
      for (const [p, start, end] of spilled) {
        let rows: ArrayLike<number> = order;
        let from = start;
        let count = end - start;
        if (p === 0 && unmatchable.size > 0) {
          const kept = Array.from(order.subarray(start, end)).filter(
            (row) => !unmatchable.has(row),
          );
          rows = kept;
          from = 0;
          count = kept.length;
        }
        if (count > 0) await level.stageProbe(p, batch.columns, rows, from, count);
      }
    })();
  }

  /** Phase 1: canonical keys and first matches of the resident rows. */
  #lookup(batch: Batch, level: Level): Step {
    const shared = this.shared;
    const resident = shared.resident;
    const table = level.table;
    const count = resident.count;
    if (count > 0) {
      const keys = shared.spec.probeKeys.map((c) => columnOf(batch.columns, c));
      const direct =
        table !== null && shared.kind !== "mark" && keys.length === 1
          ? table.lookupVector(columnOf(keys, 0), resident.rows, count, shared.heads)
          : -1;
      if (direct < 0 && shared.layout.family !== "words") {
        // Bytes keys: hashing, then lookups with resumable comparisons (review B7-p2c-2);
        // this phase resumes here after each yield.
        this.#job ??= this.#lookupJob(keys, resident.rows, count, table);
        const step = this.#job.next();
        if (step.done !== true) return step.value ?? this.#yieldStep();
        this.#job = null;
        if (step.value < 0) return shared.need(2 * shared.scratch.requiredBytes);
      } else if (direct < 0) {
        const hashed = table?.hashed ?? false;
        if (extractKeys(keys, resident.rows, 0, count, shared.scratch, hashed) < count)
          return shared.need(2 * shared.scratch.requiredBytes);
        if (table === null) shared.heads.fill(0, 0, count);
        else table.lookup(shared.scratch, count, shared.heads);
      }
    }
    shared.profile.probeRows += count;
    const kind = shared.kind;
    const residual = shared.spec.residual;
    if (kind === "mark") {
      if (!shared.conditioned) {
        for (let k = 0; k < count; k++)
          if ((shared.heads[k] ?? 0) !== 0) shared.rowState[resident.rows[k] ?? 0] = TRUE_CLASS;
        this.#phase = 4;
      } else this.#phase = 3;
      return DONE;
    }
    const unique = table?.unique ?? true;
    this.#phase = residual === null && (unique || kind === "semi" || kind === "anti") ? 2 : 3;
    return DONE;
  }

  /** Bytes keys of the resident rows: hashed, then looked up (−1: the encoder needs room). */
  *#lookupJob(
    keys: readonly Vector[],
    rows: Uint16Array,
    count: number,
    table: JoinTable | null,
  ): SlicedJob<number> {
    const shared = this.shared;
    const memo = this.#memo(keys[0]);
    const done = yield* extractKeysJob(
      keys,
      rows,
      0,
      count,
      shared.scratch,
      shared.work,
      shared.slice,
      memo,
    );
    if (done < count) return -1;
    if (table === null) shared.heads.fill(0, 0, count);
    else
      yield* table.lookupJob(
        shared.scratch,
        count,
        shared.heads,
        shared.work,
        shared.slice,
        this.#headMemo,
      );
    return count;
  }

  /** Phase 2: zero-copy output over the probe batch (no residual; unique keys or existence). */
  #emitAligned(batch: Batch, level: Level): Step {
    const shared = this.shared;
    const kind = shared.kind;
    const resident = shared.resident;
    const out = shared.out;
    const heads = shared.heads;
    const chunk = this.chunk;
    this.#phase = 9;
    out.count = 0;
    if (kind === "semi" || kind === "anti") {
      for (let k = 0; k < resident.count; k++) {
        const row = resident.rows[k] ?? 0;
        const hit = (heads[k] ?? 0) !== 0;
        if (chunk !== null) {
          const ordinal = chunk.base + row;
          if (hit && kind === "semi" && !getBit(chunk.matched, ordinal))
            out.rows[out.count++] = row;
          if (hit) setBit(chunk.matched, ordinal);
        } else if (hit === (kind === "semi")) out.rows[out.count++] = row;
      }
      if (chunk !== null && kind === "anti") return DONE;
      return shared.emit({ length: batch.length, columns: batch.columns, selection: out });
    }
    // inner, left, right, full with unique build keys.
    const aligned = shared.aligned;
    aligned.fill(-1, 0, batch.length);
    const outer = (kind === "left" || kind === "full") && chunk === null;
    const matched = level.matched;
    for (let k = 0; k < resident.count; k++) {
      const row = resident.rows[k] ?? 0;
      const head = heads[k] ?? 0;
      if (head !== 0) {
        aligned[row] = head - 1;
        if (matched !== null) matched[head - 1] = 1;
        if (chunk !== null) setBit(chunk.matched, chunk.base + row);
        out.rows[out.count++] = row;
      } else if (outer) out.rows[out.count++] = row;
    }
    // Matched-only outputs reuse probe key vectors; outer ones need nulls, so gather everything.
    const store = outer ? shared.buildOut : shared.buildPart;
    shared.reset(store);
    const columns = outer ? shared.buildColumns : shared.gatherColumns;
    const gathered = store.appendFrom(level.store, columns, aligned, 0, batch.length);
    if (gathered < batch.length) {
      // Too many build bytes for one aligned batch: emit as pairs instead.
      shared.reset(store);
      for (let k = 0; k < resident.count; k++) shared.rowState[resident.rows[k] ?? 0] = 0;
      this.#phase = 3;
      return DONE;
    }
    return shared.emit({
      length: batch.length,
      columns: [
        ...batch.columns,
        ...(outer ? store.vectors() : shared.matchedBuildColumns(batch.columns)),
      ],
      selection: out,
    });
  }

  /** Phase 3: expands matches into dense pair batches (exact matches, class TRUE). */
  #emitPairs(batch: Batch, level: Level): Step {
    const shared = this.shared;
    const resident = shared.resident;
    const heads = shared.heads;
    const next = level.table?.next ?? null;
    const pairProbe = shared.pairProbe;
    const pairBuild = shared.pairBuild;
    let n = this.#carry;
    while (n < BATCH_ROWS && this.#pk < resident.count) {
      if (this.#chain === 0) {
        const head = heads[this.#pk] ?? 0;
        if (head === 0) {
          this.#pk++;
          continue;
        }
        this.#chain = head;
      }
      pairProbe[n] = resident.rows[this.#pk] ?? 0;
      pairBuild[n] = this.#chain - 1;
      n++;
      this.#chain = next === null ? 0 : (next[this.#chain - 1] ?? 0);
      if (this.#chain === 0) this.#pk++;
    }
    if (n === 0) {
      this.#phase = shared.kind === "mark" ? 4 : 5;
      return DONE;
    }
    return this.#pairBatch(batch, level, n, TRUE_CLASS);
  }

  /**
   * Gathers `n` pairs, applies the residual, and emits them (inner/outer kinds) or folds them
   * into per-row state. A byte-bounded output may take fewer: the rest are carried.
   */
  #pairBatch(batch: Batch, level: Level, n: number, cls: number): Step {
    const shared = this.shared;
    const kind = shared.kind;
    const probeOut = shared.probeOut;
    const buildOut = shared.buildPart;
    shared.reset(probeOut);
    shared.reset(buildOut);
    let taken = probeOut.append(batch.columns, shared.pairProbe, 0, n);
    const built = buildOut.appendFrom(
      level.store,
      shared.gatherColumns,
      shared.pairBuild,
      0,
      taken,
    );
    if (built < taken) {
      probeOut.truncate(built);
      taken = built;
    }
    if (taken === 0) {
      // One output pair is one bounded step: room for exactly that row on both sides.
      this.#carry = n;
      return shared.need(
        probeOut.appendNeed(batch.columns, shared.pairProbe[0] ?? 0) +
          buildOut.appendFromNeed(level.store, shared.gatherColumns, shared.pairBuild[0] ?? 0),
      );
    }
    const probeColumns = probeOut.vectors();
    const pairs: Batch = {
      length: taken,
      columns: [...probeColumns, ...shared.matchedBuildColumns(probeColumns)],
      selection: null,
    };
    const residual = shared.spec.residual;
    // A mark's filter decides which pairs take part at all (ruling R20.5).
    const filter = kind === "mark" ? (shared.spec.filter ?? null) : null;
    const admitted = filter === null ? null : filter.filter(pairs, shared.admitted);
    const tested: Batch = admitted === null ? pairs : { ...pairs, selection: admitted };
    const passing = residual === null ? admitted : residual.filter(tested, shared.passing);
    const count = passing === null ? taken : passing.count;
    const matched = level.matched;
    const state = shared.rowState;
    for (let i = 0; i < count; i++) {
      const pair = passing === null ? i : (passing.rows[i] ?? 0);
      if (matched !== null) matched[shared.pairBuild[pair] ?? 0] = 1;
      const row = shared.pairProbe[pair] ?? 0;
      if ((state[row] ?? 0) < cls) state[row] = cls;
    }
    if (kind === "mark" && residual !== null) {
      // A NULL residual makes the pair's condition NULL, whether its keys were equal (`cls`
      // TRUE) or only not provably different (`cls` NULL): ruling R20.1.
      const unknown = residual.unknown?.(tested, shared.out);
      if (unknown === undefined)
        throw new TypeError("A mark join's residual must report NULL rows");
      for (let i = 0; i < unknown.count; i++) {
        const row = shared.pairProbe[unknown.rows[i] ?? 0] ?? 0;
        if ((state[row] ?? 0) < NULL_CLASS) state[row] = NULL_CLASS;
      }
    }
    // Carry the untaken pairs to the front for the next batch.
    if (taken < n) {
      shared.pairProbe.copyWithin(0, taken, n);
      shared.pairBuild.copyWithin(0, taken, n);
    }
    this.#carry = n - taken;
    if (kind === "inner" || kind === "left" || kind === "right" || kind === "full")
      return shared.emit(
        passing === null ? pairs : { length: taken, columns: pairs.columns, selection: passing },
      );
    return DONE;
  }

  /**
   * Phase 4 (mark): a row without TRUE is NULL when its group holds a row that is not provably
   * different; with a residual, such candidates are confirmed as pairs.
   */
  #markCandidates(batch: Batch, level: Level): Step {
    const shared = this.shared;
    const resident = shared.resident;
    const correlated = shared.correlated;
    const tests = shared.layout.types.length - correlated;
    const cMask = (1 << correlated) - 1;
    const store = level.store;
    const groupHeads = shared.aligned;
    if (!this.#groupsLooked) {
      this.#groupsLooked = true;
      if (level.groups !== null && level.groupScratch !== null && resident.count > 0) {
        const keys = shared.spec.probeKeys
          .slice(0, correlated)
          .map((c) => columnOf(batch.columns, c));
        extractKeys(keys, resident.rows, 0, resident.count, level.groupScratch);
        level.groups.lookup(level.groupScratch, resident.count, groupHeads);
      } else
        groupHeads.fill(
          store.rows > 0 && level.groups === null && correlated === 0 ? 1 : 0,
          0,
          resident.count,
        );
    }
    const groupNext = level.groups?.next ?? null;
    const implicit = correlated === 0;
    const nextInGroup = (row: number): number =>
      implicit ? (row + 1 < store.rows ? row + 2 : 0) : (groupNext?.[row] ?? 0);
    const tNull = level.tNull;
    const state = shared.rowState;
    let n = this.#carry;
    while (this.#candidate < resident.count && n < BATCH_ROWS) {
      const k = this.#candidate;
      const row = resident.rows[k] ?? 0;
      const probeNulls = shared.scratch.nulls[k] ?? 0;
      const groupHead = groupHeads[k] ?? 0;
      if (
        state[row] !== 0 ||
        groupHead === 0 ||
        (probeNulls & cMask & ~shared.layout.nullSafeMask) !== 0
      ) {
        this.#candidate++;
        this.#candidateCursor = 0;
        continue;
      }
      const probeTNull = (probeNulls & ~cMask) !== 0;
      if (!shared.conditioned && tests === 1) {
        if (probeTNull || groupNullCount(level, groupHead - 1, nextInGroup) > 0)
          state[row] = NULL_CLASS;
        this.#candidate++;
        continue;
      }
      let cursor = this.#candidateCursor === 0 ? groupHead : this.#candidateCursor;
      let decided = false;
      while (cursor !== 0 && n < BATCH_ROWS) {
        const buildRow = cursor - 1;
        cursor = nextInGroup(buildRow);
        if (!probeTNull && (tNull?.[buildRow] ?? 0) !== 1) continue;
        if (!compatible(shared, batch, row, store, buildRow)) continue;
        if (!shared.conditioned) {
          state[row] = NULL_CLASS;
          decided = true;
          break;
        }
        shared.pairProbe[n] = row;
        shared.pairBuild[n] = buildRow;
        n++;
      }
      if (!decided && cursor !== 0) {
        this.#candidateCursor = cursor;
        break;
      }
      this.#candidateCursor = 0;
      this.#candidate++;
    }
    if (n > 0) return this.#pairBatch(batch, level, n, NULL_CLASS);
    if (this.#candidate >= resident.count) this.#phase = 5;
    return DONE;
  }

  /** Phase 5: outer misses (left/full), matched (semi), misses (anti), every row (mark). */
  #emitTrailing(batch: Batch): Step {
    const shared = this.shared;
    const kind = shared.kind;
    const resident = shared.resident;
    const state = shared.rowState;
    const out = shared.out;
    const chunk = this.chunk;
    this.#phase = 9;
    out.count = 0;
    if (kind === "inner" || kind === "right") return DONE;
    if (chunk !== null) {
      // Chunk mode folds state; semi emits first matches, the final replay emits the rest.
      for (let k = 0; k < resident.count; k++) {
        const row = resident.rows[k] ?? 0;
        const ordinal = chunk.base + row;
        if (state[row] === TRUE_CLASS) {
          if (kind === "semi" && !getBit(chunk.matched, ordinal)) out.rows[out.count++] = row;
          setBit(chunk.matched, ordinal);
        } else if (state[row] === NULL_CLASS) setBit(chunk.nulls, ordinal);
      }
      if (kind !== "semi") return DONE;
      return shared.emit({ length: batch.length, columns: batch.columns, selection: out });
    }
    if (kind === "mark") {
      const mark = shared.markOut;
      const values = (mark.data as { values: Uint8Array }).values;
      let validity: Uint32Array | null = null;
      for (let k = 0; k < resident.count; k++) {
        const row = resident.rows[k] ?? 0;
        const s = state[row] ?? 0;
        values[row] = s === TRUE_CLASS ? 1 : 0;
        if (s === NULL_CLASS) {
          if (validity === null) {
            validity = shared.markValidity;
            validity.fill(0xffff_ffff);
          }
          validity[row >>> 5] = (validity[row >>> 5] ?? 0) & ~(1 << (row & 31));
        }
        out.rows[out.count++] = row;
      }
      mark.length = batch.length;
      mark.validity = validity;
      return shared.emit({
        length: batch.length,
        columns: [...batch.columns, mark],
        selection: out,
      });
    }
    const wantMatched = kind === "semi";
    for (let k = 0; k < resident.count; k++) {
      const row = resident.rows[k] ?? 0;
      if ((state[row] === TRUE_CLASS) === wantMatched) out.rows[out.count++] = row;
    }
    if (kind === "semi" || kind === "anti")
      return shared.emit({ length: batch.length, columns: batch.columns, selection: out });
    return shared.emit({
      length: batch.length,
      columns: [
        ...batch.columns,
        ...shared.buildNulls.map((vector) => withLength(vector, batch.length)),
      ],
      selection: out,
    });
  }
}

/** Item `c` of a column list; a missing one is a planning bug. */
function columnOf<T>(columns: readonly T[], c: number): T {
  const column = columns[c];
  if (column === undefined) throw new RangeError(`No column ${String(c)}`);
  return column;
}

function withLength(vector: Vector, length: number): Vector {
  vector.length = length;
  return vector;
}

/** Rows with a null tested key in the group starting at build row `head` (memoised). */
function groupNullCount(level: Level, head: number, nextInGroup: (row: number) => number): number {
  const memo = level.groupNulls;
  const cached = memo?.[head] ?? -1;
  if (cached >= 0) return cached;
  let count = 0;
  for (let cursor = head + 1; cursor !== 0;) {
    const row = cursor - 1;
    if ((level.tNull?.[row] ?? 0) === 1) count++;
    cursor = nextInGroup(row);
  }
  if (memo !== null) memo[head] = count;
  return count;
}

const FREE: ByteCharge = { allocate: () => true, free: () => undefined };
// Module scratch, sync-only (R23.1): `compatible` fills and reads it inside one synchronous call,
// so operators sharing a key type never see each other's rows.
const compatScratch = new WeakMap<
  PhysicalType,
  { probe: KeyScratch; build: KeyScratch; ids: Int32Array }
>();

/** Mark NULL test: every tested key is equal, or null on one side. */
function compatible(
  shared: Shared,
  batch: Batch,
  row: number,
  store: ColumnStore,
  buildRow: number,
): boolean {
  const probeKeys = shared.spec.probeKeys;
  for (let i = shared.correlated; i < probeKeys.length; i++) {
    const type = shared.layout.types[i];
    if (type === undefined) continue;
    let scratch = compatScratch.get(type);
    if (scratch === undefined) {
      const layout = new KeyLayout([type], [false]);
      scratch = {
        probe: new KeyScratch(layout, 1, FREE),
        build: new KeyScratch(layout, 1, FREE),
        ids: new Int32Array(1),
      };
      compatScratch.set(type, scratch);
    }
    scratch.ids[0] = row;
    extractKeys(
      [columnOf(batch.columns, probeKeys[i] ?? 0)],
      scratch.ids,
      0,
      1,
      scratch.probe,
      false,
    );
    const column = shared.spec.buildKeys[i] ?? 0;
    scratch.ids[0] = buildRow & store.chunkMask;
    extractKeys(
      [store.chunk(column, buildRow >>> store.chunkShift)],
      scratch.ids,
      0,
      1,
      scratch.build,
    );
    if ((scratch.probe.nulls[0] ?? 0) !== 0 || (scratch.build.nulls[0] ?? 0) !== 0) continue;
    if (!sameKey(scratch.probe, scratch.build)) return false;
  }
  return true;
}

function sameKey(a: KeyScratch, b: KeyScratch): boolean {
  const layout = a.layout;
  if (layout.family === "words") {
    for (let w = 0; w < layout.width; w++) if (a.words[w] !== b.words[w]) return false;
    return true;
  }
  if (layout.family === "raw") {
    const x = a.rawData;
    const y = b.rawData;
    if (x === null || y === null) return false;
    const i = a.slots[0] ?? 0;
    const j = b.slots[0] ?? 0;
    return bytesEqual(
      x.bytes,
      x.offsets[i] ?? 0,
      x.offsets[i + 1] ?? 0,
      y.bytes,
      y.offsets[j] ?? 0,
      y.offsets[j + 1] ?? 0,
    );
  }
  const x = a.encoded;
  const y = b.encoded;
  if (x === null || y === null) return false;
  return bytesEqual(
    x.writer.bytes,
    x.offsets[0] ?? 0,
    x.offsets[1] ?? 0,
    y.writer.bytes,
    y.offsets[0] ?? 0,
    y.offsets[1] ?? 0,
  );
}

/**
 * A hash join (see the module comment). `build` is the build pipeline's sink; `probe(down)` is
 * the probe pipeline's operator. The join owns its grant: closed when the probe finishes, or
 * when the join is aborted or fails.
 */
export class HashJoin {
  readonly spec: HashJoinSpec;
  readonly build: Operator;
  /** Runtime filters, set when the build finishes (none when the build spilled). */
  runtimeFilters: readonly JoinRuntimeFilter[] = [];
  /** The first failure closing spill files after the join ended (also `closed`'s rejection). */
  cleanupError: unknown = undefined;
  readonly #shared: Shared;
  readonly #prober: Prober;
  readonly #level: Level;
  #built = false;
  #pending: Promise<void> | null = null;
  #closed = false;

  constructor(spec: HashJoinSpec, context: HashJoinContext) {
    this.spec = spec;
    this.#shared = new Shared(spec, context);
    this.#prober = new Prober(this.#shared);
    this.#level = new Level(this.#shared, 0, true);
    this.build = new BuildSink(this);
  }

  get profile(): HashJoinProfile {
    const shared = this.#shared;
    shared.profile.peakHeldBytes = shared.charge.peak;
    shared.profile.spilledBytes = shared.files.written;
    shared.profile.loans = shared.pool.loans;
    shared.profile.largestLoan = shared.pool.largestLoan;
    return shared.profile;
  }

  /** The probe pipeline's operator, emitting into `downstream`. */
  probe(downstream: Operator): Operator {
    this.#shared.downstream = downstream;
    return new ProbeOperator(this);
  }

  #fail(error: unknown): never {
    this.abort(error);
    throw error;
  }

  /** @internal */
  pushBuild(batch: Batch, transfer?: Transfer): PushResult {
    if (this.#built) throw new TypeError("The join's build has finished");
    const shared = this.#shared;
    shared.checkAborted();
    const count = batch.selection === null ? batch.length : batch.selection.count;
    shared.profile.buildRows += count;
    const ids = batch.selection?.rows ?? null;
    let result: MaybePromise<void>;
    const keys = this.spec.buildKeys.map((c) => columnOf(batch.columns, c));
    // Only encoded keys are worked on per part at arrival (a partitioned build encodes them).
    const first =
      shared.layout.family === "enc" ? keyStepRows(keys, ids, 0, count, KEY_STEP_BYTES / 2) : count;
    if (first < count) result = this.#buildParts(batch.columns, ids, count, keys, first);
    else result = this.#level.addBuild(batch.columns, ids, 0, count);
    if (!isPromise(result)) {
      transfer?.release();
      return "ready";
    }
    this.#pending = result.then(
      () => transfer?.release(),
      (error: unknown) => {
        transfer?.release();
        this.#fail(error);
      },
    );
    return "blocked";
  }

  /**
   * A build batch with more key bytes than one step: added in parts of at most that (one row at
   * least), a task yield and the abort check between them (R23.2, review B7-p2c-1 #3).
   */
  async #buildParts(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    count: number,
    keys: readonly Vector[],
    first: number,
  ): Promise<void> {
    const shared = this.#shared;
    let at = 0;
    let n = first;
    for (;;) {
      const added = this.#level.addBuild(columns, ids, at, n);
      if (isPromise(added)) await added;
      at += n;
      if (at >= count) return;
      await yieldTask();
      shared.checkAborted();
      n = keyStepRows(keys, ids, at, count - at, KEY_STEP_BYTES / 2);
    }
  }

  /** @internal */
  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  /** @internal */
  finishBuild(): MaybePromise<void> {
    const shared = this.#shared;
    let result: MaybePromise<void>;
    try {
      shared.checkAborted();
      shared.slice.reset();
      result = this.#level.endBuild();
    } catch (error) {
      this.#fail(error);
    }
    // Then the runtime filters, sliced too; the probe may start once they are published.
    const publish = (): MaybePromise<void> => {
      const published = shared.sliced(this.#publishJob());
      if (!isPromise(published)) {
        this.#built = true;
        return;
      }
      return published.then(
        () => {
          this.#built = true;
        },
        (error: unknown) => this.#fail(error),
      );
    };
    if (!isPromise(result)) {
      try {
        return publish();
      } catch (error) {
        this.#fail(error);
      }
    }
    return result.then(publish, (error: unknown) => this.#fail(error));
  }

  *#publishJob(): SlicedJob<void> {
    const level = this.#level;
    if (level.parts !== null || this.spec.publishes.length === 0) return;
    const filters: JoinRuntimeFilter[] = [];
    for (const { id, key } of this.spec.publishes) {
      const column = this.spec.buildKeys[key];
      if (column === undefined) throw new RangeError("Runtime filter key out of range");
      const filter = yield* JoinRuntimeFilter.buildJob(
        id,
        level.store,
        column,
        this.spec.nullSafe[key] === true,
        this.#shared.charge,
        BATCH_ROWS,
        this.#shared.slice,
      );
      if (filter !== null) filters.push(filter);
    }
    this.runtimeFilters = filters;
  }

  /** @internal */
  pushProbe(batch: Batch): PushResult {
    if (!this.#built) throw new TypeError("The join's build has not finished");
    this.#shared.checkAborted();
    // Each probe push may run one slice before it yields (R23.2).
    this.#shared.slice.reset();
    // More key bytes than one step: the batch goes through in parts (raw keys count a
    // dictionary once, since they hash and look up each distinct entry once).
    const selection = batch.selection;
    const active = selection === null ? batch.length : selection.count;
    const keys = this.spec.probeKeys.map((c) => columnOf(batch.columns, c));
    // Encoded keys are encoded per part (by logical bytes); raw and word keys spend the work
    // budget inside the probe itself (hashing and comparing resume across yields).
    const raw = this.#shared.layout.family !== "enc";
    const ids = selection?.rows ?? null;
    const first = raw ? active : keyStepRows(keys, ids, 0, active, KEY_STEP_BYTES / 2);
    if (first < active) {
      this.#pending = this.#probeParts(batch, keys, active, first, raw).catch((error: unknown) =>
        this.#fail(error),
      );
      return "blocked";
    }
    this.#prober.start(batch, this.#level, null);
    let step: Step;
    try {
      step = this.#prober.run();
    } catch (error) {
      this.#fail(error);
    }
    if (step === DONE) return "ready";
    this.#pending = this.#prober.continue(step).catch((error: unknown) => this.#fail(error));
    return "blocked";
  }

  /** A probe batch in parts of at most one step of key bytes (ascending sub-selections). */
  async #probeParts(
    batch: Batch,
    keys: readonly Vector[],
    active: number,
    first: number,
    raw: boolean,
  ): Promise<void> {
    const shared = this.#shared;
    const ids = batch.selection?.rows ?? null;
    let at = 0;
    let n = first;
    for (;;) {
      const rows = new Uint16Array(n);
      for (let k = 0; k < n; k++) rows[k] = ids === null ? at + k : (ids[at + k] ?? 0);
      this.#prober.start(
        { length: batch.length, columns: batch.columns, selection: { rows, count: n } },
        this.#level,
        null,
      );
      await this.#prober.continue(this.#prober.run());
      at += n;
      if (at >= active) return;
      await yieldTask();
      shared.checkAborted();
      shared.slice.reset();
      n = keyStepRows(keys, ids, at, active - at, KEY_STEP_BYTES / 2, !raw);
    }
  }

  /** @internal */
  finishProbe(): MaybePromise<void> {
    const shared = this.#shared;
    const level = this.#level;
    const downstream = shared.downstream;
    if (downstream === null) throw new TypeError("The join's probe has no downstream");
    try {
      shared.checkAborted();
    } catch (error) {
      this.#fail(error);
    }
    shared.slice.reset();
    if (level.parts === null && level.matched === null) {
      this.#release();
      return downstream.finish();
    }
    return this.#finishLevel(level).then(
      async () => {
        this.#release();
        // A spilled join ends only once its spill space is given back (or fails with why not).
        await this.closed;
        await downstream.finish();
      },
      (error: unknown) => this.#fail(error),
    );
  }

  /** Emits `level`'s unmatched build rows (right/full) and joins its spilled partitions. */
  async #finishLevel(level: Level): Promise<void> {
    const shared = this.#shared;
    await level.flushStaging("probe");
    await this.#emitUnmatchedBuild(level);
    const parts = level.parts;
    const total = level.totalBuildRows;
    level.close();
    if (parts === null) return;
    for (let p = 0; p < parts.fanout; p++) {
      shared.checkAborted();
      const paced = shared.pace();
      if (paced !== null) await paced;
      const build = parts.build[p];
      const probe = parts.probe[p];
      if (build === undefined || probe === undefined || parts.resident[p] === true) continue;
      if (this.#partitionMatters(build, probe)) {
        shared.profile.spillPasses++;
        const heavy =
          parts.fanout === 1 ||
          (build.rows > 1 && build.rows === total) ||
          level.level + 1 > MAX_LEVEL;
        if (heavy) await this.#chunked(level.level + 1, build, probe);
        else await this.#subJoin(level.level + 1, build, probe);
      }
      await shared.files.release(build);
      await shared.files.release(probe);
    }
  }

  #partitionMatters(build: SpilledRun, probe: SpilledRun): boolean {
    const kind = this.spec.kind;
    if (kind === "inner" || kind === "semi") return build.rows > 0 && probe.rows > 0;
    if (kind === "right") return build.rows > 0;
    if (kind === "full") return build.rows > 0 || probe.rows > 0;
    return probe.rows > 0;
  }

  /** Joins one spilled partition pair one level down (it may partition again). */
  async #subJoin(level: number, build: SpilledRun, probe: SpilledRun): Promise<void> {
    const shared = this.#shared;
    const sub = new Level(shared, level, true);
    try {
      for (let i = 0; i < build.pages.length; i++) {
        const paced = shared.pace();
        if (paced !== null) await paced;
        const page = await shared.readPage(build, i);
        try {
          const { rows, columns } = await shared.decode(page, this.spec.buildTypes);
          await sub.addPage(page.bytes, rows, columns);
        } finally {
          shared.releasePage(page);
        }
      }
      await sub.endBuild();
      await this.#replay(probe, sub, null);
      await this.#finishLevel(sub);
    } finally {
      sub.close();
    }
  }

  /** Streams a probe run's pages through `level`. */
  async #replay(probe: SpilledRun, level: Level, chunk: ChunkState | null): Promise<void> {
    const shared = this.#shared;
    let ordinal = 0;
    for (let i = 0; i < probe.pages.length; i++) {
      const paced = shared.pace();
      if (paced !== null) await paced;
      const page = await shared.readPage(probe, i);
      try {
        const { rows, columns } = await shared.decode(page, this.spec.probeTypes);
        if (rows > BATCH_ROWS) throw new RangeError("A probe spill page holds more than a batch");
        if (chunk === null && (await level.forwardPage("probe", page.bytes, rows, columns))) {
          ordinal += rows;
          continue;
        }
        if (chunk !== null) chunk.base = ordinal;
        this.#prober.start({ length: rows, columns, selection: null }, level, chunk);
        await this.#prober.continue(this.#prober.run());
        ordinal += rows;
      } finally {
        shared.releasePage(page);
      }
    }
  }

  /**
   * Chunk mode: tables over as many build pages as fit, the whole probe run replayed against
   * each, per-probe-row state kept across chunks, then a final replay for unmatched/mark rows.
   */
  async #chunked(level: number, build: SpilledRun, probe: SpilledRun): Promise<void> {
    const shared = this.#shared;
    const kind = this.spec.kind;
    const words = Math.ceil(probe.rows / 32) + 1;
    const stateBytes = words * 8;
    // The state lives across every chunk: from the pool's idle staging room, else the grant.
    const stateCharge: ByteCharge = shared.pool.allocate(stateBytes) ? shared.pool : shared.charge;
    if (stateCharge === shared.charge && !shared.charge.allocate(stateBytes))
      throw new RangeError("Chunk state for a spilled partition could not be allocated");
    const chunk: ChunkState = {
      matched: new Uint32Array(words),
      nulls: new Uint32Array(words),
      base: 0,
    };
    // The largest probe page: one bounded step of the replay.
    let probePage = 0;
    for (const ref of probe.pages) probePage = Math.max(probePage, ref.length);
    try {
      // Rows load in row units from a cursor (page, row): a page is never one value.
      let page = 0;
      let row = 0;
      while (page < build.pages.length) {
        shared.checkAborted();
        shared.profile.chunkPasses++;
        // A chunk fills the pool's idle room but leaves the replay's steps theirs: one page in
        // flight and the output chunks' growth.
        const keep = 2 * POOL_PAGE_BYTES + probePage + 3 * STAGING_TARGET_BYTES;
        const chunkCharge: ByteCharge = {
          allocate: (bytes) => shared.pool.allocate(bytes, keep),
          free: (bytes) => {
            shared.pool.free(bytes);
          },
        };
        const sub = new Level(shared, level, false, chunkCharge);
        const loaded: Array<{ page: number; from: number; count: number }> = [];
        try {
          let reserved = false;
          while (page < build.pages.length) {
            const paced = shared.pace();
            if (paced !== null) await paced;
            const bytes = await shared.readPage(build, page);
            let full = false;
            let retry = 0;
            try {
              const { rows, columns } = await shared.decode(bytes, this.spec.buildTypes);
              let taken = sub.store.append(columns, null, row, rows - row);
              if (taken === 0 && sub.store.rows === 0) {
                if (reserved) throw new RangeError("A build row could not be allocated");
                // An empty chunk that cannot take one row. The step is this page, the row, its
                // table and the headroom the chunk leaves the replay: give the page back, make
                // room for all of it at once, and read the page again.
                retry =
                  bytes.charged +
                  sub.store.appendNeed(columns, row) +
                  JoinTable.estimateBytes(shared.layout, 1) +
                  keep;
                taken = 0;
              }
              if (taken > 0) loaded.push({ page, from: row, count: taken });
              row += taken;
              if (retry > 0) full = false;
              else if (row >= rows) {
                page++;
                row = 0;
              } else full = true;
            } finally {
              shared.releasePage(bytes);
            }
            if (retry > 0) {
              await shared.need(retry);
              reserved = true;
              continue;
            }
            if (full) break;
          }
          // The chunk's table; give rows back (halving) while it does not fit.
          while (!(await shared.sliced(sub.buildTableJob()))) {
            if (sub.store.rows <= 1)
              throw new RangeError("A join chunk table could not be allocated for one row");
            let drop = sub.store.rows >> 1;
            sub.store.truncate(sub.store.rows - drop);
            while (drop > 0) {
              const last = loaded[loaded.length - 1];
              if (last === undefined) break;
              const take = Math.min(drop, last.count);
              last.count -= take;
              drop -= take;
              page = last.page;
              row = last.from + last.count;
              if (last.count === 0) loaded.pop();
            }
          }
          if (probe.rows > 0) await this.#replay(probe, sub, chunk);
          await this.#emitUnmatchedBuild(sub);
        } finally {
          sub.close();
          shared.reset(shared.probeOut);
          shared.reset(shared.buildOut);
          shared.reset(shared.buildPart);
        }
      }
      if (kind === "left" || kind === "full" || kind === "anti" || kind === "mark")
        await this.#emitFromState(probe, chunk);
    } finally {
      stateCharge.free(stateBytes);
    }
  }

  /** Final replay of chunk mode: probe rows by their folded state. */
  async #emitFromState(probe: SpilledRun, chunk: ChunkState): Promise<void> {
    const shared = this.#shared;
    const kind = this.spec.kind;
    let ordinal = 0;
    for (let i = 0; i < probe.pages.length; i++) {
      const paced = shared.pace();
      if (paced !== null) await paced;
      const page = await shared.readPage(probe, i);
      try {
        const { rows, columns } = await shared.decode(page, this.spec.probeTypes);
        const out = shared.out;
        out.count = 0;
        let batch: Batch;
        if (kind === "mark") {
          const mark = shared.markOut;
          const values = (mark.data as { values: Uint8Array }).values;
          let validity: Uint32Array | null = null;
          for (let r = 0; r < rows; r++) {
            const isTrue = getBit(chunk.matched, ordinal + r);
            values[r] = isTrue ? 1 : 0;
            if (!isTrue && getBit(chunk.nulls, ordinal + r)) {
              if (validity === null) {
                validity = shared.markValidity;
                validity.fill(0xffff_ffff);
              }
              validity[r >>> 5] = (validity[r >>> 5] ?? 0) & ~(1 << (r & 31));
            }
          }
          mark.length = rows;
          mark.validity = validity;
          batch = { length: rows, columns: [...columns, mark], selection: null };
        } else {
          for (let r = 0; r < rows; r++)
            if (!getBit(chunk.matched, ordinal + r)) out.rows[out.count++] = r;
          batch =
            kind === "anti"
              ? { length: rows, columns, selection: out }
              : {
                  length: rows,
                  columns: [
                    ...columns,
                    ...shared.buildNulls.map((vector) => withLength(vector, rows)),
                  ],
                  selection: out,
                };
        }
        if (shared.emit(batch) === BLOCKED) {
          await shared.waitDownstream();
          shared.checkAborted();
        }
        ordinal += rows;
      } finally {
        shared.releasePage(page);
      }
    }
  }

  /** Right/full: emits the build rows of `level` that no probe row matched. */
  async #emitUnmatchedBuild(level: Level): Promise<void> {
    const matched = level.matched;
    if (matched === null) return;
    const shared = this.#shared;
    const store = level.store;
    const ids = shared.pairBuild;
    let n = 0;
    const flush = async () => {
      let at = 0;
      while (at < n) {
        shared.reset(shared.buildOut);
        const taken = shared.buildOut.appendFrom(store, shared.buildColumns, ids, at, n - at);
        if (taken === 0) {
          // One unmatched build row is one bounded step.
          await shared.need(
            shared.buildOut.appendFromNeed(store, shared.buildColumns, ids[at] ?? 0),
          );
          continue;
        }
        const columns = [
          ...shared.probeNulls.map((vector) => withLength(vector, taken)),
          ...shared.buildOut.vectors(),
        ];
        if (shared.emit({ length: taken, columns, selection: null }) === BLOCKED) {
          await shared.waitDownstream();
          shared.checkAborted();
        }
        at += taken;
        const paced = shared.pace();
        if (paced !== null) await paced;
      }
      n = 0;
    };
    for (let row = 0; row < store.rows; row++) {
      if ((row & (SLICE_ROWS - 1)) === SLICE_ROWS - 1) {
        const paced = shared.pace();
        if (paced !== null) await paced;
      }
      if (matched[row] === 1) continue;
      ids[n++] = row;
      if (n === BATCH_ROWS) await flush();
    }
    if (n > 0) await flush();
  }

  #release(): void {
    if (this.#closed) return;
    this.#closed = true;
    const shared = this.#shared;
    this.#prober.release();
    this.#level.close();
    for (const filter of this.runtimeFilters) filter.close();
    shared.probeOut.close();
    shared.buildOut.close();
    shared.buildPart.close();
    shared.pages.close();
    shared.pool.close();
    // Spill files close asynchronously; a failure is kept and surfaces through `closed`.
    const closing = shared.files.closeAll();
    closing.catch((error: unknown) => {
      this.cleanupError ??= error;
    });
    shared.profile.peakHeldBytes = shared.charge.peak;
    shared.context.grant.close();
  }

  /**
   * Settles when the join's spill files are all closed (including any whose creation was in
   * flight when it ended); rejects with the first close failure. Resolved before the join ends.
   */
  get closed(): Promise<void> {
    return this.#shared.files.closed ?? Promise.resolve();
  }

  /** Releases everything (grant, spill files, tables); idempotent. */
  abort(reason: unknown): void {
    const shared = this.#shared;
    if (shared.aborted === undefined) shared.aborted = reason ?? new Error("Aborted");
    this.#release();
  }
}

class BuildSink implements Operator {
  constructor(readonly join: HashJoin) {}
  push(batch: Batch, transfer?: Transfer): PushResult {
    return this.join.pushBuild(batch, transfer);
  }
  ready(): Promise<void> {
    return this.join.ready();
  }
  finish(): MaybePromise<void> {
    return this.join.finishBuild();
  }
  abort(reason: unknown): void {
    this.join.abort(reason);
  }
}

class ProbeOperator implements Operator {
  constructor(readonly join: HashJoin) {}
  push(batch: Batch, transfer?: Transfer): PushResult {
    if (transfer !== undefined) throw new TypeError("The join's probe borrows its input");
    return this.join.pushProbe(batch);
  }
  ready(): Promise<void> {
    return this.join.ready();
  }
  finish(): MaybePromise<void> {
    return this.join.finishProbe();
  }
  abort(reason: unknown): void {
    this.join.abort(reason);
  }
}
