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
 * - No keys (ruling R24.2): every kind but mark may have empty `probeKeys` / `buildKeys`, the
 *   ON condition being the residual alone; such a join runs over one constant key (`Keyless`).
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
 * usage fits the ordinary pool again. Chunk loading and probe replay reserve the larger of
 * their phase peaks. A single-row probe stays borrowed during pair output; completed chunks
 * shrink output arenas before starting another step. This includes two-sided 850 KiB values
 * at the minimum grant; a value whose required bounded step exceeds the reserve is still a
 * `ValueTooLargeError` (the governor's size error).
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
  equalJob,
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
import { JoinCheckpoint } from "./join-checkpoint.js";
import { RecordCheckpoint } from "./record-checkpoint.js";

type MaybePromise<T> = T | Promise<T>;

/** The internal flag is a view when checkpointing resident matches, a constant otherwise. */
function matchVector(length: number, values?: Uint8Array): Vector {
  const type: PhysicalType = { kind: "bool" };
  return values
    ? { form: "flat", type, length, validity: null, data: { layout: "bool", values } }
    : {
        form: "constant",
        type,
        length,
        value: {
          form: "flat",
          type,
          length: 1,
          validity: null,
          data: { layout: "bool", values: new Uint8Array(1) },
        },
      };
}

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
export function hashJoinMinimumBytes(declared: HashJoinSpec): number {
  // A keyless join's is its keyed form's (its constant key columns).
  const spec =
    declared.probeKeys.length === 0 && declared.kind !== "mark"
      ? new Keyless(declared).spec
      : declared;
  const keyTypes = spec.probeKeys
    .map((c) => spec.probeTypes[c])
    .filter((type): type is PhysicalType => type !== undefined);
  const layout = new KeyLayout(keyTypes, spec.nullSafe);
  return KeyScratch.bytes(layout, BATCH_ROWS) + SCRATCH_BYTES + poolBytes(spec);
}

/** Validates immutable join metadata without allocating a hash table, pool or batch buffer. */
function checkJoinSpec(spec: HashJoinSpec): { layout: KeyLayout; correlated: number } {
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
  const correlated = spec.kind === "mark" ? (spec.correlated ?? 0) : spec.probeKeys.length;
  if (spec.kind === "mark") {
    if (correlated < 0 || correlated >= keyTypes.length)
      throw new RangeError("A mark join tests at least one key");
    for (let i = correlated; i < keyTypes.length; i++)
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
  const layout = new KeyLayout(keyTypes, spec.nullSafe);
  return { layout, correlated };
}

export function validateHashJoinSpec(declared: HashJoinSpec): void {
  const spec =
    declared.probeKeys.length === 0 && declared.buildKeys.length === 0
      ? new Keyless(declared).spec
      : declared;
  checkJoinSpec(spec);
}

/** State shared by every level of one join: spec, scratch, outputs, spill, downstream. */
class Shared {
  /** Restored RIGHT/FULL records carry their match bit through repartitioning. */
  readonly buildTypes: readonly PhysicalType[];
  readonly initialMatchColumn: number;
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
  /** Mark joins: scratch for each tested key's compatibility test (index: key position). */
  readonly compat: Array<CompatScratch | undefined> = [];

  constructor(
    readonly spec: HashJoinSpec,
    readonly context: HashJoinContext,
    restoreMatches = false,
  ) {
    this.initialMatchColumn = restoreMatches ? spec.buildTypes.length : -1;
    this.buildTypes = restoreMatches ? [...spec.buildTypes, { kind: "bool" }] : spec.buildTypes;
    this.kind = spec.kind;
    const checked = checkJoinSpec(spec);
    this.correlated = checked.correlated;
    this.layout = checked.layout;
    this.conditioned = spec.residual !== null || (spec.filter ?? null) !== null;
    this.charge = new Charge(context.grant);
    if (!this.charge.allocate(SCRATCH_BYTES))
      throw new RangeError("A join's minimum must be admitted before it is built");
    this.pool = new Pool(this.charge, poolBytes(spec));
    this.files = new SpillFiles(context.spill, context.queryId, this.charge);
    this.pages = new PageWriter(this.pool, 16 * 1024);
    this.io = new SpillIo(
      this.pool,
      this.files,
      this.pages,
      context,
      () => {
        this.checkAborted();
      },
      () => this.pace(),
    );
    this.scratch = new KeyScratch(this.layout, BATCH_ROWS, this.charge, this.pool);
    if (spec.kind === "mark")
      for (let i = this.correlated; i < this.layout.types.length; i++) {
        const type = this.layout.types[i];
        if (type === undefined) continue;
        const layout = new KeyLayout([type], [false]);
        this.compat[i] = {
          probe: new KeyScratch(layout, 1, this.pool),
          build: new KeyScratch(layout, 1, this.pool),
          ids: new Int32Array(1),
        };
      }
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

  /**
   * Runs a job in the current slice: task yields between slices, the abort check at each
   * resumption. Its deadline is the worker's task clock (R23.5: nothing here starts a slice; a
   * task begins only at a real yield or an owner entry point).
   */
  sliced<T>(job: SlicedJob<T>): MaybePromise<T> {
    return runSliced(job, this.slice, () => {
      this.checkAborted();
    });
  }

  /** Between async steps (pages, partitions, rows): a task yield and the abort check when due. */
  pace(): Promise<void> | null {
    if (!this.slice.due()) return null;
    return yieldTask().then(() => {
      this.checkAborted();
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

  /** Routing and lookup keep hashes/null masks, never the encoded byte buffer.
   * Return that loan before asking the staging/output step for another resource. */
  resetKeys(): void {
    if (!this.pool.owes) return;
    this.scratch.encoded?.writer.reset();
    this.scratch.encodedParts?.writer.reset();
    this.pool.settle();
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
    this.store = new ColumnStore(shared.buildTypes, charge);
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
    try {
      const keyColumns = side === "build" ? shared.spec.buildKeys : shared.spec.probeKeys;
      const keys = keyColumns.map((c) => columnOf(columns, c));
      await extractAll(shared, keys, null, 0, rows, shared.scratch);
      const mask = this.unmatchableMask();
      for (let k = 0; k < rows; k++)
        if (((shared.scratch.nulls[k] ?? 0) & mask) !== 0) return false;
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
    } finally {
      shared.resetKeys();
    }
  }

  /** Converts to partitioned mode, spilling every resident row. */
  async spill(): Promise<void> {
    if (this.parts === null) await this.#overflow();
  }

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
    const types = side === "build" ? this.shared.buildTypes : this.shared.spec.probeTypes;
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
      shared.resetKeys();
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
    if (shared.kind === "mark") shared.resetKeys();
    shared.profile.table = table.kind;
    if (shared.kind === "right" || shared.kind === "full") {
      if (!this.#tableCharge.allocate(this.store.rows)) return this.#dropTable();
      this.#extraBytes += this.store.rows;
      this.matched = new Uint8Array(this.store.rows);
      if (shared.initialMatchColumn >= 0)
        for (let k = 0; k < this.store.chunkCount; k++) {
          const column = this.store.chunk(shared.initialMatchColumn, k);
          const values = (column.data as { values: Uint8Array }).values;
          this.matched.set(values.subarray(0, column.length), k * this.store.chunkRows);
          if (slice.due()) yield;
        }
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
    this.groupScratch?.encoded?.close();
    this.groupScratch?.encodedParts?.close();
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
    const scratch = new KeyScratch(
      cLayout,
      BATCH_ROWS,
      {
        // Fixed arrays were prepaid above; variable encoders use real pool credit.
        allocate: () => true,
        free: () => undefined,
      },
      shared.pool,
    );
    this.groupScratch = scratch;
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
    if (shared.pool.owes) {
      scratch.encoded?.writer.reset();
      scratch.encodedParts?.writer.reset();
    }
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
        const { rows, columns } = await shared.decode(page, shared.buildTypes);
        shared.checkAborted();
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
    // The yield begins a task: it stamps the worker's clock (R23.5).
    return yieldTask();
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
      if (this.#phase !== 9 && this.shared.slice.due()) return yieldTask();
    }
  }

  /** Continues after `step` until the batch is done, awaiting whatever it waits on. */
  async continue(step: Step): Promise<void> {
    for (let s = step; s !== DONE; s = this.run()) {
      if (s === BLOCKED) {
        await this.shared.waitDownstream();
        this.shared.checkAborted();
        // Readiness may be a resolved promise in the same task. Honour its remaining
        // deadline before doing more probe work; only a real yield can restart it.
        const pause = this.shared.pace();
        if (pause !== null) await pause;
      } else await s;
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
    shared.resetKeys();
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
    shared.resetKeys();
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
        ...(outer
          ? store.vectors().slice(0, shared.spec.buildTypes.length)
          : shared.matchedBuildColumns(batch.columns)),
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
    // One physical probe row already has the required dense layout; keep its page borrowed
    // through emission instead of holding a second copy of an oversized value.
    const borrowProbe = n === 1 && batch.length === 1 && shared.pairProbe[0] === 0;
    let taken = borrowProbe ? 1 : probeOut.append(batch.columns, shared.pairProbe, 0, n);
    const borrowBuild = taken === 1 && level.store.rows === 1 && shared.pairBuild[0] === 0;
    const built = borrowBuild
      ? 1
      : buildOut.appendFrom(level.store, shared.gatherColumns, shared.pairBuild, 0, taken);
    if (built < taken) {
      probeOut.truncate(built);
      taken = built;
    }
    if (taken === 0) {
      // One output pair is one bounded step: room for exactly that row on both sides.
      this.#carry = n;
      return shared.need(
        (borrowProbe ? 0 : probeOut.appendNeed(batch.columns, shared.pairProbe[0] ?? 0)) +
          buildOut.appendFromNeed(level.store, shared.gatherColumns, shared.pairBuild[0] ?? 0),
      );
    }
    const probeColumns = borrowProbe ? batch.columns : probeOut.vectors();
    const pairs: Batch = {
      length: taken,
      columns: [
        ...probeColumns,
        ...(borrowBuild
          ? shared.buildColumns.map((c) => level.store.chunk(c, 0))
          : shared.matchedBuildColumns(probeColumns)),
      ],
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
   * different; with a residual, such candidates are confirmed as pairs. A resumable job (review
   * B7-p2c-3 #2): the group lookup hashes and compares through the key jobs, every candidate
   * visited spends key work (a deadline checkpoint per candidate, accepted or rejected), and
   * the compatibility test compares tested keys through `equalJob`.
   */
  #markCandidates(batch: Batch, level: Level): Step {
    this.#job ??= this.#markCandidatesJob(batch, level);
    const step = this.#job.next();
    if (step.done !== true) return step.value ?? this.#yieldStep();
    this.#job = null;
    const n = step.value;
    if (n > 0) return this.#pairBatch(batch, level, n, NULL_CLASS);
    if (this.#candidate >= this.shared.resident.count) this.#phase = 5;
    return DONE;
  }

  /** The pairs (or NULL marks) of phase 4 up to one pair batch: returns the pairs found. */
  *#markCandidatesJob(batch: Batch, level: Level): SlicedJob<number> {
    const shared = this.shared;
    const work = shared.work;
    const slice = shared.slice;
    const resident = shared.resident;
    const correlated = shared.correlated;
    const tests = shared.layout.types.length - correlated;
    const cMask = (1 << correlated) - 1;
    const store = level.store;
    const groupHeads = shared.aligned;
    if (!this.#groupsLooked) {
      if (level.groups !== null && level.groupScratch !== null && resident.count > 0) {
        const keys = shared.spec.probeKeys
          .slice(0, correlated)
          .map((c) => columnOf(batch.columns, c));
        const done = yield* extractKeysJob(
          keys,
          resident.rows,
          0,
          resident.count,
          level.groupScratch,
          work,
          slice,
        );
        if (done < resident.count) throw new RangeError("A mark join's group keys did not fit");
        yield* level.groups.lookupJob(level.groupScratch, resident.count, groupHeads, work, slice);
        if (shared.pool.owes) {
          level.groupScratch.encoded?.writer.reset();
          level.groupScratch.encodedParts?.writer.reset();
        }
      } else
        groupHeads.fill(
          store.rows > 0 && level.groups === null && correlated === 0 ? 1 : 0,
          0,
          resident.count,
        );
      this.#groupsLooked = true;
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
      if (work.spend(CANDIDATE_WORK, slice)) yield;
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
        if (
          probeTNull ||
          (yield* groupNullCountJob(level, groupHead - 1, nextInGroup, work, slice)) > 0
        )
          state[row] = NULL_CLASS;
        this.#candidate++;
        continue;
      }
      let cursor = this.#candidateCursor === 0 ? groupHead : this.#candidateCursor;
      let decided = false;
      while (cursor !== 0 && n < BATCH_ROWS) {
        const buildRow = cursor - 1;
        cursor = nextInGroup(buildRow);
        // A checkpoint per candidate, whether the tests below reject it or not.
        this.#candidateCursor = cursor;
        if (work.spend(CANDIDATE_WORK, slice)) yield;
        if (!probeTNull && (tNull?.[buildRow] ?? 0) !== 1) continue;
        if (!(yield* compatibleJob(shared, batch, row, store, buildRow))) continue;
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
    return n;
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

/** Key work one visited mark candidate (a chain step and its tests) costs. */
const CANDIDATE_WORK = 16;

/**
 * Rows with a null tested key in the group starting at build row `head` (memoised): a walk
 * of the group that spends key work per row, so a large group spans steps.
 */
function* groupNullCountJob(
  level: Level,
  head: number,
  nextInGroup: (row: number) => number,
  work: KeyWork,
  slice: Slice,
): SlicedJob<number> {
  const memo = level.groupNulls;
  const cached = memo?.[head] ?? -1;
  if (cached >= 0) return cached;
  let count = 0;
  for (let cursor = head + 1; cursor !== 0;) {
    const row = cursor - 1;
    if ((level.tNull?.[row] ?? 0) === 1) count++;
    cursor = nextInGroup(row);
    if (work.spend(CANDIDATE_WORK, slice)) yield;
  }
  if (memo !== null) memo[head] = count;
  return count;
}

/** A mark join's per-tested-key scratch (operator-owned: a comparison spans yields, R23.1). */
export interface CompatScratch {
  readonly probe: KeyScratch;
  readonly build: KeyScratch;
  readonly ids: Int32Array;
}

/**
 * Mark NULL test: every tested key is equal, or null on one side. Keys are extracted without
 * hashing and compared through `equalJob` (resumable within a key, review B7-p2c-3 #2).
 */
function* compatibleJob(
  shared: Shared,
  batch: Batch,
  row: number,
  store: ColumnStore,
  buildRow: number,
): SlicedJob<boolean> {
  const probeKeys = shared.spec.probeKeys;
  for (let i = shared.correlated; i < probeKeys.length; i++) {
    const scratch = shared.compat[i];
    if (scratch === undefined) continue;
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
      false,
    );
    if ((scratch.probe.nulls[0] ?? 0) !== 0 || (scratch.build.nulls[0] ?? 0) !== 0) continue;
    if (!(yield* sameKeyJob(scratch.probe, scratch.build, shared.work, shared.slice))) return false;
  }
  return true;
}

function* sameKeyJob(
  a: KeyScratch,
  b: KeyScratch,
  work: KeyWork,
  slice: Slice,
): SlicedJob<boolean> {
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
    return yield* equalJob(
      x.bytes,
      x.offsets[i] ?? 0,
      x.offsets[i + 1] ?? 0,
      y.bytes,
      y.offsets[j] ?? 0,
      y.offsets[j + 1] ?? 0,
      work,
      slice,
    );
  }
  const x = a.encoded;
  const y = b.encoded;
  if (x === null || y === null) return false;
  return yield* equalJob(
    x.writer.bytes,
    x.offsets[0] ?? 0,
    x.offsets[1] ?? 0,
    y.writer.bytes,
    y.offsets[0] ?? 0,
    y.offsets[1] ?? 0,
    work,
    slice,
  );
}

const KEY_TYPE: PhysicalType = { kind: "int32", bits: 32 };
const KEY_VALUE: FlatVector = {
  form: "flat",
  type: KEY_TYPE,
  length: 1,
  validity: null,
  data: { layout: "i32", values: new Int32Array(1) },
};

/**
 * A join with no equality keys, only a residual (ruling R24.2: PostgreSQL refuses a FULL JOIN
 * without a hash- or merge-joinable condition for an implementation reason; the new core keeps
 * the well-defined result). It runs as the keyed join over one constant key: every build row
 * is a candidate for every probe row and the residual decides, so outer rows, matched bits,
 * spilling (one partition that does not split: chunk mode) and slicing are the keyed join's.
 * Both inputs get a constant int column appended; the residual sees the declared columns, and
 * the output drops the constant ones.
 */
class Keyless {
  readonly spec: HashJoinSpec;
  readonly #probeWidth: number;
  readonly #buildWidth: number;

  constructor(declared: HashJoinSpec) {
    if (declared.kind === "mark") throw new TypeError("A mark join tests at least one key");
    if (declared.publishes.length > 0)
      throw new TypeError("A join without keys publishes no runtime filters");
    const p = declared.probeTypes.length;
    const b = declared.buildTypes.length;
    this.#probeWidth = p;
    this.#buildWidth = b;
    const residual = declared.residual;
    const unknown =
      residual?.unknown === undefined
        ? undefined
        : (batch: Batch, out: Selection) => residual.unknown?.(batch, out) ?? out;
    this.spec = {
      ...declared,
      probeTypes: [...declared.probeTypes, KEY_TYPE],
      buildTypes: [...declared.buildTypes, KEY_TYPE],
      probeKeys: [p],
      buildKeys: [b],
      nullSafe: [false],
      residual:
        residual === null
          ? null
          : {
              filter: (batch, out) => residual.filter(this.#pairs(batch), out),
              ...(unknown === undefined
                ? {}
                : {
                    unknown: (batch: Batch, out: Selection) => unknown(this.#pairs(batch), out),
                  }),
            },
    };
  }

  /** A pair batch (probe ++ build, each with its key) as the declared columns. */
  #pairs(batch: Batch): Batch {
    const p = this.#probeWidth;
    const columns = batch.columns;
    return {
      ...batch,
      columns: [...columns.slice(0, p), ...columns.slice(p + 1, p + 1 + this.#buildWidth)],
    };
  }

  /** An input operator that appends the constant key to each batch. */
  input(operator: Operator): Operator {
    return {
      push: (batch, transfer) =>
        operator.push(
          {
            ...batch,
            columns: [
              ...batch.columns,
              { form: "constant", type: KEY_TYPE, length: batch.length, value: KEY_VALUE },
            ],
          },
          transfer,
        ),
      ready: () => operator.ready(),
      finish: () => operator.finish(),
      abort: (reason) => {
        operator.abort(reason);
      },
    };
  }

  /** The downstream, seeing the declared output: the constant key columns dropped. */
  output(downstream: Operator): Operator {
    const p = this.#probeWidth;
    const b = this.#buildWidth;
    return {
      push: (batch, transfer) => {
        const columns = batch.columns;
        // probe ++ key (++ build ++ key): semi and anti emit probe columns only.
        const kept =
          columns.length > p + 1
            ? [...columns.slice(0, p), ...columns.slice(p + 1, p + 1 + b)]
            : columns.slice(0, p);
        return downstream.push({ ...batch, columns: kept }, transfer);
      },
      ready: () => downstream.ready(),
      finish: () => downstream.finish(),
      abort: (reason) => {
        downstream.abort(reason);
      },
    };
  }
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
  #cleanup: Promise<void> | null = null;
  #phaseWork: Promise<void> | null = null;
  #checkpointed = false;
  #restoreStarted = false;
  #finishing = false;
  #priorSpilled = 0;

  /** A join without keys (ruling R24.2): its inputs carry one constant key column each. */
  readonly #keyless: Keyless | null;

  constructor(spec: HashJoinSpec, context: HashJoinContext, saved?: JoinCheckpoint) {
    const keyless = spec.probeKeys.length === 0 && spec.buildKeys.length === 0;
    this.#keyless = keyless ? new Keyless(spec) : null;
    // A keyless join runs as the keyed join over its constant key (`spec` is that form).
    this.spec = this.#keyless?.spec ?? spec;
    this.#shared = new Shared(this.spec, context, saved?.hasMatches === true);
    this.#prober = new Prober(this.#shared);
    this.#level = new Level(this.#shared, 0, true);
    const build = new BuildSink(this);
    this.build = this.#keyless?.input(build) ?? build;
  }

  get profile(): HashJoinProfile {
    const shared = this.#shared;
    shared.profile.peakHeldBytes = shared.charge.peak;
    shared.profile.spilledBytes = this.#priorSpilled + shared.files.written;
    shared.profile.loans = shared.pool.loans;
    shared.profile.largestLoan = shared.pool.largestLoan;
    return shared.profile;
  }

  /** The probe pipeline's operator, emitting into `downstream`. */
  probe(downstream: Operator): Operator {
    const keyless = this.#keyless;
    this.#shared.downstream = keyless?.output(downstream) ?? downstream;
    const probe = new ProbeOperator(this);
    return keyless?.input(probe) ?? probe;
  }

  get buildComplete(): boolean {
    return this.#built;
  }

  /** At a child-ready boundary, save values and exact match bits, never rerun a
   * consumed source or residual. No active probe/finish continuation may cross this seam. */
  checkpoint(): Promise<JoinCheckpoint> {
    if (this.#checkpointed || this.#phaseWork || this.#finishing || this.#closed)
      return Promise.reject(new TypeError("Join checkpoint requires an unfinished input boundary"));
    const prior = this.#pending;
    const work = (async () => {
      await prior;
      const shared = this.#shared,
        level = this.#level;
      shared.checkAborted();
      shared.reset(shared.probeOut);
      shared.reset(shared.buildOut);
      shared.reset(shared.buildPart);
      const matches = shared.kind === "right" || shared.kind === "full";
      const types = matches
        ? [...this.spec.buildTypes, { kind: "bool" } as PhysicalType]
        : this.spec.buildTypes;
      const build = new RecordCheckpoint(shared.io, shared.context.spill, types);
      let probe: RecordCheckpoint | null = null;
      try {
        await level.flushStaging("build");
        await level.flushStaging("probe");
        for (let k = 0; k < level.store.chunkCount; k++) {
          shared.checkAborted();
          const columns: Vector[] = this.spec.buildTypes.map((_, c) => level.store.chunk(c, k));
          const rows = columns[0]?.length ?? 0;
          if (matches)
            columns.push(
              matchVector(
                rows,
                level.matched?.subarray(
                  k * level.store.chunkRows,
                  k * level.store.chunkRows + rows,
                ),
              ),
            );
          await build.write(columns, 0, rows);
        }
        const copy = async (
          run: SpilledRun,
          records: RecordCheckpoint,
          side: "build" | "probe",
        ) => {
          const sourceTypes = side === "build" ? shared.buildTypes : this.spec.probeTypes;
          for (let p = 0; p < run.pages.length; p++) {
            shared.checkAborted();
            const ref = run.pages[p];
            if (!ref) throw new Error("Missing checkpoint source page");
            await shared.need(6 * ref.length + 64 * 1024);
            const page = await shared.readPage(run, p);
            try {
              const decoded = await shared.decode(page, sourceTypes);
              const columns: readonly Vector[] =
                side === "build" && matches && shared.initialMatchColumn < 0
                  ? [...decoded.columns, matchVector(decoded.rows)]
                  : decoded.columns;
              await records.write(columns, 0, decoded.rows);
            } finally {
              shared.releasePage(page);
            }
          }
        };
        const parts = level.parts;
        if (parts)
          for (let p = 0; p < parts.fanout; p++) {
            const run = parts.build[p];
            if (run && !parts.resident[p]) await copy(run, build, "build");
          }
        await build.seal();
        probe = new RecordCheckpoint(shared.io, shared.context.spill, this.spec.probeTypes);
        if (parts)
          for (let p = 0; p < parts.fanout; p++) {
            const run = parts.probe[p];
            if (run && !parts.resident[p]) await copy(run, probe, "probe");
          }
        await probe.seal();
        shared.checkAborted();
        this.#checkpointed = true;
        return new JoinCheckpoint(build, probe, this.#built, matches, { ...this.profile });
      } catch (error) {
        await Promise.allSettled([build.close(), probe?.close()]);
        throw error;
      }
    })();
    this.#trackPhase(work);
    return work;
  }

  /** Copies into new admitted state; the saved owner closes only after success.
   * Prior resident probes are absent. Deferred probes stay deferred until finish. */
  restore(saved: JoinCheckpoint): Promise<void> {
    if (
      this.#phaseWork ||
      this.#built ||
      this.#level.totalBuildRows ||
      this.#closed ||
      this.#restoreStarted
    )
      return Promise.reject(new TypeError("Join restore requires a new admitted operator"));
    this.#restoreStarted = true;
    const work = (async () => {
      const shared = this.#shared,
        level = this.#level;
      shared.checkAborted();
      if (saved.hasMatches !== shared.initialMatchColumn >= 0)
        throw new TypeError("Join restore schema was not selected at construction");
      await level.spill();
      await saved.build.read(
        shared.io,
        async (columns, rows) => {
          await level.addBuild(columns, null, 0, rows);
          await level.flushStaging("build");
        },
        saved.progressBytes,
      );
      if (saved.buildComplete) {
        if (!(await shared.sliced(level.buildTableJob(shared.pool))))
          throw new RangeError("The restored empty join table could not be allocated");
        this.#built = true;
      }
      await saved.probe.read(
        shared.io,
        async (columns, rows) => {
          const parts = level.parts;
          if (!parts) throw new Error("Missing restored join partitions");
          const keys = this.spec.probeKeys.map((c) => columnOf(columns, c));
          await extractAll(shared, keys, null, 0, rows, shared.scratch);
          const counts = level.groupByPartition(null, 0, rows, false);
          for (let p = 0; p < parts.fanout; p++) {
            const start = counts[p] ?? 0,
              count = (counts[p + 1] ?? 0) - start;
            if (count) await level.stageProbe(p, columns, shared.order, start, count);
          }
          await level.flushStaging("probe");
        },
        saved.progressBytes,
      );
      shared.checkAborted();
      Object.assign(shared.profile, saved.profile);
      await saved.close();
      this.#priorSpilled = saved.spilledBytes;
    })();
    this.#trackPhase(work);
    return work;
  }

  #trackPhase(work: Promise<unknown>): void {
    const settled = work.then(
      () => undefined,
      () => undefined,
    );
    this.#phaseWork = settled;
    void settled.then(() => {
      if (this.#phaseWork === settled) this.#phaseWork = null;
    });
  }

  #fail(error: unknown): never {
    this.abort(error);
    throw error;
  }

  /** @internal */
  pushBuild(batch: Batch, transfer?: Transfer): PushResult {
    if (this.#built) throw new TypeError("The join's build has finished");
    if (this.#checkpointed || this.#phaseWork)
      throw new TypeError("The join's input is checkpointed");
    const shared = this.#shared;
    if (shared.initialMatchColumn >= 0)
      batch = {
        ...batch,
        columns: [...batch.columns, matchVector(batch.length)],
      };
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
    if (this.#checkpointed || this.#phaseWork)
      throw new TypeError("The join's input is checkpointed");
    this.#shared.checkAborted();
    // No fresh slice per push (R23.5: the task clock's start is the task's, so short pushes
    // whose output blocks with a resolved ready still come due and yield).
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
      n = keyStepRows(keys, ids, at, active - at, KEY_STEP_BYTES / 2, !raw);
    }
  }

  /** @internal */
  finishProbe(): MaybePromise<void> {
    this.#finishing = true;
    const shared = this.#shared;
    const level = this.#level;
    const downstream = shared.downstream;
    if (downstream === null) throw new TypeError("The join's probe has no downstream");
    try {
      shared.checkAborted();
    } catch (error) {
      this.#fail(error);
    }
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
        const ref = build.pages[i];
        if (!ref) throw new Error("Missing build spill page");
        await shared.need(3 * ref.length + 64 * 1024);
        const page = await shared.readPage(build, i);
        try {
          const { rows, columns } = await shared.decode(page, shared.buildTypes);
          shared.checkAborted();
          await sub.addPage(page.bytes, rows, columns);
        } finally {
          shared.resetKeys();
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
      shared.reset(shared.probeOut);
      shared.reset(shared.buildOut);
      shared.reset(shared.buildPart);
      shared.resetKeys();
      const ref = probe.pages[i];
      if (!ref) throw new Error("Missing probe spill page");
      await shared.need((chunk === null ? 3 : 2) * ref.length + 64 * 1024);
      const page = await shared.readPage(probe, i);
      try {
        const { rows, columns } = await shared.decode(page, this.spec.probeTypes);
        shared.checkAborted();
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
    shared.resetKeys();
    const kind = this.spec.kind;
    const groupLayout =
      kind === "mark" && shared.correlated > 0
        ? new KeyLayout(
            shared.layout.types.slice(0, shared.correlated),
            shared.layout.nullSafe.slice(0, shared.correlated),
          )
        : null;
    const groupBytes =
      kind === "mark"
        ? 5 +
          (groupLayout
            ? KeyScratch.bytes(groupLayout, BATCH_ROWS) +
              KeyScratch.bytes(groupLayout, 0) +
              JoinTable.estimateBytes(groupLayout, 1)
            : 0)
        : 0;
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
        // Only a reserved single-row load may use replay headroom temporarily.
        let loadingFirst = false;
        const chunkCharge: ByteCharge = {
          allocate: (bytes) => shared.pool.allocate(bytes, loadingFirst ? 0 : keep),
          free: (bytes) => {
            shared.pool.free(bytes);
          },
        };
        const sub = new Level(shared, level, false, chunkCharge);
        const loaded: Array<{ page: number; from: number; count: number }> = [];
        let releaseStep: (() => void) | null = null;
        try {
          let reserved = false;
          while (page < build.pages.length) {
            const paced = shared.pace();
            if (paced !== null) await paced;
            const bytes = await shared.readPage(build, page);
            let full = false;
            let retry = 0;
            try {
              const { rows, columns } = await shared.decode(bytes, shared.buildTypes);
              shared.checkAborted();
              loadingFirst = reserved && sub.store.rows === 0;
              let taken = sub.store.append(columns, null, row, loadingFirst ? 1 : rows - row);
              loadingFirst = false;
              if (taken === 0 && sub.store.rows === 0) {
                if (reserved) throw new RangeError("A build row could not be allocated");
                // Give the page back before reserving the step. Loading holds the page;
                // replay holds its headroom after that page is released. These peaks
                // overlap the build row/table, but not each other.
                retry =
                  sub.store.appendNeed(columns, row) +
                  JoinTable.estimateBytes(shared.layout, 1) +
                  (shared.layout.family === "enc" ? bytes.charged : 0) +
                  (groupLayout?.family === "enc" ? bytes.charged : 0) +
                  Math.max(bytes.charged, 2 * probePage, keep) +
                  groupBytes +
                  64 * 1024;
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
              releaseStep ??= shared.pool.pinStep();
              await shared.need(retry);
              reserved = true;
              continue;
            }
            if (full || (reserved && sub.store.rows > 0)) break;
          }
          // The chunk's table; give rows back (halving) while it does not fit.
          // A single row cannot be halved: its complete table/key/replay transition
          // shares one pinned preflight through all intervening allocations/frees.
          const buildTable = async () => {
            if (sub.store.rows === 1) {
              shared.resetKeys();
              const key =
                shared.layout.family === "enc"
                  ? PageWriter.encodedBytes(
                      shared.buildTypes.map((_, c) => sub.store.chunk(c, 0)),
                      1,
                    )
                  : 0;
              releaseStep ??= shared.pool.pinStep();
              // The row may fit without using replay headroom while its encoded
              // table does not. Preflight BEFORE either table/key allocation.
              await shared.need(
                JoinTable.estimateBytes(shared.layout, 1) +
                  key +
                  (groupLayout?.family === "enc" ? key : 0) +
                  Math.max(key, 2 * probePage, keep) +
                  groupBytes +
                  64 * 1024,
              );
            }
            return shared.sliced(
              sub.buildTableJob(sub.store.rows === 1 ? shared.pool : chunkCharge),
            );
          };
          while (!(await buildTable())) {
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
          shared.resetKeys();
          if (probe.rows > 0) await this.#replay(probe, sub, chunk);
          await this.#emitUnmatchedBuild(sub);
        } finally {
          sub.close();
          shared.probeOut.reset(true);
          shared.buildOut.reset(true);
          shared.buildPart.reset(true);
          shared.resetKeys();
          releaseStep?.();
          shared.pool.settle();
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
        shared.checkAborted();
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
    if (store.rows === 1) {
      if (matched[0] !== 1) {
        const columns = [
          ...shared.probeNulls.map((vector) => withLength(vector, 1)),
          ...shared.buildColumns.map((c) => store.chunk(c, 0)),
        ];
        if (shared.emit({ length: 1, columns, selection: null }) === BLOCKED) {
          await shared.waitDownstream();
          shared.checkAborted();
        }
      }
      return;
    }
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

  #releaseOwned(): void {
    const shared = this.#shared;
    try {
      this.#prober.release();
      this.#level.close();
      for (const filter of this.runtimeFilters) filter.close();
      shared.probeOut.close();
      shared.buildOut.close();
      shared.buildPart.close();
      shared.pages.close();
      shared.pool.close();
    } finally {
      shared.profile.peakHeldBytes = shared.charge.peak;
      shared.context.grant.close();
    }
  }

  #release(): void {
    if (this.#closed) return;
    this.#closed = true;
    const shared = this.#shared;
    // Publish the fence before calling external close implementations, which may reenter.
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    this.#cleanup = new Promise<void>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    void this.#cleanup.catch((error: unknown) => {
      this.cleanupError ??= error;
    });
    const phaseWork = this.#phaseWork;
    const drain = shared.files.needsDrain || phaseWork !== null;
    const finish = (failed: boolean, error?: unknown) => {
      try {
        if (drain) this.#releaseOwned();
      } catch (releaseError) {
        if (!failed) error = releaseError;
        failed = true;
      }
      if (failed) reject(error);
      else resolve();
    };
    const closing = phaseWork
      ? phaseWork.catch(() => undefined).then(() => shared.files.closeAll())
      : shared.files.closeAll();
    // Resident completion remains synchronous. Spill pages, stores, pool and grant stay
    // owned until every append and file close has stopped borrowing their charged buffers.
    if (!drain) this.#releaseOwned();
    void closing.then(
      () => finish(false),
      (error: unknown) => finish(true, error),
    );
  }

  /** After termination, settles once spill borrows drain and all charges release; resolved before termination. */
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
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
