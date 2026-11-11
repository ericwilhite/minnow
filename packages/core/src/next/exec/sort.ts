/**
 * Sort, top-N and per-group top-k (track B7, increment 2).
 *
 * ## Operators (the executor composes them; exec/executor.ts)
 * - `SortOperator`: ORDER BY with optional LIMIT / OFFSET / WITH TIES (IR `sort`). A breaker:
 *   `push` takes rows, `finish` emits them in order to its downstream, then finishes it.
 * - `GroupTopKOperator`: sole ROW_NUMBER/RANK upper bounds as a per-group top-k, with the
 *   ranking value appended (int64). RANK keeps every K-th-row boundary peer and counts rows
 *   before a peer group. Compiler recognition keeps the outer filter and only traverses
 *   immutable direct aliases (window-topk-plan.ts).
 * Key columns are input column indexes; outputs are the input columns (plus the row number).
 *
 * ## How (sort-keys.ts for the key law)
 * - **Rows** are copied into a `ColumnStore` under the operator's spillable grant, each with its
 *   normalized key bytes and prefix words (key mode), and sorted as row ids (the locator): radix
 *   over the prefix, byte refinement of ties; payload is gathered only for the rows emitted.
 * - **Top-N** (k = offset + limit): rows are kept while their key beats the current k-th key (a
 *   threshold); periodically the buffer is compacted to its best k (ties kept for WITH TIES),
 *   so memory is O(k) and most rows are rejected before encoding when their first numeric key is worse. Ties and unsupported first keys use the full encoded comparison. A k too large for
 *   memory falls back to the external sort, still filtering by the threshold.
 * - **Per-group top-k** compacts the same way per partition (k rows plus boundary peers for RANK); if the
 *   survivors do not fit, it falls back to the external sort with the per-group cut at output.
 * - **External sort**: when the grant refuses, the rows in memory are sorted and written as a
 *   run of typed, checksummed spill pages (keys included); at the end the runs are merged with
 *   bounded fan-in (each input holds one page from the spill pool), in passes when needed. OFFSET
 *   rows are counted, not materialized.
 * - **ICU collations** (C8): rows are kept, the distinct collated values collected, and at the
 *   end ranked with `Intl.Collator` (ties by bytes) and substituted into the keys. If the values
 *   or keys do not fit, or the input spilled, the sort runs in **compare mode**: runs ordered and
 *   merged by a value comparator (also used for JSON and array keys).
 *
 * ## Memory and backpressure
 * Grant-or-spill, never refuse. Transient buffers come from the spill pool (spill-pool.ts), whose
 * loans cover one bounded step. `push` returns `blocked` while a run is written; `finish` awaits
 * downstream readiness. Without a spill the whole sort runs without an await.
 */
import { RecordCheckpoint } from "./record-checkpoint.js";
import type { Grant } from "../memory/governor.js";
import { stringBytes } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import { layoutOf, type PhysicalType } from "../types.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  allocateData,
  type Batch,
  type FlatVector,
  type Vector,
} from "../vector/vector.js";
import { lsdSortIds } from "./sort-lsd.js";
import { ColumnarSortOperator, columnarNumericSort } from "./sort-columnar.js";
import type { TaskClock } from "./pipeline.js";
import { ColumnStore } from "./column-store.js";
import { sortRowView, SORT_ROW_VIEW_BYTES } from "./sort-row.js";
import {
  YIELD_INTERVAL_MS,
  isPromise,
  yieldTask,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import {
  DirectKeyEncoder,
  FirstKeyThreshold,
  PREFIX_WORDS,
  RadixScratch,
  SortKeyEncoder,
  TUPLE_WORDS,
  directWidth,
  msdSort,
  msdStackWords,
  collatorFor,
  compareRanges,
  compareRows,
  decodeOperands,
  dropOperands,
  emptyOperands,
  isByteCollation,
  mergeSortJob,
  needsOperands,
  operandBytes,
  sortMode,
  type Operands,
  type SortColumn,
} from "./sort-keys.js";
import { Slice, SLICE_ROWS, runSliced, runToEnd, type SlicedJob } from "./slices.js";
import { PageWriter, SpillFiles, SpilledRun } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";

type MaybePromise<T> = T | Promise<T>;

const BYTES: PhysicalType = { kind: "bytes" };
const INT: PhysicalType = { kind: "int32", bits: 32 };
const BIGINT: PhysicalType = { kind: "int64" };
/** Most spill runs one merge reads at once. */
export const MAX_FAN_IN = 64;
/**
 * Sort scratch reserved per row as rows arrive: ids, then (prefix words, row id) tuples and the
 * radix's copy of them, and a byte for the MSD stack (n / 49 ranges of 12 bytes; compare and
 * ranks modes use ids and a merge buffer, 8 bytes).
 */
const SCRATCH_PER_ROW = 4 + 2 * 4 * TUPLE_WORDS + 1;
const POOL_BYTES = 1024 * 1024;
/** Most operand bytes one decode step builds (the store-wide decode is sliced by them). */
const DECODE_BYTES = 256 * 1024;
/** Most varlen bytes of an arriving batch handled in one step (encoding, filtering, copying). */
const ARRIVAL_BYTES = 512 * 1024;
/** Top-N up to this many rows holds its next compaction's store in reserve (never spills). */
const COMPACT_RESERVE_ROWS = 65_536;
/**
 * A sorter's own scratch (R23.1), charged at construction: survivor rows, partition key
 * lengths, identity rows and group survivors per batch row, and the radix histograms.
 */
const SORTER_SCRATCH_BYTES =
  BATCH_ROWS * (2 + 4 + 2 + 2) + RadixScratch.BYTES + FirstKeyThreshold.BYTES + 2048;

export interface SortSpec {
  readonly types: readonly PhysicalType[];
  readonly keys: readonly SortColumn[];
  /** Rows to emit after the offset (null: all); bound by the executor. */
  readonly limit: number | null;
  readonly offset: number;
  /** WITH TIES: also emit the rows equal to the last one within the limit. */
  readonly withTies: boolean;
}

export interface GroupTopKSpec {
  readonly types: readonly PhysicalType[];
  /** PARTITION BY columns. */
  readonly partitionBy: readonly number[];
  readonly orderBy: readonly SortColumn[];
  /** Row-count bound per partition; RANK includes all peers at the boundary. */
  readonly k: number;
  readonly ranking?: "row_number" | "rank";
  readonly partitionNulls?: "first" | "last";
}

export interface SortContext {
  /** Spillable; the operator closes it when done or aborted. */
  readonly grant: Grant;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal?: AbortSignal;
  /** Compiler opt-in; ordered-set and window-internal sorts retain their existing recipes. */
  readonly columnar?: boolean;
  /** Paired control for dedicated gather/result ownership. */
  readonly columnarOwnership?: boolean;
  readonly columnarDictionaries?: boolean;
  readonly columnarCompression?: boolean;
  /** Deterministic private clock for lifecycle tests; production uses the shared task clock. */
  readonly clock?: TaskClock;
}

export interface SortProfile {
  readonly keyBits?: number;
  readonly packedWords?: number;
  readonly rankedKeys?: number;
  readonly orderedSuffixKeys?: number;
  readonly rankRefusals?: number;
  readonly radixPasses?: number;
  mode: "keys" | "ranks" | "compare";
  rowsIn: number;
  rowsOut: number;
  /** Rows a top-N threshold rejected before storing them. */
  rejected: number;
  compactions: number;
  runs: number;
  mergePasses: number;
  spilledBytes: number;
  peakHeldBytes: number;
  loans: number;
  /** Compare mode: most bytes of decoded operands (JSONB, arrays, ICU text) held, charged. */
  operandBytes: number;
  /** Per-group top-k: most bytes of partition threshold keys held (charged). */
  thresholdBytes: number;
}

/** Bytes a sort needs admitted before it runs (encoder, pool, output chunk). */
export function sortMinimumBytes(types: readonly PhysicalType[]): number {
  // Pool, the key encoders, and one store chunk with its sort scratch: an empty store always
  // takes its first chunk, so only a row larger than all of that needs a loan.
  let row = 4 + PREFIX_WORDS * 4 + 4;
  for (const type of types) row += type.kind === "text" || type.kind === "bytes" ? 24 : 17;
  return (
    POOL_BYTES +
    SORTER_SCRATCH_BYTES +
    2 * (2048 * (PREFIX_WORDS * 4 + 4) + 4096 + 260) +
    2048 * (row + SCRATCH_PER_ROW) +
    16 * 1024 * (types.length + 2) +
    // Byte sets (ICU ranks, group thresholds): their initial arenas and slots.
    (types.length + 1) * (4096 + 256) +
    64 * 1024
  );
}

/** Rows of one page held by a merge input. */
interface Cursor {
  readonly run: SpilledRun;
  page: number;
  held: { bytes: Uint8Array; charged: number } | null;
  columns: readonly FlatVector[];
  /** Compare mode: the page's decoded operands, charged to the pool while the page is held. */
  readonly operands: Operands;
  rows: number;
  pos: number;
}

/** A small open-addressing set of byte strings (ICU ranks), charged; ids in insertion order. */
export class ByteSet {
  #slots = new Int32Array(64).fill(-1);
  #arena = new Uint8Array(4096);
  #used = 0;
  starts: number[] = [];
  ends: number[] = [];
  hashes: number[] = [];
  charged = 0;
  /** Charges its initial arena and slot array (the caller's minimum covers them). */
  constructor(readonly charge: { allocate(n: number): boolean; free(n: number): void }) {
    const initial = 4096 + 64 * 4;
    if (!charge.allocate(initial)) throw new RangeError("A sort's byte set must be admitted");
    this.charged = initial;
  }

  get size(): number {
    return this.starts.length;
  }

  bytes(id: number): Uint8Array {
    return this.#arena.subarray(this.starts[id] ?? 0, this.ends[id] ?? 0);
  }

  get arena(): Uint8Array {
    return this.#arena;
  }

  #hash(bytes: Uint8Array, start: number, end: number): number {
    let h = 0x811c_9dc5;
    for (let i = start; i < end; i++) h = Math.imul(h ^ (bytes[i] ?? 0), 0x0100_0193);
    return h >>> 0;
  }

  /** The id of the value, inserting it; −1 when the charge refused. */
  add(bytes: Uint8Array, start: number, end: number): number {
    const h = this.#hash(bytes, start, end);
    const mask = this.#slots.length - 1;
    for (let s = h & mask; ; s = (s + 1) & mask) {
      const id = this.#slots[s] ?? -1;
      if (id < 0) break;
      if ((this.hashes[id] ?? 0) === h) {
        const a = this.starts[id] ?? 0;
        if (compareRanges(this.#arena, a, this.ends[id] ?? 0, bytes, start, end) === 0) return id;
      }
    }
    const length = end - start;
    // Grow the arena / table first (charged), then insert.
    if (this.#used + length > this.#arena.length) {
      let size = this.#arena.length * 2;
      while (size < this.#used + length) size *= 2;
      if (!this.charge.allocate(size + 64)) return -1;
      const grown = new Uint8Array(size);
      grown.set(this.#arena.subarray(0, this.#used));
      this.charged += size + 64 - this.#arena.length;
      this.charge.free(this.#arena.length);
      this.#arena = grown;
    }
    if ((this.size + 1) * 2 > this.#slots.length) {
      const size = this.#slots.length * 2;
      if (!this.charge.allocate(size * 4)) return -1;
      // The old slots go back once the new ones are held.
      this.charged += size * 4 - this.#slots.length * 4;
      this.charge.free(this.#slots.length * 4);
      this.#slots = new Int32Array(size).fill(-1);
      for (let id = 0; id < this.size; id++) this.#place(id);
    }
    if (!this.charge.allocate(3 * 8 + 16)) return -1;
    this.charged += 3 * 8 + 16;
    this.#arena.set(bytes.subarray(start, end), this.#used);
    const id = this.size;
    this.starts.push(this.#used);
    this.ends.push(this.#used + length);
    this.hashes.push(h);
    this.#used += length;
    this.#place(id);
    return id;
  }

  /** The id of a value present in the set (−1 when absent). */
  find(bytes: Uint8Array, start: number, end: number): number {
    const h = this.#hash(bytes, start, end);
    const mask = this.#slots.length - 1;
    for (let s = h & mask; ; s = (s + 1) & mask) {
      const id = this.#slots[s] ?? -1;
      if (id < 0) return -1;
      if (
        (this.hashes[id] ?? 0) === h &&
        compareRanges(this.#arena, this.starts[id] ?? 0, this.ends[id] ?? 0, bytes, start, end) ===
          0
      )
        return id;
    }
  }

  #place(id: number): void {
    const mask = this.#slots.length - 1;
    let s = (this.hashes[id] ?? 0) & mask;
    while ((this.#slots[s] ?? -1) >= 0) s = (s + 1) & mask;
    this.#slots[s] = id;
  }

  close(): void {
    this.charge.free(this.charged);
    this.charged = 0;
  }
}

/** Options of the shared sorter. */
interface SorterSpec {
  readonly types: readonly PhysicalType[];
  /** Full key: partition columns (group top-k) then order columns. */
  readonly keys: readonly SortColumn[];
  /** Leading keys that form the partition (0: none). */
  readonly partition: number;
  /** Rows to keep while buffering: top-N (offset + limit) or per partition; null: all. */
  readonly keep: number | null;
  readonly withTies: boolean;
  /** WITH TIES and group top-k keep one boundary/partition row beside the merge inputs. */
  readonly heldRow: boolean;
}

/** A contiguous range of sorted rows: of the store by ids, or of a merge input's page. */
type Range =
  | {
      readonly kind: "store";
      readonly ids: Int32Array;
      readonly from: number;
      readonly count: number;
    }
  | {
      readonly kind: "page";
      readonly columns: readonly Vector[];
      /** Compare mode: the page's decoded operands (rows as in `columns`). */
      readonly operands: Operands;
      readonly from: number;
      readonly count: number;
    };

/**
 * What both operators share: rows with keys in a store, compaction, runs, merge. It owns its
 * grant and closes it at the end.
 */
class Sorter {
  #checkpointWork: Promise<void> | null = null;
  #checkpointClose: Promise<void> | null = null;
  readonly #keyStorage: boolean;
  readonly #emptyTail: readonly Vector[];
  #wideOutputBytes = 0;
  #pageKeyBytes = 0;
  #mergeStep: (() => void) | null = null;
  /** One complete small-row ingestion/spill step, repaid before returning to the source. */
  #rowStep: { loan: { repay(): void }; release: () => void } | null = null;
  readonly spec: SorterSpec;
  readonly width: number;
  mode: "keys" | "ranks" | "compare";
  readonly charge: Charge;
  readonly pool: Pool;
  readonly files: SpillFiles;
  readonly pages: PageWriter;
  readonly io: SpillIo;
  store: ColumnStore;
  readonly encoder: SortKeyEncoder | null;
  readonly partEncoder: SortKeyEncoder | null;
  readonly runs: SpilledRun[] = [];
  readonly profile: SortProfile;
  #reserved = 0;
  /** Operator-owned scratch (R23.1): it lives across `#makeRoom`'s awaits. */
  readonly keptRows: Uint16Array;
  readonly partLengths: Int32Array;
  /** Rows 0 … BATCH_ROWS − 1 (never written after construction). */
  readonly #identity: Uint16Array;
  /** Group thresholds (partition > 0), or the raw first-key gate (partition = 0).
   * This charged buffer is distinct from keptRows, which full-key filtering overwrites.
   */
  readonly groupKept: Uint16Array;
  readonly radix: RadixScratch;
  #groupKeptCount = 0;
  /**
   * Compare mode: the resident rows' decoded operands by row id, charged as the rows arrive, so
   * a run's comparisons never decode (and a full store already holds what its sort needs).
   */
  readonly operands: Operands;
  /** Whether compare mode decodes operands (a JSONB, array or ICU key). */
  readonly decodes: boolean;
  readonly #partKeys: readonly SortColumn[];
  /** The slice of the current finish or compaction work (R23.2); merges and runs pace by it. */
  readonly slice: Slice;
  /** Compare mode: most operand bytes of one written page (a merge input holds one). */
  #pageOperands = 0;
  /** A loan for operands of rows stored before compare mode (repaid after their run). */
  #operandLoan: { repay(): void } | null = null;
  /** Longest key bytes stored: within the prefix words no full comparison is needed. */
  #maxKey = 0;
  #threshold: Uint8Array | null = null;
  /** Direct mode: the threshold's prefix words. */
  #thresholdWords: Uint32Array | null = null;
  readonly #firstThreshold: FirstKeyThreshold | null;
  /** Fixed-width keys encoded straight into the prefix words (no key bytes, no refinement). */
  readonly direct: boolean;
  readonly directEncoder: DirectKeyEncoder | null;
  /** Per-group top-k: partition key → its k-th order key so far (rows must beat it). */
  readonly #groups: ByteSet | null;
  #groupThresholds: Array<Uint8Array | undefined> = [];
  #groupFirstThresholds: Array<FirstKeyThreshold | undefined> = [];
  /** Bytes of the threshold copies (charged before each copy, freed on replace and close). */
  #thresholdBytes = 0;
  /** The grant refused a threshold: the optimization is off (rows are kept, just not filtered). */
  #groupThresholdsOff = false;
  #lastSurvivors = 0;
  /**
   * Top-N: bytes held for the next compaction's store, so a full grant can always compact
   * rather than spill: the chunks for `keep` rows (`fixedBytes`, initial arenas included), and
   * per varlen column twice `keep` of its largest value so far. The compaction copies at most
   * `SLICE_ROWS` (four whole chunks) per append, so each chunk's arena grows once, to at most
   * twice its bytes. Freed just before the compaction allocates; held again as rows arrive.
   */
  #compactionHeld = 0;
  readonly #reservesCompaction: boolean;
  readonly #varlenColumns: readonly number[];
  /** Largest value of each varlen payload column stored so far (for `#compactionHeld`). */
  readonly #maxVarlen: number[];
  #sets: Array<ByteSet | null>;
  #aborted: unknown = undefined;
  #closed = false;

  constructor(
    spec: SorterSpec,
    readonly context: SortContext,
  ) {
    this.spec = spec;
    this.slice = new Slice(YIELD_INTERVAL_MS, context.clock);
    this.width = spec.types.length;
    this.mode = sortMode(spec.types, spec.keys);
    this.#keyStorage = this.mode === "keys";
    this.operands = emptyOperands(spec.types, spec.keys);
    this.decodes = needsOperands(spec.types, spec.keys);
    this.#partKeys = spec.keys.slice(0, spec.partition);
    this.#reservesCompaction =
      this.mode === "keys" &&
      spec.partition === 0 &&
      spec.keep !== null &&
      spec.keep > 0 &&
      spec.keep <= COMPACT_RESERVE_ROWS;
    this.#varlenColumns = spec.types.flatMap((type, c) => (layoutOf(type) === "varlen" ? [c] : []));
    this.#maxVarlen = this.#varlenColumns.map(() => 0);
    this.charge = new Charge(context.grant);
    this.pool = new Pool(this.charge, POOL_BYTES);
    if (!this.charge.allocate(SORTER_SCRATCH_BYTES)) {
      this.pool.close();
      throw new RangeError("A sort's minimum must be admitted");
    }
    // All sorter scratch is constructed only after its owning reservation succeeds.
    this.keptRows = new Uint16Array(BATCH_ROWS);
    this.partLengths = new Int32Array(BATCH_ROWS);
    this.#identity = Uint16Array.from({ length: BATCH_ROWS }, (_, i) => i);
    this.groupKept = new Uint16Array(BATCH_ROWS);
    this.radix = new RadixScratch();
    this.files = new SpillFiles(context.spill, context.queryId, {
      // A backend's retained write buffer outlives this bounded step. It may use ordinary
      // credit, never progress credit that the row/page step must repay before another input.
      allocate: (bytes) => {
        const grant = context.grant;
        if (grant.loaned > 0 && bytes > grant.held - grant.loaned - grant.allocated) return false;
        return this.charge.allocate(bytes);
      },
      free: (bytes) => this.charge.free(bytes),
    });
    this.pages = new PageWriter(this.pool, 16 * 1024);
    this.io = new SpillIo(this.pool, this.files, this.pages, context, () => {
      this.checkAborted();
    });
    this.io.onFlush = (staging) => {
      const columns = staging.vectors();
      this.#observePageKeys(columns, staging.rows);
      if (this.mode !== "keys" && this.decodes) {
        const bytes = operandBytes(spec.types, spec.keys, columns, 0, staging.rows);
        this.#pageOperands = Math.max(this.#pageOperands, bytes);
      }
      if (staging.rows === 1 && PageWriter.encodedBytes(columns, 1) > 64 * 1024)
        this.#observeWide(columns, 0);
    };
    this.direct =
      this.mode === "keys" && spec.partition === 0 && directWidth(spec.types, spec.keys) !== null;
    const first = spec.keys[0];
    this.#firstThreshold =
      this.mode === "keys" &&
      spec.partition === 0 &&
      spec.keep !== null &&
      first !== undefined &&
      directWidth(spec.types, [first]) !== null
        ? new FirstKeyThreshold(first)
        : null;
    this.directEncoder = this.direct
      ? new DirectKeyEncoder(spec.types, spec.keys, this.pool)
      : null;
    this.encoder =
      this.mode === "keys" && !this.direct ? new SortKeyEncoder(spec.keys, this.pool) : null;
    this.#groups = this.mode === "keys" && spec.partition > 0 ? new ByteSet(this.charge) : null;
    this.partEncoder =
      this.mode === "keys" && spec.partition > 0
        ? new SortKeyEncoder(spec.keys.slice(0, spec.partition), this.pool)
        : null;
    const blank = (type: PhysicalType): Vector => ({
      form: "constant",
      type,
      length: BATCH_ROWS,
      value: { form: "flat", type, length: 1, validity: null, data: allocateData(type, 1) },
    });
    const zero = blank(INT);
    this.#emptyTail = [
      blank(BYTES),
      ...Array.from({ length: PREFIX_WORDS }, () => zero),
      ...(spec.partition > 0 ? [zero] : []),
    ];
    this.store = this.#newStore();
    this.#sets = spec.keys.map((key) =>
      this.mode === "ranks" && !isByteCollation(key.collation) ? new ByteSet(this.charge) : null,
    );
    this.profile = {
      mode: this.mode,
      rowsIn: 0,
      rowsOut: 0,
      rejected: 0,
      compactions: 0,
      runs: 0,
      mergePasses: 0,
      spilledBytes: 0,
      peakHeldBytes: 0,
      loans: 0,
      operandBytes: 0,
      thresholdBytes: 0,
    };
  }

  /** Store columns: payload, then (key mode) key bytes, prefix words, partition key length. */
  get storeTypes(): PhysicalType[] {
    if (!this.#keyStorage) return [...this.spec.types];
    if (this.direct)
      return [...this.spec.types, ...Array.from({ length: PREFIX_WORDS }, () => INT)];
    return [
      ...this.spec.types,
      BYTES,
      ...Array.from({ length: PREFIX_WORDS }, () => INT),
      ...(this.spec.partition > 0 ? [INT] : []),
    ];
  }

  #newStore(): ColumnStore {
    return new ColumnStore(this.storeTypes, this.charge);
  }

  get keyColumn(): number {
    return this.width;
  }
  /** First prefix-word column (after the key bytes, except in direct mode). */
  get prefixColumn(): number {
    return this.width + (this.direct ? 0 : 1);
  }

  checkAborted(): void {
    if (this.#aborted !== undefined)
      throw this.#aborted instanceof Error
        ? this.#aborted
        : new Error("Aborted", { cause: this.#aborted });
    this.context.signal?.throwIfAborted();
  }

  // --- input -----------------------------------------------------------------------------

  /** Takes the active rows of a batch; a Promise while a run is being written. */
  add(batch: Batch): MaybePromise<void> {
    this.checkAborted();
    const count = batch.selection === null ? batch.length : batch.selection.count;
    this.profile.rowsIn += count;
    if (count === 0) return;
    const ids = batch.selection?.rows ?? null;
    const first = partRows(batch.columns, ids, 0, count);
    if (first >= count) return this.#addRows(batch.columns, ids, count);
    return this.#addParts(batch.columns, ids ?? this.#identity, count, first);
  }

  #addRows(columns: readonly Vector[], ids: Uint16Array | null, count: number): MaybePromise<void> {
    if (this.mode === "keys") {
      const gate = this.#firstThreshold;
      if (gate?.ready) {
        const vector = columns[gate.key.column];
        if (vector !== undefined) {
          const n = gate.filter(vector, ids, count, this.groupKept);
          this.profile.rejected += count - n;
          if (n === 0) return;
          if (n !== count) return this.#addKeyed(columns, this.groupKept.subarray(0, n), n);
        }
      }
      const groups = this.#groups,
        part = this.partEncoder;
      if (groups !== null && part !== null && this.#groupFirstThresholds.length > 0) {
        const encoded = part.encode(columns, ids, 0, count);
        const first = this.spec.keys[this.spec.partition];
        const vector = first === undefined ? undefined : columns[first.column];
        if (encoded.done === count && vector !== undefined) {
          const bytes = part.keys.writer.bytes,
            offsets = part.keys.offsets;
          let kept = 0;
          for (let i = 0; i < count; i++) {
            const id = groups.find(bytes, offsets[i] ?? 0, offsets[i + 1] ?? 0);
            const row = ids === null ? i : (ids[i] ?? 0);
            if (id < 0 || this.#groupFirstThresholds[id]?.accepts(vector, row) !== false)
              this.groupKept[kept++] = row;
          }
          this.profile.rejected += count - kept;
          if (kept === 0) return;
          if (kept < count) return this.#addKeyed(columns, this.groupKept.subarray(0, kept), kept);
        }
      }
      return this.#addKeyed(columns, ids, count);
    }
    if (this.mode === "ranks" && !this.#collect(columns, ids, count)) this.#toCompare();
    return this.#append(columns, ids, count, [], 0);
  }

  /**
   * A batch with more than `ARRIVAL_BYTES` of varlen values arrives in parts of at most that
   * (one row at least), a task yield and the abort check between them: its encoding, filtering,
   * decoding and copying are bounded by bytes, not rows (R23.2). The parts are copies.
   */
  async #addParts(
    columns: readonly Vector[],
    rows: Uint16Array,
    count: number,
    first: number,
  ): Promise<void> {
    let at = 0;
    let n = first;
    for (;;) {
      const done = this.#addRows(columns, rows.slice(at, at + n), n);
      if (isPromise(done)) await done;
      at += n;
      if (at >= count) return;
      await yieldTask();
      this.checkAborted();
      n = partRows(columns, rows, at, count - at);
    }
  }

  #addKeyed(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    count: number,
  ): MaybePromise<void> {
    if (this.mode !== "keys") return this.#append(columns, ids, count, [], 0);
    if (this.directEncoder !== null) return this.#addDirect(columns, ids, count);
    const encoder = this.encoder;
    if (encoder === null) return;
    let rows: Uint16Array | null = ids;
    let n = count;
    const threshold = this.#threshold;
    if (threshold !== null && this.spec.partition === 0) {
      // Top-N: rows whose key cannot beat the k-th best so far are not stored at all.
      const encoded = encoder.encode(columns, ids, 0, count);
      if (encoded.done < count) return this.#split(columns, ids, count, encoded);
      const keys = encoder.keys;
      let m = 0;
      for (let k = 0; k < count; k++) {
        const order = compareRanges(
          keys.writer.bytes,
          keys.offsets[k] ?? 0,
          keys.offsets[k + 1] ?? 0,
          threshold,
          0,
          threshold.length,
        );
        if (order < 0 || (order === 0 && this.spec.withTies))
          this.keptRows[m++] = ids === null ? k : (ids[k] ?? 0);
      }
      this.profile.rejected += count - m;
      if (m === 0) return;
      rows = this.keptRows.subarray(0, m);
      n = m;
    }
    const encoded = encoder.encode(columns, rows, 0, n);
    if (encoded.done < n) return this.#split(columns, rows, n, encoded);
    const tail = encoder.tail;
    const offsets = encoder.keys.offsets;
    for (let k = 0; k < n; k++) {
      const length = (offsets[k + 1] ?? 0) - (offsets[k] ?? 0);
      if (length > this.#maxKey) this.#maxKey = length;
    }
    if (this.partEncoder !== null) {
      const part = this.partEncoder.encode(columns, rows, 0, n);
      if (part.done < n) return this.#split(columns, rows, n, part);
      const offsets = this.partEncoder.keys.offsets;
      for (let k = 0; k < n; k++) this.partLengths[k] = (offsets[k + 1] ?? 0) - (offsets[k] ?? 0);
      tail.push(partLengthVector(this.partLengths, n));
      if (this.#groupThresholds.length > 0 && this.#filterGroups(rows, n)) {
        // Some rows lost to their partition's k-th key: encode the survivors again.
        const kept = this.groupKept.subarray(0, this.#groupKeptCount);
        this.profile.rejected += n - kept.length;
        if (kept.length === 0) return;
        return this.#addKeyedRows(columns, kept);
      }
    }
    return this.#append(columns, rows, n, tail, 0);
  }

  /** Re-encodes and appends rows that passed the per-group thresholds. */
  #addKeyedRows(columns: readonly Vector[], rows: Uint16Array): MaybePromise<void> {
    const encoder = this.encoder;
    const part = this.partEncoder;
    if (encoder === null || part === null) return;
    const n = rows.length;
    const encoded = encoder.encode(columns, rows, 0, n);
    if (encoded.done < n) return this.#split(columns, rows, n, encoded);
    const parted = part.encode(columns, rows, 0, n);
    if (parted.done < n) return this.#split(columns, rows, n, parted);
    const offsets = part.keys.offsets;
    for (let k = 0; k < n; k++) this.partLengths[k] = (offsets[k + 1] ?? 0) - (offsets[k] ?? 0);
    const tail = encoder.tail;
    tail.push(partLengthVector(this.partLengths, n));
    return this.#append(columns, rows, n, tail, 0);
  }

  /**
   * Per-group top-k: keeps (in `groupKept`) the rows whose key beats their partition's k-th key
   * so far; true when some were dropped. Keys and partition lengths are in the encoders.
   */
  #filterGroups(rows: Uint16Array | null, n: number): boolean {
    const groups = this.#groups;
    const encoder = this.encoder;
    if (groups === null || encoder === null) return false;
    const bytes = encoder.keys.writer.bytes;
    const offsets = encoder.keys.offsets;
    let m = 0;
    for (let k = 0; k < n; k++) {
      const start = offsets[k] ?? 0;
      const end = offsets[k + 1] ?? 0;
      const id = groups.find(bytes, start, start + (this.partLengths[k] ?? 0));
      const threshold = id < 0 ? undefined : this.#groupThresholds[id];
      if (
        threshold === undefined ||
        compareRanges(bytes, start, end, threshold, 0, threshold.length) <
          (this.spec.withTies ? 1 : 0)
      )
        this.groupKept[m++] = rows === null ? k : (rows[k] ?? 0);
    }
    this.#groupKeptCount = m;
    return m < n;
  }

  /** Direct mode: words straight from the vectors; top-N rows that cannot win are dropped. */
  #addDirect(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    count: number,
  ): MaybePromise<void> {
    const encoder = this.directEncoder;
    if (encoder === null) return;
    encoder.encode(columns, ids, 0, count);
    const threshold = this.#thresholdWords;
    if (threshold === null) return this.#append(columns, ids, count, encoder.tail, 0);
    const prefixes = encoder.prefixes;
    let m = 0;
    for (let k = 0; k < count; k++) {
      let order = 0;
      for (let w = 0; w < PREFIX_WORDS && order === 0; w++) {
        const x = (prefixes[w]?.[k] ?? 0) >>> 0;
        const y = threshold[w] ?? 0;
        if (x !== y) order = x < y ? -1 : 1;
      }
      if (order < 0 || (order === 0 && this.spec.withTies))
        this.keptRows[m++] = ids === null ? k : (ids[k] ?? 0);
    }
    this.profile.rejected += count - m;
    if (m === 0) return;
    if (m === count) return this.#append(columns, ids, count, encoder.tail, 0);
    const kept = this.keptRows.slice(0, m);
    encoder.encode(columns, kept, 0, m);
    return this.#append(columns, kept, m, encoder.tail, 0);
  }

  /**
   * Appends rows `done … n` (by `ids`, or 0 … n) with their dense key tail; when the grant
   * refuses, makes room (compaction or a run) and resumes at the row it reached.
   */
  #append(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    n: number,
    tail: readonly Vector[],
    done: number,
  ): MaybePromise<void> {
    if (this.mode !== "keys" && this.#keyStorage) tail = this.#emptyTail;
    // Rows stored before compare mode get their operands first (or the store is spilled).
    const decoding = this.mode === "compare" && this.decodes;
    while (done < n && (!decoding || this.#decodeStore() === 0)) {
      const want = Math.min(n - done, 2048);
      if (!this.#reserve(this.store.rows + want)) break;
      // Top-N: rows go in only while the next compaction's store stays in reserve.
      if (
        this.#rowStep === null &&
        this.#reservesCompaction &&
        !this.#holdCompaction(columns, ids, done, want)
      )
        break;
      const before = this.store.rows;
      const taken = this.store.append(
        columns,
        ids,
        done,
        want,
        tail.length > 0 ? { vectors: tail, from: done } : undefined,
      );
      done += taken;
      if (decoding && this.#decodeStore() > 0) {
        // Rows whose operands do not fit go back out, and in again once room is made.
        const decoded = this.operands.rows;
        this.store.truncate(decoded);
        done -= before + taken - decoded;
        break;
      }
      if (taken < want) break;
    }
    if (done >= n) {
      // A legacy source may itself need credit for its next batch. Finish this complete
      // ingestion/spill step before returning readiness, rather than retaining its loan.
      if (this.#rowStep !== null) return this.#spillRun();
      return this.#maybeCompact();
    }
    const row = ids === null ? done : (ids[done] ?? 0);
    return this.#makeRoom(columns, row, tail, done).then((written) =>
      this.#append(columns, ids, n, tail, done + Number(written)),
    );
  }

  /**
   * Top-N: holds the next compaction's store for rows `from … from + count` arriving; false
   * when the grant refused and compaction would shrink the store (the caller makes room, by
   * compacting). A grant that cannot hold the reserve beside `keep` rows ends in a run, as
   * without it, and reserving stops: after a run, compaction no longer applies.
   */
  #holdCompaction(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    from: number,
    count: number,
  ): boolean {
    // After a run, compaction no longer applies (the threshold filters arrivals instead).
    if (this.runs.length > 0) {
      this.#releaseCompaction();
      return true;
    }
    const keep = this.spec.keep ?? 0;
    let widest = this.#maxKey;
    for (let v = 0; v < this.#varlenColumns.length; v++) {
      const vector = columns[this.#varlenColumns[v] ?? 0];
      let max = this.#maxVarlen[v] ?? 0;
      if (vector !== undefined)
        for (let k = 0; k < count; k++) {
          const bytes = varlenBytes(vector, ids === null ? from + k : (ids[from + k] ?? 0));
          if (bytes > max) max = bytes;
        }
      this.#maxVarlen[v] = max;
      widest += max;
    }
    const need = this.store.fixedBytes(keep) + 2 * keep * widest;
    if (need <= this.#compactionHeld) return true;
    if (this.charge.allocate(need - this.#compactionHeld)) {
      this.#compactionHeld = need;
      return true;
    }
    return this.store.rows <= keep;
  }

  #releaseCompaction(): void {
    if (this.#compactionHeld > 0) this.charge.free(this.#compactionHeld);
    this.#compactionHeld = 0;
  }

  #collect(columns: readonly Vector[], ids: Uint16Array | null, count: number): boolean {
    for (let k = 0; k < this.spec.keys.length; k++) {
      const set = this.#sets[k];
      const key = this.spec.keys[k];
      if (set === null || set === undefined || key === undefined) continue;
      const vector = columns[key.column];
      if (vector === undefined) continue;
      for (let i = 0; i < count; i++) {
        const value = textAt(vector, ids === null ? i : (ids[i] ?? 0));
        if (value !== null && set.add(value.bytes, value.start, value.end) < 0) return false;
      }
    }
    return true;
  }

  #toCompare(): void {
    for (const set of this.#sets) set?.close();
    this.#sets = this.#sets.map(() => null);
    this.mode = "compare";
    this.profile.mode = "compare";
  }

  /** Reserves sort scratch for `rows` stored rows; false when the grant refuses. */
  #reserve(rows: number): boolean {
    const want = Math.ceil(rows / 2048) * 2048 * SCRATCH_PER_ROW;
    if (want <= this.#reserved) return true;
    if (!this.charge.allocate(want - this.#reserved)) return false;
    this.#reserved = want;
    return true;
  }

  /**
   * The key bytes of `count` rows did not fit the encoder: the rows go in as parts (those that
   * were encoded, then the rest), each encoded alone. A refused single key abandons byte
   * normalization for exact value comparison before any progress request. The parts are copies,
   * so no scratch is shared across awaits.
   */
  #split(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    count: number,
    encoded: { done: number; requiredBytes: number },
  ): MaybePromise<void> {
    if (count === 1) {
      // A key loan followed by a row/page loan is hold-and-wait. Abandon normalization before
      // making any further request; the exact value comparator orders existing and new runs.
      this.encoder?.reset();
      this.partEncoder?.reset();
      this.pool.settle();
      this.#toCompare();
      return this.#append(columns, ids, 1, this.#emptyTail, 0);
    }
    const rows = ids ?? this.#identity;
    const cut = encoded.done > 0 ? encoded.done : count >>> 1;
    const first = rows.slice(0, cut);
    const rest = rows.slice(cut, count);
    const done = this.#addKeyed(columns, first, first.length);
    if (isPromise(done)) return done.then(() => this.#addKeyed(columns, rest, rest.length));
    return this.#addKeyed(columns, rest, rest.length);
  }

  /**
   * Top-N / per-group: compacts the buffer once it holds a few times what is kept, in slices
   * (the push blocks while a long compaction yields).
   */
  #maybeCompact(): MaybePromise<void> {
    const keep = this.spec.keep;
    if (keep === null || this.mode !== "keys" || this.runs.length > 0) return;
    const floor = this.spec.partition > 0 ? 65_536 : keep + 4096;
    if (this.store.rows < Math.max(2 * keep, floor, 4 * this.#lastSurvivors)) return;
    const done = this.sliced(this.compactJob());
    if (isPromise(done)) return done.then(() => undefined);
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

  /** Between merge and run steps: a task yield and the abort check once the slice is due. */
  pace(): Promise<void> | null {
    if (!this.slice.due()) return null;
    return yieldTask().then(() => {
      this.checkAborted();
    });
  }

  /** Keeps the best `keep` rows (per partition when grouped); false when it did not fit. */
  compact(): boolean {
    return runToEnd(this.compactJob(new Slice(Infinity)));
  }

  /** `compact` as a sliced job (sort, cut, then the survivors copied in bounded steps). */
  *compactJob(slice: Slice = this.slice): SlicedJob<boolean> {
    const keep = this.spec.keep;
    if (keep === null || this.mode !== "keys") return false;
    const ids = yield* this.sortJob(slice);
    if (ids === null) return false;
    const survivors = yield* this.cutJob(ids, slice);
    if (survivors.length === this.store.rows) {
      this.#lastSurvivors = survivors.length;
      return true;
    }
    // The reserve becomes the new store (re-held as rows arrive after it).
    this.#releaseCompaction();
    const next = this.#newStore();
    const columns = this.storeTypes.map((_, c) => c);
    let taken = 0;
    while (taken < survivors.length) {
      const want = Math.min(SLICE_ROWS, survivors.length - taken);
      const got = next.appendFrom(this.store, columns, survivors, taken, want);
      taken += got;
      if (got < want) break;
      if (slice.due()) yield;
    }
    if (taken < survivors.length) {
      next.close();
      return false;
    }
    if (this.spec.partition === 0 && keep > 0 && survivors.length >= keep) {
      const id = survivors[keep - 1] ?? 0;
      const gate = this.#firstThreshold;
      if (gate !== null)
        gate.capture(
          this.store.chunk(gate.key.column, id >>> this.store.chunkShift),
          id & this.store.chunkMask,
        );
      if (this.direct) this.#thresholdWords = this.#wordsOf(id);
      else {
        const [bytes, start, end] = keyOf(this.store, this.keyColumn, id);
        this.#threshold = bytes.slice(start, end);
      }
    }
    if (this.spec.partition > 0) yield* this.#setGroupThresholds(survivors, keep, slice);
    this.store.close();
    this.store = next;
    this.#lastSurvivors = survivors.length;
    this.profile.compactions++;
    return true;
  }

  /** Prefix words of store row `id`. */
  #wordsOf(id: number): Uint32Array {
    const store = this.store;
    const words = new Uint32Array(PREFIX_WORDS);
    for (let w = 0; w < PREFIX_WORDS; w++)
      words[w] = valueAt32(store, this.prefixColumn + w, id) >>> 0;
    return words;
  }

  /** After a per-group compaction: each partition with k survivors gets its k-th key. */
  *#setGroupThresholds(survivors: Int32Array, keep: number, slice: Slice): SlicedJob<void> {
    const groups = this.#groups;
    if (groups === null || this.#groupThresholdsOff) return;
    const store = this.store;
    let inGroup = 0;
    for (let i = 0; i < survivors.length; i++) {
      const id = survivors[i] ?? 0;
      if (i > 0 && !this.samePartitionIds(survivors[i - 1] ?? 0, id)) inGroup = 0;
      inGroup++;
      if (slice.tick()) yield;
      if (inGroup !== keep) continue;
      const [bytes, start, end] = keyOf(store, this.keyColumn, id);
      const length = valueAt32(store, this.prefixColumn + PREFIX_WORDS, id);
      const group = groups.add(bytes, start, start + length);
      // Charged before the copy; the old one (overlapping it) goes back after.
      if (group < 0 || !this.charge.allocate(end - start)) {
        this.#dropGroupThresholds();
        return;
      }
      const old = this.#groupThresholds[group];
      const first = this.spec.keys[this.spec.partition];
      if (first !== undefined && directWidth(this.spec.types, [first]) !== null) {
        let gate = this.#groupFirstThresholds[group];
        if (gate === undefined && this.charge.allocate(FirstKeyThreshold.BYTES)) {
          gate = new FirstKeyThreshold(first);
          this.#groupFirstThresholds[group] = gate;
          this.#thresholdBytes += FirstKeyThreshold.BYTES;
        }
        gate?.capture(store.chunk(first.column, id >>> store.chunkShift), id & store.chunkMask);
      }
      this.#groupThresholds[group] = bytes.slice(start, end);
      this.#thresholdBytes += end - start;
      if (this.#thresholdBytes > this.profile.thresholdBytes)
        this.profile.thresholdBytes = this.#thresholdBytes;
      if (old !== undefined) {
        this.charge.free(old.length);
        this.#thresholdBytes -= old.length;
      }
    }
  }

  /** Frees every threshold copy and stops making them (no row is lost: none is filtered). */
  #dropGroupThresholds(): void {
    if (this.#thresholdBytes > 0) this.charge.free(this.#thresholdBytes);
    this.#thresholdBytes = 0;
    this.#groupThresholds = [];
    this.#groupFirstThresholds = [];
    this.#groupThresholdsOff = true;
  }

  /** From sorted ids, the ones to keep: the first `keep` (with ties), or per partition. */
  *cutJob(ids: Int32Array, slice: Slice): SlicedJob<Int32Array> {
    const keep = this.spec.keep;
    if (keep === null) return ids;
    if (this.spec.partition === 0) {
      let n = Math.min(keep, ids.length);
      if (this.spec.withTies && n > 0)
        while (n < ids.length && this.compareIds(ids[n - 1] ?? 0, ids[n] ?? 0) === 0) {
          n++;
          if (slice.tick()) yield;
        }
      return ids.subarray(0, n);
    }
    let m = 0,
      inGroup = 0,
      rank = 1,
      previous = -1;
    for (const id of ids) {
      if (previous < 0 || !this.samePartitionIds(previous, id)) {
        inGroup = 0;
        rank = 1;
      } else if (this.spec.withTies && this.compareIds(previous, id) !== 0) rank = inGroup + 1;
      if (inGroup < keep || (this.spec.withTies && rank <= keep)) ids[m++] = id;
      previous = id;
      inGroup++;
      if (slice.tick()) yield;
    }
    return ids.subarray(0, m);
  }

  /** Order of store rows a, b. */
  compareIds(a: number, b: number): number {
    const store = this.store;
    if (this.direct) {
      for (let w = 0; w < PREFIX_WORDS; w++) {
        const x = valueAt32(store, this.prefixColumn + w, a) >>> 0;
        const y = valueAt32(store, this.prefixColumn + w, b) >>> 0;
        if (x !== y) return x < y ? -1 : 1;
      }
      return 0;
    }
    if (this.mode === "keys") {
      const shift = store.chunkShift;
      const mask = store.chunkMask;
      const x = store.chunk(this.keyColumn, a >>> shift).data as {
        bytes: Uint8Array;
        offsets: Int32Array;
      };
      const y = store.chunk(this.keyColumn, b >>> shift).data as {
        bytes: Uint8Array;
        offsets: Int32Array;
      };
      const i = a & mask;
      const j = b & mask;
      return compareRanges(
        x.bytes,
        x.offsets[i] ?? 0,
        x.offsets[i + 1] ?? 0,
        y.bytes,
        y.offsets[j] ?? 0,
        y.offsets[j + 1] ?? 0,
      );
    }
    return compareRows(
      this.spec.types,
      this.spec.keys,
      chunkColumns(store, a >>> store.chunkShift),
      a & store.chunkMask,
      chunkColumns(store, b >>> store.chunkShift),
      b & store.chunkMask,
      this.operands,
      this.operands,
      a,
      b,
    );
  }

  /**
   * Compare mode: decodes the operands of resident rows not yet decoded (charged first); 0, or
   * the bytes the charge refused (what is decoded stays).
   */
  #decodeStore(): number {
    return runToEnd(this.#decodeStoreJob(new Slice(Infinity)));
  }

  /**
   * `#decodeStore` as a sliced job: rows are decoded in ranges of at most `DECODE_BYTES` of
   * operands (a decode's cost is its bytes), the deadline checked after each.
   */
  *#decodeStoreJob(slice: Slice): SlicedJob<number> {
    if (this.mode !== "compare" || !this.decodes) return 0;
    const store = this.store;
    const operands = this.operands;
    const { types, keys } = this.spec;
    while (operands.rows < store.rows) {
      const from = operands.rows & store.chunkMask;
      const end = Math.min(store.chunkRows, from + store.rows - operands.rows);
      const columns = chunkColumns(store, operands.rows >>> store.chunkShift);
      let to = from + 1;
      let bytes = operandBytes(types, keys, columns, from, to);
      while (to < end && bytes < DECODE_BYTES) {
        bytes += operandBytes(types, keys, columns, to, to + 1);
        to++;
      }
      let needed = decodeOperands(types, keys, columns, from, to, this.charge, operands);
      if (needed > 0 && to - from > 1) {
        // Refused as a whole: as many single rows as fit (a loaned row always gets its own).
        needed = 0;
        for (let row = from; row < to && needed === 0; row++)
          needed = decodeOperands(types, keys, columns, row, row + 1, this.charge, operands);
      }
      if (needed > 0) return needed;
      if (slice.due()) yield;
    }
    if (operands.bytes > this.profile.operandBytes) this.profile.operandBytes = operands.bytes;
    return 0;
  }

  /** Bytes the resident rows' missing operands would charge. */
  #missingOperandBytes(): number {
    const store = this.store;
    let bytes = 0;
    for (let row = this.operands.rows; row < store.rows;) {
      const from = row & store.chunkMask;
      const to = Math.min(store.chunkRows, from + store.rows - row);
      bytes += operandBytes(
        this.spec.types,
        this.spec.keys,
        chunkColumns(store, row >>> store.chunkShift),
        from,
        to,
      );
      row += to - from;
    }
    return bytes;
  }

  samePartitionIds(a: number, b: number): boolean {
    if (this.spec.partition === 0) return true;
    const store = this.store;
    const shift = store.chunkShift;
    const mask = store.chunkMask;
    if (this.mode !== "keys")
      return (
        compareRows(
          this.spec.types,
          this.#partKeys,
          chunkColumns(store, a >>> shift),
          a & mask,
          chunkColumns(store, b >>> shift),
          b & mask,
          this.operands,
          this.operands,
          a,
          b,
        ) === 0
      );
    // Direct reads: the partition key length, then that prefix of the key bytes.
    const plen = this.prefixColumn + PREFIX_WORDS;
    const la = valueAt32(store, plen, a);
    if (la !== valueAt32(store, plen, b)) return false;
    const x = store.chunk(this.keyColumn, a >>> shift).data as {
      bytes: Uint8Array;
      offsets: Int32Array;
    };
    const y = store.chunk(this.keyColumn, b >>> shift).data as {
      bytes: Uint8Array;
      offsets: Int32Array;
    };
    const xs = x.offsets[a & mask] ?? 0;
    const ys = y.offsets[b & mask] ?? 0;
    return compareRanges(x.bytes, xs, xs + la, y.bytes, ys, ys + la) === 0;
  }

  /** Whether two rows (page or held vectors, with their operands) are in one partition. */
  samePartition(
    a: readonly Vector[],
    i: number,
    b: readonly Vector[],
    j: number,
    ao: Operands | null = null,
    bo: Operands | null = null,
  ): boolean {
    if (this.spec.partition === 0) return true;
    if (this.mode !== "keys")
      return compareRows(this.spec.types, this.#partKeys, a, i, b, j, ao, bo) === 0;
    const plen = this.prefixColumn + PREFIX_WORDS;
    const la = int32At(a[plen], i);
    if (la !== int32At(b[plen], j)) return false;
    const x = varlenOf(a[this.keyColumn]);
    const y = varlenOf(b[this.keyColumn]);
    const xs = x.offsets[i] ?? 0;
    const ys = y.offsets[j] ?? 0;
    return compareRanges(x.bytes, xs, xs + la, y.bytes, ys, ys + la) === 0;
  }

  /** Whether two rows (page or held vectors, with their operands) have equal keys (ties). */
  sameKey(
    a: readonly Vector[],
    i: number,
    b: readonly Vector[],
    j: number,
    ao: Operands | null = null,
    bo: Operands | null = null,
  ): boolean {
    if (this.mode !== "keys")
      return compareRows(this.spec.types, this.spec.keys, a, i, b, j, ao, bo) === 0;
    if (this.direct) {
      for (let w = 0; w < PREFIX_WORDS; w++)
        if (int32At(a[this.prefixColumn + w], i) !== int32At(b[this.prefixColumn + w], j))
          return false;
      return true;
    }
    const x = varlenOf(a[this.keyColumn]);
    const y = varlenOf(b[this.keyColumn]);
    return (
      compareRanges(
        x.bytes,
        x.offsets[i] ?? 0,
        x.offsets[i + 1] ?? 0,
        y.bytes,
        y.offsets[j] ?? 0,
        y.offsets[j + 1] ?? 0,
      ) === 0
    );
  }

  /**
   * Makes room for row `row`: compacts (buffered top-N) when that drops rows, else writes the rows
   * in memory as a run. A row refused by an empty store becomes a page directly from its
   * borrowed input, without pinning a second resident copy or a row-store progress loan.
   */
  async #makeRoom(
    columns: readonly Vector[],
    row: number,
    tail: readonly Vector[],
    k: number,
  ): Promise<boolean> {
    this.checkAborted();
    const before = this.store.rows;
    if (
      this.spec.keep !== null &&
      this.mode === "keys" &&
      this.runs.length === 0 &&
      (await this.sliced(this.compactJob())) &&
      this.store.rows < before
    )
      return false;
    if (this.store.rows > 0) {
      await this.#spillRun();
      return false;
    }
    // A failed append can have allocated an empty chunk. Drop it before requesting a page.
    this.store.truncate(0);
    if (this.mode === "keys" && this.encoder !== null) {
      this.encoder.reset();
      this.partEncoder?.reset();
      this.pool.settle();
      this.#toCompare();
      tail = this.#emptyTail;
      k = 0;
    }
    let payload = 0;
    for (const column of this.#varlenColumns) {
      const vector = columns[column];
      if (vector !== undefined) payload += varlenBytes(vector, row);
    }
    const boundedCoefficients = this.spec.types.every(
      (type) => type.kind !== "decimal" || (type.precision !== null && type.precision <= 38),
    );
    if (!this.decodes && boundedCoefficients && payload <= 64 * 1024) {
      if (this.#rowStep !== null) throw new RangeError("A prepaid sort row could not be stored");
      this.#releaseCompaction();
      const options = this.#stagingOptions();
      const staging = new ColumnStore(this.storeTypes, this.pool, options);
      // Staging metadata, bounded payload/page resize overlap and fixed bookkeeping are
      // reserved together with the resident chunk and its sorting scratch. The pool takes
      // its share from that SAME loan; no second resource is requested while it is held.
      const ioBytes =
        5 * staging.fixedBytes(options.chunkRows) + 4 * options.targetBytes + 64 * 1024;
      staging.close();
      const bytes =
        this.store.appendNeed(columns, row) +
        2048 * SCRATCH_PER_ROW +
        64 * 1024 +
        Math.max(0, ioBytes - this.pool.room);
      const loan = await this.context.grant.borrow(bytes, this.context.signal);
      try {
        this.checkAborted();
      } catch (error) {
        loan.repay();
        throw error;
      }
      this.#rowStep = { loan, release: this.pool.pinStep() };
      this.profile.loans++;
      await this.io.need(ioBytes);
      this.checkAborted();
      return false;
    }
    const run = new SpilledRun();
    this.#observeWide(columns, row);
    await this.#writeRow(run, columns, row, tail, k);
    this.runs.push(run);
    this.profile.runs++;
    this.profile.spilledBytes = this.files.written;
    if (this.mode === "ranks") this.#toCompare();
    return true;
  }

  /** Actual variable-width payload, excluding a staging chunk's fixed capacity/metadata. */
  #rowPayloadBytes(store: ColumnStore, row: number): number {
    const chunk = row >>> store.chunkShift,
      slot = row & store.chunkMask;
    let bytes = 0;
    for (const column of this.#varlenColumns)
      bytes += varlenBytes(store.chunk(column, chunk), slot);
    if (this.#keyStorage && !this.direct)
      bytes += varlenBytes(store.chunk(this.keyColumn, chunk), slot);
    return bytes;
  }

  #observePageKeys(columns: readonly Vector[], rows: number): void {
    if (!this.#keyStorage || this.direct) return;
    const keys = columns[this.keyColumn];
    if (keys !== undefined)
      for (let row = 0; row < rows; row++)
        this.#pageKeyBytes = Math.max(this.#pageKeyBytes, varlenBytes(keys, row));
  }

  /** Wide output leaves before the merge advances or aligns its next input page. */
  get wide(): boolean {
    return this.#wideOutputBytes > 0;
  }

  #observeWide(columns: readonly Vector[], row: number): void {
    const output = new ColumnStore(this.spec.types, this.pool, { targetBytes: BATCH_TARGET_BYTES });
    this.#wideOutputBytes = Math.max(this.#wideOutputBytes, output.appendNeed(columns, row));
  }

  /** Borrow the input row until its one-row page is appended; never retain a row-store loan. */
  async #writeRow(
    run: SpilledRun,
    columns: readonly Vector[],
    row: number,
    tail: readonly Vector[] = [],
    key = 0,
  ): Promise<void> {
    const metadata = SORT_ROW_VIEW_BYTES * (columns.length + tail.length);
    if (!this.pool.allocate(metadata))
      throw new RangeError("Sort row views exceeded their admitted minimum");
    const views: FlatVector[] = [];
    try {
      for (const column of columns) views.push(sortRowView(column, row));
      for (const column of tail) views.push(sortRowView(column, key));
      this.#observePageKeys(views, 1);
      if (this.mode !== "keys" && this.decodes)
        this.#pageOperands = Math.max(
          this.#pageOperands,
          operandBytes(this.spec.types, this.spec.keys, views, 0, 1),
        );
      const header = {
        queryId: this.context.queryId,
        run: this.runs.length,
        partition: 0,
        sequence: run.pages.length,
      };
      let page = this.pages.encode(header, views, 1);
      if (page === null) {
        await this.io.need(this.pages.growthFor(views, 1));
        this.checkAborted();
        page = this.pages.encode(header, views, 1);
        if (page === null) throw new RangeError("A single sort row could not be encoded");
      }
      await this.files.append(run, page, 1);
    } finally {
      views.length = 0;
      this.pages.shrinkIf(true);
      this.pool.settle();
      this.pool.free(metadata);
    }
  }

  // --- sorting ---------------------------------------------------------------------------

  /**
   * The in-memory sort as a sliced job (see `msdSort`, `mergeSortJob`): row ids of the
   * store in order, or null when its memory (rank arrays, operands) was refused. Its scratch was
   * reserved as the rows arrived.
   */
  *sortJob(slice: Slice = this.slice): SlicedJob<Int32Array | null> {
    const store = this.store;
    const n = store.rows;
    if (!this.#reserve(n)) return null;
    const ids = new Int32Array(n);
    if (this.mode === "keys") {
      if (this.direct && n >= 65_536) {
        // Normalized fixed-width words already live in columns. Keep this layout and scatter
        // row ids, rather than constructing/moving six-word tuples on every digit pass.
        // 5 word columns + ids + temporary = 28 bytes/row, within the existing tuple reserve.
        const columns = Array.from({ length: PREFIX_WORDS }, () => new Uint32Array(n));
        for (let w = 0; w < PREFIX_WORDS; w++) {
          const column = columns[w];
          if (column === undefined) throw new Error("Missing normalized sort column");
          for (let k = 0; k < store.chunkCount; k++) {
            const values = (store.chunk(this.prefixColumn + w, k).data as { values: Int32Array })
              .values;
            column.set(
              values.subarray(0, store.chunk(this.prefixColumn + w, k).length),
              k * store.chunkRows,
            );
            if (slice.due()) yield;
          }
        }
        yield* lsdSortIds(columns, ids, new Int32Array(n), n, slice, this.radix);
        return ids;
      }
      // (prefix words, row id) tuples read straight from the store's word columns, then an MSD
      // radix over them; 4 + 2 × 4 × TUPLE_WORDS bytes per row, inside the reserve.
      const S = TUPLE_WORDS;
      const tuples = new Uint32Array(n * S);
      // Written through a signed view of the same words (the stored words are int32).
      const words = new Int32Array(tuples.buffer);
      const prefix = (w: number, k: number): Int32Array =>
        (store.chunk(this.prefixColumn + w, k).data as { values: Int32Array }).values;
      for (let k = 0; k < store.chunkCount; k++) {
        const base = k * store.chunkRows;
        const length = store.chunk(0, k).length;
        // Row by row (one pass over the tuples); PREFIX_WORDS is 5.
        const [p0, p1, p2, p3, p4] = [0, 1, 2, 3, 4].map((w) => prefix(w, k)) as [
          Int32Array,
          Int32Array,
          Int32Array,
          Int32Array,
          Int32Array,
        ];
        for (let i = 0; i < length; i++) {
          const at = (base + i) * S;
          words[at] = p0[i] ?? 0;
          words[at + 1] = p1[i] ?? 0;
          words[at + 2] = p2[i] ?? 0;
          words[at + 3] = p3[i] ?? 0;
          words[at + 4] = p4[i] ?? 0;
          words[at + 5] = base + i;
        }
        if (slice.due()) yield;
      }
      const covered = this.direct || this.#maxKey <= 4 * PREFIX_WORDS;
      yield* msdSort(
        tuples,
        new Uint32Array(n * S),
        n,
        covered ? null : (a, b) => this.compareIds(a, b),
        slice,
        this.radix,
        // Its stack (at most n / 49 ranges of 3 words): the reserve's last byte per row.
        new Int32Array(msdStackWords(n)),
      );
      for (let i = 0; i < n; i++) {
        ids[i] = tuples[i * S + PREFIX_WORDS] ?? 0;
        if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
      }
      return ids;
    }
    for (let i = 0; i < n; i++) ids[i] = i;
    // Inside the reserve (ids and the merge buffer are 8 of its bytes per row).
    const temp = new Int32Array(n);
    if (this.mode === "ranks") {
      const order = yield* this.#rankOrder(slice);
      if (order !== null) {
        yield* mergeSortJob(ids, temp, order, slice);
        return ids;
      }
      this.#toCompare();
    }
    if ((yield* this.#decodeStoreJob(slice)) > 0) return null;
    yield* mergeSortJob(ids, temp, (a, b) => this.compareIds(a, b), slice);
    return ids;
  }

  /**
   * Ranks mode (C8): a comparator over query-wide ranks of the collated keys, or null when the
   * ranks or their decoded strings do not fit (compare mode then). Sliced like the sort.
   */
  *#rankOrder(slice: Slice): SlicedJob<((a: number, b: number) => number) | null> {
    const store = this.store;
    const n = store.rows;
    const ranks: Array<Int32Array | null> = [];
    const decoder = new TextDecoder();
    for (let k = 0; k < this.spec.keys.length; k++) {
      const set = this.#sets[k];
      const key = this.spec.keys[k];
      if (set === null || set === undefined || key === undefined) {
        ranks.push(null);
        continue;
      }
      let charged = 0;
      try {
        const strings: string[] = [];
        for (let id = 0; id < set.size; id++) {
          const value = decoder.decode(set.bytes(id));
          const bytes = stringBytes(value);
          if (!this.charge.allocate(bytes)) return null;
          charged += bytes;
          strings.push(value);
          // A decode's cost is the value's bytes: each one counts.
          if (slice.tick()) yield;
        }
        // The order, its merge buffer and the rank of each value: charged while they live.
        if (!this.charge.allocate(3 * set.size * 4)) return null;
        charged += 3 * set.size * 4;
        const collator = collatorFor(key.collation ?? "");
        const order = Int32Array.from({ length: set.size }, (_, id) => id);
        yield* mergeSortJob(
          order,
          new Int32Array(set.size),
          (a, b) => {
            const c = collator.compare(strings[a] ?? "", strings[b] ?? "");
            if (c !== 0) return c;
            return compareRanges(
              set.arena,
              set.starts[a] ?? 0,
              set.ends[a] ?? 0,
              set.arena,
              set.starts[b] ?? 0,
              set.ends[b] ?? 0,
            );
          },
          slice,
        );
        if (!this.charge.allocate(n * 4)) return null;
        this.#rankBytes += n * 4;
        const rankOf = new Int32Array(set.size);
        for (let rank = 0; rank < order.length; rank++) {
          rankOf[order[rank] ?? 0] = rank;
          if ((rank & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
        }
        const perRow = new Int32Array(n).fill(-1);
        for (let c = 0; c < store.chunkCount; c++) {
          const vector = store.chunk(key.column, c);
          for (let i = 0; i < vector.length; i++) {
            const value = textAt(vector, i);
            if (value !== null)
              perRow[c * store.chunkRows + i] =
                rankOf[set.find(value.bytes, value.start, value.end)] ?? 0;
            // A lookup hashes the value's bytes: each one counts.
            if (slice.tick()) yield;
          }
          if (slice.due()) yield;
        }
        ranks.push(perRow);
      } finally {
        this.charge.free(charged);
      }
    }
    const keys = this.spec.keys;
    const types = this.spec.types;
    return (a, b) => {
      for (let k = 0; k < keys.length; k++) {
        const key = keys[k];
        if (key === undefined) continue;
        const rank = ranks[k];
        if (rank !== null && rank !== undefined) {
          const x = rank[a] ?? -1;
          const y = rank[b] ?? -1;
          if (x < 0 || y < 0) {
            if (x < 0 && y < 0) continue;
            return x < 0 === (key.nulls === "first") ? -1 : 1;
          }
          if (x !== y) return (key.direction === "desc" ? y - x : x - y) < 0 ? -1 : 1;
          continue;
        }
        // Ranks mode's other keys are byte-comparable (no operands).
        const order = compareRows(
          types,
          [key],
          chunkColumns(store, a >>> store.chunkShift),
          a & store.chunkMask,
          chunkColumns(store, b >>> store.chunkShift),
          b & store.chunkMask,
        );
        if (order !== 0) return order;
      }
      return 0;
    };
  }
  #rankBytes = 0;

  // --- runs ------------------------------------------------------------------------------

  /** Spill at a batch boundary before another admitted pipeline's read can be refused. */
  async reclaim(): Promise<void> {
    // A checkpoint borrows resident chunks across writes. The phase controller
    // retires this graph after sealing, so a new reclaim must only drain that
    // borrow, never spill/truncate the same rows while it is being copied.
    if (this.#checkpointWork !== null) {
      await this.#checkpointWork;
      return;
    }
    this.checkAborted();
    if (this.store.rows > 0) await this.#spillRun();
    const free = this.context.grant.held - this.context.grant.allocated - this.context.grant.loaned;
    if (free > 0) this.context.grant.release(free);
  }

  /** Sorts the rows in memory and writes them as one run; the store is emptied. */
  async #spillRun(): Promise<void> {
    const store = this.store;
    if (store.rows === 0) return;
    let ids = await this.sliced(this.sortJob());
    if (ids === null && this.mode === "compare" && this.#operandLoan === null) {
      // Rows stored before compare mode (ranks that did not fit) lack operands the grant cannot
      // hold: one loan for them (rule 5), repaid when their run is written.
      const needed = this.#missingOperandBytes();
      this.#operandLoan = await this.context.grant.borrow(needed, this.context.signal);
      this.profile.loans++;
      ids = await this.sliced(this.sortJob());
    }
    if (ids === null) throw new RangeError("Sort scratch could not be allocated");
    const run = new SpilledRun();
    const staging = new ColumnStore(store.types, this.pool, this.#stagingOptions());
    const columns = store.types.map((_, c) => c);
    const header = { run: this.runs.length, partition: 0 };
    try {
      let at = 0;
      while (at < ids.length) {
        this.checkAborted();
        const paced = this.pace();
        if (paced !== null) await paced;
        const row = ids[at] ?? 0;
        if (this.#rowPayloadBytes(store, row) > staging.targetBytes) {
          // A wide resident row is already owned: borrow it into the page writer rather than
          // acquiring staging credit and then requesting a second loan for its page.
          await this.io.flush(staging, run, header);
          staging.reset(true);
          const source = chunkColumns(store, row >>> store.chunkShift);
          this.#observeWide(source, row & store.chunkMask);
          await this.#writeRow(run, source, row & store.chunkMask);
          at++;
          continue;
        }
        at += staging.appendFrom(store, columns, ids, at, ids.length - at);
        if (at >= ids.length) break;
        if (staging.rows > 0) await this.io.flush(staging, run, header);
        else {
          // One row larger than a page's room: room for it and its page, then it goes alone.
          await this.io.need(2 * staging.appendFromNeed(store, columns, ids[at] ?? 0) + 64 * 1024);
          at += staging.appendFrom(store, columns, ids, at, 1);
          await this.io.flush(staging, run, header);
        }
      }
      await this.io.flush(staging, run, header);
    } finally {
      staging.close();
    }
    this.runs.push(run);
    this.profile.runs++;
    this.profile.spilledBytes = this.files.written;
    store.truncate(0);
    dropOperands(this.operands, this.charge);
    // Later rows compare (a run's ranks do not carry over): their operands come as they arrive.
    if (this.mode === "ranks") this.#toCompare();
    // The scratch goes back with the rows: the next run reserves its own.
    if (this.#reserved > 0) this.charge.free(this.#reserved);
    this.#reserved = 0;
    if (this.#rankBytes > 0) {
      this.charge.free(this.#rankBytes);
      this.#rankBytes = 0;
    }
    if (this.#operandLoan !== null) {
      this.#operandLoan.repay();
      this.#operandLoan = null;
    }
    if (this.#rowStep !== null) {
      this.#rowStep.release();
      this.#rowStep.loan.repay();
      this.#rowStep = null;
    }
  }

  /**
   * Spill page staging: 64 KiB pages, or 8 KiB (128 rows) when compare mode decodes, since a
   * merge input holds its page's operands too (JSONB trees run to ~24 bytes per byte).
   */
  #stagingOptions(): { chunkRows: number; targetBytes: number; varlenBytes: number } {
    return this.mode !== "keys" && this.decodes
      ? { chunkRows: 128, targetBytes: 8 * 1024, varlenBytes: 4096 }
      : { chunkRows: 512, targetBytes: 64 * 1024, varlenBytes: 4096 };
  }

  /** Writes what is in memory as a run, then merges runs in passes down to `MAX_FAN_IN`. */
  async prepareMerge(): Promise<void> {
    if (this.store.rows > 0) await this.#spillRun();
    this.encoder?.reset();
    this.partEncoder?.reset();
    this.pool.settle();
    // Each input holds one page (and its operands) from the pool: the fan-in is what the pool's
    // room allows.
    let page = 0;
    for (const run of this.runs) for (const ref of run.pages) page = Math.max(page, ref.length);
    const input = 2 * page + this.#pageOperands;
    const fanIn =
      this.#wideOutputBytes > 0
        ? 2
        : Math.max(2, Math.min(MAX_FAN_IN, Math.floor(this.pool.room / Math.max(1, input))));
    if (this.#wideOutputBytes > 0 && this.#mergeStep === null) {
      this.#mergeStep = this.pool.pinStep();
      // Two input pages plus the larger of a spill page or one output row. No later buffer
      // asks for more credit while either cursor holds the step. Normal pages keep their path.
      const held = this.spec.heldRow
        ? this.#wideOutputBytes + 2 * this.#pageKeyBytes + 16 * 1024
        : 0;
      await this.io.need(
        2 * page +
          held +
          Math.max(page + 4096, this.#wideOutputBytes) +
          (2 + Number(this.spec.heldRow)) * this.#pageOperands +
          SORT_ROW_VIEW_BYTES * this.storeTypes.length +
          64 * 1024,
      );
      this.checkAborted();
    }
    while (this.runs.length > fanIn) {
      const inputs = this.runs.splice(0, fanIn);
      const run = new SpilledRun();
      const staging = new ColumnStore(this.storeTypes, this.pool, this.#stagingOptions());
      const header = { run: this.runs.length + fanIn, partition: 0 };
      try {
        for await (const part of this.merge(inputs)) {
          if (this.#wideOutputBytes > 0) {
            for (let row = part.from; row < part.from + part.count; row++) {
              const paced = this.pace();
              if (paced !== null) await paced;
              this.checkAborted();
              await this.#writeRow(run, part.columns, row);
            }
          } else
            await this.io.stage(staging, run, header, part.columns, null, part.from, part.count);
        }
        await this.io.flush(staging, run, header);
      } finally {
        staging.close();
      }
      for (const input of inputs) await this.files.release(input);
      this.runs.push(run);
      this.profile.mergePasses++;
    }
    this.profile.mergePasses++;
  }

  /** k-way merge of runs (bounded fan-in): ranges of one input's page, in order. */
  async *merge(
    runs: readonly SpilledRun[] = this.runs,
  ): AsyncGenerator<Extract<Range, { kind: "page" }>> {
    const types = this.storeTypes;
    const cursors: Cursor[] = [];
    try {
      for (const run of runs) {
        const cursor: Cursor = {
          run,
          page: -1,
          held: null,
          columns: [],
          operands: emptyOperands(this.spec.types, this.spec.keys),
          rows: 0,
          pos: 0,
        };
        cursors.push(cursor);
        if (!(await this.#nextPage(cursor, types))) cursors.pop();
      }
      const heap = cursors.slice();
      const less = (a: Cursor, b: Cursor) => this.#compareCursors(a, a.pos, b, b.pos) < 0;
      const sift = (start: number) => {
        let i = start;
        for (;;) {
          const l = 2 * i + 1;
          const r = l + 1;
          let m = i;
          if (l < heap.length && less(cursorAt(heap, l), cursorAt(heap, m))) m = l;
          if (r < heap.length && less(cursorAt(heap, r), cursorAt(heap, m))) m = r;
          if (m === i) return;
          const t = cursorAt(heap, i);
          heap[i] = cursorAt(heap, m);
          heap[m] = t;
          i = m;
        }
      };
      for (let i = (heap.length >> 1) - 1; i >= 0; i--) sift(i);
      while (heap.length > 0) {
        this.checkAborted();
        const top = cursorAt(heap, 0);
        // The rows of this input that stay ahead of the best other input go out as one range.
        let next: Cursor | null = null;
        if (heap.length === 2) next = cursorAt(heap, 1);
        else if (heap.length > 2)
          next = less(cursorAt(heap, 1), cursorAt(heap, 2)) ? cursorAt(heap, 1) : cursorAt(heap, 2);
        let end = top.pos + 1;
        if (next === null) end = top.rows;
        else while (end < top.rows && this.#compareCursors(top, end, next, next.pos) <= 0) end++;
        yield {
          kind: "page",
          columns: top.columns,
          operands: top.operands,
          from: top.pos,
          count: end - top.pos,
        };
        top.pos = end;
        // The consumer's work counts toward the slice too: a task yield once it is due.
        const paced = this.pace();
        if (paced !== null) await paced;
        if (top.pos >= top.rows && !(await this.#nextPage(top, types))) {
          const last = heap.pop();
          if (last !== undefined && last !== top) heap[0] = last;
        }
        if (heap.length > 0) sift(0);
      }
    } finally {
      for (const cursor of cursors) this.#releaseCursor(cursor);
    }
  }

  #releaseCursor(cursor: Cursor): void {
    cursor.columns = [];
    if (cursor.held !== null) this.io.releasePage(cursor.held);
    cursor.held = null;
    dropOperands(cursor.operands, this.pool);
  }

  /**
   * Moves a cursor to its run's next non-empty page: read, decoded into views, and (compare
   * mode) its operands decoded once. When the operands do not fit, the page goes back and the
   * page, its copies and its operands are made room for as one step before it is read again.
   */
  async #nextPage(cursor: Cursor, types: readonly PhysicalType[]): Promise<boolean> {
    let retried = false;
    for (;;) {
      this.#releaseCursor(cursor);
      cursor.page++;
      if (cursor.page >= cursor.run.pages.length) return false;
      const held = await this.io.readPage(cursor.run, cursor.page);
      cursor.held = held;
      const decoded = await this.io.decode(held, types);
      cursor.columns = decoded.columns;
      cursor.rows = decoded.rows;
      cursor.pos = 0;
      if (cursor.rows === 0) continue;
      if (this.mode === "keys" || !this.decodes) return true;
      const { types: values, keys } = this.spec;
      const needed = decodeOperands(
        values,
        keys,
        decoded.columns,
        0,
        decoded.rows,
        this.pool,
        cursor.operands,
      );
      if (needed === 0) return true;
      if (retried) throw new RangeError("A spill page's sort operands could not be decoded");
      retried = true;
      const step = held.charged + needed;
      this.#releaseCursor(cursor);
      cursor.page--;
      await this.io.need(step);
    }
  }

  #compareCursors(a: Cursor, i: number, b: Cursor, j: number): number {
    if (this.mode !== "keys")
      return compareRows(
        this.spec.types,
        this.spec.keys,
        a.columns,
        i,
        b.columns,
        j,
        a.operands,
        b.operands,
      );
    for (let w = 0; w < PREFIX_WORDS; w++) {
      const x = int32At(a.columns[this.prefixColumn + w], i) >>> 0;
      const y = int32At(b.columns[this.prefixColumn + w], j) >>> 0;
      if (x !== y) return x < y ? -1 : 1;
    }
    if (this.direct) return 0;
    const ka = varlenOf(a.columns[this.keyColumn]);
    const kb = varlenOf(b.columns[this.keyColumn]);
    return compareRanges(
      ka.bytes,
      ka.offsets[i] ?? 0,
      ka.offsets[i + 1] ?? 0,
      kb.bytes,
      kb.offsets[j] ?? 0,
      kb.offsets[j + 1] ?? 0,
    );
  }

  /** Pre-finish runs are chronological and stable within equal keys; resident input follows. */
  checkpoint(): Promise<RecordCheckpoint> {
    if (this.#closed || this.#checkpointWork)
      return Promise.reject(new Error("Sort checkpoint is not at an input boundary"));
    const work = (async () => {
      const checkpointIO = new SpillIo(
        this.pool,
        this.files,
        this.pages,
        this.context,
        () => this.checkAborted(),
        () => this.pace(),
      );
      const records = new RecordCheckpoint(checkpointIO, this.context.spill, this.spec.types);
      try {
        for (const run of this.runs)
          for (let at = 0; at < run.pages.length; at++) {
            this.checkAborted();
            const ref = run.pages[at];
            if (!ref) throw new Error("Missing sort checkpoint page");
            await this.io.need(6 * ref.length + 64 * 1024);
            const page = await this.io.readPage(run, at);
            try {
              const decoded = await this.io.decode(page, this.storeTypes);
              await records.write(decoded.columns, 0, decoded.rows);
            } finally {
              this.io.releasePage(page);
            }
          }
        for (let at = 0; at < this.store.chunkCount; at++) {
          this.checkAborted();
          const columns = this.spec.types.map((_, c) => this.store.chunk(c, at));
          await records.write(columns, 0, columns[0]?.length ?? 0);
        }
        await records.seal();
        this.checkAborted();
        return records;
      } catch (error) {
        await records.close().catch(() => undefined);
        throw error;
      }
    })();
    const settled = work.then(
      () => undefined,
      () => undefined,
    );
    this.#checkpointWork = settled;
    void settled.then(() => {
      if (this.#checkpointWork === settled) this.#checkpointWork = null;
    });
    return work;
  }

  /** Mark cancellation before draining asynchronous borrowers; do not free their buffers. */
  cancel(reason: unknown): void {
    if (this.#aborted === undefined) this.#aborted = reason ?? new Error("Aborted");
  }

  abort(reason: unknown): void {
    this.cancel(reason);
    this.close();
  }

  close(): void {
    if (this.#checkpointWork) {
      this.#checkpointClose ??= this.#checkpointWork.then(() => this.close());
      void this.#checkpointClose.catch(() => undefined);
      return;
    }
    if (this.#closed) return;
    this.#closed = true;
    this.store.close();
    this.encoder?.close(this.pool);
    this.directEncoder?.close(this.pool);
    this.#groups?.close();
    if (this.#thresholdBytes > 0) this.charge.free(this.#thresholdBytes);
    this.#thresholdBytes = 0;
    this.#groupThresholds = [];
    this.#groupFirstThresholds = [];
    this.partEncoder?.close(this.pool);
    for (const set of this.#sets) set?.close();
    if (this.#reserved > 0) this.charge.free(this.#reserved);
    if (this.#rankBytes > 0) this.charge.free(this.#rankBytes);
    dropOperands(this.operands, this.charge);
    this.#releaseCompaction();
    this.charge.free(SORTER_SCRATCH_BYTES);
    this.#operandLoan?.repay();
    this.#operandLoan = null;
    this.#reserved = 0;
    this.#rankBytes = 0;
    this.pages.close();
    this.#mergeStep?.();
    this.#mergeStep = null;
    this.profile.peakHeldBytes = this.charge.peak;
    this.profile.loans += this.pool.loans;
    this.pool.close();
    this.#rowStep?.release();
    this.#rowStep?.loan.repay();
    this.#rowStep = null;
    this.profile.spilledBytes = this.files.written;
    this.files.closeAll().catch(() => undefined);
    this.context.grant.close();
  }

  /** Settles when the sort's spill files are closed (rejects with the first close failure). */
  get closed(): Promise<void> {
    return (
      this.#checkpointClose?.then(async () => {
        await this.files.closed;
      }) ??
      this.files.closed ??
      Promise.resolve()
    );
  }
}

function cursorAt(heap: readonly Cursor[], i: number): Cursor {
  const cursor = heap[i];
  if (cursor === undefined) throw new RangeError("No such merge input");
  return cursor;
}

function partLengthVector(values: Int32Array, n: number): FlatVector {
  return { form: "flat", type: INT, length: n, validity: null, data: { layout: "i32", values } };
}

function chunkColumns(store: ColumnStore, k: number): FlatVector[] {
  return store.types.map((_, c) => store.chunk(c, k));
}

function keyOf(store: ColumnStore, column: number, id: number): [Uint8Array, number, number] {
  const data = store.chunk(column, id >>> store.chunkShift).data as {
    bytes: Uint8Array;
    offsets: Int32Array;
  };
  const slot = id & store.chunkMask;
  return [data.bytes, data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0];
}

function valueAt32(store: ColumnStore, column: number, id: number): number {
  return (
    (store.chunk(column, id >>> store.chunkShift).data as { values: Int32Array }).values[
      id & store.chunkMask
    ] ?? 0
  );
}

function int32At(vector: Vector | undefined, row: number): number {
  return (
    ((vector as FlatVector | undefined)?.data as { values: Int32Array } | undefined)?.values[row] ??
    0
  );
}

function varlenOf(vector: Vector | undefined): { bytes: Uint8Array; offsets: Int32Array } {
  return (vector as FlatVector).data as { bytes: Uint8Array; offsets: Int32Array };
}

/** Bytes of row `row`'s value in a varlen vector (0 for null or fixed-width vectors). */
function varlenBytes(vector: Vector, row: number): number {
  const flat =
    vector.form === "flat" ? vector : vector.form === "constant" ? vector.value : vector.dictionary;
  if (flat.data.layout !== "varlen") return 0;
  const slot =
    vector.form === "flat" ? row : vector.form === "constant" ? 0 : (vector.codes[row] ?? 0);
  return (flat.data.offsets[slot + 1] ?? 0) - (flat.data.offsets[slot] ?? 0);
}

/**
 * How many of rows `from … from + count` (by `ids`) fit `ARRIVAL_BYTES` of varlen values, at
 * least one: `count` without a scan when the batch has no varlen columns.
 */
function partRows(
  columns: readonly Vector[],
  ids: Uint16Array | null,
  from: number,
  count: number,
): number {
  const varlen = columns.filter((vector) => {
    const flat =
      vector.form === "flat"
        ? vector
        : vector.form === "constant"
          ? vector.value
          : vector.dictionary;
    return flat.data.layout === "varlen";
  });
  if (varlen.length === 0) return count;
  let bytes = 0;
  for (let k = 0; k < count; k++) {
    const row = ids === null ? from + k : (ids[from + k] ?? 0);
    for (const vector of varlen) bytes += varlenBytes(vector, row);
    if (bytes > ARRIVAL_BYTES) return Math.max(1, k);
  }
  return count;
}

/** UTF-8 bytes of a text row, or null. */
function textAt(
  vector: Vector,
  row: number,
): { bytes: Uint8Array; start: number; end: number } | null {
  const valid = (v: Uint32Array | null, r: number) =>
    v === null || ((v[r >>> 5] ?? 0) & (1 << (r & 31))) !== 0;
  let flat: FlatVector;
  let slot: number;
  if (vector.form === "flat") {
    if (!valid(vector.validity, row)) return null;
    flat = vector;
    slot = row;
  } else if (vector.form === "constant") {
    if (!valid(vector.value.validity, 0)) return null;
    flat = vector.value;
    slot = 0;
  } else {
    if (!valid(vector.validity, row)) return null;
    flat = vector.dictionary;
    slot = vector.codes[row] ?? 0;
  }
  const data = flat.data as { bytes: Uint8Array; offsets: Int32Array };
  return { bytes: data.bytes, start: data.offsets[slot] ?? 0, end: data.offsets[slot + 1] ?? 0 };
}

// --- emission -------------------------------------------------------------------------------

/**
 * Pushes output batches to a downstream: rows are copied into one reusable output chunk (the
 * payload columns, plus a row-number column for group top-k) and pushed when it fills.
 */
class Emitter {
  readonly out: ColumnStore;
  readonly rowNumbers: Float64Array | null;
  #rowNumber: FlatVector | null;
  rowsOut = 0;
  constructor(
    readonly sorter: Sorter,
    readonly downstream: Operator,
    withRowNumber: boolean,
  ) {
    this.out = new ColumnStore(sorter.spec.types, sorter.pool, { targetBytes: BATCH_TARGET_BYTES });
    this.rowNumbers = withRowNumber ? new Float64Array(BATCH_ROWS) : null;
    this.#rowNumber =
      this.rowNumbers === null
        ? null
        : {
            form: "flat",
            type: BIGINT,
            length: 0,
            validity: null,
            data: { layout: "int64", values: { repr: "safe", values: this.rowNumbers } },
          };
  }

  get payload(): readonly number[] {
    return this.sorter.spec.types.map((_, c) => c);
  }

  /** Copies rows in; returns how many it took (fewer when the chunk is full: flush first). */
  take(range: Range, from: number, count: number, rowNumber: number): number {
    const before = this.out.rows;
    const room = BATCH_ROWS - before;
    const n = Math.min(room, count);
    if (n <= 0) return 0;
    const taken =
      range.kind === "store"
        ? this.out.appendFrom(this.sorter.store, this.payload, range.ids, from, n)
        : this.out.append(range.columns.slice(0, this.sorter.width), null, from, n);
    if (this.rowNumbers !== null)
      for (let i = 0; i < taken; i++) this.rowNumbers[before + i] = rowNumber + i;
    return taken;
  }

  get full(): boolean {
    return this.out.rows >= BATCH_ROWS;
  }

  /** Pushes the chunk; a Promise when the downstream blocks. */
  flush(): MaybePromise<void> {
    const rows = this.out.rows;
    if (rows === 0) return;
    const columns: Vector[] = this.out.vectors();
    if (this.#rowNumber !== null) {
      this.#rowNumber.length = rows;
      columns.push(this.#rowNumber);
    }
    this.rowsOut += rows;
    const result = this.downstream.push({ length: rows, columns, selection: null });
    const done = () => {
      this.out.reset(this.sorter.pool.owes);
      this.sorter.pool.settle();
    };
    if (result === "blocked")
      return this.downstream.ready().then(() => {
        done();
        // A resumption after backpressure: the query may have been cancelled meanwhile.
        this.sorter.checkAborted();
      });
    done();
  }

  close(): void {
    this.out.close();
  }
}

/** Bytes the emitter's chunk needs for row `from + done` of a range (one oversized row). */
function rowNeed(emitter: Emitter, range: Range, at: number): number {
  return range.kind === "store"
    ? emitter.out.appendFromNeed(emitter.sorter.store, emitter.payload, range.ids[at] ?? 0)
    : emitter.out.appendNeed(range.columns.slice(0, emitter.sorter.width), at);
}

/**
 * Emits rows `from … from + count` of a range as a sliced job: flushes as the chunk fills
 * (blocking on the downstream when it blocks, yielding when the slice is due); one oversized
 * row makes room for itself first.
 */
function* emitJob(
  emitter: Emitter,
  range: Range,
  from: number,
  count: number,
  rowNumber: number,
  slice: Slice,
): SlicedJob<void> {
  let done = 0;
  while (done < count) {
    done += emitter.take(range, from + done, count - done, rowNumber + done);
    if (done >= count && !emitter.full) return;
    if (emitter.out.rows === 0) {
      yield emitter.sorter.io.need(rowNeed(emitter, range, from + done));
      continue;
    }
    const flushed = emitter.flush();
    if (isPromise(flushed)) yield flushed;
    else if (slice.due()) yield;
  }
}

/** `emitJob` from async code (merge output): a task yield whenever its slice is due. */
async function emitRange(
  emitter: Emitter,
  range: Range,
  from: number,
  count: number,
  rowNumber: number,
): Promise<void> {
  const done = runSliced(
    emitJob(emitter, range, from, count, rowNumber, emitter.sorter.slice),
    emitter.sorter.slice,
    () => {
      emitter.sorter.checkAborted();
    },
  );
  if (isPromise(done)) await done;
  if (emitter.sorter.wide) {
    const flushed = emitter.flush();
    if (isPromise(flushed)) await flushed;
  }
}

/**
 * One row held past its page (WITH TIES' last row, a group's first row), with its operands in
 * compare mode; both charged to the pool.
 */
class HeldRow {
  readonly store: ColumnStore;
  readonly operands: Operands;
  constructor(readonly sorter: Sorter) {
    this.store = new ColumnStore(sorter.storeTypes, sorter.pool, {
      chunkRows: 1,
      varlenBytes: 64,
      targetBytes: BATCH_TARGET_BYTES,
    });
    this.operands = emptyOperands(sorter.spec.types, sorter.spec.keys);
  }

  get empty(): boolean {
    return this.store.rows === 0;
  }

  get vectors(): readonly Vector[] {
    return this.store.vectors();
  }

  /** Holds row `row` of `columns` in place of the last one; room is made for it when refused. */
  async hold(columns: readonly Vector[], row: number): Promise<void> {
    const sorter = this.sorter;
    const { types, keys } = sorter.spec;
    const decodes = sorter.mode !== "keys" && sorter.decodes;
    for (let attempt = 0; ; attempt++) {
      this.#drop();
      const stored = this.store.append(columns, null, row, 1) === 1;
      if (
        stored &&
        (!decodes ||
          decodeOperands(types, keys, columns, row, row + 1, sorter.pool, this.operands) === 0)
      )
        return;
      this.#drop();
      if (attempt > 0) throw new RangeError("A held sort row could not be allocated");
      const operands = decodes ? operandBytes(types, keys, columns, row, row + 1) : 0;
      await sorter.io.need(this.store.appendNeed(columns, row) + operands);
    }
  }

  #drop(): void {
    this.store.reset(this.sorter.pool.owes);
    dropOperands(this.operands, this.sorter.pool);
  }

  close(): void {
    this.store.close();
    dropOperands(this.operands, this.sorter.pool);
  }
}

// --- operators ------------------------------------------------------------------------------

/** ORDER BY [LIMIT / OFFSET / WITH TIES] (see the module comment). */
class GeneralSortOperator implements Operator {
  async checkpoint(): Promise<RecordCheckpoint> {
    if (this.#done || this.#finishing) throw new Error("Sort evaluation has begun");
    await this.#working;
    await this.#reclaiming;
    return this.#sorter.checkpoint();
  }

  readonly spec: SortSpec;
  readonly #sorter: Sorter;
  readonly #emitter: Emitter;
  #pending: Promise<void> | null = null;
  #done = false;
  #working: Promise<void> | null = null;
  #reclaiming: Promise<void> | null = null;
  #finishing = false;

  constructor(spec: SortSpec, context: SortContext, downstream: Operator) {
    this.spec = spec;
    const keep = spec.limit === null ? null : spec.offset + spec.limit;
    this.#sorter = new Sorter(
      {
        types: spec.types,
        keys: spec.keys,
        partition: 0,
        keep,
        withTies: spec.withTies,
        heldRow: spec.withTies,
      },
      context,
    );
    this.#emitter = new Emitter(this.#sorter, downstream, false);
    context.grant.onReclaimRequest(() => this.reclaim());
  }

  /** Serialize a cooperative spill with push/finish and retain borrowed batches until it ends. */
  reclaim(): void | Promise<void> {
    if (this.#done || this.#finishing) return;
    if (this.#reclaiming !== null) return this.#reclaiming;
    const run = async () => {
      if (this.#working !== null) await this.#working;
      if (!this.#done && !this.#finishing) await this.#sorter.reclaim();
    };
    const pending = run().then(
      () => {
        this.#reclaiming = null;
      },
      (error: unknown) => {
        this.#reclaiming = null;
        this.abort(error);
        throw error;
      },
    );
    this.#reclaiming = pending;
    return pending;
  }

  get profile(): SortProfile {
    const profile = this.#sorter.profile;
    profile.rowsOut = this.#emitter.rowsOut;
    profile.peakHeldBytes = Math.max(profile.peakHeldBytes, this.#sorter.charge.peak);
    profile.spilledBytes = this.#sorter.files.written;
    return profile;
  }

  get closed(): Promise<void> {
    return this.#sorter.closed;
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    let result: MaybePromise<void>;
    try {
      result =
        this.#reclaiming === null
          ? this.#sorter.add(batch)
          : this.#reclaiming.then(() => this.#sorter.add(batch));
    } catch (error) {
      this.abort(error);
      throw error;
    }
    if (!isPromise(result)) {
      transfer?.release();
      return "ready";
    }
    this.#pending = result.then(
      () => transfer?.release(),
      (error: unknown) => {
        transfer?.release();
        this.abort(error);
        throw error;
      },
    );
    const working = this.#pending;
    this.#working = working;
    void working.then(
      () => {
        if (this.#working === working) this.#working = null;
      },
      () => {
        if (this.#working === working) this.#working = null;
      },
    );
    return "blocked";
  }

  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  finish(): MaybePromise<void> {
    if (this.#reclaiming !== null) return this.#reclaiming.then(() => this.finish());
    this.#finishing = true;
    const sorter = this.#sorter;
    let resident: MaybePromise<boolean>;
    try {
      sorter.checkAborted();
      resident = sorter.runs.length === 0 ? sorter.sliced(this.#memoryJob()) : false;
    } catch (error) {
      this.abort(error);
      throw error;
    }
    const fail = (error: unknown): never => {
      this.abort(error);
      throw error;
    };
    const after = (inMemory: boolean): MaybePromise<void> => {
      if (inMemory) {
        this.#end();
        return this.#emitter.downstream.finish();
      }
      return this.#emitMerged().then(async () => {
        this.#end();
        await sorter.closed;
        await this.#emitter.downstream.finish();
      }, fail);
    };
    if (isPromise(resident)) return resident.then(after, fail);
    return after(resident);
  }

  /**
   * In-memory output as a sliced job: compaction (top-N), the sort, then offset, limit and ties
   * over the sorted ids. False when the sort's memory was refused (the rows go out merged).
   */
  *#memoryJob(): SlicedJob<boolean> {
    const sorter = this.#sorter;
    const slice = sorter.slice;
    const spec = this.spec;
    const all = yield* sorter.sortJob(slice);
    if (all === null) return false;
    let end = spec.limit === null ? all.length : Math.min(all.length, spec.offset + spec.limit);
    if (spec.withTies && spec.limit !== null && end > spec.offset && end < all.length)
      while (end < all.length && sorter.compareIds(all[end - 1] ?? 0, all[end] ?? 0) === 0) {
        end++;
        if (slice.tick()) yield;
      }
    const count = Math.max(0, end - spec.offset);
    const range: Range = { kind: "store", ids: all, from: spec.offset, count };
    yield* emitJob(this.#emitter, range, spec.offset, count, 0, slice);
    const flushed = this.#emitter.flush();
    if (isPromise(flushed)) yield flushed;
    return true;
  }

  /** External output: the merge, with offset counted, limit and ties. */
  async #emitMerged(): Promise<void> {
    const sorter = this.#sorter;
    const spec = this.spec;
    await sorter.prepareMerge();
    let skip = spec.offset;
    let left = spec.limit ?? Infinity;
    const last = new HeldRow(sorter);
    try {
      for await (const range of sorter.merge()) {
        let from = range.from;
        let count = range.count;
        if (skip > 0) {
          const skipped = Math.min(skip, count);
          skip -= skipped;
          from += skipped;
          count -= skipped;
          if (count === 0) continue;
        }
        if (left <= 0) {
          // WITH TIES: rows equal to the last one emitted still go out.
          if (!spec.withTies || last.empty) break;
          let ties = 0;
          const held = last.vectors;
          while (
            ties < count &&
            sorter.sameKey(held, 0, range.columns, from + ties, last.operands, range.operands)
          )
            ties++;
          if (ties > 0) await emitRange(this.#emitter, range, from, ties, 0);
          if (ties < count) break;
          continue;
        }
        const n = Math.min(left, count);
        await emitRange(this.#emitter, range, from, n, 0);
        left -= n;
        if (spec.withTies) {
          await last.hold(range.columns, from + n - 1);
          // Ties past the limit inside this range.
          if (left <= 0 && n < count) {
            let ties = 0;
            const held = last.vectors;
            while (
              n + ties < count &&
              sorter.sameKey(held, 0, range.columns, from + n + ties, last.operands, range.operands)
            )
              ties++;
            if (ties > 0) await emitRange(this.#emitter, range, from + n, ties, 0);
            if (n + ties < count) break;
          }
        } else if (left <= 0) break;
      }
      await this.#emitter.flush();
    } finally {
      last.close();
    }
  }

  #end(): void {
    if (this.#done) return;
    this.#done = true;
    this.#emitter.close();
    this.#sorter.close();
  }

  /** Releases a sort that took no rows and never will (its owner sorted another way). */
  discard(): void {
    this.#end();
  }

  abort(reason: unknown): void {
    this.#sorter.abort(reason);
    if (!this.#done) {
      this.#done = true;
      this.#emitter.close();
    }
  }
}

/** The full numeric path is optional; the original sorter owns every other shape/fallback. */
export class SortOperator implements Operator {
  checkpoint(): Promise<RecordCheckpoint> {
    return this.#operator.checkpoint();
  }

  readonly #operator: GeneralSortOperator | ColumnarSortOperator;
  constructor(
    readonly spec: SortSpec,
    context: SortContext,
    downstream: Operator,
  ) {
    this.#operator =
      context.columnar === true && columnarNumericSort(spec)
        ? new ColumnarSortOperator(
            spec,
            context,
            downstream,
            sortMinimumBytes(spec.types),
            (fallback) => new GeneralSortOperator(spec, fallback, downstream),
          )
        : new GeneralSortOperator(spec, context, downstream);
  }
  get profile(): SortProfile & { readonly columnarRows?: number } {
    return this.#operator.profile;
  }
  get closed(): Promise<void> {
    return this.#operator.closed;
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    return this.#operator.push(batch, transfer);
  }
  ready(): Promise<void> {
    return this.#operator.ready();
  }
  finish(): MaybePromise<void> {
    return this.#operator.finish();
  }
  reclaim(): void | Promise<void> {
    return this.#operator.reclaim();
  }
  discard(): void {
    this.#operator.discard();
  }
  abort(reason: unknown): void {
    this.#operator.abort(reason);
  }
}

/** Sole ROW_NUMBER/RANK <= k, with complete boundary peers for RANK. */
export class GroupTopKOperator implements Operator {
  readonly spec: GroupTopKSpec;
  readonly #sorter: Sorter;
  readonly #emitter: Emitter;
  #pending: Promise<void> | null = null;
  #done = false;
  #working: Promise<void> | null = null;
  #closing: Promise<void> | null = null;
  readonly #cancel = new AbortController();

  constructor(spec: GroupTopKSpec, context: SortContext, downstream: Operator) {
    this.spec = spec;
    const partition: SortColumn[] = spec.partitionBy.map((column) => ({
      column,
      direction: "asc",
      nulls: spec.partitionNulls ?? "first",
      collation: null,
    }));
    this.#sorter = new Sorter(
      {
        types: spec.types,
        keys: [...partition, ...spec.orderBy],
        partition: partition.length,
        keep: spec.k,
        withTies: spec.ranking === "rank",
        heldRow: true,
      },
      {
        ...context,
        signal: context.signal
          ? AbortSignal.any([context.signal, this.#cancel.signal])
          : this.#cancel.signal,
      },
    );
    this.#emitter = new Emitter(this.#sorter, downstream, true);
  }

  get profile(): SortProfile {
    const profile = this.#sorter.profile;
    profile.rowsOut = this.#emitter.rowsOut;
    profile.spilledBytes = this.#sorter.files.written;
    return profile;
  }

  get closed(): Promise<void> {
    return this.#closing ?? this.#sorter.closed;
  }

  #track(work: Promise<unknown>): void {
    const settled = work.then(
      () => undefined,
      () => undefined,
    );
    this.#working = settled;
    void settled.then(() => {
      if (this.#working === settled) this.#working = null;
    });
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    let result: MaybePromise<void>;
    try {
      result = this.#sorter.add(batch);
    } catch (error) {
      this.abort(error);
      throw error;
    }
    if (!isPromise(result)) {
      transfer?.release();
      return "ready";
    }
    this.#pending = result.then(
      () => transfer?.release(),
      (error: unknown) => {
        transfer?.release();
        this.abort(error);
        throw error;
      },
    );
    this.#track(this.#pending);
    return "blocked";
  }

  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  finish(): MaybePromise<void> {
    const sorter = this.#sorter;
    let resident: MaybePromise<boolean>;
    try {
      sorter.checkAborted();
      resident = sorter.runs.length === 0 ? sorter.sliced(this.#memoryJob()) : false;
    } catch (error) {
      this.abort(error);
      throw error;
    }
    if (isPromise(resident)) this.#track(resident);
    const fail = (error: unknown): never => {
      this.abort(error);
      throw error;
    };
    const after = (inMemory: boolean): MaybePromise<void> => {
      sorter.checkAborted();
      if (inMemory) {
        this.#end();
        return this.#emitter.downstream.finish();
      }
      const merged = this.#emitMerged();
      this.#track(merged);
      return merged.then(async () => {
        this.#end();
        await sorter.closed;
        await this.#emitter.downstream.finish();
      }, fail);
    };
    if (isPromise(resident)) return resident.then(after, fail);
    return after(resident);
  }

  /**
   * In-memory output as a sliced job: compaction, the sort, then row numbers restarting at each
   * partition (compare mode cuts here). False when the sort's memory was refused.
   */
  *#memoryJob(): SlicedJob<boolean> {
    const sorter = this.#sorter;
    const slice = sorter.slice;
    const k = this.spec.k;
    const ids = yield* sorter.sortJob(slice);
    if (ids === null) return false;
    const kept = sorter.mode === "keys" ? yield* sorter.cutJob(ids, slice) : ids;
    let rowNumber = 0,
      rank = 1,
      previous = -1;
    for (let i = 0; i < kept.length; i++) {
      const id = kept[i] ?? 0;
      if (previous < 0 || !sorter.samePartitionIds(previous, id)) {
        rowNumber = 0;
        rank = 1;
      } else if (sorter.compareIds(previous, id) !== 0) rank = rowNumber + 1;
      rowNumber++;
      const value = this.spec.ranking === "rank" ? rank : rowNumber;
      if (value <= k) {
        const range: Range = { kind: "store", ids: kept, from: i, count: 1 };
        yield* emitJob(this.#emitter, range, i, 1, value, slice);
      }
      previous = id;
      if (slice.tick()) yield;
    }
    const flushed = this.#emitter.flush();
    if (isPromise(flushed)) yield flushed;
    return true;
  }

  /** External output: the merge, row numbers restarting where the partition changes. */
  async #emitMerged(): Promise<void> {
    const sorter = this.#sorter;
    const k = this.spec.k;
    await sorter.prepareMerge();
    // The partition's first row: rows of one partition have equal partition keys.
    const first = new HeldRow(sorter);
    let rowNumber = 0,
      rank = 1;
    try {
      for await (const range of sorter.merge()) {
        const end = range.from + range.count;
        for (let r = range.from; r < end; r++) {
          if (
            first.empty ||
            !sorter.samePartition(
              first.vectors,
              0,
              range.columns,
              r,
              first.operands,
              range.operands,
            )
          ) {
            rowNumber = 0;
            rank = 1;
            await first.hold(range.columns, r);
          } else if (
            this.spec.ranking === "rank" &&
            rank <= k &&
            !sorter.sameKey(first.vectors, 0, range.columns, r, first.operands, range.operands)
          ) {
            rank = rowNumber + 1;
            await first.hold(range.columns, r);
          }
          rowNumber++;
          const value = this.spec.ranking === "rank" ? rank : rowNumber;
          if (value <= k) await emitRange(this.#emitter, range, r, 1, value);
          else if ((r & (SLICE_ROWS - 1)) === 0) {
            const paced = sorter.pace();
            if (paced !== null) await paced;
          }
        }
      }
      await this.#emitter.flush();
    } finally {
      first.close();
    }
  }

  #end(): void {
    if (this.#done) return;
    this.#done = true;
    this.#emitter.close();
    this.#sorter.close();
  }

  abort(reason: unknown): void {
    if (this.#closing !== null) return;
    this.#cancel.abort(reason);
    this.#sorter.cancel(reason);
    this.#done = true;
    const working = this.#working;
    this.#closing = (async () => {
      await working;
      this.#emitter.close();
      this.#sorter.abort(reason);
      await this.#sorter.closed;
    })();
    void this.#closing.catch(() => undefined);
  }
}
