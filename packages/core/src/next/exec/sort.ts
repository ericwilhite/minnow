/**
 * Sort, top-N and per-group top-k (track B7, increment 2).
 *
 * ## Operators (the executor composes them; exec/executor.ts)
 * - `SortOperator`: ORDER BY with optional LIMIT / OFFSET / WITH TIES (IR `sort`). A breaker:
 *   `push` takes rows, `finish` emits them in order to its downstream, then finishes it.
 * - `GroupTopKOperator`: `ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) <= k` as a per-group
 *   top-k: the first k rows of every partition, in (partition, order) order, with the row number
 *   appended (int64). Lowering rewrites the window + filter to it only when nothing else needs
 *   the other rows (design-wide-window-sort-parallel.md §4); RANK is not this (ties).
 * Key columns are input column indexes; outputs are the input columns (plus the row number).
 *
 * ## How (sort-keys.ts for the key law)
 * - **Rows** are copied into a `ColumnStore` under the operator's spillable grant, each with its
 *   normalized key bytes and prefix words (key mode), and sorted as row ids (the locator): radix
 *   over the prefix, byte refinement of ties; payload is gathered only for the rows emitted.
 * - **Top-N** (k = offset + limit): rows are kept while their key beats the current k-th key (a
 *   threshold); at 2k candidates the buffer is compacted to its best k (ties kept for WITH TIES),
 *   so memory is O(k) and most rows are rejected after their key is encoded. A k too large for
 *   memory falls back to the external sort, still filtering by the threshold.
 * - **Per-group top-k** compacts the same way per partition (≤ k survivors per group); if the
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
import type { Grant } from "../memory/governor.js";
import { stringBytes } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import type { PhysicalType } from "../types.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  type Batch,
  type FlatVector,
  type Vector,
} from "../vector/vector.js";
import { ColumnStore } from "./column-store.js";
import { isPromise, type Operator, type PushResult, type Transfer } from "./pipeline.js";
import {
  PREFIX_WORDS,
  SortKeyEncoder,
  collatorFor,
  compareRanges,
  compareRows,
  isByteCollation,
  sortByPrefix,
  sortMode,
  sortScratchBytes,
  type SortColumn,
} from "./sort-keys.js";
import { PageWriter, SpillFiles, SpilledRun } from "./spill-pages.js";
import { Charge, Pool, SpillIo } from "./spill-pool.js";

type MaybePromise<T> = T | Promise<T>;

const BYTES: PhysicalType = { kind: "bytes" };
const INT: PhysicalType = { kind: "int32", bits: 32 };
const BIGINT: PhysicalType = { kind: "int64" };
/** Most spill runs one merge reads at once. */
export const MAX_FAN_IN = 64;
/** Sort scratch reserved per row as rows arrive: ids, prefix words, radix buffers. */
const SCRATCH_PER_ROW = 4 + 4 * PREFIX_WORDS + 2 * 4 * (PREFIX_WORDS + 1);
const POOL_BYTES = 1024 * 1024;

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
  /** Rows kept per partition (ROW_NUMBER() <= k). */
  readonly k: number;
}

export interface SortContext {
  /** Spillable; the operator closes it when done or aborted. */
  readonly grant: Grant;
  readonly spill: SpillService;
  readonly queryId: number;
  readonly signal?: AbortSignal;
}

export interface SortProfile {
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
}

/** Bytes a sort needs admitted before it runs (encoder, pool, output chunk). */
export function sortMinimumBytes(types: readonly PhysicalType[]): number {
  // Pool, the key encoders, and one store chunk with its sort scratch: an empty store always
  // takes its first chunk, so only a row larger than all of that needs a loan.
  let row = 4 + PREFIX_WORDS * 4 + 4;
  for (const type of types) row += type.kind === "text" || type.kind === "bytes" ? 24 : 17;
  return (
    POOL_BYTES +
    2 * (2048 * (PREFIX_WORDS * 4 + 4) + 4096 + 260) +
    2048 * (row + SCRATCH_PER_ROW) +
    16 * 1024 * (types.length + 2) +
    64 * 1024
  );
}

/** Rows of one page held by a merge input. */
interface Cursor {
  readonly run: SpilledRun;
  page: number;
  held: { bytes: Uint8Array; charged: number } | null;
  columns: readonly FlatVector[];
  rows: number;
  pos: number;
}

/** A small open-addressing set of byte strings (ICU ranks), charged; ids in insertion order. */
class ByteSet {
  #slots = new Int32Array(64).fill(-1);
  #arena = new Uint8Array(4096);
  #used = 0;
  starts: number[] = [];
  ends: number[] = [];
  hashes: number[] = [];
  charged = 0;
  constructor(readonly charge: { allocate(n: number): boolean; free(n: number): void }) {}

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
      this.charged += size * 4;
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
      readonly from: number;
      readonly count: number;
    };

/**
 * What both operators share: rows with keys in a store, compaction, runs, merge. It owns its
 * grant and closes it at the end.
 */
class Sorter {
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
  /** Longest key bytes stored: within the prefix words no full comparison is needed. */
  #maxKey = 0;
  #threshold: Uint8Array | null = null;
  #lastSurvivors = 0;
  #sets: Array<ByteSet | null>;
  #aborted: unknown = undefined;
  #closed = false;
  /** A loan for one row that did not fit an empty store (repaid after its run is written). */
  #rowLoan: { repay(): void } | null = null;

  constructor(
    spec: SorterSpec,
    readonly context: SortContext,
  ) {
    this.spec = spec;
    this.width = spec.types.length;
    this.mode = sortMode(spec.types, spec.keys);
    this.charge = new Charge(context.grant);
    this.pool = new Pool(this.charge, POOL_BYTES);
    this.files = new SpillFiles(context.spill, context.queryId, this.charge);
    this.pages = new PageWriter(this.pool, 16 * 1024);
    this.io = new SpillIo(this.pool, this.files, this.pages, context, () => {
      this.checkAborted();
    });
    this.encoder = this.mode === "keys" ? new SortKeyEncoder(spec.keys, this.pool) : null;
    this.partEncoder =
      this.mode === "keys" && spec.partition > 0
        ? new SortKeyEncoder(spec.keys.slice(0, spec.partition), this.pool)
        : null;
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
    };
  }

  /** Store columns: payload, then (key mode) key bytes, prefix words, partition key length. */
  get storeTypes(): PhysicalType[] {
    if (this.mode !== "keys") return [...this.spec.types];
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
    if (this.mode === "keys") return this.#addKeyed(batch.columns, ids, count);
    if (this.mode === "ranks" && !this.#collect(batch.columns, ids, count)) this.#toCompare();
    return this.#append(batch.columns, ids, count, [], 0);
  }

  #addKeyed(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    count: number,
  ): MaybePromise<void> {
    const encoder = this.encoder;
    if (encoder === null) return;
    let rows: Uint16Array | null = ids;
    let n = count;
    const threshold = this.#threshold;
    if (threshold !== null && this.spec.partition === 0) {
      // Top-N: rows whose key cannot beat the k-th best so far are not stored at all.
      const encoded = encoder.encode(columns, ids, 0, count);
      if (encoded.done < count)
        return this.#needThen(2 * encoded.requiredBytes, () => this.#addKeyed(columns, ids, count));
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
          keptRows[m++] = ids === null ? k : (ids[k] ?? 0);
      }
      this.profile.rejected += count - m;
      if (m === 0) return;
      rows = keptRows.subarray(0, m);
      n = m;
    }
    const encoded = encoder.encode(columns, rows, 0, n);
    if (encoded.done < n)
      return this.#needThen(2 * encoded.requiredBytes, () => this.#addKeyed(columns, ids, count));
    const tail = encoder.tail;
    const offsets = encoder.keys.offsets;
    for (let k = 0; k < n; k++) {
      const length = (offsets[k + 1] ?? 0) - (offsets[k] ?? 0);
      if (length > this.#maxKey) this.#maxKey = length;
    }
    if (this.partEncoder !== null) {
      const part = this.partEncoder.encode(columns, rows, 0, n);
      if (part.done < n)
        return this.#needThen(2 * part.requiredBytes, () => this.#addKeyed(columns, ids, count));
      const offsets = this.partEncoder.keys.offsets;
      for (let k = 0; k < n; k++) partLengths[k] = (offsets[k + 1] ?? 0) - (offsets[k] ?? 0);
      tail.push(partLengthVector(n));
    }
    return this.#append(columns, rows, n, tail, 0);
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
    while (done < n) {
      const want = Math.min(n - done, 2048);
      if (!this.#reserve(this.store.rows + want)) break;
      const taken = this.store.append(
        columns,
        ids,
        done,
        want,
        tail.length > 0 ? { vectors: tail, from: done } : undefined,
      );
      done += taken;
      if (taken < want) break;
    }
    if (done >= n) {
      this.#maybeCompact();
      return;
    }
    const row = ids === null ? done : (ids[done] ?? 0);
    return this.#makeRoom(columns, row, tail, done).then(() =>
      this.#append(columns, ids, n, tail, done),
    );
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

  async #needThen(bytes: number, then: () => MaybePromise<void>): Promise<void> {
    await this.io.need(bytes);
    await then();
  }

  /** Top-N / per-group: compacts the buffer once it holds a few times what is kept. */
  #maybeCompact(): void {
    const keep = this.spec.keep;
    if (keep === null || this.mode !== "keys" || this.runs.length > 0) return;
    const floor = this.spec.partition > 0 ? 65_536 : keep + 4096;
    if (this.store.rows < Math.max(2 * keep, floor, 4 * this.#lastSurvivors)) return;
    this.compact();
  }

  /** Keeps the best `keep` rows (per partition when grouped); false when it did not fit. */
  compact(): boolean {
    const keep = this.spec.keep;
    if (keep === null || this.mode !== "keys") return false;
    const ids = this.sortedIds();
    if (ids === null) return false;
    const survivors = this.cut(ids);
    if (survivors.length === this.store.rows) {
      this.#lastSurvivors = survivors.length;
      return true;
    }
    const next = this.#newStore();
    const taken = next.appendFrom(
      this.store,
      this.storeTypes.map((_, c) => c),
      survivors,
      0,
      survivors.length,
    );
    if (taken < survivors.length) {
      next.close();
      return false;
    }
    if (this.spec.partition === 0 && keep > 0 && survivors.length >= keep) {
      const [bytes, start, end] = keyOf(this.store, this.keyColumn, survivors[keep - 1] ?? 0);
      this.#threshold = bytes.slice(start, end);
    }
    this.store.close();
    this.store = next;
    this.#lastSurvivors = survivors.length;
    this.profile.compactions++;
    return true;
  }

  /** From sorted ids, the ones to keep: the first `keep` (with ties), or per partition. */
  cut(ids: Int32Array): Int32Array {
    const keep = this.spec.keep;
    if (keep === null) return ids;
    if (this.spec.partition === 0) {
      let n = Math.min(keep, ids.length);
      if (this.spec.withTies && n > 0)
        while (n < ids.length && this.compareIds(ids[n - 1] ?? 0, ids[n] ?? 0) === 0) n++;
      return ids.subarray(0, n);
    }
    let m = 0;
    let inGroup = 0;
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i] ?? 0;
      if (i > 0 && !this.samePartitionIds(ids[i - 1] ?? 0, id)) inGroup = 0;
      if (inGroup < keep) ids[m++] = id;
      inGroup++;
    }
    return ids.subarray(0, m);
  }

  /** Order of store rows a, b. */
  compareIds(a: number, b: number): number {
    const store = this.store;
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
    );
  }

  samePartitionIds(a: number, b: number): boolean {
    const store = this.store;
    const shift = store.chunkShift;
    const mask = store.chunkMask;
    if (this.mode !== "keys")
      return this.samePartition(
        chunkColumns(store, a >>> shift),
        a & mask,
        chunkColumns(store, b >>> shift),
        b & mask,
      );
    // Direct reads: the partition key length, then that prefix of the key bytes.
    const plen = this.keyColumn + 1 + PREFIX_WORDS;
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

  /** Whether two rows (store or page vectors) are in one partition. */
  samePartition(a: readonly Vector[], i: number, b: readonly Vector[], j: number): boolean {
    if (this.mode !== "keys")
      return (
        compareRows(this.spec.types, this.spec.keys.slice(0, this.spec.partition), a, i, b, j) === 0
      );
    const plen = this.keyColumn + 1 + PREFIX_WORDS;
    const la = int32At(a[plen], i);
    if (la !== int32At(b[plen], j)) return false;
    const x = varlenOf(a[this.keyColumn]);
    const y = varlenOf(b[this.keyColumn]);
    const xs = x.offsets[i] ?? 0;
    const ys = y.offsets[j] ?? 0;
    return compareRanges(x.bytes, xs, xs + la, y.bytes, ys, ys + la) === 0;
  }

  /** Whether two rows (store or page vectors) have equal keys (ties). */
  sameKey(a: readonly Vector[], i: number, b: readonly Vector[], j: number): boolean {
    if (this.mode !== "keys") return compareRows(this.spec.types, this.spec.keys, a, i, b, j) === 0;
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
   * in memory as a run; an empty store that cannot take the row borrows a loan sized for it.
   */
  async #makeRoom(
    columns: readonly Vector[],
    row: number,
    tail: readonly Vector[],
    k: number,
  ): Promise<void> {
    this.checkAborted();
    const before = this.store.rows;
    if (
      this.spec.keep !== null &&
      this.mode === "keys" &&
      this.runs.length === 0 &&
      this.compact() &&
      this.store.rows < before
    )
      return;
    if (this.store.rows > 0) {
      await this.#spillRun();
      return;
    }
    // One row larger than everything the grant has left: a loan for it (rule 5).
    if (this.#rowLoan !== null) throw new RangeError("A sort row could not be stored");
    const key = tail[0] === undefined ? 0 : varlenLength(tail[0], k);
    const bytes = this.store.appendNeed(columns, row) + key + 2048 * SCRATCH_PER_ROW;
    this.#rowLoan = await this.context.grant.borrow(bytes, this.context.signal);
    this.profile.loans++;
  }

  // --- sorting ---------------------------------------------------------------------------

  /** Row ids of the store in order, or null when the scratch could not be allocated. */
  sortedIds(): Int32Array | null {
    const store = this.store;
    const n = store.rows;
    if (!this.#reserve(n)) return null;
    const ids = new Int32Array(n);
    if (this.mode === "keys") {
      const words = new Int32Array(n * PREFIX_WORDS);
      for (let k = 0; k < store.chunkCount; k++) {
        const base = k * store.chunkRows;
        const length = store.chunk(0, k).length;
        for (let w = 0; w < PREFIX_WORDS; w++) {
          const values = (store.chunk(this.keyColumn + 1 + w, k).data as { values: Int32Array })
            .values;
          for (let i = 0; i < length; i++) words[(base + i) * PREFIX_WORDS + w] = values[i] ?? 0;
        }
      }
      const covered = this.#maxKey <= 4 * PREFIX_WORDS;
      sortByPrefix(
        n,
        words,
        ids,
        new Uint32Array(sortScratchBytes(n) / 4),
        covered ? null : (a, b) => this.compareIds(a, b),
        Math.ceil(this.#maxKey / 4),
      );
      return ids;
    }
    for (let i = 0; i < n; i++) ids[i] = i;
    if (this.mode === "ranks") {
      const order = this.#rankOrder();
      if (order !== null) {
        ids.sort(order);
        return ids;
      }
      this.#toCompare();
    }
    ids.sort((a, b) => this.compareIds(a, b));
    return ids;
  }

  /**
   * Ranks mode (C8): a comparator over query-wide ranks of the collated keys, or null when the
   * ranks or their decoded strings do not fit (compare mode then).
   */
  #rankOrder(): ((a: number, b: number) => number) | null {
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
        }
        const collator = collatorFor(key.collation ?? "");
        const order = Int32Array.from({ length: set.size }, (_, id) => id).sort((a, b) => {
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
        });
        if (!this.charge.allocate(n * 4 + set.size * 4)) return null;
        const rankOf = new Int32Array(set.size);
        order.forEach((id, rank) => (rankOf[id] = rank));
        const perRow = new Int32Array(n).fill(-1);
        for (let c = 0; c < store.chunkCount; c++) {
          const vector = store.chunk(key.column, c);
          for (let i = 0; i < vector.length; i++) {
            const value = textAt(vector, i);
            if (value !== null)
              perRow[c * store.chunkRows + i] =
                rankOf[set.find(value.bytes, value.start, value.end)] ?? 0;
          }
        }
        this.charge.free(set.size * 4);
        this.#rankBytes += n * 4;
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

  /** Sorts the rows in memory and writes them as one run; the store is emptied. */
  async #spillRun(): Promise<void> {
    if (this.mode === "ranks") this.#toCompare();
    const store = this.store;
    if (store.rows === 0) return;
    const ids = this.sortedIds();
    if (ids === null) throw new RangeError("Sort scratch could not be allocated");
    const run = new SpilledRun();
    const staging = new ColumnStore(store.types, this.pool, {
      chunkRows: 512,
      targetBytes: 64 * 1024,
      varlenBytes: 4096,
    });
    const columns = store.types.map((_, c) => c);
    const header = { run: this.runs.length, partition: 0 };
    try {
      let at = 0;
      while (at < ids.length) {
        this.checkAborted();
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
    // The scratch goes back with the rows: the next run reserves its own.
    if (this.#reserved > 0) this.charge.free(this.#reserved);
    this.#reserved = 0;
    if (this.#rankBytes > 0) {
      this.charge.free(this.#rankBytes);
      this.#rankBytes = 0;
    }
    if (this.#rowLoan !== null) {
      this.#rowLoan.repay();
      this.#rowLoan = null;
    }
  }

  /** Writes what is in memory as a run, then merges runs in passes down to `MAX_FAN_IN`. */
  async prepareMerge(): Promise<void> {
    if (this.store.rows > 0) await this.#spillRun();
    // Each input holds one page from the pool: the fan-in is what the pool's room allows.
    let page = 0;
    for (const run of this.runs) for (const ref of run.pages) page = Math.max(page, ref.length);
    const fanIn = Math.max(
      2,
      Math.min(MAX_FAN_IN, Math.floor(this.pool.room / Math.max(1, 2 * page))),
    );
    while (this.runs.length > fanIn) {
      const inputs = this.runs.splice(0, fanIn);
      const run = new SpilledRun();
      const staging = new ColumnStore(this.storeTypes, this.pool, {
        chunkRows: 512,
        targetBytes: 64 * 1024,
        varlenBytes: 4096,
      });
      const header = { run: this.runs.length + fanIn, partition: 0 };
      try {
        for await (const part of this.merge(inputs))
          await this.io.stage(staging, run, header, part.columns, null, part.from, part.count);
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
        const cursor: Cursor = { run, page: -1, held: null, columns: [], rows: 0, pos: 0 };
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
        yield { kind: "page", columns: top.columns, from: top.pos, count: end - top.pos };
        top.pos = end;
        if (top.pos >= top.rows && !(await this.#nextPage(top, types))) {
          const last = heap.pop();
          if (last !== undefined && last !== top) heap[0] = last;
        }
        if (heap.length > 0) sift(0);
      }
    } finally {
      for (const cursor of cursors) if (cursor.held !== null) this.io.releasePage(cursor.held);
    }
  }

  async #nextPage(cursor: Cursor, types: readonly PhysicalType[]): Promise<boolean> {
    for (;;) {
      if (cursor.held !== null) {
        this.io.releasePage(cursor.held);
        cursor.held = null;
      }
      cursor.page++;
      if (cursor.page >= cursor.run.pages.length) return false;
      const held = await this.io.readPage(cursor.run, cursor.page);
      cursor.held = held;
      const decoded = await this.io.decode(held, types);
      cursor.columns = decoded.columns;
      cursor.rows = decoded.rows;
      cursor.pos = 0;
      if (cursor.rows > 0) return true;
    }
  }

  #compareCursors(a: Cursor, i: number, b: Cursor, j: number): number {
    if (this.mode !== "keys")
      return compareRows(this.spec.types, this.spec.keys, a.columns, i, b.columns, j);
    for (let w = 0; w < PREFIX_WORDS; w++) {
      const x = int32At(a.columns[this.keyColumn + 1 + w], i) >>> 0;
      const y = int32At(b.columns[this.keyColumn + 1 + w], j) >>> 0;
      if (x !== y) return x < y ? -1 : 1;
    }
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

  abort(reason: unknown): void {
    if (this.#aborted === undefined) this.#aborted = reason ?? new Error("Aborted");
    this.close();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.store.close();
    this.encoder?.close(this.pool);
    this.partEncoder?.close(this.pool);
    for (const set of this.#sets) set?.close();
    if (this.#reserved > 0) this.charge.free(this.#reserved);
    if (this.#rankBytes > 0) this.charge.free(this.#rankBytes);
    this.#reserved = 0;
    this.#rankBytes = 0;
    this.pages.close();
    this.profile.peakHeldBytes = this.charge.peak;
    this.profile.loans += this.pool.loans;
    this.pool.close();
    this.profile.spilledBytes = this.files.written;
    this.files.closeAll().catch(() => undefined);
    this.context.grant.close();
  }

  /** Settles when the sort's spill files are closed (rejects with the first close failure). */
  get closed(): Promise<void> {
    return this.files.closed ?? Promise.resolve();
  }
}

function cursorAt(heap: readonly Cursor[], i: number): Cursor {
  const cursor = heap[i];
  if (cursor === undefined) throw new RangeError("No such merge input");
  return cursor;
}

const keptRows = new Uint16Array(BATCH_ROWS);
const partLengths = new Int32Array(BATCH_ROWS);
function partLengthVector(n: number): FlatVector {
  return {
    form: "flat",
    type: INT,
    length: n,
    validity: null,
    data: { layout: "i32", values: partLengths },
  };
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

function varlenLength(vector: Vector, row: number): number {
  const data = varlenOf(vector);
  return (data.offsets[row + 1] ?? 0) - (data.offsets[row] ?? 0);
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
    if (result === "blocked") return this.downstream.ready().then(done);
    done();
  }

  close(): void {
    this.out.close();
  }
}

/** Takes as many rows of `count` as the chunk holds; flushes when full; one oversized row borrows. */
async function emitRange(
  emitter: Emitter,
  range: Range,
  from: number,
  count: number,
  rowNumber: number,
): Promise<void> {
  let done = 0;
  while (done < count) {
    const taken = emitter.take(range, from + done, count - done, rowNumber + done);
    done += taken;
    if (done < count) {
      if (emitter.out.rows > 0) await emitter.flush();
      else {
        const id = range.kind === "store" ? (range.ids[from + done] ?? 0) : from + done;
        const need =
          range.kind === "store"
            ? emitter.out.appendFromNeed(emitter.sorter.store, emitter.payload, id)
            : emitter.out.appendNeed(range.columns.slice(0, emitter.sorter.width), id);
        await emitter.sorter.io.need(need);
      }
    }
  }
}

// --- operators ------------------------------------------------------------------------------

/** ORDER BY [LIMIT / OFFSET / WITH TIES] (see the module comment). */
export class SortOperator implements Operator {
  readonly spec: SortSpec;
  readonly #sorter: Sorter;
  readonly #emitter: Emitter;
  #pending: Promise<void> | null = null;
  #done = false;

  constructor(spec: SortSpec, context: SortContext, downstream: Operator) {
    this.spec = spec;
    const keep = spec.limit === null ? null : spec.offset + spec.limit;
    this.#sorter = new Sorter(
      { types: spec.types, keys: spec.keys, partition: 0, keep, withTies: spec.withTies },
      context,
    );
    this.#emitter = new Emitter(this.#sorter, downstream, false);
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
    return "blocked";
  }

  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  finish(): MaybePromise<void> {
    const sorter = this.#sorter;
    try {
      if (sorter.runs.length === 0) {
        if (sorter.spec.keep !== null) sorter.compact();
        const ids = sorter.sortedIds();
        if (ids !== null) {
          const result = this.#emitMemory(ids);
          if (!isPromise(result)) {
            this.#end();
            return this.#emitter.downstream.finish();
          }
          return result.then(
            () => {
              this.#end();
              return this.#emitter.downstream.finish();
            },
            (error: unknown) => {
              this.abort(error);
              throw error;
            },
          );
        }
      }
    } catch (error) {
      this.abort(error);
      throw error;
    }
    return this.#emitMerged().then(
      async () => {
        this.#end();
        await sorter.closed;
        await this.#emitter.downstream.finish();
      },
      (error: unknown) => {
        this.abort(error);
        throw error;
      },
    );
  }

  /** In-memory output: offset, limit and ties over sorted ids; synchronous unless it blocks. */
  #emitMemory(all: Int32Array): MaybePromise<void> {
    const sorter = this.#sorter;
    const spec = this.spec;
    let end = spec.limit === null ? all.length : Math.min(all.length, spec.offset + spec.limit);
    if (spec.withTies && spec.limit !== null && end > spec.offset && end < all.length)
      while (end < all.length && sorter.compareIds(all[end - 1] ?? 0, all[end] ?? 0) === 0) end++;
    const ids = all;
    const range: Range = {
      kind: "store",
      ids,
      from: spec.offset,
      count: Math.max(0, end - spec.offset),
    };
    const emitter = this.#emitter;
    let at = spec.offset;
    const step = (): MaybePromise<void> => {
      while (at < end) {
        const taken = emitter.take(range, at, end - at, 0);
        at += taken;
        if (taken === 0 || emitter.full) {
          if (taken === 0 && emitter.out.rows === 0) {
            const need = emitter.out.appendFromNeed(sorter.store, emitter.payload, ids[at] ?? 0);
            return sorter.io.need(need).then(step);
          }
          const flushed = emitter.flush();
          if (isPromise(flushed)) return flushed.then(step);
        }
      }
      const flushed = emitter.flush();
      if (isPromise(flushed)) return flushed;
    };
    return step();
  }

  /** External output: the merge, with offset counted, limit and ties. */
  async #emitMerged(): Promise<void> {
    const sorter = this.#sorter;
    const spec = this.spec;
    await sorter.prepareMerge();
    let skip = spec.offset;
    let left = spec.limit ?? Infinity;
    const last = new ColumnStore(sorter.storeTypes, sorter.pool, { chunkRows: 1, varlenBytes: 64 });
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
          if (!spec.withTies || last.rows === 0) break;
          let ties = 0;
          while (ties < count && sorter.sameKey(last.vectors(), 0, range.columns, from + ties))
            ties++;
          if (ties > 0) await emitRange(this.#emitter, range, from, ties, 0);
          if (ties < count) break;
          continue;
        }
        const n = Math.min(left, count);
        await emitRange(this.#emitter, range, from, n, 0);
        left -= n;
        if (spec.withTies) {
          last.reset();
          last.append(range.columns, null, from + n - 1, 1);
          // Ties past the limit inside this range.
          if (left <= 0 && n < count) {
            let ties = 0;
            while (
              n + ties < count &&
              sorter.sameKey(last.vectors(), 0, range.columns, from + n + ties)
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

  abort(reason: unknown): void {
    this.#sorter.abort(reason);
    if (!this.#done) {
      this.#done = true;
      this.#emitter.close();
    }
  }
}

/** `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= k` as a per-group top-k. */
export class GroupTopKOperator implements Operator {
  readonly spec: GroupTopKSpec;
  readonly #sorter: Sorter;
  readonly #emitter: Emitter;
  #pending: Promise<void> | null = null;
  #done = false;

  constructor(spec: GroupTopKSpec, context: SortContext, downstream: Operator) {
    this.spec = spec;
    const partition: SortColumn[] = spec.partitionBy.map((column) => ({
      column,
      direction: "asc",
      nulls: "first",
      collation: null,
    }));
    this.#sorter = new Sorter(
      {
        types: spec.types,
        keys: [...partition, ...spec.orderBy],
        partition: partition.length,
        keep: spec.k,
        withTies: false,
      },
      context,
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
    return this.#sorter.closed;
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
    return "blocked";
  }

  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }

  finish(): MaybePromise<void> {
    return this.#run().then(
      async () => {
        this.#end();
        await this.#sorter.closed;
        await this.#emitter.downstream.finish();
      },
      (error: unknown) => {
        this.abort(error);
        throw error;
      },
    );
  }

  async #run(): Promise<void> {
    const sorter = this.#sorter;
    const k = this.spec.k;
    if (sorter.runs.length === 0) {
      sorter.compact();
      const ids = sorter.sortedIds();
      if (ids !== null) {
        const kept = sorter.mode === "keys" ? sorter.cut(ids) : ids;
        // Row numbers restart at each partition; compare mode also cuts here.
        let i = 0;
        let rowNumber = 0;
        while (i < kept.length) {
          const id = kept[i] ?? 0;
          if (i === 0 || !sorter.samePartitionIds(kept[i - 1] ?? 0, id)) rowNumber = 0;
          rowNumber++;
          if (rowNumber <= k) {
            const range: Range = { kind: "store", ids: kept, from: i, count: 1 };
            await emitRange(this.#emitter, range, i, 1, rowNumber);
          }
          i++;
        }
        await this.#emitter.flush();
        return;
      }
    }
    await sorter.prepareMerge();
    const last = new ColumnStore(sorter.storeTypes, sorter.pool, { chunkRows: 1, varlenBytes: 64 });
    let rowNumber = 0;
    try {
      for await (const range of sorter.merge()) {
        for (let r = range.from; r < range.from + range.count; r++) {
          if (last.rows === 0 || !sorter.samePartition(last.vectors(), 0, range.columns, r))
            rowNumber = 0;
          rowNumber++;
          last.reset();
          last.append(range.columns, null, r, 1);
          if (rowNumber <= k) await emitRange(this.#emitter, range, r, 1, rowNumber);
        }
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

  abort(reason: unknown): void {
    this.#sorter.abort(reason);
    if (!this.#done) {
      this.#done = true;
      this.#emitter.close();
    }
  }
}
