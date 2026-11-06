/**
 * A chunked columnar store of rows an operator keeps past a batch's borrow: the build side of a
 * hash join, a spill partition read back, a spill page being filled, and the dense output batches
 * a join assembles. A shared exec/ file (ruling R20.2), owned by track B7: aggregation spill and
 * sort runs use it too.
 *
 * ## Shape
 * - One `FlatVector` per column per chunk. Chunk k holds rows `[k × chunkRows, (k + 1) ×
 *   chunkRows)`, so a row id is `chunk << chunkShift | slot` and a gather is two array reads.
 * - Every chunk of a column shares one representation, fixed by what was appended so far:
 *   - int64-family (int64, time, timestamp, interval µs): `safe` (Float64Array) until a value
 *     outside ±(2^53 − 1) arrives, then `int64` (BigInt64Array) for the whole column. A `int64`
 *     batch whose values are all safe is narrowed on the way in, so the fast path survives pages
 *     that could not prove their bounds.
 *   - decimal coefficients: the highest tier seen (safe → int64 → int128 → wide). Scales are kept
 *     as appended (uniform for a declared scale, per slot otherwise); `specials` always exist.
 *   - dictionary input is flattened; constants are expanded.
 * - int64 words are read and written through Int32 views (lo, hi), never as BigInt values, so
 *   no value allocates except in the wide decimal tier (which is `bigint[]` by definition).
 *
 * ## Memory
 * Every buffer is allocated from the store's `ByteCharge` before it exists, a varlen arena grows
 * with both arenas charged during the copy, and a representation upgrade charges the new chunk
 * buffers before freeing the old ones. When the charge refuses, `append` stops at a row boundary
 * and returns how many rows it took: no row is ever half appended. The caller spills or borrows a
 * progress loan (memory/governor.ts rule 5) and calls again.
 *
 * ## Single-chunk mode
 * With `targetBytes` set, a store is one reusable output chunk: `append` also stops when its
 * varlen bytes would pass the target (one oversized row is admitted alone), `reset` empties it
 * without freeing, and `vectors()` lends the chunk as a batch's columns.
 */
import { bigintBytes } from "../memory/governor.js";
import { layoutOf, type Layout, type PhysicalType } from "../types.js";
import { int128At, setInt128 } from "../vector/int128.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { FlatVector, Validity, Vector, VectorData } from "../vector/vector.js";

const TWO_32 = 4_294_967_296;
/**
 * Values up to this many bytes are copied with a loop, not `set(subarray(…))`, which allocates
 * a view per value; past it the view's cost is negligible next to the copy.
 */
const LOOP_COPY_BYTES = 4096;
/** Contiguous fixed-width appends of at most this many rows copy by a loop. */
const SMALL_COPY = 16;
const MAX_SAFE = Number.MAX_SAFE_INTEGER;

type Varlen = Extract<VectorData, { layout: "varlen" }>;
type Decimal = Extract<VectorData, { layout: "decimal" }>;
type Interval = Extract<VectorData, { layout: "interval" }>;

/** Coefficient / int64 representation tiers, ordered by width. */
export const TIER_SAFE = 0;
export const TIER_INT64 = 1;
export const TIER_INT128 = 2;
export const TIER_WIDE = 3;

// A cache, not scratch (R23.1): each view is fixed by its array, so sharing it is safe.
const wordViews = new WeakMap<BigInt64Array, Int32Array>();

/** The (lo, hi) Int32 words of a BigInt64Array, cached per array: no BigInt per read. */
export function wordsOf(values: BigInt64Array): Int32Array {
  let view = wordViews.get(values);
  if (view === undefined) {
    view = new Int32Array(values.buffer, values.byteOffset, values.length * 2);
    wordViews.set(values, view);
  }
  return view;
}

/** The value of words (lo, hi) as a number when it is a safe integer, else NaN. */
export function safeFromWords(lo: number, hi: number): number {
  const value = hi * TWO_32 + (lo >>> 0);
  return value >= -MAX_SAFE && value <= MAX_SAFE ? value : Number.NaN;
}

/** High word of a safe integer (its low word is `value >>> 0`). */
export function hiOfSafe(value: number): number {
  return Math.floor(value / TWO_32) | 0;
}

/** Bit `row` of a validity bitmap (null = valid). */
function valid(validity: Validity | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/** A row source resolved once per call: the flat data and how a row maps to a slot. */
interface Source {
  /** 0 flat (slot = row), 1 constant (slot 0), 2 dictionary (slot = codes[row]). */
  readonly form: 0 | 1 | 2;
  readonly flat: FlatVector;
  /** Null bits by row (flat, dictionary) or of slot 0 (constant). */
  readonly validity: Validity | null;
  readonly codes: Int32Array | null;
}

function resolve(vector: Vector): Source {
  if (vector.form === "flat")
    return { form: 0, flat: vector, validity: vector.validity, codes: null };
  if (vector.form === "constant")
    return { form: 1, flat: vector.value, validity: vector.value.validity, codes: null };
  return { form: 2, flat: vector.dictionary, validity: vector.validity, codes: vector.codes };
}

/** Source slot of `row`, or −1 when it is null. */
function slotOf(source: Source, row: number): number {
  switch (source.form) {
    case 0:
      return valid(source.validity, row) ? row : -1;
    case 1:
      return valid(source.validity, 0) ? 0 : -1;
    default:
      return valid(source.validity, row) ? (source.codes?.[row] ?? 0) : -1;
  }
}

/** Representation tier of an int64-family `Int64Values`. */
function int64Tier(values: { readonly repr: "safe" | "int64" }): number {
  return values.repr === "safe" ? TIER_SAFE : TIER_INT64;
}

function decimalTier(data: Decimal): number {
  switch (data.coefficients.repr) {
    case "safe":
      return TIER_SAFE;
    case "int64":
      return TIER_INT64;
    case "int128":
      return TIER_INT128;
    case "wide":
      return TIER_WIDE;
  }
}

/** Rows a store column hands to its writer: either a vector with row ids, or store chunks. */
interface Rows {
  /** Row ids into `vector` (null: identity from `from`), or store row ids (−1: null). */
  readonly ids: ArrayLike<number> | null;
  readonly from: number;
  readonly count: number;
}

class Column {
  readonly chunks: FlatVector[] = [];
  /** One all-ones-initialised bitmap per chunk; `chunk.validity` points at it once it has a null. */
  readonly bitmaps: Validity[] = [];
  /** Bytes used in each varlen chunk's arena. */
  readonly used: number[] = [];
  tier: number;
  hasNulls = false;
  /** Fixed-width layouts: each chunk's value array (int64: only at the safe tier), for gathers. */
  arrays: Array<Int32Array | Float64Array | Float32Array | Uint8Array> = [];
  constructor(
    readonly type: PhysicalType,
    readonly layout: Layout,
  ) {
    this.tier = TIER_SAFE;
  }
}

/** One value of slot `slot` of a decimal flat vector as a bigint coefficient. */
function coefficientAt(data: Decimal, slot: number): bigint {
  const coefficients = data.coefficients;
  switch (coefficients.repr) {
    case "safe":
      return BigInt(coefficients.values[slot] ?? 0);
    case "int64": {
      const words = wordsOf(coefficients.values);
      return (BigInt(words[slot * 2 + 1] ?? 0) << 32n) | BigInt((words[slot * 2] ?? 0) >>> 0);
    }
    case "int128":
      return int128At(coefficients.limbs, slot);
    case "wide":
      return coefficients.values[slot] ?? 0n;
  }
}

export interface ColumnStoreOptions {
  /** Rows per chunk, a power of two ≤ 65,536 (default 2,048). */
  readonly chunkRows?: number;
  /** Single-chunk mode: stop appending when varlen bytes would pass this. */
  readonly targetBytes?: number;
  /** Initial bytes of a varlen chunk arena (default 16 KiB, at most chunkRows × 32). */
  readonly varlenBytes?: number;
}

export class ColumnStore {
  readonly types: readonly PhysicalType[];
  readonly chunkRows: number;
  readonly chunkShift: number;
  readonly chunkMask: number;
  readonly targetBytes: number;
  rows = 0;
  /** Bytes allocated from the charge. */
  retainedBytes = 0;
  readonly #columns: Column[];
  readonly #charge: ByteCharge;
  readonly #varlenInitial: number;
  /** Wide decimal values held (charged per value). */
  #wideBytes = 0;
  #closed = false;

  constructor(
    types: readonly PhysicalType[],
    charge: ByteCharge,
    options: ColumnStoreOptions = {},
  ) {
    const chunkRows = options.chunkRows ?? 2048;
    if (!(chunkRows > 0 && chunkRows <= 65_536 && (chunkRows & (chunkRows - 1)) === 0))
      throw new RangeError("Store chunks are a power of two rows up to 65,536");
    this.types = types;
    this.chunkRows = chunkRows;
    this.chunkShift = Math.log2(chunkRows);
    this.chunkMask = chunkRows - 1;
    this.targetBytes = options.targetBytes ?? Infinity;
    this.#charge = charge;
    this.#varlenInitial = Math.max(64, Math.min(options.varlenBytes ?? 16_384, chunkRows * 32));
    this.#columns = types.map((type) => new Column(type, layoutOf(type)));
  }

  get chunkCount(): number {
    return this.#columns[0]?.chunks.length ?? 0;
  }

  /** The tier of column `c` (int64-family repr, or decimal coefficient tier). */
  tier(c: number): number {
    return this.#col(c).tier;
  }

  hasNulls(c: number): boolean {
    return this.#col(c).hasNulls;
  }

  /** Chunk k of column c as a flat vector whose `length` is the rows it holds. */
  chunk(c: number, k: number): FlatVector {
    const chunk = this.#col(c).chunks[k];
    if (chunk === undefined) throw new RangeError("No such chunk");
    chunk.length = Math.max(0, Math.min(this.chunkRows, this.rows - k * this.chunkRows));
    return chunk;
  }

  /** Single-chunk mode: the chunk's vectors, `length` = rows. Valid until the next append/reset. */
  vectors(): FlatVector[] {
    if (this.chunkCount === 0 && !this.#addChunk()) throw new RangeError("Store has no chunk");
    return this.#columns.map((_, c) => this.chunk(c, 0));
  }

  /** Varlen bytes column c holds in chunk k. */
  varlenUsed(c: number, k: number): number {
    return this.#col(c).used[k] ?? 0;
  }

  #col(c: number): Column {
    const column = this.#columns[c];
    if (column === undefined) throw new RangeError(`No column ${String(c)}`);
    return column;
  }

  #allocate(bytes: number): boolean {
    if (!this.#charge.allocate(bytes)) return false;
    this.retainedBytes += bytes;
    return true;
  }

  #free(bytes: number): void {
    this.#charge.free(bytes);
    this.retainedBytes -= bytes;
  }

  /**
   * Bytes an empty store of these columns charges for the chunks holding `rows` rows: the fixed
   * arrays and the initial varlen arenas (growth past them is the values' own).
   */
  fixedBytes(rows: number): number {
    let perChunk = 0;
    for (const column of this.#columns) perChunk += this.#chunkBytes(column);
    return Math.ceil(rows / this.chunkRows) * perChunk;
  }

  /** Bytes of one chunk of a column at its current tier. */
  #chunkBytes(column: Column): number {
    const rows = this.chunkRows;
    const bitmap = ((rows + 31) >>> 5) * 4;
    switch (column.layout) {
      case "bool":
        return bitmap + rows;
      case "i32":
      case "f32":
        return bitmap + rows * 4;
      case "f64":
      case "int64":
        return bitmap + rows * 8;
      case "fixed16":
        return bitmap + rows * 16;
      case "interval":
        return bitmap + rows * 16;
      case "varlen":
        return bitmap + (rows + 1) * 4 + this.#varlenInitial;
      case "decimal": {
        const scales = column.type.kind === "decimal" && column.type.scale === null ? rows * 2 : 0;
        const width = column.tier === TIER_INT128 ? 16 : 8;
        return bitmap + rows * (width + 1) + scales;
      }
    }
  }

  #newData(column: Column): VectorData {
    const rows = this.chunkRows;
    const type = column.type;
    const int64 = (tier: number) =>
      tier === TIER_SAFE
        ? ({ repr: "safe", values: new Float64Array(rows) } as const)
        : ({ repr: "int64", values: new BigInt64Array(rows) } as const);
    switch (column.layout) {
      case "bool":
        return { layout: "bool", values: new Uint8Array(rows) };
      case "i32":
        return { layout: "i32", values: new Int32Array(rows) };
      case "f32":
        return { layout: "f32", values: new Float32Array(rows) };
      case "f64":
        return { layout: "f64", values: new Float64Array(rows) };
      case "int64":
        return { layout: "int64", values: int64(column.tier) };
      case "fixed16":
        return { layout: "fixed16", bytes: new Uint8Array(rows * 16) };
      case "interval":
        return {
          layout: "interval",
          months: new Int32Array(rows),
          days: new Int32Array(rows),
          micros: int64(column.tier),
        };
      case "varlen":
        return {
          layout: "varlen",
          bytes: new Uint8Array(this.#varlenInitial),
          offsets: new Int32Array(rows + 1),
        };
      case "decimal": {
        const declared = type.kind === "decimal" ? type.scale : null;
        const coefficients =
          column.tier === TIER_INT128
            ? ({ repr: "int128", limbs: new Uint32Array(rows * 4) } as const)
            : column.tier === TIER_WIDE
              ? ({ repr: "wide", values: new Array<bigint>(rows).fill(0n) } as const)
              : int64(column.tier);
        return {
          layout: "decimal",
          scale: declared === null ? null : Math.max(0, declared),
          scales: declared === null ? new Uint16Array(rows) : null,
          coefficients,
          specials: new Uint8Array(rows),
        };
      }
    }
  }

  #addChunk(): boolean {
    let bytes = 0;
    for (const column of this.#columns) bytes += this.#chunkBytes(column);
    if (!this.#allocate(bytes)) return false;
    for (const column of this.#columns) {
      const bitmap = new Uint32Array((this.chunkRows + 31) >>> 5).fill(0xffff_ffff);
      column.chunks.push({
        form: "flat",
        type: column.type,
        length: 0,
        validity: null,
        data: this.#newData(column),
      });
      column.bitmaps.push(bitmap);
      column.used.push(0);
      pushArray(column);
    }
    return true;
  }

  /**
   * Converts every chunk of `column` to `tier`, charging the new buffers before freeing the old.
   * False (nothing changed) when the charge refuses.
   */
  #upgrade(column: Column, tier: number): boolean {
    if (tier <= column.tier) return true;
    const before = this.#chunkBytes(column);
    const previous = column.tier;
    column.tier = tier;
    const after = this.#chunkBytes(column);
    column.tier = previous;
    const chunks = column.chunks.length;
    if (!this.#allocate(after * chunks)) return false;
    const rows = this.chunkRows;
    for (let k = 0; k < chunks; k++) {
      const old = column.chunks[k];
      if (old === undefined) continue;
      column.tier = tier;
      const data = this.#newData(column);
      column.tier = previous;
      if (column.layout === "int64" || column.layout === "interval") {
        const from = column.layout === "int64" ? (old.data as { values: unknown }) : null;
        const oldValues =
          from !== null
            ? (old.data as Extract<VectorData, { layout: "int64" }>).values
            : (old.data as Interval).micros;
        const newValues =
          column.layout === "int64"
            ? (data as Extract<VectorData, { layout: "int64" }>).values
            : (data as Interval).micros;
        if (oldValues.repr === "safe" && newValues.repr === "int64") {
          const words = wordsOf(newValues.values);
          for (let i = 0; i < rows; i++) {
            const value = oldValues.values[i] ?? 0;
            words[i * 2] = value >>> 0;
            words[i * 2 + 1] = hiOfSafe(value);
          }
        }
        if (column.layout === "interval") {
          (data as Interval).months.set((old.data as Interval).months);
          (data as Interval).days.set((old.data as Interval).days);
        }
      } else if (column.layout === "decimal") {
        const source = old.data as Decimal;
        const target = data as Decimal;
        if (target.scales !== null && source.scales !== null) target.scales.set(source.scales);
        target.specials?.set(source.specials ?? new Uint8Array(0));
        const coefficients = target.coefficients;
        for (let i = 0; i < rows; i++) {
          if (coefficients.repr === "int64") {
            const value = (source.coefficients as { values: Float64Array }).values[i] ?? 0;
            const words = wordsOf(coefficients.values);
            words[i * 2] = value >>> 0;
            words[i * 2 + 1] = hiOfSafe(value);
          } else if (coefficients.repr === "int128") {
            writeLimbsFrom(coefficients.limbs, i, source, i);
          } else if (coefficients.repr === "wide") {
            const value = coefficientAt(source, i);
            coefficients.values[i] = value;
          }
        }
      }
      column.chunks[k] = {
        form: "flat",
        type: column.type,
        length: old.length,
        validity: old.validity,
        data,
      };
    }
    this.#free(before * chunks);
    column.tier = tier;
    column.arrays = [];
    for (let k = 0; k < column.chunks.length; k++) pushArray(column, k);
    if (tier === TIER_WIDE) this.#chargeWideColumn(column);
    return true;
  }

  /** On entering the wide tier, charges the BigInts already in the column. */
  #chargeWideColumn(column: Column): void {
    let bytes = 0;
    for (let k = 0; k < column.chunks.length; k++) {
      const data = column.chunks[k]?.data as Decimal | undefined;
      if (data?.coefficients.repr !== "wide") continue;
      const rows = Math.max(0, Math.min(this.chunkRows, this.rows - k * this.chunkRows));
      for (let i = 0; i < rows; i++) bytes += bigintBytes(data.coefficients.values[i] ?? 0n);
    }
    // The BigInts already exist; charging is accounting, so a refusal cannot undo them.
    this.#charge.allocate(bytes);
    this.retainedBytes += bytes;
    this.#wideBytes += bytes;
  }

  /**
   * Appends rows of `vectors` (one per store column): active rows `ids[from … from + count)` (or
   * `from … from + count` when `ids` is null). Returns how many it appended, in order; fewer than
   * `count` when the charge refused (or, in single-chunk mode, the chunk or its byte target filled).
   */
  append(
    vectors: readonly Vector[],
    ids: ArrayLike<number> | null,
    from: number,
    count: number,
    dense?: { readonly vectors: readonly Vector[]; readonly from: number },
  ): number {
    const sources = vectors.map(resolve);
    // Optional dense tail: the store's last columns, row k of this call at `dense.from + k`.
    const tail = dense?.vectors.length ?? 0;
    if (tail > 0 && dense !== undefined) sources.push(...dense.vectors.map(resolve));
    const split = sources.length - tail;
    const denseFrom = dense?.from ?? 0;
    const rowsOf = (c: number, n: number, done: number): Rows =>
      c >= split
        ? { ids: null, from: denseFrom + done, count: n }
        : { ids, from: from + done, count: n };
    return this.#appendWith(
      count,
      (n, done) =>
        this.#prepareVectors(sources, rowsOf, n, done) &&
        (this.#writeVectors(sources, rowsOf, n, done), true),
      (c, n, done) => this.#varlenNeedVector(sources[c], rowsOf(c, n, done)),
    );
  }

  /**
   * Appends rows of `source` by store row id: `ids[start … start + count)`, where −1 appends a
   * null row. Columns map store-to-store by position (`columns[c]` is the source column of `c`);
   * `columns[c] < 0` skips column c: its slots are left unwritten (empty for varlen), for a
   * caller that never reads them (the window's partition column, emitted as a constant).
   */
  appendFrom(
    source: ColumnStore,
    columns: readonly number[],
    ids: ArrayLike<number>,
    start: number,
    count: number,
  ): number {
    return this.#appendWith(
      count,
      (n, done) => {
        const rows: Rows = { ids, from: start + done, count: n };
        return (
          this.#prepareStore(source, columns, rows) &&
          (this.#writeStore(source, columns, rows), true)
        );
      },
      (c, n, done) =>
        (columns[c] ?? 0) < 0
          ? 0
          : this.#varlenNeedStore(source, columns[c] ?? 0, { ids, from: start + done, count: n }),
    );
  }

  /**
   * The shared append loop: per tail-chunk segment, reserve varlen bytes (halving the segment if
   * refused), prepare tiers, write, commit.
   */
  #appendWith(
    count: number,
    write: (n: number, done: number) => boolean,
    varlenNeed: (c: number, n: number, done: number) => number,
  ): number {
    if (this.#closed) return 0;
    let done = 0;
    while (done < count) {
      if (this.rows === this.chunkCount * this.chunkRows) {
        if (this.targetBytes !== Infinity && this.chunkCount > 0) return done;
        if (!this.#addChunk()) return done;
      }
      const k = this.chunkCount - 1;
      const at = this.rows - k * this.chunkRows;
      let n = Math.min(this.chunkRows - at, count - done);
      for (;;) {
        const fits = this.#reserveVarlen(k, at, n, done, varlenNeed);
        if (fits === n) break;
        if (fits <= 0) return done;
        n = fits;
      }
      if (!write(n, done)) return done;
      this.rows += n;
      done += n;
      for (const column of this.#columns) {
        const chunk = column.chunks[k];
        if (chunk !== undefined) chunk.length = at + n;
      }
    }
    return done;
  }

  /**
   * Grows varlen arenas of chunk k for n more rows. Returns n when they fit, a smaller row count
   * to retry with, or 0 when not even one row fits.
   */
  #reserveVarlen(
    k: number,
    at: number,
    n: number,
    done: number,
    varlenNeed: (c: number, n: number, done: number) => number,
  ): number {
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      if (column?.layout !== "varlen") continue;
      const used = column.used[k] ?? 0;
      const need = varlenNeed(c, n, done);
      if (used + need > this.targetBytes && (at > 0 || n > 1)) {
        if (n === 1) return 0;
        return Math.max(1, n >> 1);
      }
      const chunk = column.chunks[k];
      if (chunk === undefined) return 0;
      const data = chunk.data as Varlen;
      if (used + need <= data.bytes.length) continue;
      const size = this.#growTo(data.bytes.length, used + need);
      if (!this.#allocate(size)) {
        if (n === 1) return 0;
        return Math.max(1, n >> 1);
      }
      const grown = new Uint8Array(size);
      grown.set(data.bytes.subarray(0, used));
      this.#free(data.bytes.length);
      column.chunks[k] = {
        form: "flat",
        type: column.type,
        length: chunk.length,
        validity: chunk.validity,
        data: { layout: "varlen", bytes: grown, offsets: data.offsets },
      };
    }
    return n;
  }

  /**
   * The arena size growth picks: doubling, but in single-chunk mode exactly the need once it
   * passes the target (one oversized value is charged at its size, not twice it).
   */
  #growTo(length: number, need: number): number {
    if (this.targetBytes !== Infinity && need > this.targetBytes) return need;
    let size = Math.max(1, length) * 2;
    while (size < need) size *= 2;
    return size;
  }

  /** Bytes a varlen arena of chunk k must grow by to take `length` more (0 if it fits). */
  #arenaGrowth(column: Column, k: number, newChunk: boolean, length: number): number {
    const used = newChunk ? 0 : (column.used[k] ?? 0);
    const current = newChunk
      ? this.#varlenInitial
      : ((column.chunks[k]?.data as Varlen | undefined)?.bytes.length ?? 0);
    return used + length <= current ? 0 : this.#growTo(current, used + length);
  }

  #varlenNeedVector(source: Source | undefined, rows: Rows): number {
    if (source === undefined) return 0;
    const data = source.flat.data as Varlen;
    let bytes = 0;
    for (let i = 0; i < rows.count; i++) {
      const row = rows.ids === null ? rows.from + i : (rows.ids[rows.from + i] ?? 0);
      const slot = slotOf(source, row);
      if (slot >= 0) bytes += (data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0);
    }
    return bytes;
  }

  #varlenNeedStore(source: ColumnStore, c: number, rows: Rows): number {
    const column = source.#col(c);
    const shift = source.chunkShift;
    const mask = source.chunkMask;
    let bytes = 0;
    for (let i = 0; i < rows.count; i++) {
      const id = rows.ids?.[rows.from + i] ?? -1;
      if (id < 0) continue;
      const chunk = column.chunks[id >>> shift];
      if (chunk === undefined) continue;
      const slot = id & mask;
      if (!valid(chunk.validity, slot)) continue;
      const offsets = (chunk.data as Varlen).offsets;
      bytes += (offsets[slot + 1] ?? 0) - (offsets[slot] ?? 0);
    }
    return bytes;
  }

  /** Raises column tiers so the incoming vector rows fit; false if the charge refused. */
  #prepareVectors(
    sources: readonly Source[],
    rowsOf: (c: number, n: number, done: number) => Rows,
    n: number,
    done: number,
  ): boolean {
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      const source = sources[c];
      if (column === undefined || source === undefined) continue;
      const need = this.#tierNeedVector(column, source, rowsOf(c, n, done));
      if (need > column.tier && !this.#upgrade(column, need)) return false;
    }
    return true;
  }

  #tierNeedVector(column: Column, source: Source, rows: Rows): number {
    const data = source.flat.data;
    let values: { readonly repr: "safe" | "int64"; readonly values: Float64Array | BigInt64Array };
    if (data.layout === "int64") values = data.values;
    else if (data.layout === "interval") values = data.micros;
    else if (data.layout === "decimal") {
      const tier = decimalTier(data);
      if (tier !== TIER_INT64 || column.tier >= TIER_INT64) return tier;
      values = data.coefficients as { repr: "int64"; values: BigInt64Array };
    } else return column.tier;
    if (values.repr === "safe" || column.tier >= TIER_INT64) return int64Tier(values);
    // Narrow optimistically: an int64 batch whose values are safe keeps the safe column.
    const words = wordsOf(values.values as BigInt64Array);
    for (let i = 0; i < rows.count; i++) {
      const row = rows.ids === null ? rows.from + i : (rows.ids[rows.from + i] ?? 0);
      const slot = slotOf(source, row);
      if (slot < 0) continue;
      if (Number.isNaN(safeFromWords(words[slot * 2] ?? 0, words[slot * 2 + 1] ?? 0)))
        return TIER_INT64;
    }
    return TIER_SAFE;
  }

  #prepareStore(source: ColumnStore, columns: readonly number[], rows: Rows): boolean {
    void rows;
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      if (column === undefined || (columns[c] ?? 0) < 0) continue;
      const need = source.#col(columns[c] ?? 0).tier;
      if (need > column.tier && !this.#upgrade(column, need)) return false;
    }
    return true;
  }

  #writeVectors(
    sources: readonly Source[],
    rowsOf: (c: number, n: number, done: number) => Rows,
    n: number,
    done: number,
  ): void {
    const k = this.chunkCount - 1;
    const at = this.rows - k * this.chunkRows;
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      const source = sources[c];
      if (column === undefined || source === undefined) continue;
      writeFromSource(column, k, at, source, rowsOf(c, n, done));
    }
  }

  #writeStore(source: ColumnStore, columns: readonly number[], rows: Rows): void {
    const k = this.chunkCount - 1;
    const at = this.rows - k * this.chunkRows;
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      if (column === undefined) continue;
      if ((columns[c] ?? 0) < 0) {
        skipRows(column, k, at, rows.count);
        continue;
      }
      writeFromStore(column, k, at, source.#col(columns[c] ?? 0), source.chunkShift, rows);
    }
    if (this.#columns.some((column) => column.tier === TIER_WIDE))
      this.#chargeWideRows(k, at, rows.count);
  }

  #chargeWideRows(k: number, at: number, n: number): void {
    let bytes = 0;
    for (const column of this.#columns) {
      const data = column.chunks[k]?.data as Decimal | undefined;
      if (data?.coefficients.repr !== "wide") continue;
      for (let i = at; i < at + n; i++) bytes += bigintBytes(data.coefficients.values[i] ?? 0n);
    }
    this.#charge.allocate(bytes);
    this.retainedBytes += bytes;
    this.#wideBytes += bytes;
  }

  /**
   * An upper bound of the bytes appending row `row` of `vectors` would allocate (a new chunk, and
   * varlen arenas growing by doubling): the size of a progress loan for one row.
   */
  appendNeed(vectors: readonly Vector[], row: number): number {
    const sources = vectors.map(resolve);
    let bytes = 0;
    const newChunk = this.rows === this.chunkCount * this.chunkRows;
    const k = this.chunkCount - 1;
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      if (column === undefined) continue;
      if (newChunk) bytes += this.#chunkBytes(column);
      const source = sources[c];
      if (column.layout !== "varlen" || source === undefined) continue;
      const length = this.#varlenNeedVector(source, { ids: [row], from: 0, count: 1 });
      bytes += this.#arenaGrowth(column, k, newChunk, length);
    }
    return bytes;
  }

  /** `appendNeed` for one row `id` of another store. */
  appendFromNeed(source: ColumnStore, columns: readonly number[], id: number): number {
    let bytes = 0;
    const newChunk = this.rows === this.chunkCount * this.chunkRows;
    const k = this.chunkCount - 1;
    for (let c = 0; c < this.#columns.length; c++) {
      const column = this.#columns[c];
      if (column === undefined) continue;
      if (newChunk) bytes += this.#chunkBytes(column);
      if (column.layout !== "varlen") continue;
      const length = this.#varlenNeedStore(source, columns[c] ?? 0, {
        ids: [id],
        from: 0,
        count: 1,
      });
      bytes += this.#arenaGrowth(column, k, newChunk, length);
    }
    return bytes;
  }

  /** Drops rows past `rows`, freeing whole chunks beyond it. */
  truncate(rows: number): void {
    if (rows < 0 || rows > this.rows) throw new RangeError("Truncate outside the store");
    const keep = Math.ceil(rows / this.chunkRows);
    for (const column of this.#columns) {
      while (column.chunks.length > keep) {
        const k = column.chunks.length - 1;
        const chunk = column.chunks.pop();
        column.bitmaps.pop();
        column.used.pop();
        column.arrays.length = Math.min(column.arrays.length, column.chunks.length);
        if (chunk === undefined) continue;
        let bytes = this.#chunkBytes(column);
        if (column.layout === "varlen")
          bytes += (chunk.data as Varlen).bytes.length - this.#varlenInitial;
        void k;
        this.#free(bytes);
      }
    }
    this.rows = rows;
    const last = keep - 1;
    if (last >= 0) {
      const at = rows - last * this.chunkRows;
      for (const column of this.#columns) {
        const chunk = column.chunks[last];
        if (chunk === undefined) continue;
        chunk.length = at;
        if (column.layout === "varlen") column.used[last] = (chunk.data as Varlen).offsets[at] ?? 0;
      }
    }
  }

  /**
   * Single-chunk mode: empties the chunk, keeping buffers but shrinking an arena past the target
   * (with `shrink`, every arena past its initial size: give a progress loan back).
   */
  reset(shrink = false): void {
    this.rows = 0;
    for (const column of this.#columns) {
      const bitmap = column.bitmaps[0];
      bitmap?.fill(0xffff_ffff);
      const chunk = column.chunks[0];
      if (chunk === undefined) continue;
      chunk.validity = null;
      chunk.length = 0;
      column.used[0] = 0;
      column.hasNulls = false;
      if (column.layout === "varlen") {
        const data = chunk.data as Varlen;
        const limit = shrink
          ? this.#varlenInitial
          : Math.max(this.#varlenInitial, this.targetBytes === Infinity ? 0 : this.targetBytes);
        if (data.bytes.length > limit) {
          this.#free(data.bytes.length - this.#varlenInitial);
          column.chunks[0] = {
            form: "flat",
            type: column.type,
            length: 0,
            validity: null,
            data: {
              layout: "varlen",
              bytes: new Uint8Array(this.#varlenInitial),
              offsets: data.offsets,
            },
          };
        }
      }
    }
    if (this.#wideBytes > 0) {
      this.#free(this.#wideBytes);
      this.#wideBytes = 0;
    }
  }

  /** Returns everything to the charge. Idempotent; the store is empty and inert afterwards. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const column of this.#columns) {
      column.chunks.length = 0;
      column.bitmaps.length = 0;
      column.used.length = 0;
      column.arrays.length = 0;
    }
    if (this.retainedBytes > 0) this.#charge.free(this.retainedBytes);
    this.retainedBytes = 0;
    this.rows = 0;
  }
}

/** Marks slot `at` of chunk k null. */
function setNull(column: Column, k: number, at: number): void {
  const chunk = column.chunks[k];
  const bitmap = column.bitmaps[k];
  if (chunk === undefined || bitmap === undefined) return;
  bitmap[at >>> 5] = (bitmap[at >>> 5] ?? 0) & ~(1 << (at & 31));
  chunk.validity = bitmap;
  column.hasNulls = true;
}

/** Marks slot `at` valid (only needed when a chunk slot is rewritten). */
function setValidSlot(column: Column, k: number, at: number): void {
  const bitmap = column.bitmaps[k];
  if (bitmap !== undefined) bitmap[at >>> 5] = (bitmap[at >>> 5] ?? 0) | (1 << (at & 31));
}

/** Writes int64-family `values` slot into an int64-family target at its column tier. */
function writeInt64(
  target: { readonly repr: "safe" | "int64"; readonly values: Float64Array | BigInt64Array },
  at: number,
  source: { readonly repr: "safe" | "int64"; readonly values: Float64Array | BigInt64Array },
  slot: number,
): void {
  if (target.repr === "safe") {
    if (source.repr === "safe") (target.values as Float64Array)[at] = source.values[slot] as number;
    else {
      const words = wordsOf(source.values as BigInt64Array);
      (target.values as Float64Array)[at] = safeFromWords(
        words[slot * 2] ?? 0,
        words[slot * 2 + 1] ?? 0,
      );
    }
    return;
  }
  const out = wordsOf(target.values as BigInt64Array);
  if (source.repr === "safe") {
    const value = (source.values as Float64Array)[slot] ?? 0;
    out[at * 2] = value >>> 0;
    out[at * 2 + 1] = hiOfSafe(value);
  } else {
    const words = wordsOf(source.values as BigInt64Array);
    out[at * 2] = words[slot * 2] ?? 0;
    out[at * 2 + 1] = words[slot * 2 + 1] ?? 0;
  }
}

/** Writes the int128 limbs of slot `slot` of a decimal at or below the int128 tier. */
function writeLimbsFrom(limbs: Uint32Array, at: number, source: Decimal, slot: number): void {
  const coefficients = source.coefficients;
  let lo: number;
  let hi: number;
  if (coefficients.repr === "int128") {
    for (let limb = 0; limb < 4; limb++)
      limbs[at * 4 + limb] = coefficients.limbs[slot * 4 + limb] ?? 0;
    return;
  }
  if (coefficients.repr === "safe") {
    const value = coefficients.values[slot] ?? 0;
    lo = value >>> 0;
    hi = hiOfSafe(value);
  } else if (coefficients.repr === "int64") {
    const words = wordsOf(coefficients.values);
    lo = words[slot * 2] ?? 0;
    hi = words[slot * 2 + 1] ?? 0;
  } else {
    setInt128(limbs, at, coefficients.values[slot] ?? 0n);
    return;
  }
  const extension = hi < 0 ? 0xffff_ffff : 0;
  limbs[at * 4] = lo >>> 0;
  limbs[at * 4 + 1] = hi >>> 0;
  limbs[at * 4 + 2] = extension;
  limbs[at * 4 + 3] = extension;
}

/** Copies one slot of any decimal into a decimal target of tier ≥ the source's. */
function writeDecimal(target: Decimal, at: number, source: Decimal, slot: number): void {
  const coefficients = target.coefficients;
  if (coefficients.repr === "safe" || coefficients.repr === "int64")
    writeInt64(coefficients, at, source.coefficients as typeof coefficients, slot);
  else if (coefficients.repr === "int128") writeLimbsFrom(coefficients.limbs, at, source, slot);
  else coefficients.values[at] = coefficientAt(source, slot);
  const scale = source.scale ?? source.scales?.[slot] ?? 0;
  if (target.scales !== null) target.scales[at] = scale;
  else if (target.scale !== scale)
    throw new TypeError(
      `A decimal of scale ${String(scale)} cannot join a column of scale ${String(target.scale)}`,
    );
  if (target.specials !== null) target.specials[at] = source.specials?.[slot] ?? 0;
}

/** One value: slot `slot` of `flat` (non-null) into slot `at` of chunk `target`. */
function writeSlot(
  column: Column,
  target: FlatVector,
  at: number,
  flat: FlatVector,
  slot: number,
  used: number,
): number {
  const out = target.data;
  const data = flat.data;
  switch (out.layout) {
    case "bool":
      out.values[at] = (data as typeof out).values[slot] ?? 0;
      return used;
    case "i32":
      out.values[at] = (data as typeof out).values[slot] ?? 0;
      return used;
    case "f32":
      out.values[at] = (data as typeof out).values[slot] ?? 0;
      return used;
    case "f64":
      out.values[at] = (data as typeof out).values[slot] ?? 0;
      return used;
    case "int64":
      writeInt64(out.values, at, (data as typeof out).values, slot);
      return used;
    case "fixed16": {
      const from = (data as typeof out).bytes;
      for (let b = 0; b < 16; b++) out.bytes[at * 16 + b] = from[slot * 16 + b] ?? 0;
      return used;
    }
    case "interval": {
      const source = data as typeof out;
      out.months[at] = source.months[slot] ?? 0;
      out.days[at] = source.days[slot] ?? 0;
      writeInt64(out.micros, at, source.micros, slot);
      return used;
    }
    case "decimal":
      writeDecimal(out, at, data as Decimal, slot);
      return used;
    case "varlen": {
      const source = data as Varlen;
      const start = source.offsets[slot] ?? 0;
      const end = source.offsets[slot + 1] ?? 0;
      const length = end - start;
      if (length <= LOOP_COPY_BYTES) {
        const from = source.bytes;
        const to = out.bytes;
        for (let b = 0; b < length; b++) to[used + b] = from[start + b] ?? 0;
      } else out.bytes.set(source.bytes.subarray(start, end), used);
      used += length;
      out.offsets[at + 1] = used;
      return used;
    }
  }
  void column;
}

/** Finishes a null slot: a varlen null is an empty value; the bitmap carries the null. */
/** Leaves `count` slots from `at` unwritten (a varlen column's values empty). */
function skipRows(column: Column, k: number, at: number, count: number): void {
  const target = column.chunks[k];
  if (target?.data.layout !== "varlen") return;
  const used = column.used[k] ?? 0;
  const offsets = target.data.offsets;
  if (at === 0) offsets[0] = 0;
  for (let i = 1; i <= count; i++) offsets[at + i] = used;
}

function writeNull(column: Column, k: number, target: FlatVector, at: number, used: number): void {
  setNull(column, k, at);
  if (target.data.layout === "varlen") target.data.offsets[at + 1] = used;
}

function writeFromSource(column: Column, k: number, at: number, source: Source, rows: Rows): void {
  const target = column.chunks[k];
  if (target === undefined) return;
  const ids = rows.ids;
  const n = rows.count;
  const out = target.data;
  const flatNoNulls = source.form === 0 && source.validity === null;
  // Fast paths: fixed-width flat sources without nulls.
  if (flatNoNulls && out.layout === source.flat.data.layout) {
    switch (out.layout) {
      case "i32":
      case "f64":
      case "f32":
      case "bool": {
        const src = (
          source.flat.data as { values: Int32Array | Float64Array | Float32Array | Uint8Array }
        ).values;
        const dst = out.values;
        // A few rows copy by a loop: a subarray per call costs more than it saves.
        if (ids === null && n <= SMALL_COPY)
          for (let i = 0; i < n; i++) dst[at + i] = src[rows.from + i] ?? 0;
        else if (ids === null) dst.set(src.subarray(rows.from, rows.from + n), at);
        else if (dst instanceof Int32Array) copyI32(src as Int32Array, ids, rows.from, n, dst, at);
        else if (dst instanceof Float64Array)
          copyF64(src as Float64Array, ids, rows.from, n, dst, at);
        else if (dst instanceof Float32Array)
          copyF32(src as Float32Array, ids, rows.from, n, dst, at);
        else copyU8(src as Uint8Array, ids, rows.from, n, dst, at);
        if (target.validity !== null) for (let i = 0; i < n; i++) setValidSlot(column, k, at + i);
        return;
      }
      case "int64": {
        const src = (source.flat.data as typeof out).values;
        const dst = out.values;
        if (src.repr === "safe" && dst.repr === "safe") {
          if (ids === null && n <= SMALL_COPY)
            for (let i = 0; i < n; i++) dst.values[at + i] = src.values[rows.from + i] ?? 0;
          else if (ids === null) dst.values.set(src.values.subarray(rows.from, rows.from + n), at);
          else copyF64(src.values, ids, rows.from, n, dst.values, at);
        } else if (src.repr === "int64" && dst.repr === "int64") {
          const from = wordsOf(src.values);
          const to = wordsOf(dst.values);
          for (let i = 0; i < n; i++) {
            const row = ids === null ? rows.from + i : (ids[rows.from + i] ?? 0);
            to[(at + i) * 2] = from[row * 2] ?? 0;
            to[(at + i) * 2 + 1] = from[row * 2 + 1] ?? 0;
          }
        } else break;
        if (target.validity !== null) for (let i = 0; i < n; i++) setValidSlot(column, k, at + i);
        return;
      }
      default:
        break;
    }
  }
  let used = column.used[k] ?? 0;
  if (out.layout === "varlen" && at === 0) out.offsets[0] = 0;
  if (out.layout === "varlen" && flatNoNulls) {
    // Flat bytes without nulls: contiguous rows copy in bulk, selected rows without a switch.
    const data = source.flat.data as Varlen;
    const offsets = data.offsets;
    const bytes = data.bytes;
    const to = out.bytes;
    const toOffsets = out.offsets;
    if (ids === null) {
      const start = offsets[rows.from] ?? 0;
      const end = offsets[rows.from + n] ?? 0;
      to.set(bytes.subarray(start, end), used);
      const shift = used - start;
      for (let i = 1; i <= n; i++) toOffsets[at + i] = (offsets[rows.from + i] ?? 0) + shift;
      used += end - start;
    } else
      for (let i = 0; i < n; i++) {
        const row = ids[rows.from + i] ?? 0;
        const start = offsets[row] ?? 0;
        const length = (offsets[row + 1] ?? 0) - start;
        // A byte loop: a subarray per row would allocate a view per value.
        if (length <= LOOP_COPY_BYTES)
          for (let b = 0; b < length; b++) to[used + b] = bytes[start + b] ?? 0;
        else to.set(bytes.subarray(start, start + length), used);
        used += length;
        toOffsets[at + i + 1] = used;
      }
    if (target.validity !== null) for (let i = 0; i < n; i++) setValidSlot(column, k, at + i);
    column.used[k] = used;
    return;
  }
  for (let i = 0; i < n; i++) {
    const row = ids === null ? rows.from + i : (ids[rows.from + i] ?? 0);
    const slot = slotOf(source, row);
    if (slot < 0) {
      writeNull(column, k, target, at + i, used);
      continue;
    }
    if (target.validity !== null) setValidSlot(column, k, at + i);
    used = writeSlot(column, target, at + i, source.flat, slot, used);
  }
  column.used[k] = used;
}

function writeFromStore(
  column: Column,
  k: number,
  at: number,
  source: Column,
  shift: number,
  rows: Rows,
): void {
  const target = column.chunks[k];
  if (target === undefined) return;
  const ids = rows.ids;
  if (ids === null) return;
  const n = rows.count;
  const mask = (1 << shift) - 1;
  const out = target.data;
  const chunks = source.chunks;
  // Fast paths: fixed-width columns without nulls in the source, one monomorphic kernel each.
  if (!source.hasNulls) {
    let nulls = -1;
    const from = rows.from;
    switch (out.layout) {
      case "i32":
        nulls = gatherI32(source.arrays as Int32Array[], ids, from, n, out.values, at, shift, mask);
        break;
      case "f64":
        nulls = gatherF64(
          source.arrays as Float64Array[],
          ids,
          from,
          n,
          out.values,
          at,
          shift,
          mask,
        );
        break;
      case "f32":
        nulls = gatherF32(
          source.arrays as Float32Array[],
          ids,
          from,
          n,
          out.values,
          at,
          shift,
          mask,
        );
        break;
      case "bool":
        nulls = gatherU8(source.arrays as Uint8Array[], ids, from, n, out.values, at, shift, mask);
        break;
      case "int64":
        if (out.values.repr === "safe" && source.tier === TIER_SAFE)
          nulls = gatherF64(
            source.arrays as Float64Array[],
            ids,
            from,
            n,
            out.values.values,
            at,
            shift,
            mask,
          );
        break;
      default:
        break;
    }
    if (nulls >= 0) {
      if (nulls > 0 || target.validity !== null)
        for (let i = 0; i < n; i++) {
          if ((ids[from + i] ?? -1) < 0) writeNull(column, k, target, at + i, 0);
          else if (target.validity !== null) setValidSlot(column, k, at + i);
        }
      return;
    }
    switch (out.layout) {
      case "int64": {
        const dst = out.values;
        if (dst.repr !== (source.tier === TIER_SAFE ? "safe" : "int64")) break;
        let nulls = false;
        if (dst.repr === "safe") {
          const values = dst.values;
          for (let i = 0; i < n; i++) {
            const id = ids[rows.from + i] ?? -1;
            if (id < 0) {
              nulls = true;
              continue;
            }
            values[at + i] =
              ((chunks[id >>> shift]?.data as typeof out).values.values as Float64Array)[
                id & mask
              ] ?? 0;
          }
        } else {
          const to = wordsOf(dst.values);
          for (let i = 0; i < n; i++) {
            const id = ids[rows.from + i] ?? -1;
            if (id < 0) {
              nulls = true;
              continue;
            }
            const from = wordsOf(
              (chunks[id >>> shift]?.data as typeof out).values.values as BigInt64Array,
            );
            const slot = id & mask;
            to[(at + i) * 2] = from[slot * 2] ?? 0;
            to[(at + i) * 2 + 1] = from[slot * 2 + 1] ?? 0;
          }
        }
        if (nulls || target.validity !== null)
          for (let i = 0; i < n; i++) {
            if ((ids[rows.from + i] ?? -1) < 0) writeNull(column, k, target, at + i, 0);
            else if (target.validity !== null) setValidSlot(column, k, at + i);
          }
        return;
      }
      default:
        break;
    }
  }
  let used = column.used[k] ?? 0;
  if (out.layout === "varlen" && at === 0) out.offsets[0] = 0;
  if (out.layout === "varlen" && !source.hasNulls) {
    // Bytes of non-null rows: one loop, no per-value dispatch or view.
    const to = out.bytes;
    const toOffsets = out.offsets;
    let nulls = false;
    for (let i = 0; i < n; i++) {
      const id = ids[rows.from + i] ?? -1;
      if (id < 0) {
        nulls = true;
        toOffsets[at + i + 1] = used;
        continue;
      }
      const data = chunks[id >>> shift]?.data as Varlen | undefined;
      if (data === undefined) continue;
      const slot = id & mask;
      const start = data.offsets[slot] ?? 0;
      const length = (data.offsets[slot + 1] ?? 0) - start;
      const from = data.bytes;
      if (length <= LOOP_COPY_BYTES)
        for (let b = 0; b < length; b++) to[used + b] = from[start + b] ?? 0;
      else to.set(from.subarray(start, start + length), used);
      used += length;
      toOffsets[at + i + 1] = used;
    }
    column.used[k] = used;
    if (nulls || target.validity !== null)
      for (let i = 0; i < n; i++) {
        if ((ids[rows.from + i] ?? -1) < 0) setNull(column, k, at + i);
        else if (target.validity !== null) setValidSlot(column, k, at + i);
      }
    return;
  }
  for (let i = 0; i < n; i++) {
    const id = ids[rows.from + i] ?? -1;
    const chunk = id < 0 ? undefined : chunks[id >>> shift];
    const slot = id & mask;
    if (chunk === undefined || !valid(chunk.validity, slot)) {
      writeNull(column, k, target, at + i, used);
      continue;
    }
    if (target.validity !== null) setValidSlot(column, k, at + i);
    used = writeSlot(column, target, at + i, chunk, slot, used);
  }
  column.used[k] = used;
}

/** A constant null vector of `type` (one shared slot), for null-extended join sides. */
export function nullConstant(type: PhysicalType, length: number, charge?: ByteCharge): Vector {
  const store = new ColumnStore([type], charge ?? { allocate: () => true, free: () => undefined }, {
    chunkRows: 1,
    varlenBytes: 64,
  });
  const value = store.vectors()[0];
  if (value === undefined) throw new RangeError("No null slot");
  value.length = 1;
  value.validity = new Uint32Array(1);
  if (value.data.layout === "varlen") value.data.offsets[1] = 0;
  return { form: "constant", type, length, value };
}

/** Records chunk k's value array (fixed-width layouts) for the gather kernels. */
function pushArray(column: Column, k = column.chunks.length - 1): void {
  const data = column.chunks[k]?.data;
  if (data === undefined) return;
  switch (data.layout) {
    case "i32":
    case "f64":
    case "f32":
    case "bool":
      column.arrays[k] = data.values;
      break;
    case "int64":
      if (data.values.repr === "safe") column.arrays[k] = data.values.values;
      break;
    default:
      break;
  }
}

/*
 * Gather kernels: `dst[at + i] = chunk(ids[from + i])[slot]`, −1 ids skipped. One per array type,
 * so every element access stays monomorphic. They return how many ids were −1.
 */
/** A fully checked contiguous run (endpoints alone do not establish a permutation). */
function contiguousStart(ids: ArrayLike<number>, from: number, n: number): number {
  const first = ids[from] ?? -1;
  if (n < 2 || first < 0 || ids[from + n - 1] !== first + n - 1) return -1;
  for (let i = 1; i + 1 < n; i++) if (ids[from + i] !== first + i) return -1;
  return first;
}

function gatherI32(
  arrays: Int32Array[],
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Int32Array,
  at: number,
  shift: number,
  mask: number,
): number {
  const first = contiguousStart(ids, from, n);
  if (first >= 0) {
    let copied = 0;
    while (copied < n) {
      const id = first + copied;
      const values = arrays[id >>> shift];
      if (values === undefined) break;
      const slot = id & mask;
      const take = Math.min(n - copied, (1 << shift) - slot);
      dst.set(values.subarray(slot, slot + take), at + copied);
      copied += take;
    }
    if (copied === n) return 0;
  }
  let nulls = 0;
  for (let i = 0; i < n; i++) {
    const id = ids[from + i] ?? -1;
    if (id < 0) nulls++;
    else dst[at + i] = arrays[id >>> shift]?.[id & mask] ?? 0;
  }
  return nulls;
}

function gatherF64(
  arrays: Float64Array[],
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Float64Array,
  at: number,
  shift: number,
  mask: number,
): number {
  const first = contiguousStart(ids, from, n);
  if (first >= 0) {
    let copied = 0;
    while (copied < n) {
      const id = first + copied;
      const values = arrays[id >>> shift];
      if (values === undefined) break;
      const slot = id & mask;
      const take = Math.min(n - copied, (1 << shift) - slot);
      dst.set(values.subarray(slot, slot + take), at + copied);
      copied += take;
    }
    if (copied === n) return 0;
  }
  let nulls = 0;
  for (let i = 0; i < n; i++) {
    const id = ids[from + i] ?? -1;
    if (id < 0) nulls++;
    else dst[at + i] = arrays[id >>> shift]?.[id & mask] ?? 0;
  }
  return nulls;
}

function gatherF32(
  arrays: Float32Array[],
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Float32Array,
  at: number,
  shift: number,
  mask: number,
): number {
  let nulls = 0;
  for (let i = 0; i < n; i++) {
    const id = ids[from + i] ?? -1;
    if (id < 0) nulls++;
    else dst[at + i] = arrays[id >>> shift]?.[id & mask] ?? 0;
  }
  return nulls;
}

function gatherU8(
  arrays: Uint8Array[],
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Uint8Array,
  at: number,
  shift: number,
  mask: number,
): number {
  let nulls = 0;
  for (let i = 0; i < n; i++) {
    const id = ids[from + i] ?? -1;
    if (id < 0) nulls++;
    else dst[at + i] = arrays[id >>> shift]?.[id & mask] ?? 0;
  }
  return nulls;
}

/* Row copies from one vector by row id, one monomorphic kernel per array type. */
function copyI32(
  src: Int32Array,
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Int32Array,
  at: number,
): void {
  for (let i = 0; i < n; i++) dst[at + i] = src[ids[from + i] ?? 0] ?? 0;
}
function copyF64(
  src: Float64Array,
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Float64Array,
  at: number,
): void {
  for (let i = 0; i < n; i++) dst[at + i] = src[ids[from + i] ?? 0] ?? 0;
}
function copyF32(
  src: Float32Array,
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Float32Array,
  at: number,
): void {
  for (let i = 0; i < n; i++) dst[at + i] = src[ids[from + i] ?? 0] ?? 0;
}
function copyU8(
  src: Uint8Array,
  ids: ArrayLike<number>,
  from: number,
  n: number,
  dst: Uint8Array,
  at: number,
): void {
  for (let i = 0; i < n; i++) dst[at + i] = src[ids[from + i] ?? 0] ?? 0;
}
