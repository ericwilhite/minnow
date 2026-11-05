/**
 * The streaming column-group writer: rows in (as vectors), one group's column chunks out, written
 * into the store's data space as they fill.
 *
 * ## Shape of what it writes
 * - Each column is cut into pages of about `pageBytes` encoded bytes (default 32 KiB), from a raw
 *   page buffer of at most `rawPageBytes` (default 64 KiB). The next page's row target comes from
 *   the previous page's encoded bytes per row, so compressible columns get longer pages.
 * - Text, bytes, json, enum, array and uuid columns start with a group-scoped dictionary. A page
 *   whose dictionary form (codes plus its new entries) would be larger than plain, or that would
 *   overflow the dictionary's bounds or its memory grant, is written plain, and the dictionary
 *   takes no more entries: later pages are plain too. The dictionary page is written at the end.
 * - With `gzip`, varlen and uuid pages ≥ 16 KiB are offered to gzip; it is kept at ≥ 2× (gzip.ts).
 *   After two misses in a row a column stops trying for the rest of the group. Off by default:
 *   the space caches encoded blobs, so a gzip page inflates on every load.
 * - A chunk is `[pages][dictionary page][directory]`, 8-aligned. Its directory `BlobRef` and its
 *   distinct sketch `BlobRef` point inside the chunk's extent.
 *
 * ## Placement (format/manifest.ts: a chunk is one contiguous extent; offsets `DATA_ALIGN`ed)
 * - A chunk stays in memory until it passes `inlineBytes` (default 16 KiB). Chunks still that
 *   small at `finish` are packed into one shared extent (their `ColumnChunk.data` is the same
 *   extent; page offsets are relative to it), so a narrow column does not cost a 4 KiB extent.
 * - A larger chunk reserves its own extent from the `ExtentAllocator`, sized from its rows so far,
 *   and is written in `DATA_ALIGN` multiples once `slabBytes` (default 16 KiB) are staged. When it
 *   outgrows the reservation the writer first asks to extend it in place and otherwise relocates
 *   it (copies the written prefix to a bigger extent and releases the old one). `finish` trims
 *   each extent to what it used.
 *
 * ## Memory
 * Per column: the raw page buffer, the staged bytes (< `slabBytes` + one page), the dictionary
 * (≤ 256 KiB of entries plus its table) and the page entries. Nothing is O(group) except the
 * entries. With a `charge` (memory/governor.ts grant), the fixed part is charged at construction
 * and every growth before it happens; a refused dictionary growth falls back to plain pages, and
 * a refused growth for a single oversized value throws `RangeError` (the reserve's one size error).
 *
 * ## Values
 * - Input vectors may be flat, constant or dictionary. Their type must equal the column's.
 * - Decimals (DECISIONS.md C2): a declared NUMERIC(p, s) is a fixed-scale chunk (a value at
 *   another scale is a `RangeError`); an unconstrained NUMERIC is a mixed-scale chunk that keeps
 *   every value's own scale. int64 and int128 coefficients are stored; coefficients past 128 bits
 *   (`bigint[]`) raise `UnsupportedPageLayoutError` (no stored form yet).
 */
import type { ColumnChunk, ExtentRef, PageEntry, StatValue } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { DataRange } from "../storage/contract.js";
import { DATA_ALIGN } from "../storage/contract.js";
import { samePhysicalType, type ColumnStatistics, type PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import { compareBytes } from "../vector/varlen.js";
import type { FlatVector, Int64Values, Vector } from "../vector/vector.js";
import { copyBits, countBits, fillBits } from "./bits.js";
import { DictionaryBuilder } from "./dictionary.js";
import { encodeDirectory } from "./directory.js";
import {
  ByteBounds,
  encodeBoolPage,
  encodeDictionaryEntries,
  encodeDictionaryPage,
  encodeFixed16Page,
  encodeFloatPage,
  encodeInt128Page,
  encodeInt32Page,
  encodeInt64Page,
  encodeIntervalPage,
  encodeVarlenPage,
  PageScratch,
  type EncodedPage,
} from "./encode.js";
import { align8, chunkTagOf, UnsupportedPageLayoutError, ValueTag } from "./format.js";
import { gzipPage } from "./gzip.js";
import { MAX_BLOB_BYTES } from "./reader.js";
import { DistinctSketch, hashInt64, mix32 } from "./sketch.js";
import { int128At } from "../vector/int128.js";

/** Where extents come from: the write path's free-space map (the caller owns its policy). */
export interface ExtentAllocator {
  /** A free extent of at least `length` bytes at a `DATA_ALIGN` offset. */
  reserve(length: number): ExtentRef;
  /** Grows `extent` in place to at least `length`, or null when the space after it is taken. */
  extend(extent: ExtentRef, length: number): ExtentRef | null;
  /** Frees everything past the first `used` bytes; returns the kept extent. */
  trim(extent: ExtentRef, used: number): ExtentRef;
  /** Frees the whole extent. */
  release(extent: ExtentRef): void;
}

/** The slice of a `StoreSession` the writer needs. */
export interface ExtentStore {
  writeData(offset: number, bytes: Uint8Array): Promise<void>;
  readData(ranges: readonly DataRange[]): Promise<Uint8Array[]>;
}

export interface WriterColumn {
  readonly columnId: number;
  readonly type: PhysicalType;
}

export interface GroupWriterOptions {
  readonly columns: readonly WriterColumn[];
  readonly store: ExtentStore;
  readonly allocator: ExtentAllocator;
  /** Rows per group (default 65,536, the most a locator position addresses). */
  readonly maxRows?: number;
  readonly pageBytes?: number;
  readonly rawPageBytes?: number;
  readonly inlineBytes?: number;
  readonly slabBytes?: number;
  /**
   * Offer large varlen and uuid pages to gzip (default false): a gzip page inflates on every load,
   * because the space caches encoded blobs (ruling R5) — about 200 µs per 32 KiB page.
   */
  readonly gzip?: boolean;
  readonly charge?: ByteCharge;
  /** Dictionary bounds per chunk (default 65,536 entries, 256 KiB). */
  readonly dictionaryEntries?: number;
  readonly dictionaryBytes?: number;
}

export interface WrittenGroup {
  readonly rowCount: number;
  readonly columns: readonly ColumnChunk[];
  /** Every extent the group's chunks use (each once), for the caller's free-space accounting. */
  readonly extents: readonly ExtentRef[];
}

export const MAX_GROUP_ROWS = 65_536;
/**
 * Raw page bytes never pass this (offsets, validity and header fit the rest of a 1 MiB blob),
 * and a dictionary's entries never pass `MAX_DICTIONARY_BYTES` (its offsets fit the rest).
 */
const MAX_RAW_PAGE_BYTES = MAX_BLOB_BYTES - 192 * 1024;
const MAX_DICTIONARY_BYTES = 512 * 1024;
const DEFAULTS = {
  pageBytes: 32 * 1024,
  rawPageBytes: 64 * 1024,
  inlineBytes: 16 * 1024,
  slabBytes: 16 * 1024,
};

class Charged {
  #charged = 0;
  constructor(readonly charge: ByteCharge | null) {}
  get bytes(): number {
    return this.#charged;
  }
  allocate(bytes: number): boolean {
    if (bytes <= 0) return true;
    if (this.charge !== null && !this.charge.allocate(bytes)) return false;
    this.#charged += bytes;
    return true;
  }
  grow(bytes: number): void {
    if (!this.allocate(bytes))
      throw new RangeError("The writer's memory grant refused a value larger than its buffers");
  }
  free(bytes: number): void {
    if (bytes <= 0) return;
    this.#charged -= bytes;
    this.charge?.free(bytes);
  }
}

// ---------------------------------------------------------------------------------------------
// Spool: a chunk's bytes, in memory or streaming into its own extent

class ChunkSpool {
  staged: Uint8Array;
  stagedLength = 0;
  extent: ExtentRef | null = null;
  /** Bytes already written to `extent` (a multiple of DATA_ALIGN). */
  flushed = 0;

  constructor(
    readonly writer: GroupWriter,
    initial: number,
  ) {
    this.staged = new Uint8Array(initial);
  }

  get position(): number {
    return this.flushed + this.stagedLength;
  }

  /** Appends `bytes` at the next 8-aligned chunk offset and returns that offset. */
  append(bytes: Uint8Array): number {
    const padded = align8(this.stagedLength) - this.stagedLength;
    const needed = this.stagedLength + padded + bytes.length;
    if (needed > this.staged.length) {
      let size = this.staged.length * 2;
      while (size < needed) size *= 2;
      this.writer.memory.grow(size);
      const grown = new Uint8Array(size);
      grown.set(this.staged.subarray(0, this.stagedLength));
      this.writer.memory.free(this.staged.length);
      this.staged = grown;
    }
    this.staged.fill(0, this.stagedLength, this.stagedLength + padded);
    this.stagedLength += padded;
    const offset = this.position;
    this.staged.set(bytes, this.stagedLength);
    this.stagedLength += bytes.length;
    return offset;
  }

  async #ensureCapacity(end: number, estimate: number): Promise<void> {
    const allocator = this.writer.allocator;
    if (this.extent === null) {
      this.extent = this.writer.reserve(Math.max(end, estimate));
      return;
    }
    if (end <= this.extent.length) return;
    const wanted = Math.max(end, estimate, this.extent.length * 2);
    const grown = allocator.extend(this.extent, wanted);
    if (grown !== null) {
      this.writer.replaceExtent(this.extent, grown);
      this.extent = grown;
      return;
    }
    const fresh = this.writer.reserve(wanted);
    const store = this.writer.store;
    const step = 1024 * 1024;
    for (let done = 0; done < this.flushed; done += step) {
      const length = Math.min(step, this.flushed - done);
      const [chunk] = await store.readData([{ offset: this.extent.offset + done, length }]);
      if (chunk?.length !== length)
        throw new RangeError("Relocation read returned the wrong length");
      await store.writeData(fresh.offset + done, chunk.slice());
    }
    this.writer.releaseExtent(this.extent);
    this.extent = fresh;
  }

  /** Streams whole `DATA_ALIGN` multiples once past the inline size and `slabBytes` staged. */
  async maybeFlush(estimate: number): Promise<void> {
    const options = this.writer.options;
    if (this.extent === null && this.position <= options.inlineBytes) return;
    if (this.stagedLength < options.slabBytes) return;
    const length = this.stagedLength - (this.stagedLength % DATA_ALIGN);
    await this.#ensureCapacity(this.flushed + length, estimate);
    await this.#write(length);
  }

  async #write(length: number): Promise<void> {
    const extent = this.extent;
    if (extent === null || length === 0) return;
    await this.writer.store.writeData(extent.offset + this.flushed, this.staged.slice(0, length));
    this.staged.copyWithin(0, length, this.stagedLength);
    this.stagedLength -= length;
    this.flushed += length;
  }

  /** Writes everything staged into the chunk's own extent and trims it. */
  async finishStreaming(): Promise<ExtentRef> {
    const end = this.position;
    await this.#ensureCapacity(end, end);
    await this.#write(this.stagedLength);
    const extent = this.extent;
    if (extent === null) throw new Error("A streamed chunk has no extent");
    const kept = this.writer.allocator.trim(extent, end);
    this.writer.replaceExtent(extent, kept);
    this.extent = kept;
    return kept;
  }
}

// ---------------------------------------------------------------------------------------------
// Column writers

interface PageStats {
  min: StatValue | null;
  max: StatValue | null;
  nulls: number;
  unknownMin: boolean;
  unknownMax: boolean;
}

function compareStat(tag: number, a: StatValue, b: StatValue): number {
  if (a instanceof Uint8Array && b instanceof Uint8Array)
    return compareBytes(a, 0, a.length, b, 0, b.length);
  if (typeof a === "boolean") return Number(a) - Number(b);
  if (tag === ValueTag.f64 || tag === ValueTag.f32) {
    const x = a as number;
    const y = b as number;
    // PostgreSQL order: NaN above everything.
    if (x !== x) return y !== y ? 0 : 1;
    if (y !== y) return -1;
  }
  return a < b ? -1 : a > b ? 1 : 0;
}

abstract class ColumnWriter {
  readonly tag: number;
  readonly sketch = new DistinctSketch();
  readonly spool: ChunkSpool;
  readonly entries: PageEntry[] = [];
  readonly stats: PageStats = {
    min: null,
    max: null,
    nulls: 0,
    unknownMin: false,
    unknownMax: false,
  };
  validity: Uint32Array;
  /** Whether a slot of the current page may be null (else encoders skip validity checks). */
  mayHaveNulls = false;
  rows = 0;
  rowsWritten = 0;
  bytesWritten = 0;
  pageRows: number;
  dictionaryEntry: PageEntry | null = null;
  #gzipMisses = 0;

  constructor(
    readonly writer: GroupWriter,
    readonly column: WriterColumn,
    readonly capacity: number,
    readonly rawWidth: number,
  ) {
    this.tag = chunkTagOf(column.type);
    this.validity = new Uint32Array((capacity + 31) >>> 5);
    this.pageRows = Math.max(
      1,
      Math.min(capacity, Math.floor(writer.options.pageBytes / Math.max(1, rawWidth))),
    );
    this.spool = new ChunkSpool(writer, 4096);
  }

  /** Fixed bytes held for this column (charged at construction). */
  get fixedBytes(): number {
    return (
      this.validity.byteLength + this.spool.staged.byteLength + this.sketch.registers.byteLength
    );
  }

  /** Rows of `vector` from `from` that fit the current page (≥ 1 when the page is empty). */
  room(vector: Vector, from: number, count: number): number {
    void vector;
    void from;
    return Math.min(count, this.pageRows - this.rows);
  }

  setValidity(vector: Vector, from: number, count: number): void {
    const at = this.rows;
    if (vector.form === "constant") {
      const valid = vector.value.validity === null || ((vector.value.validity[0] ?? 0) & 1) === 1;
      fillBits(this.validity, at, count, valid);
      if (!valid) this.mayHaveNulls = true;
      return;
    }
    if (vector.validity === null) fillBits(this.validity, at, count, true);
    else this.mayHaveNulls = true;
    if (vector.validity === null) return;
    if (vector.form === "flat") copyBits(vector.validity, from, this.validity, at, count);
    else
      for (let k = 0; k < count; k++) {
        const row = at + k;
        const valid = (((vector.validity[(from + k) >>> 5] ?? 0) >>> ((from + k) & 31)) & 1) === 1;
        this.validity[row >>> 5] = valid
          ? (this.validity[row >>> 5] ?? 0) | (1 << (row & 31))
          : (this.validity[row >>> 5] ?? 0) & ~(1 << (row & 31));
      }
  }

  /**
   * Whether this column's pages are offered to gzip. Only varlen and uuid pages are: numeric
   * pages are already narrow, rarely halve, and each attempt costs a full deflate.
   */
  get offersGzip(): boolean {
    return this.tag === ValueTag.varlen || this.tag === ValueTag.fixed16;
  }

  /** The page's validity for an encoder: null when no slot can be null. */
  get pageValidity(): Uint32Array | null {
    return this.mayHaveNulls ? this.validity : null;
  }

  /** Copies the values of rows `[from, from + count)` into the raw page at `this.rows`. */
  abstract copyValues(vector: Vector, from: number, count: number): void;
  abstract encode(): EncodedPage;
  abstract resetPage(): void;
  /** Bytes the raw page holds now (for the byte-based cut). */
  rawBytes(): number {
    return this.rows * this.rawWidth;
  }

  isFull(): boolean {
    return this.rows >= this.pageRows || this.rawBytes() >= this.writer.options.rawPageBytes;
  }

  async append(vector: Vector, from: number, count: number): Promise<void> {
    let done = 0;
    while (done < count) {
      const take = this.room(vector, from + done, count - done);
      this.setValidity(vector, from + done, take);
      this.copyValues(vector, from + done, take);
      this.rows += take;
      done += take;
      if (this.isFull()) await this.flushPage();
    }
  }

  async flushPage(): Promise<void> {
    if (this.rows === 0) return;
    const page = this.encode();
    await this.storePage(page);
    this.rowsWritten += this.rows;
    // Next page's row target from this page's density.
    const perRow = page.bytes.length / this.rows;
    const target = Math.floor(this.writer.options.pageBytes / Math.max(perRow, 1 / 64));
    this.pageRows = Math.max(64, Math.min(this.capacity, target - (target % 64)));
    this.rows = 0;
    this.mayHaveNulls = false;
    this.resetPage();
  }

  async storePage(page: EncodedPage): Promise<void> {
    let stored = page.bytes;
    let encoding = page.encoding;
    if (
      this.offersGzip &&
      this.writer.options.gzip &&
      this.#gzipMisses < 2 &&
      stored.length >= 16 * 1024
    ) {
      const zipped = await gzipPage(stored);
      if (zipped === null) this.#gzipMisses++;
      else {
        this.#gzipMisses = 0;
        stored = zipped;
        encoding |= PageEncoding.gzip;
      }
    }
    if (stored.length > MAX_BLOB_BYTES)
      throw new UnsupportedPageLayoutError(
        `A page of ${String(stored.length)} bytes passes the 1 MiB blob cap: values that large need a large-value layout, not stored yet`,
      );
    const offset = this.spool.append(stored);
    this.entries.push({
      offset,
      length: stored.length,
      firstRow: this.rowsWritten,
      rowCount: page.rowCount,
      encoding,
      nullCount: page.nullCount,
      min: page.min,
      max: page.max,
      checksum: xxHash32(stored),
    });
    this.bytesWritten += stored.length;
    this.#mergeStats(page);
    const estimate =
      Math.ceil(
        (this.spool.position / Math.max(1, this.rowsWritten + page.rowCount)) *
          this.writer.options.maxRows *
          1.25,
      ) + 4096;
    await this.spool.maybeFlush(estimate);
  }

  #mergeStats(page: EncodedPage): void {
    const stats = this.stats;
    stats.nulls += page.nullCount;
    if (page.nullCount === page.rowCount) return;
    if (page.min === null) stats.unknownMin = true;
    else if (stats.min === null || compareStat(this.tag, page.min, stats.min) < 0)
      stats.min = page.min;
    if (page.max === null) stats.unknownMax = true;
    else if (stats.max === null || compareStat(this.tag, page.max, stats.max) > 0)
      stats.max = page.max;
  }

  /** The dictionary page bytes, if the chunk has one (varlen / uuid). */
  dictionaryPage(): Uint8Array | null {
    return null;
  }
  dictionarySize(): number | null {
    return null;
  }
  scale(): number | null {
    return 0;
  }
  hasSpecials(): boolean {
    return false;
  }
  close(): void {
    // Subclasses with charged extras release them.
  }
}

/** Source row of slot `row` of a vector: flat as is, constant 0, dictionary its code. */
function sourceRow(vector: Vector, row: number): number {
  return vector.form === "flat" ? row : vector.form === "constant" ? 0 : (vector.codes[row] ?? 0);
}

function sourceFlat(vector: Vector): FlatVector {
  return vector.form === "flat"
    ? vector
    : vector.form === "constant"
      ? vector.value
      : vector.dictionary;
}

class BoolWriter extends ColumnWriter {
  values: Uint8Array;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const capacity = Math.min(writer.options.maxRows, writer.options.rawPageBytes);
    super(writer, column, capacity, 1);
    this.values = new Uint8Array(capacity);
  }
  override get fixedBytes(): number {
    return super.fixedBytes + this.values.byteLength;
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const data = sourceFlat(vector).data;
    if (data.layout !== "bool") throw new TypeError("Expected bool data");
    if (vector.form === "flat")
      this.values.set(data.values.subarray(from, from + count), this.rows);
    else
      for (let k = 0; k < count; k++)
        this.values[this.rows + k] = data.values[sourceRow(vector, from + k)] ?? 0;
  }
  encode(): EncodedPage {
    return encodeBoolPage(
      this.values,
      this.pageValidity,
      this.rows,
      this.sketch,
      this.writer.scratch,
    );
  }
  resetPage(): void {
    // Values are overwritten by the next page.
  }
}

class Int32Writer extends ColumnWriter {
  values: Int32Array;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const capacity = Math.min(writer.options.maxRows, writer.options.rawPageBytes >>> 2);
    super(writer, column, capacity, 4);
    this.values = new Int32Array(capacity);
  }
  override get fixedBytes(): number {
    return super.fixedBytes + this.values.byteLength;
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const data = sourceFlat(vector).data;
    if (data.layout !== "i32") throw new TypeError("Expected i32 data");
    if (vector.form === "flat")
      this.values.set(data.values.subarray(from, from + count), this.rows);
    else
      for (let k = 0; k < count; k++)
        this.values[this.rows + k] = data.values[sourceRow(vector, from + k)] ?? 0;
  }
  encode(): EncodedPage {
    return encodeInt32Page(
      this.values,
      this.pageValidity,
      this.rows,
      this.sketch,
      this.writer.scratch,
    );
  }
  resetPage(): void {
    // Values are overwritten by the next page.
  }
}

const TWO32 = 4_294_967_296;

/** Writes an int64-family slot as (low, high) words into `words[2 × slot …]`. */
function setWords(words: Int32Array, slot: number, value: number): void {
  const high = Math.floor(value / TWO32);
  words[slot * 2] = (value - high * TWO32) | 0;
  words[slot * 2 + 1] = high | 0;
}

class Int64Writer extends ColumnWriter {
  words: Int32Array;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const capacity = Math.min(writer.options.maxRows, writer.options.rawPageBytes >>> 3);
    super(writer, column, capacity, 8);
    this.words = new Int32Array(capacity * 2);
  }
  override get fixedBytes(): number {
    return super.fixedBytes + this.words.byteLength;
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const data = sourceFlat(vector).data;
    if (data.layout !== "int64") throw new TypeError("Expected int64 data");
    copyInt64(data.values, vector, from, count, this.words, this.rows);
  }
  encode(): EncodedPage {
    return encodeInt64Page(
      this.words,
      this.pageValidity,
      this.rows,
      ValueTag.i64,
      this.sketch,
      this.writer.scratch,
    );
  }
  resetPage(): void {
    // Values are overwritten by the next page.
  }
}

function copyInt64(
  values: Int64Values,
  vector: Vector,
  from: number,
  count: number,
  words: Int32Array,
  at: number,
): void {
  if (values.repr === "int64") {
    const source = new Int32Array(
      values.values.buffer,
      values.values.byteOffset,
      values.values.length * 2,
    );
    if (vector.form === "flat") words.set(source.subarray(from * 2, (from + count) * 2), at * 2);
    else
      for (let k = 0; k < count; k++) {
        const row = sourceRow(vector, from + k);
        words[(at + k) * 2] = source[row * 2] ?? 0;
        words[(at + k) * 2 + 1] = source[row * 2 + 1] ?? 0;
      }
    return;
  }
  const source = values.values;
  for (let k = 0; k < count; k++) {
    const value = source[sourceRow(vector, from + k)] ?? 0;
    // Garbage under a null slot (NaN included) never throws: store 0.
    setWords(words, at + k, Number.isFinite(value) ? value : 0);
  }
}

class FloatWriter extends ColumnWriter {
  values: Float64Array | Float32Array;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const wide = column.type.kind === "float64";
    const width = wide ? 8 : 4;
    const capacity = Math.min(
      writer.options.maxRows,
      Math.floor(writer.options.rawPageBytes / width),
    );
    super(writer, column, capacity, width);
    this.values = wide ? new Float64Array(capacity) : new Float32Array(capacity);
  }
  override get fixedBytes(): number {
    return super.fixedBytes + this.values.byteLength;
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const data = sourceFlat(vector).data;
    if (data.layout !== "f64" && data.layout !== "f32") throw new TypeError("Expected float data");
    if (vector.form === "flat")
      this.values.set(data.values.subarray(from, from + count), this.rows);
    else
      for (let k = 0; k < count; k++)
        this.values[this.rows + k] = data.values[sourceRow(vector, from + k)] ?? 0;
  }
  encode(): EncodedPage {
    return encodeFloatPage(
      this.values,
      this.pageValidity,
      this.rows,
      this.sketch,
      this.writer.scratch,
    );
  }
  resetPage(): void {
    // Values are overwritten by the next page.
  }
}

class IntervalWriter extends ColumnWriter {
  months: Int32Array;
  days: Int32Array;
  micros: Int32Array;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const capacity = Math.min(writer.options.maxRows, writer.options.rawPageBytes >>> 4);
    super(writer, column, capacity, 16);
    this.months = new Int32Array(capacity);
    this.days = new Int32Array(capacity);
    this.micros = new Int32Array(capacity * 2);
  }
  override get fixedBytes(): number {
    return super.fixedBytes + this.months.byteLength * 4;
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const data = sourceFlat(vector).data;
    if (data.layout !== "interval") throw new TypeError("Expected interval data");
    for (let k = 0; k < count; k++) {
      const row = sourceRow(vector, from + k);
      this.months[this.rows + k] = data.months[row] ?? 0;
      this.days[this.rows + k] = data.days[row] ?? 0;
    }
    copyInt64(data.micros, vector, from, count, this.micros, this.rows);
  }
  encode(): EncodedPage {
    return encodeIntervalPage(
      this.months,
      this.days,
      this.micros,
      this.pageValidity,
      this.rows,
      this.sketch,
      this.writer.scratch,
    );
  }
  resetPage(): void {
    // Values are overwritten by the next page.
  }
}

/**
 * NUMERIC. A declared scale (NUMERIC(p, s)) makes a fixed-scale chunk: every value is at s. An
 * unconstrained NUMERIC makes a mixed-scale chunk: each page keeps its rows' scales (one scale
 * for the page when they agree), never rescaled (DECISIONS.md C2), and the distinct sketch hashes
 * the value with trailing fraction zeros removed, so 1.5 and 1.50 count once.
 */
class DecimalWriter extends ColumnWriter {
  limbs: Uint32Array;
  specials: Uint8Array;
  scales: Uint16Array | null;
  pairs: Int32Array;
  readonly #fixedScale: number | null;
  #sawSpecials = false;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const capacity = Math.min(writer.options.maxRows, writer.options.rawPageBytes >>> 4);
    super(writer, column, capacity, 16);
    this.limbs = new Uint32Array(capacity * 4);
    this.specials = new Uint8Array(capacity);
    this.pairs = new Int32Array(capacity * 2);
    const type = column.type;
    this.#fixedScale =
      type.kind === "decimal" && type.scale !== null ? Math.max(0, type.scale) : null;
    this.scales = this.#fixedScale === null ? new Uint16Array(capacity) : null;
  }
  override get fixedBytes(): number {
    return (
      super.fixedBytes +
      this.limbs.byteLength +
      this.specials.byteLength +
      this.pairs.byteLength +
      (this.scales?.byteLength ?? 0)
    );
  }
  override scale(): number | null {
    return this.#fixedScale;
  }
  override hasSpecials(): boolean {
    return this.#sawSpecials;
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const flat = sourceFlat(vector);
    const data = flat.data;
    if (data.layout !== "decimal") throw new TypeError("Expected decimal data");
    const coefficients = data.coefficients;
    if (coefficients.repr === "wide")
      throw new UnsupportedPageLayoutError("NUMERIC coefficients past 128 bits are not stored yet");
    const words =
      coefficients.repr === "int64"
        ? new Int32Array(
            coefficients.values.buffer,
            coefficients.values.byteOffset,
            coefficients.values.length * 2,
          )
        : null;
    const at = this.rows;
    const fixed = this.#fixedScale;
    for (let k = 0; k < count; k++) {
      const row = sourceRow(vector, from + k);
      const valid =
        flat.validity === null || (((flat.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1;
      const scale = data.scale ?? data.scales?.[row] ?? 0;
      if (valid && fixed !== null && scale !== fixed)
        throw new RangeError(
          `NUMERIC column ${String(this.column.columnId)} is declared at scale ${String(fixed)} but received scale ${String(scale)}`,
        );
      if (this.scales !== null) this.scales[at + k] = valid ? scale : 0;
      const special = valid && data.specials !== null ? (data.specials[row] ?? 0) : 0;
      this.specials[at + k] = special;
      if (special !== 0) this.#sawSpecials = true;
      const slot = (at + k) * 4;
      if (special !== 0 || !valid) {
        this.limbs.fill(0, slot, slot + 4);
        continue;
      }
      if (coefficients.repr === "int128") {
        this.limbs[slot] = coefficients.limbs[row * 4] ?? 0;
        this.limbs[slot + 1] = coefficients.limbs[row * 4 + 1] ?? 0;
        this.limbs[slot + 2] = coefficients.limbs[row * 4 + 2] ?? 0;
        this.limbs[slot + 3] = coefficients.limbs[row * 4 + 3] ?? 0;
        continue;
      }
      let low: number;
      let high: number;
      if (words !== null) {
        low = words[row * 2] ?? 0;
        high = words[row * 2 + 1] ?? 0;
      } else {
        const value = coefficients.repr === "safe" ? (coefficients.values[row] ?? 0) : 0;
        high = Math.floor(value / TWO32);
        low = (value - high * TWO32) | 0;
      }
      this.limbs[slot] = low >>> 0;
      this.limbs[slot + 1] = high >>> 0;
      this.limbs[slot + 2] = (high >> 31) >>> 0;
      this.limbs[slot + 3] = (high >> 31) >>> 0;
    }
  }
  encode(): EncodedPage {
    const scales = this.scales;
    if (scales !== null) this.#hashMixed(scales);
    return encodeInt128Page(
      this.limbs,
      this.pageValidity,
      this.rows,
      { specials: this.specials, scales },
      scales === null ? this.sketch : null,
      this.writer.scratch,
      this.pairs,
    );
  }
  /** Sketch hashes by value: coefficient and scale with trailing fraction zeros removed. */
  #hashMixed(scales: Uint16Array): void {
    const validity = this.pageValidity;
    for (let row = 0; row < this.rows; row++) {
      if (validity !== null && (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0) continue;
      const special = this.specials[row] ?? 0;
      if (special !== 0) {
        this.sketch.add(mix32(0x5bec_1a10 + special));
        continue;
      }
      let scale = scales[row] ?? 0;
      const slot = row * 4;
      const high = (this.limbs[slot + 1] ?? 0) | 0;
      const sign = (high >> 31) >>> 0;
      if (
        (this.limbs[slot + 2] ?? 0) === sign &&
        (this.limbs[slot + 3] ?? 0) === sign &&
        high > -0x20_0000 &&
        high < 0x20_0000
      ) {
        // |coefficient| < 2^53: normalize in f64, exactly, with no BigInt.
        let value = high * TWO32 + (this.limbs[slot] ?? 0);
        while (scale > 0 && value % 10 === 0 && value !== 0) {
          value /= 10;
          scale--;
        }
        if (value === 0) scale = 0;
        const top = Math.floor(value / TWO32);
        this.sketch.add(
          mix32(hashInt64((value - top * TWO32) | 0, top | 0) ^ Math.imul(scale + 1, 0x9e37_79b1)),
        );
        continue;
      }
      let coefficient = int128At(this.limbs, row);
      while (scale > 0 && coefficient % 10n === 0n) {
        coefficient /= 10n;
        scale--;
      }
      if (coefficient === 0n) scale = 0;
      const lowWord = Number(BigInt.asUintN(32, coefficient));
      const highWord = Number(BigInt.asIntN(32, coefficient >> 32n));
      this.sketch.add(mix32(hashInt64(lowWord, highWord) ^ Math.imul(scale + 1, 0x9e37_79b1)));
    }
  }
  resetPage(): void {
    // Values are overwritten by the next page.
  }
}

/** varlen (text, bytes, json, enum, array) and fixed16 (uuid), with the group dictionary. */
class BytesWriter extends ColumnWriter {
  readonly fixed: boolean;
  bytes: Uint8Array;
  offsets: Int32Array;
  codes: Int32Array;
  readonly bounds = new ByteBounds();
  readonly dictionary: DictionaryBuilder;
  #useDictionary = true;

  constructor(writer: GroupWriter, column: WriterColumn) {
    const fixed = column.type.kind === "uuid";
    const capacity = Math.min(
      writer.options.maxRows,
      fixed ? writer.options.rawPageBytes >>> 4 : 16_384,
    );
    super(writer, column, capacity, fixed ? 16 : 8);
    this.fixed = fixed;
    this.bytes = new Uint8Array(fixed ? capacity * 16 : writer.options.rawPageBytes);
    this.offsets = new Int32Array(fixed ? 1 : capacity + 1);
    this.codes = new Int32Array(capacity);
    this.dictionary = new DictionaryBuilder(
      fixed,
      writer.memory,
      writer.options.dictionaryEntries,
      writer.options.dictionaryBytes,
    );
  }
  override get fixedBytes(): number {
    return (
      super.fixedBytes + this.bytes.byteLength + this.offsets.byteLength + this.codes.byteLength
    );
  }
  override rawBytes(): number {
    return this.fixed ? this.rows * 16 : (this.offsets[this.rows] ?? 0);
  }
  override room(vector: Vector, from: number, count: number): number {
    const rows = Math.min(count, this.pageRows - this.rows);
    if (this.fixed) return rows;
    const data = sourceFlat(vector).data;
    if (data.layout !== "varlen") throw new TypeError("Expected varlen data");
    // Take rows while the raw page stays under its byte limit (always at least one).
    let used = this.rawBytes();
    const limit = this.writer.options.rawPageBytes;
    let take = 0;
    while (take < rows) {
      const row = sourceRow(vector, from + take);
      const length = (data.offsets[row + 1] ?? 0) - (data.offsets[row] ?? 0);
      // A page under half its limit takes one more value, however large: every page but the
      // last holds ≥ limit / 2 bytes or a full row target, which bounds the page count (and so
      // the directory blob, ≤ 1 MiB) by the chunk's bytes.
      if ((take > 0 || this.rows > 0) && used + length > limit && used >= limit / 2) break;
      // Never let a page of several values pass the blob cap (a single value may still).
      if ((take > 0 || this.rows > 0) && used + length > MAX_RAW_PAGE_BYTES) break;
      used += length;
      take++;
    }
    return Math.max(take, this.rows === 0 ? 1 : 0);
  }
  override isFull(): boolean {
    return (
      this.rows >= this.pageRows ||
      this.rawBytes() >= this.writer.options.rawPageBytes ||
      this.rows === this.capacity
    );
  }
  override async append(vector: Vector, from: number, count: number): Promise<void> {
    let done = 0;
    while (done < count) {
      let take = this.room(vector, from + done, count - done);
      if (take === 0) {
        await this.flushPage();
        take = this.room(vector, from + done, count - done);
      }
      this.setValidity(vector, from + done, take);
      this.copyValues(vector, from + done, take);
      this.rows += take;
      done += take;
      if (this.isFull()) await this.flushPage();
    }
  }
  copyValues(vector: Vector, from: number, count: number): void {
    const flat = sourceFlat(vector);
    const data = flat.data;
    const at = this.rows;
    if (this.fixed) {
      if (data.layout !== "fixed16") throw new TypeError("Expected fixed16 data");
      for (let k = 0; k < count; k++) {
        const source = sourceRow(vector, from + k) * 16;
        const target = (at + k) * 16;
        for (let b = 0; b < 16; b++) this.bytes[target + b] = data.bytes[source + b] ?? 0;
      }
      return;
    }
    if (data.layout !== "varlen") throw new TypeError("Expected varlen data");
    let cursor = this.offsets[at] ?? 0;
    if (vector.form === "flat" && vector.validity === null) {
      // No nulls: one bulk copy and a rebase of the offsets.
      const first = data.offsets[from] ?? 0;
      const last = data.offsets[from + count] ?? 0;
      if (cursor + last - first > this.bytes.length) this.#growBytes(cursor + last - first);
      this.bytes.set(data.bytes.subarray(first, last), cursor);
      const shift = cursor - first;
      for (let k = 1; k <= count; k++) this.offsets[at + k] = (data.offsets[from + k] ?? 0) + shift;
      return;
    }
    for (let k = 0; k < count; k++) {
      const row = sourceRow(vector, from + k);
      const valid =
        !this.mayHaveNulls ||
        (((this.validity[(at + k) >>> 5] ?? 0) >>> ((at + k) & 31)) & 1) === 1;
      if (valid) {
        const start = data.offsets[row] ?? 0;
        const end = data.offsets[row + 1] ?? 0;
        const length = end - start;
        if (cursor + length > this.bytes.length) this.#growBytes(cursor + length);
        if (length > 32) this.bytes.set(data.bytes.subarray(start, end), cursor);
        else for (let b = 0; b < length; b++) this.bytes[cursor + b] = data.bytes[start + b] ?? 0;
        cursor += length;
      }
      this.offsets[at + k + 1] = cursor;
    }
  }
  #growBytes(needed: number): void {
    let size = this.bytes.length * 2;
    while (size < needed) size *= 2;
    this.writer.memory.grow(size);
    const grown = new Uint8Array(size);
    grown.set(this.bytes.subarray(0, this.offsets[this.rows] ?? 0));
    this.writer.memory.free(this.bytes.length);
    this.bytes = grown;
  }
  /** Arrays and JSON are not ordered by their bytes, so their pages carry no byte bounds. */
  readonly #byteOrdered = this.column.type.kind !== "array" && this.column.type.kind !== "json";

  encode(): EncodedPage {
    const page = this.encodePage();
    return this.#byteOrdered || (page.min === null && page.max === null)
      ? page
      : { ...page, min: null, max: null };
  }

  encodePage(): EncodedPage {
    const rows = this.rows;
    const scratch = this.writer.scratch;
    // An all-null page needs no dictionary (a chunk of only such pages has none).
    if (this.#useDictionary && (!this.mayHaveNulls || countBits(this.validity, rows) > 0)) {
      const dictionary = this.dictionary;
      const oldSize = dictionary.size;
      const oldBytes = dictionary.byteLength;
      this.bounds.reset();
      let ok = true;
      for (let row = 0; row < rows; row++) {
        if (this.mayHaveNulls && (((this.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0)
          continue;
        const start = this.fixed ? row * 16 : (this.offsets[row] ?? 0);
        const end = this.fixed ? start + 16 : (this.offsets[row + 1] ?? 0);
        const code = dictionary.codeOf(this.bytes, start, end);
        if (code < 0) {
          ok = false;
          break;
        }
        this.codes[row] = code;
        this.bounds.add(this.bytes, start, end);
      }
      const maxCode = dictionary.size - 1;
      const codeWidth = maxCode < 0x100 ? 1 : maxCode < 0x1_0000 ? 2 : 4;
      const added = dictionary.size - oldSize;
      const dictionaryCost =
        rows * codeWidth + (dictionary.byteLength - oldBytes) + added * (this.fixed ? 0 : 4);
      const plainCost = this.fixed ? rows * 16 : this.rawBytes() + (rows + 1) * 2;
      if (ok && dictionaryCost <= plainCost) {
        for (let entry = oldSize; entry < dictionary.size; entry++)
          this.sketch.add(dictionary.hashes[entry] ?? 0);
        return encodeDictionaryPage(
          this.codes,
          this.pageValidity,
          rows,
          this.tag,
          this.bounds,
          scratch,
        );
      }
      dictionary.truncate(oldSize);
      this.#useDictionary = false;
    }
    if (this.fixed)
      return encodeFixed16Page(
        this.bytes,
        this.pageValidity,
        rows,
        this.sketch,
        this.bounds,
        scratch,
      );
    return encodeVarlenPage(
      this.bytes,
      this.offsets,
      this.pageValidity,
      rows,
      this.sketch,
      this.bounds,
      scratch,
    );
  }
  resetPage(): void {
    this.offsets[0] = 0;
  }
  override dictionaryPage(): Uint8Array | null {
    const dictionary = this.dictionary;
    if (dictionary.size === 0) return null;
    return encodeDictionaryEntries(
      dictionary.bytes,
      this.fixed ? null : dictionary.offsets,
      dictionary.size,
      this.tag,
      this.writer.scratch,
    );
  }
  override dictionarySize(): number | null {
    return this.dictionary.size === 0 ? null : this.dictionary.size;
  }
  override close(): void {
    this.dictionary.close();
  }
}

function columnWriter(writer: GroupWriter, column: WriterColumn): ColumnWriter {
  switch (column.type.kind) {
    case "bool":
      return new BoolWriter(writer, column);
    case "int32":
    case "date":
      return new Int32Writer(writer, column);
    case "int64":
    case "time":
    case "timestamp":
      return new Int64Writer(writer, column);
    case "float64":
    case "float32":
      return new FloatWriter(writer, column);
    case "decimal":
      return new DecimalWriter(writer, column);
    case "interval":
      return new IntervalWriter(writer, column);
    case "locator":
      throw new UnsupportedPageLayoutError("Locators are query-local and never stored");
    default:
      return new BytesWriter(writer, column);
  }
}

// ---------------------------------------------------------------------------------------------

interface ResolvedOptions {
  readonly maxRows: number;
  readonly pageBytes: number;
  readonly rawPageBytes: number;
  readonly inlineBytes: number;
  readonly slabBytes: number;
  readonly gzip: boolean;
  readonly dictionaryEntries: number;
  readonly dictionaryBytes: number;
}

export class GroupWriter {
  readonly options: ResolvedOptions;
  readonly store: ExtentStore;
  readonly allocator: ExtentAllocator;
  readonly memory: Charged;
  readonly scratch: PageScratch;
  readonly #columns: ColumnWriter[];
  readonly #extents: ExtentRef[] = [];
  #rows = 0;
  #state: "open" | "finished" | "aborted" = "open";

  constructor(options: GroupWriterOptions) {
    const maxRows = options.maxRows ?? MAX_GROUP_ROWS;
    if (!(Number.isInteger(maxRows) && maxRows > 0 && maxRows <= MAX_GROUP_ROWS))
      throw new RangeError("A group holds 1 … 65,536 rows");
    this.options = {
      maxRows,
      pageBytes: options.pageBytes ?? DEFAULTS.pageBytes,
      rawPageBytes: Math.min(MAX_RAW_PAGE_BYTES, options.rawPageBytes ?? DEFAULTS.rawPageBytes),
      inlineBytes: options.inlineBytes ?? DEFAULTS.inlineBytes,
      slabBytes: Math.max(DATA_ALIGN, options.slabBytes ?? DEFAULTS.slabBytes),
      gzip: options.gzip ?? false,
      dictionaryEntries: Math.min(options.dictionaryEntries ?? 65_536, maxRows),
      dictionaryBytes: Math.min(MAX_DICTIONARY_BYTES, options.dictionaryBytes ?? 256 * 1024),
    };
    this.store = options.store;
    this.allocator = options.allocator;
    this.memory = new Charged(options.charge ?? null);
    this.scratch = new PageScratch((grown, old) => {
      this.memory.grow(grown);
      this.memory.free(old);
    });
    this.#columns = options.columns.map((column) => columnWriter(this, column));
    const fixed = this.#columns.reduce(
      (sum, column) => sum + column.fixedBytes,
      this.scratch.byteLength,
    );
    if (!this.memory.allocate(fixed))
      throw new RangeError("A group writer's minimum must be admitted before it is built");
  }

  get rowCount(): number {
    return this.#rows;
  }

  /** Bytes the writer holds (all of it charged when a grant was given). */
  get retainedBytes(): number {
    return this.memory.bytes;
  }

  reserve(length: number): ExtentRef {
    const extent = this.allocator.reserve(length);
    this.#extents.push(extent);
    return extent;
  }

  replaceExtent(old: ExtentRef, next: ExtentRef): void {
    const index = this.#extents.indexOf(old);
    if (index >= 0) this.#extents[index] = next;
  }

  releaseExtent(extent: ExtentRef): void {
    const index = this.#extents.indexOf(extent);
    if (index >= 0) this.#extents.splice(index, 1);
    this.allocator.release(extent);
  }

  /**
   * Appends rows `[from, from + count)` of `columns` (one vector per writer column, in order).
   * Returns the rows taken: fewer than `count` when the group is full (`rowCount === maxRows`);
   * the caller then calls `finish` and continues in a new group.
   */
  async append(columns: readonly Vector[], count: number, from = 0): Promise<number> {
    if (this.#state !== "open") throw new Error("The group writer is closed");
    if (columns.length !== this.#columns.length)
      throw new RangeError("One vector per column is required");
    const take = Math.min(count, this.options.maxRows - this.#rows);
    if (take <= 0) return 0;
    for (let index = 0; index < columns.length; index++) {
      const vector = columns[index];
      const writer = this.#columns[index];
      if (vector === undefined || writer === undefined) continue;
      if (!samePhysicalType(vector.type, writer.column.type))
        throw new TypeError(
          `Column ${String(writer.column.columnId)} received a vector of another type`,
        );
      if (vector.length < from + take)
        throw new RangeError("A vector is shorter than the rows appended");
    }
    try {
      for (let index = 0; index < columns.length; index++) {
        const vector = columns[index];
        const writer = this.#columns[index];
        if (vector !== undefined && writer !== undefined) await writer.append(vector, from, take);
      }
    } catch (error) {
      await this.abort();
      throw error;
    }
    this.#rows += take;
    return take;
  }

  /** Writes the last pages, dictionaries and directories; returns the group's column chunks. */
  async finish(): Promise<WrittenGroup> {
    if (this.#state !== "open") throw new Error("The group writer is closed");
    if (this.#rows === 0) throw new RangeError("An empty group has no chunks");
    try {
      const chunks: Array<ColumnChunk | null> = [];
      const inline: ColumnWriter[] = [];
      for (const column of this.#columns) {
        await column.flushPage();
        const dictionary = column.dictionaryPage();
        let dictionaryEntry: PageEntry | null = null;
        if (dictionary !== null) {
          const offset = column.spool.append(dictionary);
          dictionaryEntry = {
            offset,
            length: dictionary.length,
            firstRow: 0,
            rowCount: column.dictionarySize() ?? 0,
            encoding: PageEncoding.plain,
            nullCount: 0,
            min: null,
            max: null,
            checksum: xxHash32(dictionary),
          };
        }
        if (column.spool.extent === null && column.spool.position <= this.options.inlineBytes) {
          inline.push(column);
          chunks.push(null);
          column.dictionaryEntry = dictionaryEntry;
          continue;
        }
        const directory = this.#directory(column, dictionaryEntry, 0);
        const directoryOffset = column.spool.append(directory);
        const extent = await column.spool.finishStreaming();
        chunks.push(this.#chunk(column, extent, extent.offset + directoryOffset, directory));
      }
      if (inline.length > 0) await this.#pack(inline, chunks);
      this.#state = "finished";
      const columns = chunks.map((chunk) => {
        if (chunk === null) throw new Error("A column chunk was not written");
        return chunk;
      });
      return { rowCount: this.#rows, columns, extents: [...this.#extents] };
    } catch (error) {
      await this.abort();
      throw error;
    } finally {
      this.#close();
    }
  }

  async #pack(inline: ColumnWriter[], chunks: Array<ColumnChunk | null>): Promise<void> {
    const placed: Array<{
      column: ColumnWriter;
      at: number;
      directory: Uint8Array;
      directoryAt: number;
    }> = [];
    let length = 0;
    for (const column of inline) {
      const at = align8(length);
      const dictionaryEntry = column.dictionaryEntry;
      const rebased =
        dictionaryEntry === null
          ? null
          : { ...dictionaryEntry, offset: dictionaryEntry.offset + at };
      const directory = this.#directory(column, rebased, at);
      const directoryAt = align8(at + column.spool.position);
      placed.push({ column, at, directory, directoryAt });
      length = directoryAt + directory.length;
    }
    const bytes = new Uint8Array(length);
    for (const { column, at, directory, directoryAt } of placed) {
      bytes.set(column.spool.staged.subarray(0, column.spool.stagedLength), at);
      bytes.set(directory, directoryAt);
    }
    const extent = this.reserve(length);
    await this.store.writeData(extent.offset, bytes);
    const kept = this.allocator.trim(extent, length);
    this.replaceExtent(extent, kept);
    for (const { column, directory, directoryAt } of placed) {
      const index = this.#columns.indexOf(column);
      chunks[index] = this.#chunk(column, kept, kept.offset + directoryAt, directory);
    }
  }

  #directory(column: ColumnWriter, dictionary: PageEntry | null, shift: number): Uint8Array {
    const sketch = column.sketch.serialize();
    return encodeDirectory({
      tag: column.tag,
      rowCount: this.#rows,
      scale: column.scale(),
      hasSpecials: column.hasSpecials(),
      pages:
        shift === 0
          ? column.entries
          : column.entries.map((entry) => ({ ...entry, offset: entry.offset + shift })),
      dictionary,
      sketch,
    });
  }

  #chunk(
    column: ColumnWriter,
    extent: ExtentRef,
    directoryOffset: number,
    directory: Uint8Array,
  ): ColumnChunk {
    const sketch = column.sketch.serialize();
    const sketchOffset = directoryOffset + directory.length - sketch.length;
    const stats = column.stats;
    const statistics: ColumnStatistics = {
      nullCount: stats.nulls,
      min: stats.unknownMin ? null : stats.min,
      max: stats.unknownMax ? null : stats.max,
      dictionarySize: column.dictionarySize(),
    };
    return {
      columnId: column.column.columnId,
      type: column.column.type,
      data: extent,
      directory: {
        offset: directoryOffset,
        length: directory.length,
        checksum: xxHash32(directory),
      },
      statistics,
      distinctSketch: { offset: sketchOffset, length: sketch.length, checksum: xxHash32(sketch) },
    };
  }

  /** Releases every reserved extent and buffer; the group is not written. Idempotent. */
  async abort(): Promise<void> {
    if (this.#state !== "open") return;
    this.#state = "aborted";
    for (const extent of this.#extents.splice(0)) this.allocator.release(extent);
    this.#close();
  }

  #close(): void {
    for (const column of this.#columns) column.close();
    this.memory.free(this.memory.bytes);
  }
}
