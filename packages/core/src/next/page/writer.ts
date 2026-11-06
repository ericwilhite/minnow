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
 * - `gzip` is for maintenance compaction only (ruling R15.5), never a memtable flush: varlen and
 *   uuid pages ≥ 16 KiB are offered to gzip and kept at ≥ 2× (gzip.ts); after two misses in a row
 *   a column stops trying for the rest of the group. Readers keep gzip pages inflated in the
 *   inflated-page cache (inflated-cache.ts), so a page inflates once per cache residency.
 * - A chunk is `[pages][dictionary page][directory]`, back to back in one extent. Its directory
 *   `BlobRef` and its distinct sketch `BlobRef` (a range of the directory blob) live in it too.
 *
 * ## Placement (storage/extents.ts; rulings R15.1, R15.2)
 * - The caller hands in one `ExtentWriter` (it publishes or aborts it). A chunk stays in memory
 *   until it passes `inlineBytes` (default 8 KiB). Chunks still that small at `finish` are packed
 *   into one shared extent (their `ColumnChunk.data` is that extent; page offsets are relative to
 *   it), so a narrow column does not cost a 4 KiB extent.
 * - A larger chunk opens its own `ExtentBuilder`, sized from its bytes per row so far times the
 *   group's rows (a page-statistics estimate), and appends each page as it is encoded; the space
 *   stages the bytes. When a page would not fit, the writer calls `extend` (in place when the
 *   following space is free, else the space relocates once at `seal`). `seal` trims the tail.
 * - Every page, dictionary page and directory is one blob (≤ `MAX_BLOB_BYTES`); the chunk's
 *   `BlobRef`s carry their extent.
 *
 * ## Memory (bounded per column; everything charged before it is allocated)
 * Per column: the raw page buffer (≤ `rawPageBytes`, default 32 KiB, allocated on first use),
 * its validity, the distinct sketch (1 KiB), the page entries, and while the chunk is small its
 * inline pages (≤ `inlineBytes`). Shared by all columns: the encoder scratch (one page) and the
 * code/limb scratch. The group dictionary of a text or uuid column is charged separately and
 * bounded (`dictionaryEntries`, `dictionaryBytes`); `workingBytes` and `peakWorkingBytes`
 * report the rest. With a `charge` (a grant), a refused dictionary growth falls back to plain
 * pages; any other refusal is a `RangeError` (the one size error: a value larger than the grant).
 *
 * ## Values
 * - Input vectors may be flat, constant or dictionary. Their type must equal the column's.
 * - Decimals (DECISIONS.md C2): a declared NUMERIC(p, s) is a fixed-scale chunk (a value at
 *   another scale is a `RangeError`); an unconstrained NUMERIC is a mixed-scale chunk that keeps
 *   every value's own scale. int64 and int128 coefficients are stored; coefficients past 128 bits
 *   (`bigint[]`) raise `UnsupportedPageLayoutError` (no stored form yet).
 */
import type { ChunkData, ColumnChunk, PageEntry, StatValue } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import { xxHash32 } from "../hash/xxhash32.js";
import {
  MAX_BLOB_BYTES,
  type ExtentBuilder,
  type ExtentRef,
  type ExtentWriter,
  type PendingBlob,
} from "../storage/extents.js";
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
  hashInt128,
  type EncodedPage,
} from "./encode.js";
import { chunkTagOf, STAT_PREFIX_BYTES, UnsupportedPageLayoutError, ValueTag } from "./format.js";
import { gzipPage } from "./gzip.js";
import { DistinctSketch, hashBytes, hashInt64, mix32 } from "./sketch.js";
import {
  encodePointer,
  LARGE_VALUE_BYTES,
  MAX_VALUE_BYTES,
  VALUE_PART_BYTES,
} from "./large-value.js";
import { int128At, setInt128 } from "../vector/int128.js";
import { encodeSortKey } from "../keys/key-encoding.js";

export interface WriterColumn {
  readonly columnId: number;
  readonly type: PhysicalType;
}

export interface GroupWriterOptions {
  readonly columns: readonly WriterColumn[];
  /** The extent writer the group's chunks are written through (the caller publishes it). */
  readonly writer: ExtentWriter;
  /** Rows per group (default 65,536, the most a locator position addresses). */
  readonly maxRows?: number;
  readonly pageBytes?: number;
  readonly rawPageBytes?: number;
  readonly inlineBytes?: number;
  /**
   * Maintenance compaction only (ruling R15.5): offer large varlen and uuid pages to gzip. A
   * memtable flush never sets it. Default false.
   */
  readonly gzip?: boolean;
  readonly charge?: ByteCharge;
  /** Dictionary bounds per chunk (default 65,536 entries, 256 KiB). */
  readonly dictionaryEntries?: number;
  readonly dictionaryBytes?: number;
  /**
   * The most data bytes one column chunk takes (default `MAX_CHUNK_BYTES`, 1 GiB): `append`
   * takes fewer rows (the group is full) before a varlen chunk would pass it, so directory
   * offsets (u32) always hold. A group takes at least one row however large.
   */
  readonly maxChunkBytes?: number;
}

export interface WrittenGroup {
  readonly rowCount: number;
  readonly columns: readonly ColumnChunk[];
  /** Every extent the group's chunks use (each once), sealed through the caller's writer. */
  readonly extents: readonly ExtentRef[];
}

export const MAX_GROUP_ROWS = 65_536;
/** Default `GroupWriterOptions.maxChunkBytes`: far below the 4 GiB directory offsets address. */
export const MAX_CHUNK_BYTES = 1024 * 1024 * 1024;
/**
 * Raw page bytes never pass this (offsets, validity and header fit the rest of a 1 MiB blob),
 * and a dictionary's entries never pass `MAX_DICTIONARY_BYTES` (its offsets fit the rest).
 */
const MAX_RAW_PAGE_BYTES = MAX_BLOB_BYTES - 192 * 1024;
const MAX_DICTIONARY_BYTES = 512 * 1024;
const DEFAULTS = {
  pageBytes: 32 * 1024,
  rawPageBytes: 32 * 1024,
  inlineBytes: 8 * 1024,
};

/**
 * The writer's memory: every buffer is charged here before it is allocated. Dictionaries charge
 * through `dictionaries` (the same grant, counted apart so the bounded per-column working set can
 * be reported without them).
 */
class Charged {
  #charged = 0;
  #dictionaryBytes = 0;
  #peakWorking = 0;
  readonly dictionaries: ByteCharge;
  constructor(readonly charge: ByteCharge | null) {
    this.dictionaries = {
      allocate: (bytes) => {
        if (!this.allocate(bytes)) return false;
        this.#dictionaryBytes += bytes;
        return true;
      },
      free: (bytes) => {
        this.#dictionaryBytes -= bytes;
        this.free(bytes);
      },
    };
  }
  get bytes(): number {
    return this.#charged;
  }
  /** Charged bytes other than dictionaries. */
  get working(): number {
    return this.#charged - this.#dictionaryBytes;
  }
  get peakWorking(): number {
    return this.#peakWorking;
  }
  allocate(bytes: number): boolean {
    if (bytes <= 0) return true;
    if (this.charge !== null && !this.charge.allocate(bytes)) return false;
    this.#charged += bytes;
    this.#peakWorking = Math.max(this.#peakWorking, this.#charged - this.#dictionaryBytes);
    return true;
  }
  grow(bytes: number): void {
    if (!this.allocate(bytes))
      throw new RangeError("The writer's memory grant refused a buffer it needs");
  }
  free(bytes: number): void {
    if (bytes <= 0) return;
    this.#charged -= bytes;
    this.charge?.free(bytes);
  }
  // Typed buffers, charged first.
  int32(count: number): Int32Array {
    this.grow(count * 4);
    return new Int32Array(count);
  }
  uint32(count: number): Uint32Array {
    this.grow(count * 4);
    return new Uint32Array(count);
  }
  uint16(count: number): Uint16Array {
    this.grow(count * 2);
    return new Uint16Array(count);
  }
  uint8(count: number): Uint8Array {
    this.grow(count);
    return new Uint8Array(count);
  }
  float64(count: number): Float64Array {
    this.grow(count * 8);
    return new Float64Array(count);
  }
  float32(count: number): Float32Array {
    this.grow(count * 4);
    return new Float32Array(count);
  }
}

/** Zero bytes that move the next part to an 8-byte offset. */
const PADDING = new Uint8Array(7);

/**
 * Appends `parts` to `builder`, extending it first when they do not fit, and returns their
 * blobs. With `aligned`, each part starts at an 8-byte offset of the extent (extents start
 * `DATA_ALIGN`-aligned): up to 7 zero bytes go before it, as their own unreferenced blob. A page
 * read with its neighbours into one buffer is then viewed in place by typed arrays, never
 * copied to align it (page/format.ts "Stored form"). Directories allow the gaps.
 */
async function appendTo(
  builder: ExtentBuilder,
  parts: readonly Uint8Array[],
  aligned: boolean,
): Promise<PendingBlob[]> {
  const written: Uint8Array[] = [];
  const real: number[] = [];
  let at = builder.length;
  for (const part of parts) {
    const misaligned = at & 7;
    if (aligned && misaligned !== 0) {
      written.push(PADDING.subarray(0, 8 - misaligned));
      at += 8 - misaligned;
    }
    real.push(written.length);
    written.push(part);
    at += part.length;
  }
  const short = at - builder.capacity;
  if (short > 0) builder.extend(Math.max(short, Math.ceil(builder.capacity / 2)));
  const blobs = await builder.append(written);
  return real.map((index) => {
    const blob = blobs[index];
    if (blob === undefined) throw new Error("the builder returned fewer blobs than parts");
    return blob;
  });
}

// ---------------------------------------------------------------------------------------------
// Sink: a chunk's pages, inline (in memory, small) or streaming into its own builder

/** A page waiting for its place: its directory entry is completed when it is appended. */
interface PendingPage {
  readonly bytes: Uint8Array;
  readonly entry: Omit<PageEntry, "offset" | "checksum">;
}

class ChunkSink {
  /** Small-chunk pages kept until the chunk outgrows `inlineBytes` or the group packs them. */
  inline: PendingPage[] = [];
  inlineBytes = 0;
  builder: ExtentBuilder | null = null;
  /** Directory entries of the pages appended so far. */
  readonly entries: PageEntry[] = [];

  constructor(
    readonly writer: GroupWriter,
    readonly column: ColumnWriter,
  ) {}

  /** Adds an encoded page (`bytes` may be the shared scratch: it is copied or written now). */
  async add(bytes: Uint8Array, entry: PendingPage["entry"]): Promise<void> {
    if (
      this.builder === null &&
      this.inlineBytes + bytes.length <= this.writer.options.inlineBytes
    ) {
      const copy = this.writer.memory.uint8(bytes.length);
      copy.set(bytes);
      this.inline.push({ bytes: copy, entry });
      this.inlineBytes += bytes.length;
      return;
    }
    if (this.builder === null) await this.#open(bytes.length);
    await this.#append([bytes], [entry]);
  }

  /** Moves a still-inline chunk into its own builder (out-of-line values need one). */
  async ensureBuilder(): Promise<void> {
    if (this.builder === null) await this.#open(0);
  }

  async #open(next: number): Promise<void> {
    const column = this.column;
    const rows = Math.max(1, column.rowsWritten + column.rows);
    const perRow = (this.inlineBytes + next) / rows;
    const estimate = Math.ceil(perRow * this.writer.options.maxRows * 1.1) + 8192;
    this.builder = this.writer.extents.open(Math.max(estimate, this.inlineBytes + next + 8192));
    const pages = this.inline.splice(0);
    this.inlineBytes = 0;
    if (pages.length === 0) return;
    await this.#append(
      pages.map((page) => page.bytes),
      pages.map((page) => page.entry),
    );
    for (const page of pages) this.writer.memory.free(page.bytes.length);
  }

  /**
   * Appends parts to the builder, extending it first when they do not fit. `aligned`: each part
   * starts at an 8-byte offset of the extent (pages, dictionaries, directories); large-value
   * parts, which tile their region exactly, are not.
   */
  async appendParts(parts: readonly Uint8Array[], aligned = false): Promise<PendingBlob[]> {
    const builder = this.builder;
    if (builder === null) throw new Error("append to a chunk without a builder");
    return appendTo(builder, parts, aligned);
  }

  async #append(
    parts: readonly Uint8Array[],
    entries: ReadonlyArray<PendingPage["entry"]>,
  ): Promise<void> {
    const blobs = await this.appendParts(parts, true);
    blobs.forEach((blob, index) => {
      const entry = entries[index];
      if (entry !== undefined)
        this.entries.push({ ...entry, offset: blob.offset, checksum: blob.checksum });
    });
  }

  /** Frees inline copies (abort). */
  close(): void {
    for (const page of this.inline.splice(0)) this.writer.memory.free(page.bytes.length);
    this.inlineBytes = 0;
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
  readonly sketch: DistinctSketch;
  readonly sink: ChunkSink;
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
    writer.memory.grow(1024); // the sketch's registers, charged before they exist
    this.sketch = new DistinctSketch();
    this.validity = writer.memory.uint32((capacity + 31) >>> 5);
    this.pageRows = Math.max(
      1,
      Math.min(capacity, Math.floor(writer.options.pageBytes / Math.max(1, rawWidth))),
    );
    this.sink = new ChunkSink(writer, this);
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
  /** Rows of `[from, from + count)` this chunk takes before passing `maxChunkBytes`. */
  rowsFitting(_vector: Vector, _from: number, count: number): number {
    return count;
  }

  /** The current page's large-value region (BytesWriter), recorded in its directory entry. */
  pageRegion(): { offset: number; length: number } | null {
    return null;
  }

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
    this.#mergeStats(page);
    const largeRegion = this.pageRegion();
    await this.sink.add(stored, {
      length: stored.length,
      firstRow: this.rowsWritten,
      rowCount: page.rowCount,
      encoding,
      nullCount: page.nullCount,
      min: page.min,
      max: page.max,
      ...(largeRegion === null ? {} : { largeRegion }),
    });
    this.bytesWritten += stored.length;
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
    this.sink.close();
  }
}

/** Whether slot `index` of a vector is valid. */
function validAt(vector: Vector, index: number): boolean {
  if (vector.form === "constant")
    return vector.value.validity === null || ((vector.value.validity[0] ?? 0) & 1) === 1;
  const validity = vector.validity;
  return validity === null || (((validity[index >>> 5] ?? 0) >>> (index & 31)) & 1) === 1;
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
    this.values = writer.memory.uint8(capacity);
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
    this.values = writer.memory.int32(capacity);
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
    this.words = writer.memory.int32(capacity * 2);
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
    this.values = wide ? writer.memory.float64(capacity) : writer.memory.float32(capacity);
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
    this.months = writer.memory.int32(capacity);
    this.days = writer.memory.int32(capacity);
    this.micros = writer.memory.int32(capacity * 2);
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
  readonly #fixedScale: number | null;
  #sawSpecials = false;
  constructor(writer: GroupWriter, column: WriterColumn) {
    const capacity = Math.min(writer.options.maxRows, writer.options.rawPageBytes >>> 4);
    super(writer, column, capacity, 16);
    this.limbs = writer.memory.uint32(capacity * 4);
    this.specials = writer.memory.uint8(capacity);
    const type = column.type;
    this.#fixedScale =
      type.kind === "decimal" && type.scale !== null ? Math.max(0, type.scale) : null;
    this.scales = this.#fixedScale === null ? writer.memory.uint16(capacity) : null;
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
    const page = encodeInt128Page(
      this.limbs,
      this.pageValidity,
      this.rows,
      { specials: this.specials, scales },
      scales === null ? this.sketch : null,
      this.writer.scratch,
      this.writer.pairs(this.rows),
    );
    if (scales === null || page.nullCount === page.rowCount) return page;
    const [min, max] = this.#mixedBounds(scales);
    return { ...page, min, max };
  }

  /**
   * Normalized bounds of a mixed-scale page (ruling R15.10): the sort keys (keys/key-encoding.ts:
   * value order, scale ignored, specials in PostgreSQL's order) of its least and greatest values,
   * chosen by comparing those exact key bytes, never an approximation (B2-2 #1).
   */
  #mixedBounds(scales: Uint16Array): [Uint8Array, Uint8Array] {
    const validity = this.pageValidity;
    let min: Uint8Array | null = null;
    let max: Uint8Array | null = null;
    for (let row = 0; row < this.rows; row++) {
      if (validity !== null && (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0) continue;
      const key = this.#sortKey(row, scales);
      if (min === null || compareBytes(key, 0, key.length, min, 0, min.length) < 0) min = key;
      if (max === null || compareBytes(key, 0, key.length, max, 0, max.length) > 0) max = key;
    }
    if (min === null || max === null) throw new Error("bounds of a page without valid rows");
    return [min, max];
  }

  #sortKey(row: number, scales: Uint16Array): Uint8Array {
    const special = this.specials[row] ?? 0;
    const value =
      special === 0
        ? { coefficient: int128At(this.limbs, row), scale: scales[row] ?? 0 }
        : {
            coefficient: 0n,
            scale: 0,
            special:
              special === 1
                ? ("NaN" as const)
                : special === 2
                  ? ("Infinity" as const)
                  : ("-Infinity" as const),
          };
    return encodeSortKey([{ type: this.column.type, value, direction: "asc", nulls: "first" }]);
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
      // Every significant limb, the way `hashInt128` hashes it (equal values hash equally in
      // either tier): a value that fits int64 hashes its two words, otherwise all four.
      const limbs = this.writer.limbScratch;
      setInt128(limbs, 0, coefficient);
      this.sketch.add(mix32(hashInt128(limbs, 0) ^ Math.imul(scale + 1, 0x9e37_79b1)));
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
  readonly bounds = new ByteBounds();
  readonly dictionary: DictionaryBuilder;
  #useDictionary = true;
  /** Rows of the current page stored out of line (large-value.ts), and their value prefixes. */
  readonly #large: Uint32Array;
  readonly #prefixes = new Map<number, Uint8Array>();
  /** The current page's large-value region: its parts are appended back to back. */
  #regionStart = -1;
  #regionEnd = 0;
  /** Varlen bytes taken into this chunk (values, plus 4 per row for offsets). */
  #takenBytes = 0;

  constructor(writer: GroupWriter, column: WriterColumn) {
    const fixed = column.type.kind === "uuid";
    const capacity = Math.min(
      writer.options.maxRows,
      fixed ? writer.options.rawPageBytes >>> 4 : 4096,
    );
    super(writer, column, capacity, fixed ? 16 : 8);
    this.fixed = fixed;
    this.bytes = writer.memory.uint8(fixed ? capacity * 16 : writer.options.rawPageBytes);
    this.offsets = writer.memory.int32(fixed ? 1 : capacity + 1);
    this.#large = writer.memory.uint32(fixed ? 0 : (capacity + 31) >>> 5);
    this.dictionary = new DictionaryBuilder(
      fixed,
      writer.memory.dictionaries,
      writer.options.dictionaryEntries,
      writer.options.dictionaryBytes,
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
      // An out-of-line value is taken on its own (`append`).
      if (length > LARGE_VALUE_BYTES && validAt(vector, from + take)) break;
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
    if (!this.fixed) {
      const data = sourceFlat(vector).data;
      if (data.layout === "varlen")
        for (let k = 0; k < count; k++) {
          const row = sourceRow(vector, from + k);
          this.#takenBytes +=
            4 +
            (validAt(vector, from + k)
              ? (data.offsets[row + 1] ?? 0) - (data.offsets[row] ?? 0)
              : 0);
        }
    }
    let done = 0;
    while (done < count) {
      if (!this.fixed && this.#isLargeAt(vector, from + done)) {
        if (this.rows >= this.pageRows) await this.flushPage();
        await this.#appendLarge(vector, from + done);
        done++;
        if (this.isFull()) await this.flushPage();
        continue;
      }
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
  #isLargeAt(vector: Vector, index: number): boolean {
    const data = sourceFlat(vector).data;
    if (data.layout !== "varlen" || !validAt(vector, index)) return false;
    const row = sourceRow(vector, index);
    return (data.offsets[row + 1] ?? 0) - (data.offsets[row] ?? 0) > LARGE_VALUE_BYTES;
  }

  /**
   * Writes one value out of line (large-value.ts): its parts go straight from the caller's vector
   * into the chunk's builder (no copy is held), and the page gets the pointer.
   */
  async #appendLarge(vector: Vector, index: number): Promise<void> {
    const data = sourceFlat(vector).data;
    if (data.layout !== "varlen") throw new TypeError("Expected varlen data");
    const row = sourceRow(vector, index);
    const start = data.offsets[row] ?? 0;
    const end = data.offsets[row + 1] ?? 0;
    const length = end - start;
    if (length > MAX_VALUE_BYTES)
      throw new UnsupportedPageLayoutError(
        `A value of ${String(length)} bytes passes the ${String(MAX_VALUE_BYTES)}-byte limit`,
      );
    await this.sink.ensureBuilder();
    const parts: Uint8Array[] = [];
    for (let at = start; at < end; at += VALUE_PART_BYTES)
      parts.push(data.bytes.subarray(at, Math.min(end, at + VALUE_PART_BYTES)));
    const blobs = await this.sink.appendParts(parts);
    const first = blobs[0];
    if (first === undefined) throw new Error("A large value wrote no parts");
    // The builder places parts back to back: part i is at offset + i × VALUE_PART_BYTES, and the
    // page's values follow each other (nothing else is appended to the chunk in between).
    if (this.#regionStart < 0) this.#regionStart = first.offset;
    else if (first.offset !== this.#regionEnd)
      throw new Error("A page's out-of-line values are not contiguous");
    this.#regionEnd = first.offset + length;
    const pointer = encodePointer({
      offset: first.offset,
      length,
      checksums: blobs.map((blob) => blob.checksum),
    });
    this.sketch.add(hashBytes(data.bytes, start, end));
    const slot = this.rows;
    this.setValidity(vector, index, 1);
    const cursor = this.offsets[slot] ?? 0;
    if (cursor + pointer.length > this.bytes.length) this.#growBytes(cursor + pointer.length);
    this.bytes.set(pointer, cursor);
    this.offsets[slot + 1] = cursor + pointer.length;
    this.#large[slot >>> 5] = (this.#large[slot >>> 5] ?? 0) | (1 << (slot & 31));
    this.#prefixes.set(slot, data.bytes.slice(start, start + STAT_PREFIX_BYTES + 1));
    this.rows++;
    this.bytesWritten += length;
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
    const codes = this.writer.codes(rows);
    const scratch = this.writer.scratch;
    // A page with out-of-line values is plain; the dictionary carries on for the next pages.
    const large = this.#prefixes.size > 0;
    // An all-null page needs no dictionary (a chunk of only such pages has none).
    if (
      !large &&
      this.#useDictionary &&
      (!this.mayHaveNulls || countBits(this.validity, rows) > 0)
    ) {
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
        codes[row] = code;
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
        return encodeDictionaryPage(codes, this.pageValidity, rows, this.tag, this.bounds, scratch);
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
    const prefixes = this.#prefixes;
    return encodeVarlenPage(
      this.bytes,
      this.offsets,
      this.pageValidity,
      rows,
      this.sketch,
      this.bounds,
      scratch,
      large
        ? { bitmap: this.#large, prefix: (row) => prefixes.get(row) ?? new Uint8Array(0) }
        : null,
    );
  }
  override pageRegion(): { offset: number; length: number } | null {
    return this.#regionStart < 0
      ? null
      : { offset: this.#regionStart, length: this.#regionEnd - this.#regionStart };
  }

  override rowsFitting(vector: Vector, from: number, count: number): number {
    if (this.fixed) return count;
    const data = sourceFlat(vector).data;
    if (data.layout !== "varlen") return count;
    let room = this.writer.options.maxChunkBytes - this.#takenBytes;
    for (let k = 0; k < count; k++) {
      const row = sourceRow(vector, from + k);
      const cost =
        4 +
        (validAt(vector, from + k) ? (data.offsets[row + 1] ?? 0) - (data.offsets[row] ?? 0) : 0);
      if (cost > room) return k;
      room -= cost;
    }
    return count;
  }

  resetPage(): void {
    this.offsets[0] = 0;
    this.#regionStart = -1;
    if (this.#prefixes.size > 0) {
      this.#large.fill(0);
      this.#prefixes.clear();
    }
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
  readonly maxChunkBytes: number;
  readonly pageBytes: number;
  readonly rawPageBytes: number;
  readonly inlineBytes: number;
  readonly gzip: boolean;
  readonly dictionaryEntries: number;
  readonly dictionaryBytes: number;
}

export class GroupWriter {
  readonly options: ResolvedOptions;
  /** The caller's extent writer. */
  readonly extents: ExtentWriter;
  readonly memory: Charged;
  readonly scratch: PageScratch;
  readonly #columns: ColumnWriter[];
  /** Extents this group sealed (handed back by `finish`, discarded by `abort`). */
  readonly #sealed: ExtentRef[] = [];
  /** One 128-bit slot for hashing a normalized coefficient. */
  readonly limbScratch = new Uint32Array(4);
  #codes = new Int32Array(0);
  #pairs = new Int32Array(0);
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
      gzip: options.gzip ?? false,
      dictionaryEntries: Math.min(options.dictionaryEntries ?? 65_536, maxRows),
      dictionaryBytes: Math.min(MAX_DICTIONARY_BYTES, options.dictionaryBytes ?? 256 * 1024),
      maxChunkBytes: Math.min(MAX_CHUNK_BYTES, options.maxChunkBytes ?? MAX_CHUNK_BYTES),
    };
    this.extents = options.writer;
    this.memory = new Charged(options.charge ?? null);
    this.scratch = new PageScratch((grown, old) => {
      this.memory.grow(grown);
      this.memory.free(old);
    });
    try {
      this.#columns = options.columns.map((column) => columnWriter(this, column));
    } catch (error) {
      this.memory.free(this.memory.bytes);
      throw error;
    }
  }

  get rowCount(): number {
    return this.#rows;
  }

  /** Bytes the writer holds now (all of it charged when a grant was given). */
  get retainedBytes(): number {
    return this.memory.bytes;
  }

  /** Bytes held now apart from dictionaries; and the most it held at once. */
  get workingBytes(): number {
    return this.memory.working;
  }
  get peakWorkingBytes(): number {
    return this.memory.peakWorking;
  }

  /** Shared Int32 scratch for a page's dictionary codes (charged as it grows). */
  codes(rows: number): Int32Array {
    if (this.#codes.length < rows) {
      this.memory.grow(rows * 4);
      this.memory.free(this.#codes.byteLength);
      this.#codes = new Int32Array(rows);
    }
    return this.#codes;
  }

  /** Shared word-pair scratch for 128-bit decimal pages that fit int64. */
  pairs(rows: number): Int32Array {
    if (this.#pairs.length < rows * 2) {
      this.memory.grow(rows * 8);
      this.memory.free(this.#pairs.byteLength);
      this.#pairs = new Int32Array(rows * 2);
    }
    return this.#pairs;
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
    let take = Math.min(count, this.options.maxRows - this.#rows);
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
    // A varlen chunk stops short of `maxChunkBytes`; an empty group takes one row regardless.
    for (let index = 0; index < columns.length; index++) {
      const vector = columns[index];
      const writer = this.#columns[index];
      if (vector !== undefined && writer !== undefined)
        take = Math.min(take, writer.rowsFitting(vector, from, take));
    }
    if (this.#rows === 0) take = Math.max(take, 1);
    if (take === 0) return 0;
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
        if (column.sink.builder === null) {
          inline.push(column);
          chunks.push(null);
          continue;
        }
        chunks.push(await this.#finishStreamed(column));
      }
      if (inline.length > 0) await this.#pack(inline, chunks);
      this.#state = "finished";
      const columns = chunks.map((chunk) => {
        if (chunk === null) throw new Error("A column chunk was not written");
        return chunk;
      });
      return { rowCount: this.#rows, columns, extents: [...this.#sealed] };
    } catch (error) {
      await this.abort();
      throw error;
    } finally {
      this.#close();
    }
  }

  /** The dictionary page's entry once appended at `blob`. */
  #dictionaryEntry(column: ColumnWriter, blob: PendingBlob | undefined): PageEntry | null {
    if (blob === undefined) return null;
    return {
      offset: blob.offset,
      length: blob.length,
      firstRow: 0,
      rowCount: column.dictionarySize() ?? 0,
      encoding: PageEncoding.plain,
      nullCount: 0,
      min: null,
      max: null,
      checksum: blob.checksum,
    };
  }

  /** Appends the dictionary, directory and sketch of one column through `append`. */
  /**
   * Appends the dictionary, directory and sketch of one column through `append`. `base` is the
   * extent offset where the chunk's data starts (0 for its own extent, its place in a pack): page
   * and dictionary offsets in the directory are relative to it (manifest.ts "Chunks").
   */
  async #tail(
    column: ColumnWriter,
    append: (parts: readonly Uint8Array[]) => Promise<PendingBlob[]>,
    base: number,
  ): Promise<{ directory: PendingBlob; sketch: Uint8Array }> {
    const dictionary = column.dictionaryPage();
    let dictionaryEntry: PageEntry | null = null;
    if (dictionary !== null) {
      // The dictionary page is in the shared scratch: append copies it before the next encode.
      const [blob] = await append([dictionary]);
      const entry = this.#dictionaryEntry(column, blob);
      dictionaryEntry = entry === null ? null : { ...entry, offset: entry.offset - base };
    }
    const sketch = column.sketch.serialize();
    const directory = encodeDirectory({
      tag: column.tag,
      rowCount: this.#rows,
      scale: column.scale(),
      hasSpecials: column.hasSpecials(),
      pages:
        base === 0
          ? column.sink.entries
          : column.sink.entries.map((entry) => ({ ...entry, offset: entry.offset - base })),
      dictionary: dictionaryEntry,
      sketch,
    });
    const [blob] = await append([directory]);
    if (blob === undefined) throw new Error("The directory was not appended");
    return { directory: blob, sketch };
  }

  async #finishStreamed(column: ColumnWriter): Promise<ColumnChunk> {
    const sink = column.sink;
    const { directory, sketch } = await this.#tail(
      column,
      (parts) => sink.appendParts(parts, true),
      0,
    );
    const builder = sink.builder;
    if (builder === null) throw new Error("A streamed chunk has no builder");
    const written = await builder.seal();
    sink.builder = null;
    if (written === null) throw new Error("A streamed chunk sealed empty");
    this.#sealed.push(written.extent);
    return this.#chunk(
      column,
      { extent: written.extent, offset: 0, length: written.extent.length },
      directory,
      sketch,
    );
  }

  /** Packs every small chunk into one shared extent, column after column. */
  async #pack(inline: ColumnWriter[], chunks: Array<ColumnChunk | null>): Promise<void> {
    const estimate = inline.reduce((sum, column) => sum + column.sink.inlineBytes + 2048, 0);
    const builder = this.extents.open(estimate);
    const append = (parts: readonly Uint8Array[]): Promise<PendingBlob[]> =>
      appendTo(builder, parts, true);
    const tails: Array<{
      column: ColumnWriter;
      base: number;
      directory: PendingBlob;
      sketch: Uint8Array;
    }> = [];
    try {
      for (const column of inline) {
        const sink = column.sink;
        // The chunk's data starts here in the shared extent; its offsets are relative to it.
        const base = builder.length;
        const pages = sink.inline.splice(0);
        if (pages.length > 0) {
          const blobs = await append(pages.map((page) => page.bytes));
          blobs.forEach((blob, index) => {
            const page = pages[index];
            if (page !== undefined)
              sink.entries.push({ ...page.entry, offset: blob.offset, checksum: blob.checksum });
          });
          for (const page of pages) this.memory.free(page.bytes.length);
          sink.inlineBytes = 0;
        }
        tails.push({ column, base, ...(await this.#tail(column, append, base)) });
      }
    } catch (error) {
      const written = await builder.seal();
      if (written !== null) await this.extents.discard(written.extent);
      throw error;
    }
    const written = await builder.seal();
    if (written === null) throw new Error("The pack sealed empty");
    this.#sealed.push(written.extent);
    for (const { column, base, directory, sketch } of tails)
      chunks[this.#columns.indexOf(column)] = this.#chunk(
        column,
        // Pages, dictionary and directory: [base, end of the directory) of the pack.
        {
          extent: written.extent,
          offset: base,
          length: directory.offset + directory.length - base,
        },
        directory,
        sketch,
      );
  }

  #chunk(
    column: ColumnWriter,
    data: ChunkData,
    directory: PendingBlob,
    sketch: Uint8Array,
  ): ColumnChunk {
    const extent = data.extent;
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
      data,
      directory: { extent, ...directory },
      statistics,
      // The sketch is the directory blob's tail (directory.ts), under its own checksum.
      distinctSketch: {
        extent,
        offset: directory.offset + directory.length - sketch.length,
        length: sketch.length,
        checksum: xxHash32(sketch),
      },
    };
  }

  /**
   * Abandons the group: seals and discards every extent it wrote (so the caller's writer can go
   * on with other groups) and returns every buffer. Idempotent; a no-op after `finish`.
   */
  async abort(): Promise<void> {
    if (this.#state !== "open") return;
    this.#state = "aborted";
    try {
      for (const column of this.#columns) {
        const builder = column.sink.builder;
        column.sink.builder = null;
        const written = builder === null ? null : await builder.seal();
        if (written !== null) await this.extents.discard(written.extent);
      }
      for (const extent of this.#sealed.splice(0)) await this.extents.discard(extent);
    } finally {
      this.#close();
    }
  }

  #close(): void {
    for (const column of this.#columns) column.close();
    this.memory.free(this.memory.bytes);
  }
}
