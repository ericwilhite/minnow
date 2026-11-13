/**
 * The page directory of one column chunk (format/manifest.ts `ColumnChunk.directory`): every
 * page's position, rows, encoding, checksum and statistics, the dictionary page, and the chunk's
 * distinct sketch. Planning and pruning read this blob, never pages.
 *
 * Byte layout (little-endian), version 1:
 *
 * | field | bytes |
 * | --- | --- |
 * | magic `MPD1` | u32 |
 * | version (1), chunk tag, flags (1 dictionary, 2 sketch, 4 decimal specials), 0 | 4 × u8 |
 * | row count, page count, decimal scale (`MIXED_SCALE` when pages carry their own) | 3 × u32 |
 * | dictionary page (flag 1): offset, length, entries, checksum, encoding, 3 × 0 | 4 × u32 + 4 × u8 |
 * | per page: offset, length, first row, rows, nulls, checksum | 6 × u32 |
 * | per page: encoding, flags (1 min, 2 max, 4 large-value region) | 2 × u8 |
 * | per page: large-value region (flag 4): offset, length | 2 × u32 |
 * | per page: min, max when present | by tag (below) |
 * | sketch (flag 2): length, bytes | u32 + bytes |
 *
 * Statistic values: bool u8; int32 i32; int64 i64; decimal i128 (coefficient at the chunk's
 * scale; a mixed-scale chunk stores u16 length + its sort key, keys/key-encoding.ts, so bounds
 * compare by value whatever the scales, ruling R15.10); float64 and float32 f64; text, bytes and uuid u16 length + bytes (byte order, possibly
 * truncated, `ByteBounds` in encode.ts). Interval has none.
 *
 * The directory is a `BlobRef` with its own checksum; `parseDirectory` additionally checks that
 * pages tile the chunk's rows in order, lie inside its extent in byte order without overlapping
 * (the dictionary page after them), and that each large-value region (`PageEntry.largeRegion`,
 * plain varlen pages only) lies between the previous page and its own: the page's pointers must
 * tile it (decode.ts `parsePage`), so no value aliases another or a page (review B2-p2b #1).
 */
import { DURABLE_FORMATS, requireHeader } from "../durable-format.js";
import type { PageEntry, StatValue } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import { NUMERIC_MAX_SCALE } from "../types.js";
import { check, MIXED_SCALE, ValueTag, PageCorruptionError } from "./format.js";
import { LARGE_VALUE_BYTES } from "./large-value.js";

const MAGIC = DURABLE_FORMATS.pageDirectory.magic; // "MPD1"
/** Every blob is at most 1 MiB (storage track ruling); a page directory is a blob. */
export const MAX_DIRECTORY_BYTES = 1024 * 1024;
const VERSION = DURABLE_FORMATS.pageDirectory.version;

export interface ChunkDirectory {
  readonly tag: number;
  readonly rowCount: number;
  /**
   * Decimal chunks: the scale every coefficient is at, or null for a mixed-scale chunk (each page
   * carries its scales; stored as `MIXED_SCALE`). 0 for other chunks.
   */
  readonly scale: number | null;
  /** Decimal chunks: some value is NaN or ±Infinity (targets need a specials array). */
  readonly hasSpecials: boolean;
  readonly pages: readonly PageEntry[];
  /** The dictionary page (`rowCount` = entries), or null. */
  readonly dictionary: PageEntry | null;
  /** The serialized distinct sketch (sketch.ts), or null. */
  readonly sketch: Uint8Array | null;
}

class Writer {
  bytes = new Uint8Array(256);
  view = new DataView(this.bytes.buffer);
  length = 0;

  #reserve(extra: number): void {
    if (this.length + extra <= this.bytes.length) return;
    let size = this.bytes.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(this.bytes.subarray(0, this.length));
    this.bytes = grown;
    this.view = new DataView(grown.buffer);
  }

  u8(value: number): void {
    this.#reserve(1);
    this.view.setUint8(this.length++, value);
  }
  u16(value: number): void {
    this.#reserve(2);
    this.view.setUint16(this.length, value, true);
    this.length += 2;
  }
  u32(value: number): void {
    this.#reserve(4);
    this.view.setUint32(this.length, value >>> 0, true);
    this.length += 4;
  }
  i32(value: number): void {
    this.#reserve(4);
    this.view.setInt32(this.length, value, true);
    this.length += 4;
  }
  f64(value: number): void {
    this.#reserve(8);
    this.view.setFloat64(this.length, value, true);
    this.length += 8;
  }
  i128(value: bigint): void {
    this.#reserve(16);
    const unsigned = BigInt.asUintN(128, value);
    this.view.setBigUint64(this.length, unsigned & 0xffff_ffff_ffff_ffffn, true);
    this.view.setBigUint64(this.length + 8, unsigned >> 64n, true);
    this.length += 16;
  }
  i64(value: bigint): void {
    this.#reserve(8);
    this.view.setBigInt64(this.length, value, true);
    this.length += 8;
  }
  raw(bytes: Uint8Array): void {
    this.#reserve(bytes.length);
    this.bytes.set(bytes, this.length);
    this.length += bytes.length;
  }
}

class Reader {
  readonly view: DataView;
  at = 0;
  constructor(readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }
  #need(count: number): void {
    check(this.at + count <= this.bytes.length, "Page directory is truncated");
  }
  u8(): number {
    this.#need(1);
    return this.view.getUint8(this.at++);
  }
  u16(): number {
    this.#need(2);
    const value = this.view.getUint16(this.at, true);
    this.at += 2;
    return value;
  }
  u32(): number {
    this.#need(4);
    const value = this.view.getUint32(this.at, true);
    this.at += 4;
    return value;
  }
  i32(): number {
    this.#need(4);
    const value = this.view.getInt32(this.at, true);
    this.at += 4;
    return value;
  }
  f64(): number {
    this.#need(8);
    const value = this.view.getFloat64(this.at, true);
    this.at += 8;
    return value;
  }
  i64(): bigint {
    this.#need(8);
    const value = this.view.getBigInt64(this.at, true);
    this.at += 8;
    return value;
  }
  i128(): bigint {
    this.#need(16);
    const low = this.view.getBigUint64(this.at, true);
    const high = this.view.getBigUint64(this.at + 8, true);
    this.at += 16;
    return BigInt.asIntN(128, (high << 64n) | low);
  }
  raw(count: number): Uint8Array {
    this.#need(count);
    const out = this.bytes.slice(this.at, this.at + count);
    this.at += count;
    return out;
  }
}

function writeStat(writer: Writer, tag: number, value: StatValue): void {
  if (value instanceof Uint8Array) {
    writer.u16(value.length);
    writer.raw(value);
    return;
  }
  switch (tag) {
    case ValueTag.bool:
      writer.u8(value === true ? 1 : 0);
      return;
    case ValueTag.i32:
      writer.i32(value as number);
      return;
    case ValueTag.i64:
      writer.i64(value as bigint);
      return;
    case ValueTag.dec64:
      writer.i128(value as bigint);
      return;
    case ValueTag.f64:
    case ValueTag.f32:
      writer.f64(value as number);
      return;
    default:
      throw new RangeError("A byte statistic must be a Uint8Array");
  }
}

/** `bytes`: the chunk keeps byte bounds (varlen, uuid, mixed-scale decimal sort keys). */
function readStat(reader: Reader, tag: number, bytes: boolean): StatValue {
  if (bytes) return reader.raw(reader.u16());
  switch (tag) {
    case ValueTag.bool: {
      const value = reader.u8();
      check(value <= 1, "Bad bool statistic");
      return value === 1;
    }
    case ValueTag.i32:
      return reader.i32();
    case ValueTag.i64:
      return reader.i64();
    case ValueTag.dec64:
      return reader.i128();
    case ValueTag.f64:
    case ValueTag.f32:
      return reader.f64();
    default:
      return reader.raw(reader.u16());
  }
}

const hasStats = (tag: number): boolean => tag !== ValueTag.interval;

function validEncoding(encoding: number): boolean {
  const base = encoding & ~PageEncoding.gzip;
  return (
    (encoding & ~(PageEncoding.gzip | 0x7)) === 0 &&
    base >= PageEncoding.plain &&
    base <= PageEncoding.scaledFloat
  );
}

export function encodeDirectory(directory: ChunkDirectory): Uint8Array {
  const writer = new Writer();
  writer.u32(MAGIC);
  writer.u8(VERSION);
  writer.u8(directory.tag);
  writer.u8(
    (directory.dictionary !== null ? 1 : 0) |
      (directory.sketch !== null ? 2 : 0) |
      (directory.hasSpecials ? 4 : 0),
  );
  writer.u8(0);
  writer.u32(directory.rowCount);
  writer.u32(directory.pages.length);
  writer.u32(directory.scale ?? MIXED_SCALE);
  const dictionary = directory.dictionary;
  if (dictionary !== null) {
    writer.u32(dictionary.offset);
    writer.u32(dictionary.length);
    writer.u32(dictionary.rowCount);
    writer.u32(dictionary.checksum);
    writer.u8(dictionary.encoding);
    writer.u8(0);
    writer.u8(0);
    writer.u8(0);
  }
  for (const page of directory.pages) {
    writer.u32(page.offset);
    writer.u32(page.length);
    writer.u32(page.firstRow);
    writer.u32(page.rowCount);
    writer.u32(page.nullCount);
    writer.u32(page.checksum);
    writer.u8(page.encoding);
    const stats = hasStats(directory.tag);
    const region = page.largeRegion;
    writer.u8(
      (stats ? (page.min !== null ? 1 : 0) | (page.max !== null ? 2 : 0) : 0) |
        (region === undefined ? 0 : 4),
    );
    if (region !== undefined) {
      writer.u32(region.offset);
      writer.u32(region.length);
    }
    if (stats && page.min !== null) writeStat(writer, directory.tag, page.min);
    if (stats && page.max !== null) writeStat(writer, directory.tag, page.max);
  }
  if (directory.sketch !== null) {
    writer.u32(directory.sketch.length);
    writer.raw(directory.sketch);
  }
  if (writer.length > MAX_DIRECTORY_BYTES)
    throw new RangeError(
      `A page directory of ${String(writer.length)} bytes exceeds the 1 MiB blob cap`,
    );
  return writer.bytes.slice(0, writer.length);
}

/** Where each parsed directory's last page (or dictionary) ends: computed once, at parse. */
const extentEnds = new WeakMap<ChunkDirectory, number>();

function endOf(directory: ChunkDirectory): number {
  let end =
    directory.dictionary === null ? 0 : directory.dictionary.offset + directory.dictionary.length;
  for (const page of directory.pages) end = Math.max(end, page.offset + page.length);
  return end;
}

/**
 * Throws unless every page and the dictionary of `directory` lie inside a chunk of
 * `extentLength` bytes. A directory parsed for one chunk is checked again when another chunk
 * references it from a cache (review B2-p2a #1); O(1) after the parse. Page lengths are
 * positive whichever chunk references it (`parseDirectory` checks that).
 */
export function checkDirectoryWithin(directory: ChunkDirectory, extentLength: number): void {
  let end = extentEnds.get(directory);
  if (end === undefined) {
    end = endOf(directory);
    extentEnds.set(directory, end);
  }
  check(end <= extentLength, "Page outside its chunk's extent");
}

/**
 * Parses a directory blob (already verified against its `BlobRef` checksum). `extentLength` is
 * the chunk's extent length: every page must lie inside it.
 */
export function parseDirectory(bytes: Uint8Array, extentLength: number): ChunkDirectory {
  check(bytes.length <= MAX_DIRECTORY_BYTES, "Page directory exceeds the blob cap");
  requireHeader("pageDirectory", bytes, PageCorruptionError);
  const reader = new Reader(bytes);
  check(reader.u32() === MAGIC, "Not a page directory");
  check(reader.u8() === VERSION, "Unknown page directory version");
  const tag = reader.u8();
  check(tag >= ValueTag.bool && tag <= ValueTag.dec64, "Bad chunk tag");
  const flags = reader.u8();
  check((flags & ~7) === 0 && reader.u8() === 0, "Bad page directory flags");
  const rowCount = reader.u32();
  const pageCount = reader.u32();
  const stored = reader.u32();
  check(stored === 0 || tag === ValueTag.dec64, "Scale outside a decimal chunk");
  check(stored === MIXED_SCALE || stored <= NUMERIC_MAX_SCALE, "Decimal scale out of range");
  const scale = stored === MIXED_SCALE ? null : stored;
  const byteStats =
    tag === ValueTag.varlen ||
    tag === ValueTag.fixed16 ||
    (tag === ValueTag.dec64 && scale === null);
  const inExtent = (offset: number, length: number): void =>
    check(length > 0 && offset + length <= extentLength, "Page outside its chunk's extent");
  let dictionary: PageEntry | null = null;
  if ((flags & 1) !== 0) {
    check(tag === ValueTag.varlen || tag === ValueTag.fixed16, "Dictionary outside varlen");
    const offset = reader.u32();
    const length = reader.u32();
    const entries = reader.u32();
    const checksum = reader.u32();
    const encoding = reader.u8();
    check(reader.u8() === 0 && reader.u8() === 0 && reader.u8() === 0, "Bad dictionary padding");
    check(
      entries > 0 && (encoding & ~PageEncoding.gzip) === PageEncoding.plain,
      "Bad dictionary page",
    );
    inExtent(offset, length);
    dictionary = {
      offset,
      length,
      firstRow: 0,
      rowCount: entries,
      encoding,
      nullCount: 0,
      min: null,
      max: null,
      checksum,
    };
  }
  check(pageCount <= rowCount && (rowCount === 0) === (pageCount === 0), "Bad page count");
  const pages: PageEntry[] = [];
  let next = 0;
  // Pages (and each one's large-value region, just before it) lie in order and never overlap.
  let end = 0;
  for (let index = 0; index < pageCount; index++) {
    const offset = reader.u32();
    const length = reader.u32();
    const firstRow = reader.u32();
    const rows = reader.u32();
    const nullCount = reader.u32();
    const checksum = reader.u32();
    const encoding = reader.u8();
    const statFlags = reader.u8();
    check(firstRow === next && rows > 0 && nullCount <= rows, "Pages do not tile the chunk's rows");
    check(validEncoding(encoding), "Bad page encoding");
    check(
      (statFlags & ~7) === 0 && (hasStats(tag) || (statFlags & 3) === 0),
      "Bad statistics flags",
    );
    let largeRegion: PageEntry["largeRegion"];
    if ((statFlags & 4) !== 0) {
      check(
        tag === ValueTag.varlen && (encoding & ~PageEncoding.gzip) === PageEncoding.plain,
        "Large values outside a plain varlen page",
      );
      const regionOffset = reader.u32();
      const regionLength = reader.u32();
      check(
        regionLength > LARGE_VALUE_BYTES &&
          regionOffset >= end &&
          regionOffset + regionLength <= offset,
        "A large-value region overlaps a page or another region",
      );
      largeRegion = { offset: regionOffset, length: regionLength };
    }
    check(offset >= end, "Pages overlap or are out of order");
    end = offset + length;
    if (
      encoding === PageEncoding.dictionary ||
      encoding === (PageEncoding.dictionary | PageEncoding.gzip)
    )
      check(dictionary !== null, "Dictionary page without a dictionary");
    inExtent(offset, length);
    const min = (statFlags & 1) !== 0 ? readStat(reader, tag, byteStats) : null;
    const max = (statFlags & 2) !== 0 ? readStat(reader, tag, byteStats) : null;
    pages.push({
      offset,
      length,
      firstRow,
      rowCount: rows,
      encoding,
      nullCount,
      min,
      max,
      checksum,
      ...(largeRegion === undefined ? {} : { largeRegion }),
    });
    next += rows;
  }
  check(next === rowCount, "Pages do not cover the chunk's rows");
  check(dictionary === null || dictionary.offset >= end, "The dictionary page overlaps a page");
  let sketch: Uint8Array | null = null;
  if ((flags & 2) !== 0) sketch = reader.raw(reader.u32());
  check(reader.at === bytes.length, "Page directory has trailing bytes");
  const directory = {
    tag,
    rowCount,
    scale,
    hasSpecials: (flags & 4) !== 0,
    pages,
    dictionary,
    sketch,
  };
  extentEnds.set(directory, endOf(directory));
  return directory;
}

/** The index of the page holding `row` (binary search over first rows). */
export function pageIndexOf(pages: readonly PageEntry[], row: number): number {
  let low = 0;
  let high = pages.length - 1;
  while (low < high) {
    const middle = (low + high + 1) >>> 1;
    if ((pages[middle]?.firstRow ?? 0) <= row) low = middle;
    else high = middle - 1;
  }
  return low;
}
