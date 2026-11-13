/**
 * The page format (version 1): constants, errors and the byte layout every page codec shares.
 *
 * ## A page
 * A page is independently decodable: it holds one column's values for a run of rows of one
 * group. Everything is little-endian. Sections start at 8-byte offsets from the page start, so a
 * page read into an 8-aligned buffer is viewed in place by typed arrays.
 *
 * | offset | size | field |
 * | --- | --- | --- |
 * | 0 | u8 | `kind`: a `PageEncoding` (format/manifest.ts) without the gzip bit |
 * | 1 | u8 | `tag`: the stored value family (`ValueTag`) |
 * | 2 | u8 | `flags` (`PageFlag`) |
 * | 3 | u8 | `width`: bytes per element of the main array (0, 1, 2, 4, 8, 16) |
 * | 4 | u32 | `rowCount` |
 * | 8 | u32 | `aux`: plain varlen: value bytes; dictionary: the largest code (width 0: every row's code); decimal with `uniformScale`: the page's scale |
 * | 12 | u32 | reserved, 0 |
 * | 16 | | validity words (`flags & validity`), u32 per 32 rows, bit set = valid |
 * | | | decimal specials (`flags & specials`), one `DecimalSpecial` byte per row |
 * | | | decimal scales (`flags & scales`), one u16 per row (mixed-scale chunks) |
 * | | | large-value rows (`flags & large`, plain varlen only), u32 bit words like validity |
 * | | | payload (below) |
 *
 * Payload by `kind`:
 * - **constant**: one value. varlen: `aux` bytes. All-null pages (`flags & allNull`) have none.
 * - **plain**: `rowCount` values of `width` bytes. varlen: `rowCount + 1` offsets of `width`
 *   (2 or 4) bytes, padded to 8, then `aux` value bytes. interval: months (i32), days (i32),
 *   micros (i64) arrays, each padded to 8.
 * - **bitmap** (bool): the values as u32 bit words.
 * - **frameOfReference**: an 8-byte base (int64, low word first), then `rowCount` unsigned
 *   deltas of `width` bytes: value = base + delta. With `flags & delta`, the base is the minimum
 *   successive difference, then one 8-byte absolute value per `DELTA_BLOCK` rows (the block's
 *   first value), then the deltas: value[i] = value[i − 1] + base + delta[i] for i inside a
 *   block. Width 0 means every delta is 0. Arithmetic wraps modulo 2^64 (int64) and is exact.
 * - **dictionary**: `rowCount` codes of `width` bytes into the chunk's dictionary (width 0: every
 *   row has code `aux`).
 * - **scaledFloat** (float64 only): exact decimals. `aux` is the exponent e (≤
 *   `SCALED_FLOAT_MAX_EXPONENT`), `width` 0, 1, 2 or 4. An 8-byte f64 base (a safe integer, the
 *   smallest n), then `rowCount` unsigned deltas of `width` bytes padded to 8, then a u32 patch
 *   count and a u32 0, the patched rows (u32, increasing) padded to 8, and each patched row's own
 *   8 bytes. A row decodes as (base + delta) / 10^e, a patched row as its bytes: bit-identical
 *   for every float64. The writer patches every value that does not satisfy that formula
 *   exactly (−0, NaN with its payload, ±Infinity, subnormals, inexact decimals) and keeps the
 *   page plain when more than 1 in 16 valid rows would be patched or it would not be smaller.
 *   Null and patched rows hold delta 0. Readers decode it straight into a target; predicates
 *   read a plain f64 lane materialized on first use.
 *
 * The page's length is exactly the end of its last section, so every header field is checked
 * against it: a page whose computed length differs from its stored length is corrupt.
 *
 * ## Stored form
 * The stored bytes of a page are either the page itself, or, when `PageEntry.encoding` has the
 * gzip bit, a u32 raw length followed by a gzip stream of the page. The xxHash32 in the page's
 * directory entry covers the stored bytes and is checked once when they come from storage.
 * Every page, dictionary page and directory starts at an 8-byte offset of its extent (the
 * writer puts up to 7 zero bytes before it), so pages read together into one buffer are viewed
 * in place.
 */
import { DURABLE_CODECS, DURABLE_FORMATS, PAGE_CODEC } from "../durable-format.js";
import type { PhysicalType } from "../types.js";

export const PAGE_FORMAT_VERSION = DURABLE_FORMATS.page.version;
export const PAGE_HEADER_BYTES = 16;
/** Rows per delta block: a delta page restarts its prefix sum every this many rows. */
export const DELTA_BLOCK = PAGE_CODEC.deltaRows;
/** The largest exponent of a `scaledFloat` page (10^15 is exact; integers stay safe). */
export const SCALED_FLOAT_MAX_EXPONENT = PAGE_CODEC.scaledFloatMaxExponent;
/**
 * The one validity rule of a `scaledFloat` page's integers, shared by writer and reader (review
 * B2-p2i #1): the base and base + the largest stored delta are safe integers, so every row's
 * integer is. The writer declines any page this refuses; the reader refuses any page it refuses.
 */
export function scaledFloatSafe(base: number, maxDelta: number): boolean {
  return Number.isSafeInteger(base) && Number.isSafeInteger(base + maxDelta);
}
/** Text and bytes statistics keep at most this many bytes of a bound. */
export const STAT_PREFIX_BYTES = PAGE_CODEC.statPrefixBytes;

/** The stored value family of a page or chunk. */
export const ValueTag = DURABLE_CODECS.value;
export type ValueTagValue = (typeof ValueTag)[keyof typeof ValueTag];

export const PageFlag = DURABLE_CODECS.pageFlag;

/** The directory's scale field of a mixed-scale decimal chunk (each page carries its scales). */
export const MIXED_SCALE = PAGE_CODEC.mixedScale;

/** Stored bytes that cannot be what the writer wrote: checksum, structure or range failures. */
export class PageCorruptionError extends Error {
  readonly code = "MINNOW_PAGE_CORRUPT";
  constructor(message: string) {
    super(message);
    this.name = "PageCorruptionError";
  }
}

/**
 * A value layout the page format does not store (yet): decimal coefficients past 128 bits
 * (`bigint[]`), and locators (query-local by design). Raised by the writer, never by a
 * reader of stored pages.
 */
export class UnsupportedPageLayoutError extends Error {
  readonly code = "MINNOW_PAGE_UNSUPPORTED_LAYOUT";
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedPageLayoutError";
  }
}

export function align8(value: number): number {
  return (value + 7) & ~7;
}

export function validityBytes(rows: number): number {
  return ((rows + 31) >>> 5) * 4;
}

/** The chunk tag of a physical type. */
export function chunkTagOf(type: PhysicalType): ValueTagValue {
  switch (type.kind) {
    case "bool":
      return ValueTag.bool;
    case "int32":
    case "date":
      return ValueTag.i32;
    case "int64":
    case "time":
    case "timestamp":
      return ValueTag.i64;
    case "float64":
      return ValueTag.f64;
    case "float32":
      return ValueTag.f32;
    case "decimal":
      return ValueTag.dec64;
    case "text":
    case "bytes":
    case "json":
    case "enum":
    case "array":
      return ValueTag.varlen;
    case "uuid":
      return ValueTag.fixed16;
    case "interval":
      return ValueTag.interval;
    case "locator":
      throw new UnsupportedPageLayoutError("Locators are query-local and never stored");
  }
}

/** Fixed bytes per value of a tag's plain form (0 for varlen). */
export function plainWidth(tag: number): number {
  switch (tag) {
    case ValueTag.i32:
    case ValueTag.f32:
      return 4;
    case ValueTag.i64:
    case ValueTag.f64:
    case ValueTag.dec64:
      return 8;
    case ValueTag.fixed16:
    case ValueTag.interval:
    case ValueTag.dec128:
      return 16;
    default:
      return 0;
  }
}

/** Throws `PageCorruptionError` unless `condition`. */
export function check(condition: boolean, message: string): asserts condition {
  if (!condition) throw new PageCorruptionError(message);
}

const probe = new Uint16Array([1]);
/** Typed-array views of stored bytes assume a little-endian platform (every browser engine). */
export const LITTLE_ENDIAN = new Uint8Array(probe.buffer)[0] === 1;
