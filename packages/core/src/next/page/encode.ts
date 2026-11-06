/**
 * Page encoders. Each takes one page's raw values (the writer's page buffer), computes the page
 * statistics in the same pass, picks the smallest encoding, and writes the page into a reusable
 * scratch buffer.
 *
 * ## Selection rules (smallest stored size wins; ties go to the earlier entry)
 * - Every family: all rows null → constant all-null; every valid value equal → constant.
 * - int32, date, int64, time, timestamp, int64-tier decimal coefficients:
 *   1. plain (4 or 8 bytes per row);
 *   2. frame of reference: the page's value range picks u8 (< 2^8), u16 (< 2^16) or u32 (< 2^32,
 *      int64 only); base = minimum;
 *   3. delta (pages without nulls): the range of successive differences picks width 0 (an
 *      arithmetic sequence), u8, u16 or u32, plus one checkpoint per `DELTA_BLOCK` rows. Never
 *      used when a difference reaches 2^52, so the f64 decode path stays exact.
 * - 128-bit decimal coefficients: when every value fits int64, the int64 rules (tag `dec64`);
 *   otherwise plain limbs.
 * - float64, float32, interval, uuid without a dictionary: plain.
 * - bool: a bitmap.
 * - text, bytes, json, enum, array, uuid: dictionary codes (writer.ts decides, per page) or plain
 *   (u16 offsets when the page's bytes are < 64 KiB, else u32).
 *
 * Every encoder also adds each valid value to the chunk's distinct sketch, except dictionary
 * pages, whose new entries are added once by the dictionary.
 */
import type { StatValue } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import { DecimalSpecial } from "../vector/vector.js";
import { compareBytes } from "../vector/varlen.js";
import { countBits } from "./bits.js";
import {
  align8,
  DELTA_BLOCK,
  PAGE_HEADER_BYTES,
  PageFlag,
  STAT_PREFIX_BYTES,
  validityBytes,
  SCALED_FLOAT_MAX_EXPONENT,
  scaledFloatSafe,
  ValueTag,
} from "./format.js";
import { DistinctSketch, hashBytes, hashFloat, hashInt64, mix32 } from "./sketch.js";

export interface EncodedPage {
  /** The page bytes; a view of the encoder's scratch, valid until its next page. */
  readonly bytes: Uint8Array;
  /** A `PageEncoding` value (no gzip bit). */
  readonly encoding: number;
  readonly rowCount: number;
  readonly nullCount: number;
  readonly min: StatValue | null;
  readonly max: StatValue | null;
}

/** Grows by doubling; every page starts at byte 0 of an 8-aligned ArrayBuffer. */
export class PageScratch {
  /** Empty until the first page: the owner charges (via `onGrow`) before anything is allocated. */
  #buffer = new ArrayBuffer(0);

  /** `onGrow(newBytes, oldBytes)` runs before a larger buffer replaces the current one. */
  constructor(private readonly onGrow: ((grown: number, old: number) => void) | null = null) {}

  get byteLength(): number {
    return this.#buffer.byteLength;
  }

  /** A zeroed buffer of at least `bytes`, valid until the next call. */
  take(bytes: number): ArrayBuffer {
    if (bytes > this.#buffer.byteLength) {
      let size = Math.max(64 * 1024, this.#buffer.byteLength * 2);
      while (size < bytes) size *= 2;
      this.onGrow?.(size, this.#buffer.byteLength);
      this.#buffer = new ArrayBuffer(size);
    } else new Uint8Array(this.#buffer, 0, bytes).fill(0);
    return this.#buffer;
  }
}

function header(
  buffer: ArrayBuffer,
  kind: number,
  tag: number,
  flags: number,
  width: number,
  rows: number,
  aux: number,
): void {
  const bytes = new Uint8Array(buffer, 0, PAGE_HEADER_BYTES);
  bytes[0] = kind;
  bytes[1] = tag;
  bytes[2] = flags;
  bytes[3] = width;
  const words = new Uint32Array(buffer, 4, 3);
  words[0] = rows;
  words[1] = aux >>> 0;
  words[2] = 0;
}

/** Validity section (when the page has some nulls but not all); returns the next offset. */
function writeValidity(
  buffer: ArrayBuffer,
  at: number,
  validity: Uint32Array | null,
  rows: number,
  nulls: number,
): number {
  if (validity === null || nulls === 0 || nulls === rows) return at;
  const words = (rows + 31) >>> 5;
  const target = new Uint32Array(buffer, at, words);
  target.set(validity.subarray(0, words));
  const rest = rows & 31;
  if (rest !== 0) target[words - 1] = (target[words - 1] ?? 0) & ((1 << rest) - 1);
  return align8(at + words * 4);
}

function validityFlags(rows: number, nulls: number): number {
  if (nulls === rows) return PageFlag.allNull;
  return nulls > 0 ? PageFlag.validity : 0;
}

function sectionStart(rows: number, nulls: number): number {
  return PAGE_HEADER_BYTES + (nulls > 0 && nulls < rows ? align8(validityBytes(rows)) : 0);
}

function finish(
  buffer: ArrayBuffer,
  length: number,
  encoding: number,
  rows: number,
  nulls: number,
  min: StatValue | null,
  max: StatValue | null,
): EncodedPage {
  return {
    bytes: new Uint8Array(buffer, 0, length),
    encoding,
    rowCount: rows,
    nullCount: nulls,
    min,
    max,
  };
}

function isValidAt(validity: Uint32Array | null, row: number): boolean {
  return validity === null || (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1;
}

function nullsOf(validity: Uint32Array | null, rows: number): number {
  return validity === null ? 0 : rows - countBits(validity, rows);
}

function allNullPage(scratch: PageScratch, tag: number, rows: number, flags = 0): EncodedPage {
  const buffer = scratch.take(PAGE_HEADER_BYTES);
  header(buffer, PageEncoding.constant, tag, PageFlag.allNull | flags, 0, rows, 0);
  return finish(buffer, PAGE_HEADER_BYTES, PageEncoding.constant, rows, rows, null, null);
}

const forWidth = (range: number): number => (range < 0x100 ? 1 : range < 0x1_0000 ? 2 : 4);
const deltaWidth = (range: number): number =>
  range === 0 ? 0 : range < 0x100 ? 1 : range < 0x1_0000 ? 2 : 4;

let deltaScratch = new Int32Array(4096);
/** Int32 scratch for deltas and codes: computed in one monomorphic loop, then narrowed natively. */
function scratchOf(rows: number): Int32Array {
  if (deltaScratch.length < rows)
    deltaScratch = new Int32Array(Math.max(rows, deltaScratch.length * 2));
  return deltaScratch;
}

/** Writes `values[0 … rows)` (each < 2^(8 × width)) as `width`-byte unsigned integers at `at`. */
function narrowInto(
  buffer: ArrayBuffer,
  at: number,
  width: number,
  values: Int32Array,
  rows: number,
): void {
  writeUnsigned(buffer, at, width, rows).set(values.subarray(0, rows));
}

function writeUnsigned(
  buffer: ArrayBuffer,
  at: number,
  width: number,
  rows: number,
): Uint8Array | Uint16Array | Uint32Array {
  return width === 1
    ? new Uint8Array(buffer, at, rows)
    : width === 2
      ? new Uint16Array(buffer, at, rows)
      : new Uint32Array(buffer, at, rows);
}

function writeBase(buffer: ArrayBuffer, at: number, low: number, high: number): void {
  const words = new Int32Array(buffer, at, 2);
  words[0] = low | 0;
  words[1] = high | 0;
}

// ---------------------------------------------------------------------------------------------
// int32 / date

export function encodeInt32Page(
  values: Int32Array,
  validity: Uint32Array | null,
  rows: number,
  sketch: DistinctSketch,
  scratch: PageScratch,
): EncodedPage {
  const tag = ValueTag.i32;
  let nulls = 0;
  let min = 0x7fff_ffff;
  let max = -0x8000_0000;
  if (validity === null)
    for (let row = 0; row < rows; row++) {
      const value = values[row] ?? 0;
      if (value < min) min = value;
      if (value > max) max = value;
      sketch.add(mix32(value));
    }
  else
    for (let row = 0; row < rows; row++) {
      if (!isValidAt(validity, row)) {
        nulls++;
        continue;
      }
      const value = values[row] ?? 0;
      if (value < min) min = value;
      if (value > max) max = value;
      sketch.add(mix32(value));
    }
  if (nulls === rows) return allNullPage(scratch, tag, rows);
  const start = sectionStart(rows, nulls);
  const flags = validityFlags(rows, nulls);
  if (min === max) {
    const buffer = scratch.take(start + 8);
    header(buffer, PageEncoding.constant, tag, flags, 4, rows, 0);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    new Int32Array(buffer, start, 1)[0] = min;
    return finish(buffer, start + 4, PageEncoding.constant, rows, nulls, min, max);
  }
  const plainSize = rows * 4;
  const range = max - min;
  const width = forWidth(range);
  const forSize = width < 4 ? 8 + rows * width : Infinity;
  let deltaSize = Infinity;
  let minDelta = 0;
  let dWidth = 0;
  if (nulls === 0 && rows > 1) {
    let low = Infinity;
    let high = -Infinity;
    for (let row = 1; row < rows; row++) {
      const difference = (values[row] ?? 0) - (values[row - 1] ?? 0);
      if (difference < low) low = difference;
      if (difference > high) high = difference;
    }
    if (high - low < 0x1_0000_0000) {
      minDelta = low;
      dWidth = deltaWidth(high - low);
      deltaSize = 8 + Math.ceil(rows / DELTA_BLOCK) * 8 + rows * dWidth;
    }
  }
  if (plainSize <= forSize && plainSize <= deltaSize) {
    const buffer = scratch.take(start + plainSize);
    header(buffer, PageEncoding.plain, tag, flags, 4, rows, 0);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    new Int32Array(buffer, start, rows).set(values.subarray(0, rows));
    return finish(buffer, start + plainSize, PageEncoding.plain, rows, nulls, min, max);
  }
  if (forSize <= deltaSize) {
    const buffer = scratch.take(start + align8(forSize));
    header(buffer, PageEncoding.frameOfReference, tag, flags, width, rows, 0);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    writeBase(buffer, start, min, min < 0 ? -1 : 0);
    const deltas = scratchOf(rows);
    if (validity === null)
      for (let row = 0; row < rows; row++) deltas[row] = (values[row] ?? 0) - min;
    else
      for (let row = 0; row < rows; row++)
        deltas[row] = isValidAt(validity, row) ? (values[row] ?? 0) - min : 0;
    narrowInto(buffer, start + 8, width, deltas, rows);
    return finish(buffer, start + forSize, PageEncoding.frameOfReference, rows, nulls, min, max);
  }
  const blocks = Math.ceil(rows / DELTA_BLOCK);
  const buffer = scratch.take(start + align8(deltaSize));
  header(buffer, PageEncoding.frameOfReference, tag, flags | PageFlag.delta, dWidth, rows, 0);
  writeBase(buffer, start, minDelta | 0, minDelta < 0 ? -1 : 0);
  const checkpoints = new Int32Array(buffer, start + 8, blocks * 2);
  for (let block = 0; block < blocks; block++) {
    const value = values[block * DELTA_BLOCK] ?? 0;
    checkpoints[block * 2] = value;
    checkpoints[block * 2 + 1] = value < 0 ? -1 : 0;
  }
  if (dWidth > 0) {
    const deltas = scratchOf(rows);
    deltas[0] = 0;
    for (let row = 1; row < rows; row++)
      deltas[row] =
        row % DELTA_BLOCK === 0 ? 0 : (values[row] ?? 0) - (values[row - 1] ?? 0) - minDelta;
    narrowInto(buffer, start + 8 + blocks * 8, dWidth, deltas, rows);
  }
  return finish(buffer, start + deltaSize, PageEncoding.frameOfReference, rows, nulls, min, max);
}

// ---------------------------------------------------------------------------------------------
// int64 family, as (low, high) word pairs

const SAFE_HIGH = 0x20_0000; // 2^21: |v| ≤ 2^53 − 1 ⇔ high ∈ [−2^21, 2^21) except −2^53 itself

function int64Stat(low: number, high: number): bigint {
  return BigInt.asIntN(64, (BigInt(high | 0) << 32n) | BigInt(low >>> 0));
}

/** low < 2^32 unsigned compare of (high, low) pairs: a < b. */
function less(aLow: number, aHigh: number, bLow: number, bHigh: number): boolean {
  return aHigh < bHigh || (aHigh === bHigh && aLow >>> 0 < bLow >>> 0);
}

/** A decimal page's side arrays: specials, and per-row scales for a mixed-scale chunk. */
export interface DecimalSide {
  readonly specials: Uint8Array | null;
  /** Non-null only in a mixed-scale chunk (format.ts `MIXED_SCALE`). */
  readonly scales: Uint16Array | null;
}

interface SideLayout {
  readonly flags: number;
  readonly aux: number;
  /** Section bytes after the validity. */
  readonly bytes: number;
  /** Mixed scale: statistics are not comparable across scales, so the page has none. */
  readonly noStats: boolean;
  write(buffer: ArrayBuffer, at: number): void;
}

function sideLayout(
  side: DecimalSide | null,
  validity: Uint32Array | null,
  rows: number,
): SideLayout {
  const specials = side?.specials ?? null;
  const scales = side?.scales ?? null;
  const hasSpecials = specials !== null && hasNonZero(specials, rows);
  let uniform = -1;
  let mixed = false;
  if (scales !== null)
    for (let row = 0; row < rows && !mixed; row++) {
      if (!isValidAt(validity, row)) continue;
      const scale = scales[row] ?? 0;
      if (uniform < 0) uniform = scale;
      else if (scale !== uniform) mixed = true;
    }
  const specialsBytes = hasSpecials ? align8(rows) : 0;
  return {
    flags:
      (hasSpecials ? PageFlag.specials : 0) |
      (scales === null ? 0 : mixed ? PageFlag.scales : PageFlag.uniformScale),
    aux: scales !== null && !mixed ? Math.max(0, uniform) : 0,
    bytes: specialsBytes + (mixed ? align8(rows * 2) : 0),
    noStats: hasSpecials || scales !== null,
    write(buffer: ArrayBuffer, at: number): void {
      if (hasSpecials) new Uint8Array(buffer, at, rows).set(specials.subarray(0, rows));
      if (mixed && scales !== null)
        new Uint16Array(buffer, at + specialsBytes, rows).set(scales.subarray(0, rows));
    },
  };
}

/**
 * Encodes an int64-family page from word pairs (`words[2i]` low, `words[2i + 1]` high), the
 * memory layout of a BigInt64Array. `tag` is `i64` or `dec64`; a decimal's `side` arrays are
 * written as sections (decode.ts). `sketch` null: the caller hashes (mixed-scale decimals).
 */
export function encodeInt64Page(
  words: Int32Array,
  validity: Uint32Array | null,
  rows: number,
  tag: number,
  sketch: DistinctSketch | null,
  scratch: PageScratch,
  side: DecimalSide | null = null,
): EncodedPage {
  let nulls = 0;
  let minLow = -1;
  let minHigh = 0x7fff_ffff;
  let maxLow = 0;
  let maxHigh = -0x8000_0000;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) {
      nulls++;
      continue;
    }
    const low = words[row * 2] ?? 0;
    const high = words[row * 2 + 1] ?? 0;
    if (less(low, high, minLow, minHigh)) {
      minLow = low;
      minHigh = high;
    }
    if (less(maxLow, maxHigh, low, high)) {
      maxLow = low;
      maxHigh = high;
    }
    sketch?.add(hashInt64(low, high));
  }
  if (nulls === rows) return allNullPage(scratch, tag, rows);
  const sides = sideLayout(side, validity, rows);
  const safe =
    (minHigh > -SAFE_HIGH || (minHigh === -SAFE_HIGH && minLow >>> 0 >= 1)) && maxHigh < SAFE_HIGH;
  const flags = validityFlags(rows, nulls) | sides.flags | (safe ? PageFlag.safe : 0);
  const start = sectionStart(rows, nulls) + sides.bytes;
  const aux = sides.aux;
  const writeSections = (buffer: ArrayBuffer): void => {
    sides.write(buffer, writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls));
  };
  const min = sides.noStats ? null : int64Stat(minLow, minHigh);
  const max = sides.noStats ? null : int64Stat(maxLow, maxHigh);
  if (minLow === maxLow && minHigh === maxHigh) {
    const buffer = scratch.take(start + 8);
    header(buffer, PageEncoding.constant, tag, flags, 8, rows, aux);
    writeSections(buffer);
    writeBase(buffer, start, minLow, minHigh);
    return finish(buffer, start + 8, PageEncoding.constant, rows, nulls, min, max);
  }
  const plainSize = rows * 8;
  // range = max − min as an unsigned 64-bit value; FOR needs its high word to be 0.
  const rangeLow = ((maxLow >>> 0) - (minLow >>> 0)) >>> 0;
  const borrow = maxLow >>> 0 < minLow >>> 0 ? 1 : 0;
  const rangeHigh = (maxHigh - minHigh - borrow) | 0;
  const width = rangeHigh === 0 ? forWidth(rangeLow) : 0;
  const forSize = width > 0 ? 8 + rows * width : Infinity;
  let deltaSize = Infinity;
  let dWidth = 0;
  let dMinLow = 0;
  let dMinHigh = 0;
  if (nulls === 0 && rows > 1) {
    let lowLow = -1;
    let lowHigh = 0x7fff_ffff;
    let highLow = 0;
    let highHigh = -0x8000_0000;
    let bounded = true;
    for (let row = 1; row < rows; row++) {
      const aLow = words[row * 2] ?? 0;
      const aHigh = words[row * 2 + 1] ?? 0;
      const bLow = words[row * 2 - 2] ?? 0;
      const bHigh = words[row * 2 - 1] ?? 0;
      const dLow = ((aLow >>> 0) - (bLow >>> 0)) | 0;
      const dHigh = (aHigh - bHigh - (aLow >>> 0 < bLow >>> 0 ? 1 : 0)) | 0;
      // |difference| < 2^52 keeps every step of the f64 decode exact.
      if (dHigh >= 0x10_0000 || dHigh < -0x10_0000) {
        bounded = false;
        break;
      }
      if (less(dLow, dHigh, lowLow, lowHigh)) {
        lowLow = dLow;
        lowHigh = dHigh;
      }
      if (less(highLow, highHigh, dLow, dHigh)) {
        highLow = dLow;
        highHigh = dHigh;
      }
    }
    if (bounded) {
      const spanLow = ((highLow >>> 0) - (lowLow >>> 0)) >>> 0;
      const spanHigh = (highHigh - lowHigh - (highLow >>> 0 < lowLow >>> 0 ? 1 : 0)) | 0;
      if (spanHigh === 0) {
        dWidth = deltaWidth(spanLow);
        dMinLow = lowLow;
        dMinHigh = lowHigh;
        deltaSize = 8 + Math.ceil(rows / DELTA_BLOCK) * 8 + rows * dWidth;
      }
    }
  }
  if (plainSize <= forSize && plainSize <= deltaSize) {
    const buffer = scratch.take(start + plainSize);
    header(buffer, PageEncoding.plain, tag, flags, 8, rows, aux);
    writeSections(buffer);
    const out = new Int32Array(buffer, start, rows * 2);
    out.set(words.subarray(0, rows * 2));
    zeroInvalidPairs(out, validity, rows);
    return finish(buffer, start + plainSize, PageEncoding.plain, rows, nulls, min, max);
  }
  if (forSize <= deltaSize) {
    const buffer = scratch.take(start + align8(forSize));
    header(buffer, PageEncoding.frameOfReference, tag, flags, width, rows, aux);
    writeSections(buffer);
    writeBase(buffer, start, minLow, minHigh);
    const deltas = scratchOf(rows);
    const base = minLow >>> 0;
    for (let row = 0; row < rows; row++)
      deltas[row] = isValidAt(validity, row) ? ((words[row * 2] ?? 0) >>> 0) - base : 0;
    narrowInto(buffer, start + 8, width, deltas, rows);
    return finish(buffer, start + forSize, PageEncoding.frameOfReference, rows, nulls, min, max);
  }
  const blocks = Math.ceil(rows / DELTA_BLOCK);
  const buffer = scratch.take(start + align8(deltaSize));
  header(buffer, PageEncoding.frameOfReference, tag, flags | PageFlag.delta, dWidth, rows, aux);
  writeSections(buffer);
  writeBase(buffer, start, dMinLow, dMinHigh);
  const checkpoints = new Int32Array(buffer, start + 8, blocks * 2);
  for (let block = 0; block < blocks; block++) {
    checkpoints[block * 2] = words[block * DELTA_BLOCK * 2] ?? 0;
    checkpoints[block * 2 + 1] = words[block * DELTA_BLOCK * 2 + 1] ?? 0;
  }
  if (dWidth > 0) {
    const deltas = scratchOf(rows);
    const base = dMinLow >>> 0;
    deltas[0] = 0;
    for (let row = 1; row < rows; row++) {
      const difference = ((words[row * 2] ?? 0) >>> 0) - ((words[row * 2 - 2] ?? 0) >>> 0);
      deltas[row] = row % DELTA_BLOCK === 0 ? 0 : (difference - base) | 0;
    }
    narrowInto(buffer, start + 8 + blocks * 8, dWidth, deltas, rows);
  }
  return finish(buffer, start + deltaSize, PageEncoding.frameOfReference, rows, nulls, min, max);
}

/**
 * Null slots of a plain int64 page hold 0, not the input's garbage: a page flagged `safe` then
 * holds only safe values, which the decoder rechecks (decode.ts `checkSafe`).
 */
function zeroInvalidPairs(words: Int32Array, validity: Uint32Array | null, rows: number): void {
  if (validity === null) return;
  for (let row = 0; row < rows; row++)
    if (!isValidAt(validity, row)) {
      words[row * 2] = 0;
      words[row * 2 + 1] = 0;
    }
}

function hasNonZero(bytes: Uint8Array, rows: number): boolean {
  for (let row = 0; row < rows; row++) if ((bytes[row] ?? 0) !== DecimalSpecial.finite) return true;
  return false;
}

/**
 * 128-bit decimal coefficients (four u32 limbs per row). Pages whose values all fit int64 are
 * encoded by the int64 rules under tag `dec64`; `pairs` is scratch of ≥ 2 × rows words.
 */
export function encodeInt128Page(
  limbs: Uint32Array,
  validity: Uint32Array | null,
  rows: number,
  side: DecimalSide,
  sketch: DistinctSketch | null,
  scratch: PageScratch,
  pairs: Int32Array,
): EncodedPage {
  let fits = true;
  for (let row = 0; row < rows && fits; row++) {
    if (!isValidAt(validity, row)) continue;
    const sign = (limbs[row * 4 + 1] ?? 0) >> 31;
    fits = (limbs[row * 4 + 2] ?? 0) === sign >>> 0 && (limbs[row * 4 + 3] ?? 0) === sign >>> 0;
  }
  if (fits) {
    for (let row = 0; row < rows; row++) {
      pairs[row * 2] = limbs[row * 4] ?? 0;
      pairs[row * 2 + 1] = limbs[row * 4 + 1] ?? 0;
    }
    return encodeInt64Page(pairs, validity, rows, ValueTag.dec64, sketch, scratch, side);
  }
  const tag = ValueTag.dec128;
  const nulls = nullsOf(validity, rows);
  const sides = sideLayout(side, validity, rows);
  let minRow = -1;
  let maxRow = -1;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) continue;
    sketch?.add(hashInt128(limbs, row));
    if (minRow < 0 || compareInt128(limbs, row, minRow) < 0) minRow = row;
    if (maxRow < 0 || compareInt128(limbs, row, maxRow) > 0) maxRow = row;
  }
  const flags = validityFlags(rows, nulls) | sides.flags;
  const start = sectionStart(rows, nulls) + sides.bytes;
  const size = rows * 16;
  const buffer = scratch.take(start + size);
  header(buffer, PageEncoding.plain, tag, flags, 16, rows, sides.aux);
  sides.write(buffer, writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls));
  new Uint32Array(buffer, start, rows * 4).set(limbs.subarray(0, rows * 4));
  const stat = (row: number): bigint =>
    BigInt.asIntN(
      128,
      BigInt(limbs[row * 4] ?? 0) |
        (BigInt(limbs[row * 4 + 1] ?? 0) << 32n) |
        (BigInt(limbs[row * 4 + 2] ?? 0) << 64n) |
        (BigInt(limbs[row * 4 + 3] ?? 0) << 96n),
    );
  return finish(
    buffer,
    start + size,
    PageEncoding.plain,
    rows,
    nulls,
    sides.noStats ? null : stat(minRow),
    sides.noStats ? null : stat(maxRow),
  );
}

function compareInt128(limbs: Uint32Array, a: number, b: number): number {
  const aTop = (limbs[a * 4 + 3] ?? 0) | 0;
  const bTop = (limbs[b * 4 + 3] ?? 0) | 0;
  if (aTop !== bTop) return aTop < bTop ? -1 : 1;
  for (let limb = 2; limb >= 0; limb--) {
    const x = limbs[a * 4 + limb] ?? 0;
    const y = limbs[b * 4 + limb] ?? 0;
    if (x !== y) return x < y ? -1 : 1;
  }
  return 0;
}

/** Tier-independent: a value that fits int64 hashes as `hashInt64`. */
export function hashInt128(limbs: Uint32Array, row: number): number {
  const low = limbs[row * 4] ?? 0;
  const high = limbs[row * 4 + 1] ?? 0;
  const sign = (high >> 31) >>> 0;
  const h = hashInt64(low, high);
  if ((limbs[row * 4 + 2] ?? 0) === sign && (limbs[row * 4 + 3] ?? 0) === sign) return h;
  return mix32(h ^ hashInt64(limbs[row * 4 + 2] ?? 0, limbs[row * 4 + 3] ?? 0));
}

// ---------------------------------------------------------------------------------------------
// floats

/**
 * Bounds in PostgreSQL's float order (NaN above +Infinity): min and max over the non-NaN values;
 * max is NaN when any NaN is present; both are NaN only when no non-NaN value is.
 */
function floatStats(
  min: number,
  max: number,
  sawNaN: boolean,
  sawNumber: boolean,
): [number, number] {
  if (!sawNumber) return [Number.NaN, Number.NaN];
  return [min, sawNaN ? Number.NaN : max];
}

/** float64 (`width` 8) or float32 (`width` 4): plain, or constant when every value is equal. */
export function encodeFloatPage(
  values: Float64Array | Float32Array,
  validity: Uint32Array | null,
  rows: number,
  sketch: DistinctSketch,
  scratch: PageScratch,
): EncodedPage {
  const wide = values instanceof Float64Array;
  const tag = wide ? ValueTag.f64 : ValueTag.f32;
  const width = wide ? 8 : 4;
  let nulls = 0;
  let min = Infinity;
  let max = -Infinity;
  let sawNaN = false;
  let sawNumber = false;
  let constant = true;
  let first: number | null = null;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) {
      nulls++;
      continue;
    }
    const value = values[row] ?? 0;
    if (first === null) first = value;
    else if (constant && !Object.is(first, value)) constant = false;
    if (value !== value) sawNaN = true;
    else {
      sawNumber = true;
      if (value < min) min = value;
      if (value > max) max = value;
    }
    sketch.add(hashFloat(value));
  }
  if (nulls === rows || first === null) return allNullPage(scratch, tag, rows);
  const [low, high] = floatStats(min, max, sawNaN, sawNumber);
  const flags = validityFlags(rows, nulls);
  const start = sectionStart(rows, nulls);
  if (constant) {
    const buffer = scratch.take(start + 8);
    header(buffer, PageEncoding.constant, tag, flags, width, rows, 0);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    if (wide) new Float64Array(buffer, start, 1)[0] = first;
    else new Float32Array(buffer, start, 1)[0] = first;
    return finish(buffer, start + width, PageEncoding.constant, rows, nulls, low, high);
  }
  if (wide) {
    const scaled = encodeScaledFloat(
      values,
      validity,
      rows,
      nulls,
      flags,
      start,
      scratch,
      low,
      high,
    );
    if (scaled !== null) return scaled;
  }
  const size = rows * width;
  const buffer = scratch.take(start + align8(size));
  header(buffer, PageEncoding.plain, tag, flags, width, rows, 0);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  new Uint8Array(buffer, start, size).set(new Uint8Array(values.buffer, values.byteOffset, size));
  return finish(buffer, start + size, PageEncoding.plain, rows, nulls, low, high);
}

const POWERS_OF_TEN = Array.from({ length: SCALED_FLOAT_MAX_EXPONENT + 1 }, (_, e) => 10 ** e);
/** A `scaledFloat` page may patch at most this share of its rows; past it, plain. */
const SCALED_FLOAT_MAX_PATCHES = 1 / 16;

/**
 * The smallest exponent e with `value` = n / 10^e exactly for a safe integer n, the decoder's
 * own formula compared bit for bit (`Object.is`), or −1: −0, NaN, ±Infinity, subnormals and
 * values that only look decimal never qualify.
 */
function scaledExponent(value: number): number {
  if (!Number.isFinite(value) || Object.is(value, -0)) return -1;
  for (let e = 0; e <= SCALED_FLOAT_MAX_EXPONENT; e++) {
    const power = POWERS_OF_TEN[e] ?? 1;
    const n = Math.round(value * power);
    if (Number.isSafeInteger(n) && Object.is(n / power, value)) return e;
  }
  return -1;
}

/**
 * float64 values as frame-of-reference integers over 10^e (page/format.ts `scaledFloat`): the
 * smallest e that covers all but `SCALED_FLOAT_MAX_PATCHES` of the valid rows, each row checked
 * at that e with the decoder's formula; the rows that fail are patched with their own 8 bytes.
 * Null when it would not be smaller than plain.
 */
function encodeScaledFloat(
  values: Float64Array,
  validity: Uint32Array | null,
  rows: number,
  nulls: number,
  flags: number,
  start: number,
  scratch: PageScratch,
  low: number,
  high: number,
): EncodedPage | null {
  const counts = new Int32Array(SCALED_FLOAT_MAX_EXPONENT + 2);
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) continue;
    const e = scaledExponent(values[row] ?? 0);
    const slot = e < 0 ? SCALED_FLOAT_MAX_EXPONENT + 1 : e;
    counts[slot] = (counts[slot] ?? 0) + 1;
  }
  const valid = rows - nulls;
  const allowed = Math.floor(valid * SCALED_FLOAT_MAX_PATCHES);
  let exponent = -1;
  let covered = 0;
  for (let e = 0; e <= SCALED_FLOAT_MAX_EXPONENT; e++) {
    covered += counts[e] ?? 0;
    if (valid - covered <= allowed) {
      exponent = e;
      break;
    }
  }
  if (exponent < 0) return null;
  const power = POWERS_OF_TEN[exponent] ?? 1;
  // The integers at that exponent, checked one by one; the rows that fail are patches.
  const integers = new Float64Array(rows);
  const isPatched = new Uint8Array(rows);
  const patched: number[] = [];
  let min = Infinity;
  let max = -Infinity;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) continue;
    const value = values[row] ?? 0;
    // −0 is always patched: n / 10^e would give it back, but base + delta cannot be −0.
    const n =
      Number.isFinite(value) && !Object.is(value, -0) ? Math.round(value * power) : Number.NaN;
    if (!(Number.isSafeInteger(n) && Object.is(n / power, value))) {
      patched.push(row);
      isPatched[row] = 1;
      if (patched.length > allowed) return null;
      continue;
    }
    integers[row] = n;
    if (n < min) min = n;
    if (n > max) max = n;
  }
  if (min === Infinity) return null;
  const range = max - min;
  if (!scaledFloatSafe(min, range)) return null; // the reader's rule (format.ts)
  const width =
    range === 0 ? 0 : range < 256 ? 1 : range < 65_536 ? 2 : range < 4_294_967_296 ? 4 : 8;
  if (width === 8) return null;
  const deltas = align8(rows * width);
  const patchPositions = align8(patched.length * 4);
  const length = start + 8 + deltas + 8 + patchPositions + patched.length * 8;
  if (length >= start + rows * 8) return null; // not smaller than plain
  const buffer = scratch.take(length);
  header(buffer, PageEncoding.scaledFloat, ValueTag.f64, flags, width, rows, exponent);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  new Float64Array(buffer, start, 1)[0] = min;
  const at = start + 8;
  const out =
    width === 1
      ? new Uint8Array(buffer, at, rows)
      : width === 2
        ? new Uint16Array(buffer, at, rows)
        : width === 4
          ? new Uint32Array(buffer, at, rows)
          : null;
  // Null and patched rows keep delta 0 (the buffer is zeroed); patched rows' bytes follow.
  if (out !== null)
    for (let row = 0; row < rows; row++)
      if (isValidAt(validity, row) && isPatched[row] === 0) out[row] = (integers[row] ?? min) - min;
  const counts32 = new Uint32Array(buffer, at + deltas, 2);
  counts32[0] = patched.length;
  counts32[1] = 0;
  const positions = new Uint32Array(buffer, at + deltas + 8, patched.length);
  const bytes = new Uint8Array(buffer, at + deltas + 8 + patchPositions, patched.length * 8);
  const source = new Uint8Array(values.buffer, values.byteOffset, rows * 8);
  patched.forEach((row, k) => {
    positions[k] = row;
    // The value's own bytes (a NaN keeps its payload), never through a JS number.
    bytes.set(source.subarray(row * 8, row * 8 + 8), k * 8);
  });
  return finish(buffer, length, PageEncoding.scaledFloat, rows, nulls, low, high);
}

// ---------------------------------------------------------------------------------------------
// bool

export function encodeBoolPage(
  values: Uint8Array,
  validity: Uint32Array | null,
  rows: number,
  sketch: DistinctSketch,
  scratch: PageScratch,
): EncodedPage {
  const tag = ValueTag.bool;
  let nulls = 0;
  let trues = 0;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) nulls++;
    else if ((values[row] ?? 0) !== 0) trues++;
  }
  if (nulls === rows) return allNullPage(scratch, tag, rows);
  const valid = rows - nulls;
  const falses = valid - trues;
  if (trues > 0) sketch.add(mix32(1));
  if (falses > 0) sketch.add(mix32(0));
  const flags = validityFlags(rows, nulls);
  const start = sectionStart(rows, nulls);
  const min = falses === 0;
  const max = trues > 0;
  if (trues === 0 || falses === 0) {
    const buffer = scratch.take(start + 8);
    header(buffer, PageEncoding.constant, tag, flags, 1, rows, 0);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    new Uint8Array(buffer, start, 1)[0] = trues > 0 ? 1 : 0;
    return finish(buffer, start + 1, PageEncoding.constant, rows, nulls, min, max);
  }
  const size = validityBytes(rows);
  const buffer = scratch.take(start + align8(size));
  header(buffer, PageEncoding.bitmap, tag, flags, 0, rows, 0);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  const bits = new Uint32Array(buffer, start, size / 4);
  for (let row = 0; row < rows; row++)
    if ((values[row] ?? 0) !== 0 && isValidAt(validity, row))
      bits[row >>> 5] = (bits[row >>> 5] ?? 0) | (1 << (row & 31));
  return finish(buffer, start + size, PageEncoding.bitmap, rows, nulls, min, max);
}

// ---------------------------------------------------------------------------------------------
// interval: months (i32), days (i32), micros (int64 word pairs)

export function encodeIntervalPage(
  months: Int32Array,
  days: Int32Array,
  micros: Int32Array,
  validity: Uint32Array | null,
  rows: number,
  sketch: DistinctSketch,
  scratch: PageScratch,
): EncodedPage {
  const tag = ValueTag.interval;
  const nulls = nullsOf(validity, rows);
  if (nulls === rows) return allNullPage(scratch, tag, rows);
  let constant = true;
  let first = -1;
  let safe = true;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) continue;
    const high = micros[row * 2 + 1] ?? 0;
    const low = micros[row * 2] ?? 0;
    if (!(high > -SAFE_HIGH || (high === -SAFE_HIGH && low >>> 0 >= 1)) || high >= SAFE_HIGH)
      safe = false;
    // PostgreSQL's interval equality: the normalized span (1 mon = 30 days).
    const span =
      ((months[row] ?? 0) * 30 + (days[row] ?? 0)) * 86_400_000_000 +
      (high * 4_294_967_296 + (low >>> 0));
    sketch.add(hashFloat(span));
    if (first < 0) first = row;
    else if (
      constant &&
      ((months[row] ?? 0) !== (months[first] ?? 0) ||
        (days[row] ?? 0) !== (days[first] ?? 0) ||
        low !== (micros[first * 2] ?? 0) ||
        high !== (micros[first * 2 + 1] ?? 0))
    )
      constant = false;
  }
  const flags = validityFlags(rows, nulls) | (safe ? PageFlag.safe : 0);
  const start = sectionStart(rows, nulls);
  if (constant) {
    const buffer = scratch.take(start + 16);
    header(buffer, PageEncoding.constant, tag, flags, 16, rows, 0);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    const out = new Int32Array(buffer, start, 4);
    out[0] = months[first] ?? 0;
    out[1] = days[first] ?? 0;
    out[2] = micros[first * 2] ?? 0;
    out[3] = micros[first * 2 + 1] ?? 0;
    return finish(buffer, start + 16, PageEncoding.constant, rows, nulls, null, null);
  }
  const lane = align8(rows * 4);
  const buffer = scratch.take(start + lane * 2 + rows * 8);
  header(buffer, PageEncoding.plain, tag, flags, 16, rows, 0);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  new Int32Array(buffer, start, rows).set(months.subarray(0, rows));
  new Int32Array(buffer, start + lane, rows).set(days.subarray(0, rows));
  const microsOut = new Int32Array(buffer, start + lane * 2, rows * 2);
  microsOut.set(micros.subarray(0, rows * 2));
  zeroInvalidPairs(microsOut, validity, rows);
  return finish(buffer, start + lane * 2 + rows * 8, PageEncoding.plain, rows, nulls, null, null);
}

// ---------------------------------------------------------------------------------------------
// varlen (text, bytes, json, enum, array) and fixed16 (uuid)

/** Byte-order bounds of a page's values, truncated to `STAT_PREFIX_BYTES`. */
export class ByteBounds {
  #minBytes: Uint8Array | null = null;
  #minStart = 0;
  #minEnd = 0;
  #maxBytes: Uint8Array | null = null;
  #maxStart = 0;
  #maxEnd = 0;

  reset(): void {
    this.#minBytes = null;
    this.#maxBytes = null;
  }

  add(bytes: Uint8Array, start: number, end: number): void {
    if (
      this.#minBytes === null ||
      compareBytes(bytes, start, end, this.#minBytes, this.#minStart, this.#minEnd) < 0
    ) {
      this.#minBytes = bytes;
      this.#minStart = start;
      this.#minEnd = end;
    }
    if (
      this.#maxBytes === null ||
      compareBytes(bytes, start, end, this.#maxBytes, this.#maxStart, this.#maxEnd) > 0
    ) {
      this.#maxBytes = bytes;
      this.#maxStart = start;
      this.#maxEnd = end;
    }
  }

  min(): Uint8Array | null {
    if (this.#minBytes === null) return null;
    const end = Math.min(this.#minEnd, this.#minStart + STAT_PREFIX_BYTES);
    return this.#minBytes.slice(this.#minStart, end);
  }

  /** The maximum, or a conservative upper bound when it is longer than the prefix. */
  max(): Uint8Array | null {
    if (this.#maxBytes === null) return null;
    return upperBound(this.#maxBytes, this.#maxStart, this.#maxEnd);
  }
}

/** The shortest byte string ≤ `STAT_PREFIX_BYTES` that is ≥ the value, or null if none. */
export function upperBound(bytes: Uint8Array, start: number, end: number): Uint8Array | null {
  if (end - start <= STAT_PREFIX_BYTES) return bytes.slice(start, end);
  for (let at = start + STAT_PREFIX_BYTES - 1; at >= start; at--) {
    if ((bytes[at] ?? 0) !== 0xff) {
      const out = bytes.slice(start, at + 1);
      out[at - start] = (out[at - start] ?? 0) + 1;
      return out;
    }
  }
  return null;
}

/** The out-of-line rows of a varlen page (large-value.ts) and what stands in for their bytes. */
export interface LargeRows {
  /** Bit set: the row's slot holds a pointer, not the value. */
  readonly bitmap: Uint32Array;
  /** The value's first `STAT_PREFIX_BYTES + 1` bytes, for the page's bounds. */
  prefix(row: number): Uint8Array;
}

/**
 * A plain varlen page from `bytes` and `offsets` (`offsets[0 … rows]`, absolute into `bytes`).
 * Null slots must be empty (`offsets[i] = offsets[i + 1]`). Constant when every valid value has
 * the same bytes. With `large`, the marked rows hold pointers: the page is plain with a `large`
 * section, their bounds come from `large.prefix`, and the caller has hashed them for the sketch.
 */
export function encodeVarlenPage(
  bytes: Uint8Array,
  offsets: Int32Array,
  validity: Uint32Array | null,
  rows: number,
  sketch: DistinctSketch | null,
  bounds: ByteBounds,
  scratch: PageScratch,
  large: LargeRows | null = null,
): EncodedPage {
  const tag = ValueTag.varlen;
  let nulls = 0;
  let first = -1;
  let constant = large === null;
  bounds.reset();
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) {
      nulls++;
      continue;
    }
    if (large !== null && isValidAt(large.bitmap, row)) {
      const prefix = large.prefix(row);
      bounds.add(prefix, 0, prefix.length);
      continue;
    }
    const start = offsets[row] ?? 0;
    const end = offsets[row + 1] ?? 0;
    sketch?.add(hashBytes(bytes, start, end));
    bounds.add(bytes, start, end);
    if (first < 0) first = row;
    else if (
      constant &&
      compareBytes(bytes, start, end, bytes, offsets[first] ?? 0, offsets[first + 1] ?? 0) !== 0
    )
      constant = false;
  }
  if (nulls === rows) return allNullPage(scratch, tag, rows);
  const flags = validityFlags(rows, nulls) | (large === null ? 0 : PageFlag.large);
  const largeAt = sectionStart(rows, nulls);
  const start = largeAt + (large === null ? 0 : align8(validityBytes(rows)));
  const base = offsets[0] ?? 0;
  if (constant) {
    const valueStart = offsets[first] ?? 0;
    const length = (offsets[first + 1] ?? 0) - valueStart;
    const buffer = scratch.take(start + align8(length));
    header(buffer, PageEncoding.constant, tag, flags, 0, rows, length);
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    new Uint8Array(buffer, start, length).set(bytes.subarray(valueStart, valueStart + length));
    return finish(
      buffer,
      start + length,
      PageEncoding.constant,
      rows,
      nulls,
      bounds.min(),
      bounds.max(),
    );
  }
  const total = (offsets[rows] ?? 0) - base;
  const width = total < 0x1_0000 ? 2 : 4;
  const offsetBytes = align8((rows + 1) * width);
  const buffer = scratch.take(start + offsetBytes + total);
  header(buffer, PageEncoding.plain, tag, flags, width, rows, total);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  if (large !== null) {
    const words = (rows + 31) >>> 5;
    const section = new Uint32Array(buffer, largeAt, words);
    section.set(large.bitmap.subarray(0, words));
    const rest = rows & 31;
    if (rest !== 0) section[words - 1] = (section[words - 1] ?? 0) & ((1 << rest) - 1);
    // A pointer under a null slot means nothing: clear it so the section marks valid rows only.
    if (validity !== null)
      for (let w = 0; w < words; w++) section[w] = (section[w] ?? 0) & (validity[w] ?? 0);
  }
  const out =
    width === 2
      ? new Uint16Array(buffer, start, rows + 1)
      : new Uint32Array(buffer, start, rows + 1);
  for (let row = 0; row <= rows; row++) out[row] = (offsets[row] ?? 0) - base;
  new Uint8Array(buffer, start + offsetBytes, total).set(bytes.subarray(base, base + total));
  return finish(
    buffer,
    start + offsetBytes + total,
    PageEncoding.plain,
    rows,
    nulls,
    bounds.min(),
    bounds.max(),
  );
}

/** A plain or constant uuid page: 16 bytes per row. */
export function encodeFixed16Page(
  bytes: Uint8Array,
  validity: Uint32Array | null,
  rows: number,
  sketch: DistinctSketch | null,
  bounds: ByteBounds,
  scratch: PageScratch,
): EncodedPage {
  const tag = ValueTag.fixed16;
  let nulls = 0;
  let first = -1;
  let constant = true;
  bounds.reset();
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) {
      nulls++;
      continue;
    }
    const at = row * 16;
    sketch?.add(hashBytes(bytes, at, at + 16));
    bounds.add(bytes, at, at + 16);
    if (first < 0) first = row;
    else if (constant && compareBytes(bytes, at, at + 16, bytes, first * 16, first * 16 + 16) !== 0)
      constant = false;
  }
  if (nulls === rows) return allNullPage(scratch, tag, rows);
  const flags = validityFlags(rows, nulls);
  const start = sectionStart(rows, nulls);
  const count = constant ? 1 : rows;
  const offset = constant ? first * 16 : 0;
  const kind = constant ? PageEncoding.constant : PageEncoding.plain;
  const buffer = scratch.take(start + count * 16);
  header(buffer, kind, tag, flags, 16, rows, 0);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  new Uint8Array(buffer, start, count * 16).set(bytes.subarray(offset, offset + count * 16));
  return finish(buffer, start + count * 16, kind, rows, nulls, bounds.min(), bounds.max());
}

/**
 * A dictionary page: `codes[0 … rows)` into the chunk's dictionary. `maxCode` is the largest
 * valid code. Width 0 when every valid row has the same code.
 */
export function encodeDictionaryPage(
  codes: Int32Array,
  validity: Uint32Array | null,
  rows: number,
  tag: number,
  bounds: ByteBounds,
  scratch: PageScratch,
): EncodedPage {
  let nulls = 0;
  let minCode = 0x7fff_ffff;
  let maxCode = -1;
  for (let row = 0; row < rows; row++) {
    if (!isValidAt(validity, row)) {
      nulls++;
      continue;
    }
    const code = codes[row] ?? 0;
    if (code < minCode) minCode = code;
    if (code > maxCode) maxCode = code;
  }
  const flags = validityFlags(rows, nulls);
  const start = sectionStart(rows, nulls);
  if (nulls === rows || minCode === maxCode) {
    const buffer = scratch.take(start);
    header(buffer, PageEncoding.dictionary, tag, flags, 0, rows, Math.max(0, maxCode));
    writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
    return finish(buffer, start, PageEncoding.dictionary, rows, nulls, bounds.min(), bounds.max());
  }
  const width = maxCode < 0x100 ? 1 : maxCode < 0x1_0000 ? 2 : 4;
  const buffer = scratch.take(start + align8(rows * width));
  header(buffer, PageEncoding.dictionary, tag, flags, width, rows, maxCode);
  writeValidity(buffer, PAGE_HEADER_BYTES, validity, rows, nulls);
  if (validity !== null)
    for (let row = 0; row < rows; row++) if (!isValidAt(validity, row)) codes[row] = 0;
  narrowInto(buffer, start, width, codes, rows);
  return finish(
    buffer,
    start + rows * width,
    PageEncoding.dictionary,
    rows,
    nulls,
    bounds.min(),
    bounds.max(),
  );
}

/**
 * The chunk's dictionary page: always plain, no validity, u32 offsets for varlen (so a reader
 * views the entries in place as an Int32Array) or 16 bytes per uuid entry.
 */
export function encodeDictionaryEntries(
  bytes: Uint8Array,
  offsets: Int32Array | null,
  entries: number,
  tag: number,
  scratch: PageScratch,
): Uint8Array {
  const start = PAGE_HEADER_BYTES;
  if (tag === ValueTag.fixed16) {
    const buffer = scratch.take(start + entries * 16);
    header(buffer, PageEncoding.plain, tag, 0, 16, entries, 0);
    new Uint8Array(buffer, start, entries * 16).set(bytes.subarray(0, entries * 16));
    return new Uint8Array(buffer, 0, start + entries * 16);
  }
  const total = offsets?.[entries] ?? 0;
  const offsetBytes = align8((entries + 1) * 4);
  const buffer = scratch.take(start + offsetBytes + total);
  header(buffer, PageEncoding.plain, tag, 0, 4, entries, total);
  if (offsets !== null)
    new Int32Array(buffer, start, entries + 1).set(offsets.subarray(0, entries + 1));
  new Uint8Array(buffer, start + offsetBytes, total).set(bytes.subarray(0, total));
  return new Uint8Array(buffer, 0, start + offsetBytes + total);
}
