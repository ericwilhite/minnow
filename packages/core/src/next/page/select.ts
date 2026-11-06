/**
 * Evaluates a compiled predicate (predicate.ts) on one parsed page, writing the group positions
 * of the qualifying rows, ascending, into a caller's `Uint16Array`. Either over a row range of the
 * page, or over a selection (refining it).
 *
 * The page is not decoded where the encoding allows it:
 * - all-null and constant pages are decided once;
 * - dictionary pages test each row's code against a bitmap of the matching dictionary entries,
 *   built once per chunk (`dictionaryMatches`);
 * - frame-of-reference pages compare the stored unsigned deltas with the predicate's bounds,
 *   translated once per page into delta space;
 * - plain int32, float, bool and varlen pages compare the stored values in place.
 * Delta pages, plain int64 and decimal pages, 128-bit decimals and decimal specials decode the
 * needed rows into the caller's `SelectScratch` (reused, no per-row allocation) and compare
 * there: int64 as word pairs, 128-bit coefficients as limbs. Bounds become numbers once per
 * page; no row loop does BigInt work.
 */
import type { PageEntry } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import type { FlatVector } from "../vector/vector.js";
import {
  checkDictionaryCodes,
  decodeRange,
  gatherRows,
  isLarge,
  largeLength,
  type LargeResolver,
  type PageDictionary,
  type PageView,
  ValueNotLoadedError,
} from "./decode.js";
import { compareBytes } from "../vector/varlen.js";
import { ValueTag } from "./format.js";
import { compareFloat, type CompiledPredicate } from "./predicate.js";

type Rows = Uint16Array | Uint32Array | Int32Array;

const TWO32 = 4_294_967_296;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const INT128_MIN = -(2n ** 127n);
const INT128_MAX = 2n ** 127n - 1n;

/** Decode buffers for the pages that are compared after decoding; grown by the owner (charged). */
export interface SelectScratch {
  /** int64 coefficients or values: 8 bytes per row. */
  int64: BigInt64Array;
  /** 128-bit coefficients: 4 u32 limbs per row. */
  limbs: Uint32Array;
  int32: Int32Array;
  specials: Uint8Array;
  /** The decoder writes validity too (pages with nulls need a target for it). */
  validity: Uint32Array;
  /** Positions, for masks of pages without a mask kernel. */
  positions: Uint16Array;
}

/** Bytes a scratch for `rows` rows takes. */
export const scratchBytes = (rows: number): number =>
  rows * (8 + 16 + 4 + 1 + 2) + ((rows + 31) >>> 5) * 4;

export function newScratch(rows: number): SelectScratch {
  return {
    int64: new BigInt64Array(rows),
    limbs: new Uint32Array(rows * 4),
    int32: new Int32Array(rows),
    specials: new Uint8Array(rows),
    validity: new Uint32Array((rows + 31) >>> 5),
    positions: new Uint16Array(rows),
  };
}

// ── Value tests ──────────────────────────────────────────────────────────────────────────────

/** Integer bounds as f64 pairs (exact below 2^53), for int32 values and FOR deltas. */
interface NumberRanges {
  readonly lows: Float64Array;
  readonly highs: Float64Array;
  readonly count: number;
}

function numberRanges(
  compiled: CompiledPredicate,
  offset: bigint,
  min: bigint,
  max: bigint,
): NumberRanges {
  const lows: number[] = [];
  const highs: number[] = [];
  for (const [low, high] of compiled.ints) {
    const a = low === null ? min : low - offset;
    const b = high === null ? max : high - offset;
    const from = a < min ? min : a;
    const to = b > max ? max : b;
    if (from > to) continue;
    lows.push(Number(from));
    highs.push(Number(to));
  }
  return { lows: Float64Array.from(lows), highs: Float64Array.from(highs), count: lows.length };
}

function inRanges(value: number, ranges: NumberRanges): boolean {
  const { lows, highs, count } = ranges;
  if (count <= 4) {
    for (let k = 0; k < count; k++)
      if (value >= (lows[k] ?? 0) && value <= (highs[k] ?? 0)) return true;
    return false;
  }
  let low = 0;
  let high = count - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    if (value < (lows[middle] ?? 0)) high = middle - 1;
    else if (value > (highs[middle] ?? 0)) low = middle + 1;
    else return true;
  }
  return false;
}

/** int64 bounds as (high, low) word pairs, compared without BigInts. */
interface WordRanges {
  /** [lowHigh, lowLow, highHigh, highLow] per range; low words unsigned. */
  readonly words: Float64Array;
  readonly count: number;
}

function wordRanges(compiled: CompiledPredicate): WordRanges {
  const out: number[] = [];
  for (const [low, high] of compiled.ints) {
    const a = low === null || low < INT64_MIN ? INT64_MIN : low;
    const b = high === null || high > INT64_MAX ? INT64_MAX : high;
    if (a > b) continue;
    out.push(
      Number(a >> 32n),
      Number(BigInt.asUintN(32, a)),
      Number(b >> 32n),
      Number(BigInt.asUintN(32, b)),
    );
  }
  return { words: Float64Array.from(out), count: out.length / 4 };
}

function inWordRanges(high: number, low: number, ranges: WordRanges): boolean {
  const words = ranges.words;
  for (let k = 0; k < ranges.count; k++) {
    const aHigh = words[k * 4] ?? 0;
    const aLow = words[k * 4 + 1] ?? 0;
    const bHigh = words[k * 4 + 2] ?? 0;
    const bLow = words[k * 4 + 3] ?? 0;
    const atLeast = high > aHigh || (high === aHigh && low >= aLow);
    const atMost = high < bHigh || (high === bHigh && low <= bLow);
    if (atLeast && atMost) return true;
  }
  return false;
}

/**
 * 128-bit bounds as numeric limbs, prepared once per page (review B2-p2d #2): per range, the
 * clipped low and high bounds as [signed limb 3, limb 2, limb 1, limb 0] (lower limbs
 * unsigned), so the row loop compares numbers, never BigInts.
 */
interface LimbRanges {
  readonly limbs: Float64Array;
  readonly count: number;
}

function limbsOf(value: bigint, out: number[]): void {
  out.push(
    Number(BigInt.asIntN(32, value >> 96n)),
    Number(BigInt.asUintN(32, value >> 64n)),
    Number(BigInt.asUintN(32, value >> 32n)),
    Number(BigInt.asUintN(32, value)),
  );
}

function limbRanges(compiled: CompiledPredicate): LimbRanges {
  const out: number[] = [];
  for (const [low, high] of compiled.ints) {
    const a = low === null || low < INT128_MIN ? INT128_MIN : low;
    const b = high === null || high > INT128_MAX ? INT128_MAX : high;
    if (a > b) continue;
    limbsOf(a, out);
    limbsOf(b, out);
  }
  return { limbs: Float64Array.from(out), count: out.length / 8 };
}

/** −1, 0, 1: row `row`'s coefficient (4 u32 limbs, low first) against bound `at` of `bounds`. */
function compareLimbs(limbs: Uint32Array, row: number, bounds: Float64Array, at: number): number {
  const top = (limbs[row * 4 + 3] ?? 0) | 0;
  const boundTop = bounds[at] ?? 0;
  if (top !== boundTop) return top < boundTop ? -1 : 1;
  for (let limb = 2; limb >= 0; limb--) {
    const value = limbs[row * 4 + limb] ?? 0;
    const other = bounds[at + 3 - limb] ?? 0;
    if (value !== other) return value < other ? -1 : 1;
  }
  return 0;
}

function inLimbRanges(limbs: Uint32Array, row: number, ranges: LimbRanges): boolean {
  const bounds = ranges.limbs;
  for (let k = 0; k < ranges.count; k++)
    if (
      compareLimbs(limbs, row, bounds, k * 8) >= 0 &&
      compareLimbs(limbs, row, bounds, k * 8 + 4) <= 0
    )
      return true;
  return false;
}

function floatMatches(value: number, compiled: CompiledPredicate): boolean {
  if (value !== value) return compiled.specials.nan;
  for (const { low, high } of compiled.floats) {
    if (low !== null) {
      const order = compareFloat(value, low.value);
      if (order < 0 || (order === 0 && !low.inclusive)) continue;
    }
    if (high !== null) {
      const order = compareFloat(value, high.value);
      if (order > 0 || (order === 0 && !high.inclusive)) continue;
    }
    return true;
  }
  return false;
}

function bytesMatch(
  source: Uint8Array,
  start: number,
  end: number,
  compiled: CompiledPredicate,
): boolean {
  for (const { low, high } of compiled.bytes) {
    if (low !== null) {
      const order = compareBytes(source, start, end, low.value, 0, low.value.length);
      if (order < 0 || (order === 0 && !low.inclusive)) continue;
    }
    if (high !== null) {
      const order = compareBytes(source, start, end, high.value, 0, high.value.length);
      if (order > 0 || (order === 0 && !high.inclusive)) continue;
    }
    return true;
  }
  return false;
}

/** Compares an out-of-line value (its parts, in order) with `constant`. */
function compareParts(parts: readonly Uint8Array[], constant: Uint8Array): number {
  let at = 0;
  for (const part of parts) {
    const take = Math.min(part.length, constant.length - at);
    if (take > 0) {
      const order = compareBytes(part, 0, take, constant, at, at + take);
      if (order !== 0) return order;
    }
    if (part.length > take) return 1; // the value is longer than the constant
    at += take;
  }
  return at < constant.length ? -1 : 0;
}

function partsMatch(parts: readonly Uint8Array[], compiled: CompiledPredicate): boolean {
  for (const { low, high } of compiled.bytes) {
    if (low !== null) {
      const order = compareParts(parts, low.value);
      if (order < 0 || (order === 0 && !low.inclusive)) continue;
    }
    if (high !== null) {
      const order = compareParts(parts, high.value);
      if (order > 0 || (order === 0 && !high.inclusive)) continue;
    }
    return true;
  }
  return false;
}

/**
 * The dictionary entries (by code) a byte predicate matches, as bits: built once per chunk
 * (the reader keeps it, charged, for its chunk's lifetime).
 */
export function dictionaryMatches(
  dictionary: PageDictionary,
  compiled: CompiledPredicate,
): Uint32Array {
  const bits = new Uint32Array((dictionary.size + 31) >>> 5);
  for (let code = 0; code < dictionary.size; code++) {
    const offsets = dictionary.offsets;
    const start = offsets === null ? code * 16 : (offsets[code] ?? 0);
    const end = offsets === null ? start + 16 : (offsets[code + 1] ?? 0);
    if (bytesMatch(dictionary.bytes, start, end, compiled))
      bits[code >>> 5] = (bits[code >>> 5] ?? 0) | (1 << (code & 31));
  }
  return bits;
}

// ── Page evaluation ──────────────────────────────────────────────────────────────────────────

export interface SelectContext {
  readonly view: PageView;
  readonly entry: PageEntry;
  readonly compiled: CompiledPredicate;
  /** The chunk's matching dictionary codes (dictionary pages). */
  readonly codes: Uint32Array | null;
  readonly dictionary: PageDictionary | null;
  readonly scratch: SelectScratch;
  /** Loaded out-of-line values (varlen pages with large rows, ordering comparisons). */
  readonly values: LargeResolver | null;
  readonly type: FlatVector["type"];
}

const bit = (words: Uint32Array, index: number): boolean =>
  (((words[index >>> 5] ?? 0) >>> (index & 31)) & 1) === 1;

/**
 * Writes the qualifying rows of the page to `out[at …]` as group positions; returns how many.
 * `rows === null`: the page rows `[from, to)`. Otherwise `rows[from … to)` (group positions,
 * ascending, all inside the page).
 */
export function selectPage(
  context: SelectContext,
  rows: Rows | null,
  from: number,
  to: number,
  out: Uint16Array,
  at: number,
): number {
  const { view, entry, compiled } = context;
  const first = entry.firstRow;
  const validity = view.validity;
  let count = 0;
  // `row` is page-relative; `position` the group position written out.
  const emit = (row: number): void => {
    out[at + count++] = first + row;
  };
  const each = (test: (row: number, k: number) => boolean): void => {
    for (let k = from; k < to; k++) {
      const row = rows === null ? k : (rows[k] ?? 0) - first;
      if (view.allNull || (validity !== null && !bit(validity, row))) continue;
      if (test(row, k - from)) emit(row);
    }
  };
  if (compiled.nulls !== null) {
    const wantNull = compiled.nulls === "isNull";
    for (let k = from; k < to; k++) {
      const row = rows === null ? k : (rows[k] ?? 0) - first;
      const isNull = view.allNull || (validity !== null && !bit(validity, row));
      if (isNull === wantNull) emit(row);
    }
    return count;
  }
  if (compiled.empty || view.allNull) return 0;
  // A constant decimal page may still carry per-row specials: compared after decoding.
  if (view.kind === PageEncoding.constant && view.specials === null) {
    const match = constantMatches(context);
    if (match) each(() => true);
    return count;
  }
  switch (view.tag) {
    case ValueTag.bool: {
      const bits = view.plain as Uint32Array;
      const ranges = numberRanges(compiled, 0n, 0n, 1n);
      const matchFalse = inRanges(0, ranges);
      const matchTrue = inRanges(1, ranges);
      if (matchFalse && matchTrue) each(() => true);
      else if (matchFalse || matchTrue) each((row) => bit(bits, row) === matchTrue);
      return count;
    }
    case ValueTag.varlen:
    case ValueTag.fixed16:
      if (view.kind === PageEncoding.dictionary)
        return dictionaryPage(context, rows, from, to, out, at);
      if (view.tag === ValueTag.fixed16) {
        const bytes = view.bytes;
        const base = view.start;
        each((row) => bytesMatch(bytes, base + row * 16, base + row * 16 + 16, compiled));
        return count;
      }
      return varlenPage(context, rows, from, to, out, at);
    case ValueTag.f64:
    case ValueTag.f32: {
      const values = view.plain as Float64Array | Float32Array;
      const single = compiled.floats.length === 1 ? compiled.floats[0] : undefined;
      if (single?.low != null && single.high !== null && !compiled.specials.nan) {
        // A closed or open finite range, NaN excluded: two comparisons per value.
        const low = single.low.value;
        const high = single.high.value;
        const lowIn = single.low.inclusive;
        const highIn = single.high.inclusive;
        each((row) => {
          const value = values[row] ?? 0;
          return (lowIn ? value >= low : value > low) && (highIn ? value <= high : value < high);
        });
      } else each((row) => floatMatches(values[row] ?? 0, compiled));
      return count;
    }
    case ValueTag.i32:
      if (view.kind === PageEncoding.plain) {
        const values = view.plain as Int32Array;
        const ranges = numberRanges(compiled, 0n, -(2n ** 31n), 2n ** 31n - 1n);
        return rangeKernel(context, rows, from, to, out, at, values, ranges, 0);
      }
      if (view.kind === PageEncoding.frameOfReference && view.checkpoints === null)
        return forPage(context, rows, from, to, out, at);
      return decodedPage(context, rows, from, to, out, at);
    case ValueTag.i64:
    case ValueTag.dec64:
      if (
        view.kind === PageEncoding.frameOfReference &&
        view.checkpoints === null &&
        view.specials === null
      )
        return forPage(context, rows, from, to, out, at);
      return decodedPage(context, rows, from, to, out, at);
    default:
      return decodedPage(context, rows, from, to, out, at);
  }
}

/** One loop for a value array against number ranges: the hot path of plain and FOR pages. */
function rangeKernel(
  context: SelectContext,
  rows: Rows | null,
  from: number,
  to: number,
  out: Uint16Array,
  at: number,
  values: Int32Array | Uint8Array | Uint16Array | Uint32Array | null,
  ranges: NumberRanges,
  constant: number,
): number {
  const { view, entry } = context;
  const first = entry.firstRow;
  const validity = view.validity;
  let count = 0;
  if (ranges.count === 0) return 0;
  if (ranges.count === 1 && rows === null && validity === null && values !== null) {
    // The hot path: one range over a page range with no nulls, one loop per array type (a loop
    // over a union of typed arrays runs several times slower).
    const low = ranges.lows[0] ?? 0;
    const high = ranges.highs[0] ?? 0;
    if (values instanceof Uint8Array) return scan8(values, from, to, low, high, out, at, first);
    if (values instanceof Uint16Array) return scan16(values, from, to, low, high, out, at, first);
    if (values instanceof Uint32Array) return scan32(values, from, to, low, high, out, at, first);
    return scanI32(values, from, to, low, high, out, at, first);
  }
  if (ranges.count === 1) {
    const low = ranges.lows[0] ?? 0;
    const high = ranges.highs[0] ?? 0;
    for (let k = from; k < to; k++) {
      const row = rows === null ? k : (rows[k] ?? 0) - first;
      if (validity !== null && (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0) continue;
      const value = values === null ? constant : (values[row] ?? 0);
      if (value >= low && value <= high) out[at + count++] = first + row;
    }
    return count;
  }
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - first;
    if (validity !== null && (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0) continue;
    if (inRanges(values === null ? constant : (values[row] ?? 0), ranges))
      out[at + count++] = first + row;
  }
  return count;
}

function scan8(
  values: Uint8Array,
  from: number,
  to: number,
  low: number,
  high: number,
  out: Uint16Array,
  at: number,
  first: number,
): number {
  let count = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    if (value >= low && value <= high) out[at + count++] = first + row;
  }
  return count;
}
function scan16(
  values: Uint16Array,
  from: number,
  to: number,
  low: number,
  high: number,
  out: Uint16Array,
  at: number,
  first: number,
): number {
  let count = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    if (value >= low && value <= high) out[at + count++] = first + row;
  }
  return count;
}
function scan32(
  values: Uint32Array,
  from: number,
  to: number,
  low: number,
  high: number,
  out: Uint16Array,
  at: number,
  first: number,
): number {
  let count = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    if (value >= low && value <= high) out[at + count++] = first + row;
  }
  return count;
}
function scanI32(
  values: Int32Array,
  from: number,
  to: number,
  low: number,
  high: number,
  out: Uint16Array,
  at: number,
  first: number,
): number {
  let count = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    if (value >= low && value <= high) out[at + count++] = first + row;
  }
  return count;
}

/** Frame of reference: value = base + delta; the bounds move into delta space once. */
function forPage(
  context: SelectContext,
  rows: Rows | null,
  from: number,
  to: number,
  out: Uint16Array,
  at: number,
): number {
  const { view, compiled } = context;
  const base = BigInt(view.baseHigh) * BigInt(TWO32) + BigInt(view.baseLow >>> 0);
  const maxDelta = view.width === 0 ? 0n : 2n ** BigInt(view.width * 8) - 1n;
  const ranges = numberRanges(compiled, base, 0n, maxDelta);
  return rangeKernel(context, rows, from, to, out, at, view.unsigned, ranges, 0);
}

function dictionaryPage(
  context: SelectContext,
  rows: Rows | null,
  from: number,
  to: number,
  out: Uint16Array,
  at: number,
): number {
  const { view, entry } = context;
  const codes = context.codes;
  if (codes === null || context.dictionary === null)
    throw new RangeError("A dictionary page needs its chunk's dictionary and code matches");
  // A page handed in directly is checked here; the reader's pages were at parse (one check).
  checkDictionaryCodes(view, context.dictionary.size);
  const first = entry.firstRow;
  const validity = view.validity;
  const stored = view.unsigned;
  let count = 0;
  if (stored === null) {
    // Width 0: every row has code `aux`.
    if (!bit(codes, view.aux)) return 0;
    for (let k = from; k < to; k++) {
      const row = rows === null ? k : (rows[k] ?? 0) - first;
      if (validity === null || bit(validity, row)) out[at + count++] = first + row;
    }
    return count;
  }
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - first;
    if (validity !== null && (((validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0) continue;
    const code = stored[row] ?? 0;
    if ((((codes[code >>> 5] ?? 0) >>> (code & 31)) & 1) === 1) out[at + count++] = first + row;
  }
  return count;
}

function varlenPage(
  context: SelectContext,
  rows: Rows | null,
  from: number,
  to: number,
  out: Uint16Array,
  at: number,
): number {
  const { view, entry, compiled } = context;
  const first = entry.firstRow;
  const validity = view.validity;
  const offsets = view.unsigned ?? new Uint32Array(1);
  const data = view.data ?? new Uint8Array(0);
  // Out-of-line rows: equality (and IN, <>) is decided by length, as every constant is shorter
  // than an out-of-line value; an ordering comparison needs the value loaded.
  const equality = compiled.bytes.every(
    ({ low, high }) =>
      low !== null && high !== null && low.inclusive && high.inclusive && low.value === high.value,
  );
  const largeMatches = equality ? false : null;
  let longest = 0;
  for (const { low, high } of compiled.bytes)
    longest = Math.max(longest, low?.value.length ?? 0, high?.value.length ?? 0);
  let count = 0;
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - first;
    if (validity !== null && !bit(validity, row)) continue;
    let match: boolean;
    if (view.large !== null && isLarge(view, row)) {
      if (largeMatches !== null && largeLength(view, row) > longest) match = largeMatches;
      else {
        const parts = context.values?.(row) ?? null;
        if (parts === null) throw new ValueNotLoadedError();
        match = partsMatch(parts, compiled);
      }
    } else match = bytesMatch(data, offsets[row] ?? 0, offsets[row + 1] ?? 0, compiled);
    if (match) out[at + count++] = first + row;
  }
  return count;
}

/** The value of a constant page, tested once. */
function constantMatches(context: SelectContext): boolean {
  const { view, compiled } = context;
  switch (view.tag) {
    case ValueTag.bool:
      return inRanges(
        (view.bytes[view.start] ?? 0) === 0 ? 0 : 1,
        numberRanges(compiled, 0n, 0n, 1n),
      );
    case ValueTag.i32:
      return inRanges(
        new Int32Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0,
        numberRanges(compiled, 0n, -(2n ** 31n), 2n ** 31n - 1n),
      );
    case ValueTag.i64:
      return inWordRanges(view.baseHigh, view.baseLow >>> 0, wordRanges(compiled));
    case ValueTag.f64:
      return floatMatches(
        new Float64Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0,
        compiled,
      );
    case ValueTag.f32:
      return floatMatches(
        new Float32Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0,
        compiled,
      );
    case ValueTag.varlen:
      return bytesMatch(view.bytes, view.start, view.start + view.aux, compiled);
    case ValueTag.fixed16:
      return bytesMatch(view.bytes, view.start, view.start + 16, compiled);
    default: {
      // Decimals: decode the one value.
      const scratch = context.scratch;
      const n = decodeInto(context, null, 0, 1);
      void n;
      return decodedMatches(context, 0, scratch);
    }
  }
}

/** Decodes page rows `[from, to)` (or `rows[from … to)`) into the scratch from slot 0. */
function decodeInto(context: SelectContext, rows: Rows | null, from: number, to: number): number {
  const { view, entry, scratch } = context;
  const count = to - from;
  const target = scratchTarget(context, count);
  if (rows === null) decodeRange(view, from, to, target, 0, context.dictionary);
  else gatherRows(view, rows, from, to, entry.firstRow, target, 0, context.dictionary);
  void scratch;
  return count;
}

function scratchTarget(context: SelectContext, count: number): FlatVector {
  const { view, scratch, type } = context;
  if (scratch.int32.length < count) throw new RangeError("The select scratch is too small");
  if (view.tag === ValueTag.i32)
    return {
      form: "flat",
      type,
      length: count,
      validity: scratch.validity,
      data: { layout: "i32", values: scratch.int32 },
    };
  if (view.tag === ValueTag.i64)
    return {
      form: "flat",
      type,
      length: count,
      validity: scratch.validity,
      data: { layout: "int64", values: { repr: "int64", values: scratch.int64 } },
    };
  // Decimals (int64 tier or 128-bit limbs).
  return {
    form: "flat",
    type,
    length: count,
    validity: scratch.validity,
    data: {
      layout: "decimal",
      scale: view.scale,
      scales: null,
      coefficients:
        view.tag === ValueTag.dec128
          ? { repr: "int128", limbs: scratch.limbs }
          : { repr: "int64", values: scratch.int64 },
      specials: view.specials === null ? null : scratch.specials,
    },
  };
}

/** Whether decoded scratch slot `slot` matches. */
function decodedMatches(context: SelectContext, slot: number, scratch: SelectScratch): boolean {
  const { view, compiled } = context;
  if (view.tag === ValueTag.i32)
    return inRanges(
      scratch.int32[slot] ?? 0,
      numberRanges(compiled, 0n, -(2n ** 31n), 2n ** 31n - 1n),
    );
  if (view.specials !== null) {
    const special = scratch.specials[slot] ?? 0;
    if (special !== 0)
      return special === 1
        ? compiled.specials.nan
        : special === 2
          ? compiled.specials.positive
          : compiled.specials.negative;
  }
  if (view.tag === ValueTag.dec128) return inLimbRanges(scratch.limbs, slot, limbRanges(compiled));
  const words = new Int32Array(
    scratch.int64.buffer,
    scratch.int64.byteOffset,
    scratch.int64.length * 2,
  );
  return inWordRanges(words[slot * 2 + 1] ?? 0, (words[slot * 2] ?? 0) >>> 0, wordRanges(compiled));
}

/** Pages compared after decoding: delta, plain int64 / decimal, 128-bit, decimal specials. */
function decodedPage(
  context: SelectContext,
  rows: Rows | null,
  from: number,
  to: number,
  out: Uint16Array,
  at: number,
): number {
  const { view, entry, compiled, scratch } = context;
  const first = entry.firstRow;
  const validity = view.validity;
  const chunk = Math.max(1, scratch.int32.length);
  // Bounds prepared once per page.
  const int32 =
    view.tag === ValueTag.i32 ? numberRanges(compiled, 0n, -(2n ** 31n), 2n ** 31n - 1n) : null;
  const words =
    view.tag === ValueTag.i64 || view.tag === ValueTag.dec64 ? wordRanges(compiled) : null;
  const limbs = view.tag === ValueTag.dec128 ? limbRanges(compiled) : null;
  const pairs = new Int32Array(
    scratch.int64.buffer,
    scratch.int64.byteOffset,
    scratch.int64.length * 2,
  );
  let count = 0;
  for (let start = from; start < to; start += chunk) {
    const end = Math.min(to, start + chunk);
    decodeInto(context, rows, start, end);
    for (let k = start; k < end; k++) {
      const row = rows === null ? k : (rows[k] ?? 0) - first;
      if (validity !== null && !bit(validity, row)) continue;
      const slot = k - start;
      let match: boolean;
      const special = view.specials === null ? 0 : (scratch.specials[slot] ?? 0);
      if (special !== 0)
        match =
          special === 1
            ? compiled.specials.nan
            : special === 2
              ? compiled.specials.positive
              : compiled.specials.negative;
      else if (int32 !== null) match = inRanges(scratch.int32[slot] ?? 0, int32);
      else if (words !== null && view.tag !== ValueTag.dec128)
        match = inWordRanges(pairs[slot * 2 + 1] ?? 0, (pairs[slot * 2] ?? 0) >>> 0, words);
      else match = limbs !== null && inLimbRanges(scratch.limbs, slot, limbs);
      if (match) out[at + count++] = first + row;
    }
  }
  return count;
}

// ── Masks (the dense path) ───────────────────────────────────────────────────────────────────

/**
 * Writes 1 to `mask[at + k]` when page row `from + k` satisfies the predicate and 0 otherwise,
 * for `k < to − from`, and returns how many are 1. Null rows are 0 (except for `isNull`). A
 * branch-free consumer (`sum += value & -mask[k]`) then filters and aggregates in one pass.
 * Measured against Uint32 bit words (scripts/pushdown-bench.mts `--mask`): bytes are as cheap
 * to write (≈ 3 ns/row) and up to 2× cheaper to consume.
 */
export function maskPage(
  context: SelectContext,
  from: number,
  to: number,
  mask: Uint8Array,
  at: number,
): number {
  const { view, compiled } = context;
  const validity = view.validity;
  const count = to - from;
  if (compiled.nulls !== null || compiled.empty || view.allNull) {
    const wantNull = compiled.nulls === "isNull";
    if (compiled.nulls === null) {
      mask.fill(0, at, at + count);
      return 0;
    }
    if (view.allNull) {
      mask.fill(wantNull ? 1 : 0, at, at + count);
      return wantNull ? count : 0;
    }
    let ones = 0;
    for (let k = 0; k < count; k++) {
      const row = from + k;
      const valid = validity === null || bit(validity, row);
      const one = valid !== wantNull ? 1 : 0;
      mask[at + k] = one;
      ones += one;
    }
    return ones;
  }
  let ones = -1;
  const ranges = maskRanges(context);
  if (ranges !== null && ranges.count === 1) {
    const low = ranges.lows[0] ?? 0;
    const high = ranges.highs[0] ?? 0;
    const values = ranges.values;
    if (values instanceof Uint8Array) ones = mask8(values, from, to, low, high, mask, at);
    else if (values instanceof Uint16Array) ones = mask16(values, from, to, low, high, mask, at);
    else if (values instanceof Uint32Array) ones = mask32(values, from, to, low, high, mask, at);
    else if (values instanceof Int32Array) ones = maskI32(values, from, to, low, high, mask, at);
  } else if (
    view.kind === PageEncoding.dictionary &&
    view.unsigned !== null &&
    context.codes !== null &&
    context.dictionary !== null
  ) {
    checkDictionaryCodes(view, context.dictionary.size); // as selectPage does (one check)
    ones = maskCodes(view.unsigned, context.codes, from, to, mask, at);
  } else if (
    (view.tag === ValueTag.f64 || view.tag === ValueTag.f32) &&
    view.kind === PageEncoding.plain
  ) {
    const single = compiled.floats.length === 1 ? compiled.floats[0] : undefined;
    if (
      single?.low != null &&
      single.high !== null &&
      !compiled.specials.nan &&
      single.low.inclusive &&
      single.high.inclusive
    )
      ones =
        view.plain instanceof Float64Array
          ? maskF64(view.plain, from, to, single.low.value, single.high.value, mask, at)
          : maskF32(
              view.plain as Float32Array,
              from,
              to,
              single.low.value,
              single.high.value,
              mask,
              at,
            );
  }
  if (ones < 0) return maskFromPositions(context, from, to, mask, at);
  if (validity === null) return ones;
  // Clear what the value kernels set under null rows.
  for (let k = 0; k < count; k++) {
    const row = from + k;
    if (!bit(validity, row) && mask[at + k] === 1) {
      mask[at + k] = 0;
      ones--;
    }
  }
  return ones;
}

/** The page's values and its predicate's bounds when a range kernel applies (plain int32, FOR). */
function maskRanges(
  context: SelectContext,
): (NumberRanges & { values: Int32Array | Uint8Array | Uint16Array | Uint32Array }) | null {
  const { view, compiled } = context;
  if (view.tag === ValueTag.i32 && view.kind === PageEncoding.plain)
    return {
      ...numberRanges(compiled, 0n, -(2n ** 31n), 2n ** 31n - 1n),
      values: view.plain as Int32Array,
    };
  const integer =
    view.tag === ValueTag.i32 || view.tag === ValueTag.i64 || view.tag === ValueTag.dec64;
  if (
    integer &&
    view.kind === PageEncoding.frameOfReference &&
    view.checkpoints === null &&
    view.specials === null &&
    view.unsigned !== null
  ) {
    const base = BigInt(view.baseHigh) * BigInt(TWO32) + BigInt(view.baseLow >>> 0);
    const maxDelta = 2n ** BigInt(view.width * 8) - 1n;
    return { ...numberRanges(compiled, base, 0n, maxDelta), values: view.unsigned };
  }
  return null;
}

/** Pages with no mask kernel: their positions, spread into the mask. */
function maskFromPositions(
  context: SelectContext,
  from: number,
  to: number,
  mask: Uint8Array,
  at: number,
): number {
  const positions = context.scratch.positions;
  const first = context.entry.firstRow;
  mask.fill(0, at, at + (to - from));
  let ones = 0;
  for (let start = from; start < to; start += positions.length) {
    const end = Math.min(to, start + positions.length);
    const count = selectPage(context, null, start, end, positions, 0);
    for (let k = 0; k < count; k++) mask[at + (positions[k] ?? 0) - first - from] = 1;
    ones += count;
  }
  return ones;
}

// One loop per array type, ternary stores (a `+(a) & +(b)` store into a Uint8Array measured
// several times slower).
function mask8(
  values: Uint8Array,
  from: number,
  to: number,
  low: number,
  high: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    const one = value >= low && value <= high ? 1 : 0;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
function mask16(
  values: Uint16Array,
  from: number,
  to: number,
  low: number,
  high: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    const one = value >= low && value <= high ? 1 : 0;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
function mask32(
  values: Uint32Array,
  from: number,
  to: number,
  low: number,
  high: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    const one = value >= low && value <= high ? 1 : 0;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
function maskI32(
  values: Int32Array,
  from: number,
  to: number,
  low: number,
  high: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    const one = value >= low && value <= high ? 1 : 0;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
function maskF64(
  values: Float64Array,
  from: number,
  to: number,
  low: number,
  high: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    const one = value >= low && value <= high ? 1 : 0;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
function maskF32(
  values: Float32Array,
  from: number,
  to: number,
  low: number,
  high: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const value = values[row] ?? 0;
    const one = value >= low && value <= high ? 1 : 0;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
function maskCodes(
  codes: Uint8Array | Uint16Array | Uint32Array,
  matches: Uint32Array,
  from: number,
  to: number,
  mask: Uint8Array,
  at: number,
): number {
  let ones = 0;
  for (let row = from; row < to; row++) {
    const code = codes[row] ?? 0;
    const one = ((matches[code >>> 5] ?? 0) >>> (code & 31)) & 1;
    mask[at + row - from] = one;
    ones += one;
  }
  return ones;
}
