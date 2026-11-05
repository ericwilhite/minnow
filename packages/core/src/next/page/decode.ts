/**
 * Page decoders.
 *
 * `parsePage` runs once per page read from storage (after its checksum): it checks the header
 * against the directory entry and the page length, counts the validity bits against the entry's
 * null count, and checks offsets, codes and specials for range, so a decoder can never read past
 * a section. The `PageView` it returns is trusted from then on: a cached view decodes with no
 * further checks (verify once).
 *
 * Decoders write into caller-owned vector buffers at a caller-chosen slot (`at`):
 * - `decodeRange` / `gatherRows`: values (and validity) of a row range, or of selected rows,
 *   into a flat vector. Dictionary pages materialize their entries' bytes (no JS strings).
 * - `decodeCodes` / `gatherCodes`: a dictionary page's codes, for a dictionary vector.
 * Nothing allocates per value. A target without validity is allowed only for pages without
 * nulls (`RangeError` otherwise: the caller sizes and allocates from the directory statistics).
 * An int64-family target in the `safe` representation is allowed only for pages flagged safe.
 */
import type { PageEntry } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import type { FlatVector, Int64Values, Validity, VectorData } from "../vector/vector.js";
import { copyBits, countBits, fillBits, gatherBits, unpackBools } from "./bits.js";
import { NUMERIC_MAX_SCALE } from "../types.js";
import { decodePointer, type LargePointer } from "./large-value.js";
import {
  align8,
  check,
  DELTA_BLOCK,
  PageCorruptionError,
  LITTLE_ENDIAN,
  PAGE_HEADER_BYTES,
  PageFlag,
  plainWidth,
  validityBytes,
  ValueTag,
} from "./format.js";

type Rows = Uint16Array | Uint32Array | Int32Array;
type Unsigned = Uint8Array | Uint16Array | Uint32Array;

if (!LITTLE_ENDIAN) throw new Error("The page format needs a little-endian platform");

export interface PageView {
  readonly bytes: Uint8Array;
  readonly kind: number;
  readonly tag: number;
  readonly flags: number;
  readonly width: number;
  readonly rowCount: number;
  readonly aux: number;
  readonly nullCount: number;
  /** Validity words, or null when the page has no nulls or is all null (`allNull`). */
  readonly validity: Uint32Array | null;
  readonly allNull: boolean;
  readonly safe: boolean;
  /** `safe` is proven by the header alone (a bound on every value), so decoding skips the recheck. */
  readonly boundSafe: boolean;
  readonly specials: Uint8Array | null;
  /** Decimals: the scale of every row (fixed chunk, or a mixed chunk's uniform page). */
  readonly scale: number;
  /** Decimals of a mixed-scale chunk with differing scales: one per row. */
  readonly scales: Uint16Array | null;
  /** Plain varlen pages with out-of-line values: bit set = the slot holds a pointer. */
  readonly large: Uint32Array | null;
  /** Byte offset of the payload in `bytes`. */
  readonly start: number;
  /** FOR / delta deltas, dictionary codes, plain varlen offsets (by width). */
  readonly unsigned: Unsigned | null;
  /** int64 base (FOR) or minimum difference (delta), as words and as an f64 when safe. */
  readonly baseLow: number;
  readonly baseHigh: number;
  readonly baseNumber: number;
  /** Delta checkpoints as word pairs. */
  readonly checkpoints: Int32Array | null;
  /** Plain values: i32 / i64 words / f64 / f32 / u32 bool words / u32 limbs, by tag. */
  readonly plain: Int32Array | Float64Array | Float32Array | Uint32Array | null;
  /** Plain varlen value bytes. */
  readonly data: Uint8Array | null;
}

/** A chunk's dictionary: its entries as an immutable flat vector (aliases the page bytes). */
export interface PageDictionary {
  readonly tag: number;
  readonly size: number;
  /** varlen: entries' bytes and Int32 offsets; fixed16: 16 bytes per entry. */
  readonly bytes: Uint8Array;
  readonly offsets: Int32Array | null;
}

function aligned(bytes: Uint8Array): Uint8Array {
  return (bytes.byteOffset & 7) === 0 ? bytes : bytes.slice();
}

const KNOWN_FLAGS =
  PageFlag.validity |
  PageFlag.allNull |
  PageFlag.delta |
  PageFlag.specials |
  PageFlag.safe |
  PageFlag.scales |
  PageFlag.uniformScale |
  PageFlag.large;

function constantWidth(tag: number): number {
  if (tag === ValueTag.bool) return 1;
  return plainWidth(tag);
}

/**
 * Parses and structurally validates a raw (uncompressed) page against its directory entry and
 * its chunk's tag. Throws `PageCorruptionError` on any inconsistency.
 */
/**
 * `chunkScale` is the decimal chunk's directory scale: a fixed scale, or null for a mixed-scale
 * chunk (whose pages carry `scales` or `uniformScale`). Ignored for other tags.
 */
export function parsePage(
  stored: Uint8Array,
  entry: PageEntry,
  chunkTag: number,
  chunkScale: number | null = 0,
): PageView {
  const bytes = aligned(stored);
  check(bytes.length >= PAGE_HEADER_BYTES, "Page is shorter than its header");
  const kind = bytes[0] ?? 0;
  const tag = bytes[1] ?? 0;
  const flags = bytes[2] ?? 0;
  const width = bytes[3] ?? 0;
  const words = new Uint32Array(bytes.buffer, bytes.byteOffset + 4, 3);
  const rows = words[0] ?? 0;
  const aux = words[1] ?? 0;
  check(words[2] === 0, "Page header reserved field is not zero");
  check(kind === (entry.encoding & 0x7f), "Page encoding differs from its directory entry");
  check(rows === entry.rowCount && rows > 0, "Page row count differs from its directory entry");
  check(
    tag === chunkTag || (chunkTag === ValueTag.dec64 && tag === ValueTag.dec128),
    "Page value tag differs from its chunk",
  );
  check((flags & ~KNOWN_FLAGS) === 0, "Page has unknown flags");
  const allNull = (flags & PageFlag.allNull) !== 0;
  const hasValidity = (flags & PageFlag.validity) !== 0;
  const delta = (flags & PageFlag.delta) !== 0;
  const hasSpecials = (flags & PageFlag.specials) !== 0;
  const safe = (flags & PageFlag.safe) !== 0;
  const decimal = tag === ValueTag.dec64 || tag === ValueTag.dec128;
  check(!(allNull && hasValidity), "Page is both all-null and partly null");
  check(!delta || kind === PageEncoding.frameOfReference, "Delta flag outside a FOR page");
  check(!hasSpecials || decimal, "Specials outside a decimal page");
  const hasScales = (flags & PageFlag.scales) !== 0;
  const hasLarge = (flags & PageFlag.large) !== 0;
  check(
    !hasLarge || (tag === ValueTag.varlen && kind === PageEncoding.plain),
    "Large values outside a plain varlen page",
  );
  const uniformScale = (flags & PageFlag.uniformScale) !== 0;
  check(!(hasScales && uniformScale), "Page has both a scale section and a uniform scale");
  if (decimal && chunkScale === null)
    check(allNull !== (hasScales || uniformScale), "Mixed-scale page without its scales");
  else check(!hasScales && !uniformScale, "Scales outside a mixed-scale decimal page");
  check(!uniformScale || aux <= NUMERIC_MAX_SCALE, "Decimal scale out of range");
  check(
    !safe || tag === ValueTag.i64 || tag === ValueTag.dec64 || tag === ValueTag.interval,
    "Safe flag outside an int64-family page",
  );
  check(
    !allNull ||
      ((kind === PageEncoding.constant || kind === PageEncoding.dictionary) && width === 0),
    "All-null page with a payload",
  );

  let at = PAGE_HEADER_BYTES;
  let validity: Uint32Array | null = null;
  let nullCount = allNull ? rows : 0;
  if (hasValidity) {
    const count = (rows + 31) >>> 5;
    check(bytes.length >= at + count * 4, "Page validity is truncated");
    validity = new Uint32Array(bytes.buffer, bytes.byteOffset + at, count);
    const rest = rows & 31;
    check(rest === 0 || (validity[count - 1] ?? 0) >>> rest === 0, "Validity bits past the page");
    nullCount = rows - countBits(validity, rows);
    check(nullCount > 0 && nullCount < rows, "Partly-null page without a partial validity");
    at += align8(count * 4);
  }
  check(nullCount === entry.nullCount, "Page null count differs from its directory entry");
  let specials: Uint8Array | null = null;
  if (hasSpecials) {
    check(bytes.length >= at + rows, "Page specials are truncated");
    specials = bytes.subarray(at, at + rows);
    for (let row = 0; row < rows; row++) check((specials[row] ?? 0) <= 3, "Bad decimal special");
    at += align8(rows);
  }
  let scales: Uint16Array | null = null;
  if (hasScales) {
    check(bytes.length >= at + rows * 2, "Page scales are truncated");
    scales = new Uint16Array(bytes.buffer, bytes.byteOffset + at, rows);
    for (let row = 0; row < rows; row++)
      check((scales[row] ?? 0) <= NUMERIC_MAX_SCALE, "Decimal scale out of range");
    at += align8(rows * 2);
  }
  let large: Uint32Array | null = null;
  if (hasLarge) {
    const count = (rows + 31) >>> 5;
    check(bytes.length >= at + count * 4, "Page large-value section is truncated");
    large = new Uint32Array(bytes.buffer, bytes.byteOffset + at, count);
    const rest = rows & 31;
    check(rest === 0 || (large[count - 1] ?? 0) >>> rest === 0, "Large-value bits past the page");
    for (let word = 0; word < count; word++)
      check(
        validity === null || ((large[word] ?? 0) & ~(validity[word] ?? 0)) === 0,
        "A null row is marked large",
      );
    at += align8(count * 4);
  }
  const start = at;
  let length = start;
  let unsigned: Unsigned | null = null;
  let plain: PageView["plain"] = null;
  let data: Uint8Array | null = null;
  let checkpoints: Int32Array | null = null;
  let baseLow = 0;
  let baseHigh = 0;
  const buffer = bytes.buffer;
  const offset = bytes.byteOffset;
  const fits = (end: number): void => check(bytes.length >= end, "Page payload is truncated");

  switch (kind) {
    case PageEncoding.constant: {
      if (allNull) break;
      if (tag === ValueTag.varlen) {
        check(width === 0, "Bad constant varlen width");
        length = start + aux;
        fits(length);
        data = bytes.subarray(start, length);
      } else {
        check(width === constantWidth(tag), "Bad constant width");
        length = start + width;
        fits(length);
        if (tag === ValueTag.i64 || tag === ValueTag.dec64) {
          const base = new Int32Array(buffer, offset + start, 2);
          baseLow = base[0] ?? 0;
          baseHigh = base[1] ?? 0;
        }
      }
      break;
    }
    case PageEncoding.plain: {
      if (tag === ValueTag.varlen) {
        check(width === 2 || width === 4, "Bad varlen offset width");
        const offsetBytes = align8((rows + 1) * width);
        length = start + offsetBytes + aux;
        fits(length);
        const offsets =
          width === 2
            ? new Uint16Array(buffer, offset + start, rows + 1)
            : new Uint32Array(buffer, offset + start, rows + 1);
        check(offsets[0] === 0 && offsets[rows] === aux, "Varlen offsets do not span the bytes");
        for (let row = 0; row < rows; row++)
          check((offsets[row] ?? 0) <= (offsets[row + 1] ?? 0), "Varlen offsets decrease");
        unsigned = offsets;
        data = bytes.subarray(start + offsetBytes, length);
        if (large !== null)
          for (let row = 0; row < rows; row++)
            if (isBit(large, row)) decodePointer(data, offsets[row] ?? 0, offsets[row + 1] ?? 0);
        break;
      }
      check(width === plainWidth(tag) && width > 0, "Bad plain width");
      if (tag === ValueTag.interval) {
        const lane = align8(rows * 4);
        length = start + lane * 2 + rows * 8;
      } else length = start + rows * width;
      fits(length);
      if (tag === ValueTag.i32) plain = new Int32Array(buffer, offset + start, rows);
      else if (tag === ValueTag.i64 || tag === ValueTag.dec64)
        plain = new Int32Array(buffer, offset + start, rows * 2);
      else if (tag === ValueTag.f64) plain = new Float64Array(buffer, offset + start, rows);
      else if (tag === ValueTag.f32) plain = new Float32Array(buffer, offset + start, rows);
      else if (tag === ValueTag.dec128) plain = new Uint32Array(buffer, offset + start, rows * 4);
      break;
    }
    case PageEncoding.bitmap: {
      check(tag === ValueTag.bool && width === 0, "Bitmap page outside bool");
      length = start + validityBytes(rows);
      fits(length);
      plain = new Uint32Array(buffer, offset + start, (rows + 31) >>> 5);
      break;
    }
    case PageEncoding.frameOfReference: {
      check(
        tag === ValueTag.i32 || tag === ValueTag.i64 || tag === ValueTag.dec64,
        "FOR page outside an integer family",
      );
      check(
        delta
          ? width === 0 || width === 1 || width === 2 || width === 4
          : width === 1 || width === 2 || width === 4,
        "Bad FOR width",
      );
      check(!(delta && nullCount > 0), "Delta page with nulls");
      fits(start + 8);
      const base = new Int32Array(buffer, offset + start, 2);
      baseLow = base[0] ?? 0;
      baseHigh = base[1] ?? 0;
      let deltas = start + 8;
      if (delta) {
        const blocks = Math.ceil(rows / DELTA_BLOCK);
        fits(deltas + blocks * 8);
        checkpoints = new Int32Array(buffer, offset + deltas, blocks * 2);
        deltas += blocks * 8;
      }
      length = deltas + rows * width;
      fits(length);
      if (width > 0) unsigned = unsignedView(buffer, offset + deltas, width, rows);
      break;
    }
    case PageEncoding.dictionary: {
      check(tag === ValueTag.varlen || tag === ValueTag.fixed16, "Dictionary page outside varlen");
      check(width === 0 || width === 1 || width === 2 || width === 4, "Bad code width");
      length = start + rows * width;
      fits(length);
      if (width > 0) {
        const codes = unsignedView(buffer, offset + start, width, rows);
        let max = 0;
        for (let row = 0; row < rows; row++) if ((codes[row] ?? 0) > max) max = codes[row] ?? 0;
        check(max <= aux, "Dictionary code above the page's maximum");
        unsigned = codes;
      }
      break;
    }
    default:
      check(false, "Unknown page encoding");
  }
  check(bytes.length === length, "Page length differs from its contents");
  const baseNumber = baseHigh * 4_294_967_296 + (baseLow >>> 0);
  // Only an int64 or decimal-coefficient payload supplies the base the proof reads; an interval's
  // base is not decoded here, so its pages are always rechecked value by value (B2-2 #2).
  const boundSafe =
    safe &&
    (tag === ValueTag.i64 || tag === ValueTag.dec64) &&
    headerBoundsSafe(kind, width, baseNumber, checkpoints, rows);
  return {
    bytes,
    kind,
    tag,
    flags,
    width,
    rowCount: rows,
    aux,
    nullCount,
    validity,
    allNull,
    safe,
    boundSafe,
    specials,
    scale: uniformScale ? aux : (chunkScale ?? 0),
    scales,
    large,
    start,
    unsigned,
    baseLow,
    baseHigh,
    baseNumber,
    checkpoints,
    plain,
    data,
  };
}

/**
 * Whether the header alone bounds every value of an int64-family page within ±(2^53 − 1):
 * constant (the value), FOR (base … base + 2^(8w) − 1), delta (each checkpoint ± 127 steps of at
 * most |base| + 2^(8w) − 1). Plain pages are rechecked value by value instead.
 */
function headerBoundsSafe(
  kind: number,
  width: number,
  base: number,
  checkpoints: Int32Array | null,
  rows: number,
): boolean {
  const span = width === 0 ? 0 : 2 ** (8 * width) - 1;
  if (kind === PageEncoding.constant) return Math.abs(base) <= Number.MAX_SAFE_INTEGER;
  if (kind !== PageEncoding.frameOfReference) return false;
  if (checkpoints === null)
    return base >= -Number.MAX_SAFE_INTEGER && base + span <= Number.MAX_SAFE_INTEGER;
  let largest = 0;
  for (let block = 0; block * DELTA_BLOCK < rows; block++) {
    const value =
      (checkpoints[block * 2 + 1] ?? 0) * 4_294_967_296 + ((checkpoints[block * 2] ?? 0) >>> 0);
    largest = Math.max(largest, Math.abs(value));
  }
  return largest + (DELTA_BLOCK - 1) * (Math.abs(base) + span) <= Number.MAX_SAFE_INTEGER;
}

function unsignedView(
  buffer: ArrayBufferLike,
  offset: number,
  width: number,
  rows: number,
): Unsigned {
  return width === 1
    ? new Uint8Array(buffer, offset, rows)
    : width === 2
      ? new Uint16Array(buffer, offset, rows)
      : new Uint32Array(buffer, offset, rows);
}

/** Parses a chunk's dictionary page (plain varlen with u32 offsets, or plain fixed16). */
export function parseDictionary(
  stored: Uint8Array,
  entry: PageEntry,
  chunkTag: number,
): PageDictionary {
  const view = parsePage(stored, entry, chunkTag);
  check(view.kind === PageEncoding.plain && view.nullCount === 0, "Bad dictionary page");
  if (view.tag === ValueTag.fixed16)
    return {
      tag: view.tag,
      size: view.rowCount,
      bytes: view.bytes.subarray(view.start, view.start + view.rowCount * 16),
      offsets: null,
    };
  check(view.width === 4 && view.unsigned !== null && view.data !== null, "Bad dictionary offsets");
  const offsets = view.unsigned as Uint32Array;
  return {
    tag: view.tag,
    size: view.rowCount,
    bytes: view.data,
    offsets: new Int32Array(offsets.buffer, offsets.byteOffset, offsets.length),
  };
}

/** The dictionary as an immutable flat vector (vector.ts rule 2: shareable past a borrow). */
export function dictionaryVector(dictionary: PageDictionary, type: FlatVector["type"]): FlatVector {
  const data: VectorData =
    dictionary.offsets === null
      ? { layout: "fixed16", bytes: dictionary.bytes }
      : { layout: "varlen", bytes: dictionary.bytes, offsets: dictionary.offsets };
  return { form: "flat", type, length: dictionary.size, validity: null, data };
}

// ---------------------------------------------------------------------------------------------
// validity

function writeValidityRange(
  view: PageView,
  from: number,
  count: number,
  target: Validity | null,
  at: number,
): void {
  if (target === null) {
    if (view.nullCount > 0)
      throw new RangeError("The target has no validity but the page has nulls");
    return;
  }
  if (view.validity !== null) copyBits(view.validity, from, target, at, count);
  else fillBits(target, at, count, !view.allNull);
}

function gatherValidity(
  view: PageView,
  rows: Rows,
  start: number,
  end: number,
  base: number,
  target: Validity | null,
  at: number,
): void {
  if (target === null) {
    if (view.nullCount > 0)
      throw new RangeError("The target has no validity but the page has nulls");
    return;
  }
  if (view.validity !== null) gatherBits(view.validity, rows, start, end, base, target, at);
  else fillBits(target, at, end - start, !view.allNull);
}

// ---------------------------------------------------------------------------------------------
// int32

/**
 * One Uint32 scratch for widening u8/u16/u32 sections with the native `TypedArray.set`, so every
 * arithmetic loop below reads a single array type (monomorphic) whatever the page's width.
 */
let scratch = new Uint32Array(4096);
function widened(source: Unsigned, from: number, to: number): Uint32Array {
  const count = to - from;
  if (scratch.length < count) scratch = new Uint32Array(Math.max(count, scratch.length * 2));
  const out = scratch.subarray(0, count);
  out.set(source.subarray(from, to));
  return out;
}

function addBase32(out: Int32Array, at: number, count: number, base: number): void {
  for (let k = at; k < at + count; k++) out[k] = ((out[k] ?? 0) + base) | 0;
}

function decodeInt32(view: PageView, from: number, to: number, out: Int32Array, at: number): void {
  const count = to - from;
  if (view.allNull) return;
  switch (view.kind) {
    case PageEncoding.constant:
      out.fill(
        new Int32Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0,
        at,
        at + count,
      );
      return;
    case PageEncoding.plain:
      out.set((view.plain as Int32Array).subarray(from, to), at);
      return;
    default:
      break;
  }
  const base = view.baseLow;
  const deltas = view.unsigned;
  if (view.checkpoints === null) {
    if (deltas === null) return;
    // Native widening copy, then one add pass.
    out.set(deltas.subarray(from, to), at);
    addBase32(out, at, count, base);
    return;
  }
  const checkpoints = view.checkpoints;
  const first = from - (from % DELTA_BLOCK);
  const steps = deltas === null ? null : widened(deltas, first, to);
  for (let block = first; block < to; block += DELTA_BLOCK) {
    let value = checkpoints[(block / DELTA_BLOCK) * 2] ?? 0;
    const end = Math.min(to, block + DELTA_BLOCK);
    let row = block;
    while (row < from) {
      row++;
      value = (value + (base + (steps === null ? 0 : (steps[row - first] ?? 0)))) | 0;
    }
    out[at + row - from] = value;
    if (steps === null)
      for (row++; row < end; row++) out[at + row - from] = value = (value + base) | 0;
    else
      for (row++; row < end; row++)
        out[at + row - from] = value = (value + (base + (steps[row - first] ?? 0))) | 0;
  }
}

function gatherInt32(
  view: PageView,
  rows: Rows,
  start: number,
  end: number,
  base0: number,
  out: Int32Array,
  at: number,
): void {
  if (view.allNull) return;
  if (view.kind === PageEncoding.constant) {
    out.fill(
      new Int32Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0,
      at,
      at + end - start,
    );
    return;
  }
  if (view.kind === PageEncoding.plain) {
    const values = view.plain as Int32Array;
    for (let k = start; k < end; k++) out[at + k - start] = values[(rows[k] ?? 0) - base0] ?? 0;
    return;
  }
  const deltas = view.unsigned;
  const base = view.baseLow;
  if (view.checkpoints === null) {
    if (deltas !== null)
      for (let k = start; k < end; k++)
        out[at + k - start] = (base + (deltas[(rows[k] ?? 0) - base0] ?? 0)) | 0;
    return;
  }
  const checkpoints = view.checkpoints;
  let cursor = -1;
  let value = 0;
  for (let k = start; k < end; k++) {
    const row = (rows[k] ?? 0) - base0;
    if (
      cursor < 0 ||
      row < cursor ||
      Math.floor(row / DELTA_BLOCK) !== Math.floor(cursor / DELTA_BLOCK)
    ) {
      cursor = row - (row % DELTA_BLOCK);
      value = checkpoints[(cursor / DELTA_BLOCK) * 2] ?? 0;
    }
    while (cursor < row) {
      cursor++;
      value = (value + (base + (deltas === null ? 0 : (deltas[cursor] ?? 0)))) | 0;
    }
    out[at + k - start] = value;
  }
}

// ---------------------------------------------------------------------------------------------
// int64 family (also decimal int64-tier coefficients and interval micros)

const TWO32 = 4_294_967_296;

function int64Words(values: BigInt64Array): Int32Array {
  return new Int32Array(values.buffer, values.byteOffset, values.length * 2);
}

function requireSafe(view: PageView, target: Int64Values): void {
  if (target.repr === "safe" && !view.safe)
    throw new RangeError("A page with values beyond ±(2^53 − 1) needs an int64 target");
}

function decodeInt64(
  view: PageView,
  from: number,
  to: number,
  target: Int64Values,
  at: number,
): void {
  if (view.allNull) return;
  requireSafe(view, target);
  const count = to - from;
  if (view.kind === PageEncoding.constant) {
    if (target.repr === "safe") target.values.fill(view.baseNumber, at, at + count);
    else {
      const words = int64Words(target.values);
      for (let k = 0; k < count; k++) {
        words[(at + k) * 2] = view.baseLow;
        words[(at + k) * 2 + 1] = view.baseHigh;
      }
    }
    return;
  }
  if (view.kind === PageEncoding.plain) {
    const words = view.plain as Int32Array;
    if (target.repr === "int64") {
      new Uint8Array(target.values.buffer, target.values.byteOffset + at * 8, count * 8).set(
        new Uint8Array(words.buffer, words.byteOffset + from * 8, count * 8),
      );
      return;
    }
    const out = target.values;
    for (let k = 0; k < count; k++) {
      const row = from + k;
      out[at + k] = (words[row * 2 + 1] ?? 0) * TWO32 + ((words[row * 2] ?? 0) >>> 0);
    }
    return;
  }
  const deltas = view.unsigned;
  if (view.checkpoints === null) {
    if (deltas === null) return;
    if (target.repr === "safe") {
      const out = target.values;
      const base = view.baseNumber;
      out.set(deltas.subarray(from, to), at);
      for (let k = at; k < at + count; k++) out[k] = (out[k] ?? 0) + base;
      return;
    }
    const words = int64Words(target.values);
    const low = view.baseLow >>> 0;
    const high = view.baseHigh;
    const steps = widened(deltas, from, to);
    for (let k = 0; k < count; k++) {
      const sum = low + (steps[k] ?? 0);
      const slot = (at + k) * 2;
      words[slot] = sum | 0;
      words[slot + 1] = sum >= TWO32 ? high + 1 : high;
    }
    return;
  }
  decodeInt64Delta(view, from, to, target, at);
}

/**
 * Rows `[from, to)` of a delta page: block by block from each block's checkpoint, with no
 * per-row block test; rows before `from` in the first block are summed but not written.
 */
function decodeInt64Delta(
  view: PageView,
  from: number,
  to: number,
  target: Int64Values,
  at: number,
): void {
  const checkpoints = view.checkpoints ?? new Int32Array(0);
  const first = from - (from % DELTA_BLOCK);
  const steps = view.unsigned === null ? null : widened(view.unsigned, first, to);
  if (target.repr === "safe") {
    const out = target.values;
    const step = view.baseNumber;
    for (let block = first; block < to; block += DELTA_BLOCK) {
      const index = (block / DELTA_BLOCK) * 2;
      let value = (checkpoints[index + 1] ?? 0) * TWO32 + ((checkpoints[index] ?? 0) >>> 0);
      const end = Math.min(to, block + DELTA_BLOCK);
      let row = block;
      while (row < from) {
        row++;
        value += step + (steps === null ? 0 : (steps[row - first] ?? 0));
      }
      out[at + row - from] = value;
      if (steps === null) for (row++; row < end; row++) out[at + row - from] = value += step;
      else
        for (row++; row < end; row++)
          out[at + row - from] = value += step + (steps[row - first] ?? 0);
    }
    return;
  }
  const words = int64Words(target.values);
  const stepLow = view.baseLow >>> 0;
  const stepHigh = view.baseHigh;
  for (let block = first; block < to; block += DELTA_BLOCK) {
    const index = (block / DELTA_BLOCK) * 2;
    let low = (checkpoints[index] ?? 0) >>> 0;
    let high = checkpoints[index + 1] ?? 0;
    const end = Math.min(to, block + DELTA_BLOCK);
    for (let row = block; row < end; row++) {
      if (row > block) {
        const sum = low + stepLow + (steps === null ? 0 : (steps[row - first] ?? 0));
        const carry = sum >= TWO32 * 2 ? 2 : sum >= TWO32 ? 1 : 0;
        low = sum - carry * TWO32;
        high = (high + stepHigh + carry) | 0;
      }
      if (row >= from) {
        const slot = (at + row - from) * 2;
        words[slot] = low | 0;
        words[slot + 1] = high;
      }
    }
  }
}

/** Selected rows of a delta page: one forward walk per block, shared by the rows inside it. */
function gatherInt64Delta(
  view: PageView,
  rows: Rows,
  start: number,
  end: number,
  base0: number,
  target: Int64Values,
  at: number,
): void {
  const deltas = view.unsigned;
  const checkpoints = view.checkpoints ?? new Int32Array(0);
  const safe = target.repr === "safe";
  const out = safe ? target.values : null;
  const words = safe ? null : int64Words(target.values);
  const step = view.baseNumber;
  const stepLow = view.baseLow >>> 0;
  const stepHigh = view.baseHigh;
  let cursor = -1;
  let value = 0;
  let low = 0;
  let high = 0;
  for (let k = start; k < end; k++) {
    const row = (rows[k] ?? 0) - base0;
    if (
      cursor < 0 ||
      row < cursor ||
      Math.floor(row / DELTA_BLOCK) !== Math.floor(cursor / DELTA_BLOCK)
    ) {
      cursor = row - (row % DELTA_BLOCK);
      const block = (cursor / DELTA_BLOCK) * 2;
      low = (checkpoints[block] ?? 0) >>> 0;
      high = checkpoints[block + 1] ?? 0;
      value = high * TWO32 + low;
    }
    while (cursor < row) {
      cursor++;
      const delta = deltas === null ? 0 : (deltas[cursor] ?? 0);
      if (safe) value += step + delta;
      else {
        const sum = low + stepLow + delta;
        const carry = sum >= TWO32 * 2 ? 2 : sum >= TWO32 ? 1 : 0;
        low = sum - carry * TWO32;
        high = (high + stepHigh + carry) | 0;
      }
    }
    const slot = at + k - start;
    if (out !== null) out[slot] = value;
    else if (words !== null) {
      words[slot * 2] = low | 0;
      words[slot * 2 + 1] = high;
    }
  }
}

function gatherInt64(
  view: PageView,
  rows: Rows,
  start: number,
  end: number,
  base0: number,
  target: Int64Values,
  at: number,
): void {
  if (view.allNull) return;
  requireSafe(view, target);
  if (view.kind === PageEncoding.constant) {
    decodeInt64(view, 0, end - start, target, at);
    return;
  }
  if (view.checkpoints !== null) {
    gatherInt64Delta(view, rows, start, end, base0, target, at);
    return;
  }
  const words = view.kind === PageEncoding.plain ? (view.plain as Int32Array) : null;
  const deltas = view.unsigned;
  if (target.repr === "safe") {
    const out = target.values;
    const base = view.baseNumber;
    for (let k = start; k < end; k++) {
      const row = (rows[k] ?? 0) - base0;
      out[at + k - start] =
        words !== null
          ? (words[row * 2 + 1] ?? 0) * TWO32 + ((words[row * 2] ?? 0) >>> 0)
          : base + (deltas?.[row] ?? 0);
    }
    return;
  }
  const out = int64Words(target.values);
  const low = view.baseLow >>> 0;
  const high = view.baseHigh;
  for (let k = start; k < end; k++) {
    const row = (rows[k] ?? 0) - base0;
    const slot = (at + k - start) * 2;
    if (words !== null) {
      out[slot] = words[row * 2] ?? 0;
      out[slot + 1] = words[row * 2 + 1] ?? 0;
    } else {
      const sum = low + (deltas?.[row] ?? 0);
      out[slot] = sum | 0;
      out[slot + 1] = sum >= TWO32 ? high + 1 : high;
    }
  }
}

// ---------------------------------------------------------------------------------------------
// int128 decimal coefficients

function decodeInt128(
  view: PageView,
  from: number,
  to: number,
  out: Uint32Array,
  at: number,
  rows: Rows | null,
  start: number,
  base0: number,
): void {
  if (view.allNull) return;
  const count = to - from;
  const rowAt = (k: number): number => (rows === null ? from + k : (rows[start + k] ?? 0) - base0);
  if (view.tag === ValueTag.dec128) {
    if (view.kind === PageEncoding.plain) {
      const limbs = view.plain as Uint32Array;
      if (rows === null) out.set(limbs.subarray(from * 4, to * 4), at * 4);
      else
        for (let k = 0; k < count; k++) {
          const row = rowAt(k);
          out.set(limbs.subarray(row * 4, row * 4 + 4), (at + k) * 4);
        }
    }
    return;
  }
  // An int64-tier page into 128-bit limbs: decode the words, then sign-extend in place.
  const scratch = new BigInt64Array(1);
  const words = int64Words(scratch);
  const pair: Int64Values = { repr: "int64", values: scratch };
  for (let k = 0; k < count; k++) {
    if (rows === null) decodeInt64(view, from + k, from + k + 1, pair, 0);
    else gatherInt64(view, rows, start + k, start + k + 1, base0, pair, 0);
    const slot = (at + k) * 4;
    const high = words[1] ?? 0;
    out[slot] = words[0] ?? 0;
    out[slot + 1] = high;
    out[slot + 2] = high >> 31;
    out[slot + 3] = high >> 31;
  }
}

// ---------------------------------------------------------------------------------------------
// varlen and fixed16

function copyBytes(
  source: Uint8Array,
  start: number,
  end: number,
  target: Uint8Array,
  at: number,
): void {
  const length = end - start;
  if (length > 32) target.set(source.subarray(start, end), at);
  else for (let k = 0; k < length; k++) target[at + k] = source[start + k] ?? 0;
}

type VarlenData = Extract<VectorData, { layout: "varlen" }>;

/** Bytes `decodeRange` / `gatherRows` will append for these rows of a varlen page. */
export function varlenBytesOf(
  view: PageView,
  dictionary: PageDictionary | null,
  rows: Rows | null,
  from: number,
  to: number,
  base0 = 0,
): number {
  if (view.allNull) return 0;
  const count = to - from;
  if (view.kind === PageEncoding.constant) return count * view.aux;
  const valueBytes = (row: number): number => {
    if (view.kind === PageEncoding.dictionary) {
      if (view.validity !== null && (((view.validity[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 0)
        return 0;
      const code = view.unsigned === null ? view.aux : (view.unsigned[row] ?? 0);
      const offsets = dictionary?.offsets;
      return offsets === undefined || offsets === null
        ? 0
        : (offsets[code + 1] ?? 0) - (offsets[code] ?? 0);
    }
    const offsets = view.unsigned;
    return offsets === null ? 0 : (offsets[row + 1] ?? 0) - (offsets[row] ?? 0);
  };
  if (rows === null && view.kind === PageEncoding.plain && view.large === null) {
    const offsets = view.unsigned;
    return offsets === null ? 0 : (offsets[to] ?? 0) - (offsets[from] ?? 0);
  }
  let total = 0;
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - base0;
    total +=
      view.large !== null && isBit(view.large, row) ? largeLength(view, row) : valueBytes(row);
  }
  return total;
}

function decodeVarlen(
  view: PageView,
  dictionary: PageDictionary | null,
  rows: Rows | null,
  from: number,
  to: number,
  base0: number,
  data: VarlenData,
  at: number,
  values: LargeResolver | null,
): void {
  const offsets = data.offsets;
  const bytes = data.bytes;
  let cursor = offsets[at] ?? 0;
  const count = to - from;
  const needed = varlenBytesOf(view, dictionary, rows, from, to, base0);
  if (cursor + needed > bytes.length) throw new RangeError("The varlen target has too few bytes");
  if (view.allNull) {
    offsets.fill(cursor, at + 1, at + count + 1);
    return;
  }
  if (view.kind === PageEncoding.constant) {
    const value = view.data ?? new Uint8Array(0);
    const length = value.length;
    for (let k = 0; k < count; k++) {
      const valid =
        view.validity === null ||
        isBit(view.validity, rows === null ? from + k : (rows[k] ?? 0) - base0);
      if (valid) {
        copyBytes(value, 0, length, bytes, cursor);
        cursor += length;
      }
      offsets[at + k + 1] = cursor;
    }
    return;
  }
  if (view.kind === PageEncoding.dictionary) {
    const entries = dictionary?.bytes ?? new Uint8Array(0);
    const entryOffsets = dictionary?.offsets ?? new Int32Array(1);
    const codes = view.unsigned;
    for (let k = 0; k < count; k++) {
      const row = rows === null ? from + k : (rows[k] ?? 0) - base0;
      if (view.validity === null || isBit(view.validity, row)) {
        const code = codes === null ? view.aux : (codes[row] ?? 0);
        const start = entryOffsets[code] ?? 0;
        const end = entryOffsets[code + 1] ?? 0;
        copyBytes(entries, start, end, bytes, cursor);
        cursor += end - start;
      }
      offsets[at + k + 1] = cursor;
    }
    return;
  }
  const source = view.data ?? new Uint8Array(0);
  const pageOffsets = view.unsigned ?? new Uint32Array(1);
  if (view.large !== null) {
    // Out-of-line rows copy their resolved value; the others their slot.
    const large = view.large;
    for (let k = 0; k < count; k++) {
      const row = rows === null ? from + k : (rows[k] ?? 0) - base0;
      if (isBit(large, row)) {
        const parts = values?.(row) ?? null;
        if (parts === null) throw new ValueNotLoadedError();
        for (const part of parts) {
          bytes.set(part, cursor);
          cursor += part.length;
        }
      } else {
        const start = pageOffsets[row] ?? 0;
        const end = pageOffsets[row + 1] ?? 0;
        copyBytes(source, start, end, bytes, cursor);
        cursor += end - start;
      }
      offsets[at + k + 1] = cursor;
    }
    return;
  }
  if (rows === null) {
    const first = pageOffsets[from] ?? 0;
    const last = pageOffsets[to] ?? 0;
    bytes.set(source.subarray(first, last), cursor);
    const shift = cursor - first;
    const ends = widened(pageOffsets, from + 1, to + 1);
    for (let k = 0; k < count; k++) offsets[at + k + 1] = (ends[k] ?? 0) + shift;
    return;
  }
  for (let k = 0; k < count; k++) {
    const row = (rows[k] ?? 0) - base0;
    const start = pageOffsets[row] ?? 0;
    const end = pageOffsets[row + 1] ?? 0;
    copyBytes(source, start, end, bytes, cursor);
    cursor += end - start;
    offsets[at + k + 1] = cursor;
  }
}

/**
 * The parts of the out-of-line value in page row `row` (page-relative), verified and checked
 * against its pointer by the reader that loaded them, or null when it is not loaded
 * (`ValueNotLoadedError`). Decoding copies the parts, in order, into the target.
 */
export type LargeResolver = (row: number) => readonly Uint8Array[] | null;

/** A decode reached an out-of-line value its caller has not loaded (`ChunkReader.loadValues`). */
export class ValueNotLoadedError extends Error {
  readonly code = "MINNOW_VALUE_NOT_LOADED";
  constructor() {
    super("An out-of-line value was decoded before it was loaded (ChunkReader.loadValues)");
    this.name = "ValueNotLoadedError";
  }
}

/** The pointer in row `row`'s slot of a plain varlen page with large values (parse checked it). */
export function largePointer(view: PageView, row: number): LargePointer {
  const offsets = view.unsigned ?? new Uint32Array(1);
  return decodePointer(view.data ?? new Uint8Array(0), offsets[row] ?? 0, offsets[row + 1] ?? 0);
}

/** The length of the out-of-line value in row `row` (its pointer's u32 at byte 4; parse checked it). */
export function largeLength(view: PageView, row: number): number {
  const data = view.data ?? new Uint8Array(0);
  const at = (view.unsigned?.[row] ?? 0) + 4;
  return (
    ((data[at] ?? 0) | ((data[at + 1] ?? 0) << 8) | ((data[at + 2] ?? 0) << 16)) +
    (data[at + 3] ?? 0) * 0x1000000
  );
}

/** Whether row `row` of the page is stored out of line. */
export function isLarge(view: PageView, row: number): boolean {
  return view.large !== null && isBit(view.large, row);
}

function isBit(words: Uint32Array, row: number): boolean {
  return (((words[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1;
}

function decodeFixed16(
  view: PageView,
  dictionary: PageDictionary | null,
  rows: Rows | null,
  from: number,
  to: number,
  base0: number,
  out: Uint8Array,
  at: number,
): void {
  if (view.allNull) return;
  const count = to - from;
  const bytes = view.bytes;
  for (let k = 0; k < count; k++) {
    const row = rows === null ? from + k : (rows[k] ?? 0) - base0;
    let source = bytes;
    let start: number;
    if (view.kind === PageEncoding.constant) start = view.start;
    else if (view.kind === PageEncoding.plain) start = view.start + row * 16;
    else {
      source = dictionary?.bytes ?? bytes;
      start = (view.unsigned === null ? view.aux : (view.unsigned[row] ?? 0)) * 16;
    }
    const target = (at + k) * 16;
    for (let b = 0; b < 16; b++) out[target + b] = source[start + b] ?? 0;
  }
}

// ---------------------------------------------------------------------------------------------
// public entry points

function checkRange(view: PageView, from: number, to: number): void {
  if (!(from >= 0 && from <= to && to <= view.rowCount))
    throw new RangeError("Row range outside the page");
}

/**
 * Decodes rows `[from, to)` of the page into `target` slots `[at, at + to − from)`. For varlen
 * targets the values are appended at `target.data.offsets[at]`, which the caller has set.
 * `dictionary` is required for dictionary pages.
 */
export function decodeRange(
  view: PageView,
  from: number,
  to: number,
  target: FlatVector,
  at: number,
  dictionary: PageDictionary | null = null,
  values: LargeResolver | null = null,
): void {
  checkRange(view, from, to);
  writeValidityRange(view, from, to - from, target.validity, at);
  decodeValues(view, null, from, to, 0, target, at, dictionary, values);
}

/**
 * Decodes the selected rows `rows[start … end)` (positions relative to `base`, so a group-level
 * selection is passed as is with `base` = the page's first row; ascending) into `target` slots
 * `[at, at + end − start)`. Plain and dictionary pages index directly; FOR is direct; delta
 * pages decode one block prefix per selected row at most.
 */
export function gatherRows(
  view: PageView,
  rows: Rows,
  start: number,
  end: number,
  base: number,
  target: FlatVector,
  at: number,
  dictionary: PageDictionary | null = null,
  values: LargeResolver | null = null,
): void {
  for (let k = start; k < end; k++) {
    const row = (rows[k] ?? 0) - base;
    if (!(row >= 0 && row < view.rowCount)) throw new RangeError("Selected row outside the page");
  }
  gatherValidity(view, rows, start, end, base, target.validity, at);
  decodeValues(
    view,
    rows.subarray(start, end),
    0,
    end - start,
    base,
    target,
    at,
    dictionary,
    values,
  );
}

function decodeValues(
  view: PageView,
  rows: Rows | null,
  from: number,
  to: number,
  base0: number,
  target: FlatVector,
  at: number,
  dictionary: PageDictionary | null,
  values: LargeResolver | null,
): void {
  const data = target.data;
  const tag = view.tag;
  const count = to - from;
  if (view.kind === PageEncoding.dictionary && dictionary === null)
    throw new RangeError("A dictionary page needs its chunk's dictionary");
  if (
    view.kind === PageEncoding.dictionary &&
    dictionary !== null &&
    view.aux >= dictionary.size &&
    !view.allNull
  )
    throw new RangeError("Dictionary code outside the dictionary");
  switch (data.layout) {
    case "bool": {
      expectTag(tag, ValueTag.bool);
      if (view.allNull) return;
      if (view.kind === PageEncoding.constant) {
        data.values.fill(view.bytes[view.start] ?? 0, at, at + count);
        return;
      }
      const bits = view.plain as Uint32Array;
      if (rows === null) unpackBools(bits, from, count, data.values, at);
      else
        for (let k = 0; k < count; k++)
          data.values[at + k] = isBit(bits, (rows[k] ?? 0) - base0) ? 1 : 0;
      return;
    }
    case "i32":
      expectTag(tag, ValueTag.i32);
      if (rows === null) decodeInt32(view, from, to, data.values, at);
      else gatherInt32(view, rows, 0, count, base0, data.values, at);
      return;
    case "int64":
      expectTag(tag, ValueTag.i64);
      if (rows === null) decodeInt64(view, from, to, data.values, at);
      else gatherInt64(view, rows, 0, count, base0, data.values, at);
      checkSafe(view, data.values, at, count);
      return;
    case "f64":
    case "f32": {
      expectTag(tag, data.layout === "f64" ? ValueTag.f64 : ValueTag.f32);
      if (view.allNull) return;
      const out = data.values;
      if (view.kind === PageEncoding.constant) {
        const value =
          data.layout === "f64"
            ? (new Float64Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0)
            : (new Float32Array(view.bytes.buffer, view.bytes.byteOffset + view.start, 1)[0] ?? 0);
        out.fill(value, at, at + count);
        return;
      }
      const values = view.plain as Float64Array | Float32Array;
      if (rows === null) out.set(values.subarray(from, to), at);
      else for (let k = 0; k < count; k++) out[at + k] = values[(rows[k] ?? 0) - base0] ?? 0;
      return;
    }
    case "varlen":
      expectTag(tag, ValueTag.varlen);
      decodeVarlen(view, dictionary, rows, from, to, base0, data, at, values);
      return;
    case "fixed16":
      expectTag(tag, ValueTag.fixed16);
      decodeFixed16(view, dictionary, rows, from, to, base0, data.bytes, at);
      return;
    case "interval":
      expectTag(tag, ValueTag.interval);
      decodeInterval(view, rows, from, to, base0, data, at);
      return;
    case "decimal": {
      if (tag !== ValueTag.dec64 && tag !== ValueTag.dec128)
        throw new RangeError("Target layout differs from the page");
      decodeScales(view, rows, from, to, base0, data.scale, data.scales, at);
      if (view.specials !== null && data.specials === null)
        throw new RangeError("The page has decimal specials but the target has none");
      if (data.specials !== null) {
        if (view.specials === null) data.specials.fill(0, at, at + count);
        else if (rows === null) data.specials.set(view.specials.subarray(from, to), at);
        else
          for (let k = 0; k < count; k++)
            data.specials[at + k] = view.specials[(rows[k] ?? 0) - base0] ?? 0;
      }
      const coefficients = data.coefficients;
      if (coefficients.repr === "wide")
        throw new RangeError("Pages decode into the int64 or int128 tier");
      if (coefficients.repr === "int128") {
        decodeInt128(view, from, to, coefficients.limbs, at, rows, 0, base0);
        return;
      }
      if (tag === ValueTag.dec128 && !view.allNull)
        throw new RangeError("A 128-bit coefficient page needs an int128 target");
      if (rows === null) decodeInt64(view, from, to, coefficients, at);
      else gatherInt64(view, rows, 0, count, base0, coefficients, at);
      checkSafe(view, coefficients, at, count);
      return;
    }
  }
}

/**
 * A decimal target either has the page's one scale (`scale`), or per-slot `scales`, which are
 * filled from the page (its section, or its single scale).
 */
function decodeScales(
  view: PageView,
  rows: Rows | null,
  from: number,
  to: number,
  base0: number,
  scale: number | null,
  scales: Uint16Array | null,
  at: number,
): void {
  const count = to - from;
  if (scale !== null) {
    if (!view.allNull && (view.scales !== null || view.scale !== scale))
      throw new RangeError("The page's scales differ from the target's scale");
    return;
  }
  if (scales === null) throw new RangeError("A decimal target needs a scale or per-slot scales");
  const source = view.scales;
  if (source === null) scales.fill(view.allNull ? 0 : view.scale, at, at + count);
  else if (rows === null) scales.set(source.subarray(from, to), at);
  else for (let k = 0; k < count; k++) scales[at + k] = source[(rows[k] ?? 0) - base0] ?? 0;
}

const MAX_SAFE = Number.MAX_SAFE_INTEGER;

/**
 * A page flagged `safe` decoded into the f64 representation is rechecked: every slot must lie in
 * ±(2^53 − 1) (null slots hold 0, encode.ts). A wrong flag is corruption, never a silent rounding:
 * a value past 2^53 rounds to a double of magnitude ≥ 2^53, which this rejects.
 */
function checkSafe(view: PageView, values: Int64Values, at: number, count: number): void {
  if (values.repr !== "safe" || view.allNull || view.boundSafe) return;
  if (!allSafe(values.values, at, at + count))
    throw new PageCorruptionError("A page flagged safe holds a value beyond ±(2^53 − 1)");
}

/** One monomorphic pass: min and max, compared once at the end. */
function allSafe(out: Float64Array, start: number, end: number): boolean {
  let low = 0;
  let high = 0;
  for (let k = start; k < end; k++) {
    const value = out[k] ?? 0;
    low = value < low ? value : low;
    high = value > high ? value : high;
  }
  return high <= MAX_SAFE && low >= -MAX_SAFE;
}

function expectTag(actual: number, expected: number): void {
  if (actual !== expected) throw new RangeError("Target layout differs from the page");
}

type IntervalData = Extract<VectorData, { layout: "interval" }>;

function decodeInterval(
  view: PageView,
  rows: Rows | null,
  from: number,
  to: number,
  base0: number,
  data: IntervalData,
  at: number,
): void {
  if (view.allNull) return;
  const micros = data.micros;
  requireSafe(view, micros);
  const count = to - from;
  const buffer = view.bytes.buffer;
  const offset = view.bytes.byteOffset + view.start;
  const constant = view.kind === PageEncoding.constant;
  const lane = constant ? 0 : align8(view.rowCount * 4);
  const months = new Int32Array(buffer, offset, constant ? 1 : view.rowCount);
  const days = constant
    ? new Int32Array(buffer, offset + 4, 1)
    : new Int32Array(buffer, offset + lane, view.rowCount);
  const words = constant
    ? new Int32Array(buffer, offset + 8, 2)
    : new Int32Array(buffer, offset + lane * 2, view.rowCount * 2);
  const microWords = micros.repr === "int64" ? int64Words(micros.values) : null;
  for (let k = 0; k < count; k++) {
    const row = constant ? 0 : rows === null ? from + k : (rows[k] ?? 0) - base0;
    data.months[at + k] = months[row] ?? 0;
    data.days[at + k] = days[row] ?? 0;
    const low = words[row * 2] ?? 0;
    const high = words[row * 2 + 1] ?? 0;
    if (microWords !== null) {
      microWords[(at + k) * 2] = low;
      microWords[(at + k) * 2 + 1] = high;
    } else (micros.values as Float64Array)[at + k] = high * TWO32 + (low >>> 0);
  }
  checkSafe(view, micros, at, count);
}

/**
 * A dictionary page's codes for rows `[from, to)` into `codes[at …]` (as Int32), with validity.
 * Codes under null slots are 0.
 */
export function decodeCodes(
  view: PageView,
  from: number,
  to: number,
  codes: Int32Array,
  validity: Validity | null,
  at: number,
): void {
  if (view.kind !== PageEncoding.dictionary) throw new RangeError("Not a dictionary page");
  checkRange(view, from, to);
  writeValidityRange(view, from, to - from, validity, at);
  if (view.unsigned === null) codes.fill(view.allNull ? 0 : view.aux, at, at + to - from);
  else codes.set(view.unsigned.subarray(from, to), at);
}

/** Codes of the selected rows `rows[start … end)` (relative to `base`) into `codes[at …]`. */
export function gatherCodes(
  view: PageView,
  rows: Rows,
  start: number,
  end: number,
  base: number,
  codes: Int32Array,
  validity: Validity | null,
  at: number,
): void {
  if (view.kind !== PageEncoding.dictionary) throw new RangeError("Not a dictionary page");
  for (let k = start; k < end; k++) {
    const row = (rows[k] ?? 0) - base;
    if (!(row >= 0 && row < view.rowCount)) throw new RangeError("Selected row outside the page");
  }
  gatherValidity(view, rows, start, end, base, validity, at);
  const source = view.unsigned;
  if (source === null) {
    codes.fill(view.allNull ? 0 : view.aux, at, at + end - start);
    return;
  }
  for (let k = start; k < end; k++) codes[at + k - start] = source[(rows[k] ?? 0) - base] ?? 0;
}
