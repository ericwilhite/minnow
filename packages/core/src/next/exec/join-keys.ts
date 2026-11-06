/**
 * Join keys in canonical form: what a hash join hashes, partitions and compares (track B7).
 *
 * ## Law (SQL equality, DECISIONS.md C7/C8)
 * Two keys are equal exactly when their canonical forms are equal, and equal keys hash equally:
 * - int32, date, bool: one word, the value.
 * - int64, time, timestamp: two words (lo, hi) of the exact value, whatever the vector's
 *   representation (`safe` Float64Array or BigInt64Array), so batches of either kind agree.
 * - float64, float32: two words of the f64 bits after −0 → +0 and every NaN → one NaN
 *   (PostgreSQL: `'NaN'::float8 = 'NaN'::float8`, `-0 = 0`).
 * - uuid: four words of its 16 bytes.
 * - text, bytes, enum (one key column, `=`): the stored bytes themselves (byte equality is SQL
 *   equality under deterministic collations; an enum equals by label).
 * - decimal, interval, several varlen columns, or any mix too wide for inline words: the
 *   persisted equality-key encoding (keys/key-encoding.ts), which normalizes `1.0 = 1.00` and
 *   `1 mon = 30 days`.
 * - JSON, arrays and locators are not join keys here (`UnsupportedPlanError`): their equality is
 *   semantic (exec/semantic.ts), so lowering must not plan a hash join on them.
 * - NULL: a key column compared with `=` makes the row unmatched (`nulls` bit set). With
 *   `IS NOT DISTINCT FROM` (`nullSafe`) a null is a value: its words are 0 and the extra mask word
 *   (or the encoding's null header) tells it apart from every non-null value.
 *
 * ## Hashing
 * `hashWords` (murmur-style mixing per word, then fmix32) and xxHash32 for bytes. Partitioning
 * uses the top bits of `levelHash(h, level)`; table slots use further bits of the same value, so
 * routing and buckets never share bits (design-joins-agg-spill.md "Radix").
 *
 * Extraction reads flat, constant and dictionary vectors, writes caller-owned scratch, and
 * allocates nothing per row.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { KeyBatch, encodeKeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import type { PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { FlatVector, Validity, Vector } from "../vector/vector.js";
import { UnsupportedPlanError } from "./errors.js";
import { hiOfSafe, wordsOf } from "./column-store.js";

export const HASH_SEED = 0x2f0b_3c4d;
/** Most words a key keeps inline in its hash-table slot. */
export const MAX_INLINE_WORDS = 6;

export type KeyPart = "w1" | "w2i" | "w2f" | "w4" | "raw" | "enc";

function partOf(type: PhysicalType): KeyPart {
  switch (type.kind) {
    case "bool":
    case "int32":
    case "date":
      return "w1";
    case "int64":
    case "time":
    case "timestamp":
      return "w2i";
    case "float64":
    case "float32":
      return "w2f";
    case "uuid":
      return "w4";
    case "text":
    case "bytes":
    case "enum":
      return "raw";
    case "decimal":
    case "interval":
      return "enc";
    case "json":
    case "array":
    case "locator":
      throw new UnsupportedPlanError(`a hash-join key of type ${type.kind}`);
  }
}

const WORDS: Record<KeyPart, number> = { w1: 1, w2i: 2, w2f: 2, w4: 4, raw: 0, enc: 0 };

/** How one join's keys are canonicalised (both sides use the same layout). */
export class KeyLayout {
  readonly parts: readonly KeyPart[];
  readonly nullSafe: readonly boolean[];
  /** Bits of key columns compared with IS NOT DISTINCT FROM. */
  readonly nullSafeMask: number;
  /** `words`: fixed-width canonical words (inline in the table); `raw`: one byte key; `enc`. */
  readonly family: "words" | "raw" | "enc";
  /** Canonical words per key (`words` family), including the null-mask word when null-safe. */
  readonly width: number;
  /** Word offset of each column inside the key (words family). */
  readonly offsets: readonly number[];

  constructor(
    readonly types: readonly PhysicalType[],
    nullSafe: readonly boolean[],
  ) {
    if (types.length === 0 || types.length > 31)
      throw new RangeError("A hash join has 1 to 31 key columns");
    this.parts = types.map(partOf);
    this.nullSafe = types.map((_, i) => nullSafe[i] === true);
    let mask = 0;
    for (let i = 0; i < types.length; i++) if (this.nullSafe[i] === true) mask |= 1 << i;
    this.nullSafeMask = mask;
    const offsets: number[] = [];
    let width = 0;
    for (const part of this.parts) {
      offsets.push(width);
      width += WORDS[part];
    }
    this.offsets = offsets;
    const fixed = this.parts.every((part) => WORDS[part] > 0);
    if (mask !== 0) width += 1;
    if (fixed && width <= MAX_INLINE_WORDS) {
      this.family = "words";
      this.width = width;
    } else if (types.length === 1 && this.parts[0] === "raw" && mask === 0) {
      this.family = "raw";
      this.width = 0;
    } else {
      this.family = "enc";
      this.width = 0;
    }
  }

  /** A single integer column (int32-family or int64-family): eligible for direct addressing. */
  get directable(): boolean {
    return (
      this.family === "words" &&
      this.parts.length === 1 &&
      this.nullSafeMask === 0 &&
      (this.parts[0] === "w1" || this.parts[0] === "w2i")
    );
  }
}

// --- hashing -------------------------------------------------------------------------------

export function fmix32(h: number): number {
  h ^= h >>> 16;
  h = Math.imul(h, 0x85eb_ca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2_ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

/** Hash of `width` words at `words[base …]`. */
export function hashWords(words: Int32Array, base: number, width: number): number {
  let h = HASH_SEED;
  for (let w = 0; w < width; w++) {
    h = Math.imul(h ^ (words[base + w] ?? 0), 0x5bd1_e995);
    h ^= h >>> 15;
  }
  return fmix32(h ^ width);
}

/** The hash a join at recursion `level` routes and buckets by (level 0: the hash itself). */
export function levelHash(hash: number, level: number): number {
  return level === 0 ? hash : fmix32((hash ^ Math.imul(level, 0x9e37_79b9)) >>> 0);
}

// --- extraction ------------------------------------------------------------------------------

// Module scratch, sync-only (R23.1): written and read inside one synchronous extraction.
const f64 = new Float64Array(1);
const f64Words = new Int32Array(f64.buffer);

function bitSet(validity: Validity | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/**
 * Per-row scratch for one batch's keys (indexed by position k in the rows handed to `extract`),
 * owned by one operator and reused.
 */
export class KeyScratch {
  readonly capacity: number;
  /** Canonical words, `width` per row (words family). */
  readonly words: Int32Array;
  /** Bit i set: key column i is null in that row. */
  readonly nulls: Int32Array;
  readonly hashes: Uint32Array;
  /** `raw`: the source slot of each row in `rawData` (−1 null). */
  readonly slots: Int32Array;
  rawData: { readonly bytes: Uint8Array; readonly offsets: Int32Array } | null = null;
  /**
   * `raw`: the slots index a dictionary (or a constant), so rows repeat slots: a lookup does one
   * comparison per distinct slot (review B7-p2c-1 #3c).
   */
  rawShared = false;
  /** `enc`: the encoded keys of the rows. */
  readonly encoded: KeyBatch | null;
  /** Rows with a null in a key compared with `=` (they never match). */
  unmatchable = 0;
  /** When `extractKeys` stopped early: the bytes the encoder was refused (else 0). */
  requiredBytes = 0;
  count = 0;

  constructor(
    readonly layout: KeyLayout,
    capacity: number,
    charge: ByteCharge,
    /** What the `enc` encoder's growth is charged to (default `charge`). */
    encodedCharge: ByteCharge = charge,
  ) {
    this.capacity = capacity;
    const bytes = capacity * (4 * Math.max(1, layout.width) + 12);
    if (!charge.allocate(bytes)) throw new RangeError("A join's key scratch must be admitted");
    this.words = new Int32Array(capacity * Math.max(1, layout.width));
    this.nulls = new Int32Array(capacity);
    this.hashes = new Uint32Array(capacity);
    this.slots = new Int32Array(capacity);
    this.encoded = layout.family === "enc" ? new KeyBatch(encodedCharge, 1 << 30) : null;
  }

  static bytes(layout: KeyLayout, capacity: number): number {
    return (
      capacity * (4 * Math.max(1, layout.width) + 12) +
      (layout.family === "enc" ? 4096 + 4 * 65 : 0)
    );
  }
}

/** Writes the canonical words of slot `slot` of `flat` (non-null) at `words[at …]`. */
function writeWords(
  part: KeyPart,
  flat: FlatVector,
  slot: number,
  words: Int32Array,
  at: number,
): void {
  const data = flat.data;
  switch (part) {
    case "w1":
      words[at] =
        data.layout === "bool"
          ? (data.values[slot] ?? 0)
          : ((data as { values: Int32Array }).values[slot] ?? 0);
      return;
    case "w2i": {
      const values = (data as Extract<FlatVector["data"], { layout: "int64" }>).values;
      if (values.repr === "safe") {
        const value = values.values[slot] ?? 0;
        words[at] = value >>> 0;
        words[at + 1] = hiOfSafe(value);
      } else {
        const source = wordsOf(values.values);
        words[at] = source[slot * 2] ?? 0;
        words[at + 1] = source[slot * 2 + 1] ?? 0;
      }
      return;
    }
    case "w2f": {
      let value =
        data.layout === "f32"
          ? (data.values[slot] ?? 0)
          : ((data as { values: Float64Array }).values[slot] ?? 0);
      if (value === 0) value = 0;
      if (value !== value) {
        words[at] = 0;
        words[at + 1] = 0x7ff8_0000;
        return;
      }
      f64[0] = value;
      words[at] = f64Words[0] ?? 0;
      words[at + 1] = f64Words[1] ?? 0;
      return;
    }
    case "w4": {
      const bytes = (data as { bytes: Uint8Array }).bytes;
      const base = slot * 16;
      for (let w = 0; w < 4; w++)
        words[at + w] =
          (bytes[base + w * 4] ?? 0) |
          ((bytes[base + w * 4 + 1] ?? 0) << 8) |
          ((bytes[base + w * 4 + 2] ?? 0) << 16) |
          ((bytes[base + w * 4 + 3] ?? 0) << 24);
      return;
    }
    default:
      return;
  }
}

/** Resolves `vector` row `row` to (flat, slot) or slot −1 for null. */
function sourceSlot(vector: Vector, row: number): number {
  if (vector.form === "flat") return bitSet(vector.validity, row) ? row : -1;
  if (vector.form === "constant") return bitSet(vector.value.validity, 0) ? 0 : -1;
  return bitSet(vector.validity, row) ? (vector.codes[row] ?? 0) : -1;
}

function flatOf(vector: Vector): FlatVector {
  return vector.form === "flat"
    ? vector
    : vector.form === "constant"
      ? vector.value
      : vector.dictionary;
}

/**
 * Canonical keys of rows `ids[from … from + count)` (or `from …` when `ids` is null) of the key
 * vectors `keys` into `out` (positions 0 … count). Returns the rows done: `count`, or fewer when
 * the `enc` encoder was refused memory (the caller borrows and calls again for the rest).
 */
export function extractKeys(
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  out: KeyScratch,
  hash = true,
): number {
  const layout = out.layout;
  const width = layout.width;
  const words = out.words;
  const nulls = out.nulls;
  const hashes = out.hashes;
  if (count > out.capacity) throw new RangeError("More key rows than the scratch holds");
  out.count = count;
  out.requiredBytes = 0;
  let unmatchable = 0;
  // Null masks first (every family needs them).
  nulls.fill(0, 0, count);
  for (let c = 0; c < keys.length; c++) {
    const vector = keys[c];
    if (vector === undefined) continue;
    const validity = vector.form === "constant" ? vector.value.validity : vector.validity;
    if (validity === null) continue;
    const bit = 1 << c;
    for (let k = 0; k < count; k++) {
      const row = ids === null ? from + k : (ids[from + k] ?? 0);
      if (sourceSlot(vector, row) < 0) nulls[k] = (nulls[k] ?? 0) | bit;
    }
  }
  const strict = ~layout.nullSafeMask;
  for (let k = 0; k < count; k++) if (((nulls[k] ?? 0) & strict) !== 0) unmatchable++;
  out.unmatchable = unmatchable;

  if (layout.family === "words") {
    const mask = layout.nullSafeMask;
    for (let c = 0; c < keys.length; c++) {
      const vector = keys[c];
      const part = layout.parts[c];
      if (vector === undefined || part === undefined) continue;
      const offset = layout.offsets[c] ?? 0;
      const flat = flatOf(vector);
      // Fast path: one flat int32 column without nulls.
      if (
        width === 1 &&
        vector.form === "flat" &&
        flat.data.layout === "i32" &&
        vector.validity === null
      ) {
        const values = flat.data.values;
        if (ids === null) words.set(values.subarray(from, from + count));
        else for (let k = 0; k < count; k++) words[k] = values[ids[from + k] ?? 0] ?? 0;
        continue;
      }
      // Fast path: one flat safe int64 column without nulls.
      if (width === 2 && part === "w2i" && vector.form === "flat" && vector.validity === null) {
        const values = (flat.data as Extract<FlatVector["data"], { layout: "int64" }>).values;
        if (values.repr === "safe") {
          const source = values.values;
          for (let k = 0; k < count; k++) {
            const value = source[ids === null ? from + k : (ids[from + k] ?? 0)] ?? 0;
            const hi = Math.floor(value / 4_294_967_296);
            words[k * 2] = value - hi * 4_294_967_296;
            words[k * 2 + 1] = hi;
          }
          continue;
        }
      }
      const span = part === "w1" ? 1 : part === "w4" ? 4 : 2;
      for (let k = 0; k < count; k++) {
        const row = ids === null ? from + k : (ids[from + k] ?? 0);
        const slot = sourceSlot(vector, row);
        const at = k * width + offset;
        if (slot < 0) for (let w = 0; w < span; w++) words[at + w] = 0;
        else writeWords(part, flat, slot, words, at);
      }
    }
    if (mask !== 0)
      for (let k = 0; k < count; k++) words[k * width + width - 1] = (nulls[k] ?? 0) & mask;
    if (!hash) return count;
    if (width === 1)
      for (let k = 0; k < count; k++) hashes[k] = fmix32(((words[k] ?? 0) ^ HASH_SEED) >>> 0);
    else for (let k = 0; k < count; k++) hashes[k] = hashWords(words, k * width, width);
    return count;
  }

  if (layout.family === "raw") {
    const vector = keys[0];
    if (vector === undefined) return count;
    const flat = flatOf(vector);
    const data = flat.data as { bytes: Uint8Array; offsets: Int32Array };
    out.rawData = data;
    const slots = out.slots;
    // A dictionary (or constant) repeats slots: each distinct one is hashed once.
    const shared = vector.form !== "flat";
    out.rawShared = shared;
    const seen = shared ? new Map<number, number>() : null;
    for (let k = 0; k < count; k++) {
      const row = ids === null ? from + k : (ids[from + k] ?? 0);
      const slot = sourceSlot(vector, row);
      slots[k] = slot;
      if (slot < 0 || !hash) {
        hashes[k] = 0;
        continue;
      }
      const known = seen?.get(slot);
      if (known !== undefined) {
        hashes[k] = known;
        continue;
      }
      const h = xxHash32(
        data.bytes,
        HASH_SEED,
        data.offsets[slot] ?? 0,
        data.offsets[slot + 1] ?? 0,
      );
      hashes[k] = h;
      seen?.set(slot, h);
    }
    return count;
  }

  // enc: the persisted equality-key encoding of the active rows.
  const encoded = out.encoded;
  if (encoded === null) return count;
  const columns: KeyColumn[] = keys.map((vector) => ({ vector, direction: "asc", nulls: "first" }));
  const result = encodeKeyBatch(
    "equality",
    columns,
    { length: Number.MAX_SAFE_INTEGER, selection: rowsSelection(ids, from, count) },
    encoded,
  );
  const done = result.encoded;
  out.requiredBytes = result.done ? 0 : result.requiredBytes;
  for (let k = 0; k < done; k++)
    hashes[k] = xxHash32(
      encoded.writer.bytes,
      HASH_SEED,
      encoded.offsets[k] ?? 0,
      encoded.offsets[k + 1] ?? 0,
    );
  out.count = done;
  return done;
}

/** Key bytes one bounded step of key work scans (hashing, encoding, comparing): R23.2. */
export const KEY_STEP_BYTES = 256 * 1024;

function varlenOf(vector: Vector): { bytes: Uint8Array; offsets: Int32Array } | null {
  const data = flatOf(vector).data;
  return data.layout === "varlen" ? data : null;
}

/**
 * How many of rows `from … from + count` (by `ids`) fit `budget` key bytes, at least one:
 * `count` at once when no key is variable-length. `logical`: a dictionary row costs its entry's
 * bytes (work per row, as encoding does); otherwise the dictionary counts once and a row its
 * code (work per distinct entry, as raw keys do).
 */
export function keyStepRows(
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  budget = KEY_STEP_BYTES,
  logical = true,
): number {
  const varlen = keys.filter((vector) => varlenOf(vector) !== null);
  if (varlen.length === 0) return count;
  let bytes = 8 * (keys.length - varlen.length) * count;
  if (!logical)
    for (const vector of varlen)
      if (vector.form !== "flat") {
        const data = varlenOf(vector);
        const n = vector.form === "constant" ? 1 : vector.dictionary.length;
        bytes += (data?.offsets[n] ?? 0) - (data?.offsets[0] ?? 0);
      }
  for (let k = 0; k < count; k++) {
    const row = ids === null ? from + k : (ids[from + k] ?? 0);
    for (const vector of varlen) {
      if (!logical && vector.form !== "flat") {
        bytes += 4;
        continue;
      }
      const slot = sourceSlot(vector, row);
      const data = varlenOf(vector);
      if (slot >= 0 && data !== null)
        bytes += (data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0);
    }
    if (bytes > budget) return Math.max(1, k);
  }
  return count;
}

/** The null mask of each row (bit c: key column c is null), without hashing or encoding. */
export function keyNulls(
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  nulls: Int32Array,
): void {
  nulls.fill(0, 0, count);
  for (let c = 0; c < keys.length; c++) {
    const vector = keys[c];
    if (vector === undefined) continue;
    const validity = vector.form === "constant" ? vector.value.validity : vector.validity;
    if (validity === null) continue;
    const bit = 1 << c;
    for (let k = 0; k < count; k++) {
      const row = ids === null ? from + k : (ids[from + k] ?? 0);
      if (sourceSlot(vector, row) < 0) nulls[k] = (nulls[k] ?? 0) | bit;
    }
  }
}

/** A selection view over explicit row ids for the key encoder (identity rows get a scratch). */
// Module scratch (R23.1), safe across awaits: filled once with identity rows and never written
// again, so a view handed out stays valid when the array is later replaced by a longer one.
let identity = new Uint16Array(0);
function rowsSelection(
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
): { rows: Uint16Array; count: number } {
  if (ids === null) {
    if (identity.length < from + count) {
      identity = new Uint16Array(Math.max(2048, from + count));
      for (let i = 0; i < identity.length; i++) identity[i] = i;
    }
    return { rows: identity.subarray(from, from + count), count };
  }
  if (ids instanceof Uint16Array) return { rows: ids.subarray(from, from + count), count };
  const rows = new Uint16Array(count);
  for (let k = 0; k < count; k++) rows[k] = ids[from + k] ?? 0;
  return { rows, count };
}

/** Byte equality of two ranges. */
export function bytesEqual(
  a: Uint8Array,
  aStart: number,
  aEnd: number,
  b: Uint8Array,
  bStart: number,
  bEnd: number,
): boolean {
  if (aEnd - aStart !== bEnd - bStart) return false;
  for (let i = 0; i < aEnd - aStart; i++) if (a[aStart + i] !== b[bStart + i]) return false;
  return true;
}
