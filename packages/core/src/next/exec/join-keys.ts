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
 * - decimal, interval, several varlen columns, or any mix too wide for inline words (`enc`):
 *   the persisted equality-key encoding (keys/key-encoding.ts) of the columns that are not
 *   text, bytes or enum, which normalizes `1.0 = 1.00` and `1 mon = 30 days`; then each text,
 *   bytes or enum column as a header byte (0 null, 1 a value), its byte length (4 bytes) and
 *   its bytes as stored. Each part is self-delimiting, so equal keys are exactly equal bytes.
 *   Text is not escaped (escaping exists for memcomparable order, which equality does not
 *   need: review B7-p2c-3 #4). The batch's keys get one buffer of their exact size (every part's
 *   length is known first), charged before it exists, and each value is copied into it once in
 *   pieces of key work: no regrowth copies a written prefix (review B7-p2c-4).
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
import { XxHash32Stream, xxHash32 } from "../hash/xxhash32.js";
import { KeyBatch, encodeKeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import type { PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { FlatVector, Validity, Vector } from "../vector/vector.js";
import { UnsupportedPlanError } from "./errors.js";
import { hiOfSafe, wordsOf } from "./column-store.js";
import type { TaskClock } from "./pipeline.js";
import { Slice, runToEnd, type SlicedJob } from "./slices.js";

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
  /** `enc`: the text, bytes and enum columns (length-prefixed), and the others (encoded). */
  readonly rawColumns: readonly number[];
  readonly encodedColumns: readonly number[];

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
    const raw = this.family === "enc" ? this.parts.map((part) => part === "raw") : [];
    this.rawColumns = raw.flatMap((yes, c) => (yes ? [c] : []));
    this.encodedColumns = raw.flatMap((yes, c) => (yes ? [] : [c]));
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
  /**
   * Bytes family: a second hash (`HASH_SEED2`) of keys longer than `LONG_KEY_BYTES`, else 0:
   * with `hashes`, 64 bits that a table compares before any byte (accidental chains vanish).
   */
  readonly hashes2: Uint32Array;
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
  /** `enc` with text, bytes or enum columns: the persisted encoding of the other columns. */
  readonly encodedParts: KeyBatch | null;
  /** What `encoded`'s buffer is charged to (its one sized allocation per batch, `encodeJob`). */
  readonly encodedCharge: ByteCharge;
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
    const bytes = capacity * (4 * Math.max(1, layout.width) + 16);
    if (!charge.allocate(bytes)) throw new RangeError("A join's key scratch must be admitted");
    this.words = new Int32Array(capacity * Math.max(1, layout.width));
    this.nulls = new Int32Array(capacity);
    this.hashes = new Uint32Array(capacity);
    this.hashes2 = new Uint32Array(capacity);
    this.slots = new Int32Array(capacity);
    this.encodedCharge = encodedCharge;
    this.encoded = layout.family === "enc" ? new KeyBatch(encodedCharge, 1 << 30) : null;
    this.encodedParts =
      layout.rawColumns.length > 0 && layout.encodedColumns.length > 0
        ? new KeyBatch(encodedCharge, 1 << 30)
        : null;
  }

  static bytes(layout: KeyLayout, capacity: number): number {
    const batch = 4096 + 4 * 65;
    return (
      capacity * (4 * Math.max(1, layout.width) + 16) +
      (layout.family === "enc" ? batch : 0) +
      (layout.rawColumns.length > 0 && layout.encodedColumns.length > 0 ? batch : 0)
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

/** The null masks and unmatchable count of rows `from … from + count` (every family). */
function extractNullMasks(
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  out: KeyScratch,
): void {
  if (count > out.capacity) throw new RangeError("More key rows than the scratch holds");
  out.count = count;
  out.requiredBytes = 0;
  keyNulls(keys, ids, from, count, out.nulls);
  const strict = ~out.layout.nullSafeMask;
  let unmatchable = 0;
  for (let k = 0; k < count; k++) if (((out.nulls[k] ?? 0) & strict) !== 0) unmatchable++;
  out.unmatchable = unmatchable;
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
  // Null masks first (every family needs them).
  extractNullMasks(keys, ids, from, count, out);

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
    const seenAt = shared ? new Map<number, number>() : null;
    for (let k = 0; k < count; k++) {
      const row = ids === null ? from + k : (ids[from + k] ?? 0);
      const slot = sourceSlot(vector, row);
      slots[k] = slot;
      if (slot < 0 || !hash) {
        hashes[k] = 0;
        out.hashes2[k] = 0;
        continue;
      }
      const known = seen?.get(slot);
      if (known !== undefined) {
        hashes[k] = known;
        out.hashes2[k] = out.hashes2[seenAt?.get(slot) ?? 0] ?? 0;
        continue;
      }
      hashInto(out, k, data.bytes, data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0);
      seen?.set(slot, hashes[k] ?? 0);
      seenAt?.set(slot, k);
    }
    return count;
  }

  // enc: the key encoding of the active rows (see the module comment), all at once.
  const encoded = out.encoded;
  if (encoded === null) return count;
  const done = runToEnd(encodeJob(keys, ids, from, count, out, new KeyWork(), UNBOUNDED));
  if (hash)
    for (let k = 0; k < done; k++)
      hashInto(out, k, encoded.writer.bytes, encoded.offsets[k] ?? 0, encoded.offsets[k + 1] ?? 0);
  out.count = done;
  return done;
}

/** A slice that is never due (the synchronous paths). */
const UNBOUNDED = new Slice(Infinity);

/**
 * The `enc` keys of rows `from … from + count` into `out.encoded` (the module comment's form):
 * the persisted encoding of the non-varlen columns, then each text, bytes or enum column
 * length-prefixed and copied in steps of key work, so an oversized value spans steps. Returns
 * the rows done: fewer when the grant refused bytes (`out.requiredBytes`).
 */
function* encodeJob(
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  out: KeyScratch,
  work: KeyWork,
  slice: Slice,
): SlicedJob<number> {
  const layout = out.layout;
  const encoded = out.encoded;
  if (encoded === null) return count;
  const selection = rowsSelection(ids, from, count);
  const columnsOf = (list: readonly number[]): KeyColumn[] =>
    list.map((c) => {
      const vector = keys[c];
      if (vector === undefined) throw new RangeError("Join key column out of range");
      return { vector, direction: "asc", nulls: "first" };
    });
  if (layout.rawColumns.length === 0) {
    // No varlen column: the persisted encoding is the key.
    const result = encodeKeyBatch(
      "equality",
      columnsOf(layout.encodedColumns),
      { length: Number.MAX_SAFE_INTEGER, selection },
      encoded,
    );
    out.requiredBytes = result.done ? 0 : result.requiredBytes;
    if (work.spend(encoded.writer.length + 4 * result.encoded, slice)) yield;
    return result.encoded;
  }
  let n = count;
  out.requiredBytes = 0;
  const parts = out.encodedParts;
  if (parts !== null) {
    const result = encodeKeyBatch(
      "equality",
      columnsOf(layout.encodedColumns),
      { length: Number.MAX_SAFE_INTEGER, selection },
      parts,
    );
    if (!result.done) {
      n = result.encoded;
      out.requiredBytes = result.requiredBytes;
    }
    if (work.spend(parts.writer.length + 4 * n, slice)) yield;
  }
  const writer = encoded.writer;
  writer.reset();
  writer.limit = Infinity;
  encoded.count = 0;
  if (!encoded.reserveOffsets(n + 1)) {
    out.requiredBytes = 4 * (n + 1);
    return 0;
  }
  encoded.offsets[0] = 0;
  const raw = layout.rawColumns;
  // Every part's encoded length is known before any copy (the persisted part, then per varlen
  // column a header, a 4-byte length and the bytes): the batch's keys get one buffer of their
  // exact size, charged before it exists, so no regrowth ever copies a written prefix (review
  // B7-p2c-3 #4 / B7-p2c-4). When the grant refuses, as many leading rows as fit (at least one).
  const ends = new Float64Array(n + 1);
  for (let k = 0; k < n; k++) {
    const row = selection.rows[k] ?? 0;
    let size = parts === null ? 0 : (parts.offsets[k + 1] ?? 0) - (parts.offsets[k] ?? 0);
    for (const c of raw) {
      const vector = keys[c];
      if (vector === undefined) continue;
      const slot = sourceSlot(vector, row);
      if (slot < 0) size += 1;
      else {
        const offsets = (flatOf(vector).data as { offsets: Int32Array }).offsets;
        size += 5 + (offsets[slot + 1] ?? 0) - (offsets[slot] ?? 0);
      }
    }
    ends[k + 1] = (ends[k] ?? 0) + size;
  }
  if (work.spend(8 * n, slice)) yield;
  for (;;) {
    const total = ends[n] ?? 0;
    if (total <= writer.bytes.length) break;
    if (out.encodedCharge.allocate(total)) {
      // The writer is empty (reset): nothing is copied into the new buffer.
      out.encodedCharge.free(writer.bytes.length);
      writer.bytes = new Uint8Array(total);
      break;
    }
    if (n === 1) {
      out.requiredBytes = total;
      return 0;
    }
    n = Math.max(1, n >> 1);
    out.requiredBytes = total;
  }
  for (let k = 0; k < n; k++) {
    const row = selection.rows[k] ?? 0;
    const keyStart = writer.length;
    try {
      if (parts !== null) {
        const start = parts.offsets[k] ?? 0;
        writer.fixed(parts.writer.bytes, start, (parts.offsets[k + 1] ?? 0) - start);
      }
      for (const c of raw) {
        const vector = keys[c];
        if (vector === undefined) continue;
        const slot = sourceSlot(vector, row);
        if (slot < 0) {
          writer.byte(0);
          continue;
        }
        const data = flatOf(vector).data as { bytes: Uint8Array; offsets: Int32Array };
        const start = data.offsets[slot] ?? 0;
        const end = data.offsets[slot + 1] ?? 0;
        writer.byte(1);
        writer.int32(end - start);
        for (let at = start; at < end;) {
          const piece = Math.min(end - at, Math.max(WORK_CHUNK, work.left));
          writer.fixed(data.bytes, at, piece);
          at += piece;
          if (work.spend(piece, slice)) yield;
        }
      }
    } catch (error) {
      // The writer's refusal (its grant denied the growth): stop before this row.
      if (!(error instanceof Error) || !("requiredBytes" in error)) throw error;
      writer.length = keyStart;
      out.requiredBytes = Number(error.requiredBytes);
      return k;
    }
    encoded.offsets[k + 1] = writer.length;
    encoded.count = k + 1;
    if (work.spend(16, slice)) yield;
  }
  return n;
}

/** Key bytes one bounded step of key work scans (hashing, encoding, comparing): R23.2. */
export const KEY_STEP_BYTES = 256 * 1024;

/** Keys longer than this get a second hash (`HASH_SEED2`): 64 bits before any byte compare. */
export const LONG_KEY_BYTES = 64;
export const HASH_SEED2 = 0x6a09_e667;

/** Both hashes of one key, at once (the synchronous paths; the key fits a step). */
function hashInto(out: KeyScratch, k: number, bytes: Uint8Array, start: number, end: number): void {
  out.hashes[k] = xxHash32(bytes, HASH_SEED, start, end);
  out.hashes2[k] = end - start > LONG_KEY_BYTES ? xxHash32(bytes, HASH_SEED2, start, end) : 0;
}

/**
 * A budget of key bytes per step (review B7-p2c-2: every byte of hashing and equality counts).
 * `spend(bytes, slice)` returns true ("yield now") when the step is used up and the slice is
 * due; the budget then starts again. Every caller must yield on true before more key work
 * (review B7-p2c-3 #1): the next `spend` throws when no task began on the slice's clock since a
 * "yield now" (the runner's yield begins one), so an ignored signal fails loudly rather than
 * running a chain in one step. `total` is the bytes spent overall (tests read it per step).
 */
export class KeyWork {
  left: number;
  total = 0;
  /** The clock and its task count when "yield now" was last returned (else null). */
  #owedClock: TaskClock | null = null;
  #owedAt = 0;
  constructor(readonly step = KEY_STEP_BYTES) {
    this.left = step;
  }
  spend(bytes: number, slice: Slice): boolean {
    const owed = this.#owedClock;
    if (owed !== null) {
      if (owed.epoch === this.#owedAt)
        throw new Error("Key work went on past a due slice: a caller ignored spend's yield");
      this.#owedClock = null;
    }
    this.total += bytes;
    this.left -= bytes;
    if (this.left > 0) return false;
    this.left = this.step;
    if (!slice.due()) return false;
    this.#owedClock = slice.clock;
    this.#owedAt = slice.clock.epoch;
    return true;
  }
}

/** Bytes a resumable job takes at once before it spends them (at least this, to make progress). */
const WORK_CHUNK = 4096;

/**
 * Both hashes of `bytes[start, end)` into `out[0]`, `out[1]` (the second 0 for short keys), in
 * steps of the budget: a key longer than what is left is streamed (XxHash32Stream), so an
 * oversized key spans many steps. Equal to `hashInto` for every key.
 */
export function* hashKeyJob(
  bytes: Uint8Array,
  start: number,
  end: number,
  work: KeyWork,
  slice: Slice,
  out: Uint32Array,
): SlicedJob<void> {
  const length = end - start;
  const long = length > LONG_KEY_BYTES;
  if ((long ? 2 * length : length) <= Math.max(work.left, WORK_CHUNK)) {
    out[0] = xxHash32(bytes, HASH_SEED, start, end);
    out[1] = long ? xxHash32(bytes, HASH_SEED2, start, end) : 0;
    if (work.spend(long ? 2 * length : length, slice)) yield;
    return;
  }
  const first = new XxHash32Stream(HASH_SEED);
  const second = new XxHash32Stream(HASH_SEED2);
  for (let at = start; at < end;) {
    const n = Math.min(end - at, Math.max(WORK_CHUNK, work.left >>> 1));
    first.update(bytes, at, at + n);
    second.update(bytes, at, at + n);
    at += n;
    if (work.spend(2 * n, slice)) yield;
  }
  out[0] = first.digest();
  out[1] = second.digest();
}

/**
 * Whether `a[as, ae)` equals `b[bs, be)`, compared in steps of the budget (a cursor that may
 * suspend mid-key; callers keep their chain position across it).
 */
export function* equalJob(
  a: Uint8Array,
  as: number,
  ae: number,
  b: Uint8Array,
  bs: number,
  be: number,
  work: KeyWork,
  slice: Slice,
): SlicedJob<boolean> {
  const length = ae - as;
  if (length !== be - bs) return false;
  for (let at = 0; at < length;) {
    const n = Math.min(length - at, Math.max(WORK_CHUNK, work.left));
    for (let i = 0; i < n; i++)
      if (a[as + at + i] !== b[bs + at + i]) {
        // A mismatch spends what it compared, and honours the step like any other spend.
        if (work.spend(i + 1, slice)) yield;
        return false;
      }
    at += n;
    if (work.spend(n, slice)) yield;
  }
  return true;
}

/**
 * `extractKeys` as a sliced job: the bytes family's hashing goes through `hashKeyJob` (every
 * byte counted, long keys streamed) and repeated dictionary slots are hashed once per `memo`
 * (a caller's per-batch cache: slot → [h1, h2]); the words family is extracted at once.
 */
export function* extractKeysJob(
  keys: readonly Vector[],
  ids: ArrayLike<number> | null,
  from: number,
  count: number,
  out: KeyScratch,
  work: KeyWork,
  slice: Slice,
  memo: Map<number, readonly [number, number]> | null = null,
): SlicedJob<number> {
  const layout = out.layout;
  if (layout.family === "words") {
    const done = extractKeys(keys, ids, from, count, out, true);
    if (work.spend(8 * Math.max(1, layout.width) * count, slice)) yield;
    return done;
  }
  let done: number;
  if (layout.family === "enc") {
    // Null masks (no encoding), then the key encoding in steps of key work.
    extractNullMasks(keys, ids, from, count, out);
    done = yield* encodeJob(keys, ids, from, count, out, work, slice);
    out.count = done;
  } else done = extractKeys(keys, ids, from, count, out, false);
  const pair = new Uint32Array(2);
  const strict = ~layout.nullSafeMask;
  for (let k = 0; k < done; k++) {
    let bytes: Uint8Array;
    let start: number;
    let end: number;
    if (layout.family === "raw") {
      const slot = out.slots[k] ?? -1;
      if (slot < 0 || ((out.nulls[k] ?? 0) & strict) !== 0) {
        out.hashes[k] = 0;
        out.hashes2[k] = 0;
        continue;
      }
      const known = memo?.get(slot);
      if (known !== undefined) {
        out.hashes[k] = known[0];
        out.hashes2[k] = known[1];
        continue;
      }
      const data = out.rawData;
      if (data === null) continue;
      bytes = data.bytes;
      start = data.offsets[slot] ?? 0;
      end = data.offsets[slot + 1] ?? 0;
      yield* hashKeyJob(bytes, start, end, work, slice, pair);
      out.hashes[k] = pair[0] ?? 0;
      out.hashes2[k] = pair[1] ?? 0;
      if (out.rawShared) memo?.set(slot, [pair[0] ?? 0, pair[1] ?? 0]);
      continue;
    }
    const encoded = out.encoded;
    if (encoded === null) break;
    bytes = encoded.writer.bytes;
    start = encoded.offsets[k] ?? 0;
    end = encoded.offsets[k + 1] ?? 0;
    yield* hashKeyJob(bytes, start, end, work, slice, pair);
    out.hashes[k] = pair[0] ?? 0;
    out.hashes2[k] = pair[1] ?? 0;
  }
  return done;
}

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
