/**
 * Sort keys for the sort, top-N and per-group top-k operators (track B7, increment 2).
 *
 * ## Law (PostgreSQL ORDER BY; DECISIONS.md C7/C8)
 * - Each key is a column with ASC/DESC and NULLS FIRST/LAST; NaN sorts above every number,
 *   −0 = 0, NUMERIC by value (`1.0` = `1.00`), INTERVAL by its normalized span, an enum by its
 *   declaration rank, text in C collation (byte order) unless the key names an ICU collation.
 * - **Key mode.** Every key encodes to the frozen sort-key bytes (keys/key-encoding.ts): byte
 *   order is the order. Rows carry the bytes and their first `PREFIX_WORDS` big-endian 32-bit
 *   words, so the in-memory sort is a radix sort over the words with a full byte comparison only
 *   among rows whose prefixes tie, and a merge compares words first.
 * - **Ranks (C8).** A key with an ICU collation sorts by a query-wide rank: the distinct values,
 *   ordered by `Intl.Collator(locale).compare` with ties broken by bytes, numbered; the rank
 *   (an int32) takes the text's place in the key bytes. Ranks need every distinct value first.
 * - **Compare mode (C8's external fallback, and JSON/array keys, C7).** When the ranks do not
 *   fit, or a key is JSON or an array, rows are ordered by a comparator over the values
 *   themselves (`compareRows`): exec/semantic.ts for JSONB and arrays, `Intl.Collator` on decoded
 *   strings for ICU text (the one documented case that decodes, charged by the caller).
 * Ties are equal keys: the sorts are not stable, as SQL allows.
 */
import { encodeKeyBatch, enumRank, KeyBatch, type KeyColumn } from "../keys/key-encoding.js";
import type { Collation } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { decodeArrayValue } from "../vector/array-value.js";
import { int128At } from "../vector/int128.js";
import { decodeUtf8, type ByteCharge } from "../vector/varlen.js";
import type { FlatVector, Validity, Vector } from "../vector/vector.js";
import { wordsOf } from "./column-store.js";
import { yieldTask } from "./pipeline.js";
import {
  compareArrays,
  compareDecimals,
  compareJsonb,
  parseJsonb,
  type JsonbValue,
  type SqlArray,
} from "./semantic.js";

export interface SortColumn {
  /** Input column the key reads (the executor projects key expressions first). */
  readonly column: number;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
  readonly collation: Collation;
}

/**
 * Big-endian 32-bit words of key prefix each row carries (20 bytes: two fixed-width keys such as
 * `f09, id`). Keys are prefix-free, so when no key is longer the words alone order the rows.
 */
export const PREFIX_WORDS = 5;

/** Whether `collation` is the byte order (C), so no ranks are needed. */
export function isByteCollation(collation: Collation): boolean {
  return (
    collation === null ||
    collation === "C" ||
    collation === "POSIX" ||
    collation === "default" ||
    collation === "ucs_basic"
  );
}

/** How a sort over these keys runs: bytes, ICU ranks, or a value comparator. */
export function sortMode(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
): "keys" | "ranks" | "compare" {
  let ranks = false;
  for (const key of keys) {
    const type = types[key.column];
    if (type === undefined) throw new RangeError("Sort key column out of range");
    if (type.kind === "json" || type.kind === "array" || type.kind === "locator") return "compare";
    if (!isByteCollation(key.collation)) {
      if (type.kind !== "text")
        throw new TypeError(`COLLATE applies to text, not ${type.kind} (lowering checks)`);
      ranks = true;
    }
  }
  return ranks ? "ranks" : "keys";
}

/** Writes the first `PREFIX_WORDS × 4` bytes of `bytes[start … end)` as big-endian words (zero padded). */
export function writePrefix(
  bytes: Uint8Array,
  start: number,
  end: number,
  out: Int32Array,
  at: number,
): void {
  for (let w = 0; w < PREFIX_WORDS; w++) {
    let word = 0;
    for (let b = 0; b < 4; b++) {
      const index = start + w * 4 + b;
      word = (word << 8) | (index < end ? (bytes[index] ?? 0) : 0);
    }
    out[at + w] = word;
  }
}

/** Unsigned byte order of two ranges: −1, 0, 1. */
export function compareRanges(
  a: Uint8Array,
  aStart: number,
  aEnd: number,
  b: Uint8Array,
  bStart: number,
  bEnd: number,
): number {
  const length = Math.min(aEnd - aStart, bEnd - bStart);
  for (let i = 0; i < length; i++) {
    const d = (a[aStart + i] ?? 0) - (b[bStart + i] ?? 0);
    if (d !== 0) return d < 0 ? -1 : 1;
  }
  const d = aEnd - aStart - (bEnd - bStart);
  return d === 0 ? 0 : d < 0 ? -1 : 1;
}

/**
 * Encodes the sort keys of rows `ids[from … from + count)` (or `from …`) of `columns` into a
 * reusable `KeyBatch` plus dense prefix words: the trailing columns rows carry.
 */
export class SortKeyEncoder {
  readonly keys: KeyBatch;
  readonly prefixes: Int32Array[];
  readonly bytesVector: FlatVector;
  readonly prefixVectors: FlatVector[];
  constructor(
    readonly sortColumns: readonly SortColumn[],
    charge: ByteCharge,
    capacity = 2048,
  ) {
    this.keys = new KeyBatch(charge, 1 << 30);
    this.prefixes = Array.from({ length: PREFIX_WORDS }, () => new Int32Array(capacity));
    if (!charge.allocate(PREFIX_WORDS * capacity * 4))
      throw new RangeError("A sort key encoder must be admitted");
    this.bytesVector = {
      form: "flat",
      type: { kind: "bytes" },
      length: 0,
      validity: null,
      data: { layout: "varlen", bytes: new Uint8Array(0), offsets: new Int32Array(1) },
    };
    this.prefixVectors = this.prefixes.map((values): FlatVector => ({
      form: "flat",
      type: { kind: "int32", bits: 32 },
      length: 0,
      validity: null,
      data: { layout: "i32", values },
    }));
  }

  /**
   * Encodes; returns the rows done (fewer when the key bytes were refused: `requiredBytes`).
   * `substitute[k]`, when set, replaces key k's column (ICU ranks).
   */
  encode(
    columns: readonly Vector[],
    ids: Uint16Array | null,
    from: number,
    count: number,
    substitute: ReadonlyArray<Vector | null> | null = null,
  ): { done: number; requiredBytes: number } {
    const keyColumns: KeyColumn[] = this.sortColumns.map((key, k) => ({
      vector: substitute?.[k] ?? columnAt(columns, key.column),
      direction: key.direction,
      nulls: key.nulls,
    }));
    const selection =
      ids === null ? identity(from, count) : { rows: ids.subarray(from, from + count), count };
    const result = encodeKeyBatch("sort", keyColumns, { length: 1 << 16, selection }, this.keys);
    const done = result.encoded;
    const keys = this.keys;
    const bytes = keys.writer.bytes;
    for (let k = 0; k < done; k++) {
      const start = keys.offsets[k] ?? 0;
      const end = keys.offsets[k + 1] ?? 0;
      for (let w = 0; w < PREFIX_WORDS; w++) {
        let word = 0;
        for (let b = 0; b < 4; b++) {
          const index = start + w * 4 + b;
          word = (word << 8) | (index < end ? (bytes[index] ?? 0) : 0);
        }
        const prefix = this.prefixes[w];
        if (prefix !== undefined) prefix[k] = word;
      }
    }
    // The dense tail vectors: row k of this call.
    (this.bytesVector as { data: unknown }).data = {
      layout: "varlen",
      bytes,
      offsets: keys.offsets,
    };
    this.bytesVector.length = done;
    for (const vector of this.prefixVectors) vector.length = done;
    return { done, requiredBytes: result.done ? 0 : result.requiredBytes };
  }

  /** The tail vectors after `encode`: key bytes, then the prefix words. */
  get tail(): Vector[] {
    return [this.bytesVector, ...this.prefixVectors];
  }

  close(charge: ByteCharge): void {
    this.keys.close();
    charge.free(PREFIX_WORDS * (this.prefixes[0]?.length ?? 0) * 4);
  }
}

function columnAt(columns: readonly Vector[], c: number): Vector {
  const column = columns[c];
  if (column === undefined) throw new RangeError(`No sort column ${String(c)}`);
  return column;
}

// Module scratch (R23.1), safe across awaits: filled once with identity rows and never written
// again, so a view handed out stays valid when the array is later replaced by a longer one.
let identityRows = new Uint16Array(0);
function identity(from: number, count: number): { rows: Uint16Array; count: number } {
  if (identityRows.length < from + count) {
    identityRows = new Uint16Array(Math.max(2048, from + count));
    for (let i = 0; i < identityRows.length; i++) identityRows[i] = i;
  }
  return { rows: identityRows.subarray(from, from + count), count };
}

/**
 * The deadline of one slice of synchronous work (R16.4, R23.2): `due()` once `ms` have passed
 * since the slice began. Long loops check it per chunk of rows, never per element.
 */
export class Slice {
  #start: number;
  constructor(
    readonly ms: number,
    readonly now: () => number = () => performance.now(),
  ) {
    this.#start = now();
  }
  due(): boolean {
    return this.now() - this.#start >= this.ms;
  }
  reset(): void {
    this.#start = this.now();
  }
}

/** Rows a sliced loop runs between deadline checks. */
export const SLICE_ROWS = 8192;

/**
 * Sorts row ids 0 … n by unsigned prefix words (`words`, PREFIX_WORDS per row) then, among equal
 * prefixes, by `refine` (full key bytes) when given. LSD radix with 16-bit digits, skipping
 * digits every row shares; `scratch` holds 2 × n × (PREFIX_WORDS + 1) words. A generator that
 * yields when `slice` is due (checked every `SLICE_ROWS` rows), so the caller can give the event
 * loop a task between slices.
 */
export function* sortByPrefixJob(
  n: number,
  words: Int32Array,
  ids: Int32Array,
  scratch: Uint32Array,
  refine: ((a: number, b: number) => number) | null,
  used: number,
  slice: Slice,
): Generator<void, void, void> {
  const stride = PREFIX_WORDS + 1;
  if (n < 512) {
    const view = ids.subarray(0, n);
    for (let i = 0; i < n; i++) view[i] = i;
    view.sort((a, b) => {
      for (let w = 0; w < PREFIX_WORDS; w++) {
        const x = (words[a * PREFIX_WORDS + w] ?? 0) >>> 0;
        const y = (words[b * PREFIX_WORDS + w] ?? 0) >>> 0;
        if (x !== y) return x < y ? -1 : 1;
      }
      return refine === null ? 0 : refine(a, b);
    });
    return;
  }
  let from = scratch.subarray(0, n * stride);
  let to = scratch.subarray(n * stride, 2 * n * stride);
  for (let i = 0; i < n; i++) {
    for (let w = 0; w < PREFIX_WORDS; w++)
      from[i * stride + w] = (words[i * PREFIX_WORDS + w] ?? 0) >>> 0;
    from[i * stride + PREFIX_WORDS] = i;
    if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
  }
  // Operator-owned histogram: the job may pause between slices (R23.1).
  const counts = new Uint32Array(65_536);
  for (let w = Math.min(used, PREFIX_WORDS) - 1; w >= 0; w--)
    for (let shift = 0; shift <= 16; shift += 16) {
      counts.fill(0);
      for (let i = 0; i < n; i++) {
        const digit = ((from[i * stride + w] ?? 0) >>> shift) & 0xffff;
        counts[digit] = (counts[digit] ?? 0) + 1;
        if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
      }
      // Every row has the same digit: the pass changes nothing.
      const first = ((from[w] ?? 0) >>> shift) & 0xffff;
      if (counts[first] === n) continue;
      let total = 0;
      for (let d = 0; d < 65_536; d++) {
        const c = counts[d] ?? 0;
        counts[d] = total;
        total += c;
      }
      for (let i = 0; i < n; i++) {
        const base = i * stride;
        const digit = ((from[base + w] ?? 0) >>> shift) & 0xffff;
        const at = (counts[digit] ?? 0) * stride;
        counts[digit] = (counts[digit] ?? 0) + 1;
        for (let x = 0; x < stride; x++) to[at + x] = from[base + x] ?? 0;
        if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
      }
      const swap = from;
      from = to;
      to = swap;
    }
  for (let i = 0; i < n; i++) {
    ids[i] = from[i * stride + PREFIX_WORDS] ?? 0;
    if ((i & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
  }
  if (refine === null) return;
  // Runs of equal prefixes: order them by the full key, merging through the free half of the
  // scratch (`to`, n × stride words), so refinement allocates nothing.
  const temp = new Int32Array(to.buffer, to.byteOffset, n);
  let start = 0;
  for (let i = 1; i <= n; i++) {
    let same = i < n;
    for (let w = 0; w < PREFIX_WORDS && same; w++)
      same = from[i * stride + w] === from[start * stride + w];
    if ((i & (SLICE_ROWS - 1)) === 0 && slice.due()) yield;
    if (same) continue;
    if (i - start > 1)
      yield* mergeSortJob(ids.subarray(start, i), temp.subarray(start, i), refine, slice);
    start = i;
  }
}

/**
 * Sorts `ids` by `compare` (stably) in slices: blocks of 1,024 sorted directly, then bottom-up
 * merges through `temp` (at least `ids.length`, the caller's charged scratch), yielding when
 * `slice` is due (checked every `SLICE_ROWS` elements moved).
 */
export function* mergeSortJob(
  ids: Int32Array,
  temp: Int32Array,
  compare: (a: number, b: number) => number,
  slice: Slice,
): Generator<void, void, void> {
  const n = ids.length;
  const block = 1024;
  for (let at = 0; at < n; at += block) {
    ids.subarray(at, Math.min(n, at + block)).sort(compare);
    if (slice.due()) yield;
  }
  if (n <= block) return;
  let from: Int32Array = ids;
  let to: Int32Array = temp.subarray(0, n);
  let moved = 0;
  for (let width = block; width < n; width *= 2) {
    for (let lo = 0; lo < n; lo += 2 * width) {
      const mid = Math.min(n, lo + width);
      const hi = Math.min(n, lo + 2 * width);
      let i = lo;
      let j = mid;
      let k = lo;
      while (i < mid && j < hi) {
        to[k++] = compare(from[i] ?? 0, from[j] ?? 0) <= 0 ? (from[i++] ?? 0) : (from[j++] ?? 0);
        if ((++moved & (SLICE_ROWS - 1)) === 0 && slice.due()) yield;
      }
      while (i < mid) to[k++] = from[i++] ?? 0;
      while (j < hi) to[k++] = from[j++] ?? 0;
      if (slice.due()) yield;
    }
    const swap = from;
    from = to;
    to = swap;
  }
  if (from !== ids) ids.set(from);
}

/**
 * A sliced job: yields nothing when its slice is due, or a Promise it is blocked on (a
 * downstream's `ready()`, a progress loan).
 */
export type SlicedJob<T> = Generator<Promise<void> | void, T, void>;

/**
 * Runs a sliced job: synchronously while it neither blocks nor runs out of slice (the result,
 * no Promise). A due slice gets a task yield (`yieldTask`, never a resolved Promise) and a fresh
 * slice; a blocked job awaits its Promise. `between()` (the abort check) runs at every
 * resumption.
 */
export function runSliced<T>(job: SlicedJob<T>, slice: Slice, between: () => void): T | Promise<T> {
  let step = job.next();
  if (step.done === true) return step.value;
  return (async () => {
    for (;;) {
      const blocked = step.value;
      if (blocked === undefined) {
        await yieldTask();
        slice.reset();
      } else await blocked;
      between();
      step = job.next();
      if (step.done === true) return step.value;
    }
  })();
}

/** Runs a job that never blocks to completion, synchronously (no deadline). */
export function runToEnd<T>(job: SlicedJob<T>): T {
  for (;;) {
    const step = job.next();
    if (step.done === true) return step.value;
    if (step.value !== undefined) throw new Error("A synchronous sort job blocked");
  }
}

/** Bytes `sortByPrefixJob` needs in `scratch` for n rows. */
export function sortScratchBytes(n: number): number {
  return n >= 512 ? 2 * n * (PREFIX_WORDS + 1) * 4 : 0;
}

// --- compare mode -------------------------------------------------------------------------

function bit(validity: Validity | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/** (flat, slot) of a row, slot −1 when null. */
function resolveRow(vector: Vector, row: number): [FlatVector, number] {
  if (vector.form === "flat") return [vector, bit(vector.validity, row) ? row : -1];
  if (vector.form === "constant") return [vector.value, bit(vector.value.validity, 0) ? 0 : -1];
  return [vector.dictionary, bit(vector.validity, row) ? (vector.codes[row] ?? 0) : -1];
}

// A cache, not scratch (R23.1): collators hold no per-call state.
const collators = new Map<string, Intl.Collator>();
/** The collator of an ICU collation name (one per name). */
export function collatorFor(collation: string): Intl.Collator {
  let collator = collators.get(collation);
  if (collator === undefined) {
    collator = new Intl.Collator(collation.replace(/-x-icu$/u, ""));
    collators.set(collation, collator);
  }
  return collator;
}

function compareNumbers(x: number, y: number): number {
  // NaN above every number, NaN = NaN, −0 = 0.
  if (x !== x) return y !== y ? 0 : 1;
  if (y !== y) return -1;
  return x < y ? -1 : x > y ? 1 : 0;
}

function int64Of(data: Extract<FlatVector["data"], { layout: "int64" }>, slot: number): bigint {
  const values = data.values;
  if (values.repr === "safe") return BigInt(values.values[slot] ?? 0);
  const words = wordsOf(values.values);
  return BigInt.asIntN(
    64,
    (BigInt(words[slot * 2 + 1] ?? 0) << 32n) | BigInt((words[slot * 2] ?? 0) >>> 0),
  );
}

/** An operand compare mode decodes once per row: ICU text, a JSONB tree, an array. */
export type Operand = string | JsonbValue | SqlArray;

/**
 * Decoded operands of a set of rows (the resident rows, one merge page, one held row): per key,
 * one operand per row, or null for keys compared natively. Every operand was charged before it
 * was built; `bytes` is what the holder frees.
 */
export interface Operands {
  readonly values: ReadonlyArray<Array<Operand | null> | null>;
  rows: number;
  bytes: number;
}

/** Whether key `key` of `types` is decoded in compare mode. */
function decodes(type: PhysicalType | undefined, key: SortColumn): boolean {
  return (
    type?.kind === "json" ||
    type?.kind === "array" ||
    (type?.kind === "text" && !isByteCollation(key.collation))
  );
}

/** Whether compare mode decodes any of these keys. */
export function needsOperands(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
): boolean {
  return keys.some((key) => decodes(types[key.column], key));
}

/** An empty operand set for these keys (nothing charged). */
export function emptyOperands(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
): Operands {
  return {
    values: keys.map((key) => (decodes(types[key.column], key) ? [] : null)),
    rows: 0,
    bytes: 0,
  };
}

/**
 * Bytes `decodeOperands` charges for rows `from … to` of `columns`: a conservative size (JS
 * strings at two bytes per UTF-8 byte; JSONB trees and arrays at a per-byte allowance), from the
 * encoded lengths alone.
 */
export function operandBytes(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  columns: readonly Vector[],
  from: number,
  to: number,
): number {
  let bytes = 0;
  for (const key of keys) {
    const type = types[key.column];
    if (!decodes(type, key)) continue;
    const vector = columnAt(columns, key.column);
    const perByte = type?.kind === "text" ? 2 : type?.kind === "json" ? 24 : 32;
    for (let row = from; row < to; row++) {
      const [flat, slot] = resolveRow(vector, row);
      bytes += 8;
      if (slot < 0) continue;
      const data = flat.data as { offsets: Int32Array };
      bytes += 32 + perByte * ((data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0));
    }
  }
  return bytes;
}

/**
 * Decodes the operands of rows `from … to` of `columns` onto the end of `into`, charging
 * `operandBytes` first; returns 0, or the bytes needed with nothing allocated or appended when
 * the charge refuses.
 */
export function decodeOperands(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  columns: readonly Vector[],
  from: number,
  to: number,
  charge: ByteCharge,
  into: Operands,
): number {
  const bytes = operandBytes(types, keys, columns, from, to);
  if (bytes > 0 && !charge.allocate(bytes)) return bytes;
  const length = into.values.map((out) => out?.length ?? 0);
  try {
    for (let k = 0; k < keys.length; k++) {
      const out = into.values[k];
      const key = keys[k];
      const type = key === undefined ? undefined : types[key.column];
      if (out === null || out === undefined || key === undefined || type === undefined) continue;
      const vector = columnAt(columns, key.column);
      for (let row = from; row < to; row++) {
        const [flat, slot] = resolveRow(vector, row);
        if (slot < 0) {
          out.push(null);
          continue;
        }
        const data = flat.data as { bytes: Uint8Array; offsets: Int32Array; layout: "varlen" };
        if (type.kind === "json") out.push(parseJsonb(decodeUtf8(data, slot)));
        else if (type.kind === "array") {
          const element = type.element;
          const start = data.offsets[slot] ?? 0;
          const value = decodeArrayValue(
            element,
            data.bytes.subarray(start, data.offsets[slot + 1] ?? 0),
          );
          out.push({ element, ...value });
        } else out.push(decodeUtf8(data, slot));
      }
    }
  } catch (error) {
    // A value that does not decode: nothing stays appended or charged.
    into.values.forEach((out, k) => {
      if (out !== null) out.length = length[k] ?? 0;
    });
    if (bytes > 0) charge.free(bytes);
    throw error;
  }
  into.rows += to - from;
  into.bytes += bytes;
  return 0;
}

/** Empties an operand set, freeing its charge. */
export function dropOperands(operands: Operands, charge: ByteCharge): void {
  if (operands.bytes > 0) charge.free(operands.bytes);
  for (const out of operands.values) if (out !== null) out.length = 0;
  operands.rows = 0;
  operands.bytes = 0;
}

/** Order of two non-null values of one sort column's type (ascending). */
function compareValues(
  type: PhysicalType,
  a: FlatVector,
  i: number,
  b: FlatVector,
  j: number,
  collation: Collation,
  // Missing (undefined) unless the caller decoded them: JSONB `null` is a decoded operand.
  p?: Operand | null,
  q?: Operand | null,
): number {
  const x = a.data;
  const y = b.data;
  switch (x.layout) {
    case "bool":
    case "i32":
      return compareNumbers(x.values[i] ?? 0, (y as typeof x).values[j] ?? 0);
    case "f32":
    case "f64":
      return compareNumbers(x.values[i] ?? 0, (y as typeof x).values[j] ?? 0);
    case "int64": {
      const p = int64Of(x, i);
      const q = int64Of(y as typeof x, j);
      return p < q ? -1 : p > q ? 1 : 0;
    }
    case "fixed16":
      return compareRanges(
        x.bytes,
        i * 16,
        i * 16 + 16,
        (y as typeof x).bytes,
        j * 16,
        j * 16 + 16,
      );
    case "interval": {
      const span = (d: typeof x, s: number) =>
        (BigInt(d.months[s] ?? 0) * 30n + BigInt(d.days[s] ?? 0)) * 86_400_000_000n +
        (d.micros.repr === "safe"
          ? BigInt(d.micros.values[s] ?? 0)
          : BigInt.asIntN(64, d.micros.values[s] ?? 0n));
      const p = span(x, i);
      const q = span(y as typeof x, j);
      return p < q ? -1 : p > q ? 1 : 0;
    }
    case "decimal": {
      const key = (d: typeof x, s: number) => {
        const special = d.specials?.[s] ?? 0;
        const c = d.coefficients;
        const coefficient =
          c.repr === "safe"
            ? BigInt(c.values[s] ?? 0)
            : c.repr === "int64"
              ? BigInt.asIntN(64, c.values[s] ?? 0n)
              : c.repr === "int128"
                ? int128At(c.limbs, s)
                : (c.values[s] ?? 0n);
        return {
          coefficient,
          scale: d.scale ?? d.scales?.[s] ?? 0,
          ...(special === 0
            ? {}
            : {
                special:
                  special === 1
                    ? ("NaN" as const)
                    : special === 2
                      ? ("Infinity" as const)
                      : ("-Infinity" as const),
              }),
        };
      };
      return compareDecimals(key(x, i), key(y as typeof x, j));
    }
    case "varlen": {
      const yd = y as typeof x;
      const as = x.offsets[i] ?? 0;
      const ae = x.offsets[i + 1] ?? 0;
      const bs = yd.offsets[j] ?? 0;
      const be = yd.offsets[j + 1] ?? 0;
      if (type.kind === "enum") {
        const p = enumRank(type, x.bytes, as, ae);
        const q = enumRank(type, yd.bytes, bs, be);
        return p - q;
      }
      // Decoded operands come from the caller's charged cache (decoded once per row).
      if (type.kind === "json") {
        if (p === undefined || q === undefined)
          throw new RangeError("JSONB keys compare decoded operands");
        return compareJsonb(p as JsonbValue, q as JsonbValue);
      }
      if (type.kind === "array") {
        if (p === undefined || q === undefined)
          throw new RangeError("Array keys compare decoded operands");
        return compareArrays(p as SqlArray, q as SqlArray);
      }
      if (type.kind === "text" && !isByteCollation(collation)) {
        if (p === undefined || q === undefined)
          throw new RangeError("ICU keys compare decoded operands");
        const order = collatorFor(collation ?? "").compare(p as string, q as string);
        if (order !== 0) return order < 0 ? -1 : 1;
      }
      return compareRanges(x.bytes, as, ae, yd.bytes, bs, be);
    }
  }
}

/**
 * Compare mode: the order of row `i` of `left` and row `j` of `right` (column vectors of one
 * schema) under the sort columns, with NULLS FIRST/LAST and DESC. Decoded keys (JSONB, arrays,
 * ICU text) read their operands at rows `oi` / `oj` of `leftOperands` / `rightOperands`.
 */
export function compareRows(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  left: readonly Vector[],
  i: number,
  right: readonly Vector[],
  j: number,
  leftOperands: Operands | null = null,
  rightOperands: Operands | null = null,
  oi = i,
  oj = j,
): number {
  for (let k = 0; k < keys.length; k++) {
    const key = keys[k];
    if (key === undefined) continue;
    const type = types[key.column];
    const a = left[key.column];
    const b = right[key.column];
    if (type === undefined || a === undefined || b === undefined) continue;
    const [fa, sa] = resolveRow(a, i);
    const [fb, sb] = resolveRow(b, j);
    if (sa < 0 || sb < 0) {
      if (sa < 0 && sb < 0) continue;
      // A null sorts first or last whatever the direction.
      return sa < 0 === (key.nulls === "first") ? -1 : 1;
    }
    const order = compareValues(
      type,
      fa,
      sa,
      fb,
      sb,
      key.collation,
      leftOperands?.values[k]?.[oi],
      rightOperands?.values[k]?.[oj],
    );
    if (order !== 0) return key.direction === "desc" ? -order : order;
  }
  return 0;
}
