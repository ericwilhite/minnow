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
import { SLICE_ROWS, type Slice } from "./slices.js";
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
 * Sorts `ids` by `compare` (stably) in slices: insertion sort of runs of 8, then bottom-up
 * merges through `temp` (at least `ids.length`, the caller's charged scratch). Every comparison
 * counts toward the slice (`Slice.tick`), since a comparison's cost is unbounded (wide keys,
 * decoded values); copies are checked every `SLICE_ROWS` elements.
 */
export function* mergeSortJob(
  ids: Int32Array,
  temp: Int32Array,
  compare: (a: number, b: number) => number,
  slice: Slice,
): Generator<void, void, void> {
  const n = ids.length;
  const run = 8;
  for (let lo = 0; lo < n; lo += run) {
    const hi = Math.min(n, lo + run);
    for (let i = lo + 1; i < hi; i++) {
      const value = ids[i] ?? 0;
      let j = i - 1;
      for (; j >= lo; j--) {
        const order = compare(ids[j] ?? 0, value);
        if (slice.tick()) yield;
        if (order <= 0) break;
        ids[j + 1] = ids[j] ?? 0;
      }
      ids[j + 1] = value;
    }
  }
  if (n <= run) return;
  let from: Int32Array = ids;
  let to: Int32Array = temp.subarray(0, n);
  for (let width = run; width < n; width *= 2) {
    for (let lo = 0; lo < n; lo += 2 * width) {
      const mid = Math.min(n, lo + width);
      const hi = Math.min(n, lo + 2 * width);
      let i = lo;
      let j = mid;
      let k = lo;
      while (i < mid && j < hi) {
        to[k++] = compare(from[i] ?? 0, from[j] ?? 0) <= 0 ? (from[i++] ?? 0) : (from[j++] ?? 0);
        if (slice.tick()) yield;
      }
      while (i < mid) {
        to[k++] = from[i++] ?? 0;
        if ((k & (SLICE_ROWS - 1)) === 0 && slice.due()) yield;
      }
      while (j < hi) {
        to[k++] = from[j++] ?? 0;
        if ((k & (SLICE_ROWS - 1)) === 0 && slice.due()) yield;
      }
    }
    const swap = from;
    from = to;
    to = swap;
  }
  if (from !== ids)
    for (let at = 0; at < n; at += SLICE_ROWS) {
      ids.set(from.subarray(at, Math.min(n, at + SLICE_ROWS)), at);
      if (slice.due()) yield;
    }
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

// --- direct fixed-width keys -------------------------------------------------------------

/** Payload bytes of a fixed-width key type in direct mode, or 0 when it has none. */
function directBytes(type: PhysicalType): number {
  switch (type.kind) {
    case "bool":
      return 1;
    case "int32":
    case "date":
      return 4;
    case "int64":
    case "time":
    case "timestamp":
    case "float64":
    case "float32":
      return 8;
    default:
      return 0;
  }
}

/**
 * Bytes of the direct key of `keys` (a header and the payload per key), or null when a key is
 * not fixed-width or the key passes the prefix words. Direct keys order exactly as the frozen
 * sort-key bytes do (the same header and payload rules, keys/key-encoding.ts), except that a
 * null keeps its zeroed payload slot so every key has one width: equal headers then compare
 * equal payloads, so the order is unchanged.
 */
export function directWidth(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
): number | null {
  let width = 0;
  for (const key of keys) {
    const type = types[key.column];
    if (type === undefined || !isByteCollation(key.collation)) return null;
    const bytes = directBytes(type);
    if (bytes === 0) return null;
    width += 1 + bytes;
  }
  return width <= 4 * PREFIX_WORDS ? width : null;
}

// Module scratch, sync-only (R23.1): written and read inside one synchronous `encode` call.
const f64 = new Float64Array(1);
const f64Words = new Uint32Array(f64.buffer);

/**
 * Encodes fixed-width keys straight into the prefix words (no byte keys): the direct mode of
 * key mode. Row k of a call is `prefixes[w][k]`.
 */
export class DirectKeyEncoder {
  readonly prefixes: Int32Array[];
  readonly prefixVectors: FlatVector[];
  readonly #bytes: Uint8Array;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly sortColumns: readonly SortColumn[],
    charge: ByteCharge,
    readonly capacity = 2048,
  ) {
    if (!charge.allocate(capacity * (PREFIX_WORDS * 4 + 4 * PREFIX_WORDS)))
      throw new RangeError("A sort key encoder must be admitted");
    this.prefixes = Array.from({ length: PREFIX_WORDS }, () => new Int32Array(capacity));
    this.#bytes = new Uint8Array(capacity * 4 * PREFIX_WORDS);
    this.prefixVectors = this.prefixes.map((values): FlatVector => ({
      form: "flat",
      type: { kind: "int32", bits: 32 },
      length: 0,
      validity: null,
      data: { layout: "i32", values },
    }));
  }

  get tail(): Vector[] {
    return [...this.prefixVectors];
  }

  encode(columns: readonly Vector[], ids: Uint16Array | null, from: number, count: number): void {
    const stride = 4 * PREFIX_WORDS;
    const out = this.#bytes;
    out.fill(0, 0, count * stride);
    let at = 0;
    for (const key of this.sortColumns) {
      const vector = columnAt(columns, key.column);
      const type = this.types[key.column];
      if (type === undefined) throw new RangeError("Sort key column out of range");
      const width = directBytes(type);
      const desc = key.direction === "desc";
      const nullHeader = key.nulls === "first" ? 0x00 : 0x02;
      for (let k = 0; k < count; k++) {
        const row = ids === null ? from + k : (ids[from + k] ?? 0);
        const [flat, slot] = resolveRow(vector, row);
        const base = k * stride + at;
        if (slot < 0) {
          out[base] = nullHeader;
          continue;
        }
        out[base] = 0x01;
        const data = flat.data;
        let hi = 0;
        let lo: number;
        switch (data.layout) {
          case "bool":
            lo = data.values[slot] === 1 ? 1 : 0;
            break;
          case "i32":
            lo = ((data.values[slot] ?? 0) ^ 0x8000_0000) >>> 0;
            break;
          case "int64": {
            const values = data.values;
            if (values.repr === "safe") {
              const value = values.values[slot] ?? 0;
              const high = Math.floor(value / 4_294_967_296);
              hi = ((high >>> 0) ^ 0x8000_0000) >>> 0;
              lo = (value - high * 4_294_967_296) >>> 0;
            } else {
              const words = wordsOf(values.values);
              hi = ((words[slot * 2 + 1] ?? 0) ^ 0x8000_0000) >>> 0;
              lo = (words[slot * 2] ?? 0) >>> 0;
            }
            break;
          }
          case "f64":
          case "f32": {
            const value = data.values[slot] ?? 0;
            f64[0] = value !== value ? Number.NaN : value === 0 ? 0 : value;
            hi = f64Words[1] ?? 0;
            lo = f64Words[0] ?? 0;
            if (hi >= 0x8000_0000) {
              hi = ~hi >>> 0;
              lo = ~lo >>> 0;
            } else hi = (hi | 0x8000_0000) >>> 0;
            break;
          }
          default:
            throw new TypeError(`No direct key for ${data.layout}`);
        }
        // Big-endian payload after the header, inverted for DESC.
        const flip = desc ? 0xff : 0;
        if (width === 8) {
          for (let b = 0; b < 4; b++) out[base + 1 + b] = ((hi >>> (24 - 8 * b)) & 0xff) ^ flip;
          for (let b = 0; b < 4; b++) out[base + 5 + b] = ((lo >>> (24 - 8 * b)) & 0xff) ^ flip;
        } else if (width === 4) {
          for (let b = 0; b < 4; b++) out[base + 1 + b] = ((lo >>> (24 - 8 * b)) & 0xff) ^ flip;
        } else out[base + 1] = lo ^ flip;
      }
      at += 1 + width;
    }
    for (let w = 0; w < PREFIX_WORDS; w++) {
      const prefix = this.prefixes[w];
      if (prefix === undefined) continue;
      for (let k = 0; k < count; k++) {
        const b = k * stride + w * 4;
        prefix[k] =
          ((out[b] ?? 0) << 24) |
          ((out[b + 1] ?? 0) << 16) |
          ((out[b + 2] ?? 0) << 8) |
          (out[b + 3] ?? 0);
      }
    }
    for (const vector of this.prefixVectors) vector.length = count;
  }

  close(charge: ByteCharge): void {
    charge.free(this.capacity * (PREFIX_WORDS * 4 + 4 * PREFIX_WORDS));
  }
}

// --- MSD radix over (prefix words, row id) tuples -----------------------------------------

/** Tuple stride: the prefix words then the row id. */
export const TUPLE_WORDS = PREFIX_WORDS + 1;

/**
 * An MSD sort's histogram and bucket starts (65,537 words each), owned and charged by the
 * operator (R23.1: a sort job pauses between slices).
 */
export class RadixScratch {
  static readonly BYTES = 2 * 65_537 * 4;
  readonly counts = new Int32Array(65_537);
  readonly starts = new Int32Array(65_537);
}

/** Unsigned order of two tuples' prefix words. */
function compareTuples(t: Uint32Array, a: number, b: number): number {
  for (let w = 0; w < PREFIX_WORDS; w++) {
    const x = t[a + w] ?? 0;
    const y = t[b + w] ?? 0;
    if (x !== y) return x < y ? -1 : 1;
  }
  return 0;
}

/**
 * Sorts `n` tuples (PREFIX_WORDS words + row id, `TUPLE_WORDS` each) in place: MSD radix on
 * 8-bit digits of the prefix (16-bit for large ranges; a digit every tuple shares is skipped
 * without moving), insertion sort below 48 tuples, then (when `refine` is given) full-key order
 * among equal prefixes. `scratch` holds n tuples; refinement sorts through it too. A sliced job:
 * it yields when `slice` is due (R23.2). One job at a time per `radix`.
 */
export function* msdSort(
  tuples: Uint32Array,
  scratch: Uint32Array,
  n: number,
  refine: ((a: number, b: number) => number) | null,
  slice: Slice,
  radix: RadixScratch,
  stack: Int32Array = new Int32Array(msdStackWords(n)),
): Generator<void, void, void> {
  const S = TUPLE_WORDS;
  const bytes = 4 * PREFIX_WORDS;
  const { counts, starts } = radix;
  /** A range of at most LEAF_ROWS rows, or whose prefix bytes are all consumed. */
  function* leaf(lo: number, hi: number): Generator<void, void, void> {
    // Insertion sort by the prefix (no moves when every prefix is equal), then the full key
    // among equal prefixes.
    for (let i = lo + 1; i < hi; i++) {
      for (let j = i; j > lo && compareTuples(tuples, (j - 1) * S, j * S) > 0; j--)
        for (let x = 0; x < S; x++) {
          const t = tuples[(j - 1) * S + x] ?? 0;
          tuples[(j - 1) * S + x] = tuples[j * S + x] ?? 0;
          tuples[j * S + x] = t;
        }
      if (((i - lo) & (SLICE_ROWS - 1)) === 0 && slice.due()) yield;
    }
    if (refine !== null) yield* refineRuns(tuples, scratch, lo, hi, refine, slice);
  }
  // The pending ranges [lo, hi, byte]: each holds more than LEAF_ROWS rows (smaller ones are
  // sorted at once) and they are disjoint, so the caller's `msdStackWords(n)` words suffice.
  let top = 0;
  const push = (lo: number, hi: number, b: number) => {
    if (top + 3 > stack.length) throw new RangeError("MSD sort stack overflow");
    stack[top++] = lo;
    stack[top++] = hi;
    stack[top++] = b;
  };
  if (n > LEAF_ROWS) push(0, n, 0);
  else if (n > 1) yield* leaf(0, n);
  while (top > 0) {
    const b = stack[--top] ?? 0;
    const hi = stack[--top] ?? 0;
    const lo = stack[--top] ?? 0;
    if (b >= bytes) {
      yield* leaf(lo, hi);
      if (slice.due()) yield;
      continue;
    }
    const word = b >>> 2;
    // Large ranges take two bytes per pass (65,536 buckets) when both lie in one word.
    const wide = hi - lo >= 65_536 && (b & 3) !== 3;
    const width = wide ? 2 : 1;
    const shift = 32 - 8 * ((b & 3) + width);
    const mask = wide ? 0xffff : 0xff;
    const buckets = mask + 1;
    counts.fill(0, 0, buckets + 1);
    for (let i = lo; i < hi; i++) {
      const digit = ((tuples[i * S + word] ?? 0) >>> shift) & mask;
      counts[digit + 1] = (counts[digit + 1] ?? 0) + 1;
      if (((i - lo) & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
    }
    // Every tuple shares this digit: nothing to move.
    const first = ((tuples[lo * S + word] ?? 0) >>> shift) & mask;
    if (counts[first + 1] === hi - lo) {
      push(lo, hi, b + width);
      continue;
    }
    for (let d = 0; d < buckets; d++) counts[d + 1] = (counts[d + 1] ?? 0) + (counts[d] ?? 0);
    starts.set(counts.subarray(0, buckets + 1));
    for (let i = lo; i < hi; i++) {
      const digit = ((tuples[i * S + word] ?? 0) >>> shift) & mask;
      const at = (lo + (counts[digit] ?? 0)) * S;
      counts[digit] = (counts[digit] ?? 0) + 1;
      for (let x = 0; x < S; x++) scratch[at + x] = tuples[i * S + x] ?? 0;
      if (((i - lo) & (SLICE_ROWS - 1)) === SLICE_ROWS - 1 && slice.due()) yield;
    }
    tuples.set(scratch.subarray(lo * S, hi * S), lo * S);
    for (let d = 0; d < buckets; d++) {
      const start = lo + (starts[d] ?? 0);
      const end = lo + (starts[d + 1] ?? 0);
      if (end - start > LEAF_ROWS) push(start, end, b + width);
      else if (end - start > 1) {
        // Small buckets are finished now: only large ones wait (the bound above).
        yield* leaf(start, end);
        if (slice.tick()) yield;
      }
    }
    if (slice.due()) yield;
  }
}

/** Ranges of at most this many rows are insertion-sorted at once (never pushed). */
const LEAF_ROWS = 48;

/** Words of an MSD sort's stack for n rows: pending ranges are disjoint, each > LEAF_ROWS. */
export function msdStackWords(n: number): number {
  return 3 * (Math.floor(n / (LEAF_ROWS + 1)) + 2);
}

/**
 * Orders runs of equal prefixes in tuples `lo … hi` by `refine` over their row ids, merging
 * through the scratch tuples of the same range (free here: 2 × ids fit in TUPLE_WORDS words).
 */
function* refineRuns(
  tuples: Uint32Array,
  scratch: Uint32Array,
  lo: number,
  hi: number,
  refine: (a: number, b: number) => number,
  slice: Slice,
): Generator<void, void, void> {
  const S = TUPLE_WORDS;
  let start = lo;
  for (let i = lo + 1; i <= hi; i++) {
    if (i < hi && compareTuples(tuples, start * S, i * S) === 0) continue;
    const length = i - start;
    if (length > 1) {
      const base = scratch.byteOffset + start * S * 4;
      const ids = new Int32Array(scratch.buffer, base, length);
      const temp = new Int32Array(scratch.buffer, base + length * 4, length);
      for (let k = 0; k < length; k++) ids[k] = tuples[(start + k) * S + PREFIX_WORDS] ?? 0;
      yield* mergeSortJob(ids, temp, refine, slice);
      for (let k = 0; k < length; k++) tuples[(start + k) * S + PREFIX_WORDS] = ids[k] ?? 0;
    }
    start = i;
  }
}

/**
 * Whether row `i` of `left` and row `j` of `right` have equal values under every key (NULL
 * equals NULL): window partitions and peers. Numbers compare as numbers (−0 = 0, NaN = NaN);
 * text, bytes, JSONB and arrays by their bytes (deterministic collations: equal values are
 * equal bytes; stored JSONB is normalized); other types by `compareValues`.
 */
export function keysEqual(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  left: readonly Vector[],
  i: number,
  right: readonly Vector[],
  j: number,
): boolean {
  for (const key of keys) {
    const type = types[key.column];
    const a = left[key.column];
    const b = right[key.column];
    if (type === undefined || a === undefined || b === undefined) continue;
    const [fa, sa] = resolveRow(a, i);
    const [fb, sb] = resolveRow(b, j);
    if (sa < 0 || sb < 0) {
      if (sa < 0 && sb < 0) continue;
      return false;
    }
    const x = fa.data;
    const y = fb.data;
    switch (x.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64":
        if (compareNumbers(x.values[sa] ?? 0, (y as typeof x).values[sb] ?? 0) !== 0) return false;
        continue;
      case "int64": {
        const p = x.values;
        const q = (y as typeof x).values;
        if (p.repr === "safe" && q.repr === "safe") {
          if ((p.values[sa] ?? 0) !== (q.values[sb] ?? 0)) return false;
        } else if (p.repr === "int64" && q.repr === "int64") {
          if (p.values[sa] !== q.values[sb]) return false;
        } else if (int64Of(x, sa) !== int64Of(y as typeof x, sb)) return false;
        continue;
      }
      case "varlen": {
        const yd = y as typeof x;
        if (
          compareRanges(
            x.bytes,
            x.offsets[sa] ?? 0,
            x.offsets[sa + 1] ?? 0,
            yd.bytes,
            yd.offsets[sb] ?? 0,
            yd.offsets[sb + 1] ?? 0,
          ) !== 0
        )
          return false;
        continue;
      }
      default:
        if (compareValues(type, fa, sa, fb, sb, key.collation) !== 0) return false;
    }
  }
  return true;
}
