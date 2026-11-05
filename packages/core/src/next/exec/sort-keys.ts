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
import { compareArrays, compareDecimals, compareJsonb, parseJsonb } from "./semantic.js";

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

let identityRows = new Uint16Array(0);
function identity(from: number, count: number): { rows: Uint16Array; count: number } {
  if (identityRows.length < from + count) {
    identityRows = new Uint16Array(Math.max(2048, from + count));
    for (let i = 0; i < identityRows.length; i++) identityRows[i] = i;
  }
  return { rows: identityRows.subarray(from, from + count), count };
}

/**
 * Sorts row ids 0 … n by unsigned prefix words (`words`, PREFIX_WORDS per row) then, among equal
 * prefixes, by `refine` (full key bytes) when given. LSD radix with 16-bit digits, skipping
 * digits every row shares; `scratch` holds 2 × n × (PREFIX_WORDS + 1) words. Small inputs use a
 * comparison sort.
 */
export function sortByPrefix(
  n: number,
  words: Int32Array,
  ids: Int32Array,
  scratch: Uint32Array,
  refine: ((a: number, b: number) => number) | null,
  /** Words that can differ (the rest are zero for every row). */
  used = PREFIX_WORDS,
): void {
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
  }
  const counts = histogram;
  for (let w = Math.min(used, PREFIX_WORDS) - 1; w >= 0; w--)
    for (let shift = 0; shift <= 16; shift += 16) {
      counts.fill(0);
      for (let i = 0; i < n; i++) {
        const digit = ((from[i * stride + w] ?? 0) >>> shift) & 0xffff;
        counts[digit] = (counts[digit] ?? 0) + 1;
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
      }
      const swap = from;
      from = to;
      to = swap;
    }
  for (let i = 0; i < n; i++) ids[i] = from[i * stride + PREFIX_WORDS] ?? 0;
  if (refine === null) return;
  // Runs of equal prefixes: order them by the full key.
  let start = 0;
  for (let i = 1; i <= n; i++) {
    let same = i < n;
    for (let w = 0; w < PREFIX_WORDS && same; w++)
      same = from[i * stride + w] === from[start * stride + w];
    if (same) continue;
    if (i - start > 1) ids.subarray(start, i).sort(refine);
    start = i;
  }
}
const histogram = new Uint32Array(65_536);

/** Bytes `sortByPrefix` needs in `scratch` for n rows. */
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

/** Order of two non-null values of one sort column's type (ascending). */
function compareValues(
  type: PhysicalType,
  a: FlatVector,
  i: number,
  b: FlatVector,
  j: number,
  collation: Collation,
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
      if (type.kind === "json") {
        const order = compareJsonb(parseJsonb(decodeUtf8(x, i)), parseJsonb(decodeUtf8(yd, j)));
        return order;
      }
      if (type.kind === "array") {
        const element = type.element;
        const p = decodeArrayValue(element, x.bytes.subarray(as, ae));
        const q = decodeArrayValue(element, yd.bytes.subarray(bs, be));
        return compareArrays({ element, ...p }, { element, ...q });
      }
      if (type.kind === "text" && !isByteCollation(collation)) {
        const order = collatorFor(collation ?? "").compare(decodeUtf8(x, i), decodeUtf8(yd, j));
        if (order !== 0) return order < 0 ? -1 : 1;
      }
      return compareRanges(x.bytes, as, ae, yd.bytes, bs, be);
    }
  }
}

/**
 * Compare mode: the order of row `i` of `left` and row `j` of `right` (column vectors of one
 * schema) under the sort columns, with NULLS FIRST/LAST and DESC.
 */
export function compareRows(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  left: readonly Vector[],
  i: number,
  right: readonly Vector[],
  j: number,
): number {
  for (const key of keys) {
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
    const order = compareValues(type, fa, sa, fb, sb, key.collation);
    if (order !== 0) return key.direction === "desc" ? -order : order;
  }
  return 0;
}
