/**
 * The memcomparable key encoding (DECISIONS.md C7), frozen with the format.
 *
 * - **Equality keys** are persisted: key-index runs, run fences and Bloom filters. Equal bytes ⇔
 *   SQL-equal values, so hashing them (xxHash32) agrees with equality.
 * - **Sort keys** are query-time only (sorts, merges, range bounds): unsigned byte order = SQL
 *   order.
 * - **What may be a key** (`indexKeyKind`, checked at DDL): scalar types only. Arrays and
 *   JSON/JSONB cannot be indexed at 1.0 (`UnindexableTypeError`); they sort and group through the
 *   semantic comparators (exec/semantic.ts), never through bytes. An enum index is equality-only:
 *   its persisted key is the label (labels can be inserted before others, which changes ranks), so
 *   range predicates on an enum never use the index; ORDER BY enum uses the declaration rank at
 *   query time (the sort key below).
 *
 * ## Component layout
 * A key is the concatenation of its components; every component is prefix-free.
 * - Sort-key component: one header byte, then the payload, with the payload's bytes inverted
 *   (`~b`) for DESC. Header (never inverted): NULLS FIRST: null `0x00`, value `0x01`;
 *   NULLS LAST: value `0x01`, null `0x02`. A null has no payload.
 * - Equality-key component: header null `0x00` (null-safe keys only), value `0x01`, payload.
 *
 * ## Payloads (big-endian throughout)
 * | type | payload |
 * | --- | --- |
 * | bool | `0x00` false, `0x01` true |
 * | int32, date | 4 bytes: value XOR 0x8000_0000 (date ±infinity are INT32 min/max: the ends) |
 * | int64, time, timestamp | 8 bytes: value + 2^63 (timestamp ±infinity are INT64 min/max) |
 * | float64, float32 | 8 bytes of the f64 (float32 widened): −0 → +0; every NaN → 0x7FF8…0; then if the sign bit is set invert all bits, else set the sign bit. NaN sorts above +Infinity and equals NaN (PostgreSQL). |
 * | decimal | class byte, then for finite non-zero values the normalized magnitude (below) |
 * | text, bytes | bytes with `0x00` → `0x00 0xFF`, then terminator `0x00 0x01` |
 * | uuid | the 16 bytes |
 * | enum | sort key: rank as u16 (declaration order); equality key: the label as text |
 * | interval | 16 bytes: (months × 30 + days) × 86,400,000,000 + µs, as int128 + 2^127 (PostgreSQL compares the normalized span, so `1 mon` = `30 days`) |
 *
 * Decimal class byte: −Infinity `0x01`, negative `0x02`, zero `0x03`, positive `0x04`,
 * +Infinity `0x05`, NaN `0x06` (PostgreSQL: NaN above +Infinity, NaN = NaN). A finite non-zero
 * value is 0.d₁d₂… × 10^E with d₁ ≠ 0 and no trailing zero digit (so 1.0 = 1.00): E as 4 bytes
 * (E XOR 0x8000_0000), digit pairs as bytes 10a + b + 1 (an odd last digit pairs with 0), then
 * `0x00`. A negative value's exponent, pair and terminator bytes are inverted.
 *
 * Collation: these are C-collation keys; ICU collations are never encoded (DECISIONS.md C8).
 *
 * ## Interfaces
 * - `KeyWriter`: a caller-owned, reusable buffer with one method per payload. Nothing is
 *   allocated per key once it has grown (int64 values outside the safe range and the 128-bit and
 *   wide decimal tiers read BigInts from their vectors).
 * - `encodeKeyBatch`: every active row of a batch into one `KeyBatch` (bytes + offsets), straight
 *   from vectors. This is what operators and run writers use.
 * - `encodeSortKey` / `encodeEqualityKey`: one tuple from `KeyValue`s, returning a copy: for tests,
 *   constants and parameters.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import type { EnumType, PhysicalType } from "../types.js";
import { int128At } from "../vector/int128.js";
import type { Batch, FlatVector, Selection, Vector } from "../vector/vector.js";

export class UnindexableTypeError extends Error {
  readonly code = "MINNOW_UNINDEXABLE_TYPE";
  constructor(type: PhysicalType) {
    super(`Columns of type ${type.kind} cannot be indexed`);
    this.name = "UnindexableTypeError";
  }
}

/**
 * How a persisted index over `type` can be used: `ordered` (equality and ranges), `equality`
 * (enum: lookups only). Throws `UnindexableTypeError` for arrays, JSON and locators (DDL refuses).
 */
export function indexKeyKind(type: PhysicalType): "ordered" | "equality" {
  switch (type.kind) {
    case "array":
    case "json":
    case "locator":
      throw new UnindexableTypeError(type);
    case "enum":
      return "equality";
    default:
      return "ordered";
  }
}

export interface DecimalKey {
  readonly coefficient: bigint;
  /** Value = coefficient × 10^−scale. Any integer scale, including negative. */
  readonly scale: number;
  readonly special?: "NaN" | "Infinity" | "-Infinity";
}

export interface IntervalKey {
  readonly months: number;
  readonly days: number;
  readonly micros: bigint;
}

/**
 * One value in key form: boolean (bool); number (int32, date, float64, float32); bigint (int64,
 * time, timestamp); Uint8Array (text, bytes; uuid as 16 bytes; enum as the label's UTF-8);
 * DecimalKey; IntervalKey.
 */
export type KeyValue = boolean | number | bigint | Uint8Array | DecimalKey | IntervalKey;

export interface SortKeyPart {
  readonly type: PhysicalType;
  readonly value: KeyValue | null;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
}

export interface EqualityKeyPart {
  readonly type: PhysicalType;
  readonly value: KeyValue | null;
}

const TWO_32 = 2 ** 32;
const floatBytes = new Uint8Array(8);
const floatScratch = new DataView(floatBytes.buffer);
const TWO_63 = 2n ** 63n;
const TWO_127 = 2n ** 127n;
const MICROS_PER_DAY = 86_400_000_000;

/** Label → rank, built once per enum type: hashed bytes, no decoder per value. */
const enumTables = new WeakMap<EnumType, Map<number, Array<[Uint8Array, number]>>>();

function enumTable(type: EnumType): Map<number, Array<[Uint8Array, number]>> {
  let table = enumTables.get(type);
  if (table === undefined) {
    table = new Map();
    const encoder = new TextEncoder();
    for (const [rank, label] of type.labels.entries()) {
      const bytes = encoder.encode(label);
      const hash = xxHash32(bytes);
      const bucket = table.get(hash) ?? [];
      bucket.push([bytes, rank]);
      table.set(hash, bucket);
    }
    enumTables.set(type, table);
  }
  return table;
}

/** The declaration rank of the label in `bytes[start … end)`. */
export function enumRank(type: EnumType, bytes: Uint8Array, start: number, end: number): number {
  for (const [label, rank] of enumTable(type).get(xxHash32(bytes, 0, start, end)) ?? []) {
    if (label.length !== end - start) continue;
    let same = true;
    for (let k = 0; k < label.length && same; k++) same = label[k] === bytes[start + k];
    if (same) return rank;
  }
  throw new RangeError(`A value is not a label of enum ${type.name}`);
}

/** A reusable key buffer (see "Interfaces"). */
export class KeyWriter {
  bytes: Uint8Array;
  length = 0;
  readonly #digits = new Uint8Array(32);

  constructor(initialBytes = 256) {
    this.bytes = new Uint8Array(initialBytes);
  }

  reset(): void {
    this.length = 0;
  }

  /** The bytes written since the last reset (aliases the buffer). */
  view(): Uint8Array {
    return this.bytes.subarray(0, this.length);
  }

  #ensure(extra: number): void {
    if (this.length + extra <= this.bytes.length) return;
    let size = this.bytes.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(this.bytes.subarray(0, this.length));
    this.bytes = grown;
  }

  byte(value: number): void {
    this.#ensure(1);
    this.bytes[this.length++] = value;
  }

  /** Inverts the bytes written since `start` (a DESC component). */
  invertFrom(start: number): void {
    for (let index = start; index < this.length; index++)
      this.bytes[index] = ~(this.bytes[index] ?? 0) & 0xff;
  }

  #u32(value: number): void {
    this.#ensure(4);
    const b = this.bytes;
    b[this.length++] = value >>> 24;
    b[this.length++] = (value >>> 16) & 0xff;
    b[this.length++] = (value >>> 8) & 0xff;
    b[this.length++] = value & 0xff;
  }

  int32(value: number): void {
    this.#u32((value ^ 0x8000_0000) >>> 0);
  }

  /** An int64-family value known to be a safe integer, without a BigInt. */
  safeInt64(value: number): void {
    const high = Math.floor(value / TWO_32);
    this.#u32(((high >>> 0) ^ 0x8000_0000) >>> 0);
    this.#u32(value - high * TWO_32);
  }

  int64(value: bigint): void {
    const biased = value + TWO_63;
    this.#u32(Number(biased >> 32n));
    this.#u32(Number(biased & 0xffff_ffffn));
  }

  float(value: number): void {
    this.#ensure(8);
    floatScratch.setFloat64(0, Number.isNaN(value) ? Number.NaN : value === 0 ? 0 : value);
    const negative = floatBytes[0] !== undefined && floatBytes[0] >= 0x80;
    for (let index = 0; index < 8; index++) {
      const byte = floatBytes[index] ?? 0;
      this.bytes[this.length++] = negative ? ~byte & 0xff : index === 0 ? byte | 0x80 : byte;
    }
  }

  escaped(source: Uint8Array, start = 0, end = source.length): void {
    this.#ensure((end - start) * 2 + 2);
    for (let index = start; index < end; index++) {
      const byte = source[index] ?? 0;
      this.bytes[this.length++] = byte;
      if (byte === 0) this.bytes[this.length++] = 0xff;
    }
    this.bytes[this.length++] = 0x00;
    this.bytes[this.length++] = 0x01;
  }

  fixed(source: Uint8Array, start: number, length: number): void {
    this.#ensure(length);
    this.bytes.set(source.subarray(start, start + length), this.length);
    this.length += length;
  }

  enumRank(rank: number): void {
    this.byte(rank >>> 8);
    this.byte(rank & 0xff);
  }

  decimalSpecial(special: "NaN" | "Infinity" | "-Infinity"): void {
    this.byte(special === "-Infinity" ? 0x01 : special === "Infinity" ? 0x05 : 0x06);
  }

  /** A finite decimal: `coefficient` (safe number or bigint) × 10^−scale. */
  decimal(coefficient: number | bigint, scale: number): void {
    // Digits of |coefficient| into the scratch buffer, most significant first.
    let count = 0;
    let digits = this.#digits;
    if (typeof coefficient === "number" && Number.isSafeInteger(coefficient)) {
      if (coefficient === 0) return this.byte(0x03);
      let magnitude = Math.abs(coefficient);
      while (magnitude > 0) {
        digits[count++] = magnitude % 10;
        magnitude = Math.floor(magnitude / 10);
      }
      for (let left = 0, right = count - 1; left < right; left++, right--) {
        const swap = digits[left] ?? 0;
        digits[left] = digits[right] ?? 0;
        digits[right] = swap;
      }
    } else {
      const big = BigInt(coefficient);
      if (big === 0n) return this.byte(0x03);
      const text = (big < 0n ? -big : big).toString();
      if (text.length > digits.length) digits = new Uint8Array(text.length);
      for (let index = 0; index < text.length; index++) digits[index] = text.charCodeAt(index) - 48;
      count = text.length;
    }
    const negative = typeof coefficient === "number" ? coefficient < 0 : coefficient < 0n;
    let significant = count;
    while (significant > 1 && digits[significant - 1] === 0) significant -= 1;
    const exponent = count - scale;
    if (Math.abs(exponent) > 0x3fff_ffff) throw new RangeError("NUMERIC exponent out of range");
    this.byte(negative ? 0x02 : 0x04);
    const start = this.length;
    this.#u32((exponent ^ 0x8000_0000) >>> 0);
    for (let index = 0; index < significant; index += 2) {
      const high = digits[index] ?? 0;
      const low = index + 1 < significant ? (digits[index + 1] ?? 0) : 0;
      this.byte(high * 10 + low + 1);
    }
    this.byte(0x00);
    if (negative) this.invertFrom(start);
  }

  /** An interval's normalized span as int128 + 2^127. */
  interval(months: number, days: number, micros: number | bigint): void {
    const dayMicros = (months * 30 + days) * MICROS_PER_DAY;
    if (
      typeof micros === "number" &&
      Number.isSafeInteger(dayMicros) &&
      Number.isSafeInteger(dayMicros + micros)
    ) {
      const span = dayMicros + micros;
      this.#u32(span < 0 ? 0x7fff_ffff : 0x8000_0000);
      this.#u32(span < 0 ? 0xffff_ffff : 0);
      const high = Math.floor(span / TWO_32);
      this.#u32(high >>> 0);
      this.#u32(span - high * TWO_32);
      return;
    }
    const span = (BigInt(months) * 30n + BigInt(days)) * BigInt(MICROS_PER_DAY) + BigInt(micros);
    const biased = span + TWO_127;
    for (let shift = 96n; shift >= 0n; shift -= 32n)
      this.#u32(Number((biased >> shift) & 0xffff_ffffn));
  }
}

function writeValue(writer: KeyWriter, type: PhysicalType, value: KeyValue, sort: boolean): void {
  switch (type.kind) {
    case "bool":
      return writer.byte(value === true ? 1 : 0);
    case "int32":
    case "date":
      return writer.int32(value as number);
    case "int64":
    case "time":
    case "timestamp":
      return writer.int64(value as bigint);
    case "float64":
    case "float32":
      return writer.float(value as number);
    case "decimal": {
      const decimal = value as DecimalKey;
      if (decimal.special !== undefined) return writer.decimalSpecial(decimal.special);
      return writer.decimal(decimal.coefficient, decimal.scale);
    }
    case "text":
    case "bytes":
      return writer.escaped(value as Uint8Array);
    case "uuid":
      return writer.fixed(value as Uint8Array, 0, 16);
    case "enum": {
      const label = value as Uint8Array;
      return sort ? writer.enumRank(enumRank(type, label, 0, label.length)) : writer.escaped(label);
    }
    case "interval": {
      const { months, days, micros } = value as IntervalKey;
      return writer.interval(months, days, micros);
    }
    case "array":
    case "json":
    case "locator":
      throw new UnindexableTypeError(type);
  }
}

const tupleWriter = new KeyWriter();

/** The sort key of one tuple (a copy). */
export function encodeSortKey(parts: readonly SortKeyPart[]): Uint8Array {
  tupleWriter.reset();
  for (const part of parts) {
    if (part.value === null) {
      tupleWriter.byte(part.nulls === "first" ? 0x00 : 0x02);
      continue;
    }
    tupleWriter.byte(0x01);
    const start = tupleWriter.length;
    writeValue(tupleWriter, part.type, part.value, true);
    if (part.direction === "desc") tupleWriter.invertFrom(start);
  }
  return tupleWriter.view().slice();
}

/** The equality key of one tuple (a copy): equal bytes ⇔ SQL-equal (null-safe). */
export function encodeEqualityKey(parts: readonly EqualityKeyPart[]): Uint8Array {
  tupleWriter.reset();
  for (const part of parts) {
    if (part.value === null) {
      tupleWriter.byte(0x00);
      continue;
    }
    tupleWriter.byte(0x01);
    writeValue(tupleWriter, part.type, part.value, false);
  }
  return tupleWriter.view().slice();
}

export interface KeyColumn {
  readonly vector: Vector;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
}

/** Keys of a batch's active rows: key k is `writer.bytes[offsets[k] … offsets[k + 1])`. */
export class KeyBatch {
  readonly writer = new KeyWriter(4096);
  offsets = new Int32Array(1);
  count = 0;

  key(k: number): Uint8Array {
    return this.writer.bytes.subarray(this.offsets[k] ?? 0, this.offsets[k + 1] ?? 0);
  }
}

function isValidAt(validity: Uint32Array | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/** Writes the payload of slot `index` of a flat vector. Returns false for a null slot. */
function writeSlot(writer: KeyWriter, flat: FlatVector, index: number, sort: boolean): boolean {
  if (!isValidAt(flat.validity, index)) return false;
  const data = flat.data;
  const type = flat.type;
  switch (data.layout) {
    case "bool":
      writer.byte(data.values[index] === 1 ? 1 : 0);
      break;
    case "i32":
      writer.int32(data.values[index] ?? 0);
      break;
    case "f64":
    case "f32":
      if (type.kind === "locator") throw new UnindexableTypeError(type);
      writer.float(data.values[index] ?? 0);
      break;
    case "int64":
      if (data.values.repr === "safe") writer.safeInt64(data.values.values[index] ?? 0);
      else writer.int64(data.values.values[index] ?? 0n);
      break;
    case "decimal": {
      const special = data.specials?.[index] ?? 0;
      if (special !== 0) {
        writer.decimalSpecial(special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity");
        break;
      }
      const scale = data.scale ?? data.scales?.[index] ?? 0;
      const coefficients = data.coefficients;
      if (coefficients.repr === "safe") writer.decimal(coefficients.values[index] ?? 0, scale);
      else if (coefficients.repr === "int64")
        writer.decimal(coefficients.values[index] ?? 0n, scale);
      else if (coefficients.repr === "int128")
        writer.decimal(int128At(coefficients.limbs, index), scale);
      else writer.decimal(coefficients.values[index] ?? 0n, scale);
      break;
    }
    case "varlen": {
      const start = data.offsets[index] ?? 0;
      const end = data.offsets[index + 1] ?? 0;
      if (type.kind === "enum" && sort) writer.enumRank(enumRank(type, data.bytes, start, end));
      else if (type.kind === "text" || type.kind === "bytes" || type.kind === "enum")
        writer.escaped(data.bytes, start, end);
      else throw new UnindexableTypeError(type);
      break;
    }
    case "fixed16":
      writer.fixed(data.bytes, index * 16, 16);
      break;
    case "interval": {
      const micros = data.micros;
      writer.interval(
        data.months[index] ?? 0,
        data.days[index] ?? 0,
        micros.repr === "safe" ? (micros.values[index] ?? 0) : (micros.values[index] ?? 0n),
      );
      break;
    }
  }
  return true;
}

/**
 * Encodes the key of every active row of `batch` (sort or equality mode) into `out`, reusing its
 * buffers. Columns are read straight from flat, constant and dictionary vectors.
 */
export function encodeKeyBatch(
  mode: "sort" | "equality",
  columns: readonly KeyColumn[],
  batch: Pick<Batch, "length" | "selection">,
  out: KeyBatch,
): void {
  const selection: Selection | null = batch.selection;
  const count = selection === null ? batch.length : selection.count;
  if (out.offsets.length < count + 1) out.offsets = new Int32Array(count + 1);
  const writer = out.writer;
  writer.reset();
  out.count = count;
  for (let k = 0; k < count; k++) {
    out.offsets[k] = writer.length;
    const row = selection === null ? k : (selection.rows[k] ?? 0);
    for (const column of columns) {
      const vector = column.vector;
      let flat: FlatVector;
      let index: number;
      if (vector.form === "flat") {
        flat = vector;
        index = row;
      } else if (vector.form === "constant") {
        flat = vector.value;
        index = 0;
      } else {
        flat = vector.dictionary;
        index = vector.codes[row] ?? 0;
        if (!isValidAt(vector.validity, row)) index = -1;
      }
      const header = writer.length;
      writer.byte(0x01);
      const ok = index >= 0 && writeSlot(writer, flat, index, mode === "sort");
      if (!ok) {
        writer.length = header;
        writer.byte(mode === "equality" ? 0x00 : column.nulls === "first" ? 0x00 : 0x02);
      } else if (mode === "sort" && column.direction === "desc") writer.invertFrom(header + 1);
    }
  }
  out.offsets[count] = writer.length;
}
