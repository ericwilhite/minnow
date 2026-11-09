/**
 * Column storage for memtables: one `ColumnChunk` per column per `MEMORY_CHUNK_ROWS` rows,
 * append-only, in the column's exact physical layout (vector/vector.ts), so a chunk is handed to
 * readers as a zero-copy `FlatVector` view.
 *
 * ## Immutability by append-only discipline
 * Slots below a chunk's `length` are never rewritten. Appends write past it; a rollback truncates
 * only rows no reader can see (pending rows of the open transaction, snapshot.ts). When a chunk
 * must change representation (an int64 value outside the f64-safe range, a decimal coefficient past
 * int64, the first null, a varlen arena that is full), it allocates new arrays and copies; views
 * already handed out keep the old arrays, whose slots are still exactly right for their length.
 * `append` borrows its source only until it returns or throws. Its reusable source slot is cleared
 * on both exits: owned chunk views keep copied arrays, never a previous producer's entire batch.
 *
 * ## Frame column codec (`encodeColumn` / `decodeColumn`)
 * The write path's log body stores inserted rows column by column (format/log-frame.ts
 * `TableChange.inserts.columns`): a layout tag, a validity bitmap (or none), then the layout's raw
 * little-endian arrays. Decoding yields a fresh `FlatVector` that replay appends like any batch.
 */
import { StoreCorruptionError } from "../storage/contract.js";
import { layoutOf, type PhysicalType } from "../types.js";
import { int128At, setInt128 } from "../vector/int128.js";
import { MAX_SAFE } from "../vector/int64.js";
import type { FlatVector, Int64Values, Validity, Vector, VectorData } from "../vector/vector.js";
import { DecimalSpecial } from "../vector/vector.js";
import {
  ByteReader,
  ByteWriter,
  type ByteSink,
  charged,
  copyRange,
  need,
  UNCHARGED,
  type Charge,
} from "./bytes.js";

export const MEMORY_CHUNK_ROWS = 2048;

/** A borrowed source slot: flat exists only during append; index −1 denotes a null value. */
interface SourceSlot {
  flat: FlatVector | null;
  index: number;
}

const bitValid = (validity: Validity | null, row: number): boolean =>
  validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;

/** Resolves row `row` of any vector form to a flat slot (index −1 for null). */
export function sourceSlot(
  vector: Vector,
  row: number,
  out: SourceSlot,
): asserts out is SourceSlot & { flat: FlatVector } {
  if (vector.form === "flat") {
    out.flat = vector;
    out.index = bitValid(vector.validity, row) ? row : -1;
  } else if (vector.form === "constant") {
    out.flat = vector.value;
    out.index = bitValid(vector.value.validity, 0) ? 0 : -1;
  } else {
    out.flat = vector.dictionary;
    out.index = bitValid(vector.validity, row) ? (vector.codes[row] ?? 0) : -1;
  }
}

/** Is row `row` of `vector` null? */
export function isNullAt(vector: Vector, row: number): boolean {
  if (vector.form === "flat") return !bitValid(vector.validity, row);
  if (vector.form === "constant") return !bitValid(vector.value.validity, 0);
  return !bitValid(vector.validity, row);
}

type DecimalData = Extract<VectorData, { layout: "decimal" }>;

/** One column's slots for one chunk of rows. */
export class ColumnChunk {
  readonly type: PhysicalType;
  readonly layout: ReturnType<typeof layoutOf>;
  length = 0;
  nullCount = 0;
  validity: Validity | null = null;
  // One of these is in use, by layout.
  bools: Uint8Array | null = null;
  i32: Int32Array | null = null;
  f64: Float64Array | null = null;
  f32: Float32Array | null = null;
  /** int64 layout, interval micros, decimal coefficients (safe / int64 tiers). */
  safe: Float64Array | null = null;
  big: BigInt64Array | null = null;
  /** Decimal coefficients past int64. */
  limbs: Uint32Array | null = null;
  /** Decimal: per-slot scales (unconstrained NUMERIC), or null for the declared scale. */
  scales: Uint16Array | null = null;
  specials: Uint8Array | null = null;
  /** varlen */
  arena: Uint8Array | null = null;
  offsets: Int32Array | null = null;
  /** fixed16 */
  fixed: Uint8Array | null = null;
  months: Int32Array | null = null;
  days: Int32Array | null = null;
  readonly #slot: SourceSlot;
  readonly #charge: Charge;
  /** Bytes charged for this chunk's arrays. */
  chargedBytes = 0;

  /** `charge` pays for every array before it is allocated (memtables pass their counter). */
  constructor(type: PhysicalType, charge: Charge = UNCHARGED) {
    this.type = type;
    this.layout = layoutOf(type);
    this.#charge = charge;
    const n = MEMORY_CHUNK_ROWS;
    this.#take(initialBytes(this.layout, type));
    switch (this.layout) {
      case "bool":
        this.bools = new Uint8Array(n);
        break;
      case "i32":
        this.i32 = new Int32Array(n);
        break;
      case "f64":
        if (type.kind === "locator") throw new TypeError("Locators are never stored");
        this.f64 = new Float64Array(n);
        break;
      case "f32":
        this.f32 = new Float32Array(n);
        break;
      case "int64":
        this.safe = new Float64Array(n);
        break;
      case "decimal":
        this.safe = new Float64Array(n);
        if (type.kind === "decimal" && type.scale === null) this.scales = new Uint16Array(n);
        break;
      case "varlen":
        this.arena = new Uint8Array(256);
        this.offsets = new Int32Array(n + 1);
        break;
      case "fixed16":
        this.fixed = new Uint8Array(n * 16);
        break;
      case "interval":
        this.months = new Int32Array(n);
        this.days = new Int32Array(n);
        this.safe = new Float64Array(n);
        break;
    }
    this.#slot = { flat: null, index: -1 };
  }

  get full(): boolean {
    return this.length >= MEMORY_CHUNK_ROWS;
  }

  #take(bytes: number): void {
    charged(this.#charge, bytes);
    this.chargedBytes += bytes;
  }

  #give(bytes: number): void {
    this.#charge.free(bytes);
    this.chargedBytes -= bytes;
  }

  /** Returns every charged byte (the chunk's arrays are no longer the memtable's). */
  releaseCharge(): void {
    this.#give(this.chargedBytes);
  }

  /** Bytes the chunk's arrays hold (for memtable accounting). */
  get bytes(): number {
    let total = this.validity?.byteLength ?? 0;
    for (const array of [
      this.bools,
      this.i32,
      this.f64,
      this.f32,
      this.safe,
      this.big,
      this.limbs,
      this.scales,
      this.specials,
      this.arena,
      this.offsets,
      this.fixed,
      this.months,
      this.days,
    ])
      if (array !== null) total += array.byteLength;
    return total;
  }

  #setNull(slot: number): void {
    if (this.validity === null) {
      this.#take(MEMORY_CHUNK_ROWS / 8);
      const words = new Uint32Array(MEMORY_CHUNK_ROWS / 32);
      words.fill(0xffff_ffff);
      this.validity = words;
    }
    const word = slot >>> 5;
    this.validity[word] = (this.validity[word] ?? 0) & ~(1 << (slot & 31));
    this.nullCount += 1;
  }

  /** A new validity bitmap whose slot is valid again (a truncate re-opened it). */
  #setValid(slot: number): void {
    if (this.validity === null) return;
    if (bitValid(this.validity, slot)) return;
    const copy = this.validity.slice();
    copy[slot >>> 5] = (copy[slot >>> 5] ?? 0) | (1 << (slot & 31));
    this.validity = copy;
  }

  /**
   * Appends rows of `vector`: `rows[from … to)` when `rows` is given, else physical rows
   * `from … to`. The caller has checked the type. Returns nothing; throws `RangeError` for a value
   * the memtable cannot store (a decimal coefficient past 128 bits).
   */
  append(vector: Vector, rows: Uint16Array | null, from: number, to: number): void {
    const slot = this.#slot;
    try {
      for (let k = from; k < to; k++) {
        const row = rows === null ? k : (rows[k] ?? 0);
        sourceSlot(vector, row, slot);
        this.#appendSlot(slot.flat, slot.index);
      }
    } finally {
      // Values have been copied, including any prefix before a failed append. Retaining this
      // borrowed vector would keep a whole previous input batch outside the chunk's charge.
      this.#slot.flat = null;
      this.#slot.index = -1;
    }
  }

  #appendSlot(flat: FlatVector, index: number): void {
    const at = this.length;
    if (at >= MEMORY_CHUNK_ROWS) throw new RangeError("A memtable chunk is full");
    if (index < 0) {
      this.#setNull(at);
      if (this.layout === "varlen") {
        const offsets = need(this.offsets);
        offsets[at + 1] = offsets[at] ?? 0;
      }
      this.length = at + 1;
      return;
    }
    this.#setValid(at);
    const data = flat.data;
    switch (data.layout) {
      case "bool":
        need(this.bools)[at] = data.values[index] ?? 0;
        break;
      case "i32":
        need(this.i32)[at] = data.values[index] ?? 0;
        break;
      case "f64":
        need(this.f64)[at] = data.values[index] ?? 0;
        break;
      case "f32":
        need(this.f32)[at] = data.values[index] ?? 0;
        break;
      case "int64":
        this.#putInt64(at, data.values, index);
        break;
      case "decimal":
        this.#putDecimal(at, data, index);
        break;
      case "varlen": {
        const start = data.offsets[index] ?? 0;
        const end = data.offsets[index + 1] ?? 0;
        this.#putBytes(at, data.bytes, start, end);
        break;
      }
      case "fixed16":
        need(this.fixed).set(data.bytes.subarray(index * 16, index * 16 + 16), at * 16);
        break;
      case "interval":
        need(this.months)[at] = data.months[index] ?? 0;
        need(this.days)[at] = data.days[index] ?? 0;
        this.#putInt64(at, data.micros, index);
        break;
    }
    this.length = at + 1;
  }

  #putInt64(at: number, values: Int64Values, index: number): void {
    if (values.repr === "safe") {
      const value = values.values[index] ?? 0;
      if (this.big !== null) this.big[at] = BigInt(value);
      else need(this.safe)[at] = value;
      return;
    }
    const value = values.values[index] ?? 0n;
    if (this.big === null && value >= -BigInt(MAX_SAFE) && value <= BigInt(MAX_SAFE)) {
      need(this.safe)[at] = Number(value);
      return;
    }
    this.#widenToInt64();
    need(this.big)[at] = value;
  }

  #widenToInt64(): void {
    if (this.big !== null) return;
    const safe = need(this.safe);
    this.#take(MEMORY_CHUNK_ROWS * 8);
    const big = new BigInt64Array(MEMORY_CHUNK_ROWS);
    for (let slot = 0; slot < this.length; slot++) big[slot] = BigInt(safe[slot] ?? 0);
    this.big = big;
    this.safe = null;
    this.#give(MEMORY_CHUNK_ROWS * 8);
  }

  #widenToInt128(): void {
    if (this.limbs !== null) return;
    this.#take(MEMORY_CHUNK_ROWS * 16);
    const freed = (this.safe?.byteLength ?? 0) + (this.big?.byteLength ?? 0);
    const limbs = new Uint32Array(MEMORY_CHUNK_ROWS * 4);
    for (let slot = 0; slot < this.length; slot++)
      setInt128(
        limbs,
        slot,
        this.big !== null ? (this.big[slot] ?? 0n) : BigInt(need(this.safe)[slot] ?? 0),
      );
    this.limbs = limbs;
    this.safe = null;
    this.big = null;
    this.#give(freed);
  }

  #putDecimal(at: number, data: DecimalData, index: number): void {
    const special = data.specials?.[index] ?? DecimalSpecial.finite;
    if (special !== DecimalSpecial.finite) {
      if (this.specials === null) {
        this.#take(MEMORY_CHUNK_ROWS);
        this.specials = new Uint8Array(MEMORY_CHUNK_ROWS);
      }
      this.specials[at] = special;
    } else if (this.specials !== null) this.specials[at] = DecimalSpecial.finite;
    const scale = data.scale ?? data.scales?.[index] ?? 0;
    if (this.scales !== null) this.scales[at] = scale;
    else {
      const declared = this.type.kind === "decimal" ? this.type.scale : null;
      if (declared !== null && scale !== Math.max(0, declared))
        throw new RangeError(
          `A NUMERIC value has scale ${String(scale)}, not the column's ${String(declared)}`,
        );
    }
    const coefficients = data.coefficients;
    let value: bigint;
    if (coefficients.repr === "safe") {
      const number = coefficients.values[index] ?? 0;
      if (this.limbs === null && this.big === null) {
        need(this.safe)[at] = number;
        return;
      }
      value = BigInt(number);
    } else if (coefficients.repr === "int64") value = coefficients.values[index] ?? 0n;
    else if (coefficients.repr === "int128") value = int128At(coefficients.limbs, index);
    else value = coefficients.values[index] ?? 0n;
    if (this.limbs === null) {
      if (value >= -BigInt(MAX_SAFE) && value <= BigInt(MAX_SAFE) && this.big === null) {
        need(this.safe)[at] = Number(value);
        return;
      }
      if (value >= -(2n ** 63n) && value < 2n ** 63n) {
        this.#widenToInt64();
        need(this.big)[at] = value;
        return;
      }
      this.#widenToInt128();
    }
    setInt128(need(this.limbs), at, value);
  }

  #putBytes(at: number, source: Uint8Array, start: number, end: number): void {
    const offsets = need(this.offsets);
    const used = offsets[at] ?? 0;
    const total = used + (end - start);
    let arena = need(this.arena);
    if (total > arena.length) {
      let size = arena.length * 2;
      while (size < total) size *= 2;
      let grown = this.#charge.reuseBytes?.(size);
      if (grown === undefined) {
        this.#take(size);
        grown = new Uint8Array(size);
      } else this.chargedBytes += grown.byteLength;
      grown.set(arena.subarray(0, used));
      this.#give(arena.byteLength);
      arena = grown;
      this.arena = grown;
    }
    copyRange(arena, used, source, start, end);
    offsets[at + 1] = total;
  }

  /** Drops slots from `length` on (rows no reader can see). */
  truncate(length: number): void {
    if (length >= this.length) return;
    if (this.validity !== null) {
      let removed = 0;
      for (let slot = length; slot < this.length; slot++)
        if (!bitValid(this.validity, slot)) removed += 1;
      this.nullCount -= removed;
    }
    this.length = length;
  }

  /** A view of slots `0 … length − 1` (or fewer): immutable for its length. */
  view(length = this.length): FlatVector {
    return { form: "flat", type: this.type, length, validity: this.validity, data: this.#data() };
  }

  #int64(): Int64Values {
    return this.big !== null
      ? { repr: "int64", values: this.big }
      : { repr: "safe", values: need(this.safe) };
  }

  #data(): VectorData {
    switch (this.layout) {
      case "bool":
        return { layout: "bool", values: need(this.bools) };
      case "i32":
        return { layout: "i32", values: need(this.i32) };
      case "f64":
        return { layout: "f64", values: need(this.f64) };
      case "f32":
        return { layout: "f32", values: need(this.f32) };
      case "int64":
        return { layout: "int64", values: this.#int64() };
      case "decimal": {
        const declared = this.type.kind === "decimal" ? this.type.scale : null;
        return {
          layout: "decimal",
          scale: this.scales === null ? Math.max(0, declared ?? 0) : null,
          scales: this.scales,
          coefficients: this.limbs !== null ? { repr: "int128", limbs: this.limbs } : this.#int64(),
          specials: this.specials,
        };
      }
      case "varlen":
        return {
          layout: "varlen",
          bytes: need(this.arena),
          offsets: need(this.offsets),
        };
      case "fixed16":
        return { layout: "fixed16", bytes: need(this.fixed) };
      case "interval":
        return {
          layout: "interval",
          months: need(this.months),
          days: need(this.days),
          micros: this.#int64(),
        };
    }
  }

  /** UTF-8 bytes of the values in slots `[0, length)` (varlen), for statistics. */
  get varlenBytes(): number {
    return this.offsets === null ? 0 : (this.offsets[this.length] ?? 0);
  }
}

/** Bytes a fresh chunk of `layout` allocates (charged before its arrays exist). */
function initialBytes(layout: ReturnType<typeof layoutOf>, type: PhysicalType): number {
  const n = MEMORY_CHUNK_ROWS;
  switch (layout) {
    case "bool":
      return n;
    case "i32":
    case "f32":
      return n * 4;
    case "f64":
    case "int64":
      return n * 8;
    case "decimal":
      return n * 8 + (type.kind === "decimal" && type.scale === null ? n * 2 : 0);
    case "varlen":
      return 256 + (n + 1) * 4;
    case "fixed16":
      return n * 16;
    case "interval":
      return n * 16;
  }
}

// ── Frame column codec ─────────────────────────────────────────────────────────────────────────

const LAYOUT_TAG = {
  bool: 1,
  i32: 2,
  f64: 3,
  f32: 4,
  int64: 5,
  decimal: 6,
  varlen: 7,
  fixed16: 8,
  interval: 9,
} as const;

function writeInt64(writer: ByteSink, values: Int64Values, from: number, count: number): void {
  writer.u8(values.repr === "safe" ? 0 : 1);
  writer.typed(values.values.subarray(from, from + count));
}

function readInt64(reader: ByteReader, count: number): Int64Values {
  const repr = reader.u8();
  const bytes = reader.raw(count * 8).slice();
  if (repr === 0) return { repr: "safe", values: new Float64Array(bytes.buffer) };
  if (repr === 1) return { repr: "int64", values: new BigInt64Array(bytes.buffer) };
  throw new StoreCorruptionError("Bad int64 representation in a log column");
}

/**
 * Encodes slots `[from, from + count)` of `vector` (a stored flat vector), appended to `writer`
 * (a fresh one when omitted, whose bytes are returned).
 */
export function encodeColumn(
  vector: FlatVector,
  from: number,
  count: number,
  into?: ByteSink,
): Uint8Array {
  const writer = into ?? new ByteWriter(64 + count * 9);
  const data = vector.data;
  writer.u8(LAYOUT_TAG[data.layout]);
  writer.varuint(count);
  if (vector.validity === null) writer.u8(0);
  else {
    writer.u8(1);
    writeBits(writer, vector.validity, from, count);
  }
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      writer.range(data.values, from, count);
      break;
    case "int64":
      writeInt64(writer, data.values, from, count);
      break;
    case "decimal": {
      writer.u8(data.scale === null ? 1 : 0);
      if (data.scale === null) writer.typed(need(data.scales).subarray(from, from + count));
      else writer.varuint(data.scale);
      writer.u8(data.specials === null ? 0 : 1);
      if (data.specials !== null) writer.raw(data.specials.subarray(from, from + count));
      const coefficients = data.coefficients;
      if (coefficients.repr === "int128") {
        writer.u8(2);
        writer.typed(coefficients.limbs.subarray(from * 4, (from + count) * 4));
      } else if (coefficients.repr === "wide") {
        throw new RangeError("Decimal coefficients past 128 bits are not stored yet");
      } else writeInt64(writer, coefficients, from, count);
      break;
    }
    case "varlen": {
      const base = data.offsets[from] ?? 0;
      const end = data.offsets[from + count] ?? base;
      writer.ensure(4 * (count + 1) + (end - base));
      for (let k = 0; k <= count; k++) writer.u32((data.offsets[from + k] ?? base) - base);
      writer.rawRange(data.bytes, base, end);
      break;
    }
    case "fixed16":
      writer.raw(data.bytes.subarray(from * 16, (from + count) * 16));
      break;
    case "interval":
      writer.typed(data.months.subarray(from, from + count));
      writer.typed(data.days.subarray(from, from + count));
      writeInt64(writer, data.micros, from, count);
      break;
  }
  return into === undefined ? (writer as ByteWriter).finish() : EMPTY;
}

const EMPTY = new Uint8Array(0);

/** Validity bits `from … from + count − 1` as `ceil(count / 32)` little-endian words. */
function writeBits(writer: ByteSink, validity: Uint32Array, from: number, count: number): void {
  const words = Math.ceil(count / 32);
  const shift = from & 31;
  const base = from >>> 5;
  if (shift === 0) {
    const full = Math.floor(count / 32);
    writer.range(validity, base, full);
    const tail = count & 31;
    if (tail !== 0) writer.u32((validity[base + full] ?? 0) & ((1 << tail) - 1));
    return;
  }
  for (let word = 0; word < words; word++) {
    let value =
      ((validity[base + word] ?? 0) >>> shift) | ((validity[base + word + 1] ?? 0) << (32 - shift));
    if (word === words - 1 && (count & 31) !== 0) value &= (1 << (count & 31)) - 1;
    writer.u32(value);
  }
}

/** Decodes an encoded column into a fresh flat vector of `type`. */
export function decodeColumn(bytes: Uint8Array, type: PhysicalType): FlatVector {
  const reader = new ByteReader(bytes);
  const tag = reader.u8();
  const layout = layoutOf(type);
  if (LAYOUT_TAG[layout] !== tag)
    throw new StoreCorruptionError("A log column's layout differs from its type");
  const count = reader.varuint();
  const validity =
    reader.u8() === 1
      ? new Uint32Array(reader.raw(Math.ceil(count / 32) * 4).slice().buffer)
      : null;
  const copy = (bytesPer: number) => reader.raw(count * bytesPer).slice().buffer;
  let data: VectorData;
  switch (layout) {
    case "bool":
      data = { layout, values: new Uint8Array(copy(1)) };
      break;
    case "i32":
      data = { layout, values: new Int32Array(copy(4)) };
      break;
    case "f64":
      data = { layout, values: new Float64Array(copy(8)) };
      break;
    case "f32":
      data = { layout, values: new Float32Array(copy(4)) };
      break;
    case "int64":
      data = { layout, values: readInt64(reader, count) };
      break;
    case "decimal": {
      const mixed = reader.u8() === 1;
      const scales = mixed ? new Uint16Array(copy(2)) : null;
      const scale = mixed ? null : reader.varuint();
      const specials = reader.u8() === 1 ? reader.raw(count).slice() : null;
      const tier = reader.u8();
      let coefficients: DecimalData["coefficients"];
      if (tier === 2) coefficients = { repr: "int128", limbs: new Uint32Array(copy(16)) };
      else {
        reader.offset -= 1;
        coefficients = readInt64(reader, count);
      }
      data = { layout, scale, scales, coefficients, specials };
      break;
    }
    case "varlen": {
      const relative = new Int32Array(reader.raw((count + 1) * 4).slice().buffer);
      const values = reader.raw(relative[count] ?? 0).slice();
      data = { layout, bytes: values, offsets: relative };
      break;
    }
    case "fixed16":
      data = { layout, bytes: new Uint8Array(copy(16)) };
      break;
    case "interval": {
      const months = new Int32Array(copy(4));
      const days = new Int32Array(copy(4));
      data = { layout, months, days, micros: readInt64(reader, count) };
      break;
    }
  }
  reader.end();
  return { form: "flat", type, length: count, validity, data };
}
