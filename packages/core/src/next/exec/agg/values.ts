/**
 * Per-group values: a growable typed column indexed by group id, appended slot by slot from
 * vectors of any form, overwritten in place, and emitted as flat vectors. It holds group key
 * values, MIN/MAX states and single-value results. (B7's exec/column-store.ts is the shared
 * append-only row store for spill and join builds; this one is indexed and updatable.)
 *
 * - Storage is struct-of-arrays per layout (no object per value): fixed-width values in typed
 *   arrays, varlen bytes in one arena with offsets, int64 and decimal coefficients as f64 while
 *   exact with a sparse BigInt side table for the rare wider ones.
 * - Every byte is charged to the `ByteCharge` (a grant) before it is allocated; growth doubles and
 *   counts the overlap while copying. `close` frees everything.
 * - `set` overwrites an entry in place (MIN/MAX updates). For varlen it appends new bytes and
 *   repoints the entry; the arena is compacted when more than half of it is dead.
 */
import type { PhysicalType } from "../../types.js";
import { layoutOf } from "../../types.js";
import { MAX_SAFE } from "../../vector/int64.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { FlatVector, Validity, Vector, VectorData } from "../../vector/vector.js";
import { bigAt, coefficientAt, scaleAt, specialAt } from "../kernels/numeric.js";
import { slotOf } from "../kernels/vectors.js";

const MAX_SAFE_BIG = BigInt(MAX_SAFE);

type Typed = Uint8Array | Uint16Array | Int32Array | Float32Array | Float64Array;

export class GroupValues {
  readonly layout: ReturnType<typeof layoutOf>;
  #length = 0;
  #capacity = 0;
  #charged = 0;
  /** 1 where the entry is SQL null. */
  #nulls = new Uint8Array(0);
  #values: Typed = new Float64Array(0);
  /** int64 and decimal coefficients that do not fit the safe range. */
  readonly #wide = new Map<number, bigint>();
  #scales = new Uint16Array(0);
  #specials = new Uint8Array(0);
  #days = new Int32Array(0);
  #bytes = new Uint8Array(0);
  /** varlen: start and end of each entry in `#bytes`. */
  #starts = new Int32Array(0);
  #ends = new Int32Array(0);
  #used = 0;
  #dead = 0;

  constructor(
    readonly type: PhysicalType,
    readonly charge: ByteCharge,
  ) {
    this.layout = layoutOf(type);
  }

  get length(): number {
    return this.#length;
  }

  #take(bytes: number): void {
    if (!this.charge.allocate(bytes))
      throw new RangeError(
        `A ${String(bytes)}-byte group value store growth was refused by its grant`,
      );
    this.#charged += bytes;
  }

  #give(bytes: number): void {
    this.charge.free(bytes);
    this.#charged -= bytes;
  }

  #grow<T extends Typed>(old: T, make: (n: number) => T, capacity: number): T {
    const next = make(capacity);
    this.#take(next.byteLength);
    next.set(old);
    this.#give(old.byteLength);
    return next;
  }

  #ensure(capacity: number): void {
    if (capacity <= this.#capacity) return;
    const next = Math.max(64, capacity, 2 * this.#capacity);
    this.#nulls = this.#grow(this.#nulls, (n) => new Uint8Array(n), next);
    switch (this.layout) {
      case "bool":
        this.#values = this.#grow(this.#values as Uint8Array, (n) => new Uint8Array(n), next);
        break;
      case "i32":
        this.#values = this.#grow(this.#values as Int32Array, (n) => new Int32Array(n), next);
        break;
      case "f32":
        this.#values = this.#grow(this.#values as Float32Array, (n) => new Float32Array(n), next);
        break;
      case "f64":
      case "int64":
        this.#values = this.#grow(this.#values as Float64Array, (n) => new Float64Array(n), next);
        break;
      case "decimal":
        this.#values = this.#grow(this.#values as Float64Array, (n) => new Float64Array(n), next);
        this.#scales = this.#grow(this.#scales, (n) => new Uint16Array(n), next);
        this.#specials = this.#grow(this.#specials, (n) => new Uint8Array(n), next);
        break;
      case "interval":
        this.#values = this.#grow(this.#values as Float64Array, (n) => new Float64Array(n), next);
        this.#days = this.#grow(this.#days, (n) => new Int32Array(n), next);
        this.#scales = new Uint16Array(0);
        this.#starts = this.#grow(this.#starts, (n) => new Int32Array(n), next); // months
        break;
      case "varlen":
      case "fixed16":
        this.#starts = this.#grow(this.#starts, (n) => new Int32Array(n), next);
        this.#ends = this.#grow(this.#ends, (n) => new Int32Array(n), next);
        break;
    }
    this.#capacity = next;
  }

  #bytesFor(extra: number): void {
    if (this.#used + extra <= this.#bytes.length) return;
    if (this.#dead > this.#used / 2) this.#compact();
    if (this.#used + extra <= this.#bytes.length) return;
    const size = Math.max(1024, this.#used + extra, 2 * this.#bytes.length);
    const next = new Uint8Array(size);
    this.#take(size);
    next.set(this.#bytes.subarray(0, this.#used));
    this.#give(this.#bytes.length);
    this.#bytes = next;
  }

  #compact(): void {
    const next = new Uint8Array(Math.max(1024, this.#used - this.#dead));
    this.#take(next.length);
    let at = 0;
    for (let e = 0; e < this.#length; e++) {
      const start = this.#starts[e] ?? 0;
      const end = this.#ends[e] ?? 0;
      next.set(this.#bytes.subarray(start, end), at);
      this.#starts[e] = at;
      at += end - start;
      this.#ends[e] = at;
    }
    this.#give(this.#bytes.length);
    this.#bytes = next;
    this.#used = at;
    this.#dead = 0;
  }

  /** Appends slot `row` of `vector` (null when that slot is invalid). Returns the new index. */
  append(vector: Vector, row: number): number {
    const index = this.#length;
    this.#ensure(index + 1);
    this.#length = index + 1;
    if (this.layout === "varlen" || this.layout === "fixed16") {
      this.#starts[index] = this.#used;
      this.#ends[index] = this.#used;
    }
    this.set(index, vector, row);
    return index;
  }

  /** Overwrites entry `index` with slot `row` of `vector`. */
  set(index: number, vector: Vector, row: number): void {
    const valid =
      vector.form === "constant"
        ? vector.value.validity === null || ((vector.value.validity[0] ?? 0) & 1) === 1
        : vector.validity === null || ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
    this.#nulls[index] = valid ? 0 : 1;
    if (!valid) return;
    const [flat, slot] = slotOf(vector, row);
    this.#setValue(index, flat.data, slot);
  }

  #setValue(index: number, data: VectorData, slot: number): void {
    switch (data.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64":
        this.#values[index] = data.values[slot] ?? 0;
        return;
      case "int64": {
        this.#wide.delete(index);
        if (data.values.repr === "safe") this.#values[index] = data.values.values[slot] ?? 0;
        else this.#setBig(index, bigAt(data.values, slot));
        return;
      }
      case "decimal": {
        this.#wide.delete(index);
        this.#specials[index] = specialAt(data, slot);
        this.#scales[index] = scaleAt(data, slot);
        if (data.coefficients.repr === "safe")
          this.#values[index] = data.coefficients.values[slot] ?? 0;
        else this.#setBig(index, coefficientAt(data, slot));
        return;
      }
      case "interval": {
        this.#starts[index] = data.months[slot] ?? 0;
        this.#days[index] = data.days[slot] ?? 0;
        this.#wide.delete(index);
        if (data.micros.repr === "safe") this.#values[index] = data.micros.values[slot] ?? 0;
        else this.#setBig(index, bigAt(data.micros, slot));
        return;
      }
      case "varlen": {
        const start = data.offsets[slot] ?? 0;
        const end = data.offsets[slot + 1] ?? 0;
        this.#writeBytes(index, data.bytes, start, end);
        return;
      }
      case "fixed16":
        this.#writeBytes(index, data.bytes, slot * 16, slot * 16 + 16);
        return;
    }
  }

  #writeBytes(index: number, source: Uint8Array, start: number, end: number): void {
    const length = end - start;
    const oldStart = this.#starts[index] ?? 0;
    const oldEnd = this.#ends[index] ?? 0;
    if (length <= oldEnd - oldStart) {
      this.#bytes.set(source.subarray(start, end), oldStart);
      this.#dead += oldEnd - oldStart - length;
      this.#ends[index] = oldStart + length;
      return;
    }
    this.#dead += oldEnd - oldStart;
    this.#bytesFor(length);
    this.#bytes.set(source.subarray(start, end), this.#used);
    this.#starts[index] = this.#used;
    this.#used += length;
    this.#ends[index] = this.#used;
  }

  #setBig(index: number, value: bigint): void {
    if (value >= -MAX_SAFE_BIG && value <= MAX_SAFE_BIG) this.#values[index] = Number(value);
    else this.#wide.set(index, value);
  }

  isNull(index: number): boolean {
    return this.#nulls[index] === 1;
  }

  /** Fixed number layouts: the stored number (safe int64 and decimal coefficients included). */
  number(index: number): number {
    return this.#values[index] ?? 0;
  }

  /** int64 / decimal coefficient as bigint. */
  big(index: number): bigint {
    return this.#wide.get(index) ?? BigInt(this.#values[index] ?? 0);
  }

  isWide(index: number): boolean {
    return this.#wide.has(index);
  }

  scale(index: number): number {
    return this.#scales[index] ?? 0;
  }

  special(index: number): number {
    return this.#specials[index] ?? 0;
  }

  /** varlen / fixed16 bytes of entry `index` (a view, valid until the next write). */
  bytes(index: number): Uint8Array {
    return this.#bytes.subarray(this.#starts[index] ?? 0, this.#ends[index] ?? 0);
  }

  get byteArena(): Uint8Array {
    return this.#bytes;
  }

  start(index: number): number {
    return this.#starts[index] ?? 0;
  }

  end(index: number): number {
    return this.#ends[index] ?? 0;
  }

  /** Entries [from, from + count) as a new, exactly sized flat vector (caller charges it). */
  emit(from: number, count: number): FlatVector {
    return this.emitAt(null, from, count);
  }

  /** Entries `ids[0 … count)` (or from … from + count − 1 when `ids` is null), gathered. */
  emitAt(ids: Int32Array | null, from: number, count: number): FlatVector {
    const at = (k: number): number => (ids === null ? from + k : (ids[k] ?? 0));
    const type = this.type;
    let anyNull = false;
    for (let k = 0; k < count; k++) if (this.#nulls[at(k)] === 1) anyNull = true;
    let validity: Validity | null = null;
    if (anyNull) {
      validity = new Uint32Array((count + 31) >>> 5);
      for (let k = 0; k < count; k++)
        if (this.#nulls[at(k)] !== 1)
          validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
    }
    let wideInRange = false;
    if (this.#wide.size > 0)
      for (let k = 0; k < count; k++) if (this.#wide.has(at(k))) wideInRange = true;
    const numbers = this.#values;
    const pick = (k: number) => numbers[at(k)] ?? 0;
    let data: VectorData;
    switch (this.layout) {
      case "bool":
        data = { layout: "bool", values: Uint8Array.from({ length: count }, (_, k) => pick(k)) };
        break;
      case "i32":
        data = { layout: "i32", values: Int32Array.from({ length: count }, (_, k) => pick(k)) };
        break;
      case "f32":
        data = { layout: "f32", values: Float32Array.from({ length: count }, (_, k) => pick(k)) };
        break;
      case "f64":
        data = { layout: "f64", values: Float64Array.from({ length: count }, (_, k) => pick(k)) };
        break;
      case "int64":
        data = {
          layout: "int64",
          values: wideInRange
            ? {
                repr: "int64",
                values: BigInt64Array.from({ length: count }, (_, k) => this.big(at(k))),
              }
            : { repr: "safe", values: Float64Array.from({ length: count }, (_, k) => pick(k)) },
        };
        break;
      case "decimal": {
        const specials = Uint8Array.from({ length: count }, (_, k) => this.#specials[at(k)] ?? 0);
        data = {
          layout: "decimal",
          scale: null,
          scales: Uint16Array.from({ length: count }, (_, k) => this.#scales[at(k)] ?? 0),
          coefficients: wideInRange
            ? { repr: "wide", values: Array.from({ length: count }, (_, k) => this.big(at(k))) }
            : { repr: "safe", values: Float64Array.from({ length: count }, (_, k) => pick(k)) },
          specials: specials.some((x) => x !== 0) ? specials : null,
        };
        break;
      }
      case "interval":
        data = {
          layout: "interval",
          months: Int32Array.from({ length: count }, (_, k) => this.#starts[at(k)] ?? 0),
          days: Int32Array.from({ length: count }, (_, k) => this.#days[at(k)] ?? 0),
          micros: wideInRange
            ? {
                repr: "int64",
                values: BigInt64Array.from({ length: count }, (_, k) => this.big(at(k))),
              }
            : { repr: "safe", values: Float64Array.from({ length: count }, (_, k) => pick(k)) },
        };
        break;
      case "fixed16": {
        const bytes = new Uint8Array(16 * count);
        for (let k = 0; k < count; k++) bytes.set(this.bytes(at(k)).subarray(0, 16), 16 * k);
        data = { layout: "fixed16", bytes };
        break;
      }
      case "varlen": {
        let total = 0;
        for (let k = 0; k < count; k++)
          total += (this.#ends[at(k)] ?? 0) - (this.#starts[at(k)] ?? 0);
        const bytes = new Uint8Array(total);
        const offsets = new Int32Array(count + 1);
        let o = 0;
        for (let k = 0; k < count; k++) {
          const start = this.#starts[at(k)] ?? 0;
          const end = this.#ends[at(k)] ?? 0;
          bytes.set(this.#bytes.subarray(start, end), o);
          o += end - start;
          offsets[k + 1] = o;
        }
        data = { layout: "varlen", bytes, offsets };
        break;
      }
    }
    return { form: "flat", type, length: count, validity, data };
  }

  /** Bytes this store holds from its charge. */
  get chargedBytes(): number {
    return this.#charged;
  }

  close(): void {
    if (this.#charged > 0) this.charge.free(this.#charged);
    this.#charged = 0;
    this.#length = 0;
    this.#capacity = 0;
    this.#nulls = new Uint8Array(0);
    this.#values = new Float64Array(0);
    this.#bytes = new Uint8Array(0);
    this.#starts = new Int32Array(0);
    this.#ends = new Int32Array(0);
    this.#wide.clear();
  }
}

/** Bytes of a flat vector's buffers (for charging an emitted batch). */
export function flatBytes(vector: FlatVector): number {
  const data = vector.data;
  let bytes = vector.validity?.byteLength ?? 0;
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return bytes + data.values.byteLength;
    case "int64":
      return bytes + data.values.values.byteLength;
    case "decimal":
      bytes += (data.scales?.byteLength ?? 0) + (data.specials?.byteLength ?? 0);
      return (
        bytes +
        (data.coefficients.repr === "wide"
          ? 32 * data.coefficients.values.length
          : data.coefficients.repr === "int128"
            ? data.coefficients.limbs.byteLength
            : data.coefficients.values.byteLength)
      );
    case "varlen":
      return bytes + data.bytes.byteLength + data.offsets.byteLength;
    case "fixed16":
      return bytes + data.bytes.byteLength;
    case "interval":
      return bytes + data.months.byteLength + data.days.byteLength + data.micros.values.byteLength;
  }
}
