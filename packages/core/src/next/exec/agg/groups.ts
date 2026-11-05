/**
 * Group ids for GROUP BY: maps each active row's key to a dense group id (0, 1, 2… in first-seen
 * order), creating groups as new keys arrive, and emits the groups' key values.
 *
 * ## Equality (SQL GROUP BY)
 * NULLs group together; floats group −0 with 0 and every NaN together; NUMERIC groups by value
 * (1.5 and 1.50); text by bytes. A group's key value is the first one seen.
 *
 * ## Structure
 * One key column maps to dense value ids through a `KeyColumn` chosen by its type:
 * - `Int32Keys` (int2, int4, date and other 32-bit layouts): one interleaved (key, id) table, so a
 *   probe reads one cache line.
 * - `NumberKeys` (bool, int8, floats, times, timestamps): open addressing over the value
 *   as f64, the key stored beside its slot (one probe reads id, hash and key at one index). Exact
 *   int64 keys past 2^53 use a side map (rare; one BigInt each).
 * - `ByteKeys` (text, bytes, enum labels, uuid): open addressing over xxHash32, the hash stored in
 *   the slot array, the bytes in one arena. A dictionary column is translated once per entry and
 *   dictionary; rows then map code → id with one array read.
 * - `EncodedKeys` (decimal, interval, json, array): each row's equality-key bytes
 *   (keys/key-encoding.ts, batched) in the byte table, the first value kept for output.
 * A single key's value ids are the group ids. Several keys map each column to its value ids, then
 * the id tuple to a group id (`TupleKeys`): composite keys over dictionaries cost a few array
 * reads per row, never a byte encoding.
 *
 * A new id is inserted at the empty slot its probe stopped at; tables double at load ½ by
 * re-placing ids from their stored hashes (plain loops, no callback per id).
 *
 * ## Memory
 * Every typed array is charged to the grant before it grows. A refused growth throws `RangeError`:
 * partitioned aggregation spill is milestone 3.
 */
import { xxHash32 } from "../../hash/xxhash32.js";
import { KeyBatch, encodeKeyBatch } from "../../keys/key-encoding.js";
import type { PhysicalType } from "../../types.js";
import { layoutOf } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { FlatVector, Selection, Validity, Vector, VectorData } from "../../vector/vector.js";
import { IDENTITY, flatOf } from "../kernels/vectors.js";
import { GroupValues } from "./values.js";

/** Charged growable typed arrays (shared by the group tables and the accumulators). */
export class Charged {
  #bytes = 0;
  constructor(readonly charge: ByteCharge) {}
  take(bytes: number): void {
    if (!this.charge.allocate(bytes))
      throw new RangeError(`A ${String(bytes)}-byte aggregation growth was refused by its grant`);
    this.#bytes += bytes;
  }
  give(bytes: number): void {
    this.charge.free(bytes);
    this.#bytes -= bytes;
  }
  #grow<T extends Uint8Array | Uint16Array | Uint32Array | Int32Array | Float64Array>(
    old: T,
    next: T,
  ): T {
    this.take(next.byteLength);
    next.set(old.subarray(0, Math.min(old.length, next.length)));
    this.give(old.byteLength);
    return next;
  }
  uint8(old: Uint8Array, length: number): Uint8Array {
    return this.#grow(old, new Uint8Array(length));
  }
  uint16(old: Uint16Array, length: number): Uint16Array {
    return this.#grow(old, new Uint16Array(length));
  }
  uint32(old: Uint32Array, length: number): Uint32Array {
    return this.#grow(old, new Uint32Array(length));
  }
  int32(old: Int32Array, length: number): Int32Array {
    return this.#grow(old, new Int32Array(length));
  }
  float64(old: Float64Array, length: number): Float64Array {
    return this.#grow(old, new Float64Array(length));
  }
  close(): void {
    if (this.#bytes > 0) this.charge.free(this.#bytes);
    this.#bytes = 0;
  }
}

const F64 = new Float64Array(1);
const WORDS = new Int32Array(F64.buffer);

function finalize(h: number): number {
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  return h ^ (h >>> 16);
}

/** A 32-bit hash of a number (−0 and NaN normalized by the caller); integers hash fast. */
function hashNumber(value: number): number {
  if ((value | 0) === value) return finalize(Math.imul(value, 0x9e3779b1));
  F64[0] = value;
  return finalize((WORDS[0] ?? 0) ^ Math.imul(WORDS[1] ?? 0, 0x9e3779b1));
}

/** The NULL id's validity over the emitted ids (null when none is the NULL id). */
function validityFromNull(
  nullId: number,
  ids: Int32Array | null,
  from: number,
  count: number,
): Validity | null {
  if (nullId < 0) return null;
  let any = false;
  const validity = new Uint32Array((count + 31) >>> 5);
  for (let k = 0; k < count; k++) {
    const id = ids === null ? from + k : (ids[k] ?? 0);
    if (id === nullId) any = true;
    else validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
  }
  return any ? validity : null;
}

/** One key column → dense value ids. */
interface KeyColumn {
  readonly count: number;
  /** ids[row] for every active row; `length` is the batch's. */
  assign(key: Vector, length: number, rows: Selection | null, ids: Int32Array): void;
  /** The key values of ids from … from + count − 1, or of `ids[0 … count)` when given. */
  emit(type: PhysicalType, from: number, count: number, ids: Int32Array | null): FlatVector;
  close(): void;
}

/** Open-addressing slots: id + 1 (0 empty) and the stored hash, side by side. */
class Slots {
  ids: Int32Array = new Int32Array(1024);
  hashes: Int32Array = new Int32Array(1024);
  mask = 1023;
  constructor(readonly memory: Charged) {
    memory.take(8 * 1024);
  }
  /** Whether `used` ids need a larger table (load factor ½). */
  full(used: number): boolean {
    return 2 * used > this.ids.length;
  }
  /**
   * Doubles the table and re-places ids 0 … ids − 1 from their stored hashes; ids with
   * `outside[id] === 1` (NULL, wide int64 keys) stay out of it.
   */
  grow(idHashes: Int32Array, ids: number, outside: Uint8Array | null): void {
    const size = 2 * this.ids.length;
    this.memory.take(8 * size);
    this.memory.give(8 * this.ids.length);
    const slotIds = new Int32Array(size);
    const slotHashes = new Int32Array(size);
    const mask = size - 1;
    for (let id = 0; id < ids; id++) {
      if (outside !== null && outside[id] === 1) continue;
      const h = idHashes[id] ?? 0;
      let s = h & mask;
      while (slotIds[s] !== 0) s = (s + 1) & mask;
      slotIds[s] = id + 1;
      slotHashes[s] = h;
    }
    this.ids = slotIds;
    this.hashes = slotHashes;
    this.mask = mask;
  }
}

/** Per-id hashes and the "outside the slot table" marks, grown together. */
class IdColumns {
  hashes: Int32Array = new Int32Array(1024);
  outside: Uint8Array = new Uint8Array(1024);
  constructor(readonly memory: Charged) {
    memory.take(5 * 1024);
  }
  ensure(id: number): void {
    if (id < this.hashes.length) return;
    this.hashes = this.memory.int32(this.hashes, 2 * this.hashes.length);
    this.outside = this.memory.uint8(this.outside, 2 * this.outside.length);
  }
}

class NumberKeys implements KeyColumn {
  readonly #memory: Charged;
  readonly #slots: Slots;
  readonly #idColumns: IdColumns;
  /** The key of each id (output). */
  #keys: Float64Array = new Float64Array(1024);
  /** The key beside each slot. */
  #slotKeys: Float64Array = new Float64Array(1024);
  count = 0;
  #nullId = -1;
  readonly #wide = new Map<bigint, number>();
  readonly #wideOf = new Map<number, bigint>();
  constructor(memory: Charged) {
    this.#memory = memory;
    memory.take(8 * 1024 + 8 * 1024);
    this.#slots = new Slots(memory);
    this.#idColumns = new IdColumns(memory);
  }
  #newId(outside: boolean): number {
    const id = this.count++;
    this.#idColumns.ensure(id);
    if (id >= this.#keys.length)
      this.#keys = this.#memory.float64(this.#keys, 2 * this.#keys.length);
    this.#idColumns.outside[id] = outside ? 1 : 0;
    return id;
  }
  /** Grows the slot table (and the key column beside it) when the load passes ½. */
  #maybeGrow(): void {
    const slots = this.#slots;
    if (!slots.full(this.count)) return;
    slots.grow(this.#idColumns.hashes, this.count, this.#idColumns.outside);
    const slotKeys = new Float64Array(slots.ids.length);
    this.#memory.take(slotKeys.byteLength);
    this.#memory.give(this.#slotKeys.byteLength);
    const keys = this.#keys;
    const slotIds = slots.ids;
    for (let s = 0; s < slotIds.length; s++) {
      const entry = slotIds[s] ?? 0;
      if (entry !== 0) slotKeys[s] = keys[entry - 1] ?? 0;
    }
    this.#slotKeys = slotKeys;
  }
  assign(key: Vector, length: number, selection: Selection | null, ids: Int32Array): void {
    const rows = selection === null ? IDENTITY : selection.rows;
    const count = selection === null ? length : selection.count;
    const data = flatOf(key).data;
    const constant = key.form === "constant";
    const validity = constant ? key.value.validity : key.validity;
    const codes = key.form === "dictionary" ? key.codes : null;
    let numbers: ArrayLike<number> | null = null;
    let bigs: BigInt64Array | null = null;
    if (data.layout === "int64") {
      if (data.values.repr === "safe") numbers = data.values.values;
      else bigs = data.values.values;
    } else if (
      data.layout === "bool" ||
      data.layout === "i32" ||
      data.layout === "f32" ||
      data.layout === "f64"
    )
      numbers = data.values;
    else throw new TypeError(`number group keys over ${data.layout}`);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const validRow = constant ? 0 : i;
      if (validity !== null && ((validity[validRow >>> 5] ?? 0) & (1 << (validRow & 31))) === 0) {
        if (this.#nullId < 0) this.#nullId = this.#newId(true);
        ids[i] = this.#nullId;
        continue;
      }
      const slot = constant ? 0 : codes !== null ? (codes[i] ?? 0) : i;
      let value: number;
      if (bigs !== null) {
        const big = bigs[slot] ?? 0n;
        if (big > 9007199254740991n || big < -9007199254740991n) {
          let id = this.#wide.get(big);
          if (id === undefined) {
            id = this.#newId(true);
            this.#wide.set(big, id);
            this.#wideOf.set(id, big);
          }
          ids[i] = id;
          continue;
        }
        value = Number(big);
      } else value = (numbers?.[slot] ?? 0) + 0; // −0 groups with 0
      const nan = value !== value;
      const h = hashNumber(nan ? NaN : value);
      const slots = this.#slots;
      const slotIds = slots.ids;
      const slotHashes = slots.hashes;
      const slotKeys = this.#slotKeys;
      const mask = slots.mask;
      let s = h & mask;
      let id = -1;
      for (;;) {
        const entry = slotIds[s] ?? 0;
        if (entry === 0) break;
        if (slotHashes[s] === h) {
          const stored = slotKeys[s] ?? 0;
          if (stored === value || (nan && stored !== stored)) {
            id = entry - 1;
            break;
          }
        }
        s = (s + 1) & mask;
      }
      if (id < 0) {
        id = this.#newId(false);
        this.#keys[id] = value;
        this.#idColumns.hashes[id] = h;
        slotIds[s] = id + 1;
        slotHashes[s] = h;
        slotKeys[s] = value;
        this.#maybeGrow();
      }
      ids[i] = id;
    }
  }
  emit(type: PhysicalType, from: number, count: number, ids: Int32Array | null): FlatVector {
    const validity = validityFromNull(this.#nullId, ids, from, count);
    const keys = this.#keys;
    const nullId = this.#nullId;
    const values = new Float64Array(count);
    for (let k = 0; k < count; k++) {
      const id = ids === null ? from + k : (ids[k] ?? 0);
      values[k] = id === nullId ? 0 : (keys[id] ?? 0);
    }
    const layout = layoutOf(type);
    let data: VectorData;
    if (layout === "int64") {
      let wide = false;
      if (this.#wideOf.size > 0)
        for (let k = 0; k < count; k++)
          if (this.#wideOf.has(ids === null ? from + k : (ids[k] ?? 0))) wide = true;
      data = wide
        ? {
            layout,
            values: {
              repr: "int64",
              values: BigInt64Array.from(
                { length: count },
                (_, k) =>
                  this.#wideOf.get(ids === null ? from + k : (ids[k] ?? 0)) ??
                  BigInt(values[k] ?? 0),
              ),
            },
          }
        : { layout, values: { repr: "safe", values } };
    } else
      data =
        layout === "bool"
          ? { layout, values: Uint8Array.from(values) }
          : layout === "i32"
            ? { layout, values: Int32Array.from(values) }
            : layout === "f32"
              ? { layout, values: Float32Array.from(values) }
              : { layout: "f64", values };
    return { form: "flat", type, length: count, validity, data };
  }
  close(): void {
    this.#wide.clear();
    this.#wideOf.clear();
  }
}

/** A 32-bit integer's hash (Int32Keys). */
function hashInt32(value: number): number {
  let h = Math.imul(value, 0x9e3779b1);
  h ^= h >>> 15;
  h = Math.imul(h, 0x85ebca6b);
  return h ^ (h >>> 13);
}

/**
 * 32-bit integer keys (int2, int4, date…): one interleaved table of (key, id + 1) pairs, so a
 * probe touches one cache line; no stored hashes (rehashing an int is cheaper than reading one).
 */
class Int32Keys implements KeyColumn {
  readonly #memory: Charged;
  /** Slot s holds the key at 2s and id + 1 at 2s + 1 (0: empty). */
  #table: Int32Array = new Int32Array(2048);
  #mask = 1023;
  /** The key of each id (output). */
  #keys: Int32Array = new Int32Array(1024);
  count = 0;
  #nullId = -1;
  constructor(memory: Charged) {
    this.#memory = memory;
    memory.take(4 * 2048 + 4 * 1024);
  }
  #grow(): void {
    const size = 2 * (this.#mask + 1);
    const table = new Int32Array(2 * size);
    this.#memory.take(table.byteLength);
    this.#memory.give(this.#table.byteLength);
    const mask = size - 1;
    const keys = this.#keys;
    for (let id = 0; id < this.count; id++) {
      if (id === this.#nullId) continue;
      const key = keys[id] ?? 0;
      let s = hashInt32(key) & mask;
      while (table[2 * s + 1] !== 0) s = (s + 1) & mask;
      table[2 * s] = key;
      table[2 * s + 1] = id + 1;
    }
    this.#table = table;
    this.#mask = mask;
  }
  #newId(): number {
    const id = this.count++;
    if (id >= this.#keys.length) this.#keys = this.#memory.int32(this.#keys, 2 * this.#keys.length);
    return id;
  }
  assign(key: Vector, length: number, selection: Selection | null, ids: Int32Array): void {
    const rows = selection === null ? IDENTITY : selection.rows;
    const count = selection === null ? length : selection.count;
    const data = flatOf(key).data;
    if (data.layout !== "i32") throw new TypeError(`int32 group keys over ${data.layout}`);
    const values = data.values;
    const constant = key.form === "constant";
    const validity = constant ? key.value.validity : key.validity;
    const codes = key.form === "dictionary" ? key.codes : null;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const validRow = constant ? 0 : i;
      if (validity !== null && ((validity[validRow >>> 5] ?? 0) & (1 << (validRow & 31))) === 0) {
        if (this.#nullId < 0) this.#nullId = this.#newId();
        ids[i] = this.#nullId;
        continue;
      }
      const value = values[constant ? 0 : codes !== null ? (codes[i] ?? 0) : i] ?? 0;
      const table = this.#table;
      const mask = this.#mask;
      let s = hashInt32(value) & mask;
      let id = -1;
      for (;;) {
        const entry = table[2 * s + 1] ?? 0;
        if (entry === 0) break;
        if (table[2 * s] === value) {
          id = entry - 1;
          break;
        }
        s = (s + 1) & mask;
      }
      if (id < 0) {
        id = this.#newId();
        this.#keys[id] = value;
        table[2 * s] = value;
        table[2 * s + 1] = id + 1;
        if (2 * this.count > mask + 1) this.#grow();
      }
      ids[i] = id;
    }
  }
  emit(type: PhysicalType, from: number, count: number, ids: Int32Array | null): FlatVector {
    const validity = validityFromNull(this.#nullId, ids, from, count);
    const keys = this.#keys;
    const values = new Int32Array(count);
    if (ids === null) values.set(keys.subarray(from, from + count));
    else for (let k = 0; k < count; k++) values[k] = keys[ids[k] ?? 0] ?? 0;
    // The NULL id's key is never written (0), and its slot is masked by `validity` anyway.
    return { form: "flat", type, length: count, validity, data: { layout: "i32", values } };
  }
  close(): void {
    // Charged memory is freed by the owner's Charged.
  }
}

/** Bytes per id in one arena: id i is `bytes[starts[i] … starts[i + 1])`. */
class ByteArena {
  bytes: Uint8Array = new Uint8Array(0);
  starts: Int32Array = new Int32Array(1025);
  constructor(readonly memory: Charged) {
    memory.take(4 * 1025);
  }
  append(id: number, source: Uint8Array, start: number, end: number): void {
    if (id + 2 > this.starts.length)
      this.starts = this.memory.int32(this.starts, 2 * this.starts.length);
    const at = this.starts[id] ?? 0;
    const length = end - start;
    if (at + length > this.bytes.length) {
      const next = new Uint8Array(Math.max(4096, 2 * this.bytes.length, at + length));
      this.memory.take(next.byteLength);
      next.set(this.bytes.subarray(0, at));
      this.memory.give(this.bytes.byteLength);
      this.bytes = next;
    }
    this.bytes.set(source.subarray(start, end), at);
    this.starts[id + 1] = at + length;
  }
  equals(id: number, source: Uint8Array, start: number, end: number): boolean {
    const at = this.starts[id] ?? 0;
    const length = end - start;
    if ((this.starts[id + 1] ?? 0) - at !== length) return false;
    const bytes = this.bytes;
    for (let b = 0; b < length; b++) if (bytes[at + b] !== source[start + b]) return false;
    return true;
  }
}

const EMPTY = new Uint8Array(0);

class ByteKeys implements KeyColumn {
  readonly #slots: Slots;
  readonly #arena: ByteArena;
  readonly #idColumns: IdColumns;
  count = 0;
  #nullId = -1;
  #dictionaryId = -1;
  #translation: Int32Array = new Int32Array(0);
  constructor(readonly memory: Charged) {
    this.#slots = new Slots(memory);
    this.#arena = new ByteArena(memory);
    this.#idColumns = new IdColumns(memory);
  }
  #null(): number {
    if (this.#nullId < 0) {
      const id = this.count++;
      this.#idColumns.ensure(id);
      this.#idColumns.outside[id] = 1;
      this.#arena.append(id, EMPTY, 0, 0);
      this.#nullId = id;
    }
    return this.#nullId;
  }
  #id(source: Uint8Array, start: number, end: number): number {
    const h = xxHash32(source, 0, start, end) | 0;
    const slots = this.#slots;
    const slotIds = slots.ids;
    const slotHashes = slots.hashes;
    const mask = slots.mask;
    let s = h & mask;
    for (;;) {
      const entry = slotIds[s] ?? 0;
      if (entry === 0) break;
      if (slotHashes[s] === h && this.#arena.equals(entry - 1, source, start, end))
        return entry - 1;
      s = (s + 1) & mask;
    }
    const id = this.count++;
    this.#idColumns.ensure(id);
    this.#idColumns.hashes[id] = h;
    this.#idColumns.outside[id] = 0;
    this.#arena.append(id, source, start, end);
    slotIds[s] = id + 1;
    slotHashes[s] = h;
    if (slots.full(this.count))
      slots.grow(this.#idColumns.hashes, this.count, this.#idColumns.outside);
    return id;
  }
  #slotId(data: VectorData, slot: number): number {
    if (data.layout === "varlen")
      return this.#id(data.bytes, data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0);
    if (data.layout === "fixed16") return this.#id(data.bytes, slot * 16, slot * 16 + 16);
    throw new TypeError(`byte group keys over ${data.layout}`);
  }
  assign(key: Vector, length: number, selection: Selection | null, ids: Int32Array): void {
    const rows = selection === null ? IDENTITY : selection.rows;
    const count = selection === null ? length : selection.count;
    if (key.form === "dictionary") {
      // Each dictionary entry is looked up once per dictionary; rows map codes.
      if (this.#dictionaryId !== key.dictionaryId) {
        this.#dictionaryId = key.dictionaryId;
        if (this.#translation.length < key.dictionary.length)
          this.#translation = this.memory.int32(
            this.#translation,
            Math.max(key.dictionary.length, 2 * this.#translation.length),
          );
        this.#translation.fill(-1, 0, key.dictionary.length);
      }
      const translation = this.#translation;
      const codes = key.codes;
      const validity = key.validity;
      const data = key.dictionary.data;
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        if (validity !== null && ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0) {
          ids[i] = this.#null();
          continue;
        }
        const code = codes[i] ?? 0;
        let id = translation[code] ?? -1;
        if (id < 0) {
          id = this.#slotId(data, code);
          translation[code] = id;
        }
        ids[i] = id;
      }
      return;
    }
    const data = flatOf(key).data;
    const constant = key.form === "constant";
    const validity = constant ? key.value.validity : key.validity;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const slot = constant ? 0 : i;
      if (validity !== null && ((validity[slot >>> 5] ?? 0) & (1 << (slot & 31))) === 0) {
        ids[i] = this.#null();
        continue;
      }
      ids[i] = this.#slotId(data, slot);
    }
  }
  emit(type: PhysicalType, from: number, count: number, ids: Int32Array | null): FlatVector {
    const validity = validityFromNull(this.#nullId, ids, from, count);
    const arena = this.#arena;
    const at = (k: number) => (ids === null ? from + k : (ids[k] ?? 0));
    if (layoutOf(type) === "fixed16") {
      const bytes = new Uint8Array(16 * count);
      for (let k = 0; k < count; k++) {
        const id = at(k);
        if (id !== this.#nullId)
          bytes.set(arena.bytes.subarray(arena.starts[id] ?? 0, arena.starts[id + 1] ?? 0), 16 * k);
      }
      return { form: "flat", type, length: count, validity, data: { layout: "fixed16", bytes } };
    }
    let total = 0;
    for (let k = 0; k < count; k++) {
      const id = at(k);
      total += (arena.starts[id + 1] ?? 0) - (arena.starts[id] ?? 0);
    }
    const bytes = new Uint8Array(total);
    const offsets = new Int32Array(count + 1);
    let o = 0;
    for (let k = 0; k < count; k++) {
      const id = at(k);
      const start = arena.starts[id] ?? 0;
      const end = arena.starts[id + 1] ?? 0;
      bytes.set(arena.bytes.subarray(start, end), o);
      o += end - start;
      offsets[k + 1] = o;
    }
    return {
      form: "flat",
      type,
      length: count,
      validity,
      data: { layout: "varlen", bytes, offsets },
    };
  }
  close(): void {
    // Memory belongs to the owning table's Charged.
  }
}

/** Decimal, interval, JSON, array keys: equality-key bytes, first values kept for output. */
class EncodedKeys implements KeyColumn {
  readonly #slots: Slots;
  readonly #arena: ByteArena;
  readonly #values: GroupValues;
  readonly #batch: KeyBatch;
  readonly #idColumns: IdColumns;
  count = 0;
  constructor(
    type: PhysicalType,
    readonly memory: Charged,
  ) {
    this.#slots = new Slots(memory);
    this.#arena = new ByteArena(memory);
    this.#idColumns = new IdColumns(memory);
    this.#values = new GroupValues(type, memory.charge);
    this.#batch = new KeyBatch(memory.charge);
  }
  assign(key: Vector, length: number, selection: Selection | null, ids: Int32Array): void {
    const rows = selection === null ? IDENTITY : selection.rows;
    const count = selection === null ? length : selection.count;
    const columns = [{ vector: key, direction: "asc" as const, nulls: "last" as const }];
    let from = 0;
    while (from < count) {
      const result = encodeKeyBatch("equality", columns, { length, selection }, this.#batch, from);
      if (!result.done && result.encoded === 0)
        throw new RangeError("A group key's encoding was refused by its grant");
      for (let k = 0; k < result.encoded; k++) {
        const row = rows[from + k] ?? 0;
        ids[row] = this.#id(this.#batch.key(k), key, row);
      }
      from += result.encoded;
    }
  }
  #id(bytes: Uint8Array, key: Vector, row: number): number {
    const h = xxHash32(bytes) | 0;
    const slots = this.#slots;
    let s = h & slots.mask;
    for (;;) {
      const entry = slots.ids[s] ?? 0;
      if (entry === 0) break;
      if (slots.hashes[s] === h && this.#arena.equals(entry - 1, bytes, 0, bytes.length))
        return entry - 1;
      s = (s + 1) & slots.mask;
    }
    const id = this.count++;
    this.#idColumns.ensure(id);
    this.#idColumns.hashes[id] = h;
    this.#arena.append(id, bytes, 0, bytes.length);
    this.#values.append(key, row);
    slots.ids[s] = id + 1;
    slots.hashes[s] = h;
    if (slots.full(this.count)) slots.grow(this.#idColumns.hashes, this.count, null);
    return id;
  }
  emit(_type: PhysicalType, from: number, count: number, ids: Int32Array | null): FlatVector {
    return this.#values.emitAt(ids, from, count);
  }
  close(): void {
    this.#values.close();
    this.#batch.close();
  }
}

function keyColumnFor(type: PhysicalType, memory: Charged): KeyColumn {
  const layout = layoutOf(type);
  if (layout === "i32") return new Int32Keys(memory);
  if (layout === "bool" || layout === "f32" || layout === "f64" || layout === "int64")
    return new NumberKeys(memory);
  if (
    (layout === "varlen" || layout === "fixed16") &&
    type.kind !== "json" &&
    type.kind !== "array"
  )
    return new ByteKeys(memory);
  return new EncodedKeys(type, memory);
}

/** Group ids over the tuple of per-column value ids (several keys). */
class TupleKeys {
  readonly #slots: Slots;
  /** Per key column: the value id of each group. */
  readonly columnIds: Int32Array[];
  #hashes: Int32Array = new Int32Array(1024);
  count = 0;
  constructor(
    readonly width: number,
    readonly memory: Charged,
  ) {
    memory.take(4 * 1024 * (width + 1));
    this.#slots = new Slots(memory);
    this.columnIds = Array.from({ length: width }, () => new Int32Array(1024));
  }
  assign(ids: readonly Int32Array[], rows: Uint16Array, count: number, gids: Int32Array): void {
    const width = this.width;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      let h = 0x2545f491;
      for (let c = 0; c < width; c++) h = Math.imul(h ^ (ids[c]?.[i] ?? 0), 0x9e3779b1) + c;
      h = finalize(h);
      const slots = this.#slots;
      let s = h & slots.mask;
      let g = -1;
      for (;;) {
        const entry = slots.ids[s] ?? 0;
        if (entry === 0) break;
        if (slots.hashes[s] === h) {
          const candidate = entry - 1;
          let same = true;
          for (let c = 0; c < width; c++)
            if (this.columnIds[c]?.[candidate] !== ids[c]?.[i]) {
              same = false;
              break;
            }
          if (same) {
            g = candidate;
            break;
          }
        }
        s = (s + 1) & slots.mask;
      }
      if (g < 0) {
        g = this.count++;
        if (g >= this.#hashes.length) {
          const size = 2 * this.#hashes.length;
          this.#hashes = this.memory.int32(this.#hashes, size);
          this.columnIds.forEach((column, c) => {
            this.columnIds[c] = this.memory.int32(column, size);
          });
        }
        this.#hashes[g] = h;
        for (let c = 0; c < width; c++) {
          const column = this.columnIds[c];
          if (column !== undefined) column[g] = ids[c]?.[i] ?? 0;
        }
        slots.ids[s] = g + 1;
        slots.hashes[s] = h;
        if (slots.full(this.count)) slots.grow(this.#hashes, this.count, null);
      }
      gids[i] = g;
    }
  }
}

export class GroupTable {
  readonly #memory: Charged;
  readonly #columns: KeyColumn[];
  readonly #tuple: TupleKeys | null;
  /** Per key column: this batch's value ids (several keys only). */
  readonly #ids: Int32Array[];

  constructor(
    readonly keyTypes: readonly PhysicalType[],
    charge: ByteCharge,
    capacity = 2048,
  ) {
    this.#memory = new Charged(charge);
    this.#columns = keyTypes.map((type) => keyColumnFor(type, this.#memory));
    this.#tuple = keyTypes.length > 1 ? new TupleKeys(keyTypes.length, this.#memory) : null;
    if (this.#tuple !== null) this.#memory.take(4 * capacity * keyTypes.length);
    this.#ids = this.#tuple === null ? [] : keyTypes.map(() => new Int32Array(capacity));
  }

  get groupCount(): number {
    return this.#tuple !== null ? this.#tuple.count : (this.#columns[0]?.count ?? 0);
  }

  /** Writes the group id of each active row into `gids[row]`, creating groups for new keys. */
  assign(keys: readonly Vector[], length: number, rows: Selection | null, gids: Int32Array): void {
    const target = (c: number): Int32Array => (this.#tuple === null ? gids : this.#ids[c]) ?? gids;
    this.#columns.forEach((column, c) => {
      const key = keys[c];
      if (key === undefined) throw new RangeError(`Group key ${String(c)} missing`);
      column.assign(key, length, rows, target(c));
    });
    if (this.#tuple === null) return;
    this.#tuple.assign(
      this.#ids,
      rows === null ? IDENTITY : rows.rows,
      rows === null ? length : rows.count,
      gids,
    );
  }

  /** Key column `c` of groups [from, from + count). */
  emitKey(c: number, from: number, count: number): FlatVector {
    const column = this.#columns[c];
    const type = this.keyTypes[c];
    if (column === undefined || type === undefined)
      throw new RangeError(`No key column ${String(c)}`);
    if (this.#tuple === null) return column.emit(type, from, count, null);
    const ids = this.#tuple.columnIds[c];
    if (ids === undefined) throw new RangeError(`No key column ${String(c)}`);
    return column.emit(type, 0, count, ids.subarray(from, from + count));
  }

  close(): void {
    for (const column of this.#columns) column.close();
    this.#memory.close();
  }
}
