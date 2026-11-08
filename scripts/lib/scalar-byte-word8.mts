/** Benchmark-only eight-byte transport experiment; never canonicalize NaN payloads or zero signs. */
/** Count-only byte membership: no value ids, key outputs or second seen-marker array. */
import { xxHash32View } from "../../packages/core/dist/next/hash/xxhash32.js";
import { OBJECT_OVERHEAD_BYTES } from "../../packages/core/dist/next/memory/governor.js";
import type { PhysicalType } from "../../packages/core/dist/next/types.js";
import type { ByteCharge } from "../../packages/core/dist/next/vector/varlen.js";
import type { Selection, Vector } from "../../packages/core/dist/next/vector/vector.js";
import { IDENTITY } from "../../packages/core/dist/next/exec/kernels/vectors.js";
import { Charged, grownCapacity } from "../../packages/core/dist/next/exec/agg/groups.js";

const EMPTY_BYTES = new Uint8Array(0);
const EMPTY_SLOTS = new Uint32Array(0);
const EMPTY_VIEW = new DataView(EMPTY_BYTES.buffer);

export function supportsScalarByteDistinct(type: PhysicalType): boolean {
  return (
    type.kind === "text" || type.kind === "bytes" || type.kind === "uuid" || type.kind === "enum"
  );
}

export class ScalarByteWord8 {
  readonly #memory: Charged;
  /** Record offset + 1; a compact byte tag rejects most probes without reading this array. */
  #slots: Uint32Array;
  #tags: Uint8Array;
  #mask = 1023;
  /** Packed records: uint32 hash, uint32 byte length, then exactly those bytes. */
  #arena: Uint8Array;
  #view: DataView;
  #used = 0;
  #count = 0;
  #dictionary = -1;
  #translation: Uint8Array;
  #closed = false;
  constructor(type: PhysicalType, charge: ByteCharge) {
    if (!supportsScalarByteDistinct(type))
      throw new TypeError("A scalar byte distinct argument is required");
    const memory = (this.#memory = new Charged(charge));
    try {
      // Class, tracker, arena DataView, and four typed-array view/buffer pairs.
      memory.take(11 * OBJECT_OVERHEAD_BYTES);
      this.#slots = memory.make(Uint32Array, 1024);
      this.#tags = memory.make(Uint8Array, 1024);
      this.#arena = memory.make(Uint8Array, 4096);
      this.#translation = memory.make(Uint8Array, 0);
      this.#view = new DataView(this.#arena.buffer);
    } catch (error) {
      memory.close();
      throw error;
    }
  }
  get count(): number {
    return this.#count;
  }

  #grow(): void {
    const old = this.#slots,
      oldTags = this.#tags,
      memory = this.#memory;
    memory.take(4 * OBJECT_OVERHEAD_BYTES);
    let next: Uint32Array | undefined, tags: Uint8Array | undefined;
    try {
      next = memory.make(Uint32Array, old.length * 2);
      tags = memory.make(Uint8Array, next.length);
      const mask = next.length - 1,
        view = this.#view;
      // Read stored hashes in append order, instead of scanning a sparse old table or
      // chasing its record pointers. Growth never hashes input bytes again.
      for (let record = 0; record < this.#used; record += 8 + view.getUint32(record + 4, true)) {
        const hash = view.getUint32(record, true),
          tag = hash >>> 24 || 1;
        let at = hash & mask;
        while (tags[at] !== 0) at = (at + 1) & mask;
        next[at] = record + 1;
        tags[at] = tag;
      }
      this.#slots = next;
      this.#tags = tags;
      this.#mask = mask;
      memory.give(old.byteLength + oldTags.byteLength + 4 * OBJECT_OVERHEAD_BYTES);
    } catch (error) {
      if (next !== undefined) memory.give(next.byteLength);
      if (tags !== undefined) memory.give(tags.byteLength);
      memory.give(4 * OBJECT_OVERHEAD_BYTES);
      throw error;
    }
  }
  #reserve(needed: number): void {
    if (needed <= this.#arena.length) return;
    if (needed > 0xffff_fffe)
      throw new RangeError("Scalar distinct byte arena exceeds its offset range");
    const old = this.#arena,
      memory = this.#memory;
    memory.take(3 * OBJECT_OVERHEAD_BYTES);
    let next: Uint8Array | undefined;
    try {
      next = memory.make(Uint8Array, Math.min(0xffff_fffe, grownCapacity(old.length, needed)));
      next.set(old);
      const view = new DataView(next.buffer);
      this.#arena = next;
      this.#view = view;
      memory.give(old.byteLength + 3 * OBJECT_OVERHEAD_BYTES);
    } catch (error) {
      if (next !== undefined) memory.give(next.byteLength);
      memory.give(3 * OBJECT_OVERHEAD_BYTES);
      throw error;
    }
  }
  #equal(record: number, source: DataView, from: number, length: number): boolean {
    const own = this.#view;
    if (own.getUint32(record + 4, true) !== length) return false;
    let b = 0;
    for (; b + 8 <= length; b += 8) {
      const x = own.getFloat64(record + 8 + b, true),
        y = source.getFloat64(from + b, true);
      if (x !== x && y !== y) {
        if (
          own.getUint32(record + 8 + b, true) !== source.getUint32(from + b, true) ||
          own.getUint32(record + 12 + b, true) !== source.getUint32(from + 4 + b, true)
        )
          return false;
      } else if (!Object.is(x, y)) return false;
    }

    for (; b + 4 <= length; b += 4)
      if (own.getUint32(record + 8 + b, true) !== source.getUint32(from + b, true)) return false;
    for (; b < length; b++)
      if (own.getUint8(record + 8 + b) !== source.getUint8(from + b)) return false;
    return true;
  }
  #insert(source: DataView, from: number, to: number): boolean {
    const hash = xxHash32View(source, 0, from, to),
      length = to - from,
      tag = hash >>> 24 || 1;
    let slots = this.#slots,
      tags = this.#tags,
      mask = this.#mask,
      at = hash & mask;
    for (;;) {
      const before = tags[at] ?? 0;
      if (before === 0) break;
      if (before === tag) {
        const record = (slots[at] ?? 0) - 1;
        if (
          this.#view.getUint32(record, true) === hash &&
          this.#equal(record, source, from, length)
        )
          return false;
      }
      at = (at + 1) & mask;
    }
    if (2 * (this.#count + 1) > mask + 1) {
      this.#grow();
      slots = this.#slots;
      tags = this.#tags;
      mask = this.#mask;
      at = hash & mask;
      while (tags[at] !== 0) at = (at + 1) & mask;
    }
    const record = this.#used,
      end = record + 8 + length;
    this.#reserve(end);
    const own = this.#view;
    own.setUint32(record, hash, true);
    own.setUint32(record + 4, length, true);
    if (length > 64) {
      this.#memory.take(OBJECT_OVERHEAD_BYTES);
      try {
        this.#arena.set(
          new Uint8Array(source.buffer, source.byteOffset + from, length),
          record + 8,
        );
      } finally {
        this.#memory.give(OBJECT_OVERHEAD_BYTES);
      }
    } else {
      let b = 0;
      for (; b + 8 <= length; b += 8) {
        const value = source.getFloat64(from + b, true);
        if (value === value) own.setFloat64(record + 8 + b, value, true);
        else {
          own.setUint32(record + 8 + b, source.getUint32(from + b, true), true);
          own.setUint32(record + 12 + b, source.getUint32(from + 4 + b, true), true);
        }
      }

      for (; b + 4 <= length; b += 4)
        own.setUint32(record + 8 + b, source.getUint32(from + b, true), true);
      for (; b < length; b++) own.setUint8(record + 8 + b, source.getUint8(from + b));
    }
    this.#used = end;
    slots[at] = record + 1;
    tags[at] = tag;
    this.#count++;
    return true;
  }

  /** Same first-new-row selection contract as DistinctSet, restricted to scalar byte COUNT. */
  filter(
    vector: Vector,
    _gids: Int32Array,
    length: number,
    rows: Selection | null,
    scalar: boolean,
    out: Selection,
  ): Selection {
    if (this.#closed) throw new TypeError("Scalar distinct set is closed");
    if (!scalar) throw new TypeError("Scalar distinct cannot serve grouped aggregates");
    out.count = 0;
    const count = rows === null ? length : rows.count;
    if (count === 0) return out;
    const active = rows === null ? IDENTITY : rows.rows,
      constant = vector.form === "constant";
    const flat =
      vector.form === "dictionary" ? vector.dictionary : constant ? vector.value : vector;
    const data = flat.data;
    if (data.layout !== "varlen" && data.layout !== "fixed16")
      throw new TypeError("Byte distinct vector required");
    const validity = constant ? vector.value.validity : vector.validity;
    if (constant && validity !== null && ((validity[0] ?? 0) & 1) === 0) return out;
    if (vector.form === "dictionary") {
      if (this.#translation.length < vector.dictionary.length) {
        const old = this.#translation;
        this.#memory.take(2 * OBJECT_OVERHEAD_BYTES);
        let next: Uint8Array | undefined;
        try {
          next = this.#memory.make(Uint8Array, Math.max(vector.dictionary.length, old.length * 2));
          next.set(old);
          this.#translation = next;
          this.#memory.give(old.byteLength);
        } catch (error) {
          if (next !== undefined) this.#memory.give(next.byteLength);
          throw error;
        } finally {
          this.#memory.give(2 * OBJECT_OVERHEAD_BYTES);
        }
      }
      if (this.#dictionary !== vector.dictionaryId) {
        this.#translation.fill(0);
        this.#dictionary = vector.dictionaryId;
      }
    }
    this.#memory.take(OBJECT_OVERHEAD_BYTES);
    try {
      const source = new DataView(data.bytes.buffer, data.bytes.byteOffset, data.bytes.byteLength);
      if (constant) {
        const from = data.layout === "varlen" ? (data.offsets[0] ?? 0) : 0;
        const end = data.layout === "varlen" ? (data.offsets[1] ?? 0) : 16;
        if (this.#insert(source, from, end)) {
          out.rows[0] = active[0] ?? 0;
          out.count = 1;
        }
        return out;
      }
      for (let k = 0; k < count; k++) {
        const row = active[k] ?? 0;
        if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
        const slot = vector.form === "dictionary" ? (vector.codes[row] ?? 0) : row;
        if (vector.form === "dictionary" && this.#translation[slot] === 1) continue;
        const from = data.layout === "varlen" ? (data.offsets[slot] ?? 0) : slot * 16;
        const end = data.layout === "varlen" ? (data.offsets[slot + 1] ?? 0) : from + 16;
        if (this.#insert(source, from, end)) out.rows[out.count++] = row;
        if (vector.form === "dictionary") this.#translation[slot] = 1;
      }
      return out;
    } finally {
      this.#memory.give(OBJECT_OVERHEAD_BYTES);
    }
  }
  close(): void {
    this.#closed = true;
    this.#slots = EMPTY_SLOTS;
    this.#tags = EMPTY_BYTES;
    this.#arena = EMPTY_BYTES;
    this.#view = EMPTY_VIEW;
    this.#translation = EMPTY_BYTES;
    this.#memory.close();
  }
}
