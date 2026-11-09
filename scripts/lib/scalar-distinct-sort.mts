/** Benchmark-only scalar byte distinct via owned encoding plus explicit index sorting. */
import { xxHash32View } from "../../packages/core/dist/next/hash/xxhash32.js";
import { Charged, grownCapacity } from "../../packages/core/dist/next/exec/agg/groups.js";
import { OBJECT_OVERHEAD_BYTES } from "../../packages/core/dist/next/memory/governor.js";
import type { ByteCharge } from "../../packages/core/dist/next/vector/varlen.js";
import type { Vector } from "../../packages/core/dist/next/vector/vector.js";

/** Catalogue component inputs are flat/all-valid; other forms are not yet an integrated path. */
export class ScalarSortDistinct {
  readonly #memory: Charged;
  #bytes: Uint8Array;
  #view: DataView;
  #records: Uint32Array;
  #capacity = 1024;
  #used = 0;
  #count = 0;
  constructor(
    charge: ByteCharge,
    readonly hashOrder: boolean,
  ) {
    const memory = (this.#memory = new Charged(charge));
    try {
      memory.take(7 * OBJECT_OVERHEAD_BYTES);
      this.#bytes = memory.make(Uint8Array, 4096);
      this.#view = new DataView(this.#bytes.buffer);
      this.#records = memory.make(Uint32Array, 3 * 1024);
    } catch (error) {
      memory.close();
      throw error;
    }
  }
  add(vector: Vector): void {
    if (vector.form !== "flat" || vector.validity !== null || vector.data.layout !== "varlen")
      throw new TypeError("sort experiment requires flat all-valid byte input");
    const { bytes, offsets } = vector.data,
      first = offsets[0] ?? 0,
      last = offsets[vector.length] ?? 0;
    const n = this.#count + vector.length,
      needed = this.#used + last - first,
      memory = this.#memory;
    if (n > this.#capacity) {
      const size = grownCapacity(this.#capacity, n),
        old = this.#records;
      memory.take(2 * OBJECT_OVERHEAD_BYTES);
      let next: Uint32Array | undefined;
      try {
        next = memory.make(Uint32Array, 3 * size);
        next.set(old);
        this.#records = next;
        this.#capacity = size;
        memory.give(old.byteLength);
      } catch (error) {
        if (next !== undefined) memory.give(next.byteLength);
        throw error;
      } finally {
        memory.give(2 * OBJECT_OVERHEAD_BYTES);
      }
    }
    if (needed > this.#bytes.length) {
      const old = this.#bytes;
      memory.take(3 * OBJECT_OVERHEAD_BYTES);
      let next: Uint8Array | undefined;
      try {
        next = memory.make(Uint8Array, grownCapacity(old.length, needed));
        next.set(old);
        const view = new DataView(next.buffer);
        this.#bytes = next;
        this.#view = view;
        memory.give(old.byteLength);
      } catch (error) {
        if (next !== undefined) memory.give(next.byteLength);
        throw error;
      } finally {
        memory.give(3 * OBJECT_OVERHEAD_BYTES);
      }
    }
    const base = this.#used - first,
      records = this.#records;
    memory.take(OBJECT_OVERHEAD_BYTES);
    try {
      const source = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      if (first === 0 && last === bytes.length) this.#bytes.set(bytes, this.#used);
      else {
        memory.take(OBJECT_OVERHEAD_BYTES);
        try {
          this.#bytes.set(
            new Uint8Array(bytes.buffer, bytes.byteOffset + first, last - first),
            this.#used,
          );
        } finally {
          memory.give(OBJECT_OVERHEAD_BYTES);
        }
      }
      for (let row = 0; row < vector.length; row++) {
        const from = offsets[row] ?? 0,
          to = offsets[row + 1] ?? 0,
          id = this.#count++;
        records[3 * id] = this.hashOrder ? xxHash32View(source, 0, from, to) : 0;
        records[3 * id + 1] = to - from;
        records[3 * id + 2] = base + from;
      }
      this.#used = needed;
    } finally {
      memory.give(OBJECT_OVERHEAD_BYTES);
    }
  }
  #compare(a: number, b: number): number {
    const records = this.#records,
      view = this.#view,
      al = records[3 * a + 1] ?? 0,
      bl = records[3 * b + 1] ?? 0;
    if (al !== bl) return al - bl;
    const af = records[3 * a + 2] ?? 0,
      bf = records[3 * b + 2] ?? 0;
    let k = 0;
    for (; k + 4 <= al; k += 4) {
      const x = view.getUint32(af + k, true),
        y = view.getUint32(bf + k, true);
      if (x !== y) return x - y;
    }
    for (; k < al; k++) {
      const x = view.getUint8(af + k),
        y = view.getUint8(bf + k);
      if (x !== y) return x - y;
    }
    return 0;
  }
  finish(): number {
    const n = this.#count,
      memory = this.#memory;
    memory.take(6 * OBJECT_OVERHEAD_BYTES);
    let ownedA: Uint32Array | undefined,
      ownedB: Uint32Array | undefined,
      ownedHistogram: Uint32Array | undefined;
    try {
      ownedA = memory.make(Uint32Array, n);
      ownedB = memory.make(Uint32Array, n);
      ownedHistogram = memory.make(Uint32Array, 2049);
      let ids: Uint32Array = ownedA,
        scratch: Uint32Array = ownedB;
      const histogram = ownedHistogram;
      for (let i = 0; i < n; i++) ids[i] = i;
      if (this.hashOrder) {
        const records = this.#records;
        for (let shift = 0; shift <= 22; shift += 11) {
          histogram.fill(0);
          for (let i = 0; i < n; i++) {
            const bucket = ((records[3 * (ids[i] ?? 0)] ?? 0) >>> shift) & 2047;
            histogram[bucket + 1] = (histogram[bucket + 1] ?? 0) + 1;
          }
          for (let i = 1; i <= 2048; i++)
            histogram[i] = (histogram[i] ?? 0) + (histogram[i - 1] ?? 0);
          for (let i = 0; i < n; i++) {
            const id = ids[i] ?? 0,
              bucket = ((records[3 * id] ?? 0) >>> shift) & 2047,
              at = histogram[bucket] ?? 0;
            scratch[at] = id;
            histogram[bucket] = at + 1;
          }
          const swap: Uint32Array = ids;
          ids = scratch;
          scratch = swap;
        }
        let unique = 0;
        for (let start = 0; start < n;) {
          const hash = records[3 * (ids[start] ?? 0)] ?? 0;
          let end = start + 1;
          while (end < n && records[3 * (ids[end] ?? 0)] === hash) end++;
          // Same hashes can contain distinct byte strings, with repeated values interleaved.
          // Compact each bucket's unique ids into its already-read prefix.
          let accepted = 0;
          for (let i = start; i < end; i++) {
            const id = ids[i] ?? 0;
            let duplicate = false;
            for (let k = 0; k < accepted; k++)
              if (this.#compare(id, ids[start + k] ?? 0) === 0) {
                duplicate = true;
                break;
              }
            if (!duplicate) ids[start + accepted++] = id;
          }
          unique += accepted;
          start = end;
        }
        return unique;
      }
      // Length, little-endian words, then tail bytes form a total order. COUNT needs equality,
      // not SQL output ordering. All sort storage is explicit and charged.
      for (let width = 1; width < n; width *= 2) {
        for (let start = 0; start < n; start += 2 * width) {
          const middle = Math.min(start + width, n),
            end = Math.min(start + 2 * width, n);
          let a = start,
            b = middle,
            out = start;
          while (a < middle && b < end) {
            if (this.#compare(ids[a] ?? 0, ids[b] ?? 0) <= 0) scratch[out++] = ids[a++] ?? 0;
            else scratch[out++] = ids[b++] ?? 0;
          }
          while (a < middle) scratch[out++] = ids[a++] ?? 0;
          while (b < end) scratch[out++] = ids[b++] ?? 0;
        }
        const swap: Uint32Array = ids;
        ids = scratch;
        scratch = swap;
      }
      let unique = 0;
      for (let i = 0; i < n; i++)
        if (i === 0 || this.#compare(ids[i - 1] ?? 0, ids[i] ?? 0) !== 0) unique++;
      return unique;
    } finally {
      if (ownedA !== undefined) memory.give(ownedA.byteLength);
      if (ownedB !== undefined) memory.give(ownedB.byteLength);
      if (ownedHistogram !== undefined) memory.give(ownedHistogram.byteLength);
      memory.give(6 * OBJECT_OVERHEAD_BYTES);
    }
  }
  close(): void {
    this.#memory.close();
  }
}
