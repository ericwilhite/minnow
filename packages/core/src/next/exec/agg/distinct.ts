/**
 * Exact DISTINCT for aggregates (COUNT(DISTINCT x), SUM(DISTINCT x)…): keeps, per batch, only
 * the rows whose (group, value) pair has not been seen.
 *
 * Values are first mapped to dense value ids by a `GroupTable` over the argument (so SQL equality
 * holds: NUMERIC by value, −0 = 0, one NaN; dictionary columns translated once per entry), then
 * (group id, value id) pairs live in an open-addressing set of two Int32Arrays. NULL values are
 * dropped (every aggregate ignores them). Grouped COUNT can opt into per-group membership
 * bitmaps over those same global value ids: no second pair hash, no copied per-group values.
 * Large-group-first inputs retain pairs; later group growth uses disjoint bitmap/pair routes.
 * Scalar byte COUNT uses a count-only membership table in AggregateOperator; other scalar
 * arguments retain this generic value-id/seen-marker path.
 * This synchronous set requires a complete admitted workspace. The query factory
 * uses the bounded external DISTINCT recipe when no whole-set bound is certified;
 * it never retries filter() after a partially applied allocation failure.
 */
import { OBJECT_OVERHEAD_BYTES } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Selection, Vector } from "../../vector/vector.js";
import { IDENTITY } from "../kernels/vectors.js";
import { Charged, GroupTable, grownCapacity } from "./groups.js";
import { arrayGrowth } from "./growth.js";

function mix(a: number, b: number): number {
  let h = Math.imul(a ^ Math.imul(b, 0x9e3779b1), 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  return h ^ (h >>> 16);
}

const BITMAP_GROUPS = 64;

export class DistinctSet {
  growthBytes(vector: Vector, rows: number): number {
    const values = this.#values.groupCount + rows,
      pairs = this.#size + rows;
    let bytes =
      4096 +
      this.#values.growthBytes([vector], rows) +
      arrayGrowth(this.#slots.length, 2 * pairs, 4, true) +
      arrayGrowth(this.#groups.length, pairs, 8, true) +
      arrayGrowth(this.#seen.length, values, 1);
    if (this.#bitmaps !== null) {
      const words = Math.ceil(values / 32);
      for (let group = 0; group < BITMAP_GROUPS; group++)
        bytes +=
          arrayGrowth(this.#bitmaps[group]?.length ?? 0, words, 4, true) +
          2 * OBJECT_OVERHEAD_BYTES;
    }
    return bytes;
  }

  readonly #values: GroupTable;
  readonly #memory: Charged;
  readonly #vids: Int32Array;
  #slots: Int32Array;
  #mask = 1023;
  #groups: Int32Array;
  #ids: Int32Array;
  #size = 0;
  /** Scalar DISTINCT: value ids already counted. */
  #seen: Uint8Array;
  readonly #one: Vector[] = [];
  #bitmaps: Array<Uint32Array | undefined> | null = null;
  #bitmapMode: boolean | null = false;

  /** Throws (having released everything it took) when the grant refuses its first buffers. */
  constructor(type: PhysicalType, charge: ByteCharge, capacity: number, bitmap = false) {
    this.#values = new GroupTable([type], charge);
    const memory = (this.#memory = new Charged(charge));
    try {
      if (bitmap) {
        memory.take(BITMAP_GROUPS * 8 + OBJECT_OVERHEAD_BYTES);
        this.#bitmaps = new Array<Uint32Array | undefined>(BITMAP_GROUPS);
        this.#bitmapMode = null;
      }
      this.#vids = memory.make(Int32Array, capacity);
      this.#slots = memory.make(Int32Array, 1024);
      this.#groups = memory.make(Int32Array, 1024);
      this.#ids = memory.make(Int32Array, 1024);
      this.#seen = memory.make(Uint8Array, 0);
    } catch (error) {
      this.close();
      throw error;
    }
  }

  /** The rows of `rows` (or every row) with a non-null value whose pair is new, into `out`. */
  filter(
    vector: Vector,
    gids: Int32Array,
    length: number,
    rows: Selection | null,
    scalar: boolean,
    out: Selection,
  ): Selection {
    const active = rows === null ? IDENTITY : rows.rows;
    const count = rows === null ? length : rows.count;
    out.count = 0;
    const constant = vector.form === "constant";
    const validity = constant ? vector.value.validity : vector.validity;
    if (constant && validity !== null && ((validity[0] ?? 0) & 1) === 0) return out;
    this.#one[0] = vector;
    const vids = this.#vids;
    this.#values.assign(this.#one, length, rows, vids);
    if (this.#bitmapMode === null && count > 0) {
      let maxGroup = 0;
      if (!scalar)
        for (let k = 0; k < count; k++) maxGroup = Math.max(maxGroup, gids[active[k] ?? 0] ?? 0);
      // Do not add bitmap state to queries that already start with many groups.
      this.#bitmapMode = scalar || maxGroup < BITMAP_GROUPS;
    }
    if (this.#bitmapMode === true) {
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        if (!constant && validity !== null && ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0)
          continue;
        const group = scalar ? 0 : (gids[i] ?? 0),
          id = vids[i] ?? 0;
        // A later increase in group cardinality needs no migration or duplicate membership:
        // groups below the cutoff always use bits, the others always use the original set.
        if (group < BITMAP_GROUPS ? this.#mark(group, id) : this.#insert(group, id))
          out.rows[out.count++] = i;
      }
      return out;
    }
    if (scalar) {
      // One group: a value is new iff its id was never seen (no pair set needed).
      if (this.#seen.length < this.#values.groupCount)
        this.#seen = this.#memory.uint8(
          this.#seen,
          grownCapacity(this.#seen.length, this.#values.groupCount),
        );
      const seen = this.#seen;
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        if (!constant && validity !== null && ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0)
          continue;
        const id = vids[i] ?? 0;
        if (seen[id] === 1) continue;
        seen[id] = 1;
        out.rows[out.count++] = i;
      }
      return out;
    }
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      if (!constant && validity !== null && ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0)
        continue;
      if (this.#insert(gids[i] ?? 0, vids[i] ?? 0)) out.rows[out.count++] = i;
    }
    return out;
  }

  /** Snapshot logical membership, not hash-slot identities. Undefined is a bounded
   * scan checkpoint for sparse bitmap words / long pair arrays. */
  *members(scalar: boolean): Generator<{ group: number; id: number } | undefined, void, void> {
    if (scalar) {
      for (let id = 0; id < this.#values.groupCount; id++) {
        if (this.#seen[id] === 1) yield { group: 0, id };
        if ((id & 511) === 511) yield;
      }
      return;
    }
    if (this.#bitmapMode === true && this.#bitmaps !== null) {
      for (let group = 0; group < BITMAP_GROUPS; group++) {
        const bits = this.#bitmaps[group];
        if (!bits) continue;
        for (let word = 0; word < bits.length; word++) {
          const mask = bits[word] ?? 0;
          if (mask !== 0)
            for (let b = 0; b < 32; b++)
              if ((mask & (1 << b)) !== 0) yield { group, id: 32 * word + b };
          if ((word & 511) === 511) yield;
        }
      }
    }
    for (let i = 0; i < this.#size; i++) {
      yield { group: this.#groups[i] ?? 0, id: this.#ids[i] ?? 0 };
      if ((i & 511) === 511) yield;
    }
  }
  /** Restore the original strategy and global value-ID order before memberships. Replaying
   * memberships group-by-group must not select a different bitmap layout or reorder arenas. */
  get checkpointMode(): number {
    return this.#bitmapMode === null ? -1 : this.#bitmapMode ? 1 : 0;
  }
  restoreMode(mode: number): void {
    if (mode !== -1 && mode !== 0 && mode !== 1) throw new Error("Invalid DISTINCT mode");
    if (this.#bitmaps === null && mode !== 0) throw new Error("Missing DISTINCT bitmap slots");
    this.#bitmapMode = mode < 0 ? null : mode === 1;
  }
  get valueCount(): number {
    return this.#values.groupCount;
  }
  restoreValue(vector: Vector, row: number): void {
    this.#one[0] = vector;
    this.#values.assign(
      this.#one,
      vector.length,
      { rows: IDENTITY.subarray(row, row + 1), count: 1 },
      this.#vids,
    );
  }

  memberBytes(id: number): number {
    return this.#values.keyBytes(0, id, 1);
  }
  memberValue(id: number): ReturnType<GroupTable["emitKey"]> {
    return this.#values.emitKey(0, id, 1);
  }

  /** One bit per observed (group,value-id), with no second tuple hash or pair arrays. */
  #mark(group: number, id: number): boolean {
    const maps = this.#bitmaps;
    if (maps === null) throw new Error("Missing distinct bitmaps");
    const word = id >>> 5;
    let bits = maps[group];
    if (bits === undefined) {
      let size = 1;
      while (size <= word) size *= 2;
      this.#memory.take(2 * OBJECT_OVERHEAD_BYTES);
      try {
        bits = this.#memory.make(Uint32Array, size);
      } catch (error) {
        this.#memory.give(2 * OBJECT_OVERHEAD_BYTES);
        throw error;
      }
      maps[group] = bits;
    } else if (word >= bits.length) {
      let size = bits.length * 2;
      while (size <= word) size *= 2;
      // Count old/new view and buffer headers during resize, as well as their payloads.
      this.#memory.take(2 * OBJECT_OVERHEAD_BYTES);
      try {
        bits = this.#memory.uint32(bits, size);
        maps[group] = bits;
      } finally {
        this.#memory.give(2 * OBJECT_OVERHEAD_BYTES);
      }
    }
    const mask = 1 << (id & 31),
      before = bits[word] ?? 0;
    if (before & mask) return false;
    bits[word] = before | mask;
    return true;
  }

  #insert(group: number, id: number): boolean {
    const h = mix(group, id);
    let s = h & this.#mask;
    for (;;) {
      const entry = this.#slots[s] ?? 0;
      if (entry === 0) break;
      const e = entry - 1;
      if (this.#groups[e] === group && this.#ids[e] === id) return false;
      s = (s + 1) & this.#mask;
    }
    const e = this.#size++;
    if (e >= this.#groups.length) {
      this.#groups = this.#memory.int32(this.#groups, 2 * this.#groups.length);
      this.#ids = this.#memory.int32(this.#ids, 2 * this.#ids.length);
    }
    this.#groups[e] = group;
    this.#ids[e] = id;
    if (2 * this.#size > this.#slots.length) this.#rehash(2 * this.#slots.length);
    else this.#slots[s] = e + 1;
    return true;
  }

  #rehash(size: number): void {
    const slots = this.#memory.make(Int32Array, size);
    this.#memory.give(this.#slots.byteLength);
    this.#slots = slots;
    this.#mask = size - 1;
    for (let e = 0; e < this.#size; e++) {
      let s = mix(this.#groups[e] ?? 0, this.#ids[e] ?? 0) & this.#mask;
      while (slots[s] !== 0) s = (s + 1) & this.#mask;
      slots[s] = e + 1;
    }
  }

  close(): void {
    this.#one.length = 0;
    this.#bitmaps?.fill(undefined);
    this.#bitmaps = null;
    this.#values.close();
    this.#memory.close();
  }
}
