/**
 * Exact DISTINCT for aggregates (COUNT(DISTINCT x), SUM(DISTINCT x)…): keeps, per batch, only
 * the rows whose (group, value) pair has not been seen.
 *
 * Values are first mapped to dense value ids by a `GroupTable` over the argument (so SQL equality
 * holds: NUMERIC by value, −0 = 0, one NaN; dictionary columns translated once per entry), then
 * (group id, value id) pairs live in an open-addressing set of two Int32Arrays. NULL values are
 * dropped (every aggregate ignores them). Memory is charged to the grant; spilling the set by
 * partition is milestone 3.
 */
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Selection, Vector } from "../../vector/vector.js";
import { IDENTITY } from "../kernels/vectors.js";
import { Charged, GroupTable } from "./groups.js";

function mix(a: number, b: number): number {
  let h = Math.imul(a ^ Math.imul(b, 0x9e3779b1), 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  return h ^ (h >>> 16);
}

export class DistinctSet {
  readonly #values: GroupTable;
  readonly #memory: Charged;
  readonly #vids: Int32Array;
  #slots: Int32Array = new Int32Array(1024);
  #mask = 1023;
  #groups: Int32Array = new Int32Array(1024);
  #ids: Int32Array = new Int32Array(1024);
  #size = 0;
  /** Scalar DISTINCT: value ids already counted. */
  #seen: Uint8Array = new Uint8Array(0);
  readonly #one: Vector[] = [];

  constructor(type: PhysicalType, charge: ByteCharge, capacity: number) {
    this.#values = new GroupTable([type], charge);
    this.#memory = new Charged(charge);
    this.#memory.take(4 * (capacity + 3 * 1024));
    this.#vids = new Int32Array(capacity);
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
    if (scalar) {
      // One group: a value is new iff its id was never seen (no pair set needed).
      if (this.#seen.length < this.#values.groupCount)
        this.#seen = this.#memory.uint8(
          this.#seen,
          Math.max(this.#values.groupCount, 2 * this.#seen.length),
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
    const slots = new Int32Array(size);
    this.#memory.take(slots.byteLength);
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
    this.#values.close();
    this.#memory.close();
  }
}
