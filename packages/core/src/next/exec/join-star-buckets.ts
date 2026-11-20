/** Charged row/payload buckets for the narrow resident three-dimension star.
 * The native two intermediate-build LIFO chains visit B ascending, A descending,
 * fact ascending. Tail insertion retains that fact order without payload gathers
 * at either intermediate join. Keys are compact query/dimension ordinals, never
 * local dictionary codes or a sparse stored-value range.
 */
import type { ByteCharge } from "../vector/varlen.js";
import { Charged } from "./agg/groups.js";
import { BATCH_ROWS } from "../vector/vector.js";
const EMPTY_I32 = new Int32Array(0),
  EMPTY_U32 = new Uint32Array(0),
  EMPTY_F64 = new Float64Array(0);
export const STAR_BUCKET_LIMIT = 1 << 20;
export class StarBuckets {
  static bytes(rows: number, a: number, b: number): number | null {
    const buckets = a * b;
    if (
      !Number.isSafeInteger(rows) ||
      rows < 0 ||
      rows > 1 << 21 ||
      !Number.isSafeInteger(a) ||
      a < 0 ||
      !Number.isSafeInteger(b) ||
      b < 0 ||
      !Number.isSafeInteger(buckets) ||
      buckets > STAR_BUCKET_LIMIT ||
      buckets > Math.max(1024, rows * 4)
    )
      return null;
    return 256 + 16 * rows + 8 * buckets;
  }
  readonly #memory: Charged;
  #groups = EMPTY_I32;
  #values = EMPTY_F64;
  #links = EMPTY_U32;
  #heads = EMPTY_U32;
  #tails = EMPTY_U32;
  #rows = 0;
  #a = 0;
  #b = 0;
  #at = 0;
  #usedA = 0;
  #usedB = 0;
  #sealed = false;
  #closed = false;
  #lastLinked = -1;
  constructor(
    readonly capacity: number,
    readonly aCapacity: number,
    readonly bCapacity: number,
    charge: ByteCharge,
  ) {
    if (StarBuckets.bytes(capacity, aCapacity, bCapacity) === null)
      throw new RangeError("Unaffordable star bucket domain");
    this.#memory = new Charged(charge);
    try {
      this.#memory.take(256);
      this.#groups = this.#memory.make(Int32Array, capacity);
      this.#values = this.#memory.make(Float64Array, capacity);
      this.#links = this.#memory.make(Uint32Array, capacity);
      this.#heads = this.#memory.make(Uint32Array, aCapacity * bCapacity);
      this.#tails = this.#memory.make(Uint32Array, aCapacity * bCapacity);
    } catch (error) {
      this.close();
      throw error;
    }
  }
  get rows(): number {
    return this.#rows;
  }
  get groupCodes(): Int32Array {
    return this.#groups;
  }
  get values(): Float64Array {
    return this.#values;
  }
  isNull(row: number): boolean {
    return ((this.#links[row] ?? 0) & 1) !== 0;
  }
  append(a: number, b: number, cGroup: number, value: number | null): void {
    if (this.#closed || this.#sealed) throw new Error("Star buckets are not accepting input");
    if (
      !Number.isInteger(a) ||
      a < 0 ||
      a >= this.aCapacity ||
      !Number.isInteger(b) ||
      b < 0 ||
      b >= this.bCapacity ||
      !Number.isInteger(cGroup) ||
      cGroup < -1 ||
      cGroup > 0x7fffffff
    )
      throw new RangeError("Star bucket index outside its exact bound");
    if (this.#rows >= this.capacity)
      throw new RangeError("Star fact count exceeds its exact bound");
    const row = this.stage(cGroup, value);
    this.link(row, a, b);
  }
  /** Retain the C-survivor payload before later A/B sources are demanded. */
  stage(cGroup: number, value: number | null): number {
    if (this.#closed || this.#sealed) throw new Error("Star buckets are not accepting input");
    if (!Number.isInteger(cGroup) || cGroup < -1 || cGroup > 0x7fffffff)
      throw new RangeError("Star group outside its exact bound");
    if (this.#rows >= this.capacity)
      throw new RangeError("Star fact count exceeds its exact bound");
    const row = this.#rows++;
    this.#groups[row] = cGroup;
    this.#values[row] = value ?? 0;
    this.#links[row] = value === null ? 1 : 0;
    return row;
  }
  /** Link staged rows once, in original fact order; skipped A keys stay unlinked. */
  link(row: number, a: number, b: number): void {
    if (this.#closed || this.#sealed) throw new Error("Star buckets are not accepting links");
    if (
      !Number.isInteger(row) ||
      row <= this.#lastLinked ||
      row >= this.#rows ||
      !Number.isInteger(a) ||
      a < 0 ||
      a >= this.aCapacity ||
      !Number.isInteger(b) ||
      b < 0 ||
      b >= this.bCapacity
    )
      throw new RangeError("Star link outside its exact bound or fact order");
    this.#lastLinked = row;
    const bucket = b * this.aCapacity + a,
      tail = (this.#tails[bucket] ?? 0) - 1;
    if (tail < 0) this.#heads[bucket] = row + 1;
    else this.#links[tail] = ((row + 1) << 1) | ((this.#links[tail] ?? 0) & 1);
    this.#tails[bucket] = row + 1;
  }
  /** One B source row at a time, so its prefix is consumed before another read. */
  beginB(a: number, b: number): void {
    if (this.#closed || (this.#sealed && this.#b < this.#usedB))
      throw new Error("Star bucket prefix is still borrowed");
    if (
      !Number.isInteger(a) ||
      a < 0 ||
      a > this.aCapacity ||
      !Number.isInteger(b) ||
      b < 0 ||
      b >= this.bCapacity
    )
      throw new RangeError("Star B prefix outside its exact bound");
    this.#sealed = true;
    this.#usedA = a;
    this.#usedB = b + 1;
    this.#a = a;
    this.#b = b;
    this.#at = 0;
  }

  begin(a: number, b: number): void {
    if (this.#closed || this.#sealed) throw new Error("Star buckets cannot begin replay");
    if (
      !Number.isInteger(a) ||
      a < 0 ||
      a > this.aCapacity ||
      !Number.isInteger(b) ||
      b < 0 ||
      b > this.bCapacity
    )
      throw new RangeError("Star dimension count exceeds its exact bound");
    this.#sealed = true;
    this.#usedA = a;
    this.#usedB = b;
    this.#a = a;
    this.#b = 0;
    this.#at = 0;
  }
  /** Work includes empty buckets, so an empty/filtered star also has bounded steps.
   * Output arrays belong to the caller; no new allocation occurs during replay.
   */
  read(
    records: Int32Array,
    aOrdinals: Int32Array,
    bOrdinals: Int32Array,
    maxWork = BATCH_ROWS,
  ): { length: number; done: boolean } {
    if (this.#closed || !this.#sealed) throw new Error("Star buckets are not replaying");
    if (
      records.length === 0 ||
      aOrdinals.length < records.length ||
      bOrdinals.length < records.length ||
      !Number.isSafeInteger(maxWork) ||
      maxWork < 1
    )
      throw new RangeError("Invalid star replay step");
    let length = 0,
      work = 0;
    while (length < records.length && work < maxWork && this.#b < this.#usedB) {
      work++;
      if (this.#at !== 0) {
        const row = this.#at - 1;
        records[length] = row;
        aOrdinals[length] = this.#a;
        bOrdinals[length] = this.#b;
        length++;
        this.#at = (this.#links[row] ?? 0) >>> 1;
      } else {
        this.#a--;
        if (this.#a < 0) {
          this.#b++;
          this.#a = this.#usedA - 1;
        }
        if (this.#b < this.#usedB && this.#a >= 0)
          this.#at = this.#heads[this.#b * this.aCapacity + this.#a] ?? 0;
      }
    }
    return { length, done: this.#b >= this.#usedB };
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#groups = EMPTY_I32;
    this.#values = EMPTY_F64;
    this.#links = EMPTY_U32;
    this.#heads = EMPTY_U32;
    this.#tails = EMPTY_U32;
    this.#memory.close();
  }
}
