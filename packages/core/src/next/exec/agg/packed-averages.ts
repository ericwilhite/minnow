/** Five adjacent, direct-column AVG(float8)s, packed by group.
 *
 * Each AVG keeps PostgreSQL's Sx and Youngs-Cramer Sxx, in input order. N is shared
 * only while every input mask since construction has matched. Before a different
 * mask (or an exceptional transition) it is copied into five independent Ns.
 * Sxx is never omitted, reconstructed, combined or reassociated.
 */
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, FlatVector, Vector } from "../../vector/vector.js";
import { NumericOutOfRangeError } from "../errors.js";
import { Charged, grownCapacity } from "./groups.js";
import type { Accumulator } from "./states.js";

// N, Sx[5], Sxx[5]. Independent Ns are allocated only on promotion.
const STRIDE = 11;

export class PackedAverages {
  readonly #memory: Charged;
  #state: Float64Array = new Float64Array(0);
  #counts: Float64Array = new Float64Array(0);
  #capacity = 0;
  #shared = true;
  #safeRows = 0;
  /** Diagnostic coverage: number of selected rows actually taken by the fused kernel. */
  fusedRows = 0;
  readonly members: readonly Accumulator[];

  constructor(
    readonly type: PhysicalType,
    charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
    this.members = Array.from({ length: 5 }, (_, index) => ({
      type,
      grow: (groups: number) => {
        this.grow(groups);
      },
      update: (
        vector: Vector | null,
        gids: Int32Array,
        rows: Uint16Array,
        count: number,
        scalar: boolean,
      ) => {
        if (vector !== null) {
          this.#separate();
          this.#update(index, vector, gids, rows, 0, count, scalar);
        }
      },
      outputBytes: (_from: number, count: number) => 8 * count + 4 * ((count + 31) >>> 5),
      emit: (from: number, count: number) => this.#emit(index, from, count),
      close: () => {
        this.#memory.close();
      },
    }));
  }

  grow(groups: number): void {
    if (groups > this.#capacity) {
      const capacity = grownCapacity(this.#capacity, groups);
      this.#state = this.#memory.float64(this.#state, STRIDE * capacity);
      if (!this.#shared) this.#counts = this.#memory.float64(this.#counts, 5 * capacity);
      this.#capacity = capacity;
    }
  }

  /** Internal transition diagnostics, including Sxx which AVG does not emit. */
  transition(index: number, group: number): readonly [number, number, number] {
    const at = group * STRIDE;
    return [
      (this.#state[at] ?? 0) >= 0 ? (this.#state[at] ?? 0) : (this.#counts[group * 5 + index] ?? 0),
      this.#state[at + 1 + index] ?? 0,
      this.#state[at + 6 + index] ?? 0,
    ];
  }

  /** All arguments are direct columns, so evaluating later columns cannot raise first. */
  updateBatch(
    batch: Batch,
    columns: readonly number[],
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
  ): void {
    const v0 = batch.columns[columns[0] ?? -1],
      v1 = batch.columns[columns[1] ?? -1],
      v2 = batch.columns[columns[2] ?? -1],
      v3 = batch.columns[columns[3] ?? -1],
      v4 = batch.columns[columns[4] ?? -1];
    if (!v0 || !v1 || !v2 || !v3 || !v4) throw new RangeError("Packed AVG column missing");
    let taken = 0;
    if (
      this.#shared &&
      v0.form === "flat" &&
      v1.form === "flat" &&
      v2.form === "flat" &&
      v3.form === "flat" &&
      v4.form === "flat" &&
      v0.validity === null &&
      v1.validity === null &&
      v2.validity === null &&
      v3.validity === null &&
      v4.validity === null &&
      v0.data.layout === "f64" &&
      v1.data.layout === "f64" &&
      v2.data.layout === "f64" &&
      v3.data.layout === "f64" &&
      v4.data.layout === "f64" &&
      this.#safeRows + count <= 2 ** 26 &&
      bounded(
        v0.data.values,
        v1.data.values,
        v2.data.values,
        v3.data.values,
        v4.data.values,
        rows,
        count,
      )
    ) {
      taken = denseFive(
        this.#state,
        v0.data.values,
        v1.data.values,
        v2.data.values,
        v3.data.values,
        v4.data.values,
        gids,
        rows,
        count,
      );
      this.fusedRows += taken;
      this.#safeRows += taken;
      if (taken === count) return;
    }
    this.#separate();
    // An uncertified batch was committed to none of the five states. Finish the
    // first aggregate before the next. NULL/non-flat batches use these same states.
    this.#update(0, v0, gids, rows, taken, count);
    this.#update(1, v1, gids, rows, taken, count);
    this.#update(2, v2, gids, rows, taken, count);
    this.#update(3, v3, gids, rows, taken, count);
    this.#update(4, v4, gids, rows, taken, count);
  }

  #separate(): void {
    if (!this.#shared) return;
    const counts = this.#memory.make(Float64Array, this.#capacity * 5);
    // Restore N lazily when a group is touched: a late mask change must not
    // introduce an unbounded synchronous loop over every previously seen group.
    this.#counts = counts;
    this.#shared = false;
  }

  #update(
    index: number,
    vector: Vector,
    gids: Int32Array,
    rows: Uint16Array,
    from: number,
    count: number,
    scalar = false,
  ): void {
    const flat =
      vector.form === "dictionary"
        ? vector.dictionary
        : vector.form === "constant"
          ? vector.value
          : vector;
    if (flat.data.layout !== "f64") throw new TypeError("Packed AVG requires float64");
    const values = flat.data.values,
      state = this.#state;
    const validity = vector.form === "constant" ? null : vector.validity;
    if (vector.form === "constant" && flat.validity !== null && ((flat.validity[0] ?? 0) & 1) === 0)
      return;
    for (let k = from; k < count; k++) {
      const row = rows[k] ?? 0;
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      const slot =
        vector.form === "constant"
          ? 0
          : vector.form === "dictionary"
            ? (vector.codes[row] ?? 0)
            : row;
      const g = scalar ? 0 : (gids[row] ?? 0),
        at = g * STRIDE;
      const shared = state[at] ?? 0;
      if (shared >= 0) {
        this.#counts.fill(shared, g * 5, g * 5 + 5);
        state[at] = -1;
      }
      const v = values[slot] ?? 0,
        before = this.#counts[g * 5 + index] ?? 0,
        n = before + 1,
        old = state[at + 1 + index] ?? 0,
        sum = old + v;
      let sxx = state[at + 6 + index] ?? 0;
      if (before > 0) {
        const t = v * n - sum;
        sxx += (t * t) / (n * before);
        if (sum - sum !== 0 || sxx - sxx !== 0) {
          if (sum === Infinity || sum === -Infinity || sxx === Infinity || sxx === -Infinity) {
            if (old !== Infinity && old !== -Infinity && v !== Infinity && v !== -Infinity)
              throw new NumericOutOfRangeError("value out of range: overflow");
            sxx = NaN;
          }
        }
      } else if (v - v !== 0) sxx = NaN;
      state[at + 1 + index] = sum;
      state[at + 6 + index] = sxx;
      this.#counts[g * 5 + index] = n;
    }
  }

  #emit(index: number, from: number, count: number): FlatVector {
    const values = new Float64Array(count),
      validity = new Uint32Array((count + 31) >>> 5);
    let all = true;
    for (let k = 0; k < count; k++) {
      const at = (from + k) * STRIDE,
        n =
          (this.#state[at] ?? 0) >= 0
            ? (this.#state[at] ?? 0)
            : (this.#counts[(from + k) * 5 + index] ?? 0);
      if (n === 0) all = false;
      else {
        validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
        values[k] = (this.#state[at + 1 + index] ?? 0) / n;
      }
    }
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity: all ? null : validity,
      data: { layout: "f64", values },
    };
  }
}

/** Certify the entire accumulated prefix, not merely this batch. N <= 2^26,
 * |x| <= 2^400: with binary64 u=2^-53, (1+u)^N < 2, hence rounded
 * |Sx| <= 2*n*2^400, |tmp| <= 8*N*2^400 <= 2^429 and tmp² <= 2^859.
 * The exact integer denominator n*(n-1) is in [2,2^52]. Allowing rounding
 * slack per increment bounds accumulated Sxx by 2*N*2^860 <= 2^887.
 * Thus none of these five transitions can throw. Sxx is still computed and
 * retained after EVERY input, so a future uncertified batch can use the exact
 * prior state. This is not the deferred query-wide Sxx omission optimization.
 */
function bounded(
  a: Float64Array,
  b: Float64Array,
  c: Float64Array,
  d: Float64Array,
  e: Float64Array,
  rows: Uint16Array,
  count: number,
): boolean {
  const max = 2 ** 400;
  for (let k = 0; k < count; k++) {
    const row = rows[k] ?? 0,
      v0 = a[row] ?? 0,
      v1 = b[row] ?? 0,
      v2 = c[row] ?? 0,
      v3 = d[row] ?? 0,
      v4 = e[row] ?? 0;
    if (!(
      v0 >= -max &&
      v0 <= max &&
      v1 >= -max &&
      v1 <= max &&
      v2 >= -max &&
      v2 <= max &&
      v3 >= -max &&
      v3 <= max &&
      v4 >= -max &&
      v4 <= max
    ))
      return false;
  }
  return true;
}

/** Every multiply/add/divide matches float8_accum. `bounded` proved this whole prefix finite.
 * GroupTable assigns gids in [0, groupCount), grow allocated all 11-field records,
 * and these flat arrays cover every selected row. Reads are inside those bounds.
 */
/* eslint-disable @typescript-eslint/non-nullable-type-assertion-style -- bounds proved above; use explicit numeric reads in the typed kernel */
function denseFive(
  state: Float64Array,
  a: Float64Array,
  b: Float64Array,
  c: Float64Array,
  d: Float64Array,
  e: Float64Array,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): number {
  let k = 0;
  for (; k < count; k++) {
    const row = rows[k] as number,
      at = (gids[row] as number) * STRIDE,
      before = state[at] as number,
      n = before + 1,
      denominator = n * before,
      v0 = a[row] as number,
      v1 = b[row] as number,
      v2 = c[row] as number,
      v3 = d[row] as number,
      v4 = e[row] as number,
      s0 = (state[at + 1] as number) + v0,
      t0 = v0 * n - s0,
      q0 = before > 0 ? (state[at + 6] as number) + (t0 * t0) / denominator : 0,
      s1 = (state[at + 2] as number) + v1,
      t1 = v1 * n - s1,
      q1 = before > 0 ? (state[at + 7] as number) + (t1 * t1) / denominator : 0,
      s2 = (state[at + 3] as number) + v2,
      t2 = v2 * n - s2,
      q2 = before > 0 ? (state[at + 8] as number) + (t2 * t2) / denominator : 0,
      s3 = (state[at + 4] as number) + v3,
      t3 = v3 * n - s3,
      q3 = before > 0 ? (state[at + 9] as number) + (t3 * t3) / denominator : 0,
      s4 = (state[at + 5] as number) + v4,
      t4 = v4 * n - s4,
      q4 = before > 0 ? (state[at + 10] as number) + (t4 * t4) / denominator : 0;
    state[at] = n;
    state[at + 1] = s0;
    state[at + 6] = q0;
    state[at + 2] = s1;
    state[at + 7] = q1;
    state[at + 3] = s2;
    state[at + 8] = q2;
    state[at + 4] = s3;
    state[at + 9] = q3;
    state[at + 5] = s4;
    state[at + 10] = q4;
  }
  return k;
}

/* eslint-enable @typescript-eslint/non-nullable-type-assertion-style */
