/** A certified row-major numeric transition bank, selected by aggregate/type shape.
 * N <= 2^21 and int32 inputs imply exact integer sums within +/-2^52. Finite
 * float inputs |x| <= 2^400 satisfy the stricter version of packed-averages.ts's
 * overflow proof (tmp² <= 2^859, Sxx <= 2^887). Every PG Sx/Sxx transition is
 * retained, in per-group input order. No state is combined or reassociated.
 * Losing any certificate promotes BEFORE the batch, in original aggregate order.
 * At most 1024 old records need restoring; new groups have an empty prefix.
 */
import type { AggregateCall } from "../../ir/ir.js";
import { layoutOf, type PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, FlatVector, Vector } from "../../vector/vector.js";
import { Charged, grownCapacity } from "./groups.js";
import { accumulatorFor, type Accumulator } from "./states.js";

interface Member {
  readonly index: number;
  readonly call: AggregateCall;
  readonly column: number;
  readonly offset: number;
}
const EMPTY_NUMERIC = new Float64Array(0);
const STRIDE = 32,
  MAX_GROUPS = 1024;
/** A reusable family kernel: 5 int sums, 5 float averages, numeric extrema,
 * 2 float sums, an int average, two counts and a safe int64 maximum. Text and
 * all other aggregates remain ordinary members of the same operator. */
export function typedLayoutCalls(
  calls: readonly AggregateCall[],
  input: readonly PhysicalType[],
): readonly Member[] | null {
  const plain = calls.flatMap((call, index) => {
    const arg = call.args[0];
    if (call.distinct || call.filter !== null || call.orderBy.length !== 0) return [];
    if (call.fn === "count_star" && call.args.length === 0 && call.type.kind === "int64")
      return [{ call, index, column: -1 }];
    if (call.args.length !== 1 || arg?.op !== "column") return [];
    return [{ call, index, column: arg.index }];
  });
  const selected: Member[] = [];
  function take(
    fn: AggregateCall["fn"],
    kind: PhysicalType["kind"],
    result: PhysicalType["kind"],
    count: number,
    offset: number,
  ): boolean {
    const found = plain
      .filter(
        (m) =>
          m.call.fn === fn &&
          (m.column < 0 || input[m.column]?.kind === kind) &&
          m.call.type.kind === result &&
          !selected.some((s) => s.index === m.index),
      )
      .slice(0, count);
    found.forEach((m, i) => selected.push({ ...m, offset: offset + i }));
    return found.length === count;
  }
  if (!(
    take("count_star", "int32", "int64", 1, 0) &&
    take("sum", "int32", "int64", 5, 1) &&
    take("avg", "float64", "float64", 5, 6) &&
    take("min", "float64", "float64", 1, 16) &&
    take("max", "float64", "float64", 1, 17) &&
    take("min", "int32", "int32", 1, 18) &&
    take("max", "int32", "int32", 1, 19) &&
    take("sum", "float64", "float64", 2, 20) &&
    take("avg", "int32", "float64", 1, 22) &&
    take("count", "int32", "int64", 1, 23)
  ))
    return null;
  // Timestamp/date use int64 as well: choose by physical layout, not SQL type name.
  const last = plain.find(
    (m) =>
      m.call.fn === "max" &&
      m.column >= 0 &&
      input[m.column] &&
      layoutOf(input[m.column] ?? m.call.type) === "int64" &&
      layoutOf(m.call.type) === "int64" &&
      !selected.some((s) => s.index === m.index),
  );
  if (!last) return null;
  selected.push({ ...last, offset: 24 });
  // Shared SUM/AVG ownership is compiled separately; do not intercept either half.
  if (
    selected.some(
      (m) =>
        (m.call.fn === "sum" || m.call.fn === "avg") &&
        plain.some(
          (o) =>
            o.index !== m.index &&
            o.column === m.column &&
            ((o.call.fn === "sum" && m.call.fn === "avg") ||
              (o.call.fn === "avg" && m.call.fn === "sum")),
        ),
    )
  )
    return null;
  return selected;
}

export class TypedLayout {
  readonly #memory: Charged;
  #state: Float64Array = new Float64Array(0);
  #capacity = 0;
  #groups = 0;
  #seen = 0;
  #fallback = false;
  #promoted = 0;
  readonly #arrays: Array<Int32Array | Float64Array> = Array.from(
    { length: 25 },
    () => EMPTY_NUMERIC,
  );
  readonly #ordinary: Array<Accumulator | undefined>;
  readonly members: ReadonlyArray<{ readonly index: number; readonly accumulator: Accumulator }>;
  fusedRows = 0;
  constructor(
    readonly calls: readonly Member[],
    readonly input: readonly PhysicalType[],
    readonly charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
    this.#ordinary = new Array<Accumulator | undefined>(calls.length);
    this.members = calls.map((member, slot) => ({
      index: member.index,
      accumulator: {
        type: member.call.type,
        grow: (groups: number) => {
          this.#groups = groups;
          if (groups > MAX_GROUPS) this.#fallback = true;
          if (!this.#fallback && groups > this.#capacity) {
            const capacity = grownCapacity(this.#capacity, groups);
            this.#state = this.#memory.float64(this.#state, STRIDE * capacity);
            this.#capacity = capacity;
          }
          this.#ordinary[slot]?.grow(groups);
        },
        update: (
          vector: Vector | null,
          gids: Int32Array,
          rows: Uint16Array,
          count: number,
          scalar: boolean,
        ) => this.#restore(slot).update(vector, gids, rows, count, scalar),
        outputBytes: (from: number, count: number) =>
          this.#ordinary[slot]?.outputBytes(from, count) ??
          (layoutOf(member.call.type) === "i32" ? 12 : 8) * count + 4 * ((count + 31) >>> 5),
        emit: (from: number, count: number) =>
          this.#ordinary[slot]?.emit(from, count) ?? this.#emit(member, from, count),
        close: () => {
          this.#ordinary[slot]?.close();
          this.#memory.close();
        },
      },
    }));
  }
  #restore(slot: number): Accumulator {
    this.#fallback = true;
    const known = this.#ordinary[slot];
    if (known) return known;
    const m = this.calls[slot];
    if (!m) throw new Error("Missing numeric layout member");
    const ordinary = accumulatorFor(
      m.call.fn,
      m.call.type,
      this.input[m.column] ?? null,
      this.charge,
    );
    this.#ordinary[slot] = ordinary;
    if (m.offset === 0 || m.offset <= 5 || m.offset === 22 || m.offset === 23) {
      if (!ordinary.restoreIntegerPrefix) throw new Error("Missing integer restore");
      ordinary.restoreIntegerPrefix(this.#state, STRIDE, m.offset, 0, this.#groups);
    } else {
      if (!ordinary.restoreNumberPrefix) throw new Error("Missing numeric restore");
      ordinary.restoreNumberPrefix(
        this.#state,
        STRIDE,
        m.offset,
        0,
        this.#groups,
        m.offset >= 6 && m.offset <= 10 ? m.offset + 5 : undefined,
      );
    }
    if (++this.#promoted === this.calls.length) {
      this.#memory.close();
      this.#state = new Float64Array(0);
    }
    return ordinary;
  }
  /** No transition changes until every field in the entire selected batch is certified. */
  tryUpdate(batch: Batch, gids: Int32Array, rows: Uint16Array, count: number): boolean {
    if (this.#fallback || this.#seen + count > 2 ** 21) {
      this.#fallback = true;
      return false;
    }
    const arrays = this.#arrays;
    try {
      let nullable: Uint32Array | null = null;
      for (const m of this.calls) {
        if (m.column < 0) continue;
        const v = batch.columns[m.column];
        if (v?.form !== "flat" || (m.offset !== 23 && v.validity !== null)) {
          this.#fallback = true;
          return false;
        }
        if (m.offset === 23) {
          nullable = v.validity;
          continue;
        }
        const d = v.data;
        if (m.offset === 24) {
          if (d.layout !== "int64" || d.values.repr !== "safe") {
            this.#fallback = true;
            return false;
          }
          arrays[m.offset] = d.values.values;
        } else if (d.layout === "i32" || d.layout === "f64") arrays[m.offset] = d.values;
        else {
          this.#fallback = true;
          return false;
        }
        if (d.layout === "f64")
          for (let k = 0; k < count; k++)
            if (!(Math.abs(d.values[rows[k] ?? 0] ?? 0) <= 2 ** 400)) {
              this.#fallback = true;
              return false;
            }
      }
      record(this.#state, arrays, nullable, gids, rows, count);
      this.#seen += count;
      this.fusedRows += count;
      return true;
    } finally {
      arrays.fill(EMPTY_NUMERIC);
    }
  }
  transition(index: number, group: number): readonly [number, number, number] {
    const slot = this.calls.findIndex((m) => m.offset === 6 + index);
    const ordinary = this.#ordinary[slot];
    if (ordinary?.floatTransition) return ordinary.floatTransition(group);
    const at = group * STRIDE;
    return [
      this.#state[at] ?? 0,
      this.#state[at + 6 + index] ?? 0,
      this.#state[at + 11 + index] ?? 0,
    ];
  }
  #emit(m: Member, from: number, count: number): FlatVector {
    const layout = layoutOf(m.call.type);
    const values = new Float64Array(count),
      validity = new Uint32Array((count + 31) >>> 5),
      isCount = m.offset === 0 || m.offset === 23;
    let all = true;
    for (let k = 0; k < count; k++) {
      const at = (from + k) * STRIDE,
        n = this.#state[at] ?? 0;
      if (!isCount && n === 0) all = false;
      else validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
      const v = this.#state[at + m.offset] ?? 0;
      values[k] = m.call.fn === "avg" ? v / n : v;
    }
    return {
      form: "flat",
      type: m.call.type,
      length: count,
      validity: all ? null : validity,
      data:
        layout === "int64"
          ? { layout, values: { repr: "safe", values } }
          : layout === "i32"
            ? { layout, values: Int32Array.from(values) }
            : { layout: "f64", values },
    };
  }
}

/** Arrays are certified dense and all-valid; rows/gids come from GroupTable.
 * Every addressed 32-field record was charged and grown before this call. */
/* eslint-disable @typescript-eslint/non-nullable-type-assertion-style -- certified kernel bounds */
function record(
  state: Float64Array,
  arrays: ReadonlyArray<Int32Array | Float64Array>,
  validity: Uint32Array | null,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): void {
  const i0 = arrays[1] as Int32Array | Float64Array,
    i1 = arrays[2] as Int32Array | Float64Array,
    i2 = arrays[3] as Int32Array | Float64Array,
    i3 = arrays[4] as Int32Array | Float64Array,
    i4 = arrays[5] as Int32Array | Float64Array,
    a0 = arrays[6] as Int32Array | Float64Array,
    a1 = arrays[7] as Int32Array | Float64Array,
    a2 = arrays[8] as Int32Array | Float64Array,
    a3 = arrays[9] as Int32Array | Float64Array,
    a4 = arrays[10] as Int32Array | Float64Array,
    mnf = arrays[16] as Int32Array | Float64Array,
    mxf = arrays[17] as Int32Array | Float64Array,
    mni = arrays[18] as Int32Array | Float64Array,
    mxi = arrays[19] as Int32Array | Float64Array,
    s0 = arrays[20] as Int32Array | Float64Array,
    s1 = arrays[21] as Int32Array | Float64Array,
    ai = arrays[22] as Int32Array | Float64Array,
    ts = arrays[24] as Int32Array | Float64Array;
  for (let k = 0; k < count; k++) {
    const row = rows[k] as number,
      at = (gids[row] as number) << 5,
      before = state[at] as number,
      n = before + 1,
      denominator = n * before;
    state[at] = n;
    state[at + 1] = (state[at + 1] as number) + (i0[row] as number);
    state[at + 2] = (state[at + 2] as number) + (i1[row] as number);
    state[at + 3] = (state[at + 3] as number) + (i2[row] as number);
    state[at + 4] = (state[at + 4] as number) + (i3[row] as number);
    state[at + 5] = (state[at + 5] as number) + (i4[row] as number);
    {
      const v = a0[row] as number,
        sum = (state[at + 6] as number) + v,
        t = v * n - sum;
      state[at + 11] = before > 0 ? (state[at + 11] as number) + (t * t) / denominator : 0;
      state[at + 6] = sum;
    }
    {
      const v = a1[row] as number,
        sum = (state[at + 7] as number) + v,
        t = v * n - sum;
      state[at + 12] = before > 0 ? (state[at + 12] as number) + (t * t) / denominator : 0;
      state[at + 7] = sum;
    }
    {
      const v = a2[row] as number,
        sum = (state[at + 8] as number) + v,
        t = v * n - sum;
      state[at + 13] = before > 0 ? (state[at + 13] as number) + (t * t) / denominator : 0;
      state[at + 8] = sum;
    }
    {
      const v = a3[row] as number,
        sum = (state[at + 9] as number) + v,
        t = v * n - sum;
      state[at + 14] = before > 0 ? (state[at + 14] as number) + (t * t) / denominator : 0;
      state[at + 9] = sum;
    }
    {
      const v = a4[row] as number,
        sum = (state[at + 10] as number) + v,
        t = v * n - sum;
      state[at + 15] = before > 0 ? (state[at + 15] as number) + (t * t) / denominator : 0;
      state[at + 10] = sum;
    }
    if (before === 0) {
      state[at + 16] = mnf[row] as number;
      state[at + 17] = mxf[row] as number;
      state[at + 18] = mni[row] as number;
      state[at + 19] = mxi[row] as number;
      state[at + 20] = -0;
      state[at + 21] = -0;
      state[at + 24] = ts[row] as number;
    } else {
      if (!((state[at + 16] as number) < (mnf[row] as number))) state[at + 16] = mnf[row] as number;
      if (!((state[at + 17] as number) > (mxf[row] as number))) state[at + 17] = mxf[row] as number;
      if (!((state[at + 18] as number) < (mni[row] as number))) state[at + 18] = mni[row] as number;
      if (!((state[at + 19] as number) > (mxi[row] as number))) state[at + 19] = mxi[row] as number;
      if (!((state[at + 24] as number) > (ts[row] as number))) state[at + 24] = ts[row] as number;
    }
    state[at + 20] = (state[at + 20] as number) + (s0[row] as number);
    state[at + 21] = (state[at + 21] as number) + (s1[row] as number);
    state[at + 22] = (state[at + 22] as number) + (ai[row] as number);
    state[at + 23] =
      (state[at + 23] as number) +
      (validity === null || ((validity[row >>> 5] as number) & (1 << (row & 31))) !== 0 ? 1 : 0);
  }
}
/* eslint-enable @typescript-eslint/non-nullable-type-assertion-style */
