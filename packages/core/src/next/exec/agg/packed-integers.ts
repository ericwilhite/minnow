/** Eight exact integer/count outputs in one 64-byte record per group.
 * The six arguments must have identical, all-valid masks while packed. At most
 * 2^21 int32 rows give |sum| <= 2^52, so every addition is exact. On losing that
 * certificate, restore each ordinary accumulator at its original update position.
 * Float states are never combined. Promotion is bounded to at most 4096 old
 * groups plus one input batch, rather than copying arbitrarily many groups.
 */
import type { AggregateCall } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { Batch, FlatVector, Vector } from "../../vector/vector.js";
import { Charged, grownCapacity } from "./groups.js";
import { accumulatorFor, type Accumulator } from "./states.js";

interface Member {
  readonly index: number;
  readonly call: AggregateCall;
  readonly column: number;
}

export function packedIntegerCalls(
  calls: readonly AggregateCall[],
  input: readonly PhysicalType[],
): readonly Member[] | null {
  const plain = calls.flatMap((call, index) => {
    const arg = call.args[0];
    if (call.filter !== null || call.distinct || call.orderBy.length !== 0) return [];
    if (call.fn === "count_star" && call.args.length === 0 && call.type.kind === "int64")
      return [{ index, call, column: -1 }];
    if (call.args.length !== 1 || arg?.op !== "column" || input[arg.index]?.kind !== "int32")
      return [];
    if (
      ((call.fn === "sum" || call.fn === "count") && call.type.kind === "int64") ||
      (call.fn === "avg" && call.type.kind === "float64")
    )
      return [{ index, call, column: arg.index }];
    return [];
  });
  const count = plain.find((m) => m.call.fn === "count_star"),
    sums = plain.filter((m) => m.call.fn === "sum").slice(0, 5),
    average = plain.find((m) => m.call.fn === "avg"),
    nullable = plain.find((m) => m.call.fn === "count");
  if (!count || sums.length !== 5 || !average || !nullable) return null;
  const selected = [count, ...sums, average, nullable];
  // SUM/AVG sharing of the same input remains the existing exact-state plan.
  if (new Set([...sums, average].map((m) => m.column)).size !== 6) return null;
  for (const member of [...sums, average])
    if (
      plain.filter(
        (m) => (m.call.fn === "sum" || m.call.fn === "avg") && m.column === member.column,
      ).length !== 1
    )
      return null;
  return selected;
}

const STRIDE = 8;
const MAX_GROUPS = 4096;
export class PackedIntegers {
  readonly #memory: Charged;
  #state: Float64Array = new Float64Array(0);
  #capacity = 0;
  #groups = 0;
  #seen = 0;
  #fallback = false;
  #promoted = 0;
  readonly #ordinary: Array<Accumulator | undefined> = new Array<Accumulator | undefined>(8);
  readonly members: ReadonlyArray<{ readonly index: number; readonly accumulator: Accumulator }>;
  fusedRows = 0;

  constructor(
    readonly calls: readonly Member[],
    readonly input: readonly PhysicalType[],
    readonly charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
    this.members = calls.map((member, slot) => ({
      index: member.index,
      accumulator: {
        type: member.call.type,
        grow: (groups: number) => {
          this.#groups = groups;
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
        ) => {
          const ordinary = this.#restore(slot);
          ordinary.update(vector, gids, rows, count, scalar);
        },
        outputBytes: (from: number, count: number) =>
          this.#ordinary[slot]?.outputBytes(from, count) ?? 8 * count + 4 * ((count + 31) >>> 5),
        emit: (from: number, count: number) =>
          this.#ordinary[slot]?.emit(from, count) ?? this.#emit(slot, from, count),
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
    const member = this.calls[slot];
    if (!member) throw new Error("Missing packed integer member");
    const ordinary = accumulatorFor(
      member.call.fn,
      member.call.type,
      this.input[member.column] ?? null,
      this.charge,
    );
    this.#ordinary[slot] = ordinary;
    if (!ordinary.restoreIntegerPrefix) throw new Error("Integer prefix cannot be restored");
    ordinary.restoreIntegerPrefix(this.#state, STRIDE, slot, 0, this.#groups);
    if (++this.#promoted === this.calls.length) {
      this.#memory.close();
      this.#state = new Float64Array(0);
    }
    return ordinary;
  }

  /** False changes no transitions; each member then restores in ordinary aggregate order. */
  tryUpdate(batch: Batch, gids: Int32Array, rows: Uint16Array, count: number): boolean {
    if (this.#fallback || this.#groups > MAX_GROUPS || this.#seen + count > 2 ** 21) {
      this.#fallback = true;
      return false;
    }
    const a = batch.columns[this.calls[1]?.column ?? -1],
      b = batch.columns[this.calls[2]?.column ?? -1],
      c = batch.columns[this.calls[3]?.column ?? -1],
      d = batch.columns[this.calls[4]?.column ?? -1],
      e = batch.columns[this.calls[5]?.column ?? -1],
      f = batch.columns[this.calls[6]?.column ?? -1],
      nullable = batch.columns[this.calls[7]?.column ?? -1];
    if (
      !a ||
      !b ||
      !c ||
      !d ||
      !e ||
      !f ||
      !nullable ||
      a.form !== "flat" ||
      b.form !== "flat" ||
      c.form !== "flat" ||
      d.form !== "flat" ||
      e.form !== "flat" ||
      f.form !== "flat" ||
      nullable.form !== "flat" ||
      a.validity !== null ||
      b.validity !== null ||
      c.validity !== null ||
      d.validity !== null ||
      e.validity !== null ||
      f.validity !== null ||
      a.data.layout !== "i32" ||
      b.data.layout !== "i32" ||
      c.data.layout !== "i32" ||
      d.data.layout !== "i32" ||
      e.data.layout !== "i32" ||
      f.data.layout !== "i32"
    ) {
      this.#fallback = true;
      return false;
    }
    exactEight(
      this.#state,
      a.data.values,
      b.data.values,
      c.data.values,
      d.data.values,
      e.data.values,
      f.data.values,
      nullable.validity,
      gids,
      rows,
      count,
    );
    this.#seen += count;
    this.fusedRows += count;
    return true;
  }

  #emit(slot: number, from: number, count: number): FlatVector {
    const member = this.calls[slot];
    if (!member) throw new Error("Missing packed integer member");
    const values = new Float64Array(count),
      validity = new Uint32Array((count + 31) >>> 5),
      isCount = slot === 0 || slot === 7;
    let all = true;
    for (let k = 0; k < count; k++) {
      const at = (from + k) * STRIDE,
        n = this.#state[at] ?? 0;
      if (!isCount && n === 0) all = false;
      else validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
      const v = this.#state[at + slot] ?? 0;
      values[k] = slot === 6 ? v / n : v;
    }
    return {
      form: "flat",
      type: member.call.type,
      length: count,
      validity: all ? null : validity,
      data:
        slot === 6
          ? { layout: "f64", values }
          : { layout: "int64", values: { repr: "safe", values } },
    };
  }
}

/** Bounds: rows selects valid input slots, gids comes from GroupTable.assign,
 * and grow(groupCount) allocated every addressed 8-field record before push.
 * The six flat int32 arrays and nullable bitmap cover the batch's row domain.
 * These type assertions remove optional-slot fallback from the certified kernel.
 */
/* eslint-disable @typescript-eslint/non-nullable-type-assertion-style -- bounds proved above; use explicit numeric reads in the typed kernel */
function exactEight(
  state: Float64Array,
  a: Int32Array,
  b: Int32Array,
  c: Int32Array,
  d: Int32Array,
  e: Int32Array,
  f: Int32Array,
  validity: Uint32Array | null,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): void {
  for (let k = 0; k < count; k++) {
    const row = rows[k] as number,
      at = (gids[row] as number) * STRIDE;
    state[at] = (state[at] as number) + 1;
    state[at + 1] = (state[at + 1] as number) + (a[row] as number);
    state[at + 2] = (state[at + 2] as number) + (b[row] as number);
    state[at + 3] = (state[at + 3] as number) + (c[row] as number);
    state[at + 4] = (state[at + 4] as number) + (d[row] as number);
    state[at + 5] = (state[at + 5] as number) + (e[row] as number);
    state[at + 6] = (state[at + 6] as number) + (f[row] as number);
    state[at + 7] =
      (state[at + 7] as number) +
      (validity === null ? 1 : ((validity[row >>> 5] as number) >>> (row & 31)) & 1);
  }
}

/* eslint-enable @typescript-eslint/non-nullable-type-assertion-style */
