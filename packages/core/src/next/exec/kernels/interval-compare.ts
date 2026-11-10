/** PostgreSQL interval_cmp: compare 30-day months, days and exact µs without changing fields. */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import { isInfiniteInterval } from "../../temporal-text.js";
import {
  flatOf,
  isValidSlot,
  rowsIn,
  countIn,
  slotIndex,
  type Compiled,
  type OutputArena,
} from "./vectors.js";
import type { CompareOp } from "./compare.js";
function value(vector: Vector, row: number): readonly [number, bigint] {
  const data = flatOf(vector).data;
  if (data.layout !== "interval") throw new TypeError("Expected interval");
  const at = slotIndex(vector, row),
    months = data.months[at] ?? 0,
    days = data.days[at] ?? 0,
    micros = int64At(data.micros, at);
  return isInfiniteInterval(months, days, micros)
    ? [months < 0 ? -1 : 1, 0n]
    : [0, (BigInt(months) * 30n + BigInt(days)) * 86400000000n + micros];
}
export class IntervalCompare implements Compiled {
  readonly type: PhysicalType = { kind: "bool" };
  readonly #out: FlatVector;
  readonly #validity: Uint32Array;
  constructor(
    readonly op: CompareOp,
    readonly left: Compiled,
    readonly right: Compiled,
    arena: OutputArena,
    readonly distinct: boolean | null = null,
  ) {
    if (left.type.kind !== "interval" || right.type.kind !== "interval")
      throw new TypeError("Expected two intervals");
    arena.take(4096);
    const out = arena.flat(this.type);
    this.#out = out.vector;
    this.#validity = out.validity;
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const a = this.left.evaluate(batch, rows),
      b = this.right.evaluate(batch, rows),
      out = this.#out.data;
    if (out.layout !== "bool") throw new TypeError("Expected boolean output");
    this.#validity.fill(0);
    const indexes = rowsIn(rows),
      count = countIn(rows, batch.length);
    for (let k = 0; k < count; k++) {
      const row = indexes[k] ?? 0,
        av = isValidSlot(a, row),
        bv = isValidSlot(b, row);
      let same = false,
        order = 0;
      if (av && bv) {
        const [ai, x] = value(a, row),
          [bi, y] = value(b, row);
        order = ai !== bi ? (ai < bi ? -1 : 1) : x < y ? -1 : x > y ? 1 : 0;
        same = order === 0;
      }
      if (this.distinct !== null) {
        same = av && bv ? same : av === bv;
        out.values[row] = Number(this.distinct ? same : !same);
      } else {
        if (!av || !bv) continue;
        out.values[row] = Number(
          this.op === "="
            ? same
            : this.op === "<>"
              ? !same
              : this.op === "<"
                ? order < 0
                : this.op === "<="
                  ? order <= 0
                  : this.op === ">"
                    ? order > 0
                    : order >= 0,
        );
      }
      this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
    }
    this.#out.length = batch.length;
    this.#out.validity = this.distinct === null ? this.#validity : null;
    return this.#out;
  }
}
