/** PG date_mi and timestamp_mi/interval_justify_hours, with strict active-row evaluation. */
import type { PhysicalType } from "../../types.js";
import { INT64_MIN, INT64_MAX, MICROS_PER_DAY } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import {
  DATE_MIN,
  DATE_MAX,
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  MICROS_PER_DAY_BIG,
} from "../../temporal.js";
import { DatetimeOverflowError, UnsupportedPlanError } from "../errors.js";
import {
  countIn,
  flatOf,
  isValidSlot,
  rowsIn,
  slotIndex,
  type Compiled,
  type OutputArena,
} from "./vectors.js";
class TemporalSubtract implements Compiled {
  readonly #out: FlatVector;
  readonly #validity: Uint32Array;
  constructor(
    readonly type: PhysicalType,
    readonly left: Compiled,
    readonly right: Compiled,
    arena: OutputArena,
  ) {
    const out = arena.flat(type);
    this.#out = out.vector;
    this.#validity = out.validity;
    // Exact differences need at most 65 bits; fixed scratch covers the wide path temporaries.
    arena.take(512);
  }
  evaluate(batch: Batch, selection: Selection | null): Vector {
    const l = this.left.evaluate(batch, selection),
      r = this.right.evaluate(batch, selection),
      a = flatOf(l).data,
      b = flatOf(r).data,
      out = this.#out.data;
    const active = rowsIn(selection),
      count = countIn(selection, batch.length);
    this.#validity.fill(0);
    for (let k = 0; k < count; k++) {
      const row = active[k] ?? 0;
      if (!isValidSlot(l, row) || !isValidSlot(r, row)) continue;
      const x = slotIndex(l, row),
        y = slotIndex(r, row);
      if (a.layout === "i32" && b.layout === "i32" && out.layout === "i32") {
        const av = a.values[x] ?? 0,
          bv = b.values[y] ?? 0;
        if (
          av === DATE_POS_INFINITY ||
          av === DATE_NEG_INFINITY ||
          bv === DATE_POS_INFINITY ||
          bv === DATE_NEG_INFINITY
        )
          throw new DatetimeOverflowError("cannot subtract infinite dates");
        if (av < DATE_MIN || av > DATE_MAX || bv < DATE_MIN || bv > DATE_MAX)
          throw new DatetimeOverflowError("date out of range");
        // The complete finite PG DATE span is 2147483493, within int4 in both directions.
        out.values[row] = av - bv;
      } else if (
        a.layout === "int64" &&
        b.layout === "int64" &&
        out.layout === "interval" &&
        out.micros.repr === "safe"
      ) {
        let days: number, micros: number;
        const av = a.values.values[x] ?? 0,
          bv = b.values.values[y] ?? 0;
        const delta =
          a.values.repr === "safe" && b.values.repr === "safe" ? Number(av) - Number(bv) : NaN;
        if (Number.isSafeInteger(delta)) {
          days = Math.trunc(delta / MICROS_PER_DAY);
          micros = delta - days * MICROS_PER_DAY;
        } else {
          const ai = int64At(a.values, x),
            bi = int64At(b.values, y);
          const aiInf = ai === TIMESTAMP_POS_INFINITY || ai === TIMESTAMP_NEG_INFINITY,
            biInf = bi === TIMESTAMP_POS_INFINITY || bi === TIMESTAMP_NEG_INFINITY;
          if (aiInf || biInf) {
            if (ai === bi) throw new DatetimeOverflowError("interval out of range");
            throw new UnsupportedPlanError("infinite INTERVAL results", { construct: "function" });
          }
          if (
            ai < TIMESTAMP_MIN ||
            ai >= TIMESTAMP_END ||
            bi < TIMESTAMP_MIN ||
            bi >= TIMESTAMP_END
          )
            throw new DatetimeOverflowError("timestamp out of range");
          const difference = ai - bi;
          if (difference < INT64_MIN || difference > INT64_MAX)
            throw new DatetimeOverflowError("interval out of range");
          days = Number(difference / MICROS_PER_DAY_BIG);
          micros = Number(difference % MICROS_PER_DAY_BIG);
        }
        out.months[row] = 0;
        out.days[row] = days;
        out.micros.values[row] = micros;
      } else throw new TypeError("Expected DATE or TIMESTAMP subtraction layouts");
      this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
    }
    this.#out.length = batch.length;
    this.#out.validity = this.#validity;
    return this.#out;
  }
}
export function compileTemporalSubtract(
  type: PhysicalType,
  left: Compiled,
  right: Compiled,
  arena: OutputArena,
): Compiled {
  if (
    !(left.type.kind === "date" && right.type.kind === "date" && type.kind === "int32") &&
    !(left.type.kind === "timestamp" && right.type.kind === "timestamp" && type.kind === "interval")
  )
    throw new TypeError("Expected DATE or TIMESTAMP subtraction");
  return new TemporalSubtract(type, left, right, arena);
}
