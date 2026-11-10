/** PostgreSQL timestamp_pl_interval: clamp months, then add days, then exact microseconds (UTC). */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import {
  civilFromDays,
  daysFromCivil,
  MICROS_PER_DAY_BIG,
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  DATE_MIN,
} from "../../temporal.js";
import { isInfiniteInterval } from "../../temporal-text.js";
import { DatetimeOverflowError, UnsupportedPlanError } from "../errors.js";
import {
  countIn,
  rowsIn,
  isValidSlot,
  flatOf,
  slotIndex,
  type Compiled,
  type OutputArena,
} from "./vectors.js";
function dayFloor(value: bigint): bigint {
  const remainder = value % MICROS_PER_DAY_BIG;
  return value / MICROS_PER_DAY_BIG - (remainder < 0n ? 1n : 0n);
}
function validDay(day: number): void {
  // Julian-day arithmetic in PostgreSQL is int32; timestamp range is checked after the time part.
  if (!Number.isSafeInteger(day) || day < DATE_MIN || day + 2451545 > 2147483647)
    throw new DatetimeOverflowError("timestamp out of range");
}
export function addCalendar(value: bigint, months: number, days: number, micros: bigint): bigint {
  if (value === TIMESTAMP_NEG_INFINITY || value === TIMESTAMP_POS_INFINITY) return value;
  if (value < TIMESTAMP_MIN || value >= TIMESTAMP_END)
    throw new DatetimeOverflowError("timestamp out of range");
  const original = dayFloor(value),
    time = value - original * MICROS_PER_DAY_BIG;
  let day = Number(original);
  const validTimestamp = () => {
    const intermediate = BigInt(day) * MICROS_PER_DAY_BIG + time;
    if (intermediate < TIMESTAMP_MIN || intermediate >= TIMESTAMP_END)
      throw new DatetimeOverflowError("timestamp out of range");
  };
  if (months !== 0) {
    const [year, month, date] = civilFromDays(day),
      total = year * 12 + month - 1 + months;
    const y = Math.floor(total / 12),
      m = total - y * 12 + 1;
    const first = daysFromCivil(y, m, 1),
      next = m === 12 ? daysFromCivil(y + 1, 1, 1) : daysFromCivil(y, m + 1, 1);
    day = first + Math.min(date, next - first) - 1;
    validDay(day);
    // PG tm2timestamp validates each calendar stage before a later field can cancel it.
    validTimestamp();
  }
  if (days !== 0) {
    day += days;
    validDay(day);
    // PG tm2timestamp validates each calendar stage before a later field can cancel it.
    validTimestamp();
  }
  const result = BigInt(day) * MICROS_PER_DAY_BIG + time + micros;
  if (result < TIMESTAMP_MIN || result >= TIMESTAMP_END)
    throw new DatetimeOverflowError("timestamp out of range");
  return result;
}
class CalendarAdd implements Compiled {
  readonly #output: FlatVector;
  readonly #validity: Uint32Array;
  readonly #values: BigInt64Array;
  constructor(
    readonly type: PhysicalType,
    readonly fn: string,
    readonly left: Compiled,
    readonly right: Compiled,
    arena: OutputArena,
  ) {
    arena.take(arena.capacity * 8 + 4096);
    this.#values = new BigInt64Array(arena.capacity);
    this.#validity = arena.validity();
    this.#output = {
      form: "flat",
      type,
      length: 0,
      validity: this.#validity,
      data: { layout: "int64", values: { repr: "int64", values: this.#values } },
    };
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const a = this.left.evaluate(batch, rows),
      b = this.right.evaluate(batch, rows),
      ad = flatOf(a).data,
      bd = flatOf(b).data;
    if ((ad.layout !== "i32" && ad.layout !== "int64") || bd.layout !== "interval")
      throw new TypeError("calendar arithmetic needs date/timestamp and interval");
    this.#validity.fill(0);
    const indexes = rowsIn(rows),
      count = countIn(rows, batch.length),
      sign = this.fn === "sub" ? -1 : 1;
    for (let k = 0; k < count; k++) {
      const row = indexes[k] ?? 0;
      if (!isValidSlot(a, row) || !isValidSlot(b, row)) continue;
      const ai = slotIndex(a, row),
        bi = slotIndex(b, row),
        months = bd.months[bi] ?? 0,
        days = bd.days[bi] ?? 0,
        micros = int64At(bd.micros, bi);
      if (isInfiniteInterval(months, days, micros))
        throw new UnsupportedPlanError("infinite INTERVAL arithmetic", { construct: "function" });
      if (
        sign < 0 &&
        (months === -2147483648 || days === -2147483648 || micros === -9223372036854775808n)
      )
        throw new DatetimeOverflowError("interval out of range");
      const value =
        ad.layout === "int64"
          ? int64At(ad.values, ai)
          : ad.values[ai] === DATE_NEG_INFINITY
            ? TIMESTAMP_NEG_INFINITY
            : ad.values[ai] === DATE_POS_INFINITY
              ? TIMESTAMP_POS_INFINITY
              : BigInt(ad.values[ai] ?? 0) * MICROS_PER_DAY_BIG;
      this.#values[row] = addCalendar(value, sign * months, sign * days, BigInt(sign) * micros);
      this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
    }
    this.#output.length = batch.length;
    return this.#output;
  }
}
export function compileCalendarAdd(
  type: PhysicalType,
  fn: string,
  left: Compiled,
  right: Compiled,
  arena: OutputArena,
): Compiled {
  if (
    type.kind !== "timestamp" ||
    (left.type.kind !== "date" && left.type.kind !== "timestamp") ||
    right.type.kind !== "interval" ||
    (fn !== "add" && fn !== "sub")
  )
    throw new TypeError("Calendar arithmetic needs date/timestamp and interval");
  return new CalendarAdd(type, fn, left, right, arena);
}
