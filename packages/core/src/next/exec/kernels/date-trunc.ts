/** timestamp[_tz] DATE_TRUNC, PostgreSQL timestamp.c; the session zone is fixed UTC (C4).
 * Unit lookup follows datetime.c's DecodeUnits (10-byte tokens). Calendar math stays exact in
 * PostgreSQL epoch days/µs; no JS Date or millisecond round trip, including BC and infinities.
 */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import {
  MICROS_PER_DAY_BIG,
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  civilFromDays,
  daysFromCivil,
} from "../../temporal.js";
import { DatetimeOverflowError, ExecutionError } from "../errors.js";
import {
  countIn,
  flatOf,
  isValidSlot,
  rowsIn,
  slotIndex,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

type Unit =
  | "microsecond"
  | "millisecond"
  | "second"
  | "minute"
  | "hour"
  | "day"
  | "week"
  | "month"
  | "quarter"
  | "year"
  | "decade"
  | "century"
  | "millennium"
  | "unsupported";
const TOKENS: ReadonlyMap<string, Unit> = new Map<string, Unit>(
  (
    [
      ["microsecond", ["microsecon", "us", "usec", "useconds", "usecs"]],
      ["millisecond", ["millisecon", "ms", "msec", "msecond", "mseconds", "msecs"]],
      ["second", ["s", "sec", "second", "seconds", "secs"]],
      ["minute", ["m", "min", "mins", "minute", "minutes"]],
      ["hour", ["h", "hour", "hours", "hr", "hrs"]],
      ["day", ["d", "day", "days"]],
      ["week", ["w", "week", "weeks"]],
      ["month", ["mon", "mons", "month", "months"]],
      ["quarter", ["qtr", "quarter"]],
      ["year", ["y", "year", "years", "yr", "yrs"]],
      ["decade", ["dec", "decade", "decades", "decs"]],
      ["century", ["c", "cent", "centuries", "century"]],
      ["millennium", ["mil", "millennia", "millennium", "mils"]],
      ["unsupported", ["timezone", "timezone_h", "timezone_m"]],
    ] satisfies Array<[Unit, readonly string[]]>
  ).flatMap(([unit, tokens]) => tokens.map((token) => [token, unit] as const)),
);
const WIDTH: Partial<Record<Unit, bigint>> = {
  microsecond: 1n,
  millisecond: 1000n,
  second: 1000000n,
  minute: 60000000n,
  hour: 3600000000n,
  day: MICROS_PER_DAY_BIG,
};
function floor(value: bigint, width: bigint): bigint {
  const rest = value % width;
  return value - rest - (rest < 0n ? width : 0n);
}
function truncate(value: bigint, unit: Unit): bigint {
  if (value === TIMESTAMP_POS_INFINITY || value === TIMESTAMP_NEG_INFINITY) return value;
  const width = WIDTH[unit];
  if (width !== undefined) return floor(value, width);
  let days = Number(floor(value, MICROS_PER_DAY_BIG) / MICROS_PER_DAY_BIG);
  if (unit === "week") days -= (((days + 5) % 7) + 7) % 7;
  else {
    let [year, month] = civilFromDays(days);
    switch (unit) {
      case "millennium":
        year = Math.floor((year - 1) / 1000) * 1000 + 1;
        month = 1;
        break;
      case "century":
        year = Math.floor((year - 1) / 100) * 100 + 1;
        month = 1;
        break;
      case "decade":
        year = Math.floor(year / 10) * 10;
        month = 1;
        break;
      case "year":
        month = 1;
        break;
      case "quarter":
        month = Math.floor((month - 1) / 3) * 3 + 1;
        break;
    }
    days = daysFromCivil(year, month, 1);
  }
  const result = BigInt(days) * MICROS_PER_DAY_BIG;
  if (result < TIMESTAMP_MIN || result >= TIMESTAMP_END)
    throw new DatetimeOverflowError("timestamp out of range");
  return result;
}
class DateTrunc implements Compiled {
  readonly #output: FlatVector;
  readonly #validity: Uint32Array;
  readonly #values: BigInt64Array;
  constructor(
    readonly type: PhysicalType,
    readonly field: Compiled,
    readonly input: Compiled,
    arena: OutputArena,
  ) {
    // One fixed int64 output, plus bounded token/calendar/BigInt scratch (no retained per-row
    // state). Read at most ten ASCII bytes; even a huge field never allocates a huge string.
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
  evaluate(batch: Batch, selection: Selection | null): Vector {
    const field = this.field.evaluate(batch, selection),
      input = this.input.evaluate(batch, selection);
    const units = flatOf(field).data,
      values = flatOf(input).data;
    if (units.layout !== "varlen" || values.layout !== "int64")
      throw new TypeError("DATE_TRUNC needs text and timestamp");
    this.#validity.fill(0);
    const active = rowsIn(selection),
      count = countIn(selection, batch.length);
    for (let k = 0; k < count; k++) {
      const row = active[k] ?? 0;
      if (!isValidSlot(field, row) || !isValidSlot(input, row)) continue;
      const u = slotIndex(field, row),
        v = slotIndex(input, row);
      const start = units.offsets[u] ?? 0,
        end = Math.min(units.offsets[u + 1] ?? 0, start + 10);
      let token = "";
      for (let at = start; at < end; at++) {
        const byte = units.bytes[at] ?? 0;
        if (byte === 0) break;
        token += String.fromCharCode(byte >= 65 && byte <= 90 ? byte + 32 : byte);
      }
      const unit = TOKENS.get(token);
      if (unit === undefined || unit === "unsupported") {
        const known = unit !== undefined;
        throw new ExecutionError(
          `unit "${token}" ${known ? "not supported" : "not recognized"} for type ${this.type.kind === "timestamp" && this.type.zoned ? "timestamp with time zone" : "timestamp without time zone"}`,
          "MINNOW_DATETIME_UNIT",
          known ? "0A000" : "22023",
        );
      }
      const micros =
        values.values.repr === "safe"
          ? BigInt(values.values.values[v] ?? 0)
          : (values.values.values[v] ?? 0n);
      this.#values[row] = truncate(micros, unit);
      this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
    }
    this.#output.length = batch.length;
    return this.#output;
  }
}
export function compileDateTrunc(
  type: PhysicalType,
  field: Compiled,
  input: Compiled,
  arena: OutputArena,
): Compiled {
  if (type.kind !== "timestamp" || input.type.kind !== "timestamp" || field.type.kind !== "text")
    throw new TypeError("DATE_TRUNC needs text and timestamp");
  return new DateTrunc(type, field, input, arena);
}
