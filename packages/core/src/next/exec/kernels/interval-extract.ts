/** PostgreSQL interval_part/extract_interval: exact NUMERIC fields and display scale. */
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import { setInt128 } from "../../vector/int128.js";
import { isInfiniteInterval } from "../../temporal-text.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import {
  countIn,
  rowsIn,
  flatOf,
  slotIndex,
  isValidSlot,
  type Compiled,
  type OutputArena,
} from "./vectors.js";
type Field =
  | "epoch"
  | "year"
  | "month"
  | "day"
  | "hour"
  | "minute"
  | "second"
  | "milliseconds"
  | "microseconds"
  | "decade"
  | "century"
  | "millennium"
  | "quarter"
  | "week"
  | "unsupported";
const units: ReadonlyMap<string, Field> = new Map(
  (
    [
      ["epoch", ["epoch"]],
      ["year", ["year", "years", "y", "yr", "yrs"]],
      ["month", ["month", "months", "mon", "mons"]],
      ["day", ["day", "days", "d"]],
      ["hour", ["hour", "hours", "h", "hr", "hrs"]],
      ["minute", ["minute", "minutes", "m", "min", "mins"]],
      ["second", ["second", "seconds", "s", "sec", "secs"]],
      ["milliseconds", ["millisecon", "ms", "msec", "msecond", "mseconds", "msecs"]],
      ["microseconds", ["microsecon", "us", "usec", "useconds", "usecs"]],
      ["decade", ["decade", "decades", "dec", "decs"]],
      ["century", ["century", "centuries", "cent", "c"]],
      ["millennium", ["millennium", "millennia", "mil", "mils"]],
      ["quarter", ["quarter", "qtr"]],
      ["week", ["week", "weeks", "w"]],
      [
        "unsupported",
        ["dow", "doy", "isodow", "isoyear", "julian", "timezone", "timezone_h", "timezone_m"],
      ],
    ] satisfies Array<[Field, readonly string[]]>
  ).flatMap(([field, names]) => names.map((name) => [name, field] as const)),
);
function extract(
  field: Field,
  months: number,
  days: number,
  micros: bigint,
): readonly [bigint, number] {
  const years = Math.trunc(months / 12);
  switch (field) {
    case "epoch":
      return [
        BigInt(years) * 31557600000000n +
          BigInt(months % 12) * 2592000000000n +
          BigInt(days) * 86400000000n +
          micros,
        6,
      ];
    case "year":
      return [BigInt(years), 0];
    case "month":
      return [BigInt(months % 12), 0];
    case "day":
      return [BigInt(days), 0];
    case "hour":
      return [micros / 3600000000n, 0];
    case "minute":
      return [(micros % 3600000000n) / 60000000n, 0];
    case "second":
      return [micros % 60000000n, 6];
    case "milliseconds":
      return [micros % 60000000n, 3];
    case "microseconds":
      return [micros % 60000000n, 0];
    case "decade":
      return [BigInt(Math.trunc(years / 10)), 0];
    case "century":
      return [BigInt(Math.trunc(years / 100)), 0];
    case "millennium":
      return [BigInt(Math.trunc(years / 1000)), 0];
    case "quarter":
      return [BigInt((months < 0 ? -1 : 1) * (Math.trunc(Math.abs(months % 12) / 3) + 1)), 0];
    case "week":
      return [BigInt(Math.trunc(days / 7)), 0];
    case "unsupported":
      throw new Error("Unsupported field already checked");
  }
}
class IntervalExtract implements Compiled {
  readonly #output: FlatVector;
  readonly #limbs: Uint32Array;
  readonly #scales: Uint16Array;
  readonly #validity: Uint32Array;
  constructor(
    readonly type: PhysicalType,
    readonly field: Compiled,
    readonly input: Compiled,
    arena: OutputArena,
  ) {
    arena.take(arena.capacity * 18 + 4096);
    this.#limbs = new Uint32Array(arena.capacity * 4);
    this.#scales = new Uint16Array(arena.capacity);
    this.#validity = arena.validity();
    this.#output = {
      form: "flat",
      type,
      length: 0,
      validity: this.#validity,
      data: {
        layout: "decimal",
        scale: null,
        scales: this.#scales,
        coefficients: { repr: "int128", limbs: this.#limbs },
        specials: null,
      },
    };
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const field = this.field.evaluate(batch, rows),
      input = this.input.evaluate(batch, rows),
      fd = flatOf(field).data,
      ad = flatOf(input).data;
    if (fd.layout !== "varlen" || ad.layout !== "interval")
      throw new TypeError("INTERVAL EXTRACT needs text and interval");
    this.#validity.fill(0);
    const ids = rowsIn(rows),
      count = countIn(rows, batch.length);
    for (let k = 0; k < count; k++) {
      const row = ids[k] ?? 0;
      if (!isValidSlot(field, row) || !isValidSlot(input, row)) continue;
      const fi = slotIndex(field, row),
        ii = slotIndex(input, row),
        start = fd.offsets[fi] ?? 0,
        end = Math.min(fd.offsets[fi + 1] ?? 0, start + 10);
      let token = "";
      for (let at = start; at < end; at++) {
        const byte = fd.bytes[at] ?? 0;
        if (byte === 0) break;
        token += String.fromCharCode(byte >= 65 && byte <= 90 ? byte + 32 : byte);
      }
      const unit = units.get(token);
      if (unit === undefined || unit === "unsupported")
        throw new ExecutionError(
          `unit "${token}" ${unit === undefined ? "not recognized" : "not supported"} for type interval`,
          "MINNOW_DATETIME_UNIT",
          unit === undefined ? "22023" : "0A000",
        );
      const months = ad.months[ii] ?? 0,
        days = ad.days[ii] ?? 0,
        micros = int64At(ad.micros, ii);
      if (isInfiniteInterval(months, days, micros))
        throw new UnsupportedPlanError("infinite INTERVAL extraction", { construct: "function" });
      const [coefficient, scale] = extract(unit, months, days, micros);
      setInt128(this.#limbs, row, coefficient);
      this.#scales[row] = scale;
      this.#validity[row >>> 5] = (this.#validity[row >>> 5] ?? 0) | (1 << (row & 31));
    }
    this.#output.length = batch.length;
    return this.#output;
  }
}
export function compileIntervalExtract(
  type: PhysicalType,
  field: Compiled,
  input: Compiled,
  arena: OutputArena,
): Compiled {
  if (type.kind !== "decimal" || field.type.kind !== "text" || input.type.kind !== "interval")
    throw new TypeError("INTERVAL EXTRACT needs text and interval");
  return new IntervalExtract(type, field, input, arena);
}
