/** Independent PG input construction for temporal kernel differentials (never output parsers). */
import type { PhysicalType } from "../../types.js";
import {
  MICROS_PER_DAY_BIG,
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
} from "../../temporal.js";
import { int64At } from "../../vector/int64.js";
import type { FlatVector, Vector } from "../../vector/vector.js";
import { flatOf, slotIndex, isValidSlot } from "./vectors.js";
import { bigints, constant, dictionary, numbers, I32 } from "./test-support.js";
export interface IntervalValue {
  months: number;
  days: number;
  micros: bigint;
}
export type TemporalValue = number | bigint | IntervalValue | null;
export const TEMPORAL_TYPES: readonly PhysicalType[] = [
  { kind: "date" },
  { kind: "timestamp", zoned: false },
  { kind: "timestamp", zoned: true },
  { kind: "interval" },
];
export function temporalVector(
  type: PhysicalType,
  values: readonly TemporalValue[],
  safe: boolean,
  form: number,
): Vector {
  let vector: FlatVector;
  if (type.kind === "interval") {
    const data = values.map((v) =>
      v !== null && typeof v === "object"
        ? v
        : { months: 2147483647, days: 2147483647, micros: 9223372036854775807n },
    );
    vector = {
      form: "flat",
      type,
      length: values.length,
      validity: numbers(
        I32,
        values.map((v) => (v === null ? null : 1)),
      ).validity,
      data: {
        layout: "interval",
        months: Int32Array.from(data.map((v) => v.months)),
        days: Int32Array.from(data.map((v) => v.days)),
        micros: { repr: "int64", values: BigInt64Array.from(data.map((v) => v.micros)) },
      },
    };
  } else if (type.kind === "date" || safe)
    vector = numbers(
      type,
      values.map((v) => (v === null ? null : Number(v))),
      2147483646,
    );
  else
    vector = bigints(
      type,
      values.map((v) => (v === null ? null : (v as bigint))),
    );
  if (form === 2) return constant({ ...vector, length: 1 }, values.length);
  if (form === 1)
    return dictionary(
      vector,
      values.map((v, i) => (v === null ? null : i)),
    );
  return vector;
}
export function temporalValue(vector: Vector, row: number): TemporalValue {
  if (!isValidSlot(vector, row)) return null;
  const data = flatOf(vector).data,
    slot = slotIndex(vector, row);
  if (data.layout === "i32") return data.values[slot] ?? 0;
  if (data.layout === "int64") return int64At(data.values, slot);
  if (data.layout === "interval")
    return {
      months: data.months[slot] ?? 0,
      days: data.days[slot] ?? 0,
      micros: int64At(data.micros, slot),
    };
  throw new Error("Expected temporal vector");
}
export function pgTemporal(type: PhysicalType, value: TemporalValue): string {
  const name = type.kind === "timestamp" ? (type.zoned ? "timestamptz" : "timestamp") : type.kind;
  if (value === null) return `NULL::${name}`;
  if (type.kind === "interval") {
    if (typeof value !== "object") throw new Error("Expected interval parts");
    return `INTERVAL '${String(value.months)} months ${String(value.days)} days ${value.micros.toString()} microseconds'`;
  }
  if (typeof value === "object") throw new Error("Expected date/timestamp value");
  const n = BigInt(value);
  if (n === BigInt(type.kind === "date" ? DATE_POS_INFINITY : TIMESTAMP_POS_INFINITY))
    return `'infinity'::${name}`;
  if (n === BigInt(type.kind === "date" ? DATE_NEG_INFINITY : TIMESTAMP_NEG_INFINITY))
    return `'-infinity'::${name}`;
  if (type.kind === "date") return `(DATE '2000-01-01'+${n.toString()}::int4)`;
  let days = n / MICROS_PER_DAY_BIG,
    rest = n % MICROS_PER_DAY_BIG;
  if (rest < 0n) {
    days -= 1n;
    rest += MICROS_PER_DAY_BIG;
  }
  // Integer interval fields avoid text formatting and float8 epoch precision loss.
  return `(${name} '2000-01-01' + INTERVAL '${days.toString()} days ${rest.toString()} microseconds')`;
}
