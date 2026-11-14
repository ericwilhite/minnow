/** Input fixtures are independent of the output formatter under test. */
import type { PhysicalType } from "../types.js";
import type { ArrayElement, ArrayValue } from "../vector/array-value.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { allocateFlat, type FlatVector, type VectorData } from "../vector/vector.js";
import { DATE_NEG_INFINITY, DATE_POS_INFINITY } from "../temporal.js";
export interface OutputShape {
  family: number;
  n: number;
  scale: number;
  sign: boolean;
  nil: boolean;
  scope: number;
  bits: bigint;
}
export interface OutputCase {
  readonly type: PhysicalType;
  readonly sqlType: string;
  readonly value: ArrayElement | null;
  readonly input: string | null;
}
const encoder = new TextEncoder();
const fraction = (n: number) => String(Math.abs(n) % 1000000).padStart(6, "0");
export function outputCase(s: OutputShape): OutputCase {
  const n = s.n,
    a = Math.abs(n),
    day = (a % 27) + 1,
    hour = a % 24,
    minute = a % 60,
    second = (a * 7) % 60,
    micro = a % 1000000,
    date = `2000-01-${String(day).padStart(2, "0")}`,
    time = `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}.${fraction(micro)}`;
  let type: PhysicalType, sqlType: string, value: ArrayElement, input: string;
  switch (s.family) {
    case 0:
      type = { kind: "int32", bits: 16 };
      sqlType = "smallint";
      value = n;
      input = String(n);
      break;
    case 1:
      type = { kind: "int32", bits: 32 };
      sqlType = "integer";
      value = n * 1000003;
      input = String(value);
      break;
    case 2:
      type = { kind: "int64" };
      sqlType = "bigint";
      value =
        s.scale === 0
          ? 9223372036854775807n
          : s.scale === 1
            ? -9223372036854775808n
            : (s.sign ? -1n : 1n) * (9007199254740992n + BigInt(a));
      input = String(value);
      break;
    case 3: {
      type = { kind: "decimal", precision: null, scale: null };
      sqlType = "numeric";
      const coefficient = BigInt(n) * (s.scale === 12 ? 10n ** 256n + 1n : 1n),
        scale = s.scale,
        digits = String(coefficient < 0n ? -coefficient : coefficient).padStart(scale + 1, "0");
      value = { coefficient, scale };
      input =
        (coefficient < 0n ? "-" : "") +
        (scale ? digits.slice(0, -scale) + "." + digits.slice(-scale) : digits);
      break;
    }
    case 4:
    case 5: {
      const bits = new DataView(new ArrayBuffer(8));
      bits.setBigUint64(0, s.bits);
      type = { kind: s.family === 4 ? "float32" : "float64" };
      sqlType = s.family === 4 ? "real" : "double precision";
      value = s.family === 4 ? bits.getFloat32(4) : bits.getFloat64(0);
      input = Object.is(value, -0) ? "-0" : String(value);
      break;
    }
    case 6:
      type = { kind: "bool" };
      sqlType = "boolean";
      value = s.sign;
      input = value ? "true" : "false";
      break;
    case 7:
      type = { kind: "date" };
      sqlType = "date";
      value = s.scale === 0 ? DATE_POS_INFINITY : s.scale === 1 ? DATE_NEG_INFINITY : day - 1;
      input = s.scale === 0 ? "infinity" : s.scale === 1 ? "-infinity" : date;
      break;
    case 8:
      type = { kind: "time" };
      sqlType = "time";
      value =
        s.scale === 0
          ? 86400000000n
          : BigInt(hour * 3600 + minute * 60 + second) * 1000000n + BigInt(micro);
      input = s.scale === 0 ? "24:00:00" : time;
      break;
    case 9:
    case 10:
      type = { kind: "timestamp", zoned: s.family === 10 };
      sqlType = s.family === 10 ? "timestamptz" : "timestamp";
      value =
        s.scale === 0
          ? 9223372036854775807n
          : s.scale === 1
            ? -9223372036854775808n
            : BigInt((day - 1) * 86400 + hour * 3600 + minute * 60 + second) * 1000000n +
              BigInt(micro);
      input =
        s.scale === 0
          ? "infinity"
          : s.scale === 1
            ? "-infinity"
            : `${date} ${time}${s.family === 10 ? "+00" : ""}`;
      break;
    case 11:
      type = { kind: "interval" };
      sqlType = "interval";
      value =
        s.scale === 0
          ? { months: 2147483647, days: 2147483647, micros: 9223372036854775807n }
          : s.scale === 1
            ? { months: -2147483648, days: -2147483648, micros: -9223372036854775808n }
            : { months: n, days: s.sign ? -a % 7 : a % 7, micros: BigInt(n * 1000001) };
      input =
        s.scale === 0
          ? "infinity"
          : s.scale === 1
            ? "-infinity"
            : `${String(value.months)} months ${String(value.days)} days ${String(value.micros)} microseconds`;
      break;
    case 12: {
      type = { kind: "uuid" };
      sqlType = "uuid";
      const hex = s.bits.toString(16).padStart(32, "0");
      input = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
      value = Uint8Array.from({ length: 16 }, (_, i) =>
        Number.parseInt(hex.slice(2 * i, 2 * i + 2), 16),
      );
      break;
    }
    case 13:
    case 14:
      type = { kind: "json", binary: s.family === 14 };
      sqlType = s.family === 14 ? "jsonb" : "json";
      input =
        s.family === 14
          ? `{"n": 1.200, "text": "汉${String(n)}"}`
          : ` { "text":"汉${String(n)}", "n":1.200 } `;
      value = encoder.encode(input);
      break;
    default: {
      type = { kind: "array", element: { kind: "text" } };
      sqlType = "text[]";
      input = `[0:1]={"v${String(n)}",NULL}`;
      value = encodeArrayValue(type.element, {
        dimensions: [2],
        lowerBounds: [0],
        elements: [encoder.encode(`v${String(n)}`), null],
      });
      break;
    }
  }
  return { type, sqlType, value: s.nil ? null : value, input: s.nil ? null : input };
}
export function outputVector(type: PhysicalType, value: ArrayElement | null): FlatVector {
  if (value === null) return { ...allocateFlat(type, 1), length: 1, validity: new Uint32Array(1) };
  let data: VectorData;
  switch (type.kind) {
    case "bool":
      data = { layout: "bool", values: Uint8Array.of(value ? 1 : 0) };
      break;
    case "int32":
    case "date":
      data = { layout: "i32", values: Int32Array.of(value as number) };
      break;
    case "float32":
      data = { layout: "f32", values: Float32Array.of(value as number) };
      break;
    case "float64":
      data = { layout: "f64", values: Float64Array.of(value as number) };
      break;
    case "int64":
    case "time":
    case "timestamp":
      data = {
        layout: "int64",
        values: { repr: "int64", values: BigInt64Array.of(value as bigint) },
      };
      break;
    case "decimal": {
      const v = value as Extract<ArrayElement, { coefficient: bigint }>;
      data = {
        layout: "decimal",
        scale: v.scale,
        scales: null,
        coefficients: { repr: "wide", values: [v.coefficient] },
        specials: v.special
          ? Uint8Array.of(v.special === "NaN" ? 1 : v.special === "Infinity" ? 2 : 3)
          : null,
      };
      break;
    }
    case "interval": {
      const v = value as Extract<ArrayElement, { months: number }>;
      data = {
        layout: "interval",
        months: Int32Array.of(v.months),
        days: Int32Array.of(v.days),
        micros: { repr: "int64", values: BigInt64Array.of(v.micros) },
      };
      break;
    }
    case "uuid":
      data = { layout: "fixed16", bytes: value as Uint8Array };
      break;
    default: {
      const bytes = value as Uint8Array;
      data = { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) };
    }
  }
  return { form: "flat", type, length: 1, validity: null, data };
}
/** Quote every non-NULL input element, independently of PG's minimal array_out quoting. */
export function arrayInput(value: ArrayValue, elements: ReadonlyArray<string | null>): string {
  if (!value.dimensions.length) return "{}";
  let at = 0;
  const nest = (d: number): string =>
    "{" +
    Array.from({ length: value.dimensions[d] ?? 0 }, () =>
      d + 1 === value.dimensions.length
        ? elements[at++] === null
          ? "NULL"
          : '"' + (elements[at - 1] ?? "").replace(/["\\]/gu, (c) => "\\" + c) + '"'
        : nest(d + 1),
    ).join(",") +
    "}";
  return (
    value.dimensions
      .map(
        (n, d) =>
          `[${String(value.lowerBounds[d])}:${String((value.lowerBounds[d] ?? 1) + n - 1)}]`,
      )
      .join("") +
    "=" +
    nest(0)
  );
}
