/**
 * Builders for kernel tests and benches: vectors of every layout, IR expressions, and a compiled
 * evaluation over one batch. Test-only (imported by *.test.ts and scripts/ benches).
 */
import type { ScalarExpr } from "../../ir/ir.js";
import { int128At, setInt128 } from "../../vector/int128.js";
import type { PhysicalType } from "../../types.js";
import type { Batch, FlatVector, Selection, Vector } from "../../vector/vector.js";
import { compileExpr, type ParameterSlots } from "./compile.js";
import { OutputArena, type Compiled } from "./vectors.js";

export const BOOL: PhysicalType = { kind: "bool" };
export const I16: PhysicalType = { kind: "int32", bits: 16 };
export const I32: PhysicalType = { kind: "int32", bits: 32 };
export const I64: PhysicalType = { kind: "int64" };
export const F64: PhysicalType = { kind: "float64" };
export const F32: PhysicalType = { kind: "float32" };
export const TEXT: PhysicalType = { kind: "text" };
export const DATE: PhysicalType = { kind: "date" };
export const TS: PhysicalType = { kind: "timestamp", zoned: false };
export const decimal = (precision: number | null, scale: number | null): PhysicalType => ({
  kind: "decimal",
  precision,
  scale,
});

function int128Limbs(values: readonly bigint[]): Uint32Array {
  const limbs = new Uint32Array(4 * values.length);
  values.forEach((value, index) => {
    setInt128(limbs, index, value);
  });
  return limbs;
}

export const UNCHARGED = { allocate: () => true, free: () => undefined };

function validityOf(nulls: readonly boolean[]): Uint32Array | null {
  if (!nulls.some(Boolean)) return null;
  const validity = new Uint32Array((nulls.length + 31) >>> 5);
  nulls.forEach((isNull, row) => {
    if (!isNull) validity[row >>> 5] = (validity[row >>> 5] ?? 0) | (1 << (row & 31));
  });
  return validity;
}

/** Null slots hold `garbage` (a value a kernel must not raise on). */
export function numbers(
  type: PhysicalType,
  values: ReadonlyArray<number | null>,
  garbage = 0,
): FlatVector {
  const nulls = values.map((value) => value === null);
  const raw = values.map((value) => value ?? garbage);
  let data: FlatVector["data"];
  switch (type.kind) {
    case "bool":
      data = { layout: "bool", values: Uint8Array.from(raw) };
      break;
    case "int32":
    case "date":
      data = { layout: "i32", values: Int32Array.from(raw) };
      break;
    case "float64":
      data = { layout: "f64", values: Float64Array.from(raw) };
      break;
    case "float32":
      data = { layout: "f32", values: Float32Array.from(raw) };
      break;
    case "int64":
    case "timestamp":
    case "time":
      data = { layout: "int64", values: { repr: "safe", values: Float64Array.from(raw) } };
      break;
    default:
      throw new TypeError(`numbers(): ${type.kind}`);
  }
  return { form: "flat", type, length: values.length, validity: validityOf(nulls), data };
}

export function bigints(type: PhysicalType, values: ReadonlyArray<bigint | null>): FlatVector {
  return {
    form: "flat",
    type,
    length: values.length,
    validity: validityOf(values.map((value) => value === null)),
    data: {
      layout: "int64",
      values: { repr: "int64", values: BigInt64Array.from(values.map((value) => value ?? 0n)) },
    },
  };
}

export type DecimalValue =
  readonly [coefficient: bigint, scale: number] | "NaN" | "Infinity" | "-Infinity" | null;

/**
 * Decimal vector: `uniform` scale or per-slot; tier `safe` (when every coefficient fits), `int64`
 * or `wide`.
 */
export function decimals(
  type: PhysicalType,
  values: readonly DecimalValue[],
  tier: "safe" | "int64" | "int128" | "wide" = "safe",
  uniform: number | null = null,
): FlatVector {
  const nulls = values.map((value) => value === null);
  const coefficients = values.map((value) => (Array.isArray(value) ? (value[0] as bigint) : 0n));
  const scales = values.map((value) => (Array.isArray(value) ? (value[1] as number) : 0));
  if (tier === "safe" && coefficients.some((c) => c > 2n ** 53n - 1n || c < -(2n ** 53n - 1n)))
    throw new RangeError("decimals(): a coefficient does not fit the safe tier");
  const specialCodes = values.map((value) =>
    value === "NaN" ? 1 : value === "Infinity" ? 2 : value === "-Infinity" ? 3 : 0,
  );
  return {
    form: "flat",
    type,
    length: values.length,
    validity: validityOf(nulls),
    data: {
      layout: "decimal",
      scale: uniform,
      scales: uniform === null ? Uint16Array.from(scales) : null,
      coefficients:
        tier === "safe"
          ? { repr: "safe", values: Float64Array.from(coefficients.map(Number)) }
          : tier === "int64"
            ? { repr: "int64", values: BigInt64Array.from(coefficients) }
            : tier === "int128"
              ? { repr: "int128", limbs: int128Limbs(coefficients) }
              : { repr: "wide", values: coefficients },
      specials: specialCodes.some((code) => code !== 0) ? Uint8Array.from(specialCodes) : null,
    },
  };
}

const encoder = new TextEncoder();

export function texts(values: ReadonlyArray<string | null>, type: PhysicalType = TEXT): FlatVector {
  const encoded = values.map((value) => encoder.encode(value ?? ""));
  const bytes = new Uint8Array(encoded.reduce((sum, value) => sum + value.length, 0));
  const offsets = new Int32Array(values.length + 1);
  encoded.forEach((value, row) => {
    bytes.set(value, offsets[row] ?? 0);
    offsets[row + 1] = (offsets[row] ?? 0) + value.length;
  });
  return {
    form: "flat",
    type,
    length: values.length,
    validity: validityOf(values.map((value) => value === null)),
    data: { layout: "varlen", bytes, offsets },
  };
}

let dictionaryIds = 1_000_000;

export function dictionary(entries: FlatVector, codes: ReadonlyArray<number | null>): Vector {
  return {
    form: "dictionary",
    type: entries.type,
    length: codes.length,
    codes: Int32Array.from(codes.map((code) => code ?? 0)),
    validity: validityOf(codes.map((code) => code === null)),
    dictionary: entries,
    dictionaryId: dictionaryIds++,
  };
}

export function constant(value: FlatVector, length: number): Vector {
  return { form: "constant", type: value.type, length, value };
}

export const col = (index: number, type: PhysicalType): ScalarExpr => ({
  op: "column",
  index,
  type,
});
export const lit = (value: FlatVector): ScalarExpr => ({ op: "literal", value, type: value.type });
export const call = (fn: string, type: PhysicalType, ...args: ScalarExpr[]): ScalarExpr => ({
  op: "call",
  fn,
  args,
  type,
});
export const cmp = (
  op: "=" | "<>" | "<" | "<=" | ">" | ">=",
  left: ScalarExpr,
  right: ScalarExpr,
): ScalarExpr => ({ op: "compare", cmp: op, left, right, collation: null, type: BOOL });

export function batchOf(columns: readonly Vector[], selection: Selection | null = null): Batch {
  return { length: columns[0]?.length ?? 0, columns, selection };
}

/** Compiles `expr` over `batch`'s column types. */
export function compileFor(
  expr: ScalarExpr,
  types: readonly PhysicalType[],
  params: ParameterSlots = { slots: [] },
  capacity = 2048,
): Compiled {
  return compileExpr(expr, { arena: new OutputArena(UNCHARGED, capacity), params, input: types });
}

/** Evaluates `expr` over `batch` and reads every active row as a JS value. */
export function evaluate(expr: ScalarExpr, batch: Batch, params?: ParameterSlots): unknown[] {
  const compiled = compileFor(
    expr,
    batch.columns.map((column) => column.type),
    params,
  );
  const vector = compiled.evaluate(batch, batch.selection);
  const rows =
    batch.selection === null
      ? Array.from({ length: batch.length }, (_, k) => k)
      : Array.from(batch.selection.rows.subarray(0, batch.selection.count));
  return rows.map((row) => valueAt(vector, row));
}

const decoder = new TextDecoder();

/** A JS view of one slot: numbers, bigints, strings, [coefficient, scale] or a special, or null. */
export function valueAt(vector: Vector, row: number): unknown {
  let flat: FlatVector;
  let slot: number;
  if (vector.form === "constant") {
    flat = vector.value;
    slot = 0;
  } else if (vector.form === "dictionary") {
    if (vector.validity !== null && ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
      return null;
    flat = vector.dictionary;
    slot = vector.codes[row] ?? 0;
  } else {
    flat = vector;
    slot = row;
  }
  if (flat.validity !== null && ((flat.validity[slot >>> 5] ?? 0) & (1 << (slot & 31))) === 0)
    return null;
  const data = flat.data;
  switch (data.layout) {
    case "bool":
      return data.values[slot] === 1;
    case "i32":
    case "f32":
    case "f64":
      return data.values[slot];
    case "int64":
      return data.values.repr === "safe"
        ? BigInt(data.values.values[slot] ?? 0)
        : data.values.values[slot];
    case "decimal": {
      const special = data.specials?.[slot] ?? 0;
      if (special !== 0) return special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
      const scale = data.scale ?? data.scales?.[slot] ?? 0;
      const c = data.coefficients;
      const coefficient =
        c.repr === "safe"
          ? BigInt(c.values[slot] ?? 0)
          : c.repr === "int128"
            ? int128At(c.limbs, slot)
            : (c.values[slot] ?? 0n);
      return [coefficient, scale];
    }
    case "varlen":
      return decoder.decode(
        data.bytes.subarray(data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0),
      );
    default:
      throw new TypeError(`valueAt: ${data.layout}`);
  }
}

/** Normalizes a decimal to its value (trailing zeros dropped) for value comparison. */
export function decimalValue(value: unknown): string {
  if (!Array.isArray(value)) return String(value);
  let [c, s] = value as [bigint, number];
  while (s > 0 && c % 10n === 0n) {
    c /= 10n;
    s -= 1;
  }
  return `${c.toString()}e-${String(s)}`;
}

/** A value as the PGlite differentials write it (sqlLiteral). */
export type SqlValue = number | bigint | string | boolean | readonly [bigint, number] | null;

/**
 * A PostgreSQL literal of `value` cast to `type` (int2, int4, int8, float4, float8, numeric,
 * numeric(p,s), text, bool), for the differential suites; a string is text, or a NUMERIC or
 * float special ("NaN", "Infinity", "-Infinity"). Every value is quoted, then cast:
 * unquoted, `-2147483648::int4` casts 2147483648 before the minus and fails with 22003, a
 * PostgreSQL error from the literal that a suite comparing SQLSTATEs would take for the
 * operation's answer. Text doubles its apostrophes. NUMERIC is [coefficient, scale]; floats keep
 * −0.
 */
export function sqlLiteral(type: string, value: SqlValue): string {
  if (value === null) return `null::${type}`;
  if (Array.isArray(value)) {
    const [c, s] = value as readonly [bigint, number];
    const negative = c < 0n;
    const digits = (negative ? -c : c).toString().padStart(s + 1, "0");
    const text =
      s === 0 ? digits : `${digits.slice(0, digits.length - s)}.${digits.slice(digits.length - s)}`;
    return `'${negative ? "-" : ""}${text}'::${type}`;
  }
  if (typeof value === "number" && Object.is(value, -0)) return `'-0'::${type}`;
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'::${type}`;
  return `'${String(value)}'::${type}`;
}
