/**
 * Physical types of the new core: the one type system storage pages, log frames, spill pages,
 * vectors and executor IR share.
 *
 * Contract:
 * - A physical type fixes the in-memory layout (`layoutOf`) and the stored encoding family. Two
 *   values of one physical type compare and hash by the rules of that type alone; collation is a
 *   property of an expression, never of a stored type.
 * - SQL types map to physical types in exactly one place, `physicalTypeOf`. Nothing else
 *   inspects `SimpleDataType`, `integer`, `sqlDomain` or `withoutTimeZone`.
 * - Every int64-family value (int64, time, timestamp, decimal coefficients) is exact. The f64
 *   layout is a representation of an int64 vector whose values are all proven to lie in
 *   ±(2^53 − 1), never an approximation (see vector/int64.ts).
 * - There are no tagged strings. A DATE is int32 days, a NUMERIC is a scaled coefficient, an
 *   INTERVAL is three fields.
 */
import type { SimpleDataType, SqlDomain } from "../storage/types.js";

export interface BoolType {
  readonly kind: "bool";
}
/** SMALLINT and INTEGER. `bits` is the SQL range that writes and arithmetic check. */
export interface Int32Type {
  readonly kind: "int32";
  readonly bits: 16 | 32;
}
/** BIGINT. */
export interface Int64Type {
  readonly kind: "int64";
}
/** DOUBLE PRECISION, and REAL (stored as float64; see DECISIONS.md). */
export interface Float64Type {
  readonly kind: "float64";
}
/**
 * NUMERIC. Values are a coefficient scaled by 10^scale plus a per-value display scale.
 * `precision`/`scale` null means unconstrained, as in PostgreSQL.
 */
export interface DecimalType {
  readonly kind: "decimal";
  readonly precision: number | null;
  readonly scale: number | null;
}
/** DATE: int32 days since 1970-01-01 (proleptic Gregorian). */
export interface DateType {
  readonly kind: "date";
}
/** TIME (without time zone): microseconds since midnight, 0 … 86_400_000_000. */
export interface TimeType {
  readonly kind: "time";
}
/**
 * TIMESTAMP / TIMESTAMPTZ: int64 microseconds since 1970-01-01T00:00:00Z. The stored instant is
 * the same for both; `zoned` changes rendering and casts only (there is no session time zone).
 */
export interface TimestampType {
  readonly kind: "timestamp";
  readonly zoned: boolean;
}
/** INTERVAL: months (int32), days (int32), microseconds (int64), PostgreSQL's three fields. */
export interface IntervalType {
  readonly kind: "interval";
}
/** TEXT / VARCHAR / CHAR: UTF-8 bytes. Equality and hashing are byte equality. */
export interface TextType {
  readonly kind: "text";
}
/** Arbitrary bytes. Reserved: the current front end has no BYTEA. */
export interface BytesType {
  readonly kind: "bytes";
}
/** JSON (`binary: false`) keeps its text; JSONB (`binary: true`) stores its canonical text. */
export interface JsonType {
  readonly kind: "json";
  readonly binary: boolean;
}
/** UUID: 16 bytes, compared as bytes (PostgreSQL's order). */
export interface UuidType {
  readonly kind: "uuid";
}
/**
 * An enum: stored as the label's UTF-8 bytes (dictionary pages make that a code), ordered by
 * `labels` position. Labels are stored text, so adding a label never rewrites data.
 */
export interface EnumType {
  readonly kind: "enum";
  readonly name: string;
  readonly labels: readonly string[];
}
/** An array of `element`: one canonical binary value per row (see DECISIONS.md, item C9). */
export interface ArrayType {
  readonly kind: "array";
  readonly element: PhysicalType;
}

export type PhysicalType =
  | BoolType
  | Int32Type
  | Int64Type
  | Float64Type
  | DecimalType
  | DateType
  | TimeType
  | TimestampType
  | IntervalType
  | TextType
  | BytesType
  | JsonType
  | UuidType
  | EnumType
  | ArrayType;

export type PhysicalKind = PhysicalType["kind"];

/**
 * The in-memory layout family of a type (vector/vector.ts defines each one):
 * - `bool`: Uint8Array, 0 or 1.
 * - `i32`: Int32Array.
 * - `int64`: exact 64-bit integers, either proven-safe Float64Array or BigInt64Array.
 * - `f64`: Float64Array of IEEE doubles.
 * - `decimal`: int64-family coefficients (or wide bigints) at one vector scale.
 * - `varlen`: UTF-8 / raw bytes plus Int32Array offsets.
 * - `fixed16`: 16 bytes per value.
 * - `interval`: months, days (Int32Array) and microseconds (int64-family).
 */
export type Layout =
  "bool" | "i32" | "int64" | "f64" | "decimal" | "varlen" | "fixed16" | "interval";

export function layoutOf(type: PhysicalType): Layout {
  switch (type.kind) {
    case "bool":
      return "bool";
    case "int32":
    case "date":
      return "i32";
    case "int64":
    case "time":
    case "timestamp":
      return "int64";
    case "float64":
      return "f64";
    case "decimal":
      return "decimal";
    case "text":
    case "bytes":
    case "json":
    case "enum":
    case "array":
      return "varlen";
    case "uuid":
      return "fixed16";
    case "interval":
      return "interval";
  }
}

/** Value range limits the type checks on write and in arithmetic. */
export const INT16_MIN = -32_768;
export const INT16_MAX = 32_767;
export const INT32_MIN = -2_147_483_648;
export const INT32_MAX = 2_147_483_647;
export const INT64_MIN = -(2n ** 63n);
export const INT64_MAX = 2n ** 63n - 1n;
export const MICROS_PER_DAY = 86_400_000_000;
/** PostgreSQL's NUMERIC limits: 131,072 digits before the point, 16,383 after. */
export const NUMERIC_MAX_SCALE = 16_383;

/**
 * The front end's description of one column or typed expression, as the catalog and the
 * compiler carry it today, plus the declared SQL type name, which the current front end drops.
 *
 * `declaredTypeName` is required for exactness: today INTEGER, SMALLINT and BIGINT all compile to
 * `{ type: "number", integer: true }`, and REAL and DOUBLE to `{ type: "number" }`. Without the
 * name an integer column maps to int64 (it never narrows a value the old engine accepted).
 */
export interface SqlTypeDescriptor {
  readonly type: SimpleDataType;
  readonly integer?: true;
  readonly sqlDomain?: SqlDomain;
  readonly withoutTimeZone?: true;
  readonly enumValues?: readonly string[];
  readonly declaredTypeName?: string;
}

const INT16_NAMES = new Set(["SMALLINT", "INT2"]);
const INT32_NAMES = new Set(["INTEGER", "INT", "INT4"]);

/**
 * The single SQL → physical type mapping.
 *
 * | SQL                              | physical                         |
 * | -------------------------------- | -------------------------------- |
 * | BOOLEAN                          | bool                             |
 * | SMALLINT / INTEGER               | int32 (bits 16 / 32)             |
 * | BIGINT, integer without a name   | int64                            |
 * | DOUBLE PRECISION, REAL, number   | float64                          |
 * | NUMERIC(p, s)                    | decimal(p, s)                    |
 * | DATE                             | date                             |
 * | TIME                             | time                             |
 * | TIMESTAMP / TIMESTAMPTZ          | timestamp (zoned false / true)   |
 * | INTERVAL                         | interval                         |
 * | TEXT / VARCHAR / CHAR            | text                             |
 * | JSON / JSONB                     | json (binary false / true)       |
 * | UUID                             | uuid                             |
 * | enum type, string with enumValues| enum                             |
 * | element[]                        | array(element)                   |
 */
export function physicalTypeOf(descriptor: SqlTypeDescriptor): PhysicalType {
  const domain = descriptor.sqlDomain;
  if (domain !== undefined) {
    switch (domain.kind) {
      case "numeric":
        return {
          kind: "decimal",
          precision: domain.precision ?? null,
          scale: domain.scale ?? null,
        };
      case "json":
        return { kind: "json", binary: false };
      case "jsonb":
        return { kind: "json", binary: true };
      case "uuid":
        return { kind: "uuid" };
      case "date":
        return { kind: "date" };
      case "time":
        return { kind: "time" };
      case "interval":
        return { kind: "interval" };
      case "enum":
        return { kind: "enum", name: domain.name, labels: [...domain.values] };
      case "array":
        return { kind: "array", element: physicalTypeOfName(domain.element) };
    }
  }
  switch (descriptor.type) {
    case "boolean":
      return { kind: "bool" };
    case "datetime":
      return { kind: "timestamp", zoned: descriptor.withoutTimeZone !== true };
    case "string":
      return descriptor.enumValues === undefined
        ? { kind: "text" }
        : { kind: "enum", name: "", labels: [...descriptor.enumValues] };
    case "number": {
      if (descriptor.integer !== true) return { kind: "float64" };
      const name = descriptor.declaredTypeName?.toUpperCase();
      if (name !== undefined && INT16_NAMES.has(name)) return { kind: "int32", bits: 16 };
      if (name !== undefined && INT32_NAMES.has(name)) return { kind: "int32", bits: 32 };
      return { kind: "int64" };
    }
  }
}

const ELEMENT_TYPES: ReadonlyMap<string, PhysicalType> = new Map<string, PhysicalType>([
  ["BOOLEAN", { kind: "bool" }],
  ["BOOL", { kind: "bool" }],
  ["SMALLINT", { kind: "int32", bits: 16 }],
  ["INT2", { kind: "int32", bits: 16 }],
  ["INTEGER", { kind: "int32", bits: 32 }],
  ["INT", { kind: "int32", bits: 32 }],
  ["INT4", { kind: "int32", bits: 32 }],
  ["BIGINT", { kind: "int64" }],
  ["INT8", { kind: "int64" }],
  ["REAL", { kind: "float64" }],
  ["FLOAT4", { kind: "float64" }],
  ["FLOAT8", { kind: "float64" }],
  ["DOUBLE PRECISION", { kind: "float64" }],
  ["NUMERIC", { kind: "decimal", precision: null, scale: null }],
  ["DECIMAL", { kind: "decimal", precision: null, scale: null }],
  ["TEXT", { kind: "text" }],
  ["VARCHAR", { kind: "text" }],
  ["DATE", { kind: "date" }],
  ["TIME", { kind: "time" }],
  ["TIMESTAMP", { kind: "timestamp", zoned: false }],
  ["TIMESTAMPTZ", { kind: "timestamp", zoned: true }],
  ["INTERVAL", { kind: "interval" }],
  ["UUID", { kind: "uuid" }],
  ["JSON", { kind: "json", binary: false }],
  ["JSONB", { kind: "json", binary: true }],
]);

/** An array element type by its SQL name, as `SqlDomain` `array.element` carries it. */
export function physicalTypeOfName(name: string): PhysicalType {
  const type = ELEMENT_TYPES.get(name.trim().toUpperCase());
  if (type === undefined) throw new TypeError(`Unknown array element type: ${name}`);
  return type;
}

/** Structural equality of two physical types. */
export function samePhysicalType(left: PhysicalType, right: PhysicalType): boolean {
  if (left.kind !== right.kind) return false;
  switch (left.kind) {
    case "int32":
      return left.bits === (right as Int32Type).bits;
    case "decimal": {
      const other = right as DecimalType;
      return left.precision === other.precision && left.scale === other.scale;
    }
    case "timestamp":
      return left.zoned === (right as TimestampType).zoned;
    case "json":
      return left.binary === (right as JsonType).binary;
    case "enum": {
      const other = right as EnumType;
      return (
        left.name === other.name &&
        left.labels.length === other.labels.length &&
        left.labels.every((label, index) => label === other.labels[index])
      );
    }
    case "array":
      return samePhysicalType(left.element, (right as ArrayType).element);
    default:
      return true;
  }
}
