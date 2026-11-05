/**
 * What lowering says about each result column beyond the IR's `OutputColumn`: the SQL type name
 * PostgreSQL would print, and how row access renders int64 values (DECISIONS.md D2, ruling R16).
 * The owner sends these once per statement (owner/protocol.ts `WireColumnMeta` is this shape).
 */
import type { OutputColumn } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";

/**
 * How row access renders a column's values in JavaScript, for the types where it is a choice:
 * - `"bigint"`: every BIGINT value, and SUM over BIGINT (exact past 2^63: its physical type is
 *   `decimal(null, 0)`, PostgreSQL's NUMERIC result, rendered as a JS bigint, as D2 decides).
 * - `"number"`: COUNT and SUM over SMALLINT/INTEGER (D2), and ROW_NUMBER, RANK and DENSE_RANK
 *   (bounded by the rows, like COUNT): exact, or a `RangeError` past 2^53 that names
 *   `::bigint`.
 * - `null`: the type's one rendering (numbers, NUMERIC text, DATE/TIMESTAMP text…).
 */
export type IntegerRender = "bigint" | "number";

export interface ResultColumn extends OutputColumn {
  /** The type as PostgreSQL's `format_type` prints it (`sqlTypeName`). */
  readonly sqlType: string;
  readonly render: IntegerRender | null;
}

/**
 * Where an output column's value comes from, as far as rendering cares. Lowering decides it
 * from the expression after peeling column references through derived tables, CTEs and set
 * operations (a set operation is `count` only when every branch is).
 */
export type ValueOrigin =
  /** COUNT(*), COUNT(x), as an aggregate or a window aggregate. */
  | "count"
  /** SUM over int2/int4 (int64 result, D2). */
  | "sumNarrow"
  /** SUM over int8 (decimal(null, 0) result, D2). */
  | "sumBigint"
  /** ROW_NUMBER(), RANK(), DENSE_RANK(). */
  | "rank"
  /** Anything else, including arithmetic over the above (`COUNT(*) + 1` is a BIGINT). */
  | "value";

/** The rendering rule (see `IntegerRender`). */
export function renderOf(type: PhysicalType, origin: ValueOrigin): IntegerRender | null {
  if (type.kind === "int64")
    return origin === "count" || origin === "sumNarrow" || origin === "rank" ? "number" : "bigint";
  if (type.kind === "decimal" && origin === "sumBigint") return "bigint";
  return null;
}

/**
 * PostgreSQL's printed name of a physical type (`format_type` with a type modifier). TEXT,
 * VARCHAR(n) and CHAR(n) all print `text`: the front end keeps no character width, and neither
 * does storage (DECISIONS.md: widths of text are not enforced). Throws for the internal
 * `locator` type, which is never a result.
 */
export function sqlTypeName(type: PhysicalType): string {
  switch (type.kind) {
    case "bool":
      return "boolean";
    case "int32":
      return type.bits === 16 ? "smallint" : "integer";
    case "int64":
      return "bigint";
    case "float64":
      return "double precision";
    case "float32":
      return "real";
    case "decimal":
      if (type.precision === null) return "numeric";
      return `numeric(${String(type.precision)},${String(type.scale ?? 0)})`;
    case "date":
      return "date";
    case "time":
      return "time without time zone";
    case "timestamp":
      return type.zoned ? "timestamp with time zone" : "timestamp without time zone";
    case "interval":
      return "interval";
    case "text":
      return "text";
    case "bytes":
      return "bytea";
    case "json":
      return type.binary ? "jsonb" : "json";
    case "uuid":
      return "uuid";
    case "enum":
      return type.name;
    case "array":
      return `${sqlTypeName(type.element)}[]`;
    case "locator":
      throw new TypeError("A row locator is internal and never a result column");
  }
}

/** A result column from an IR output column and its origin. */
export function resultColumn(column: OutputColumn, origin: ValueOrigin): ResultColumn {
  return {
    name: column.name,
    type: column.type,
    nullable: column.nullable,
    sqlType: sqlTypeName(column.type),
    render: renderOf(column.type, origin),
  };
}
