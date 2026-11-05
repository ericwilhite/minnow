/**
 * What lowering says about each result column beyond the IR's `OutputColumn`: the SQL type name
 * PostgreSQL would print, and how row access renders int64 values (DECISIONS.md D2, ruling R16).
 * The owner sends these once per statement (owner/protocol.ts `WireColumnMeta` is this shape).
 */
import type { OutputColumn } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";

/**
 * How row access renders a column's values in JavaScript, for the types where it is a choice
 * (DECISIONS.md D2, ruling R17.1–2):
 * - `"bigint"`: an int8 expression with a bigint-rendered operand (a BIGINT column, SUM over
 *   BIGINT, CAST AS BIGINT, a bigint parameter); and SUM over BIGINT itself, whose physical type
 *   is `decimal(null, 0)` (exact past 2^63, PostgreSQL's NUMERIC) rendered as a JS bigint.
 * - `"number"`: every other int8 expression (COUNT, SUM over SMALLINT/INTEGER, `COUNT(*) + 1`):
 *   exact, or a `RangeError` past 2^53 that names `::bigint`.
 * - `null`: the type's one rendering (numbers, NUMERIC text, DATE/TIMESTAMP text…).
 */
export type IntegerRender = "bigint" | "number";

export interface ResultColumn extends OutputColumn {
  /** The type as PostgreSQL's `format_type` prints it (`sqlTypeName`). */
  readonly sqlType: string;
  readonly render: IntegerRender | null;
}

/**
 * The rendering rule. `big` is whether the expression renders bigint by R17.1 (lowering tracks
 * it through every expression: lower/coerce.ts `Typed.big`).
 */
export function renderOf(type: PhysicalType, big: boolean): IntegerRender | null {
  if (type.kind === "int64") return big ? "bigint" : "number";
  if (type.kind === "decimal" && big) return "bigint";
  return null;
}

/**
 * PostgreSQL's printed name of a physical type (`format_type` with a type modifier). VARCHAR(n)
 * and CHAR(n) print their width (R17.4); TEXT and VARCHAR without one print `text`. Throws for
 * the internal `locator` type, which is never a result.
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
      if (type.maxLength === undefined) return "text";
      return `${type.fixed === true ? "character" : "character varying"}(${String(type.maxLength)})`;
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

/** A result column from an IR output column and whether it renders bigint. */
export function resultColumn(column: OutputColumn, big: boolean): ResultColumn {
  return {
    name: column.name,
    type: column.type,
    nullable: column.nullable,
    sqlType: sqlTypeName(column.type),
    render: renderOf(column.type, big),
  };
}
