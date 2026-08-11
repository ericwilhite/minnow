import { type QueryRow, type QueryValue } from "../query.js";

/**
 * Raw-SQL escape hatch: sql<Row>`SELECT ... WHERE x = ${value}`. Interpolated values render as
 * SQL literals through the engine's own literal syntax — strings escape their quotes, arrays
 * become IN lists, nested `sql` fragments splice verbatim — never by naive string concatenation
 * of untrusted text into identifiers. The result executes through the ordinary statement
 * pipeline, so it supports everything the SQL surface supports and nothing more.
 */

export interface SqlExecutable {
  $executeRaw(sql: string): Promise<QueryRow[]>;
}

export type RawSqlValue =
  QueryValue | ReadonlyArray<QueryValue | RawSqlFragment<unknown>> | RawSqlFragment<unknown>;

export class RawSqlFragment<out TRow> {
  constructor(readonly sql: string) {}

  /** Phantom row type; never materialized at runtime. */
  declare readonly __row?: TRow;

  /** Type-only: the fragment's row, e.g. `type Row = typeof fragment.$inferRow`. Undefined at runtime. */
  declare readonly $inferRow: TRow;

  async execute(db: SqlExecutable): Promise<TRow[]> {
    return (await db.$executeRaw(this.sql)) as TRow[];
  }
}

function renderLiteral(value: QueryValue): string {
  if (value === null) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("sql literals must be finite numbers");
    const rendered = String(value);
    if (!/^-?\d+(?:\.\d+)?$/.test(rendered)) {
      throw new TypeError(`sql cannot render this number as a SQL literal: ${rendered}`);
    }
    return rendered;
  }
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  const time = value.getTime();
  if (!Number.isFinite(time)) throw new TypeError("sql literals must be valid dates");
  const iso = value.toISOString();
  if (!iso.endsWith("T00:00:00.000Z")) {
    throw new TypeError(
      "The SQL surface only has DATE literals; use the typed builder for datetimes with time",
    );
  }
  return `DATE '${iso.slice(0, 10)}'`;
}

function renderValue(value: RawSqlValue): string {
  if (value instanceof RawSqlFragment) return value.sql;
  if (Array.isArray(value)) {
    if (value.length === 0) throw new TypeError("sql IN lists require at least one value");
    return `(${value
      .map((item: unknown) => {
        if (Array.isArray(item)) throw new TypeError("sql lists cannot nest arrays");
        return renderValue(item as RawSqlValue);
      })
      .join(", ")})`;
  }
  return renderLiteral(value as QueryValue);
}

export function sql<TRow = QueryRow>(
  strings: TemplateStringsArray,
  ...values: RawSqlValue[]
): RawSqlFragment<TRow> {
  let text = strings[0] ?? "";
  values.forEach((value, index) => {
    text += renderValue(value) + (strings[index + 1] ?? "");
  });
  return new RawSqlFragment<TRow>(text);
}
