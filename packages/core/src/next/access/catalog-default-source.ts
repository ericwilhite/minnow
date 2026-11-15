/** Lossless source identity for the schema API's literal-versus-SQL defaults.
 * The existing defaultSql is the payload; this discriminator never duplicates wide text.
 */
import { dateIsoString, dateMilliseconds } from "../../date-value.js";
import type { ColumnDefault } from "../../storage/types.js";
import type { DURABLE_CODECS } from "../durable-format.js";
export type CatalogDefaultSource = (typeof DURABLE_CODECS.catalogDefaultSource)[number];
export function defaultSourceOf(
  value: ColumnDefault | undefined,
): CatalogDefaultSource | undefined {
  if (!value || value.kind === "autoincrement") return undefined;
  if (value.kind === "expression") return "expression";
  if (value.value instanceof Date) return "datetime";
  if (typeof value.value === "boolean") return "boolean";
  if (typeof value.value === "number") return "number";
  return "string";
}
export function defaultLiteralSql(value: boolean | number | string | Date): string {
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Default literal must be finite");
    // A numeric SQL -0 loses its sign during integer/numeric literal inference. Unknown
    // text casts into the column's type instead; FLOAT8 retains the original signed zero.
    return Object.is(value, -0) ? "'-0'" : String(value);
  }
  if (value instanceof Date && !Number.isFinite(dateMilliseconds(value)))
    throw new TypeError("Default literal must be a valid Date");
  const text = value instanceof Date ? dateIsoString(value) : value;
  return `'${text.replaceAll("'", "''")}'`;
}
function quoted(sql: string): string {
  if (!/^'(?:[^']|'')*'$/u.test(sql)) throw new TypeError("Invalid literal default SQL");
  return sql.slice(1, -1).replaceAll("''", "'");
}
export function readDefaultLiteral(
  source: Exclude<CatalogDefaultSource, "expression">,
  sql: string,
): boolean | number | string | Date {
  switch (source) {
    case "boolean":
      if (sql === "TRUE") return true;
      if (sql === "FALSE") return false;
      break;
    case "number": {
      const value = sql === "'-0'" ? -0 : Number(sql);
      if (Number.isFinite(value) && defaultLiteralSql(value) === sql) return value;
      break;
    }
    case "string":
      return quoted(sql);
    case "datetime": {
      const value = new Date(quoted(sql));
      if (Number.isFinite(dateMilliseconds(value)) && defaultLiteralSql(value) === sql)
        return value;
      break;
    }
  }
  throw new TypeError("Invalid literal default source");
}
