/** Shared SQL cast kernel. Pure value operations run directly on row, vector and folding
 * paths; the module has no executor, catalog or storage dependency. */
import { dateIsoString, dateMilliseconds } from "../date-value.js";
import { parseSqlTimestampText, readUntypedText } from "./sql-semantics.js";
import {
  exactNumericValue,
  jsonDomainValue,
  uuidDomainValue,
  dateDomainValue,
  timeDomainValue,
  intervalDomainValue,
  externalSqlDomainValue,
  typedSqlTextValue,
  isExactNumeric,
  exactNumericRounded,
} from "./sql-domains.js";

/**
 * CAST conversions between the four logical types, matching the strict common ground of
 * SQLite and PostgreSQL: numeric strings parse or fail (never silently 0), numeric integer casts round,
 * float integer casts use ties to even, booleans render as 'true'/'false', datetimes as ISO strings, and a
 * number cast to datetime reads as milliseconds since the epoch.
 */
export function castSqlValue(value: unknown, target: string): unknown {
  if (target.startsWith("numeric")) {
    const [, precision, scale] = target.split(":");
    return exactNumericValue(
      value,
      precision === undefined || precision === "" ? undefined : Number(precision),
      scale === undefined || scale === "" ? undefined : Number(scale),
    );
  }
  if (target === "json") return jsonDomainValue(value, false);
  if (target === "jsonb") return jsonDomainValue(value, true);
  if (target === "uuid") return uuidDomainValue(value);
  if (target === "date") return dateDomainValue(value);
  if (target === "time") return timeDomainValue(value);
  if (target === "interval") return intervalDomainValue(value);
  if (target === "string") {
    const external = externalSqlDomainValue(value);
    if (typeof external === "string") return typedSqlTextValue(external);
    if (typeof value === "number") return typedSqlTextValue(String(value));
    if (typeof value === "boolean") return typedSqlTextValue(value ? "true" : "false");
    if (value instanceof Date) return typedSqlTextValue(dateIsoString(value));
  }
  if (target === "number-integer" && isExactNumeric(value)) {
    const integer = Number(externalSqlDomainValue(exactNumericRounded(value, 0, "round")));
    if (!Number.isSafeInteger(integer))
      throw new RangeError("Integer cast is outside the exact safe range");
    return integer === 0 ? 0 : integer;
  }
  if (target === "number" || target === "number-integer") {
    // Externalize first, exactly as the string and datetime targets do: a NUMERIC (or other
    // domain) value is an internally tagged string, and CAST(numeric_column AS DOUBLE
    // PRECISION) must read its decimal text, not fail on the tag (T703).
    const external = externalSqlDomainValue(value);
    let parsed: number | undefined;
    if (typeof external === "number") parsed = external;
    else if (typeof external === "boolean") parsed = external ? 1 : 0;
    else if (typeof external === "string") {
      const text = external.trim();
      if (target === "number-integer" && !/^[+-]?\d+$/.test(text)) {
        throw new TypeError(`Cannot cast this string to a number: ${text} (expected integer text)`);
      }
      const candidate = text === "" ? Number.NaN : Number(text);
      if (!Number.isFinite(candidate)) {
        throw new TypeError(`Cannot cast this string to a number: ${text}`);
      }
      parsed = candidate;
    }
    if (parsed !== undefined) {
      if (target !== "number-integer") return parsed;
      // PostgreSQL rounds a double precision value cast to an integer type to the nearest
      // integer, ties to even (2.5 -> 2, 3.5 -> 4, -2.5 -> -2); SQLite truncates. A stored
      // number is double precision, so the engine follows PostgreSQL's float8 cast.
      const integer = roundHalfToEven(parsed);
      if (!Number.isSafeInteger(integer)) {
        throw new RangeError(`Integer cast is outside the exact safe range: ${String(value)}`);
      }
      return integer === 0 ? 0 : integer;
    }
  }
  if (target === "boolean") {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") {
      if (value === 0) return false;
      if (value === 1) return true;
      throw new TypeError(`Only 0 and 1 cast to boolean, got ${String(value)}`);
    }
    if (typeof value === "string") {
      const external = externalSqlDomainValue(value);
      const text = typeof external === "string" ? external.trim().toLowerCase() : "";
      const parsed = readUntypedText("boolean", text);
      if (typeof parsed === "boolean") return parsed;
      throw new TypeError(
        `Cannot cast this string to a boolean: ${typeof external === "string" ? external : value}`,
      );
    }
  }
  if (target === "datetime") {
    if (value instanceof Date) return value;
    const external = externalSqlDomainValue(value);
    if (typeof external === "string" || typeof external === "number") {
      const parsed = typeof external === "string" ? datetimeText(external) : new Date(external);
      if (Number.isFinite(dateMilliseconds(parsed))) return parsed;
      throw new TypeError(`Cannot cast this value to a datetime: ${String(value)}`);
    }
  }
  throw new TypeError(`Unsupported CAST: ${typeof value} to ${target}`);
}

/** Nearest integer with ties to even, the rounding PostgreSQL applies to float8 -> integer. */
function roundHalfToEven(value: number): number {
  const floor = Math.floor(value);
  const fraction = value - floor;
  if (fraction < 0.5) return floor;
  if (fraction > 0.5) return floor + 1;
  return floor % 2 === 0 ? floor : floor + 1;
}

/**
 * Reads datetime text the way the TIMESTAMP literal does — a zoneless `2026-01-02 03:04:05` is
 * UTC, never the host's zone — and falls back to the JavaScript parser for other spellings.
 * `new Date("2026-01-02 03:04:05")` alone would read the same text in local time, so a CAST
 * would answer differently on two machines.
 */
export function datetimeText(text: string): Date {
  return parseSqlTimestampText(text) ?? new Date(text);
}
