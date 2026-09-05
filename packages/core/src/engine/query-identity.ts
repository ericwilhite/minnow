import { dateMilliseconds } from "../date-value.js";

/** Type-tagged, length-delimited structural identity. Unlike JSON, it preserves Date vs string,
 * -0, non-finite numbers, and undefined fields, so deduplication cannot merge distinct plans. */
export function encodeQueryIdentity(value: unknown, ancestors = new Set<object>()): string {
  if (value === null) return "z";
  if (typeof value === "undefined") return "u";
  if (typeof value === "boolean") return value ? "b1" : "b0";
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "nNaN;";
    if (Object.is(value, -0)) return "n-0;";
    return `n${String(value)};`;
  }
  if (typeof value === "bigint") return `i${String(value)};`;
  if (typeof value === "string") return `s${String(value.length)}:${value}`;
  if (value instanceof Date) return `d${String(dateMilliseconds(value))};`;
  if (typeof value !== "object") {
    throw new TypeError(`Unsupported query identity value: ${typeof value}`);
  }
  if (ancestors.has(value)) throw new TypeError("Query identity contains a cycle");
  ancestors.add(value);
  let encoded: string;
  if (Array.isArray(value)) {
    encoded = `a${String(value.length)}[${Array.from(value, (item) =>
      encodeQueryIdentity(item, ancestors),
    ).join("")}]`;
  } else {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    encoded = `o${String(keys.length)}{${keys
      .map(
        (key) =>
          `${encodeQueryIdentity(key, ancestors)}${encodeQueryIdentity(record[key], ancestors)}`,
      )
      .join("")}}`;
  }
  ancestors.delete(value);
  return encoded;
}
