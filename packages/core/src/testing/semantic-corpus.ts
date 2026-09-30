import type { QueryValue } from "../engine/query.js";

/** Small deterministic Cartesian corpus shared by Node and native workers. Text ingress
 * preserves exact digits; values, result domains and refusal names are compared separately. */
export function semanticCorpus(): Array<{ sql: string; params?: QueryValue[]; pair: string }> {
  const cases: Array<{ sql: string; params?: QueryValue[]; pair: string }> = [];
  for (const value of [
    null,
    "0",
    "-0",
    "1.25",
    "-1.25",
    "9007199254740993.0001",
    "1e-100",
    "not-a-number",
  ]) {
    for (const domain of ["NUMERIC", "DOUBLE PRECISION", "INTEGER", "TEXT"]) {
      const literal = value === null ? "NULL" : `'${value}'`;
      for (const source of ["", " FROM semantic_rows"]) {
        const pair = `${String(value)}:${domain}:${source}`;
        cases.push({ sql: `SELECT CAST(${literal} AS ${domain}) AS v${source}`, pair });
        cases.push({ sql: `SELECT CAST(? AS ${domain}) AS v${source}`, params: [value], pair });
      }
    }
  }
  for (const set of ["id < 0", "id = 1", "id > 0"]) {
    for (const value of ["NULL", "1", "3"]) {
      for (const operator of ["IN", "NOT IN"]) {
        cases.push({
          sql: `SELECT ${value} ${operator} (SELECT id FROM semantic_rows WHERE ${set}) AS v`,
          pair: "membership",
        });
      }
    }
  }
  return cases;
}

export async function semanticOutcome<T>(
  run: () => T | Promise<T>,
): Promise<{ ok: true; result: T } | { ok: false; name: string }> {
  try {
    return { ok: true, result: await run() };
  } catch (error) {
    if (!(error instanceof Error)) throw error;
    return { ok: false, name: error.name };
  }
}

/** Exact comparison across browser serialization, preserving signed zero and non-finite
 * sentinels. NUMERIC/JSON text stays text; no Float64 conversion or epsilon rounding. */
export function semanticFingerprint(value: unknown): string {
  const encode = (part: unknown): unknown => {
    if (part === null) return ["null"];
    if (typeof part === "number") return ["number", Object.is(part, -0) ? "-0" : String(part)];
    if (part instanceof Date) return ["date", part.toISOString()];
    if (Array.isArray(part)) return ["array", part.map(encode)];
    if (typeof part === "object")
      return ["object", Object.entries(part).map(([key, item]) => [key, encode(item)])];
    if (typeof part === "string" || typeof part === "boolean" || typeof part === "bigint")
      return [typeof part, String(part)];
    if (part === undefined) return ["undefined"];
    throw new TypeError("Semantic fingerprint requires data values");
  };
  return JSON.stringify(encode(value));
}
