/** Answer capture and comparison for engine head-to-head runs.
 *
 * Values normalize by their column's kind (`ColumnKind`, from the result's physical types),
 * never by guessing from the value:
 * - integer and NUMERIC compare exactly as decimal text; a bigint, a safe number and decimal
 *   text of the same integer are equal, and nothing is rounded through a float;
 * - floats compare within an explicit tolerance (|a - b| <= ABS + REL * max(|a|, |b|));
 *   NaN and infinities compare exactly;
 * - text, JSON and bytes are encoded losslessly (JSON objects with sorted keys, bytes as hex);
 * - booleans, dates and timestamps compare exactly;
 * - a value of an unknown kind is encoded losslessly by its JavaScript type.
 * Every encoding carries a tag, so NULL, undefined, text "1.0" and NUMERIC 1.0 all differ.
 *
 * Small results travel inline and compare value by value. An unordered result is first sorted
 * by an exact key, and only then compared pairwise with the tolerance. Large results (more than
 * INLINE_VALUES values) are hashed exactly at full precision: in order under a total ORDER BY,
 * otherwise as a multiset (a sum of per-row digests). Those cells return stored columns, not
 * computed floats.
 */
import { createHash } from "node:crypto";

export const ABS_TOLERANCE = 1e-12;
export const REL_TOLERANCE = 1e-9;
/** Results up to this many values compare inline with tolerance; larger ones hash exactly. */
export const INLINE_VALUES = 2_000_000;

export type ColumnKind =
  "integer" | "numeric" | "float" | "text" | "json" | "bytes" | "bool" | "temporal" | "value";

/** The comparison kind of a next/ physical type kind. */
export function columnKind(physical: string): ColumnKind {
  switch (physical) {
    case "int32":
    case "int64":
      return "integer";
    case "decimal":
      return "numeric";
    case "float32":
    case "float64":
      return "float";
    case "text":
    case "uuid":
    case "enum":
      return "text";
    case "json":
      return "json";
    case "bytes":
      return "bytes";
    case "bool":
      return "bool";
    case "date":
    case "timestamp":
    case "time":
      return "temporal";
    default:
      return "value";
  }
}

/** A float is a number; every other value is tagged text; SQL NULL is null. */
export type Normal = number | string | null;
export type AnswerMethod = "inline" | "ordered-hash" | "multiset-hash" | "fixture";
export interface AnswerDigest {
  readonly method: AnswerMethod;
  readonly rows: number;
  /** The kinds the values were normalized by (unknown columns: "value"). */
  readonly kinds?: readonly ColumnKind[];
  /** inline: normalized rows. */
  readonly values?: Normal[][];
  /** hashes: hex digest of every value of every row. */
  readonly hash?: string;
  /** fixture: rows that did not match the generator (empty when all match). */
  readonly mismatches?: string[];
}
export interface AnswerComparison {
  readonly same: boolean;
  readonly method: AnswerMethod;
  readonly detail?: string;
}

const INTEGER = /^-?\d+$/u;
const DECIMAL = /^-?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$/u;
const ISO = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(\.\d{1,3})?)?(Z|[+-]\d{2}(:?\d{2})?)?$/u;

const hex = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

/** JSON with object keys sorted, recursively; bigints as exact text. */
function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    if (typeof value === "bigint") return `${String(value)}n`;
    return JSON.stringify(value) ?? "undefined";
  }
  if (value instanceof Uint8Array) return `"\\\\x${hex(value)}"`;
  if (value instanceof Date) return JSON.stringify(value.toISOString());
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}

/** Lossless encoding by JavaScript type, for a value of an unknown kind or the wrong kind. */
function byType(value: unknown): Normal {
  if (value === null) return null;
  if (value === undefined) return "u";
  if (typeof value === "bigint") return `i${String(value)}`;
  if (typeof value === "number") {
    if (Number.isInteger(value)) return `i${BigInt(value).toString()}`;
    return Number.isFinite(value) ? value : `f${String(value)}`;
  }
  if (typeof value === "string") return `s${value}`;
  if (typeof value === "boolean") return value ? "b1" : "b0";
  if (value instanceof Date) return `t${String(value.getTime())}`;
  if (value instanceof Uint8Array) return `x${hex(value)}`;
  return `j${canonicalJson(value)}`;
}

export function normalize(value: unknown, kind: ColumnKind = "value"): Normal {
  if (value === null) return null;
  if (value === undefined) return "u";
  switch (kind) {
    case "integer":
      if (typeof value === "bigint") return `i${String(value)}`;
      if (typeof value === "number" && Number.isInteger(value))
        return `i${BigInt(value).toString()}`;
      if (typeof value === "string" && INTEGER.test(value)) return `i${BigInt(value).toString()}`;
      break;
    case "numeric":
      // Exact text, scale included: NUMERIC output keeps its scale. Integers are exact either way.
      if (typeof value === "bigint") return `n${String(value)}`;
      if (typeof value === "number" && Number.isFinite(value)) return `n${String(value)}`;
      if (typeof value === "string" && (DECIMAL.test(value) || value === "NaN")) return `n${value}`;
      break;
    case "float":
      if (typeof value === "number") return Number.isFinite(value) ? value : `f${String(value)}`;
      if (typeof value === "bigint") return Number(value);
      if (typeof value === "string" && (DECIMAL.test(value) || /^-?(Infinity|NaN)$/u.test(value))) {
        const number = Number(value);
        return Number.isFinite(number) ? number : `f${String(number)}`;
      }
      break;
    case "text":
      if (typeof value === "string") return `s${value}`;
      break;
    case "json":
      return `j${typeof value === "string" ? value : canonicalJson(value)}`;
    case "bytes":
      if (value instanceof Uint8Array) return `x${hex(value)}`;
      if (typeof value === "string" && /^\\x[0-9a-f]*$/iu.test(value))
        return `x${value.slice(2).toLowerCase()}`;
      break;
    case "bool":
      if (typeof value === "boolean") return value ? "b1" : "b0";
      break;
    case "temporal":
      if (value instanceof Date) return `t${String(value.getTime())}`;
      if (typeof value === "number") return `t${String(value)}`;
      if (typeof value === "string") {
        if (ISO.test(value)) {
          const time = Date.parse(
            value.length === 10
              ? `${value}T00:00:00Z`
              : /Z|[+-]\d{2}(:?\d{2})?$/u.test(value)
                ? value.replace(" ", "T")
                : `${value.replace(" ", "T")}Z`,
          );
          if (!Number.isNaN(time)) return `t${String(time)}`;
        }
        return `ts${value}`;
      }
      break;
    case "value":
      return byType(value);
  }
  // A value of an unexpected representation stays distinct and lossless.
  return `?${kind}:${String(byType(value))}`;
}

export function closeEnough(a: Normal, b: Normal): boolean {
  if (typeof a === "number" && typeof b === "number")
    return (
      a === b ||
      Math.abs(a - b) <= ABS_TOLERANCE + REL_TOLERANCE * Math.max(Math.abs(a), Math.abs(b))
    );
  return a === b;
}

/** Exact total order of normalized values: NULL, then numbers by value, then text by code unit. */
function compareNormal(a: Normal, b: Normal): number {
  if (a === b) return 0;
  if (a === null) return -1;
  if (b === null) return 1;
  if (typeof a === "number") return typeof b === "number" ? a - b : -1;
  if (typeof b === "number") return 1;
  return a < b ? -1 : 1;
}
function compareRows(x: readonly Normal[], y: readonly Normal[]): number {
  for (let c = 0; c < Math.min(x.length, y.length); c++) {
    const order = compareNormal(x[c] ?? null, y[c] ?? null);
    if (order !== 0) return order;
  }
  return x.length - y.length;
}

/** Streams rows into an inline list or an exact hash, decided by the result's size. */
export class AnswerCollector {
  readonly #ordered: boolean;
  #kinds: readonly ColumnKind[] | undefined;
  #rows = 0;
  #inline: Normal[][] | null = [];
  #values = 0;
  readonly #ordinal = createHash("sha256");
  #multiset = 0n;
  constructor(ordered: boolean, kinds?: readonly ColumnKind[]) {
    this.#ordered = ordered;
    this.#kinds = kinds;
  }
  /** The kinds of the result's columns, when they are known only once rows arrive. */
  setKinds(kinds: readonly ColumnKind[]): void {
    if (this.#rows > 0) throw new Error("Column kinds must be known before the first row");
    this.#kinds = kinds;
  }
  add(row: readonly unknown[]): void {
    const normal = row.map((value, c) => normalize(value, this.#kinds?.[c] ?? "value"));
    this.#rows++;
    this.#values += normal.length;
    const line = JSON.stringify(normal);
    if (this.#ordered) this.#ordinal.update(line).update("\n");
    else {
      const digest = createHash("sha256").update(line).digest();
      this.#multiset =
        (this.#multiset + digest.readBigUInt64BE(0) * 2n ** 64n + digest.readBigUInt64BE(8)) %
        2n ** 128n;
    }
    if (this.#inline !== null) {
      if (this.#values <= INLINE_VALUES) this.#inline.push(normal);
      else this.#inline = null;
    }
  }
  finish(): AnswerDigest {
    const kinds = this.#kinds === undefined ? {} : { kinds: [...this.#kinds] };
    if (this.#inline !== null)
      return { method: "inline", rows: this.#rows, ...kinds, values: this.#inline };
    return this.#ordered
      ? { method: "ordered-hash", rows: this.#rows, ...kinds, hash: this.#ordinal.digest("hex") }
      : {
          method: "multiset-hash",
          rows: this.#rows,
          ...kinds,
          hash: `${String(this.#rows)}:${this.#multiset.toString(16)}`,
        };
  }
}

export function compareAnswers(
  a: AnswerDigest,
  b: AnswerDigest,
  ordered: boolean,
): AnswerComparison {
  if (a.method !== b.method)
    return { same: false, method: a.method, detail: `methods ${a.method}/${b.method}` };
  if (a.rows !== b.rows)
    return { same: false, method: a.method, detail: `rows ${String(a.rows)}/${String(b.rows)}` };
  if (a.method === "fixture") {
    const bad = [...(a.mismatches ?? []), ...(b.mismatches ?? [])];
    return bad.length === 0
      ? { same: true, method: "fixture" }
      : { same: false, method: "fixture", detail: bad.slice(0, 3).join("; ") };
  }
  if (a.method !== "inline") {
    return a.hash === b.hash
      ? { same: true, method: a.method }
      : { same: false, method: a.method, detail: "hash" };
  }
  const left = [...(a.values ?? [])],
    right = [...(b.values ?? [])];
  if (!ordered) {
    left.sort(compareRows);
    right.sort(compareRows);
  }
  for (let r = 0; r < left.length; r++) {
    const x = left[r] ?? [],
      y = right[r] ?? [];
    if (x.length !== y.length)
      return { same: false, method: "inline", detail: `row ${String(r)} width` };
    for (let c = 0; c < x.length; c++)
      if (!closeEnough(x[c] ?? null, y[c] ?? null))
        return {
          same: false,
          method: "inline",
          detail: `row ${String(r)} column ${String(c)}: ${JSON.stringify(x[c])} vs ${JSON.stringify(y[c])}`,
        };
  }
  return { same: true, method: "inline" };
}

/** An unordered LIMIT may return any rows: check each returned row against the generator. */
export function checkFixtureRows(
  rows: ReadonlyArray<Record<string, unknown>>,
  columns: readonly string[],
  expected: (id: number) => Record<string, unknown>,
  kinds?: readonly ColumnKind[],
): AnswerDigest {
  const mismatches: string[] = [];
  const seen = new Set<number>();
  for (const row of rows) {
    const id = Number(row.id);
    if (!Number.isSafeInteger(id) || seen.has(id)) {
      mismatches.push(`bad or repeated id ${String(row.id)}`);
      continue;
    }
    seen.add(id);
    const want = expected(id);
    for (const [c, column] of columns.entries()) {
      const kind = kinds?.[c] ?? "value";
      if (!closeEnough(normalize(row[column], kind), normalize(want[column], kind))) {
        mismatches.push(`id ${String(id)} ${column}`);
        break;
      }
    }
  }
  return {
    method: "fixture",
    rows: rows.length,
    ...(kinds === undefined ? {} : { kinds: [...kinds] }),
    mismatches,
  };
}
