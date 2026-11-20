/** Answer capture and comparison for engine head-to-head runs.
 *
 * Every value is normalized across representations: NUMERIC text, safe BIGINTs and
 * floats become numbers; DATE/TIMESTAMP become epoch milliseconds; booleans 1/0; other text
 * stays exact. Small results travel inline and compare value by value: numbers within an
 * explicit tolerance (|a - b| <= ABS + REL * max(|a|, |b|)), everything else exactly. Large
 * results (more than INLINE_VALUES values) are hashed exactly at full precision: in order
 * under a total ORDER BY, otherwise as a multiset (a sum of per-row digests), so every value
 * of every row is covered. Those cells return stored columns, not computed floats.
 */
import { createHash } from "node:crypto";

export const ABS_TOLERANCE = 1e-12;
export const REL_TOLERANCE = 1e-9;
/** Results up to this many values compare inline with tolerance; larger ones hash exactly. */
export const INLINE_VALUES = 2_000_000;

export type Normal = number | string | null;
export type AnswerMethod = "inline" | "ordered-hash" | "multiset-hash" | "fixture";
export interface AnswerDigest {
  readonly method: AnswerMethod;
  readonly rows: number;
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

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/u;
const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(:?\d{2})?)?$/u;
const DECIMAL = /^-?(0|[1-9]\d*)(\.\d+)?$/u;

export function normalize(value: unknown): Normal {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : String(value);
  if (typeof value === "bigint")
    return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
      ? Number(value)
      : `b${String(value)}`;
  if (typeof value === "boolean") return value ? 1 : 0;
  if (value instanceof Date) return value.getTime();
  if (typeof value === "string") {
    if (DECIMAL.test(value)) {
      const number = Number(value);
      // Exact text beyond double precision stays text (compared exactly).
      return Number.isFinite(number) && (Math.abs(number) < 2 ** 53 || !value.includes("."))
        ? number
        : `d${value}`;
    }
    if (ISO_DATE.test(value)) return Date.parse(`${value}T00:00:00Z`);
    if (ISO_TIMESTAMP.test(value)) {
      const time = Date.parse(
        value.includes("T") || value.includes("Z") ? value : `${value.replace(" ", "T")}Z`,
      );
      if (!Number.isNaN(time)) return time;
    }
    return `s${value}`;
  }
  return `?${String(value)}`;
}

export function closeEnough(a: Normal, b: Normal): boolean {
  if (typeof a === "number" && typeof b === "number")
    return (
      a === b ||
      Math.abs(a - b) <= ABS_TOLERANCE + REL_TOLERANCE * Math.max(Math.abs(a), Math.abs(b))
    );
  return a === b;
}

/** Streams rows into an inline list or an exact hash, decided by the result's size. */
export class AnswerCollector {
  readonly #ordered: boolean;
  #rows = 0;
  #inline: Normal[][] | null = [];
  #values = 0;
  readonly #ordinal = createHash("sha256");
  #multiset = 0n;
  constructor(ordered: boolean) {
    this.#ordered = ordered;
  }
  add(row: readonly unknown[]): void {
    const normal = row.map(normalize);
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
    if (this.#inline !== null) return { method: "inline", rows: this.#rows, values: this.#inline };
    return this.#ordered
      ? { method: "ordered-hash", rows: this.#rows, hash: this.#ordinal.digest("hex") }
      : {
          method: "multiset-hash",
          rows: this.#rows,
          hash: `${String(this.#rows)}:${this.#multiset.toString(16)}`,
        };
  }
}

const sortKey = (row: readonly Normal[]) =>
  JSON.stringify(
    row.map((value) => (typeof value === "number" ? Number(value.toPrecision(6)) : value)),
  );

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
    left.sort((x, y) => (sortKey(x) < sortKey(y) ? -1 : sortKey(x) > sortKey(y) ? 1 : 0));
    right.sort((x, y) => (sortKey(x) < sortKey(y) ? -1 : sortKey(x) > sortKey(y) ? 1 : 0));
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
    for (const column of columns)
      if (!closeEnough(normalize(row[column]), normalize(want[column]))) {
        mismatches.push(`id ${String(id)} ${column}`);
        break;
      }
  }
  return { method: "fixture", rows: rows.length, mismatches };
}
