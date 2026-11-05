/**
 * Pure pieces of the browser performance lane: timing statistics, failure classes, and the
 * cross-engine result check. Kept free of Playwright and `/proc` so they are unit tested.
 */
import type { PageFailure } from "../browser-perf/protocol.ts";

export interface TimingSummary {
  readonly median: number;
  /** Nearest-rank 95th percentile. */
  readonly p95: number;
  readonly best: number;
  readonly worst: number;
  readonly count: number;
}

export function summarize(samples: readonly number[]): TimingSummary {
  if (samples.length === 0) throw new Error("Cannot summarize zero samples");
  const sorted = [...samples].sort((a, b) => a - b);
  const n = sorted.length;
  const middle = Math.floor(n / 2);
  const at = (index: number): number => sorted[index] ?? Number.NaN;
  const median = n % 2 === 1 ? at(middle) : (at(middle - 1) + at(middle)) / 2;
  const p95 = at(Math.min(n - 1, Math.ceil(0.95 * n) - 1));
  return { median, p95, best: at(0), worst: at(n - 1), count: n };
}

/**
 * The failure classes the lane distinguishes. A cell's baseline records one of these when the
 * cell is known to fail, so a different failure, or a corruption, is noticed.
 */
export type FailureClass =
  | "timeout"
  | "tab-crash"
  | "budget"
  | "spill-limit"
  | "result-limit"
  | "out-of-memory"
  | "storage-corruption"
  | "unsupported"
  | "wrong-result"
  | "error";

const CLASS_RULES: ReadonlyArray<readonly [FailureClass, RegExp]> = [
  ["storage-corruption", /StorageCorruption|corrupt/i],
  ["spill-limit", /temp (page|run)|temp-page|temp-run|fixed safety limit/i],
  ["result-limit", /result frame row count/i],
  ["budget", /budget|accounted bytes|ExecutionMemory/i],
  [
    "out-of-memory",
    /out of memory|allocation failure|memory_limit|RangeError: Array buffer allocation/i,
  ],
  ["unsupported", /not supported|unsupported|Unsupported/],
];

export function classifyFailure(
  failure: PageFailure | { readonly crashed: true } | { readonly driverTimeout: true },
): FailureClass {
  if ("crashed" in failure) return "tab-crash";
  if ("driverTimeout" in failure) return "timeout";
  if (failure.timeout) return "timeout";
  const text = `${failure.name}: ${failure.message}`;
  for (const [failureClass, pattern] of CLASS_RULES) if (pattern.test(text)) return failureClass;
  return "error";
}

export type ResultCheck =
  /** Identical canonical hashes. */
  | "match"
  /** Hashes differ, but every value agrees within 1e-9 relative / 1e-6 absolute, in order. */
  | "tolerance"
  /** Values agree within tolerance as a multiset; only the order of near-equal float keys differs. */
  | "ties"
  | "mismatch"
  /** Too many rows to keep for a tolerance check, and the hashes differ. */
  | "mismatch-unverifiable";

export interface ComparableResult {
  readonly hash?: string | undefined;
  readonly rows?: number | undefined;
  readonly dump?: ReadonlyArray<ReadonlyArray<number | string>> | undefined;
}

const RELATIVE = 1e-9;
const ABSOLUTE = 1e-6;

function valuesAgree(a: number | string | undefined, b: number | string | undefined): boolean {
  if (a === b) return true;
  if (typeof a === "number" && typeof b === "number") {
    const scale = Math.max(Math.abs(a), Math.abs(b));
    return Math.abs(a - b) <= Math.max(ABSOLUTE, RELATIVE * scale);
  }
  return false;
}

function rowsAgree(a: ReadonlyArray<number | string>, b: ReadonlyArray<number | string>): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (!valuesAgree(a[i], b[i])) return false;
  return true;
}

function sortKey(row: ReadonlyArray<number | string>): string {
  return row.map((v) => (typeof v === "number" ? v.toPrecision(8) : v)).join("\u001f");
}

/** Compares a candidate engine's result with the reference engine's. */
export function compareResults(
  reference: ComparableResult,
  candidate: ComparableResult,
  ordered: boolean,
): ResultCheck {
  if (reference.hash !== undefined && reference.hash === candidate.hash) return "match";
  if (reference.dump === undefined || candidate.dump === undefined) return "mismatch-unverifiable";
  const a = reference.dump;
  const b = candidate.dump;
  if (a.length !== b.length) return "mismatch";
  if (ordered) {
    if (a.every((row, i) => rowsAgree(row, b[i] ?? []))) return "tolerance";
  }
  const sortedA = [...a].sort((x, y) => (sortKey(x) < sortKey(y) ? -1 : 1));
  const sortedB = [...b].sort((x, y) => (sortKey(x) < sortKey(y) ? -1 : 1));
  if (sortedA.every((row, i) => rowsAgree(row, sortedB[i] ?? [])))
    return ordered ? "ties" : "tolerance";
  return "mismatch";
}

export function isMismatch(check: ResultCheck | undefined): boolean {
  return check === "mismatch" || check === "mismatch-unverifiable";
}

export function formatMs(ms: number | undefined | null): string {
  if (ms === undefined || ms === null || !Number.isFinite(ms)) return "—";
  if (ms >= 10_000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 100) return ms.toFixed(0);
  if (ms >= 10) return ms.toFixed(1);
  if (ms >= 1) return ms.toFixed(2);
  return ms.toPrecision(2);
}

export function formatRatio(ratio: number | undefined | null): string {
  if (ratio === undefined || ratio === null || !Number.isFinite(ratio)) return "—";
  return ratio >= 100
    ? `${ratio.toFixed(0)}×`
    : ratio >= 10
      ? `${ratio.toFixed(1)}×`
      : `${ratio.toPrecision(2)}×`;
}

/** Plain-text table with padded columns. */
export function renderTable(
  header: readonly string[],
  rows: ReadonlyArray<readonly string[]>,
): string {
  const widths = header.map((h, i) =>
    Math.max(h.length, ...rows.map((row) => (row[i] ?? "").length)),
  );
  const line = (cells: readonly string[]): string =>
    cells
      .map((cell, i) => cell.padEnd(widths[i] ?? 0))
      .join("  ")
      .trimEnd();
  return [line(header), widths.map((w) => "-".repeat(w)).join("  "), ...rows.map(line)].join("\n");
}
