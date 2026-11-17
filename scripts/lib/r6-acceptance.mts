/** Reviewed, committed evidence gates the 1.x release line; observations never qualify it. */
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

export const R6_ACCEPTANCE_FILE = "packages/core/r6-acceptance.json";
const TARGETS = { chromium: 250, firefox: 400, webkit: 600 } as const;
type Browser = keyof typeof TARGETS;
export interface R6Acceptance {
  readonly schemaVersion: 1;
  readonly runner: string;
  readonly recordedAt: string;
  readonly commit: string;
  readonly qualification: {
    readonly platform: "linux" | "darwin";
    readonly maxLoad1: number;
    readonly sampleCount: number;
  };
  /** Worst of the five run-level quantiles, so a median cannot conceal an over-target history. */
  readonly browsers: ReadonlyArray<{
    readonly name: Browser;
    readonly runs: 5;
    readonly p50Us: number;
    readonly loadedP95Us: number;
  }>;
}
function requireValue(condition: unknown, reason: string): asserts condition {
  if (!condition) throw new Error(`R6 NOT ACCEPTED: ${reason}`);
}
function object(value: unknown): Record<string, unknown> {
  requireValue(
    value !== null && typeof value === "object" && !Array.isArray(value),
    "invalid acceptance object",
  );
  return value as Record<string, unknown>;
}
function nonnegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}
function positiveInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}
export function parseR6Acceptance(value: unknown): R6Acceptance {
  const record = object(value);
  requireValue(record.schemaVersion === 1, "unsupported acceptance schema");
  requireValue(
    typeof record.runner === "string" && record.runner.trim().length > 0,
    "missing qualified runner",
  );
  requireValue(
    typeof record.recordedAt === "string" &&
      Number.isFinite(Date.parse(record.recordedAt)) &&
      new Date(record.recordedAt).toISOString() === record.recordedAt,
    "invalid acceptance date",
  );
  requireValue(
    typeof record.commit === "string" &&
      /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(record.commit) &&
      !/^0+$/.test(record.commit),
    "missing measured commit",
  );
  const q = object(record.qualification);
  requireValue(q.platform === "linux" || q.platform === "darwin", "unqualified host platform");
  requireValue(
    nonnegative(q.maxLoad1) && q.maxLoad1 <= 1 && positiveInteger(q.sampleCount),
    "missing or contaminated host-load qualification",
  );
  requireValue(
    Array.isArray(record.browsers) && record.browsers.length === 3,
    "all three browsers are required",
  );
  const seen = new Set<string>();
  for (const raw of record.browsers) {
    const browser = object(raw);
    requireValue(
      typeof browser.name === "string" &&
        Object.hasOwn(TARGETS, browser.name) &&
        !seen.has(browser.name),
      "invalid or duplicate browser",
    );
    seen.add(browser.name);
    requireValue(browser.runs === 5, "five complete histories per browser are required");
    requireValue(
      nonnegative(browser.p50Us) &&
        browser.p50Us <= TARGETS[browser.name as Browser] &&
        nonnegative(browser.loadedP95Us) &&
        browser.loadedP95Us <= 5_000,
      `absolute targets not met for ${browser.name}`,
    );
  }
  return value as R6Acceptance;
}
/** Major-one prereleases are gated too; changing the suffix must not bypass acceptance. */
export function requiresR6Acceptance(versions: readonly string[]): boolean {
  return versions.some((version) => /^[1-9]\d*\./.test(version));
}
export function assertR6ReleaseReady(versions: readonly string[], repoRoot: string): void {
  if (!requiresR6Acceptance(versions)) return;
  const file = join(repoRoot, R6_ACCEPTANCE_FILE);
  requireValue(
    existsSync(file),
    `commit ${R6_ACCEPTANCE_FILE} from a successful qualified quiet run before releasing 1.x or later`,
  );
  const bytes = readFileSync(file);
  const committed = spawnSync("git", ["show", `HEAD:${R6_ACCEPTANCE_FILE}`], {
    cwd: repoRoot,
    maxBuffer: 1 << 20,
  });
  requireValue(
    committed.status === 0 && bytes.equals(committed.stdout),
    `${R6_ACCEPTANCE_FILE} must be committed, with no uncommitted edits`,
  );
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    throw new Error("R6 NOT ACCEPTED: invalid acceptance JSON");
  }
  parseR6Acceptance(value);
}

/** Only a complete passing quiet run can produce the portable record for review and commit. */
export function acceptanceFromQuietRun(
  reportValue: unknown,
  samples: ReadonlyArray<{ readonly load: readonly number[] }>,
  platform: string,
  recordedAt = new Date().toISOString(),
): R6Acceptance {
  const report = object(reportValue);
  requireValue(report.status === "pass", "quiet run did not pass");
  requireValue(
    samples.length > 0 &&
      samples.every(
        (s) => s.load.length === 3 && s.load.every(nonnegative) && (s.load[0] ?? Infinity) <= 1,
      ),
    "incomplete or contaminated host-load evidence",
  );
  requireValue(Array.isArray(report.records), "missing browser histories");
  const latency = report.records.map(object).filter((r) => r.lane === "latency");
  requireValue(latency.length === 3, "incomplete browser histories");
  const browsers = Object.keys(TARGETS).map((name) => {
    const matches = latency.filter((r) => r.browser === name);
    requireValue(matches.length === 1, `missing or duplicate ${name} history`);
    const record = matches[0];
    requireValue(
      record?.exit === 0 && Array.isArray(record.metrics) && record.metrics.length === 5,
      `incomplete or failed ${name} histories`,
    );
    const p50: number[] = [],
      p95: number[] = [];
    for (const raw of record.metrics) {
      const metric = object(raw),
        warm = object(metric.uncontended),
        loaded = object(metric.loaded);
      requireValue(
        object(metric.timers).isolated === true &&
          Number.isInteger(metric.lockCalls) &&
          nonnegative(metric.lockCalls) &&
          metric.lockCalls <= 8 &&
          warm.count === 3_000 &&
          loaded.count === 1_000,
        `invalid ${name} protocol/sample invariants`,
      );
      requireValue(nonnegative(warm.p50) && nonnegative(loaded.p95), `invalid ${name} quantiles`);
      p50.push(warm.p50);
      p95.push(loaded.p95);
    }
    return {
      name: name as Browser,
      runs: 5 as const,
      p50Us: Math.max(...p50),
      loadedP95Us: Math.max(...p95),
    };
  });
  return parseR6Acceptance({
    schemaVersion: 1,
    runner: report.qualifiedRunner,
    recordedAt,
    commit: report.revision,
    qualification: {
      platform,
      maxLoad1: samples.reduce((max, s) => Math.max(max, s.load[0] ?? Infinity), 0),
      sampleCount: samples.length,
    },
    browsers,
  });
}
