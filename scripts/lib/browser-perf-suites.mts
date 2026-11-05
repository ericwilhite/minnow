/**
 * The two suites the browser lane runs, and the records they write.
 *
 * Every engine runs in its own page and worker, one cell at a time. Around each measured call the
 * driver reads the page's renderer from `/proc`: RSS after open, peak RSS (VmHWM, reset with
 * `clear_refs` before the cell), and CPU time of all its threads. A failure is a record, never an
 * abort: its class and message are kept, and after any failed Minnow cell the store is reopened
 * and counted (Gate A), because a store that will not reopen is the worst outcome there is.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import {
  GENERATOR_VERSION,
  POINT_ROWS,
  POINT_UUID_ROWS,
  analyticsCells,
  principalRows,
  sqlFor,
  type AnalyticsCell,
  type AnalyticsDataset,
  type EngineId,
  type PointCellName,
} from "../browser-perf/catalogue.ts";
import type {
  LoadResult,
  OpenRequest,
  PageFailure,
  PageResult,
  PointSamples,
} from "../browser-perf/protocol.ts";
import {
  PROC_AVAILABLE,
  diskBytes,
  memoryStatus,
  processCpuMs,
  resetPeakMemory,
} from "./browser-perf-host.mts";
import {
  classifyFailure,
  summarize,
  type FailureClass,
  type TimingSummary,
} from "./browser-perf-results.mts";
import {
  DriverTimeoutError,
  PageCrashedError,
  type LaneBrowser,
  type LanePage,
} from "./browser-perf-session.mts";

export interface FailureRecord {
  readonly class: FailureClass;
  readonly name: string;
  readonly message: string;
  /** Where it failed: open, cold, warm, or the point operation. */
  readonly stage: string;
}

export interface ReopenCheck {
  readonly ok: boolean;
  readonly openMs?: number;
  readonly count?: number;
  readonly expected?: number;
  readonly error?: string;
}

export interface CellRecord {
  readonly type: "cell";
  readonly run: string;
  readonly attempt: "initial" | "confirm";
  readonly suite: "point" | "analytics";
  readonly cell: string;
  readonly engine: EngineId;
  readonly ok: boolean;
  readonly failure?: FailureRecord;
  /** Write cells name the durability that was in force; read cells name the store's. */
  readonly durability?: string;
  /** What the engine reported as in effect: versions, pragmas read back, settings. */
  readonly config?: Record<string, unknown>;
  readonly unit: "ms" | "ms/op";
  readonly openMs?: number;
  readonly coldMs?: number;
  /** Warm samples (analytics: whole runs; point: batch means). */
  readonly samples?: number[];
  readonly timing?: TimingSummary;
  readonly onceOnly?: boolean;
  /** Per-operation latency percentiles where single operations are long enough to time. */
  readonly latency?: { p50: number; p95: number; max: number; count: number };
  /** Renderer RSS just after the engine opened, MB. Null where /proc is unavailable. */
  readonly rssAfterOpenMB: number | null;
  /** Peak renderer RSS during the cell (VmHWM after a reset), MB. */
  readonly peakRssMB: number | null;
  /** Renderer CPU time (all threads) per warm run or per operation, ms. */
  readonly cpuMs: number | null;
  readonly coldCpuMs?: number | null;
  readonly rows?: number;
  readonly columns?: number;
  readonly hash?: string;
  readonly dump?: Array<Array<number | string>>;
  readonly reopen?: ReopenCheck;
  readonly note?: string;
  readonly at: string;
}

export interface LoadRecord {
  readonly type: "load";
  readonly run: string;
  readonly dataset: string;
  readonly engine: EngineId;
  readonly cached: boolean;
  readonly ok: boolean;
  readonly failure?: FailureRecord;
  readonly durability: string;
  readonly config?: Record<string, unknown>;
  readonly load?: LoadResult;
  readonly peakRssMB?: number | null;
  readonly cpuMs?: number | null;
  readonly storedBytes?: number;
  readonly idleAfterLoadMs?: number | null;
  readonly reopen?: ReopenCheck;
  readonly at: string;
}

export type LaneRecord = CellRecord | LoadRecord;

export interface LaneContext {
  readonly browser: LaneBrowser;
  readonly run: string;
  readonly attempt: "initial" | "confirm";
  readonly warm: number;
  readonly timeoutMs: number;
  readonly reload: boolean;
  readonly cacheFile: string;
  readonly versions: Record<EngineId, string>;
  readonly log: (line: string) => void;
  readonly emit: (record: LaneRecord) => void;
}

const now = (): string => new Date().toISOString();

// ------------------------------------------------------------------------------------------------
// Process sampling around a call

interface Probe {
  readonly pid: number | undefined;
  cpu(): number | null;
  rss(): number | null;
  peak(): number | null;
  resetPeak(): void;
}

function probe(page: LanePage): Probe {
  const pid = page.rendererPid;
  const usable = PROC_AVAILABLE && pid !== undefined;
  return {
    pid,
    cpu: () => (usable ? (processCpuMs(pid) ?? null) : null),
    rss: () => (usable ? round(memoryStatus(pid)?.rssMB) : null),
    peak: () => (usable ? round(memoryStatus(pid)?.peakMB) : null),
    resetPeak: () => {
      if (usable) resetPeakMemory(pid);
    },
  };
}

const round = (value: number | undefined): number | null =>
  value === undefined ? null : Math.round(value);
const delta = (a: number | null, b: number | null, per = 1): number | null =>
  a === null || b === null ? null : Number(((b - a) / Math.max(1, per)).toFixed(3));

function failureRecord(stage: string, failure: PageFailure): FailureRecord {
  return { class: classifyFailure(failure), name: failure.name, message: failure.message, stage };
}

function thrownFailure(stage: string, error: unknown): FailureRecord {
  if (error instanceof PageCrashedError)
    return { class: "tab-crash", name: error.name, message: error.message, stage };
  if (error instanceof DriverTimeoutError)
    return { class: "timeout", name: error.name, message: error.message, stage };
  const failure = error instanceof Error ? error : new Error(String(error));
  return {
    ...failureRecord(stage, { name: failure.name, message: failure.message, timeout: false }),
    stage,
  };
}

/** Calls the page and folds a crash or driver deadline into the same failed-result shape. */
async function attempt<T extends object>(
  run: () => Promise<PageResult<T>>,
  stage: string,
): Promise<{ ok: true; value: T } | { ok: false; failure: FailureRecord }> {
  try {
    const result = await run();
    if (result.ok) return { ok: true, value: result as T };
    return { ok: false, failure: failureRecord(stage, result.failure) };
  } catch (error) {
    return { ok: false, failure: thrownFailure(stage, error) };
  }
}

/** Waits until the renderer has used under 5% of a core for three seconds in a row. */
async function waitIdle(page: LanePage, maxMs: number): Promise<number | null> {
  const p = probe(page);
  const initial = p.cpu();
  if (initial === null) return null;
  const started = Date.now();
  let last = initial;
  let quiet = 0;
  while (Date.now() - started < maxMs) {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    const current = p.cpu() ?? last;
    quiet = current - last < 50 ? quiet + 1 : 0;
    last = current;
    if (quiet >= 3) return Date.now() - started - 3000;
  }
  return -1;
}

/**
 * Waits for background work after a load. Minnow is asked directly whether maintenance has
 * settled; the other engines have no background work, so a quiet renderer is enough. Returns
 * the milliseconds it took, -1 if it never settled, or null when it could not be observed.
 */
async function settleAfterLoad(
  ctx: LaneContext,
  page: LanePage,
  engine: EngineId,
  maxMs: number,
): Promise<number | null> {
  if (engine !== "minnow") return waitIdle(page, Math.min(maxMs, 60_000));
  const settled = await attempt(
    () => page.call("settle", { timeoutMs: maxMs }, maxMs + 60_000),
    "settle",
  );
  if (!settled.ok) {
    ctx.log(`  minnow maintenance did not settle: ${settled.failure.message}`);
    return -1;
  }
  return Math.round(settled.value.settledMs);
}

// ------------------------------------------------------------------------------------------------
// Datasets: loaded once, cached in the profile, keyed by generator and engine version

interface CacheEntry {
  readonly generatorVersion: number;
  readonly engineVersion: string;
  readonly expectedCount: number;
  readonly loadedAt: string;
}

function readCache(file: string): Record<string, CacheEntry> {
  if (!existsSync(file)) return {};
  return JSON.parse(readFileSync(file, "utf8")) as Record<string, CacheEntry>;
}

function writeCacheEntry(file: string, key: string, entry: CacheEntry | undefined): void {
  const cache = Object.entries(readCache(file)).filter(([name]) => name !== key);
  if (entry !== undefined) cache.push([key, entry]);
  writeFileSync(file, `${JSON.stringify(Object.fromEntries(cache), null, 2)}\n`);
}

export const datasetDatabase = (dataset: AnalyticsDataset): string => `perf-${dataset.id}`;

function countSql(dataset: AnalyticsDataset): string {
  return dataset.kind === "W"
    ? "SELECT COUNT(*) AS n FROM events_wide"
    : "SELECT COUNT(*) AS n FROM order_items";
}

const durabilityOf = (engine: EngineId, config: Record<string, unknown> | undefined): string => {
  if (engine === "minnow") return `minnow ${String(config?.durability ?? "strict")}`;
  if (engine === "duckdb") return "duckdb: CHECKPOINT at end of load (opfs://)";
  return `sqlite journal_mode=${String(config?.journal_mode)} synchronous=${String(config?.synchronous)} locking_mode=${String(config?.locking_mode)}`;
};

/** Opens the database in a fresh page and counts its principal table. */
async function reopenAndCount(
  ctx: LaneContext,
  open: OpenRequest,
  sql: string,
  expected: number,
): Promise<ReopenCheck> {
  const page = await ctx.browser.openPage();
  try {
    const opened = await attempt(() => page.call("open", open, 600_000), "reopen");
    if (!opened.ok)
      return { ok: false, expected, error: `${opened.failure.name}: ${opened.failure.message}` };
    const counted = await attempt(
      () => page.call("count", { sql, timeoutMs: 300_000 }, 360_000),
      "reopen-count",
    );
    await page.call("close", undefined, 60_000).catch(() => undefined);
    if (!counted.ok)
      return {
        ok: false,
        openMs: opened.value.openMs,
        expected,
        error: `${counted.failure.name}: ${counted.failure.message}`,
      };
    return {
      ok: counted.value.count === expected,
      openMs: Math.round(opened.value.openMs),
      count: counted.value.count,
      expected,
      ...(counted.value.count === expected
        ? {}
        : { error: `count ${String(counted.value.count)}, expected ${String(expected)}` }),
    };
  } finally {
    await page.close();
  }
}

/** Makes sure `engine` holds `dataset`, reusing a verified cached copy when one exists. */
async function ensureLoaded(
  ctx: LaneContext,
  engine: EngineId,
  dataset: AnalyticsDataset,
): Promise<number | undefined> {
  const key = `${engine}:${dataset.id}`;
  const db = datasetDatabase(dataset);
  const cached = readCache(ctx.cacheFile)[key];
  const open: OpenRequest = { engine, db };
  if (
    !ctx.reload &&
    cached?.generatorVersion === GENERATOR_VERSION &&
    cached.engineVersion === ctx.versions[engine]
  ) {
    const check = await reopenAndCount(ctx, open, countSql(dataset), cached.expectedCount);
    if (check.ok) {
      ctx.log(
        `${engine} ${dataset.id}: cached copy verified (${String(check.count)} rows, reopen ${String(check.openMs)} ms)`,
      );
      ctx.emit({
        type: "load",
        run: ctx.run,
        dataset: dataset.id,
        engine,
        cached: true,
        ok: true,
        durability: "cached",
        reopen: check,
        at: now(),
      });
      return cached.expectedCount;
    }
    ctx.log(
      `${engine} ${dataset.id}: cached copy failed verification (${String(check.error)}); reloading`,
    );
  }
  writeCacheEntry(ctx.cacheFile, key, undefined);
  ctx.log(`${engine} ${dataset.id}: loading…`);
  const page = await ctx.browser.openPage();
  const p = probe(page);
  let record: LoadRecord;
  try {
    const opened = await attempt(
      () => page.call("open", { ...open, fresh: true, durability: "strict" }, 600_000),
      "open",
    );
    if (!opened.ok) {
      ctx.emit({
        type: "load",
        run: ctx.run,
        dataset: dataset.id,
        engine,
        cached: false,
        ok: false,
        failure: opened.failure,
        durability: "strict",
        at: now(),
      });
      return undefined;
    }
    const before = diskBytes(ctx.browser.opfsDirectory);
    p.resetPeak();
    const cpu0 = p.cpu();
    const loadTimeout =
      (dataset.kind === "W" ? dataset.rows * dataset.columns * 0.02 : dataset.scale * 600) +
      600_000;
    const loaded = await attempt(
      () =>
        dataset.kind === "W"
          ? page.call("loadWide", { columns: dataset.columns, rows: dataset.rows }, loadTimeout)
          : page.call("loadStar", { scale: dataset.scale }, loadTimeout),
      "load",
    );
    const cpu1 = p.cpu();
    const peak = p.peak();
    const idle = loaded.ok
      ? await settleAfterLoad(ctx, page, engine, dataset.large ? 600_000 : 180_000)
      : null;
    await page.call("close", undefined, 300_000).catch(() => undefined);
    const stored = diskBytes(ctx.browser.opfsDirectory) - before;
    if (!loaded.ok) {
      record = {
        type: "load",
        run: ctx.run,
        dataset: dataset.id,
        engine,
        cached: false,
        ok: false,
        failure: loaded.failure,
        durability: durabilityOf(engine, opened.value.config),
        config: opened.value.config,
        peakRssMB: peak,
        at: now(),
      };
      ctx.emit(record);
      return undefined;
    }
    const expected = dataset.kind === "W" ? dataset.rows : (loaded.value.rows.order_items ?? 0);
    const reopen = await reopenAndCount(ctx, open, countSql(dataset), expected);
    record = {
      type: "load",
      run: ctx.run,
      dataset: dataset.id,
      engine,
      cached: false,
      ok: reopen.ok,
      ...(reopen.ok
        ? {}
        : {
            failure: {
              class: "storage-corruption",
              name: "ReopenCheck",
              message: reopen.error ?? "reopen failed",
              stage: "reopen",
            },
          }),
      durability: durabilityOf(engine, opened.value.config),
      config: opened.value.config,
      load: loaded.value,
      peakRssMB: peak,
      cpuMs: delta(cpu0, cpu1),
      storedBytes: stored,
      idleAfterLoadMs: idle,
      reopen,
      at: now(),
    };
    ctx.emit(record);
    ctx.log(
      `${engine} ${dataset.id}: loaded in ${(loaded.value.totalMs / 1000).toFixed(1)} s (engine ${(loaded.value.engineMs / 1000).toFixed(1)} s), ` +
        `${String(Math.round(stored / 1048576))} MB on disk, reopen ${reopen.ok ? `ok in ${String(reopen.openMs)} ms` : `FAILED: ${String(reopen.error)}`}`,
    );
    if (!reopen.ok) return undefined;
    writeCacheEntry(ctx.cacheFile, key, {
      generatorVersion: GENERATOR_VERSION,
      engineVersion: ctx.versions[engine],
      expectedCount: expected,
      loadedAt: now(),
    });
    return expected;
  } finally {
    await page.close();
  }
}

// ------------------------------------------------------------------------------------------------
// Analytics

export interface AnalyticsPlan {
  readonly datasets: readonly AnalyticsDataset[];
  readonly engines: (dataset: AnalyticsDataset) => EngineId[];
  readonly include: (cell: AnalyticsCell) => boolean;
}

function openFor(engine: EngineId, cell: AnalyticsCell): OpenRequest {
  const db = datasetDatabase(cell.dataset);
  if (cell.budget === undefined) return { engine, db };
  if (engine === "minnow")
    return { engine, db, minnowOptions: { executionMemoryBudgetBytes: cell.budget.bytes } };
  if (engine === "duckdb") return { engine, db, duckdbMemoryLimitBytes: cell.budget.bytes };
  return { engine, db };
}

async function analyticsCell(
  ctx: LaneContext,
  engine: EngineId,
  cell: AnalyticsCell,
): Promise<CellRecord> {
  const base = {
    type: "cell" as const,
    run: ctx.run,
    attempt: ctx.attempt,
    suite: "analytics" as const,
    cell: cell.id,
    engine,
    unit: "ms" as const,
  };
  const page = await ctx.browser.openPage();
  const p = probe(page);
  const fail = (failure: FailureRecord, extra: Partial<CellRecord> = {}): CellRecord => ({
    ...base,
    ok: false,
    failure,
    rssAfterOpenMB: null,
    peakRssMB: p.peak(),
    cpuMs: null,
    at: now(),
    ...extra,
  });
  try {
    const opened = await attempt(() => page.call("open", openFor(engine, cell), 600_000), "open");
    if (!opened.ok) return fail(opened.failure);
    const rssAfterOpen = p.rss();
    const query = {
      sql: sqlFor(cell.query, engine, principalRows(cell.dataset)),
      ordered: cell.query.ordered === true,
      ...(cell.query.kind === undefined ? {} : { kind: cell.query.kind }),
      ...(cell.query.check === undefined ? {} : { check: cell.query.check }),
    };
    p.resetPeak();
    const cpu0 = p.cpu();
    const cold = await attempt(
      () =>
        page.call(
          "runQuery",
          { ...query, reps: 1, digest: true, timeoutMs: ctx.timeoutMs },
          ctx.timeoutMs + 60_000,
        ),
      "cold",
    );
    const cpu1 = p.cpu();
    const shared = {
      openMs: Math.round(opened.value.openMs),
      config: opened.value.config,
      rssAfterOpenMB: rssAfterOpen,
    };
    if (!cold.ok) return fail(cold.failure, { ...shared, peakRssMB: p.peak() });
    const reps = ctx.warm;
    const warm = await attempt(
      () =>
        page.call(
          "runQuery",
          { ...query, reps, digest: false, timeoutMs: ctx.timeoutMs },
          reps * (ctx.timeoutMs + 10_000) + 60_000,
        ),
      "warm",
    );
    const cpu2 = p.cpu();
    const digest = {
      coldMs: Number(cold.value.times[0]?.toFixed(2)),
      ...(cold.value.rows === undefined ? {} : { rows: cold.value.rows }),
      ...(cold.value.columns === undefined ? {} : { columns: cold.value.columns }),
      ...(cold.value.hash === undefined ? {} : { hash: cold.value.hash }),
      ...(cold.value.dump === undefined ? {} : { dump: cold.value.dump }),
      coldCpuMs: delta(cpu0, cpu1),
    };
    if (!warm.ok) return fail(warm.failure, { ...shared, ...digest, peakRssMB: p.peak() });
    await page.call("close", undefined, 60_000).catch(() => undefined);
    const samples = warm.value.times.map((t) => Number(t.toFixed(2)));
    return {
      ...base,
      ok: true,
      ...shared,
      ...digest,
      samples,
      timing: summarize(samples),
      peakRssMB: p.peak(),
      cpuMs: delta(cpu1, cpu2, reps),
      at: now(),
    };
  } finally {
    await page.close();
  }
}

export async function runAnalytics(ctx: LaneContext, plan: AnalyticsPlan): Promise<CellRecord[]> {
  const records: CellRecord[] = [];
  for (const dataset of plan.datasets) {
    const cells = analyticsCells(dataset).filter(plan.include);
    if (cells.length === 0) continue;
    for (const engine of plan.engines(dataset)) {
      let expected = await ensureLoaded(ctx, engine, dataset);
      for (const cell of cells) {
        if (expected === undefined) {
          const record: CellRecord = {
            type: "cell",
            run: ctx.run,
            attempt: ctx.attempt,
            suite: "analytics",
            cell: cell.id,
            engine,
            unit: "ms",
            ok: false,
            failure: {
              class: "error",
              name: "LoadFailed",
              message: `${dataset.id} could not be loaded for ${engine}`,
              stage: "load",
            },
            rssAfterOpenMB: null,
            peakRssMB: null,
            cpuMs: null,
            at: now(),
          };
          ctx.emit(record);
          records.push(record);
          continue;
        }
        let record = await analyticsCell(ctx, engine, cell);
        if (!record.ok && engine === "minnow") {
          const reopen = await reopenAndCount(
            ctx,
            { engine, db: datasetDatabase(dataset) },
            countSql(dataset),
            expected,
          );
          record = { ...record, reopen };
          if (!reopen.ok) {
            ctx.log(
              `!! ${cell.id}: the Minnow store did not reopen after the failure: ${String(reopen.error)}`,
            );
            writeCacheEntry(ctx.cacheFile, `${engine}:${dataset.id}`, undefined);
            // Later cells need an intact copy: reload now rather than measure a broken store.
            expected = await ensureLoaded(ctx, engine, dataset);
          }
        }
        ctx.emit(record);
        records.push(record);
        ctx.log(describe(record));
      }
    }
  }
  return records;
}

export function describe(record: CellRecord): string {
  const head = `${record.engine.padEnd(7)} ${record.cell.padEnd(40)}`;
  if (!record.ok) {
    const reopen =
      record.reopen === undefined ? "" : record.reopen.ok ? " · reopen ok" : " · REOPEN FAILED";
    return `${head} FAIL ${String(record.failure?.class)}: ${String(record.failure?.message).slice(0, 140)}${reopen}`;
  }
  const t = record.timing;
  return `${head} med ${t?.median.toFixed(2) ?? "—"} p95 ${t?.p95.toFixed(2) ?? "—"} cold ${String(record.coldMs ?? "—")} ${record.unit} · rss ${String(record.rssAfterOpenMB)}→${String(record.peakRssMB)} MB · cpu ${String(record.cpuMs)} ms`;
}

// ------------------------------------------------------------------------------------------------
// Point

/** Which cell's reference-engine record a point cell is compared against. */
export const POINT_REFERENCE_CELL: Partial<Record<PointCellName, PointCellName>> = {
  // SQLite's SAH pool is exclusive to one worker, so it has no cross-tab read. The cross-tab
  // read is held against SQLite's own warm read in its worker: what the competitor costs.
  pk_read_cross_tab: "pk_read_warm_int",
};

const POINT_DB = "perf-point";
const WRITE_DB = "perf-write";
const RELAXED_DB = "perf-write-relaxed";
const CONCURRENCY = [8, 32, 64] as const;
const MINNOW_MAX_CONNECTIONS = 8;

interface PointRun {
  readonly ctx: LaneContext;
  readonly engine: EngineId;
  readonly include: (cell: PointCellName) => boolean;
  readonly records: CellRecord[];
}

function strictOpen(engine: EngineId, db: string, extra: Partial<OpenRequest> = {}): OpenRequest {
  return { engine, db, durability: "strict", sqlitePragmas: "strict", ...extra };
}

/** Opens a page and engine, runs `body`, and always closes both. */
async function withEngine<T>(
  run: PointRun,
  open: OpenRequest,
  body: (
    page: LanePage,
    opened: { openMs: number; config: Record<string, unknown>; rss: number | null },
  ) => Promise<T>,
): Promise<T | FailureRecord> {
  const page = await run.ctx.browser.openPage();
  try {
    const opened = await attempt(() => page.call("open", open, 600_000), "open");
    if (!opened.ok) return opened.failure;
    const result = await body(page, {
      openMs: opened.value.openMs,
      config: opened.value.config,
      rss: probe(page).rss(),
    });
    await page.call("close", undefined, 120_000).catch(() => undefined);
    return result;
  } finally {
    await page.close();
  }
}

function emitPoint(run: PointRun, record: CellRecord): void {
  run.ctx.emit(record);
  run.records.push(record);
  run.ctx.log(describe(record));
}

function pointFailure(
  run: PointRun,
  cell: PointCellName,
  failure: FailureRecord,
  durability?: string,
): void {
  emitPoint(run, {
    type: "cell",
    run: run.ctx.run,
    attempt: run.ctx.attempt,
    suite: "point",
    cell: `point/${cell}`,
    engine: run.engine,
    unit: "ms/op",
    ok: false,
    failure,
    ...(durability === undefined ? {} : { durability }),
    rssAfterOpenMB: null,
    peakRssMB: null,
    cpuMs: null,
    at: now(),
  });
}

/** Times one point operation in an open page and records it as a cell. */
async function measurePoint(
  run: PointRun,
  page: LanePage,
  cell: PointCellName,
  opened: { openMs: number; config: Record<string, unknown>; rss: number | null },
  durability: string,
  call: () => Promise<PageResult<PointSamples>>,
  timeoutStage = cell,
): Promise<boolean> {
  const p = probe(page);
  p.resetPeak();
  const cpu0 = p.cpu();
  const result = await attempt(call, timeoutStage);
  const cpu1 = p.cpu();
  const base = {
    type: "cell" as const,
    run: run.ctx.run,
    attempt: run.ctx.attempt,
    suite: "point" as const,
    cell: `point/${cell}`,
    engine: run.engine,
    unit: "ms/op" as const,
    durability,
    config: opened.config,
    openMs: Math.round(opened.openMs),
    rssAfterOpenMB: opened.rss,
    at: now(),
  };
  if (!result.ok) {
    emitPoint(run, {
      ...base,
      ok: false,
      failure: result.failure,
      peakRssMB: p.peak(),
      cpuMs: null,
    });
    return false;
  }
  const { samples, latencies, operations, note } = result.value;
  const rounded = samples.map((s) => Number(s.toPrecision(4)));
  const latency =
    latencies === undefined || latencies.length === 0
      ? undefined
      : (() => {
          const s = summarize(latencies);
          const sorted = [...latencies].sort((a, b) => a - b);
          return {
            p50: Number(s.median.toFixed(3)),
            p95: Number(s.p95.toFixed(3)),
            max: Number((sorted[sorted.length - 1] ?? 0).toFixed(3)),
            count: latencies.length,
          };
        })();
  emitPoint(run, {
    ...base,
    ok: true,
    samples: rounded,
    timing: summarize(rounded),
    ...(rounded.length === 1 ? { onceOnly: true } : {}),
    ...(latency === undefined ? {} : { latency }),
    peakRssMB: p.peak(),
    cpuMs: delta(cpu0, cpu1, samples.length * operations),
    note: `${String(operations)} op(s) per sample${note === undefined ? "" : `; ${note}`}`,
  });
  return true;
}

const asSamples = (result: PageResult<LoadResult>): PageResult<PointSamples> =>
  result.ok
    ? {
        ok: true,
        samples: [result.totalMs],
        operations: 1,
        note: `engine ${result.engineMs.toFixed(0)} ms of ${result.totalMs.toFixed(0)} ms`,
      }
    : result;

export async function runPoint(
  ctx: LaneContext,
  engines: readonly EngineId[],
  include: (cell: PointCellName) => boolean,
): Promise<CellRecord[]> {
  const records: CellRecord[] = [];
  for (const engine of engines) {
    const run: PointRun = { ctx, engine, include, records };
    await pointEngine(run);
  }
  return records;
}

async function pointEngine(run: PointRun): Promise<void> {
  const { ctx, engine, include } = run;
  const warmBatches = Math.max(ctx.warm, 5) + 2;
  const durability = (config: Record<string, unknown>): string => durabilityOf(engine, config);
  const readCells: PointCellName[] = [
    "pk_read_warm_int",
    "pk_read_warm_int_literal",
    "pk_read_warm_uuid",
    "pk_read_cold_1m",
    "pk_read_cross_tab",
    "pk_read_compacted",
    "pk_read_after_2k_writes",
  ];
  const needsPointDb = include("load_keyed_1m_strict") || readCells.some(include);

  // 1. The keyed durable load builds the database every read cell uses, so it runs whenever any of them does.
  if (needsPointDb) {
    const outcome = await withEngine(
      run,
      strictOpen(engine, POINT_DB, { fresh: true }),
      async (page, opened) => {
        const created = await attempt(
          () => page.call("pointCreate", { tables: ["pt_int", "pt_uuid"] }, 60_000),
          "create",
        );
        if (!created.ok) return created.failure;
        const loaded = await measurePoint(
          run,
          page,
          "load_keyed_1m_strict",
          opened,
          durability(opened.config),
          async () =>
            asSamples(await page.call("pointLoad", { table: "pt_int", rows: POINT_ROWS }, 900_000)),
        );
        if (!loaded) return undefined;
        const uuid = await attempt(
          () => page.call("pointLoad", { table: "pt_uuid", rows: POINT_UUID_ROWS }, 900_000),
          "load pt_uuid",
        );
        if (!uuid.ok) return uuid.failure;
        ctx.log(
          `${engine} point: pt_uuid loaded (${String(POINT_UUID_ROWS)} rows) in ${(uuid.value.totalMs / 1000).toFixed(1)} s`,
        );
        const idle = await settleAfterLoad(ctx, page, engine, 180_000);
        ctx.log(`${engine} point: maintenance settled ${String(idle)} ms after the loads`);
        return undefined;
      },
    );
    if (outcome !== undefined) {
      pointFailure(run, "load_keyed_1m_strict", outcome);
      return;
    }
  }

  // 2. Strict single-row inserts, then concurrent inserts, on a fresh small database.
  if (include("insert_single_strict")) {
    const outcome = await withEngine(
      run,
      strictOpen(engine, WRITE_DB, { fresh: true }),
      async (page, opened) => {
        const created = await attempt(
          () => page.call("pointCreate", { tables: ["pt_int"] }, 60_000),
          "create",
        );
        if (!created.ok) return created.failure;
        await measurePoint(
          run,
          page,
          "insert_single_strict",
          opened,
          durability(opened.config),
          () =>
            page.call(
              "pointInsertSequential",
              { table: "pt_int", firstId: 1, batches: Math.max(ctx.warm, 5), perBatch: 20 },
              900_000,
            ),
        );
        return undefined;
      },
    );
    if (outcome !== undefined) pointFailure(run, "insert_single_strict", outcome);
  }
  for (const [index, clients] of CONCURRENCY.entries()) {
    const cell = `insert_concurrent_strict_${String(clients)}` as PointCellName;
    if (!include(cell)) continue;
    const connections = engine === "minnow" ? Math.min(clients, MINNOW_MAX_CONNECTIONS) : 1;
    // Each cell starts from an empty table so the three are comparable with each other.
    const outcome = await withEngine(
      run,
      strictOpen(engine, WRITE_DB, { fresh: true, connections }),
      async (page, opened) => {
        const created = await attempt(
          () => page.call("pointCreate", { tables: ["pt_int"] }, 60_000),
          "create",
        );
        if (!created.ok) return created.failure;
        const perClient = Math.max(2, Math.round(128 / clients));
        await measurePoint(run, page, cell, opened, durability(opened.config), () =>
          page.call(
            "pointInsertConcurrent",
            {
              table: "pt_int",
              firstId: 1 + index * 10_000_000,
              clients,
              perClient,
              bursts: Math.max(ctx.warm, 5),
            },
            900_000,
          ),
        );
        return undefined;
      },
    );
    if (outcome !== undefined) pointFailure(run, cell, outcome);
  }

  // 3. Relaxed durability: parameterized single-row INSERTs (Minnow caches the plan on the SQL
  //    text; SQLite reuses one prepared statement), each its own transaction.
  if (include("insert_prepared_relaxed")) {
    const open: OpenRequest = {
      engine,
      db: RELAXED_DB,
      durability: "relaxed",
      sqlitePragmas: "relaxed",
      fresh: true,
    };
    const outcome = await withEngine(run, open, async (page, opened) => {
      const created = await attempt(
        () => page.call("pointCreate", { tables: ["pt_int"] }, 60_000),
        "create",
      );
      if (!created.ok) return created.failure;
      await measurePoint(
        run,
        page,
        "insert_prepared_relaxed",
        opened,
        durability(opened.config),
        () =>
          page.call(
            "pointInsertSequential",
            { table: "pt_int", firstId: 1, batches: Math.max(ctx.warm, 5), perBatch: 100 },
            900_000,
          ),
      );
      return undefined;
    });
    if (outcome !== undefined) pointFailure(run, "insert_prepared_relaxed", outcome);
  }

  // 4. Warm primary-key reads on the loaded database.
  if (
    include("pk_read_warm_int") ||
    include("pk_read_warm_int_literal") ||
    include("pk_read_warm_uuid")
  ) {
    const outcome = await withEngine(run, strictOpen(engine, POINT_DB), async (page, opened) => {
      // Reads are timed on a settled store, not against maintenance the open started.
      await settleAfterLoad(ctx, page, engine, 180_000);
      if (include("pk_read_warm_int")) {
        await measurePoint(run, page, "pk_read_warm_int", opened, durability(opened.config), () =>
          page.call(
            "pointRead",
            { table: "pt_int", rows: POINT_ROWS, seed: 11, batches: warmBatches, perBatch: 200 },
            600_000,
          ),
        );
      }
      if (include("pk_read_warm_int_literal")) {
        await measurePoint(
          run,
          page,
          "pk_read_warm_int_literal",
          opened,
          durability(opened.config),
          () =>
            page.call(
              "pointRead",
              {
                table: "pt_int",
                rows: POINT_ROWS,
                seed: 17,
                batches: warmBatches,
                perBatch: 200,
                literal: true,
              },
              600_000,
            ),
        );
      }
      if (include("pk_read_warm_uuid")) {
        await measurePoint(run, page, "pk_read_warm_uuid", opened, durability(opened.config), () =>
          page.call(
            "pointRead",
            {
              table: "pt_uuid",
              rows: POINT_UUID_ROWS,
              seed: 12,
              batches: warmBatches,
              perBatch: 200,
            },
            600_000,
          ),
        );
      }
      return undefined;
    });
    if (outcome !== undefined) pointFailure(run, "pk_read_warm_int", outcome);
  }

  // 5. Cold: the first read in a fresh page and worker after reopening, five times.
  if (include("pk_read_cold_1m")) {
    const samples: number[] = [];
    let first: { openMs: number; config: Record<string, unknown>; rss: number | null } | undefined;
    let peak = 0;
    let cpuTotal: number | null = 0;
    let failure: FailureRecord | undefined;
    for (let sample = 0; sample < Math.max(ctx.warm, 5) && failure === undefined; sample++) {
      const outcome = await withEngine(run, strictOpen(engine, POINT_DB), async (page, opened) => {
        first ??= opened;
        const p = probe(page);
        p.resetPeak();
        const c0 = p.cpu();
        const read = await attempt(
          () =>
            page.call(
              "pointColdRead",
              { table: "pt_int", key: 1 + ((sample * 7919 * 104729) % POINT_ROWS) },
              300_000,
            ),
          "cold read",
        );
        const c1 = p.cpu();
        peak = Math.max(peak, p.peak() ?? 0);
        const used = delta(c0, c1);
        cpuTotal = cpuTotal === null || used === null ? null : cpuTotal + used;
        if (!read.ok) return read.failure;
        samples.push(Number(read.value.ms.toFixed(3)));
        return undefined;
      });
      if (outcome !== undefined) failure = outcome;
    }
    if (failure !== undefined || first === undefined) {
      pointFailure(
        run,
        "pk_read_cold_1m",
        failure ?? {
          class: "error",
          name: "NoSample",
          message: "no cold sample",
          stage: "cold read",
        },
      );
    } else {
      emitPoint(run, {
        type: "cell",
        run: ctx.run,
        attempt: ctx.attempt,
        suite: "point",
        cell: "point/pk_read_cold_1m",
        engine,
        unit: "ms/op",
        ok: true,
        durability: durability(first.config),
        config: first.config,
        openMs: Math.round(first.openMs),
        coldMs: samples[0] ?? 0,
        samples,
        timing: summarize(samples),
        rssAfterOpenMB: first.rss,
        peakRssMB: PROC_AVAILABLE ? peak : null,
        cpuMs: cpuTotal === null ? null : Number((cpuTotal / samples.length).toFixed(3)),
        note: "each sample: a fresh page and worker, open, then the first read",
        at: now(),
      });
    }
  }

  // 6. Cross-tab: one page holds the store; a second page's reads go through it.
  if (include("pk_read_cross_tab") && engine === "minnow") {
    const holder = await ctx.browser.openPage();
    try {
      const held = await attempt(
        () => holder.call("open", strictOpen(engine, POINT_DB), 600_000),
        "open holder",
      );
      if (!held.ok) pointFailure(run, "pk_read_cross_tab", held.failure);
      else {
        await settleAfterLoad(ctx, holder, engine, 180_000);
        await holder.call(
          "pointRead",
          { table: "pt_int", rows: POINT_ROWS, seed: 13, batches: 1, perBatch: 50 },
          300_000,
        );
        const outcome = await withEngine(
          run,
          strictOpen(engine, POINT_DB),
          async (page, opened) => {
            await measurePoint(
              run,
              page,
              "pk_read_cross_tab",
              opened,
              durability(opened.config),
              () =>
                page.call(
                  "pointRead",
                  {
                    table: "pt_int",
                    rows: POINT_ROWS,
                    seed: 14,
                    batches: warmBatches,
                    perBatch: 200,
                  },
                  600_000,
                ),
            );
            return undefined;
          },
        );
        if (outcome !== undefined) pointFailure(run, "pk_read_cross_tab", outcome);
        await holder.call("close", undefined, 120_000).catch(() => undefined);
      }
    } finally {
      await holder.close();
    }
  }

  // 7. A compacted table, then the same reads after 2,000 single-row updates with maintenance off.
  if (include("pk_read_compacted") || include("pk_read_after_2k_writes")) {
    const open = strictOpen(engine, POINT_DB, {
      minnowOptions: { autoCompact: false, autoCollect: false },
    });
    const outcome = await withEngine(run, open, async (page, opened) => {
      const compacted = await attempt(
        () => page.call("pointCompact", { table: "pt_int" }, 900_000),
        "compact",
      );
      if (!compacted.ok) return compacted.failure;
      ctx.log(`${engine} point: explicit compaction took ${compacted.value.ms.toFixed(0)} ms`);
      const reads = {
        table: "pt_int" as const,
        rows: POINT_ROWS,
        seed: 15,
        batches: warmBatches,
        perBatch: 200,
        updatedSeed: 16,
        updatedCount: 2000,
      };
      await measurePoint(run, page, "pk_read_compacted", opened, durability(opened.config), () =>
        page.call("pointRead", reads, 600_000),
      );
      const updated = await attempt(
        () =>
          page.call(
            "pointUpdates",
            { table: "pt_int", rows: POINT_ROWS, seed: 16, count: 2000 },
            900_000,
          ),
        "updates",
      );
      if (!updated.ok) return updated.failure;
      ctx.log(
        `${engine} point: 2,000 single-row updates took ${(updated.value.ms / 1000).toFixed(1)} s`,
      );
      await measurePoint(
        run,
        page,
        "pk_read_after_2k_writes",
        opened,
        durability(opened.config),
        () => page.call("pointRead", reads, 600_000),
      );
      return undefined;
    });
    if (outcome !== undefined) pointFailure(run, "pk_read_after_2k_writes", outcome);
  }
}

export function resultsDirectory(laneDirectory: string, run: string): string {
  return path.join(laneDirectory, "results", run);
}
