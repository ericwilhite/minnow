/**
 * The browser performance lane: Minnow against DuckDB-Wasm and SQLite-Wasm in real Chromium, on a
 * persistent profile on a real disk, judged as ratios against the checked-in baseline.
 *
 *   npm run benchmark:browser                                 # point suite + W1M, W500, S1
 *   npm run benchmark:browser -- --suite=point
 *   npm run benchmark:browser -- --suite=analytics --scale=large        # W5M and S5
 *   npm run benchmark:browser -- --suite=analytics --scale=W1M --only=scan_proj3_agg,agg_numeric_sum
 *   npm run benchmark:browser -- --update                     # recalibrate this profile
 *
 * Options:
 *   --suite=point|analytics|all     default all
 *   --scale=default|large|all|<ids> analytics datasets; default W1M,W500,S1; large W5M,S5
 *   --only=<cells>                  comma-separated cell ids, query ids, or point cell names
 *   --with-sqlite                   add SQLite-Wasm to the analytics suite (W1M and S1 only)
 *   --warm=<n>                      warm runs per cell (default and minimum 5)
 *   --timeout-ms=<ms>               per-run deadline in the page (default 120000)
 *   --reload                        ignore cached databases and load again
 *   --update [--provisional|--final] write this profile's calibration to the baseline
 *   --out=<dir>                     where to write cells.jsonl and summary.txt
 *   --no-confirm                    do not re-run flagged cells
 *
 * Validity rules (each one was a real mistake once) are enforced, not documented only:
 * persistent context on a disk-backed profile, one fixed origin, DuckDB-Wasm pinned to 1.32.0
 * (EH single-thread bundle, `opfs://`), named durability with SQLite pragmas read back, result
 * hashes checked across engines, and a reopen check after every failed Minnow cell.
 */
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  ANALYTICS_DATASETS,
  POINT_CELLS,
  SMOKE_DATASETS,
  SQLITE_ANALYTICS_DATASETS,
  analyticsCells,
  analyticsDataset,
  type AnalyticsCell,
  type AnalyticsDataset,
  type EngineId,
  type PointCellName,
} from "./browser-perf/catalogue.ts";
import {
  browserPerfProfile,
  calibrate,
  confirmedFlags,
  evaluate,
  parseBrowserPerfBaseline,
  targetProgress,
  updatedBaseline,
  type BrowserPerfBaseline,
  type CellBaseline,
  type CellObservation,
  type EngineOutcome,
  type Verdict,
} from "./lib/browser-perf-baseline.mts";
import { LANE_DIR_ENV, PROC_AVAILABLE, laneDirectory } from "./lib/browser-perf-host.mts";
import {
  compareResults,
  formatMs,
  formatRatio,
  isMismatch,
  renderTable,
} from "./lib/browser-perf-results.mts";
import {
  DEFAULT_PORT,
  LaneBrowser,
  PORT_ENV,
  packageVersion,
} from "./lib/browser-perf-session.mts";
import {
  POINT_REFERENCE_CELL,
  runAnalytics,
  runPoint,
  type CellRecord,
  type LaneContext,
  type LaneRecord,
  type LoadRecord,
} from "./lib/browser-perf-suites.mts";

/** `MINNOW_BROWSER_PERF_BASELINE` points the lane at another file, for trying the lane itself. */
const BASELINE_PATH =
  process.env.MINNOW_BROWSER_PERF_BASELINE ??
  new URL("../packages/core/browser-perf-baseline.json", import.meta.url);
const MIN_WARM = 5;

// ------------------------------------------------------------------------------------------------
// Arguments

const argv = process.argv.slice(2);
const flag = (name: string): boolean => argv.includes(`--${name}`);
const option = (name: string): string | undefined =>
  argv.find((a) => a.startsWith(`--${name}=`))?.slice(name.length + 3);
const known = new Set([
  "suite",
  "scale",
  "only",
  "with-sqlite",
  "warm",
  "timeout-ms",
  "reload",
  "update",
  "provisional",
  "final",
  "out",
  "no-confirm",
]);
for (const argument of argv) {
  const name = argument.replace(/^--/, "").split("=")[0] ?? "";
  if (!argument.startsWith("--") || !known.has(name))
    throw new Error(`Unknown argument ${argument}`);
}

const suite = option("suite") ?? "all";
if (!["point", "analytics", "all"].includes(suite))
  throw new Error(`--suite must be point, analytics or all`);
const update = flag("update");
const warm = Number(option("warm") ?? MIN_WARM);
if (!Number.isInteger(warm) || warm < 1) throw new Error("--warm must be a positive integer");
if (warm < MIN_WARM && update)
  throw new Error(`--update needs at least ${String(MIN_WARM)} warm runs per cell`);
const timeoutMs = Number(option("timeout-ms") ?? 120_000);
const only = (option("only") ?? "").split(",").filter((s) => s.length > 0);

function scaleDatasets(): AnalyticsDataset[] {
  const scale = option("scale") ?? "default";
  if (scale === "default")
    return ANALYTICS_DATASETS.filter((d) => !d.large && !SMOKE_DATASETS.includes(d.id));
  if (scale === "large") return ANALYTICS_DATASETS.filter((d) => d.large);
  if (scale === "all") return ANALYTICS_DATASETS.filter((d) => !SMOKE_DATASETS.includes(d.id));
  return scale.split(",").map(analyticsDataset);
}
const datasets = suite === "point" ? [] : scaleDatasets();

const matchesOnly = (cellId: string): boolean => {
  if (only.length === 0) return true;
  const bare = cellId.split("/")[1] ?? cellId;
  const query = bare.split("@")[0] ?? bare;
  return only.some((o) => o === cellId || o === bare || o === query);
};
const includeAnalytics = (cell: AnalyticsCell): boolean => matchesOnly(cell.id);
const includePoint = (cell: PointCellName): boolean => matchesOnly(`point/${cell}`);
const pointSelected = suite !== "analytics" && POINT_CELLS.some(includePoint);

const analyticsEngines = (dataset: AnalyticsDataset): EngineId[] =>
  flag("with-sqlite") && SQLITE_ANALYTICS_DATASETS.includes(dataset.id)
    ? ["minnow", "duckdb", "sqlite"]
    : ["minnow", "duckdb"];
const POINT_ENGINES: EngineId[] = ["minnow", "sqlite"];

// ------------------------------------------------------------------------------------------------
// Setup

const lane = laneDirectory();
const run = new Date().toISOString().replace(/[:.]/g, "-");
const outDirectory = path.resolve(option("out") ?? path.join(lane, "results", run));
mkdirSync(outDirectory, { recursive: true });
const jsonl = path.join(outDirectory, "cells.jsonl");
const summaryFile = path.join(outDirectory, "summary.txt");
const port = Number(process.env[PORT_ENV] ?? DEFAULT_PORT);
const logFile = path.join(outDirectory, "lane.log");
const log = (line: string): void => {
  console.log(line);
  appendFileSync(logFile, `${line}\n`);
};
const emit = (record: LaneRecord | Record<string, unknown>): void => {
  appendFileSync(jsonl, `${JSON.stringify(record)}\n`);
};

const baseline: BrowserPerfBaseline | undefined = existsSync(BASELINE_PATH)
  ? parseBrowserPerfBaseline(JSON.parse(readFileSync(BASELINE_PATH, "utf8")))
  : undefined;

const versions: Record<EngineId, string> = {
  minnow: packageVersion("@minnowdb/core"),
  duckdb: packageVersion("@duckdb/duckdb-wasm"),
  sqlite: packageVersion("@sqlite.org/sqlite-wasm"),
};
if (versions.duckdb !== "1.32.0") {
  throw new Error(
    `DuckDB-Wasm must be 1.32.0 (1.33.x dev keeps opfs:// databases in memory); installed ${versions.duckdb}`,
  );
}

async function withBrowser<T>(body: (browser: LaneBrowser) => Promise<T>): Promise<T> {
  const browser = new LaneBrowser({ laneDirectory: lane, port, log });
  try {
    await browser.start();
    return await body(browser);
  } finally {
    await browser.stop();
  }
}

function context(
  browser: LaneBrowser,
  attempt: "initial" | "confirm",
  sink: LaneContext["emit"],
): LaneContext {
  return {
    browser,
    run,
    attempt,
    warm,
    timeoutMs,
    reload: flag("reload") && attempt === "initial",
    cacheFile: path.join(lane, "datasets.json"),
    versions,
    log,
    emit: sink,
  };
}

async function runSuites(
  browser: LaneBrowser,
  attempt: "initial" | "confirm",
  sink: LaneContext["emit"],
  cells?: ReadonlySet<string>,
): Promise<CellRecord[]> {
  const ctx = context(browser, attempt, sink);
  const records: CellRecord[] = [];
  const pick = (id: string): boolean => cells === undefined || cells.has(id);
  if (pointSelected && (cells === undefined || [...cells].some((c) => c.startsWith("point/")))) {
    // Point cells depend on each other's database, so a confirmation re-runs the selected ones.
    records.push(
      ...(await runPoint(
        ctx,
        POINT_ENGINES,
        (cell) => includePoint(cell) && pick(`point/${cell}`),
      )),
    );
  }
  if (datasets.length > 0) {
    records.push(
      ...(await runAnalytics(ctx, {
        datasets,
        engines: analyticsEngines,
        include: (cell) => includeAnalytics(cell) && pick(cell.id),
      })),
    );
  }
  return records;
}

// ------------------------------------------------------------------------------------------------
// Judging

function findRecord(
  records: readonly CellRecord[],
  cell: string,
  engine: EngineId,
): CellRecord | undefined {
  for (let i = records.length - 1; i >= 0; i--) {
    const record = records[i];
    if (record?.cell === cell && record.engine === engine) return record;
  }
  return undefined;
}

const outcome = (record: CellRecord | undefined): EngineOutcome | undefined =>
  record === undefined
    ? undefined
    : {
        ok: record.ok,
        ...(record.failure === undefined ? {} : { failure: record.failure.class }),
        ...(record.timing === undefined ? {} : { timing: record.timing }),
        ...(record.onceOnly === true ? { onceOnly: true } : {}),
      };

interface Judged {
  readonly observation: CellObservation;
  readonly minnow: CellRecord;
  readonly reference: CellRecord | undefined;
  readonly others: CellRecord[];
  readonly verdicts: Verdict[];
}

function referenceFor(cellId: string): { engine: EngineId; cell: string } {
  if (cellId.startsWith("point/")) {
    const name = cellId.slice("point/".length) as PointCellName;
    return { engine: "sqlite", cell: `point/${POINT_REFERENCE_CELL[name] ?? name}` };
  }
  return { engine: "duckdb", cell: cellId };
}

function orderedFor(cellId: string): boolean {
  if (cellId.startsWith("point/")) return false;
  const dataset = analyticsDataset(cellId.split("/")[0] ?? "");
  return analyticsCells(dataset).find((c) => c.id === cellId)?.query.ordered === true;
}

function judge(records: readonly CellRecord[], profile: string): Judged[] {
  const judged: Judged[] = [];
  const cells = [...new Set(records.map((r) => r.cell))];
  for (const cell of cells) {
    const minnow = findRecord(records, cell, "minnow");
    if (minnow === undefined) continue;
    const ref = referenceFor(cell);
    const reference = findRecord(records, ref.cell, ref.engine);
    const others = records.filter(
      (r) => r.cell === cell && r.engine !== "minnow" && r !== reference,
    );
    const check =
      minnow.ok &&
      reference?.ok === true &&
      minnow.hash !== undefined &&
      reference.hash !== undefined
        ? compareResults(reference, minnow, orderedFor(cell))
        : undefined;
    const minnowOutcome = outcome(minnow) ?? { ok: false };
    const referenceOutcome = outcome(reference);
    const observation: CellObservation = {
      cell,
      reference: ref.engine,
      minnow: {
        ...minnowOutcome,
        ...(minnow.reopen?.ok === false ? { reopenFailed: true } : {}),
        ...(check === undefined ? {} : { check }),
      },
      ...(referenceOutcome === undefined ? {} : { referenceOutcome }),
    };
    const verdicts = update ? [] : evaluate(observation, baseline?.profiles[profile]?.cells[cell]);
    judged.push({ observation, minnow, reference, others, verdicts });
  }
  return judged;
}

// ------------------------------------------------------------------------------------------------
// Reporting

const cellSummary = (record: CellRecord | undefined): string => {
  if (record === undefined) return "—";
  if (!record.ok) return `FAIL ${String(record.failure?.class)}`;
  const t = record.timing;
  const cold = record.coldMs === undefined ? "" : ` · ${formatMs(record.coldMs)}`;
  const rss = record.peakRssMB === null ? "" : ` · ${String(record.peakRssMB)}MB`;
  return `${formatMs(t?.median)} / ${formatMs(t?.p95)}${cold}${rss}`;
};

function summaryTables(
  judged: readonly Judged[],
  loads: readonly LoadRecord[],
  profile: string,
): string {
  const lines: string[] = [];
  const groups = new Map<string, Judged[]>();
  for (const j of judged) {
    const group = j.minnow.cell.split("/")[0] ?? "";
    groups.set(group, [...(groups.get(group) ?? []), j]);
  }
  const header = [
    "cell",
    "minnow med / p95 · cold · peak",
    "cpu/run",
    "reference",
    "ratio",
    "thr",
    "result",
    "verdict",
    "target",
  ];
  for (const [group, entries] of groups) {
    const reference = entries[0]?.observation.reference ?? "";
    lines.push(
      `\n${group === "point" ? "Point suite (unit: ms per operation)" : `${group} (unit: ms per query)`} — reference ${reference}, profile ${profile}`,
    );
    const rows = entries.map((j) => {
      const base = baseline?.profiles[profile]?.cells[j.minnow.cell];
      const ratio =
        j.minnow.ok &&
        j.reference?.ok === true &&
        j.minnow.timing !== undefined &&
        j.reference.timing !== undefined
          ? j.minnow.timing.median / j.reference.timing.median
          : undefined;
      const target = baseline?.targets[j.minnow.cell];
      const versusRecord =
        target?.versus === undefined
          ? undefined
          : findRecord(
              judged.map((x) => x.minnow),
              target.versus,
              "minnow",
            );
      const verdict =
        j.verdicts
          .filter((v) => v.kind !== "ok")
          .map((v) => v.kind)
          .join(",") || (update ? "calibrating" : "ok");
      const reopen =
        j.minnow.reopen === undefined
          ? ""
          : j.minnow.reopen.ok
            ? " (reopen ok)"
            : " (REOPEN FAILED)";
      return [
        (j.minnow.cell.split("/")[1] ?? j.minnow.cell) + reopen,
        cellSummary(j.minnow),
        j.minnow.cpuMs === null ? "n/a" : formatMs(j.minnow.cpuMs),
        cellSummary(j.reference),
        formatRatio(ratio),
        base?.threshold === undefined
          ? base?.status === "fails"
            ? `fails:${String(base.failure)}`
            : "—"
          : String(base.threshold),
        j.observation.minnow.check ??
          (!j.minnow.ok
            ? "—"
            : j.minnow.suite === "point"
              ? "counted"
              : j.reference?.ok === true
                ? "unchecked"
                : "—"),
        verdict,
        targetProgress(
          target,
          outcome(j.minnow) ?? { ok: false },
          outcome(j.reference),
          outcome(versusRecord),
        ),
      ];
    });
    lines.push(renderTable(header, rows));
    const extra = entries.flatMap((j) => j.others);
    if (extra.length > 0) {
      lines.push(
        `  also measured: ${extra.map((r) => `${r.engine} ${r.cell.split("/")[1] ?? ""} ${cellSummary(r)}`).join("; ")}`,
      );
    }
  }
  if (loads.length > 0) {
    lines.push("\nLoads");
    lines.push(
      renderTable(
        [
          "dataset",
          "engine",
          "status",
          "total",
          "engine time",
          "stored MB",
          "peak MB",
          "settled after",
          "reopen",
          "durability",
        ],
        loads.map((l) => [
          l.dataset,
          l.engine,
          l.cached ? "cached" : l.ok ? "loaded" : `FAIL ${String(l.failure?.class)}`,
          formatMs(l.load?.totalMs),
          formatMs(l.load?.engineMs),
          l.storedBytes === undefined ? "—" : String(Math.round(l.storedBytes / 1048576)),
          l.peakRssMB === undefined || l.peakRssMB === null ? "—" : String(l.peakRssMB),
          l.idleAfterLoadMs === undefined || l.idleAfterLoadMs === null
            ? "—"
            : l.idleAfterLoadMs < 0
              ? "never"
              : `${(l.idleAfterLoadMs / 1000).toFixed(1)} s`,
          l.reopen === undefined ? "—" : l.reopen.ok ? `${String(l.reopen.openMs)} ms` : "FAILED",
          l.durability,
        ]),
      ),
    );
  }
  return lines.join("\n");
}

// ------------------------------------------------------------------------------------------------
// Main

async function main(): Promise<number> {
  log(`Browser performance lane → ${outDirectory}`);
  log(
    `lane directory ${lane} (${LANE_DIR_ENV}), origin http://127.0.0.1:${String(port)} (${PORT_ENV})`,
  );
  if (!PROC_AVAILABLE)
    log("/proc is unavailable on this host: peak RSS and CPU time are reported as unavailable");
  const loads: LoadRecord[] = [];
  const captured: CellRecord[] = [];
  const capture = (record: LaneRecord | Record<string, unknown>): void => {
    emit(record);
    if ((record as LaneRecord).type === "load") loads.push(record as LoadRecord);
  };
  let profile = "";
  const started = Date.now();
  const first = await withBrowser(async (browser) => {
    profile = browserPerfProfile(browser.browserVersion);
    emit({
      type: "run",
      run,
      profile,
      browser: browser.browserVersion,
      versions,
      suite,
      datasets: datasets.map((d) => d.id),
      only,
      warm,
      timeoutMs,
      host: {
        platform: process.platform,
        arch: process.arch,
        cpus: os.cpus().length,
        memoryGiB: Math.round(os.totalmem() / 2 ** 30),
        filesystem: browser.disk.filesystem,
        loadAverage: os.loadavg(),
      },
      at: new Date().toISOString(),
    });
    log(
      `profile ${profile} · Chromium ${browser.browserVersion} · Minnow ${versions.minnow} · DuckDB-Wasm ${versions.duckdb} · SQLite-Wasm ${versions.sqlite}`,
    );
    if (!update && baseline?.profiles[profile] === undefined) {
      throw new Error(
        `No browser baseline for ${profile}; run npm run benchmark:browser -- --update on that runtime`,
      );
    }
    return runSuites(browser, "initial", capture);
  });
  captured.push(...first);
  const firstMinutes = (Date.now() - started) / 60_000;
  const judged = judge(captured, profile);
  let verdicts = judged.flatMap((j) => j.verdicts);

  let confirmed: Verdict[] = verdicts.filter((v) => v.confirmable);
  const flagged = new Set(confirmed.map((v) => v.cell));
  if (!update && flagged.size > 0 && !flag("no-confirm")) {
    log(
      `\nConfirming ${String(flagged.size)} flagged cell(s) in a fresh browser: ${[...flagged].join(", ")}`,
    );
    const second = await withBrowser((browser) => runSuites(browser, "confirm", emit, flagged));
    const secondJudged = judge(second, profile);
    confirmed = confirmedFlags(
      verdicts,
      secondJudged.flatMap((j) => j.verdicts),
    );
    const cleared = [...flagged].filter((cell) => !confirmed.some((v) => v.cell === cell));
    if (cleared.length > 0) log(`Did not repeat (treated as noise): ${cleared.join(", ")}`);
    // Fatal findings from the re-run count too: a reopen failure is never noise.
    verdicts = [...verdicts, ...secondJudged.flatMap((j) => j.verdicts).filter((v) => v.fatal)];
  }

  const summary = [
    `Browser performance lane — ${profile} — run ${run} — ${firstMinutes.toFixed(1)} min`,
    `Minnow ${versions.minnow} (default options unless a cell names a cap), DuckDB-Wasm ${versions.duckdb} EH 1-thread, SQLite-Wasm ${versions.sqlite} SAH pool.`,
    `Columns: warm median / p95 · cold · peak renderer RSS. Ratio = Minnow ÷ reference warm medians (lower is better).`,
    summaryTables(judged, loads, profile),
  ].join("\n");
  writeFileSync(summaryFile, `${summary}\n`);
  log(`\n${summary}`);

  if (update) {
    const calibrated: Record<string, CellBaseline> = {};
    for (const j of judged) calibrated[j.observation.cell] = calibrate(j.observation);
    const previous = baseline?.profiles[profile];
    const provisional = flag("final")
      ? false
      : flag("provisional")
        ? true
        : (previous?.provisional ?? true);
    const host = `${String(os.cpus().length)} threads, ${String(Math.round(os.totalmem() / 2 ** 30))} GiB`;
    writeFileSync(
      BASELINE_PATH,
      `${JSON.stringify(updatedBaseline(baseline, profile, calibrated, { provisional, recorded: `${new Date().toISOString().slice(0, 10)} on ${host}`, ...(previous?.note === undefined ? {} : { note: previous.note }) }), null, 2)}\n`,
    );
    log(
      `\nBaseline profile ${profile} updated for ${String(Object.keys(calibrated).length)} cell(s)${provisional ? " (provisional)" : ""}.`,
    );
    // Calibration records what a cell does; it must never record a broken store or a wrong
    // answer as normal.
    const reopenFailures = judged.filter((j) => j.minnow.reopen?.ok === false);
    const wrong = judged.filter((j) => isMismatch(j.observation.minnow.check));
    for (const j of reopenFailures)
      log(`  ${j.minnow.cell}: reopen-failure — ${String(j.minnow.reopen?.error)}`);
    for (const j of wrong)
      log(`  ${j.minnow.cell}: wrong-result — ${String(j.observation.minnow.check)}`);
    if (reopenFailures.length + wrong.length > 0) {
      log(
        `\n${String(reopenFailures.length + wrong.length)} finding(s) above must be fixed, not calibrated.`,
      );
      return 1;
    }
    return 0;
  }

  const fatal = verdicts.filter((v) => v.fatal);
  const informational = verdicts.filter((v) => !v.fatal && !v.confirmable && v.kind !== "ok");
  if (informational.length > 0) {
    log("\nNotes:");
    for (const v of informational) log(`  ${v.cell}: ${v.kind} — ${v.message}`);
  }
  const failures = [...fatal, ...confirmed];
  if (failures.length > 0) {
    log(`\n${String(failures.length)} browser performance failure(s):`);
    for (const v of failures) log(`  ${v.cell}: ${v.kind} — ${v.message}`);
    return 1;
  }
  log("\nNo browser performance regressions.");
  return 0;
}

process.exitCode = await main();
