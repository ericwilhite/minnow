/**
 * The SQLLogicTest corpus, executed where the engine ships: a real browser, the published module
 * worker, and a real IndexedDB or OPFS database behind it.
 *
 * The Node runner (`scripts/sql-logic-test.mts`) proves the SQL semantics over MemoryBlockStore
 * under V8. This runs the same recorded corpus through everything Node cannot exercise: the
 * browser's own JavaScript engine (JavaScriptCore and SpiderMonkey differ from V8 in Date
 * parsing, regular expressions, number formatting, and sort behaviour), the worker RPC layer and
 * its structured-clone/transfer encoding of every result, and the durable stores' commit paths
 * under thousands of statements with automatic maintenance left on. The expected values are the
 * corpus's own recorded results, so a browser-only divergence is reported as a plain failure
 * against ground truth rather than as a difference from Node.
 */
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import {
  SqlLogicFailure,
  parseSqlLogicTest,
  runSqlLogicTest,
  type SqlLogicDatabase,
  type SqlLogicRunStatistics,
} from "@minnowdb/core/testing";

export type SqlLogicStoreKind = "indexeddb" | "opfs" | "memory";

export interface SqlLogicBrowserRequest {
  /** File name under packages/core/testdata/sqllogictest/. */
  readonly file: string;
  readonly store: SqlLogicStoreKind;
  /** Stop after this many records; the whole file when omitted. */
  readonly stopAfter?: number;
}

export interface SqlLogicBrowserFailure {
  readonly message: string;
  readonly file: string;
  readonly line: number;
  readonly sql: string | undefined;
  readonly cause: string | undefined;
}

export interface SqlLogicBrowserResult {
  readonly statistics: SqlLogicRunStatistics;
  readonly failures: readonly SqlLogicBrowserFailure[];
  /** Wall-clock milliseconds from the first record to the last, excluding the fetch and parse. */
  readonly elapsedMs: number;
  /** Time spent in DDL and DML statements versus queries, so a slow run names its phase. */
  readonly statementMs: number;
  readonly queryMs: number;
  readonly pageErrors: readonly string[];
  readonly progress: SqlLogicBrowserProgress;
}

export interface SqlLogicBrowserProgress {
  readonly phase: "fetching" | "opening" | "running" | "closing" | "complete";
  readonly totalRecords: number;
  readonly totalStatements: number;
  readonly totalQueries: number;
  readonly operationsStarted: number;
  readonly operationsSettled: number;
  readonly statementsSettled: number;
  readonly queriesSettled: number;
  readonly current:
    | {
        readonly kind: "statement" | "query";
        readonly ordinal: number;
        readonly sql: string;
        readonly sqlLength: number;
      }
    | undefined;
  readonly elapsedMs: number;
  readonly statementMs: number;
  readonly queryMs: number;
}

interface MutableSqlLogicBrowserProgress {
  phase: SqlLogicBrowserProgress["phase"];
  operationsStarted: number;
  operationsSettled: number;
  statementsSettled: number;
  queriesSettled: number;
  current: SqlLogicBrowserProgress["current"];
}

const PROGRESS_INTERVAL_MS = 30_000;
const PROGRESS_PREFIX = "[minnow-sqllogictest-progress] ";
const PROGRESS_SQL_LENGTH = 400;
const pageErrors: string[] = [];
window.addEventListener("error", (event) => pageErrors.push(event.message));
window.addEventListener("unhandledrejection", (event) =>
  pageErrors.push(event.reason instanceof Error ? event.reason.message : String(event.reason)),
);

function spawn(): Worker {
  return new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
}

function uniqueName(prefix: string): string {
  return `${prefix}-${String(Date.now())}-${String(Math.random()).slice(2)}`;
}

function storeDescriptor(
  kind: SqlLogicStoreKind,
): { kind: "memory" } | { kind: "indexeddb"; name: string } | { kind: "opfs"; name: string } {
  if (kind === "memory") return { kind };
  return { kind, name: uniqueName(`sqllogictest-${kind}`) };
}

async function openDatabase(
  kind: SqlLogicStoreKind,
  timings: { statementMs: number; queryMs: number },
  progress: MutableSqlLogicBrowserProgress,
): Promise<SqlLogicDatabase> {
  const worker = spawn();
  const client = new MinnowDatabaseClient(worker, { store: storeDescriptor(kind) });
  await client.ready();
  const startOperation = (kind: "statement" | "query", sql: string): void => {
    progress.operationsStarted++;
    progress.current = {
      kind,
      ordinal: progress.operationsStarted,
      sql: sql.length <= PROGRESS_SQL_LENGTH ? sql : `${sql.slice(0, PROGRESS_SQL_LENGTH)}…`,
      sqlLength: sql.length,
    };
  };
  const settleOperation = (kind: "statement" | "query"): void => {
    progress.operationsSettled++;
    if (kind === "statement") progress.statementsSettled++;
    else progress.queriesSettled++;
    progress.current = undefined;
  };
  return {
    engineName: "minnow",
    execute: async (sql) => {
      startOperation("statement", sql);
      const started = performance.now();
      try {
        return await client.execute(sql);
      } finally {
        timings.statementMs += performance.now() - started;
        settleOperation("statement");
      }
    },
    query: async (sql) => {
      startOperation("query", sql);
      const started = performance.now();
      try {
        return await client.query(sql, { memoize: false });
      } finally {
        timings.queryMs += performance.now() - started;
        settleOperation("query");
      }
    },
    close: async () => {
      progress.phase = "closing";
      await client.close({ terminateWorker: true });
    },
  };
}

export async function runSqlLogicTestInBrowser(
  request: SqlLogicBrowserRequest,
): Promise<SqlLogicBrowserResult> {
  const progressStarted = performance.now();
  const progress: MutableSqlLogicBrowserProgress = {
    phase: "fetching",
    operationsStarted: 0,
    operationsSettled: 0,
    statementsSettled: 0,
    queriesSettled: 0,
    current: undefined,
  };
  const response = await fetch(`/packages/core/testdata/sqllogictest/${request.file}`);
  if (!response.ok) {
    throw new Error(`Could not fetch ${request.file}: ${String(response.status)}`);
  }
  const records = parseSqlLogicTest(await response.text(), request.file);
  const selected = request.stopAfter === undefined ? records : records.slice(0, request.stopAfter);
  const timings = { statementMs: 0, queryMs: 0 };
  const failures: SqlLogicBrowserFailure[] = [];
  const totalStatements = selected.filter((record) => record.kind === "statement").length;
  const totalQueries = selected.filter((record) => record.kind === "query").length;
  const snapshotProgress = (): SqlLogicBrowserProgress => ({
    ...progress,
    totalRecords: selected.length,
    totalStatements,
    totalQueries,
    elapsedMs: performance.now() - progressStarted,
    statementMs: timings.statementMs,
    queryMs: timings.queryMs,
  });
  const reportProgress = (): void => {
    console.info(`${PROGRESS_PREFIX}${JSON.stringify(snapshotProgress())}`);
  };
  progress.phase = "opening";
  reportProgress();
  const progressTimer = window.setInterval(reportProgress, PROGRESS_INTERVAL_MS);
  let statistics: SqlLogicRunStatistics;
  let elapsedMs: number;
  try {
    const database = await openDatabase(request.store, timings, progress);
    const started = performance.now();
    progress.phase = "running";
    reportProgress();
    statistics = await runSqlLogicTest(selected, database, {
      onFailure: (failure) => {
        failures.push(describeFailure(failure));
        // Collect every divergence in one pass; the corpus is the oracle and a run that stops at
        // the first failure would hide the shape of a systematic browser difference.
        return failures.length < 25 ? "continue" : undefined;
      },
    });
    elapsedMs = performance.now() - started;
    progress.phase = "complete";
  } finally {
    window.clearInterval(progressTimer);
    reportProgress();
  }
  return {
    statistics,
    failures,
    elapsedMs,
    statementMs: timings.statementMs,
    queryMs: timings.queryMs,
    pageErrors: [...pageErrors],
    progress: snapshotProgress(),
  };
}

function describeFailure(failure: SqlLogicFailure): SqlLogicBrowserFailure {
  return {
    message: failure.message,
    file: failure.location.file,
    line: failure.location.line,
    sql: failure.sql,
    cause: failure.cause instanceof Error ? failure.cause.message : undefined,
  };
}

Object.assign(window, { runSqlLogicTestInBrowser });
const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "SQLLogicTest runner ready";
