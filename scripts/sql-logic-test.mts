/**
 * Native SQLLogicTest runner for Minnow.
 *
 * Each file gets an empty MemoryBlockStore, and both parsing and execution stream one record at a
 * time. A 100 MB upstream file therefore does not become a 100 MB source string plus an equally
 * large parsed tree. The first failure stops the run and prints a directly replayable command.
 */
import { createReadStream, existsSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { createInterface } from "node:readline";

import { MinnowDatabase } from "../packages/core/src/engine/index.js";
import { MemoryBlockStore } from "../packages/core/src/storage/index.js";
import {
  SqlLogicFailure,
  parseSqlLogicTestLines,
  runSqlLogicTest,
  type SqlLogicDatabase,
  type SqlLogicRecord,
  type SqlLogicRunStatistics,
} from "../packages/core/src/testing/sqllogictest.js";
import { listSqlLogicTestFiles } from "./lib/sqllogictest-corpus.mts";

const arguments_ = process.argv.slice(2);
const directories = argumentValues("--directory").map((directory) => resolve(directory));
for (const directory of directories) {
  if (!existsSync(directory) || !statSync(directory).isDirectory()) {
    usageError(`directory does not exist: ${directory}`);
  }
}
const files = [
  ...argumentValues("--file").map((file) => resolve(file)),
  ...directories.flatMap(listSqlLogicTestFiles),
];
const stopAfterText = argumentValue("--stop-after");
const stopAfter = stopAfterText === undefined ? undefined : Number(stopAfterText);
const quiet = arguments_.includes("--quiet");
const trace = arguments_.includes("--trace");
const traceFromText = argumentValue("--trace-from");
const traceFrom = Number(traceFromText ?? 1);
const progressEveryText = argumentValue("--progress-every");
const progressEvery = Number(progressEveryText ?? 0);
const keepGoing = arguments_.includes("--keep-going");
const maxFailuresText = argumentValue("--max-failures");
const maxFailures = Number(maxFailuresText ?? 100);

if (stopAfter !== undefined && (!Number.isSafeInteger(stopAfter) || stopAfter <= 0)) {
  usageError(`--stop-after must be a positive whole number, got '${stopAfterText ?? ""}'`);
}
if (files.length === 0) usageError("pass at least one --file <path> or --directory <path>");
if (!Number.isSafeInteger(progressEvery) || progressEvery < 0) {
  usageError(
    `--progress-every must be a non-negative whole number, got '${progressEveryText ?? ""}'`,
  );
}
if (!Number.isSafeInteger(traceFrom) || traceFrom <= 0) {
  usageError(`--trace-from must be a positive whole number, got '${traceFromText ?? ""}'`);
}
if (!Number.isSafeInteger(maxFailures) || maxFailures <= 0) {
  usageError(`--max-failures must be a positive whole number, got '${maxFailuresText ?? ""}'`);
}
for (const file of files) {
  if (!existsSync(file) || !statSync(file).isFile()) usageError(`file does not exist: ${file}`);
}

const started = performance.now();
const total: MutableStatistics = {
  files: 0,
  statements: 0,
  queries: 0,
  values: 0,
  skipped: 0,
  halted: false,
  hashThreshold: 8,
};
const failures: SqlLogicFailure[] = [];

try {
  for (const file of files) {
    const fileStarted = performance.now();
    const selectedRecords =
      stopAfter === undefined ? readRecords(file) : takeRecords(readRecords(file), stopAfter);
    const records = reportRecords(selectedRecords, file);
    const statistics = await runSqlLogicTest(
      records,
      createDatabase(),
      keepGoing
        ? {
            onFailure: (failure) => {
              failures.push(failure);
              const cause = failure.cause instanceof Error ? `\n${failure.cause.message}` : "";
              console.error(`FAIL ${failure.message}${cause}\n`);
              if (failures.length >= maxFailures) throw failure;
              return "continue";
            },
          }
        : {},
    );
    addStatistics(total, statistics);
    if (!quiet) {
      console.log(
        `ok  ${file} — ${String(statistics.statements)} statements, ` +
          `${String(statistics.queries)} queries, ${String(statistics.values)} values, ` +
          `${String(statistics.skipped)} conditional skips in ${secondsSince(fileStarted)}s`,
      );
    }
  }
} catch (error) {
  console.error(renderError(error));
  console.error(
    `\nReplay:\n  npm run test:sql:logic -- --file ${shellQuote(files[total.files] ?? files[0] ?? "")}` +
      (stopAfter === undefined ? "" : ` --stop-after ${String(stopAfter)}`),
  );
  process.exit(1);
}

console.log(
  `SQLLogicTest passed: ${String(total.files)} file(s), ${String(total.statements)} statements, ` +
    `${String(total.queries)} queries, ${String(total.values)} values, ` +
    `${String(total.skipped)} reported conditional skips in ${secondsSince(started)}s.`,
);
if (failures.length > 0) {
  console.error(`${String(failures.length)} SQLLogicTest failure(s).`);
  process.exit(1);
}

function createDatabase(): SqlLogicDatabase {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, {
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
    createId: sequentialIds(),
  });
  return {
    engineName: "minnow",
    execute: (sql) => database.execute(sql),
    query: (sql) => database.query(sql, { memoize: false }),
    close: () => database.close(),
  };
}

async function* readRecords(file: string): AsyncGenerator<SqlLogicRecord> {
  const input = createReadStream(file, { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Infinity });
  try {
    yield* parseSqlLogicTestLines(lines, file);
  } finally {
    lines.close();
    input.destroy();
  }
}

async function* takeRecords(
  records: AsyncIterable<SqlLogicRecord>,
  count: number,
): AsyncGenerator<SqlLogicRecord> {
  let taken = 0;
  for await (const record of records) {
    if (taken >= count) return;
    yield record;
    taken++;
  }
}

async function* reportRecords(
  records: AsyncIterable<SqlLogicRecord>,
  file: string,
): AsyncGenerator<SqlLogicRecord> {
  let recordNumber = 0;
  for await (const record of records) {
    recordNumber++;
    if (
      !quiet &&
      ((trace && recordNumber >= traceFrom) ||
        (progressEvery > 0 && recordNumber % progressEvery === 0))
    ) {
      console.log(
        `run ${file} — record ${String(recordNumber)} at source line ${String(record.location.line)} (${record.kind})`,
      );
    }
    yield record;
  }
}

function sequentialIds(): () => string {
  let next = 0;
  return () => `slt-${String(next++)}`;
}

interface MutableStatistics {
  files: number;
  statements: number;
  queries: number;
  values: number;
  skipped: number;
  halted: boolean;
  hashThreshold: number;
}

function addStatistics(total: MutableStatistics, next: SqlLogicRunStatistics): void {
  total.files += 1;
  total.statements += next.statements;
  total.queries += next.queries;
  total.values += next.values;
  total.skipped += next.skipped;
  total.halted ||= next.halted;
  total.hashThreshold = next.hashThreshold;
}

function argumentValues(name: string): string[] {
  const values: string[] = [];
  for (let index = 0; index < arguments_.length; index++) {
    if (arguments_[index] === name) {
      const value = arguments_[index + 1];
      if (value === undefined || value.startsWith("--")) usageError(`${name} requires a value`);
      values.push(value);
      index++;
    }
  }
  return values;
}

function argumentValue(name: string): string | undefined {
  const values = argumentValues(name);
  if (values.length > 1) usageError(`${name} may be passed only once`);
  return values[0];
}

function secondsSince(start: number): string {
  return ((performance.now() - start) / 1000).toFixed(2);
}

function renderError(error: unknown): string {
  const lines: string[] = [];
  let current: unknown = error;
  while (current instanceof Error) {
    lines.push(lines.length === 0 ? current.message : `caused by: ${current.message}`);
    current = current.cause;
  }
  if (lines.length === 0) return String(error);
  return lines.join("\n");
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function usageError(message: string): never {
  console.error(
    `${message}\n\nUsage: npm run test:sql:logic -- (--file <path> | --directory <path>) [--stop-after <records>] [--progress-every <records>] [--trace] [--trace-from <record>] [--keep-going] [--max-failures <count>] [--quiet]`,
  );
  process.exit(2);
}
