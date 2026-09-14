/**
 * Browser-native SQL differential campaign.
 *
 * Minnow runs through its published module worker. The independent answers come from the real
 * SQLite Wasm and PGlite browser distributions that power the public benchmark page. Every
 * engine receives the same fixture, parameters, and stateful RETURNING statements.
 */
import type { PGlite } from "@electric-sql/pglite";
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { positionalToNumbered } from "../src/testing/oracle.js";
import rawMatrix from "../sql-feature-matrix.json";
import rawPostgresProfile from "../postgres-feature-profile.json";
import {
  fixedQueries,
  fixtureRows,
  generatedQueries,
  mutationQueries,
  type DifferentialQuery,
  type SqlParameter,
} from "./sql-differential-corpus.js";
import {
  featureBehaviorProbes,
  type ExpectedFeaturePatternResult,
  type ExpectedFeatureValue,
  type FeatureBehaviorProbe,
} from "./sql-feature-probes.js";

type EngineName = "minnow" | "sqlite" | "pglite";
type OracleName = Exclude<EngineName, "minnow">;
export type BrowserSqlDifferentialStore = "indexeddb" | "opfs";

interface ResultSet {
  readonly columns: readonly string[];
  readonly rows: ReadonlyArray<readonly unknown[]>;
  readonly jsonColumns?: readonly number[];
  readonly numericColumns?: readonly number[];
}

interface MutationResult extends ResultSet {
  readonly affectedRows: number;
}

interface Engine {
  readonly name: EngineName;
  readonly version: string;
  execute(sql: string, params?: readonly SqlParameter[]): Promise<void>;
  executeQuery(sql: string, params?: readonly SqlParameter[]): Promise<ResultSet>;
  query(sql: string, params?: readonly SqlParameter[]): Promise<ResultSet>;
  mutate(sql: string, params?: readonly SqlParameter[]): Promise<MutationResult>;
  close(): Promise<void>;
}

export interface BrowserSqlDifferentialResult {
  readonly seeds: readonly number[];
  readonly versions: Record<EngineName, string>;
  readonly generatedQueries: number;
  readonly fixedQueries: number;
  readonly mutations: number;
  readonly oracleComparisons: number;
  readonly matrix: {
    readonly entries: number;
    readonly supported: number;
    readonly compatibleReadsCompared: number;
    readonly compatibleReadAcceptance: number;
    readonly nonportableReadsAccepted: number;
    readonly compatibleMutationsCompared: number;
    readonly compatibleWritesAccepted: number;
    readonly nonportableWritesAccepted: number;
    readonly behaviorProbedFeatures: number;
    readonly behaviorProbeSteps: number;
    readonly behaviorProbeOracleComparisons: number;
    readonly unsupportedRejected: number;
  };
  readonly failures: readonly string[];
  readonly workerErrors: readonly string[];
  readonly elapsedMs: number;
}

interface MatrixFeature {
  readonly id: string;
  readonly status: "supported" | "unsupported";
  readonly example: string;
  readonly params?: readonly SqlParameter[];
  readonly setup?: readonly string[];
  readonly error?: string;
}

type Classification = "compatible" | "different" | "extension" | "unsupported" | "inapplicable";

interface ProfileOverride {
  readonly id: string;
  readonly classification: Classification;
  readonly verification?: "acceptance";
}

const SQLITE_ENTRY = "/vendor/sqlite/index.mjs";
const PGLITE_ENTRY = "/vendor/pglite/index.js";
const matrixFeatures = (rawMatrix as { features: MatrixFeature[] }).features;
const postgresProfile = rawPostgresProfile as {
  oracle: { version: string };
  defaults: { supported: Classification; unsupported: Classification };
  overrides: ProfileOverride[];
};
const postgresOverrides = new Map(postgresProfile.overrides.map((entry) => [entry.id, entry]));
const behaviorProbeByFeature = new Map(
  featureBehaviorProbes.map((probe) => [probe.featureId, probe]),
);
const nonportableMutationCounts = new Map([
  ["mutation.truncate", 3],
  ["mutation.upsert-replace", 1],
  ["mutation.upsert-expression", 1],
]);

function spawnMinnowWorker(): Worker {
  return new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" });
}

async function openMinnow(
  store: BrowserSqlDifferentialStore,
  workerErrors: string[],
): Promise<Engine> {
  const worker = spawnMinnowWorker();
  const client = new MinnowDatabaseClient(worker, {
    store: {
      kind: store,
      name: `sql-differential-${store}-${crypto.randomUUID()}`,
    },
    onWorkerError: ({ context, error }) => workerErrors.push(`${context}: ${error.message}`),
  });
  await client.ready();
  const rows = (
    columns: readonly string[],
    values: ReadonlyArray<Record<string, unknown>>,
    domains: ReadonlyArray<{ kind: string } | null>,
  ) => ({
    columns,
    rows: values.map((row) =>
      columns.map((column) => {
        if (!Object.hasOwn(row, column) || row[column] === undefined) {
          throw new Error(`Minnow result omitted column ${column}`);
        }
        return row[column];
      }),
    ),
    jsonColumns: domains.flatMap((domain, index) =>
      domain?.kind === "json" || domain?.kind === "jsonb" ? [index] : [],
    ),
    numericColumns: domains.flatMap((domain, index) => (domain?.kind === "numeric" ? [index] : [])),
  });
  return {
    name: "minnow",
    version: "published worker",
    execute: async (sql, params) => {
      await client.execute(sql, params);
    },
    executeQuery: async (sql, params) => {
      const result = await client.execute(sql, params);
      if (result.kind !== "rows") {
        throw new Error(`Minnow returned ${result.kind} for a row-returning statement`);
      }
      return rows(result.result.columns, result.result.rows, result.result.columnDomains);
    },
    query: async (sql, params) => {
      const result = await client.query(sql, {
        ...(params === undefined ? {} : { params }),
        memoize: false,
      });
      return rows(result.columns, result.rows, result.columnDomains);
    },
    mutate: async (sql, params) => {
      const result = await client.execute(sql, params);
      if (
        result.kind !== "insert" &&
        result.kind !== "update" &&
        result.kind !== "delete" &&
        result.kind !== "merge"
      ) {
        throw new Error(`Minnow returned ${result.kind} for a mutation`);
      }
      if (result.kind === "merge") {
        return { columns: [], rows: [], affectedRows: result.rowCount };
      }
      const projected = rows(
        result.returnedColumns ?? [],
        result.returnedRows ?? [],
        result.returnedColumnDomains ?? [],
      );
      return { ...projected, affectedRows: result.rowCount };
    },
    close: () => client.close({ terminateWorker: true }),
  };
}

interface SqliteStatement {
  readonly columnCount: number;
  bind(params: SqlParameter[]): void;
  getColumnNames(): string[];
  step(): boolean;
  get(target: unknown[]): unknown[];
  finalize(): void;
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
  changes(): number;
  close(): void;
}

interface SqliteModule {
  readonly default: () => Promise<{
    readonly version: { readonly libVersion: string };
    readonly oo1: { readonly DB: new (filename: string) => SqliteDatabase };
  }>;
}

/** A full URL keeps Vite from turning a public, native module into a transformed source import. */
function importNative(path: string): Promise<unknown> {
  return import(/* @vite-ignore */ new URL(path, window.location.href).href) as Promise<unknown>;
}

async function openSqlite(): Promise<Engine> {
  const module = (await importNative(SQLITE_ENTRY)) as SqliteModule;
  const sqlite = await module.default();
  const database = new sqlite.oo1.DB(":memory:");
  const run = (sql: string, params: readonly SqlParameter[] = []): ResultSet => {
    const statement = database.prepare(sql);
    try {
      if (params.length > 0) statement.bind([...params]);
      const columns = statement.columnCount === 0 ? [] : statement.getColumnNames();
      const rows: unknown[][] = [];
      while (statement.step()) rows.push(statement.get([]));
      return { columns, rows };
    } finally {
      statement.finalize();
    }
  };
  return {
    name: "sqlite",
    version: sqlite.version.libVersion,
    execute: async (sql, params) => {
      run(sql, params);
    },
    executeQuery: async (sql, params) => run(sql, params),
    query: async (sql, params) => run(sql, params),
    mutate: async (sql, params) => ({
      ...run(sql, params),
      affectedRows: database.changes(),
    }),
    close: async () => database.close(),
  };
}

interface PgliteModule {
  readonly PGlite: typeof PGlite;
}

async function openPglite(): Promise<Engine> {
  const module = (await importNative(PGLITE_ENTRY)) as PgliteModule;
  const database = await module.PGlite.create();
  await database.exec("SET TIME ZONE 'UTC'");
  const run = async (
    sql: string,
    params: readonly SqlParameter[] = [],
  ): Promise<MutationResult> => {
    const result = await database.query(positionalToNumbered(sql), [...params], {
      parsers: {
        20: (value: string) => Number(value),
        1700: (value: string) => Number(value),
        1114: (value: string) => new Date(`${value.replace(" ", "T")}Z`),
      },
    });
    const columns = result.fields.map(({ name }) => name);
    return {
      columns,
      rows: result.rows.map((row) =>
        columns.map((column) => (row as Record<string, unknown>)[column]),
      ),
      affectedRows: result.rowCount ?? result.affectedRows ?? 0,
    };
  };
  return {
    name: "pglite",
    version: `${postgresProfile.oracle.version} browser Wasm`,
    execute: async (sql, params) => {
      await run(sql, params);
    },
    executeQuery: run,
    query: run,
    mutate: run,
    close: () => database.close(),
  };
}

function classification(feature: MatrixFeature): Classification {
  const override = postgresOverrides.get(feature.id);
  if (override !== undefined) return override.classification;
  return feature.status === "supported"
    ? postgresProfile.defaults.supported
    : postgresProfile.defaults.unsupported;
}

/** Wasm libm and JavaScript Math may differ below this campaign's semantic precision. */
function needsExternalFloatTolerance(sql: string): boolean {
  return /\b(?:AVG|SUM|STDDEV(?:_POP|_SAMP)?|VARIANCE|VAR_POP|VAR_SAMP|COVAR_POP|COVAR_SAMP|CORR|EXP|LN|LOG|LOG10|SQRT|CBRT|POWER|SIN|COS|TAN|ASIN|ACOS|ATAN|ATAN2|RADIANS|DEGREES)\s*\(/i.test(
    sql,
  );
}

function writesData(id: string): boolean {
  return (
    id.startsWith("mutation.") ||
    id.startsWith("ddl.") ||
    id.startsWith("trigger.") ||
    id.startsWith("transaction.")
  );
}

async function resetFixture(engines: readonly Engine[], seed: number): Promise<void> {
  const statements = [
    "DROP TABLE IF EXISTS items",
    "DROP TABLE IF EXISTS dims",
    "CREATE TABLE items (id INTEGER PRIMARY KEY, region TEXT, amount DOUBLE PRECISION, active BOOLEAN, note TEXT, payload TEXT, joined TIMESTAMP)",
    "CREATE TABLE dims (region TEXT PRIMARY KEY, label TEXT, weight INTEGER)",
  ];
  for (const engine of engines) {
    for (const sql of statements) await engine.execute(sql);
    for (const row of fixtureRows(seed)) {
      await engine.execute("INSERT INTO items VALUES (?, ?, ?, ?, ?, ?, ?)", [
        row.id,
        row.region,
        row.amount,
        row.active,
        row.note,
        row.payload,
        row.joined,
      ]);
    }
    for (const dim of [
      ["west", "West Coast", 1],
      ["east", "East Coast", 2],
      ["north", "North", 3],
      ["central", "Central", 4],
    ] as const) {
      await engine.execute("INSERT INTO dims VALUES (?, ?, ?)", dim);
    }
  }
}

async function resetMatrixFixture(engines: readonly Engine[]): Promise<void> {
  const statements = [
    'DROP TABLE IF EXISTS "rows"',
    "DROP TABLE IF EXISTS dims",
    'CREATE TABLE "rows" (region TEXT, amount DOUBLE PRECISION, active BOOLEAN, joined TIMESTAMP)',
    "CREATE TABLE dims (region TEXT, label TEXT, amount DOUBLE PRECISION)",
  ];
  const rows = [
    ["west", 10, true, "2026-01-02T00:00:00.000Z"],
    ["west", 6, false, "2025-12-30T00:00:00.000Z"],
    ["east", 3, true, "2026-02-01T00:00:00.000Z"],
    [null, 8, true, null],
  ] as const;
  for (const engine of engines) {
    for (const sql of statements) await engine.execute(sql);
    for (const row of rows) await engine.execute('INSERT INTO "rows" VALUES (?, ?, ?, ?)', row);
    await engine.execute("INSERT INTO dims VALUES (?, ?, ?)", ["west", "West Coast", 1]);
    await engine.execute("INSERT INTO dims VALUES (?, ?, ?)", ["north", "North", 2]);
  }
}

function stableObject(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableObject);
  if (value !== null && typeof value === "object" && !(value instanceof Date)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, stableObject((value as Record<string, unknown>)[key])]),
    );
  }
  return value;
}

function canonicalValue(
  value: unknown,
  index: number,
  testCase: Pick<
    DifferentialQuery,
    "jsonColumns" | "numericColumns" | "datetimeColumns" | "numericDigits"
  >,
): unknown {
  if (testCase.jsonColumns?.includes(index) === true) {
    let document = value;
    if (typeof document === "string") {
      try {
        document = JSON.parse(document) as unknown;
      } catch {
        // PGlite already decodes a JSON string scalar to its unquoted JavaScript string.
      }
    }
    return ["json", stableObject(document)];
  }
  if (value instanceof Date) return ["datetime", value.toISOString()];
  if (testCase.datetimeColumns?.includes(index) === true && typeof value === "string") {
    return [
      "datetime",
      new Date(value.replace(" ", "T") + (value.endsWith("Z") ? "" : "Z")).toISOString(),
    ];
  }
  if (value === null) return ["null"];
  if (testCase.numericColumns?.includes(index) === true && typeof value === "string") {
    return canonicalValue(Number(value), index, testCase);
  }
  if (typeof value === "boolean") return ["number", value ? 1 : 0];
  if (typeof value === "bigint") return ["number", Number(value)];
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return ["number", String(value)];
    if (Object.is(value, -0)) return ["number", 0];
    return [
      "number",
      testCase.numericDigits === undefined ? value : Number(value.toFixed(testCase.numericDigits)),
    ];
  }
  if (typeof value === "object") return ["object", stableObject(value)];
  return [typeof value, value];
}

function resultKeys(result: ResultSet, testCase: DifferentialQuery): string[] {
  const effectiveCase: DifferentialQuery = {
    ...testCase,
    jsonColumns: [...new Set([...(testCase.jsonColumns ?? []), ...(result.jsonColumns ?? [])])],
    numericColumns: [
      ...new Set([...(testCase.numericColumns ?? []), ...(result.numericColumns ?? [])]),
    ],
  };
  const keys = result.rows.map((row) =>
    JSON.stringify(row.map((value, index) => canonicalValue(value, index, effectiveCase))),
  );
  return testCase.ordered === true ? keys : [...keys].sort();
}

function compare(
  label: string,
  testCase: DifferentialQuery,
  actual: ResultSet,
  expected: ResultSet,
): string | undefined {
  if (
    actual.columns.length !== expected.columns.length ||
    actual.columns.some((column, index) => column !== expected.columns[index])
  ) {
    return `${label}: columns differ\n  minnow: ${JSON.stringify(actual.columns)}\n  oracle: ${JSON.stringify(expected.columns)}`;
  }
  const effectiveCase: DifferentialQuery = {
    ...testCase,
    jsonColumns: [...new Set([...(testCase.jsonColumns ?? []), ...(actual.jsonColumns ?? [])])],
    numericColumns: [
      ...new Set([...(testCase.numericColumns ?? []), ...(actual.numericColumns ?? [])]),
    ],
  };
  const actualKeys = resultKeys(actual, effectiveCase);
  const expectedKeys = resultKeys(expected, effectiveCase);
  if (
    actualKeys.length === expectedKeys.length &&
    actualKeys.every((key, index) => key === expectedKeys[index])
  ) {
    return undefined;
  }
  return `${label}: rows differ\n  minnow: ${JSON.stringify(actualKeys)}\n  oracle: ${JSON.stringify(expectedKeys)}`;
}

async function compareCase(
  testCase: DifferentialQuery,
  minnow: Engine,
  oracles: ReadonlyMap<OracleName, Engine>,
  mutation: boolean,
  afterMinnow?: () => void,
): Promise<{ failures: string[]; comparisons: number }> {
  const failures: string[] = [];
  const selected = testCase.oracles ?? (["sqlite", "pglite"] as const);
  let actual: ResultSet;
  try {
    actual = await (mutation
      ? minnow.mutate(testCase.sql, testCase.params)
      : minnow.query(testCase.sql, testCase.params));
    afterMinnow?.();
  } catch (error) {
    return {
      failures: [`${testCase.label}: Minnow threw: ${String(error)}\n${testCase.sql}`],
      comparisons: 0,
    };
  }
  if (mutation) {
    const actualMutation = actual as MutationResult;
    if (
      testCase.affectedRows !== undefined &&
      actualMutation.affectedRows !== testCase.affectedRows
    ) {
      failures.push(
        `${testCase.label}: Minnow affected ${String(actualMutation.affectedRows)} rows, expected ${String(testCase.affectedRows)}\n${testCase.sql}`,
      );
    }
  }
  let comparisons = 0;
  for (const name of selected) {
    const oracle = oracles.get(name);
    if (oracle === undefined) continue;
    let expected: ResultSet;
    try {
      expected = await (mutation
        ? oracle.mutate(testCase.sql, testCase.params)
        : oracle.query(testCase.sql, testCase.params));
    } catch (error) {
      failures.push(`${testCase.label}: ${name} threw: ${String(error)}\n${testCase.sql}`);
      continue;
    }
    comparisons++;
    if (
      mutation &&
      (actual as MutationResult).affectedRows !== (expected as MutationResult).affectedRows
    ) {
      failures.push(
        `${testCase.label} vs ${name}: affected rows differ\n  minnow: ${String((actual as MutationResult).affectedRows)}\n  oracle: ${String((expected as MutationResult).affectedRows)}\n${testCase.sql}`,
      );
    }
    const failure = compare(`${testCase.label} vs ${name}`, testCase, actual, expected);
    if (failure !== undefined) failures.push(`${failure}\n${testCase.sql}`);
  }
  return { failures, comparisons };
}

interface BehaviorProbeResult {
  readonly failures: readonly string[];
  readonly steps: number;
  readonly oracleComparisons: number;
}

interface TimestampWindow {
  readonly firstMs: number;
  readonly secondMs: number;
}

function patternValueFailure(
  actual: unknown,
  expected: ExpectedFeatureValue,
  timestampWindow: TimestampWindow | undefined,
): string | undefined {
  if (typeof expected !== "object" || expected === null) {
    return Object.is(actual, expected)
      ? undefined
      : `expected ${JSON.stringify(expected)}, received ${JSON.stringify(actual)}`;
  }
  if (expected.matcher === "timestamp") {
    if (!(actual instanceof Date) || !Number.isFinite(actual.getTime())) {
      return `expected a valid Date, received ${JSON.stringify(actual)}`;
    }
    if (timestampWindow !== undefined) {
      const minimum = Math.min(timestampWindow.firstMs, timestampWindow.secondMs);
      const maximum = Math.max(timestampWindow.firstMs, timestampWindow.secondMs);
      if (actual.getTime() < minimum || actual.getTime() > maximum) {
        return `expected a statement-time Date between ${new Date(minimum).toISOString()} and ${new Date(maximum).toISOString()}, received ${actual.toISOString()}`;
      }
    }
    return undefined;
  }
  if (expected.matcher === "uuid") {
    return typeof actual === "string" &&
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(actual)
      ? undefined
      : `expected an RFC 4122 version-4 UUID, received ${JSON.stringify(actual)}`;
  }
  if (typeof actual !== "number" || !Number.isFinite(actual)) {
    return `expected a finite number, received ${JSON.stringify(actual)}`;
  }
  if (expected.minimumInclusive !== undefined && actual < expected.minimumInclusive) {
    return `expected at least ${String(expected.minimumInclusive)}, received ${String(actual)}`;
  }
  if (expected.minimumExclusive !== undefined && actual <= expected.minimumExclusive) {
    return `expected greater than ${String(expected.minimumExclusive)}, received ${String(actual)}`;
  }
  if (expected.maximumExclusive !== undefined && actual >= expected.maximumExclusive) {
    return `expected less than ${String(expected.maximumExclusive)}, received ${String(actual)}`;
  }
  return undefined;
}

function patternResultFailure(
  actual: ResultSet,
  expected: ExpectedFeaturePatternResult,
  timestampWindow: TimestampWindow | undefined,
): string | undefined {
  if (
    actual.columns.length !== expected.columns.length ||
    actual.columns.some((column, index) => column !== expected.columns[index])
  ) {
    return `columns differ\n  minnow: ${JSON.stringify(actual.columns)}\n  fixed expectation: ${JSON.stringify(expected.columns)}`;
  }
  if (actual.rows.length !== expected.rows.length) {
    return `row count differs\n  minnow: ${String(actual.rows.length)}\n  fixed expectation: ${String(expected.rows.length)}`;
  }
  for (const [rowIndex, expectedRow] of expected.rows.entries()) {
    const actualRow = actual.rows[rowIndex];
    if (actualRow?.length !== expectedRow.length) {
      return `row ${String(rowIndex)} width differs\n  minnow: ${JSON.stringify(actualRow)}\n  fixed expectation: ${String(expectedRow.length)} columns`;
    }
    for (const [columnIndex, expectedValue] of expectedRow.entries()) {
      const failure = patternValueFailure(actualRow[columnIndex], expectedValue, timestampWindow);
      if (failure !== undefined) {
        return `row ${String(rowIndex)} column ${String(columnIndex)} differs: ${failure}`;
      }
    }
  }
  return undefined;
}

async function runBehaviorProbe(
  probe: FeatureBehaviorProbe,
  minnow: Engine,
  pglite: Engine | undefined,
  timestampWindow: TimestampWindow | undefined,
): Promise<BehaviorProbeResult> {
  const failures: string[] = [];
  let steps = 0;
  let oracleComparisons = 0;
  for (const [index, step] of probe.steps.entries()) {
    const label = `${probe.featureId} behavior ${String(index + 1)}`;
    steps++;
    try {
      if (step.kind === "query") {
        const testCase: DifferentialQuery = {
          label,
          sql: step.sql,
          ...(step.expected.ordered === true ? { ordered: true } : {}),
        };
        const actual = await (step.throughExecute === true
          ? minnow.executeQuery(step.sql, step.params)
          : minnow.query(step.sql, step.params));
        const fixedFailure = compare(
          `${label} vs fixed expectation`,
          testCase,
          actual,
          step.expected,
        );
        if (fixedFailure !== undefined) failures.push(`${fixedFailure}\n${step.sql}`);
        if (step.compareWithPglite === true) {
          if (pglite === undefined) {
            failures.push(`${label}: PGlite was not available\n${step.sql}`);
          } else {
            const oracle = await pglite.query(step.sql, step.params);
            oracleComparisons++;
            const oracleFailure = compare(`${label} vs pglite`, testCase, actual, oracle);
            if (oracleFailure !== undefined) failures.push(`${oracleFailure}\n${step.sql}`);
          }
        }
      } else if (step.kind === "query-pattern") {
        const actual = await minnow.query(step.sql, step.params);
        const patternFailure = patternResultFailure(actual, step.expected, timestampWindow);
        if (patternFailure !== undefined) {
          failures.push(`${label} vs fixed pattern: ${patternFailure}\n${step.sql}`);
        }
      } else if (step.kind === "mutation") {
        const actual = await minnow.mutate(step.sql, step.params);
        if (actual.affectedRows !== step.affectedRows) {
          failures.push(
            `${label}: affected rows differ\n  minnow: ${String(actual.affectedRows)}\n  fixed expectation: ${String(step.affectedRows)}\n${step.sql}`,
          );
        }
        if (step.expected !== undefined) {
          const resultFailure = compare(
            `${label} vs fixed expectation`,
            { label, sql: step.sql, ...(step.expected.ordered === true ? { ordered: true } : {}) },
            actual,
            step.expected,
          );
          if (resultFailure !== undefined) failures.push(`${resultFailure}\n${step.sql}`);
        }
      } else if (step.kind === "execute") {
        await minnow.execute(step.sql, step.params);
      } else {
        let thrown: unknown;
        try {
          await minnow.execute(step.sql, step.params);
        } catch (error) {
          thrown = error;
        }
        if (thrown === undefined) {
          failures.push(
            `${label}: expected ${step.errorName} containing ${JSON.stringify(step.includes)}\n${step.sql}`,
          );
        } else if (!(thrown instanceof Error)) {
          failures.push(
            `${label}: expected ${step.errorName}, received a non-Error rejection\n${step.sql}`,
          );
        } else {
          if (thrown.name !== step.errorName) {
            failures.push(
              `${label}: error name differed\n  expected: ${step.errorName}\n  actual: ${thrown.name}\n${step.sql}`,
            );
          } else if (!thrown.message.includes(step.includes)) {
            failures.push(
              `${label}: error message differed\n  expected to include: ${JSON.stringify(step.includes)}\n  actual: ${thrown.message}\n${step.sql}`,
            );
          }
        }
      }
    } catch (error) {
      failures.push(`${label}: probe threw: ${String(error)}\n${step.sql}`);
    }
    if (failures.length > 0) break;
  }
  return { failures, steps, oracleComparisons };
}

function createdNames(pattern: RegExp): string[] {
  const statements = [
    ...matrixFeatures.flatMap((feature) => [...(feature.setup ?? []), feature.example]),
    ...featureBehaviorProbes.flatMap(({ steps }) => steps.map(({ sql }) => sql)),
  ];
  return [
    ...new Set(
      statements.flatMap((statement) => {
        const match = pattern.exec(statement);
        return match?.[1] === undefined ? [] : [match[1]];
      }),
    ),
  ];
}

const matrixCreatedTables = createdNames(
  /\bCREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?/i,
);
const matrixCreatedViews = createdNames(
  /\bCREATE\s+VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?/i,
);
const matrixCreatedTriggers = createdNames(/\bCREATE\s+TRIGGER\s+"?([A-Za-z_][A-Za-z0-9_]*)"?/i);

function identifier(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

async function rollbackIfOpen(engine: Engine): Promise<void> {
  try {
    await engine.execute("ROLLBACK");
  } catch {
    // Most examples do not open a transaction. The next fixture reset still proves clean state.
  }
}

/** The fresh keyed fixture used by feature-matrix.test.ts for every DDL or mutation example. */
async function resetWriteFixture(engine: Engine): Promise<void> {
  await rollbackIfOpen(engine);
  for (const trigger of matrixCreatedTriggers) {
    try {
      await engine.execute(`DROP TRIGGER ${identifier(trigger)}`);
    } catch {
      // A fresh fixture does not have most of the matrix's trigger names.
    }
  }
  for (const view of matrixCreatedViews) {
    await engine.execute(`DROP VIEW IF EXISTS ${identifier(view)}`);
  }
  for (const table of [...matrixCreatedTables].reverse()) {
    await engine.execute(`DROP TABLE IF EXISTS ${identifier(table)}`);
  }
  for (const table of ["keyed", "rows", "dims"]) {
    await engine.execute(`DROP TABLE IF EXISTS ${identifier(table)}`);
  }
  await engine.execute(
    "CREATE TABLE keyed (name TEXT PRIMARY KEY, score DOUBLE PRECISION NOT NULL, bonus DOUBLE PRECISION)",
  );
  await engine.execute("INSERT INTO keyed VALUES ('x', 1, NULL), ('y', -1, NULL)");
  await engine.execute("CREATE TABLE rows (region TEXT, amount DOUBLE PRECISION)");
  await engine.execute("INSERT INTO rows VALUES ('west', 1), ('east', 2)");
  await engine.execute("CREATE TABLE dims (region TEXT, label TEXT, amount DOUBLE PRECISION)");
  await engine.execute("INSERT INTO dims VALUES ('west', 'West Coast', 1), ('north', 'North', 2)");
}

async function preparePgliteWriteFixture(engine: Engine, index: number): Promise<void> {
  const schema = `browser_feature_${String(index)}`;
  await engine.execute(`CREATE SCHEMA ${schema}`);
  await engine.execute(`SET search_path TO ${schema}`);
  await engine.execute(
    "CREATE TABLE keyed (name TEXT PRIMARY KEY, score DOUBLE PRECISION NOT NULL, bonus DOUBLE PRECISION)",
  );
  await engine.execute("INSERT INTO keyed VALUES ('x', 1, NULL), ('y', -1, NULL)");
  await engine.execute("CREATE TABLE rows (region TEXT, amount DOUBLE PRECISION)");
  await engine.execute("INSERT INTO rows VALUES ('west', 1), ('east', 2)");
  await engine.execute("CREATE TABLE dims (region TEXT, label TEXT, amount DOUBLE PRECISION)");
  await engine.execute("INSERT INTO dims VALUES ('west', 'West Coast', 1), ('north', 'North', 2)");
}

async function fixtureFingerprint(engine: Engine, writeFixture: boolean): Promise<string> {
  const queries = writeFixture
    ? [
        "SELECT name, score, bonus FROM keyed ORDER BY name",
        "SELECT region, amount FROM rows ORDER BY region, amount",
      ]
    : [
        'SELECT region, amount, active, joined FROM "rows" ORDER BY region, amount',
        "SELECT region, label, amount FROM dims ORDER BY region, amount",
      ];
  const results = await Promise.all(queries.map((sql) => engine.query(sql)));
  return JSON.stringify(
    results.map((result) => ({
      columns: result.columns,
      rows: resultKeys(result, { label: "fixture fingerprint", sql: "", ordered: true }),
    })),
  );
}

async function runMatrix(
  minnow: Engine,
  pglite: Engine,
  failures: string[],
): Promise<BrowserSqlDifferentialResult["matrix"]> {
  let behaviorProbedFeatures = 0;
  let behaviorProbeSteps = 0;
  let behaviorProbeOracleComparisons = 0;
  const verifyBehavior = async (
    feature: MatrixFeature,
    oracle: Engine | undefined,
    timestampWindow?: TimestampWindow,
  ): Promise<readonly string[]> => {
    const probe = behaviorProbeByFeature.get(feature.id);
    if (probe === undefined) {
      const failure = `${feature.id}: matrix category requires an independently authored behavior probe`;
      failures.push(failure);
      return [failure];
    }
    behaviorProbedFeatures++;
    const result = await runBehaviorProbe(probe, minnow, oracle, timestampWindow);
    behaviorProbeSteps += result.steps;
    behaviorProbeOracleComparisons += result.oracleComparisons;
    failures.push(...result.failures);
    return result.failures;
  };

  await resetMatrixFixture([minnow, pglite]);
  const supported = matrixFeatures.filter(({ status }) => status === "supported");
  const reads = supported.filter(({ id }) => !writesData(id));
  let compatibleReadsCompared = 0;
  let compatibleReadAcceptance = 0;
  let nonportableReadsAccepted = 0;
  for (const feature of reads) {
    const testCase: DifferentialQuery = {
      label: `feature matrix ${feature.id}`,
      sql: feature.example,
      ...(feature.params === undefined ? {} : { params: feature.params }),
      ...(needsExternalFloatTolerance(feature.example) ? { numericDigits: 9 } : {}),
    };
    let actual: ResultSet;
    try {
      actual = await minnow.query(feature.example, feature.params);
    } catch (error) {
      failures.push(`${testCase.label}: Minnow threw: ${String(error)}\n${feature.example}`);
      continue;
    }
    const override = postgresOverrides.get(feature.id);
    if (classification(feature) !== "compatible") {
      const probeFailures = await verifyBehavior(feature, undefined);
      if (probeFailures.length === 0) nonportableReadsAccepted++;
      continue;
    }
    let expected: ResultSet;
    try {
      expected = await pglite.query(feature.example, feature.params);
    } catch (error) {
      failures.push(`${testCase.label}: PGlite threw: ${String(error)}\n${feature.example}`);
      continue;
    }
    if (override?.verification === "acceptance") {
      const probeFailures = await verifyBehavior(feature, pglite);
      if (probeFailures.length === 0) compatibleReadAcceptance++;
      continue;
    }
    compatibleReadsCompared++;
    const failure = compare(`${testCase.label} vs pglite`, testCase, actual, expected);
    if (failure !== undefined) failures.push(`${failure}\n${feature.example}`);
  }

  let compatibleMutationsCompared = 0;
  let compatibleWritesAccepted = 0;
  let nonportableWritesAccepted = 0;
  const writes = supported.filter(({ id }) => writesData(id));
  for (const [index, feature] of writes.entries()) {
    await resetWriteFixture(minnow);
    for (const statement of feature.setup ?? []) await minnow.execute(statement);
    const compatible = classification(feature) === "compatible";
    if (compatible) {
      await preparePgliteWriteFixture(pglite, index);
      for (const statement of feature.setup ?? []) await pglite.execute(statement);
    }
    const featureStartedAt = Date.now();
    try {
      if (feature.id.startsWith("mutation.")) {
        if (compatible) {
          let featureEndedAt = featureStartedAt;
          const result = await compareCase(
            {
              label: `feature matrix ${feature.id}`,
              sql: feature.example,
              ...(feature.params === undefined ? {} : { params: feature.params }),
            },
            minnow,
            new Map([["pglite", pglite]]),
            true,
            () => {
              featureEndedAt = Date.now();
            },
          );
          failures.push(...result.failures);
          const probe = behaviorProbeByFeature.get(feature.id);
          const timestampWindow =
            feature.id === "mutation.insert-runtime-values"
              ? { firstMs: featureStartedAt, secondMs: featureEndedAt }
              : undefined;
          const stateFailures =
            probe?.replacesCompatibleMutationState === true
              ? await verifyBehavior(feature, pglite, timestampWindow)
              : (
                  await compareCase(
                    {
                      label: `feature matrix ${feature.id} resulting keyed table`,
                      sql: "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
                      ordered: true,
                    },
                    minnow,
                    new Map([["pglite", pglite]]),
                    false,
                  )
                ).failures;
          if (probe?.replacesCompatibleMutationState !== true) failures.push(...stateFailures);
          if (result.failures.length === 0 && stateFailures.length === 0) {
            compatibleMutationsCompared++;
          }
        } else {
          const result = await minnow.mutate(feature.example, feature.params);
          const expectedCount = nonportableMutationCounts.get(feature.id);
          const probeFailures = await verifyBehavior(feature, undefined);
          if (expectedCount === undefined || result.affectedRows !== expectedCount) {
            failures.push(
              `${feature.id} :: ${feature.example}\n  affected rows: ${String(result.affectedRows)}, expected ${String(expectedCount)}`,
            );
          } else if (probeFailures.length === 0) {
            nonportableWritesAccepted++;
          }
        }
      } else {
        await minnow.execute(feature.example, feature.params);
        if (compatible) {
          await pglite.execute(feature.example, feature.params);
          const probeFailures = await verifyBehavior(feature, pglite);
          if (probeFailures.length === 0) compatibleWritesAccepted++;
        } else {
          const probeFailures = await verifyBehavior(feature, undefined);
          if (probeFailures.length === 0) nonportableWritesAccepted++;
        }
      }
    } catch (error) {
      failures.push(
        `${feature.id} :: ${feature.example}\n  write acceptance failed: ${String(error)}`,
      );
    } finally {
      await rollbackIfOpen(minnow);
      if (compatible) await rollbackIfOpen(pglite);
    }
  }

  let unsupportedRejected = 0;
  for (const feature of matrixFeatures.filter(({ status }) => status === "unsupported")) {
    if (writesData(feature.id)) await resetWriteFixture(minnow);
    else await resetMatrixFixture([minnow]);
    const before = await fixtureFingerprint(minnow, writesData(feature.id));
    try {
      for (const statement of feature.setup ?? []) await minnow.execute(statement);
      if (writesData(feature.id)) await minnow.execute(feature.example, feature.params);
      else await minnow.query(feature.example, feature.params);
      failures.push(
        `${feature.id} :: ${feature.example}\n  unsupported example unexpectedly succeeded`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (feature.error === undefined || !message.includes(feature.error)) {
        failures.push(
          `${feature.id} :: ${feature.example}\n  refusal differed\n  expected: ${String(feature.error)}\n  actual: ${message}`,
        );
      } else {
        unsupportedRejected++;
      }
    } finally {
      await rollbackIfOpen(minnow);
    }
    const after = await fixtureFingerprint(minnow, writesData(feature.id));
    if (before !== after) {
      failures.push(`${feature.id} :: ${feature.example}\n  refusal changed the fixture`);
    }
  }
  return {
    entries: matrixFeatures.length,
    supported: supported.length,
    compatibleReadsCompared,
    compatibleReadAcceptance,
    nonportableReadsAccepted,
    compatibleMutationsCompared,
    compatibleWritesAccepted,
    nonportableWritesAccepted,
    behaviorProbedFeatures,
    behaviorProbeSteps,
    behaviorProbeOracleComparisons,
    unsupportedRejected,
  };
}

export async function runBrowserSqlDifferential(request: {
  readonly seeds: readonly number[];
  readonly store: BrowserSqlDifferentialStore;
}): Promise<BrowserSqlDifferentialResult> {
  const started = performance.now();
  const failures: string[] = [];
  const workerErrors: string[] = [];
  const engines = await Promise.all([
    openMinnow(request.store, workerErrors),
    openSqlite(),
    openPglite(),
  ]);
  const minnow = engines[0];
  const sqlite = engines[1];
  const pglite = engines[2];
  const oracles = new Map<OracleName, Engine>([
    ["sqlite", sqlite],
    ["pglite", pglite],
  ]);
  let generatedCount = 0;
  let fixedCount = 0;
  let mutationCount = 0;
  let oracleComparisons = 0;
  let matrix: BrowserSqlDifferentialResult["matrix"] = {
    entries: 0,
    supported: 0,
    compatibleReadsCompared: 0,
    compatibleReadAcceptance: 0,
    nonportableReadsAccepted: 0,
    compatibleMutationsCompared: 0,
    compatibleWritesAccepted: 0,
    nonportableWritesAccepted: 0,
    behaviorProbedFeatures: 0,
    behaviorProbeSteps: 0,
    behaviorProbeOracleComparisons: 0,
    unsupportedRejected: 0,
  };
  try {
    for (const seed of request.seeds) {
      await resetFixture(engines, seed);
      const queries = [...fixedQueries, ...generatedQueries(seed)];
      fixedCount += fixedQueries.length;
      generatedCount += queries.length - fixedQueries.length;
      for (const testCase of queries) {
        const result = await compareCase(testCase, minnow, oracles, false);
        failures.push(...result.failures.map((failure) => `seed ${String(seed)}: ${failure}`));
        oracleComparisons += result.comparisons;
      }
      for (const testCase of mutationQueries) {
        const result = await compareCase(testCase, minnow, oracles, true);
        failures.push(...result.failures.map((failure) => `seed ${String(seed)}: ${failure}`));
        oracleComparisons += result.comparisons;
        mutationCount++;
        if (result.failures.length > 0) break;
        const postImage: DifferentialQuery = {
          label: `state after ${testCase.label}`,
          sql: "SELECT id, region, amount, active, note, payload, joined FROM items ORDER BY id",
          ordered: true,
          datetimeColumns: [6],
        };
        const state = await compareCase(postImage, minnow, oracles, false);
        failures.push(...state.failures.map((failure) => `seed ${String(seed)}: ${failure}`));
        oracleComparisons += state.comparisons;
      }
    }
    matrix = await runMatrix(minnow, pglite, failures);
  } catch (error) {
    failures.push(`campaign failed: ${String(error)}`);
  } finally {
    for (const engine of engines) {
      try {
        await engine.close();
      } catch (error) {
        failures.push(`${engine.name} close failed: ${String(error)}`);
      }
    }
  }
  return {
    seeds: request.seeds,
    versions: {
      minnow: minnow.version,
      sqlite: sqlite.version,
      pglite: pglite.version,
    },
    generatedQueries: generatedCount,
    fixedQueries: fixedCount,
    mutations: mutationCount,
    oracleComparisons,
    matrix,
    failures,
    workerErrors,
    elapsedMs: performance.now() - started,
  };
}

Object.assign(window, { runBrowserSqlDifferential });
const ready = document.querySelector("#ready");
if (ready !== null) ready.textContent = "Browser SQL differential runner ready";
