/**
 * Live SQL conformance run over the shipped feature matrix. MinnowDatabase executes
 * every example the same way the engine's own test suite does: query examples through
 * compileQuery/executeQuery over an in-memory fixture, mutation and transaction
 * examples through database.execute() on a throwaway keyed database. Unsupported
 * examples must still fail with the recorded error — anything else is drift. SQLite and
 * PGlite run the same statements against an identical fixture, purely as information
 * about where other engines' surfaces differ.
 */
import sqlFeatureMatrix from "@minnowdb/core/sql-feature-matrix.json";
import {
  MinnowDatabase,
  bindPlanParameters,
  compileQuery,
  executeQuery,
  type DatabaseRow,
} from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { engineIds, engineNames } from "../protocol.js";
import type {
  EngineId,
  FeatureEngineOutcome,
  FeatureReport,
  FeatureSuitePayload,
  FeatureSuiteResult,
} from "../protocol.js";
import { assertNotCancelled, progress } from "./support.js";

interface MatrixFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  /** Bound values for the example's placeholders, in order. */
  params?: Array<string | number | boolean | null>;
  error?: string;
  notes?: string;
}

const matrix = sqlFeatureMatrix as { version: number; features: MatrixFeature[] };

const fixtureRows: DatabaseRow[] = [
  { region: "west", amount: 10, active: true, joined: new Date("2026-01-02T00:00:00.000Z") },
  { region: "west", amount: 6, active: false, joined: new Date("2025-12-30T00:00:00.000Z") },
  { region: "east", amount: 3, active: true, joined: new Date("2026-02-01T00:00:00.000Z") },
  { region: null, amount: 8, active: true, joined: null },
];
const fixtureDims: DatabaseRow[] = [
  { region: "west", label: "West Coast", amount: 1 },
  { region: "north", label: "North", amount: 2 },
];
const fixtureTables = new Map([
  ["rows", fixtureRows],
  ["dims", fixtureDims],
]);

function usesDatabase(feature: MatrixFeature): boolean {
  return (
    feature.id.startsWith("mutation.") ||
    feature.id.startsWith("transaction.") ||
    feature.id.startsWith("ddl.")
  );
}

export function validateFeaturePayload(value: unknown): FeatureSuitePayload {
  if (typeof value !== "object" || value === null) throw new Error("Invalid suite payload");
  const payload = value as Partial<FeatureSuitePayload>;
  if (
    !Array.isArray(payload.engines) ||
    payload.engines.length === 0 ||
    !payload.engines.every((engine): engine is EngineId => engineIds.includes(engine))
  ) {
    throw new Error("Select at least one engine");
  }
  return { engines: [...new Set(payload.engines)] };
}

export async function runFeatureSuite(
  requestId: string,
  payload: FeatureSuitePayload,
): Promise<FeatureSuiteResult> {
  const features = matrix.features;
  const runners = new Map<EngineId, FeatureRunner>();
  for (const engine of payload.engines) runners.set(engine, await createRunner(engine));
  const reports: FeatureReport[] = [];
  try {
    for (const [index, feature] of features.entries()) {
      assertNotCancelled(requestId);
      progress(requestId, {
        phase: "conformance",
        completed: index,
        total: features.length,
        message: `${feature.id} · ${payload.engines.map((engine) => engineNames[engine]).join(", ")}`,
      });
      const outcomes: FeatureEngineOutcome[] = [];
      for (const engine of payload.engines) {
        const runner = runners.get(engine);
        if (runner === undefined) continue;
        outcomes.push(await measureFeature(runner, engine, feature));
      }
      reports.push({
        id: feature.id,
        status: feature.status,
        example: feature.example,
        ...(feature.error === undefined ? {} : { expectedError: feature.error }),
        ...(feature.notes === undefined ? {} : { notes: feature.notes }),
        engines: outcomes,
      });
    }
  } finally {
    for (const runner of runners.values()) await runner.close();
  }
  const driftFailures = reports.filter((report) =>
    report.engines.some((outcome) => outcome.engine === "minnow" && outcome.outcome === "fail"),
  ).length;
  progress(requestId, {
    phase: "complete",
    completed: features.length,
    total: features.length,
    message: "Conformance run complete",
  });
  return {
    matrixVersion: matrix.version,
    supportedCount: features.filter((feature) => feature.status === "supported").length,
    unsupportedCount: features.filter((feature) => feature.status === "unsupported").length,
    engines: payload.engines,
    features: reports,
    driftFailures,
  };
}

async function measureFeature(
  runner: FeatureRunner,
  engine: EngineId,
  feature: MatrixFeature,
): Promise<FeatureEngineOutcome> {
  const started = performance.now();
  let error: string | undefined;
  try {
    await runner.execute(feature);
  } catch (caught) {
    error = caught instanceof Error ? caught.message : String(caught);
  }
  const ms = performance.now() - started;
  if (engine === "minnow") {
    if (feature.status === "supported") {
      return error === undefined
        ? { engine, outcome: "pass", ms }
        : { engine, outcome: "fail", ms, detail: error };
    }
    // An unsupported example must still fail with the recorded error; executing
    // successfully or failing differently both mean the matrix has drifted.
    if (error === undefined) {
      return {
        engine,
        outcome: "fail",
        ms,
        detail: "example executed but is cataloged as unsupported",
      };
    }
    return error.includes(feature.error ?? "")
      ? { engine, outcome: "pass", ms }
      : { engine, outcome: "fail", ms, detail: `unexpected error: ${error}` };
  }
  return error === undefined
    ? { engine, outcome: "accepts", ms }
    : { engine, outcome: "rejects", ms, detail: error };
}

interface FeatureRunner {
  execute(feature: MatrixFeature): Promise<void>;
  close(): Promise<void>;
}

async function createRunner(engine: EngineId): Promise<FeatureRunner> {
  switch (engine) {
    case "minnow":
      return Promise.resolve(minnowRunner());
    case "sqlite":
      return sqliteRunner();
    case "pglite":
      return pgliteRunner();
  }
}

function minnowRunner(): FeatureRunner {
  return {
    async execute(feature) {
      if (usesDatabase(feature)) {
        const database = await keyedDatabase();
        await database.execute(feature.example, feature.params);
        return;
      }
      executeQuery(
        bindPlanParameters(compileQuery(feature.example), feature.params),
        fixtureTables,
      );
    },
    close: () => Promise.resolve(),
  };
}

/** Mirrors the fixture the engine's own conformance test seeds for mutation examples. */
async function keyedDatabase(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
  await database.createTable({
    name: "keyed",
    uniqueKey: "name",
    columns: [
      { name: "name", type: "string" },
      { name: "score", type: "number" },
      { name: "bonus", type: "number", nullable: true },
    ],
  });
  await database.insertBatch("keyed", {
    columns: { name: ["x", "y"], score: [1, -1], bonus: [null, null] },
  });
  await database.createTable({
    name: "rows",
    columns: [
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("rows", { columns: { region: ["west", "east"], amount: [1, 2] } });
  return database;
}

const fixtureDdl = [
  `CREATE TABLE "rows" ("region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMP)`,
  `INSERT INTO "rows" ("region", "amount", "active", "joined") VALUES ` +
    `('west', 10, TRUE, '2026-01-02T00:00:00.000Z'), ` +
    `('west', 6, FALSE, '2025-12-30T00:00:00.000Z'), ` +
    `('east', 3, TRUE, '2026-02-01T00:00:00.000Z'), ` +
    `(NULL, 8, TRUE, NULL)`,
  `CREATE TABLE "dims" ("region" TEXT, "label" TEXT, "amount" DOUBLE PRECISION)`,
  `INSERT INTO "dims" ("region", "label", "amount") VALUES ('west', 'West Coast', 1), ('north', 'North', 2)`,
  `CREATE TABLE "keyed" ("name" TEXT PRIMARY KEY, "score" DOUBLE PRECISION, "bonus" DOUBLE PRECISION)`,
  `INSERT INTO "keyed" ("name", "score", "bonus") VALUES ('x', 1, NULL), ('y', -1, NULL)`,
];

const fixtureTeardown = [
  `DROP TABLE IF EXISTS "rows"`,
  `DROP TABLE IF EXISTS "dims"`,
  `DROP TABLE IF EXISTS "keyed"`,
  `DROP TABLE IF EXISTS "made"`,
];

async function sqliteRunner(): Promise<FeatureRunner> {
  const { default: sqlite3InitModule } = await import("@sqlite.org/sqlite-wasm");
  const sqlite3 = await sqlite3InitModule();
  const database = new sqlite3.oo1.DB(":memory:");
  return {
    execute(feature) {
      // A fresh fixture per example keeps mutation and DDL examples from leaking into
      // the next measurement.
      for (const sql of fixtureTeardown) database.exec(sql);
      for (const sql of fixtureDdl) database.exec(sql);
      if (feature.params === undefined) database.exec(feature.example);
      else database.exec({ sql: feature.example, bind: feature.params });
      return Promise.resolve();
    },
    close() {
      database.close();
      return Promise.resolve();
    },
  };
}

async function pgliteRunner(): Promise<FeatureRunner> {
  const { PGlite } = await import("@electric-sql/pglite");
  const database = await PGlite.create("memory://");
  return {
    async execute(feature) {
      for (const sql of fixtureTeardown) await database.exec(sql);
      for (const sql of fixtureDdl) await database.exec(sql);
      if (feature.params === undefined) await database.exec(feature.example);
      else await database.query(feature.example, [...feature.params]);
    },
    async close() {
      if (!database.closed) await database.close();
    },
  };
}
