import { MemoryBlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import rawMatrix from "../../sql-feature-matrix.json";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import { bindPlanParameters, compileQuery, executeQuery, executeRowQuery } from "./query.js";

interface MatrixFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  /** Statements executed before the example, for examples that need prior state. */
  setup?: string[];
  /** Bound values for the example's placeholders, in order. */
  params?: Array<string | number | boolean | null>;
  error?: string;
  notes?: string;
}

const matrix = rawMatrix as {
  features: MatrixFeature[];
};

const rows: DatabaseRow[] = [
  { region: "west", amount: 10, active: true, joined: new Date("2026-01-02T00:00:00.000Z") },
  { region: "west", amount: 6, active: false, joined: new Date("2025-12-30T00:00:00.000Z") },
  { region: "east", amount: 3, active: true, joined: new Date("2026-02-01T00:00:00.000Z") },
  { region: null, amount: 8, active: true, joined: null },
];
const dims: DatabaseRow[] = [
  { region: "west", label: "West Coast", amount: 1 },
  { region: "north", label: "North", amount: 2 },
];
const tables = new Map([
  ["rows", rows],
  ["dims", dims],
]);

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
  await database.insertBatch("rows", {
    columns: { region: ["west", "east"], amount: [1, 2] },
  });
  return database;
}

describe("SQL feature matrix conformance", () => {
  const features = matrix.features;

  it("covers every feature with a unique identifier", () => {
    expect(features.length).toBeGreaterThan(40);
    expect(new Set(features.map(({ id }) => id)).size).toBe(features.length);
  });

  it("uses the PostgreSQL profile as its only compatibility taxonomy", () => {
    expect(features.filter((feature) => "feature" in feature)).toEqual([]);
  });

  it("explains every PostgreSQL form it does not support", () => {
    const unsupported = features.filter(({ status }) => status === "unsupported");
    for (const feature of unsupported) {
      expect(feature.error ?? "").not.toBe("");
      expect((feature.notes ?? "").length).toBeGreaterThan(20);
    }
  });

  for (const feature of features.filter(({ status }) => status === "supported")) {
    if (
      feature.id.startsWith("mutation.") ||
      feature.id.startsWith("ddl.") ||
      feature.id.startsWith("trigger.") ||
      feature.id.startsWith("transaction.")
    ) {
      it(`executes supported ${feature.id}`, async () => {
        const database = await keyedDatabase();
        for (const statement of feature.setup ?? []) await database.execute(statement);
        const result = await database.execute(feature.example, feature.params);
        expect(result.kind).not.toBe("rows");
        // A statement that opened a transaction must not leave it open for the next example.
        if (result.kind === "transaction" && result.action === "begin") {
          await database.execute("ROLLBACK");
        }
      });
      continue;
    }
    it(`executes supported ${feature.id} identically in both executors`, () => {
      const plan = bindPlanParameters(compileQuery(feature.example), feature.params);
      expect(executeQuery(plan, tables)).toEqual(executeRowQuery(plan, tables));
      // Both executors above share one optimized plan, so they agree on whatever the optimizer
      // handed them. Several features here -- MATCH, BM25, and the rest of the extensions -- have
      // no external oracle at all, which leaves an optimizer rewrite nothing to contradict it.
      // The unoptimized plan is that missing reference.
      //
      // Correlated subqueries are the documented exception: decorrelation is lowering rather
      // than optimization, so those plans have no runnable unoptimized form. They are named
      // here rather than caught by a bare try, so a feature that stops running unoptimized for
      // any other reason fails instead of quietly opting itself out.
      if (!feature.id.startsWith("subquery.correlated") && feature.id !== "from.lateral") {
        const raw = bindPlanParameters(
          compileQuery(feature.example, { optimize: false }),
          feature.params,
        );
        expect(executeRowQuery(raw, tables)).toEqual(executeRowQuery(plan, tables));
      }
    });
  }

  for (const feature of features.filter(({ status }) => status === "unsupported")) {
    it(`rejects unsupported ${feature.id} explicitly`, async () => {
      const error = feature.error ?? "";
      if (
        feature.id.startsWith("mutation.") ||
        feature.id.startsWith("transaction.") ||
        feature.id.startsWith("ddl.")
      ) {
        const database = await keyedDatabase();
        await expect(database.execute(feature.example)).rejects.toThrow(error);
        return;
      }
      expect(() => executeRowQuery(compileQuery(feature.example), tables)).toThrow(error);
    });
  }
});
