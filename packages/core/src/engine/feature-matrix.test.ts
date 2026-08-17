import { MemoryBlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import rawMatrix from "../../sql-feature-matrix.json";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import { bindPlanParameters, compileQuery, executeQuery, executeRowQuery } from "./query.js";

interface MatrixFeature {
  id: string;
  /** ISO/IEC 9075:2023 Annex F feature identifier, or "minnow" for an engine extension. */
  feature: string;
  status: "supported" | "unsupported";
  example: string;
  /** Statements executed before the example, for examples that need prior state. */
  setup?: string[];
  /** Bound values for the example's placeholders, in order. */
  params?: Array<string | number | boolean | null>;
  error?: string;
  notes?: string;
}

const matrix = rawMatrix as { features: MatrixFeature[] };

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

  it("keys every entry to the standard it claims", () => {
    // An entry with no feature identifier cannot be checked against the standard at all, and a
    // free-text one would drift; "minnow" is the single spelling for an extension SQL:2023 does
    // not define, so the extensions stay countable.
    const identifier = /^(?:[EFSTR]\d{3}(?:-\d{2})?|minnow)$/;
    for (const feature of features) {
      expect(`${feature.id}: ${feature.feature}`).toMatch(
        new RegExp(`^${feature.id.replaceAll(".", "\\.")}: (?:[EFSTR]\\d{3}(?:-\\d{2})?|minnow)$`),
      );
      expect(identifier.test(feature.feature)).toBe(true);
    }
  });

  it("explains every feature it does not support", () => {
    // A bare "unsupported" reads as an oversight. Each one carries the error a caller actually
    // sees plus the reason the engine declines, so the boundary is a decision, not a gap.
    const unsupported = features.filter(({ status }) => status === "unsupported");
    expect(unsupported.length).toBeGreaterThan(5);
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
