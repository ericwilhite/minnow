import { MemoryBlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import rawMatrix from "../../sql-feature-matrix.json";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import { compileQuery, executeQuery, executeRowQuery } from "./query.js";

interface MatrixFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
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
    ],
  });
  await database.insertBatch("keyed", {
    columns: { name: ["x", "y"], score: [1, -1] },
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

  for (const feature of features.filter(({ status }) => status === "supported")) {
    if (feature.id.startsWith("mutation.")) {
      it(`executes supported ${feature.id}`, async () => {
        const database = await keyedDatabase();
        const result = await database.execute(feature.example);
        expect(result.kind).not.toBe("rows");
      });
      continue;
    }
    it(`executes supported ${feature.id} identically in both executors`, () => {
      const plan = compileQuery(feature.example);
      expect(executeQuery(plan, tables)).toEqual(executeRowQuery(plan, tables));
    });
  }

  for (const feature of features.filter(({ status }) => status === "unsupported")) {
    it(`rejects unsupported ${feature.id} explicitly`, async () => {
      const error = feature.error ?? "";
      if (feature.id.startsWith("mutation.") || feature.id.startsWith("transaction.")) {
        const database = await keyedDatabase();
        await expect(database.execute(feature.example)).rejects.toThrow(error);
        return;
      }
      expect(() => executeRowQuery(compileQuery(feature.example), tables)).toThrow(error);
    });
  }
});
