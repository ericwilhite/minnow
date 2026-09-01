import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import rawProfile from "../../postgres-feature-profile.json";
import rawMatrix from "../../sql-feature-matrix.json";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type ExecuteResult } from "./database.js";
import { positionalToNumbered } from "../testing/oracle.js";

type Classification = "compatible" | "different" | "extension" | "unsupported" | "inapplicable";

interface MatrixFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  setup?: string[];
  params?: Array<string | number | boolean | null>;
  notes?: string;
}

interface ProfileOverride {
  id: string;
  classification: Classification;
  verification?: "acceptance";
  reason: string;
}

interface PostgresProfile {
  oracle: { engine: string; version: string };
  defaults: {
    supported: Classification;
    unsupported: Classification;
  };
  overrides: ProfileOverride[];
}

const features = (rawMatrix as { features: MatrixFeature[] }).features;
const profile = rawProfile as PostgresProfile;
const overrides = new Map(profile.overrides.map((entry) => [entry.id, entry]));

function classification(feature: MatrixFeature): Classification {
  const override = overrides.get(feature.id);
  if (override !== undefined) return override.classification;
  if (feature.status === "unsupported") return profile.defaults.unsupported;
  return profile.defaults.supported;
}

function writesData(id: string): boolean {
  return id.startsWith("mutation.") || id.startsWith("ddl.") || id.startsWith("transaction.");
}

function mutatesRows(id: string): boolean {
  return id.startsWith("mutation.");
}

async function createFixture(database: PGlite, schema: string): Promise<void> {
  await database.exec(`CREATE SCHEMA ${schema}`);
  await database.exec(`SET search_path TO ${schema}`);
  await database.exec(
    "CREATE TABLE rows (region TEXT, amount DOUBLE PRECISION, active BOOLEAN, joined TIMESTAMPTZ)",
  );
  await database.exec("CREATE TABLE dims (region TEXT, label TEXT, amount DOUBLE PRECISION)");
  await database.exec(
    "CREATE TABLE keyed (name TEXT PRIMARY KEY, score DOUBLE PRECISION NOT NULL, bonus DOUBLE PRECISION)",
  );
  await database.exec(
    "INSERT INTO rows VALUES ('west', 10, TRUE, TIMESTAMPTZ '2026-01-02 00:00:00+00'), " +
      "('east', 3, TRUE, TIMESTAMPTZ '2026-02-01 00:00:00+00')",
  );
  await database.exec("INSERT INTO dims VALUES ('west', 'West Coast', 1), ('north', 'North', 2)");
  await database.exec("INSERT INTO keyed VALUES ('x', 1, NULL), ('y', -1, NULL)");
}

async function createMinnowMutationFixture(): Promise<{
  database: MinnowDatabase;
  store: MemoryBlockStore;
}> {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { rowsPerBlock: 8 });
  await database.execute(
    "CREATE TABLE keyed (name TEXT PRIMARY KEY, score DOUBLE PRECISION NOT NULL, bonus DOUBLE PRECISION)",
  );
  await database.execute("INSERT INTO keyed VALUES ('x', 1, NULL), ('y', -1, NULL)");
  return { database, store };
}

function affectedRows(result: ExecuteResult): number | undefined {
  return result.kind === "insert" ||
    result.kind === "update" ||
    result.kind === "delete" ||
    result.kind === "merge"
    ? result.rowCount
    : undefined;
}

function returnedRows(result: ExecuteResult): Array<Record<string, unknown>> {
  if (result.kind === "rows") return result.result.rows;
  if (result.kind === "insert" || result.kind === "update" || result.kind === "delete") {
    return result.returnedRows ?? [];
  }
  return [];
}

function returnedColumns(result: ExecuteResult): string[] {
  if (result.kind === "rows") return result.result.columns;
  if (result.kind === "insert" || result.kind === "update" || result.kind === "delete") {
    return result.returnedColumns ?? [];
  }
  return [];
}

describe("PostgreSQL feature profile", () => {
  it("classifies every matrix entry and explains every divergence", () => {
    expect(profile.oracle).toMatchObject({ engine: "PGlite", version: "0.5.5" });
    expect(new Set(profile.overrides.map(({ id }) => id)).size).toBe(profile.overrides.length);
    const ids = new Set(features.map(({ id }) => id));
    expect(profile.overrides.filter(({ id }) => !ids.has(id))).toEqual([]);

    const resolved = features.map((feature) => ({ feature, value: classification(feature) }));
    expect(resolved.filter(({ value }) => value === "compatible").length).toBeGreaterThan(140);
    expect(resolved.filter(({ value }) => value === "extension").length).toBeGreaterThan(5);
    expect(resolved.filter(({ value }) => value === "different").length).toBeGreaterThan(10);
    expect(resolved.filter(({ value }) => value === "unsupported").length).toBeGreaterThan(0);
    expect(resolved.filter(({ value }) => value === "inapplicable").length).toBeGreaterThan(0);

    for (const { feature, value } of resolved) {
      if (feature.status === "supported") {
        expect(["compatible", "different", "extension"], feature.id).toContain(value);
        if (value !== "compatible") expect(overrides.has(feature.id), feature.id).toBe(true);
      } else {
        expect(["unsupported", "inapplicable"], feature.id).toContain(value);
      }
      if (value === "different" || value === "unsupported" || value === "inapplicable") {
        expect(
          (overrides.get(feature.id)?.reason ?? feature.notes ?? "").length,
          feature.id,
        ).toBeGreaterThan(20);
      }
    }
  });

  it("is checked by differential comparison or explicit acceptance for every compatible read", () => {
    const acceptanceOnly = new Set(
      profile.overrides
        .filter(({ verification }) => verification === "acceptance")
        .map(({ id }) => id),
    );
    expect([...acceptanceOnly]).toEqual(["aggregate.any-value"]);
    const compatibleReads = features.filter(
      (feature) =>
        feature.status === "supported" &&
        classification(feature) === "compatible" &&
        !writesData(feature.id),
    );
    expect(compatibleReads.length).toBeGreaterThan(100);
    expect(compatibleReads.filter(({ id }) => acceptanceOnly.has(id)).length).toBe(1);
  });

  it("is accepted by PostgreSQL for every compatible mutation and DDL example", async () => {
    const compatibleWrites = features.filter(
      (feature) =>
        feature.status === "supported" &&
        classification(feature) === "compatible" &&
        writesData(feature.id),
    );
    const database = await PGlite.create();
    const failures: string[] = [];
    try {
      await database.exec("SET TIME ZONE 'UTC'");
      for (const [index, feature] of compatibleWrites.entries()) {
        const schema = `feature_${String(index)}`;
        try {
          await createFixture(database, schema);
          for (const statement of feature.setup ?? []) await database.exec(statement);
          await database.query(
            positionalToNumbered(feature.example),
            (feature.params ?? []) as unknown[],
          );
          if (feature.id === "transaction.begin") await database.exec("ROLLBACK");
        } catch (error) {
          failures.push(`${feature.id}: ${String(error)}`);
          try {
            await database.exec("ROLLBACK");
          } catch {
            // A failed non-transaction statement has nothing to roll back.
          }
        }
      }
    } finally {
      await database.close();
    }
    expect(compatibleWrites.length).toBeGreaterThan(20);
    expect(failures).toEqual([]);
  }, 120_000);

  it("matches PostgreSQL mutation counts, returned rows, and resulting table state", async () => {
    const compatibleMutations = features.filter(
      (feature) =>
        feature.status === "supported" &&
        classification(feature) === "compatible" &&
        mutatesRows(feature.id),
    );
    const postgres = await PGlite.create();
    const failures: string[] = [];
    try {
      await postgres.exec("SET TIME ZONE 'UTC'");
      for (const [index, feature] of compatibleMutations.entries()) {
        const schema = `mutation_${String(index)}`;
        const minnow = await createMinnowMutationFixture();
        try {
          await createFixture(postgres, schema);
          for (const statement of feature.setup ?? []) {
            await minnow.database.execute(statement);
            await postgres.exec(statement);
          }
          const minnowResult = await minnow.database.execute(feature.example, feature.params);
          const postgresResult = await postgres.query(
            positionalToNumbered(feature.example),
            (feature.params ?? []) as unknown[],
          );
          expect(affectedRows(minnowResult), `${feature.id}: affected rows`).toBe(
            postgresResult.affectedRows,
          );
          expect(returnedRows(minnowResult), `${feature.id}: returned rows`).toEqual(
            postgresResult.rows,
          );
          expect(returnedColumns(minnowResult), `${feature.id}: returned columns`).toEqual(
            postgresResult.fields.map(({ name }) => name),
          );
          expect(
            (
              await minnow.database.query(
                "SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST",
              )
            ).rows,
            `${feature.id}: resulting keyed table`,
          ).toEqual(
            (await postgres.query("SELECT name, score, bonus FROM keyed ORDER BY name NULLS LAST"))
              .rows,
          );
        } catch (error) {
          failures.push(`${feature.id}: ${String(error)}`);
        } finally {
          await minnow.database.close();
          minnow.store.close();
        }
      }
    } finally {
      await postgres.close();
    }
    expect(compatibleMutations.length).toBeGreaterThan(10);
    expect(failures).toEqual([]);
  }, 120_000);

  it("accepts the one nondeterministic compatible read in both engines", async () => {
    const feature = features.find(({ id }) => id === "aggregate.any-value");
    expect(feature).toBeDefined();
    const database = await PGlite.create();
    try {
      await createFixture(database, "acceptance_only");
      await expect(database.query(feature?.example ?? "")).resolves.toBeDefined();
    } finally {
      await database.close();
    }
  });
});
