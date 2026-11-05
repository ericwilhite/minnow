// eslint-disable-next-line no-restricted-imports -- Repository-only corpus IO, excluded from tarballs.
import { readFileSync } from "node:fs";
import fc from "fast-check";
import { MemoryBlockStore } from "../storage/index.js";
import { buildFixture, dims, templates } from "./conformance-source.js";
import { simpleWrite, sqlOf } from "./live-write-source.js";
import {
  createDatabaseDriver,
  generateInteractionPlan,
  runInteractionPlan,
} from "./interaction-simulator.js";
import { parseSqlLogicTest } from "./sqllogictest.js";
import { mulberry32, seedsFor } from "./seeds.js";
import type { Operation, Workload } from "./differential-harness.js";

const sql = (text: string): Operation => ({ kind: "sql", sql: text });
export const PROFILE_SEEDS = { fast: [24301], nightly: [24301, 24302, 24303, 24304] } as const;
export async function workloads(
  profile: "fast" | "nightly" = "fast",
  seeds: readonly number[] = seedsFor("differential-harness", PROFILE_SEEDS[profile]),
): Promise<Workload[]> {
  const result: Workload[] = [];
  for (const seed of seeds) {
    const rng = mulberry32(seed);
    const data = buildFixture(seed);
    result.push({
      name: `conformance:${String(seed)}`,
      operations: [
        sql(
          "CREATE TABLE data (id INTEGER PRIMARY KEY, region TEXT, amount DOUBLE PRECISION, active BOOLEAN, joined TIMESTAMPTZ, label TEXT, noted TIMESTAMP)",
        ),
        sql("CREATE TABLE dims (region TEXT, label TEXT, rank DOUBLE PRECISION)"),
        { kind: "insertBatch", table: "data", rows: data.map((row) => ({ ...row })) },
        { kind: "insertBatch", table: "dims", rows: dims },
        ...Array.from({ length: profile === "fast" ? 1 : 5 }, () =>
          templates.map((template): Operation => {
            const entry = template(rng);
            return {
              kind: "sql",
              sql: entry.sql,
              ...(entry.params ? { params: entry.params } : {}),
              ...(entry.ordered
                ? {
                    totalOrder:
                      "Source conformance Case.ordered contract: unique key or identical projected ties",
                  }
                : {}),
            };
          }),
        ).flat(),
      ],
    });
    result.push({
      name: `fast-check:${String(seed)}`,
      operations: [
        sql(
          "CREATE TABLE a (id INTEGER PRIMARY KEY, g TEXT, x INTEGER, p INTEGER REFERENCES a (id) NOT ENFORCED)",
        ),
        sql(
          "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a (id) NOT ENFORCED, y INTEGER)",
        ),
        ...fc
          .sample(simpleWrite, { seed, numRuns: profile === "fast" ? 30 : 300 })
          .flatMap((write) => [
            ...sqlOf(write).map(sql),
            sql("SELECT * FROM a"),
            sql("SELECT * FROM b"),
          ]),
      ],
    });
    // Capture only serial SQL operations. The original simulator retains ownership of
    // races, crash injection, maintenance and its independent shadow-model assertions.
    const plan = generateInteractionPlan(seed, {
      connections: 1,
      length: profile === "fast" ? 40 : 400,
    });
    const allowed = new Set([
      "createTable",
      "insert",
      "update",
      "delete",
      "select",
      "partition",
      "unionAll",
      "transaction",
      "reopen",
    ]);
    const operations: Operation[] = [];
    const store = new MemoryBlockStore();
    const driver = createDatabaseDriver(store);
    try {
      await runInteractionPlan(
        { ...plan, interactions: plan.interactions.filter((step) => allowed.has(step.kind)) },
        {
          async open(connection) {
            const inner = await driver.open(connection);
            return {
              async execute(text, params) {
                operations.push({ kind: "sql", sql: text, ...(params ? { params } : {}) });
                return inner.execute(text, params);
              },
              async query(text, params) {
                operations.push({
                  kind: "sql",
                  sql: text,
                  ...(params ? { params } : {}),
                  ...(/^SELECT \* FROM "t\d+" WHERE .* ORDER BY id(?: DESC)?(?: LIMIT \d+)?$/.test(
                    text,
                  )
                    ? {
                        totalOrder:
                          "Simulator table id is a primary key; single-table projection ordered by id",
                      }
                    : {}),
                });
                return inner.query(text, params);
              },
              async reopen() {
                operations.push({ kind: "reopen" });
                await inner.reopen();
              },
            };
          },
        },
      );
    } finally {
      await driver.close?.();
      store.close();
    }
    result.push({ name: `interaction:${String(seed)}`, operations });
  }
  for (let index = 1; index <= (profile === "fast" ? 1 : 5); index++) {
    const file = new URL(
      `../../testdata/sqllogictest/standard-select${String(index)}.test`,
      import.meta.url,
    );
    const records = parseSqlLogicTest(readFileSync(file, "utf8"), file.pathname);
    const operations: Operation[] = [];
    for (const record of records) {
      if (record.kind === "halt") break;
      if (record.kind !== "query" && record.kind !== "statement") continue;
      if (
        record.conditions.some((condition) =>
          condition.kind === "onlyif"
            ? condition.engine !== "minnow"
            : condition.engine === "minnow",
        )
      )
        continue;
      // SLT sort modes concern the reference formatter, not proof of a total SQL order.
      operations.push(sql(record.sql));
      if (profile === "fast" && operations.length >= 50) break;
    }
    result.push({ name: `sqllogictest:${String(index)}`, operations });
  }
  result.push({
    name: "api-contract",
    operations: [
      {
        kind: "schema",
        schema: {
          name: "api",
          uniqueKey: "id",
          columns: [
            { name: "id", type: "number" },
            { name: "v", type: "string" },
          ],
        },
      },
      {
        kind: "insertBatch",
        table: "api",
        rows: [
          { id: 1, v: "first" },
          { id: 2, v: "second" },
        ],
      },
      { kind: "upsertBatch", table: "api", rows: [{ id: 2, v: "changed" }] },
      sql("BEGIN"),
      sql("UPDATE api SET v = 'rolled back'"),
      sql("ROLLBACK"),
      sql("BEGIN"),
      sql("UPDATE api SET v = 'committed' WHERE id = 1 RETURNING id, v"),
      sql("COMMIT"),
      { kind: "reopen" },
      sql("SELECT * FROM api"),
      { kind: "deleteBatch", table: "api", keys: [1] },
      sql("SELECT * FROM api"),
      sql(
        "SELECT CAST('1.20' AS NUMERIC) AS n, CAST('2026-01-01' AS DATE) AS d, TRUE AS b, NULL AS z",
      ),
      sql("SELECT * FROM missing_table"),
    ],
  });
  return result;
}
