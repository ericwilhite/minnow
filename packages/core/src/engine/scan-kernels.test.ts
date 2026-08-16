/**
 * Differential coverage for the scan's unboxed kernels and its ascending-column narrowing,
 * against a SQLite oracle.
 *
 * The main conformance corpus runs on 150 rows, which is below the batch size the narrowing
 * requires, so none of these paths are reachable from it. Every table here is deliberately
 * larger than one batch, and the fixtures cover what each fast path is allowed to assume:
 * an ascending key (binary search), a non-ascending column (must not binary search), NULLs
 * (three-valued logic in disjunctions and NOT IN), and duplicate keys (range merging).
 */
import { DatabaseSync } from "node:sqlite";
import { beforeAll, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

/** Comfortably more than the 2048-row batch the narrowing needs before it engages. */
const ROWS = 12_000;

let minnow: MinnowDatabase;
let sqlite: DatabaseSync;

function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state |= 0;
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

beforeAll(async () => {
  const rng = mulberry32(0xc0ffee);
  const regions = ["west", "east", "north", null];
  const ids: number[] = [];
  const scattered: number[] = [];
  const amounts: number[] = [];
  const buckets: number[] = [];
  const regionValues: Array<string | null> = [];
  for (let index = 0; index < ROWS; index += 1) {
    ids.push(index + 1);
    // Deliberately not ascending, so the narrowing must decline it.
    scattered.push(Math.floor(rng() * 5_000));
    // Every value nullable, so NOT IN and disjunctions meet unknowns.
    amounts.push(rng() < 0.08 ? Number.NaN : Math.floor(rng() * 900));
    // Long ascending runs of equal values, to exercise range merging.
    buckets.push(Math.floor(index / 500));
    regionValues.push(regions[Math.floor(rng() * regions.length)] ?? null);
  }
  const nullableAmounts = amounts.map((value) => (Number.isNaN(value) ? null : value));

  minnow = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 4_000 });
  await minnow.createTable({
    name: "data",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "scattered", type: "number" },
      { name: "amount", type: "number", nullable: true },
      { name: "bucket", type: "number" },
      { name: "region", type: "string", nullable: true },
    ],
  });
  for (let start = 0; start < ROWS; start += 4_000) {
    const end = Math.min(start + 4_000, ROWS);
    await minnow.insertBatch("data", {
      columns: {
        id: ids.slice(start, end),
        scattered: scattered.slice(start, end),
        amount: nullableAmounts.slice(start, end),
        bucket: buckets.slice(start, end),
        region: regionValues.slice(start, end),
      },
    });
  }

  sqlite = new DatabaseSync(":memory:");
  sqlite.exec(
    "CREATE TABLE data (id INTEGER PRIMARY KEY, scattered REAL, amount REAL, bucket REAL, region TEXT)",
  );
  const insert = sqlite.prepare("INSERT INTO data VALUES (?, ?, ?, ?, ?)");
  sqlite.exec("BEGIN");
  for (let index = 0; index < ROWS; index += 1) {
    insert.run(
      ids[index] ?? 0,
      scattered[index] ?? 0,
      nullableAmounts[index] ?? null,
      buckets[index] ?? 0,
      regionValues[index] ?? null,
    );
  }
  sqlite.exec("COMMIT");
});

/** Runs one statement through both engines and requires identical rows in identical order. */
async function expectMatchesOracle(sql: string): Promise<void> {
  const actual = await minnow.query(sql, { memoize: false });
  const expected = sqlite.prepare(sql).all() as Array<Record<string, unknown>>;
  const normalize = (rows: Array<Record<string, unknown>>): Array<Record<string, unknown>> =>
    rows.map((row) =>
      Object.fromEntries(
        Object.entries(row).map(([key, value]) => [
          key,
          typeof value === "number" && Number.isFinite(value)
            ? Math.round(value * 1e6) / 1e6
            : (value ?? null),
        ]),
      ),
    );
  expect(normalize(actual.rows as Array<Record<string, unknown>>), sql).toEqual(
    normalize(expected),
  );
}

describe("scan kernels and ascending narrowing", () => {
  it("locates scattered IN members on an ascending key", async () => {
    // The span between the smallest and largest member covers nearly the table, so each member
    // is located on its own; the rows between them must still be excluded.
    await expectMatchesOracle(
      "SELECT id, amount FROM data WHERE id IN (7, 2222, 6001, 9999, 11999) ORDER BY id",
    );
    await expectMatchesOracle("SELECT id FROM data WHERE id IN (1, 12000) ORDER BY id");
    // Members that miss entirely, and members outside the table's range.
    await expectMatchesOracle("SELECT id FROM data WHERE id IN (-5, 0, 12001, 50000) ORDER BY id");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE id IN (4000, 4001, 4002)");
  });

  it("merges adjacent members and repeated values into one range", async () => {
    await expectMatchesOracle(
      "SELECT id FROM data WHERE id IN (500, 501, 502, 503, 504) ORDER BY id",
    );
    // bucket ascends in long runs of equal values: each member covers 500 rows.
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE bucket IN (3, 4, 17)");
    await expectMatchesOracle(
      "SELECT bucket, COUNT(*) AS n FROM data WHERE bucket IN (0, 1, 23) GROUP BY bucket ORDER BY bucket",
    );
  });

  it("declines to binary search a column that is not ascending", async () => {
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE scattered IN (10, 900, 4321)");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE scattered = 42");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE scattered BETWEEN 100 AND 200");
  });

  it("keeps NOT IN unknown-safe and unnarrowed", async () => {
    // amount is nullable: a NULL row is unknown for both IN and NOT IN, so neither keeps it.
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE amount NOT IN (5, 10, 15)");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE amount IN (5, 10, 15)");
    // NOT IN over the ascending key keeps everything outside the member span -- the exact rows
    // a span-narrowing would have discarded.
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE id NOT IN (5, 6, 7)");
    await expectMatchesOracle("SELECT id FROM data WHERE id NOT IN (2, 3) AND id < 8 ORDER BY id");
    // A NULL member makes NOT IN never true, and IN true only on a match.
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE amount NOT IN (5, NULL)");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE amount IN (5, NULL)");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE id NOT IN (5, NULL)");
  });

  it("evaluates disjunctions across columns and types", async () => {
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE amount = 100 OR region = 'west'",
    );
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE amount > 880 OR region = 'east' OR bucket = 3",
    );
    // OR of AND-groups: the disjunctive-normal-form shape the kernel is built for.
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE (amount > 880 AND region = 'east') OR (amount < 20 AND region = 'west')",
    );
    // A NULL-bearing branch: unknown must not be taken, in either branch.
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE amount > 500 OR region IS NULL",
    );
    await expectMatchesOracle(
      "SELECT id FROM data WHERE (id < 4 OR id > 11997) AND region IS NOT NULL ORDER BY id",
    );
    // A branch the kernel cannot compile falls back per row; the result must not change.
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE region LIKE 'w%' OR amount + bucket > 900",
    );
  });

  it("negates comparisons, conjunctions and disjunctions the way SQL does", async () => {
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE NOT (amount = 100)");
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE NOT (amount > 100 AND amount < 500)",
    );
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE NOT (amount = 100 OR amount = 200)",
    );
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE NOT (region LIKE 'w%')");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE NOT (region IS NULL)");
    await expectMatchesOracle("SELECT COUNT(*) AS n FROM data WHERE NOT (NOT (amount = 100))");
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE NOT (amount IN (100, 200, 300))",
    );
    // Negation over a disjunction of ANDs, which De Morgan turns into a conjunction of ORs.
    await expectMatchesOracle(
      "SELECT COUNT(*) AS n FROM data WHERE NOT ((amount > 880 AND region = 'east') OR bucket = 3)",
    );
  });

  it("combines narrowing with LIMIT and ordering", async () => {
    await expectMatchesOracle(
      "SELECT id, amount FROM data WHERE id IN (11, 5000, 11000) ORDER BY id DESC",
    );
    await expectMatchesOracle(
      "SELECT id FROM data WHERE id IN (9, 4500, 8000, 11500) ORDER BY id LIMIT 2",
    );
    await expectMatchesOracle(
      "SELECT SUM(amount) AS total, COUNT(*) AS n FROM data WHERE id IN (100, 4100, 8100)",
    );
  });
});
