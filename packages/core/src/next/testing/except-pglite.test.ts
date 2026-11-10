/** SQL EXCEPT's equality classes and bag subtraction are PostgreSQL laws, not anti-join equality. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, EXCEPT_SQL_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore(32).open();
}, 60_000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
type Row = [number | null, string | null];
function arm(rows: readonly Row[]): string {
  if (rows.length === 0) return "SELECT NULL::int4 AS n,NULL::text AS t WHERE FALSE";
  return `SELECT n,t FROM (VALUES ${rows.map(([n, t]) => `(${n === null ? "NULL" : String(n)}::int4,${t === null ? "NULL" : `'${t.replaceAll("'", "''")}'`}::text)`).join(",")}) v(n,t)`;
}
const canonical = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((row) => JSON.stringify(row)).sort();
it.each(seedsFor("next-except-sql", [0xb62d, 0xb62e]))(
  "subtracts SQL sets and bags exactly like PG (seed %i)",
  async (seed) => {
    const reach = createReach<string>("EXCEPT SQL", EXCEPT_SQL_REACH);
    const a: Row = [null, "x"],
      b: Row = [1, null],
      c: Row = [0, "'é\\"];
    const examples: Array<[Row[], Row[], boolean, boolean]> = [];
    for (const all of [false, true])
      for (const [left, right] of [
        [[], []],
        [[a, a, b], [a]],
        [
          [c, c, c],
          [c, c],
        ],
        [[b], [b, b, b]],
        [[a, b], []],
        [[], [a, b]],
        [
          [a, a, b, b, c],
          [b, c],
        ],
      ] as Array<[Row[], Row[]]>)
        for (const nested of [false, true]) examples.push([left, right, all, nested]);
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.tuple(
            fc.option(fc.integer({ min: -3, max: 3 }), { nil: null }),
            fc.option(fc.constantFrom("", "a", "x", "'é\\", "中"), { nil: null }),
          ),
          { maxLength: 80 },
        ),
        fc.array(
          fc.tuple(
            fc.option(fc.integer({ min: -3, max: 3 }), { nil: null }),
            fc.option(fc.constantFrom("", "a", "x", "'é\\", "中"), { nil: null }),
          ),
          { maxLength: 80 },
        ),
        fc.boolean(),
        fc.boolean(),
        async (left, right, all, nested) => {
          reach.beginHistory();
          const inner = `(${arm(left)}) EXCEPT${all ? " ALL" : ""} (${arm(right)})`;
          const sql = nested ? `(${inner}) EXCEPT ALL (${arm(right)})` : inner;
          const expected = (await pg.query<Record<string, unknown>>(sql)).rows;
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected EXCEPT rows");
          expect(canonical(oracleRows(result)), sql).toEqual(canonical(expected));
          reach.reach(all ? "ALL" : "DISTINCT");
          reach.reach(nested ? "nested" : "binary");
          if (left.length === 0) reach.reach("empty left");
          if (right.length === 0) reach.reach("empty right");
          if (expected.length === 0) reach.reach("empty result");
          if (left.some((row) => row.some((value) => value === null))) reach.reach("NULL keys");
          if (left.some((row) => row[1]?.includes("'"))) reach.reach("escaped Unicode");
          if (new Set(left.map((row) => JSON.stringify(row))).size < left.length)
            reach.reach("left duplicates");
          if (new Set(right.map((row) => JSON.stringify(row))).size < right.length)
            reach.reach("right duplicates");
          if (all && expected.length > 1) reach.reach("bag output");
        },
      ),
      { seed, numRuns: 80 + examples.length, examples },
    );
    reach.assertReached();
  },
  120_000,
);

it("checks equality capabilities even under LIMIT 0", async () => {
  const invalid = "SELECT NULL::json AS v EXCEPT SELECT NULL::json";
  await expect(pg.query(invalid)).rejects.toMatchObject({ code: "42883" });
  await expect(core.execute(invalid)).rejects.toMatchObject({
    name: "PlanningError",
    sqlState: "42883",
  });
  for (const sql of [
    "SELECT 'a'::char(3) AS v EXCEPT SELECT 'a '::char(3)",
    "SELECT NULL::jsonb AS v EXCEPT SELECT NULL::jsonb",
  ]) {
    expect((await pg.query(sql)).rows).toBeDefined();
    await expect(core.execute(sql), sql).rejects.toMatchObject({
      name: "UnsupportedPlanError",
      sqlState: "0A000",
    });
    await expect(core.execute(`SELECT * FROM (${sql}) q LIMIT 0`)).rejects.toMatchObject({
      name: "UnsupportedPlanError",
      sqlState: "0A000",
    });
  }
});
