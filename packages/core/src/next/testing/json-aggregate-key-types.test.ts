/** Structural grouping equality must not silently degrade to equality of JSONB wire text. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_KEY_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
  await pg.exec("CREATE TABLE json_group_keys(n INTEGER,doc TEXT)");
  await core.execute("CREATE TABLE json_group_keys(n INTEGER,doc TEXT)");
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-json-aggregate-key-admission", [0xb718, 0xb719]))(
  "refuses structural keys until semantic equality is available (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON aggregate structural key admission",
      JSON_AGGREGATE_KEY_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 5 }),
        fc.integer({ min: 1, max: 6 }),
        fc.constantFrom("scalar", "object", "array"),
        async (groups, scale, shape) => {
          reach.beginHistory();
          await pg.exec("TRUNCATE json_group_keys");
          await core.execute("DROP TABLE json_group_keys");
          await core.execute("CREATE TABLE json_group_keys(n INTEGER,doc TEXT)");
          const rows = Array.from({ length: groups * 2 }, (_, n) => {
            const number = String(Math.floor(n / 2) + 1) + (n % 2 ? "." + "0".repeat(scale) : "");
            return {
              n,
              doc:
                shape === "object"
                  ? '{"k":' + number + "}"
                  : shape === "array"
                    ? "[" + number + "]"
                    : number,
            };
          });
          await core.writeBatch("insertBatch", "json_group_keys", rows);
          for (const r of rows)
            await pg.query("INSERT INTO json_group_keys VALUES($1,$2)", [r.n, r.doc]);
          const sql =
            "SELECT json_agg(n ORDER BY n)::text j FROM json_group_keys GROUP BY doc::jsonb";
          const expected = await pg.query<{ j: string }>(sql);
          expect(expected.rows.length).toBe(groups);
          for (const row of expected.rows) expect(JSON.parse(row.j)).toHaveLength(2);
          await expect(core.execute(sql)).rejects.toMatchObject({
            code: "MINNOW_UNSUPPORTED_PLAN",
            sqlState: "0A000",
            construct: "aggregate",
          });
          reach.reach(shape);
          reach.reach(scale === 1 ? "one fractional zero" : "several fractional zeros");
          reach.reach("PG semantic equal pairs");
          reach.reach("typed key admission refusal");
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero spill files");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [2, 1, "scalar"],
          [2, 2, "object"],
          [2, 3, "array"],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it("rejects plain JSON grouping with PostgreSQL's equality SQLSTATE", async () => {
  const sql = "SELECT json_agg(n)::text j FROM json_group_keys GROUP BY doc::json";
  await expect(pg.query(sql)).rejects.toMatchObject({ code: "42883" });
  await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "42883" });
});
