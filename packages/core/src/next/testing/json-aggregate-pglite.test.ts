/** Ordered JSON aggregation and record text are compared byte-for-byte with PostgreSQL. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_REACH, JSON_RECORD_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const quote = (s: string) => "'" + s.replaceAll("'", "''") + "'";
async function compare(sql: string) {
  const expected = (await pg.query(sql)).rows;
  const actual = await core.execute(sql);
  if (!("batches" in actual)) throw new Error("Expected JSON query");
  expect(oracleRows(actual), sql).toEqual(expected);
}
it.each(seedsFor("next-json-aggregate-text", [0xb700, 0xb701]))(
  "matches ordered arrays, objects and NULL modes (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON aggregate text", JSON_AGGREGATE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 9 }),
        fc.integer({ min: 0, max: 18 }),
        fc.boolean(),
        fc.constantFrom("汉😃", "a", '"\\\n'),
        async (mode, length, desc, text) => {
          reach.beginHistory();
          const names = [
            "json_agg",
            "jsonb_agg",
            "json_agg_strict",
            "jsonb_agg_strict",
            "json_arrayagg",
            "json_object_agg",
            "jsonb_object_agg",
            "json_object_agg_strict",
            "jsonb_object_agg_strict",
            "json_object_agg_unique",
          ];
          const fn = names[mode] ?? "json_agg",
            object = mode >= 5;
          const rows = Array.from(
            { length: Math.max(1, length) },
            (_, i) =>
              `(${String(i)},${quote((mode === 9 ? String(i) : (["z", "aaa", "汉"][i % 3] ?? "z")) + text)},${i % 4 === 0 ? "NULL::int" : String(i - 7)})`,
          ).join(",");
          const sql = `SELECT ${fn}(${object ? "k,v" : "v"} ORDER BY n ${desc ? "DESC" : "ASC"})::text j
          FROM(VALUES ${rows})t(n,k,v) ${length === 0 ? "WHERE false" : ""}`;
          await compare(sql);
          reach.reach(object ? "object aggregation" : "array aggregation");
          if (object && length > 3) reach.reach("UTF8 key order");
          reach.reach(fn.includes("jsonb") ? "JSONB" : "JSON");
          reach.reach(desc ? "descending" : "ascending");
          if (length === 0) reach.reach("empty input");
          if (length === 1) reach.reach("all NULL values");
          if (fn.includes("strict") || fn === "json_arrayagg") reach.reach("omit NULL values");
          if (object && length > 4 && mode !== 9) reach.reach("duplicate object keys");
          if (mode === 9) reach.reach("unique object keys");
          reach.reach("exact text");
        },
      ),
      {
        seed,
        numRuns: 64,
        examples: [
          [0, 0, false, "a"],
          [2, 1, true, "a"],
          [1, 8, false, "汉😃"],
          [4, 4, true, "a"],
          [5, 8, false, "a"],
          [6, 8, true, "a"],
          [7, 1, false, "a"],
          [8, 4, true, "a"],
          [9, 5, false, "a"],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
it.each(seedsFor("next-json-record-text", [0xb702, 0xb703]))(
  "renders nested records and absent outer rows (seed %i)",
  async (seed) => {
    const reach = createReach<string>("whole-row JSON", JSON_RECORD_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -1000, max: 1000 }),
        fc.boolean(),
        fc.boolean(),
        async (n, pretty, present) => {
          reach.beginHistory();
          await compare(`SELECT row_to_json(q,${pretty ? "true" : "false"})::text r,
          to_json(q)::text j,to_jsonb(q)::text b FROM(SELECT ${String(n)} id,NULL::text v)q`);
          await compare(`SELECT row_to_json(ROW(${String(n)},ROW('汉',NULL)))::text r`);
          await compare(`SELECT row_to_json(r)::text r,to_json(r)::text j
          FROM(VALUES(1))l(n) LEFT JOIN(SELECT NULL::int a,NULL::text b)r ON ${present ? "true" : "false"}`);
          await compare(
            `SELECT json_agg(r ORDER BY l.n)::text a,jsonb_agg(r ORDER BY l.n)::text b FROM(VALUES(1),(2))l(n) LEFT JOIN(SELECT 1 a,NULL::text b)r ON l.n=2`,
          );
          reach.reach(pretty ? "pretty record" : "compact record");
          reach.reach("nested ROW");
          reach.reach("JSONB record");
          reach.reach(present ? "present all-NULL row" : "absent outer row");
          reach.reach("aggregate record separators");
          reach.reach("exact PG text");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [0, false, false],
          [1, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
