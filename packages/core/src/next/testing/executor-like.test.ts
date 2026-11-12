import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { it, expect } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-like-sql", [0xb783, 0xb784]))(
  "SQL LIKE lowering, parameters and lazy errors seed%i",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    try {
      await pg.waitReady;
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE"),
          fc.constantFrom("Ä", "ä", "İ", "i", "Σ", "ς", "Ⓐ", "ⓐ", "😀", "a_", "a%", null),
          fc.constantFrom("_", "%", "ä", "i", "Σ", "ⓐ", "a!_", "a!%", null),
          fc.constantFrom("default", "C", "POSIX"),
          fc.boolean(),
          async (op, value, pattern, collation, parameter) => {
            const quote = (v: string | null) =>
              v === null ? "NULL::text" : `'${v.replaceAll("'", "''")}'::text`;
            const a = parameter ? "$1::text" : quote(value),
              b = parameter ? "$2::text" : quote(pattern);
            const sql = `SELECT ${a}${collation === "default" ? "" : ` COLLATE "${collation}"`} ${op} ${b} ESCAPE '!' AS x`;
            const args = parameter ? [value, pattern] : [];
            const r = await core.execute(sql, args);
            if (r.kind !== "rows" || !("batches" in r)) throw new Error("missing rows");
            expect(oracleRows(r)).toEqual((await pg.query(sql, args)).rows);
          },
        ),
        {
          seed,
          numRuns: 64,
          examples: [
            ["ILIKE", "Ä", "ä", "default", false],
            ["ILIKE", "Ä", "ä", "C", false],
            ["ILIKE", "İ", "i", "default", true],
            ["ILIKE", "ς", "Σ", "default", true],
            ["ILIKE", "Ⓐ", "ⓐ", "default", false],
            ["LIKE", null, "%", "default", true],
          ],
        },
      );
      for (const sql of [
        "SELECT CASE WHEN false THEN 'a' LIKE '%_\\' ELSE true END AS x",
        "SELECT v FROM (VALUES ('a'))t(v) WHERE false AND v LIKE '%_\\'",
      ]) {
        const r = await core.execute(sql);
        if (r.kind !== "rows" || !("batches" in r)) throw new Error("missing lazy rows");
        expect(oracleRows(r)).toEqual((await pg.query(sql)).rows);
      }
      // Lead ruling 2026-11-12: uniform constant-fold error timing is a separate issue.
      for (const [sql, state] of [
        ["SELECT 1/0 LIMIT 0", "22012"],
        ["SELECT 'a' LIKE '%_\\' AS x LIMIT 0", "22025"],
      ]) {
        if (!sql || !state) throw new Error("Missing timing witness");
        const result = await core.execute(sql);
        if (result.kind !== "rows" || !("batches" in result))
          throw new Error("Missing timing rows");
        expect(oracleRows(result)).toEqual([]);
        await expect(pg.query(sql)).rejects.toMatchObject({ code: state });
      }
    } finally {
      await core.close();
      await pg.close();
    }
  },
  60000,
);
