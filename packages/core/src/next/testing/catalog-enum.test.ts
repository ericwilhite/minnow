/** PG enum input (not lexical text ordering), exact labels and incompatible named identities. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_ENUM_REACH, createReach } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";
async function outcome(work: () => Promise<unknown>): Promise<unknown> {
  try {
    return await work();
  } catch (error) {
    if (error && typeof error === "object") {
      if ("sqlState" in error) return { error: error.sqlState };
      if ("code" in error) return { error: error.code };
    }
    throw error;
  }
}
for (const seed of seedsFor("next-catalog-enum", [0xb784, 0xb785]))
  it(`matches enum input and casts seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      core = await new StorageCore().open(),
      reach = createReach("enum input", CATALOG_ENUM_REACH);
    const labels = ["z", "a", "汉", "😀", "", "a.b", "Case"];
    try {
      const ddl = `CREATE TYPE stage2_m AS ENUM (${labels.map((label) => `'${label}'`).join(",")})`;
      await pg.exec(ddl);
      await core.execute(ddl);
      await pg.exec("CREATE TYPE stage2_other AS ENUM ('a')");
      await core.execute("CREATE TYPE stage2_other AS ENUM ('a')");
      await pg.exec(
        `CREATE TABLE closed_text(x TEXT CHECK(x IN (${labels.map((label) => `'${label}'`).join(",")})))`,
      );
      await core.createTable({
        name: "closed_text",
        columns: [{ name: "x", type: "string", nullable: true, enumValues: labels }],
      });
      expect(core.harness.path.catalog.tableByName("closed_text")?.columns[0]?.type).toEqual({
        kind: "text",
      });
      await fc.assert(
        fc.asyncProperty(
          fc.oneof(fc.constantFrom(...labels), fc.string({ maxLength: 40 }), fc.constant(null)),
          async (value) => {
            reach.beginHistory();
            const sql = "SELECT $1::text::stage2_m::text AS v";
            const expected = await outcome(async () => (await pg.query(sql, [value])).rows);
            const actual = await outcome(async () => {
              const result = await core.execute(sql, [value]);
              if (!("batches" in result)) throw new Error("Missing enum output");
              return oracleRows(result);
            });
            expect(actual).toEqual(expected);
            const inserted = await outcome(() =>
              core.writeBatch("insertBatch", "closed_text", [{ x: value }]),
            );
            const pgInserted = await outcome(() =>
              pg.query("INSERT INTO closed_text VALUES($1)", [value]),
            );
            expect(
              inserted && typeof inserted === "object" && "error" in inserted
                ? inserted.error
                : null,
            ).toEqual(
              pgInserted && typeof pgInserted === "object" && "error" in pgInserted
                ? pgInserted.error
                : null,
            );
            reach.reach(
              value === null
                ? "closed NULL"
                : labels.includes(value)
                  ? "closed valid"
                  : "closed invalid",
            );
            reach.reach(
              value === null ? "NULL" : labels.includes(value) ? "valid label" : "invalid label",
            );
            if (value !== null && /[\u0080-\u{10ffff}]/u.test(value) && labels.includes(value))
              reach.reach("unicode label");
            if (value !== null && value.length === 0) reach.reach("empty label");
          },
        ),
        {
          seed,
          numRuns: 36,
          examples: [...labels.map((value) => [value] as [string | null]), [null], ["not a label"]],
        },
      );
      const wrong = "SELECT 'a'::stage2_m::stage2_other AS v";
      expect(await outcome(() => core.execute(wrong))).toEqual(
        await outcome(async () => (await pg.query(wrong)).rows),
      );
      const missing = "SELECT CAST(1 AS notatype)";
      expect(await outcome(() => core.execute(missing))).toEqual(
        await outcome(async () => (await pg.query(missing)).rows),
      );
      reach.reach("different named enum");
      const sorted = await core.execute("SELECT x FROM closed_text ORDER BY x");
      if (!("batches" in sorted)) throw new Error("Missing closed text ordering");
      expect(oracleRows(sorted)).toEqual(
        (await pg.query("SELECT x FROM closed_text ORDER BY x")).rows,
      );
      reach.reach("closed text order");
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 120_000);
