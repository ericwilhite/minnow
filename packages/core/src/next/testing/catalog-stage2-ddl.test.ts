/** PG authority for lowered DDL/DML; native write/read and checkpoint cover persistence.
 * Main's executor still refuses modify IR, so DML is evaluated by the existing IR interpreter.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_STAGE2_DDL_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { oracleRows } from "./differential-adapter.js";
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-catalog-stage2-ddl", [0xb780, 0xb781]))
  it(`persists exact SQL catalog definitions and their semantics seed ${String(seed)}`, async () => {
    const pg = new PGlite();
    const reach = createReach("stage2 SQL catalog", CATALOG_STAGE2_DDL_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("ordinary", "Case", "a.b", 'a"b'),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 1, max: 7 }),
          async (name, unique, identity, value) => {
            reach.beginHistory();
            await pg.exec(
              "DROP VIEW IF EXISTS stage2_v; DROP TABLE IF EXISTS stage2_t; DROP TYPE IF EXISTS stage2_m",
            );
            const core = await new StorageCore(16).open(),
              reference = new LoweredDatabase();
            try {
              const ddl = [
                "CREATE TYPE stage2_m AS ENUM ('z', 'a', '汉')",
                `CREATE TABLE stage2_t(id ${identity ? "SERIAL" : "INTEGER"} PRIMARY KEY, ${quote(name)} stage2_m DEFAULT 'z', code INTEGER ${unique ? "UNIQUE" : ""}, parent INTEGER REFERENCES stage2_t(${unique ? "code" : "id"}))`,
              ];
              for (const sql of ddl) {
                await pg.exec(sql);
                await core.execute(sql);
                reference.execute(sql);
              }
              reach.reach("named enum");
              reach.reach(unique ? "self unique" : "self primary");
              reach.reach(identity ? "identity" : "explicit integer");
              reach.reach(
                name === "ordinary"
                  ? "plain"
                  : name === "Case"
                    ? "case"
                    : name === "a.b"
                      ? "dot"
                      : "quote",
              );
              const insert = identity
                ? `INSERT INTO stage2_t(code) VALUES(${String(value)})`
                : `INSERT INTO stage2_t(id,code) VALUES(1,${String(value)})`;
              await pg.exec(insert);
              reference.execute(insert);
              const sql = `CREATE VIEW stage2_v AS SELECT id, ${quote(name)} FROM stage2_t`;
              await pg.exec(sql);
              reference.execute(sql);
              await core.execute(sql);
              const select = `SELECT id, ${quote(name)} FROM stage2_v ORDER BY id`;
              const expected = (await pg.query(select)).rows;
              expect(reference.execute(select)).toEqual(expected);
              const rows = reference.execute("SELECT * FROM stage2_t ORDER BY id");
              await core.writeBatch(
                "insertBatch",
                "stage2_t",
                rows.map((row) => ({
                  id: Number(row.id),
                  [name]: String(row[name]),
                  code: Number(row.code),
                  parent: null,
                })),
              );
              const actual = await core.execute(select);
              if (!("batches" in actual)) throw new Error("Missing native result");
              expect(oracleRows(actual)).toEqual(expected);
              const directions = unique ? (["desc", "asc"] as const) : (["asc", "desc"] as const);
              const indexSql = `CREATE INDEX stage2_idx ON stage2_t(code ${directions[0]},id ${directions[1]})`;
              await pg.exec(indexSql);
              await core.execute(indexSql);
              expect(
                core.harness.path.catalog
                  .tableByName("stage2_t")
                  ?.indexes.find((index) => index.name === "stage2_idx")?.directions,
              ).toEqual(directions);
              reach.reach(unique ? "index descending" : "index ascending");
              const bad = `INSERT INTO stage2_t(id,${quote(name)},code,parent) VALUES (2,'z',999,404)`;
              let pgState: unknown;
              try {
                await pg.exec(bad);
              } catch (error) {
                if (error && typeof error === "object" && "code" in error) pgState = error.code;
                else throw error;
              }
              expect(pgState).toBe("23503");
              await expect(
                core.writeBatch("insertBatch", "stage2_t", [
                  { id: 2, [name]: "z", code: 999, parent: 404 },
                ]),
              ).rejects.toMatchObject({ sqlState: pgState });
              reach.reach("self FK enforcement");
              const catalog = core.harness.path.catalog,
                table = catalog.tableByName("stage2_t");
              expect(table?.columns[1]?.typeId).toBe(catalog.typeByName("stage2_m")?.id);
              if (identity) {
                const id = table?.columns[0]?.identitySequence;
                if (!id) throw new Error("Missing owned sequence");
                const out = { repr: "int64", values: new BigInt64Array(1) } as const;
                await core.harness.path.sequences.next(id, 1, out, 0);
                expect(out.values[0]).toBe(1n);
              }
              const view = catalog.views.find((v) => v.name === "stage2_v");
              expect(view?.outputs?.map((c) => c.name)).toEqual(["id", name]);
              expect(view?.dependencies).toEqual(
                expect.arrayContaining([
                  expect.objectContaining({ kind: "table", objectId: table?.id }),
                ]),
              );
              reach.reach("view outputs");
              reach.reach("view dependency");
              const bytes = catalog.encode();
              await core.harness.path.quiesce();
              await core.reopen();
              expect(core.harness.path.catalog.encode()).toEqual(bytes);
              const recovered = await core.execute(select);
              if (!("batches" in recovered)) throw new Error("Missing recovered rows");
              expect(oracleRows(recovered)).toEqual(expected);
              reach.reach("checkpoint reopen");
            } finally {
              await core.close();
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ["ordinary", false, false, 1],
            ["Case", true, true, 2],
            ["a.b", false, true, 3],
            ['a"b', true, false, 4],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
