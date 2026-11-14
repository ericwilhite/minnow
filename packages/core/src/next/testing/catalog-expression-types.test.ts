/** PostgreSQL retains dependencies from stored SQL even when its result type/IR no longer does. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_EXPRESSION_TYPES_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { oracleRows } from "./differential-adapter.js";
import { Catalog } from "../write/catalog.js";
import { decodeCatalogStream } from "../write/catalog-stream.js";
import type { ExpressionDependencies } from "../access/catalog.js";
const quote = (value: string) => `"${value.replaceAll('"', '""')}"`;
const state = async (work: () => unknown) => {
  try {
    await work();
    return null;
  } catch (error) {
    if (error && typeof error === "object") {
      if ("sqlState" in error) return error.sqlState;
      if ("code" in error) return error.code;
    }
    throw error;
  }
};
for (const seed of seedsFor("next-catalog-expression-types", [0xb798, 0xb799]))
  it(`retains rule type identities through storage seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("stored expression types", CATALOG_EXPRESSION_TYPES_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 4 }),
          fc.integer({ min: 0, max: 3 }),
          fc.boolean(),
          fc.constantFrom("n", "a.b", "Case"),
          fc.integer({ min: 0, max: 2 }),
          async (place, padding, checkpoint, name, corruption) => {
            reach.beginHistory();
            const core = await new StorageCore().open(),
              reference = new LoweredDatabase();
            try {
              const execute = async (sql: string) => {
                await pg.exec(sql);
                await core.execute(sql);
                reference.execute(sql);
              };
              for (let i = 0; i < padding; i++)
                await execute(`CREATE TYPE pad_${String(i)} AS ENUM ('p')`);
              await execute("CREATE TYPE e AS ENUM ('x','y')");
              await execute("CREATE TYPE other1 AS ENUM ('p')");
              await execute("CREATE TYPE other2 AS ENUM ('p')");
              if (place === 4) await execute("CREATE SEQUENCE s");
              const n = quote(name),
                fake = JSON.stringify({
                  kind: "call",
                  name: "CAST",
                  arguments: [null, { kind: "literal", value: "enum:e" }],
                });
              const column =
                place === 0
                  ? "v TEXT DEFAULT ('x'::e)::text"
                  : place === 1
                    ? "v BOOLEAN GENERATED ALWAYS AS ('x'::e='y'::e) STORED"
                    : place === 2
                      ? `v TEXT, CHECK (('x'::e='y'::e) OR ${n}>0)`
                      : place === 3
                        ? `v JSONB DEFAULT '${fake}'::jsonb`
                        : "v TEXT, CHECK (nextval('s')>0)";
              await execute(`CREATE TABLE t(${n} INTEGER PRIMARY KEY,${column})`);
              if (place === 3) {
                // The reference interpreter lacks text→json casts; use the real kernel here.
                const query = `SELECT '${fake}'::jsonb::text AS v`;
                const result = await core.execute(query);
                if (!("batches" in result)) throw new Error("Missing literal JSON rows");
                expect(oracleRows(result)).toEqual((await pg.query(query)).rows);
              } else {
                await pg.exec(`INSERT INTO t(${n}) VALUES(1)`);
                reference.execute(`INSERT INTO t(${n}) VALUES(1)`);
                expect(reference.execute("SELECT v::text AS v FROM t")).toEqual(
                  (await pg.query("SELECT v::text AS v FROM t")).rows,
                );
              }
              if (checkpoint) {
                await core.harness.path.quiesce();
                reach.reach("checkpoint");
              } else reach.reach("WAL");
              await core.reopen();
              const pgDrop = await state(() => pg.exec("DROP TYPE e"));
              expect(await state(() => core.execute("DROP TYPE e"))).toBe(pgDrop);
              expect(await state(() => reference.execute("DROP TYPE e"))).toBe(pgDrop);
              expect(pgDrop).toBe(place >= 3 ? null : "2BP01");
              reach.reach(
                ["default", "generated", "check", "literal data", "check sequence"][place] as
                  "default" | "generated" | "check" | "literal data" | "check sequence",
              );
              if (place === 4) {
                const pgSequenceDrop = await state(() => pg.exec("DROP SEQUENCE s"));
                expect(pgSequenceDrop).toBe("2BP01");
                expect(await state(() => core.execute("DROP SEQUENCE s"))).toBe(pgSequenceDrop);
                expect(await state(() => reference.execute("DROP SEQUENCE s"))).toBe(
                  pgSequenceDrop,
                );
              }
              const catalog = core.harness.path.catalog,
                table = catalog.tableByName("t"),
                type = catalog.typeByName("e");
              if (!table) throw new Error("Missing table");
              const deps =
                place === 2 || place === 4
                  ? table.checks[0]?.dependencies
                  : place === 1
                    ? table.columns[1]?.generatedDependencies
                    : table.columns[1]?.defaultDependencies;
              expect(deps?.typeIds ?? []).toEqual(place >= 3 ? [] : [type?.id]);
              if (padding > 0) reach.reach("allocated IDs");
              const decoded = decodeCatalogStream(catalog.encode()),
                a = catalog.typeByName("other1")?.id,
                b = catalog.typeByName("other2")?.id;
              if (!deps || a === undefined || b === undefined)
                throw new Error("Missing dependency fixtures");
              const bad: ExpressionDependencies = {
                ...deps,
                typeIds:
                  corruption === 0
                    ? [catalog.counters.nextTypeId]
                    : corruption === 1
                      ? [a, a]
                      : [b, a],
              };
              const data = {
                ...decoded,
                tables: decoded.tables.map((t) =>
                  t.name !== "t"
                    ? t
                    : {
                        ...t,
                        columns: t.columns.map((c, i) =>
                          i !== 1 || place === 2 || place === 4
                            ? c
                            : place === 1
                              ? { ...c, generatedDependencies: bad }
                              : { ...c, defaultDependencies: bad },
                        ),
                        checks: t.checks.map((c, i) =>
                          (place === 2 || place === 4) && i === 0 ? { ...c, dependencies: bad } : c,
                        ),
                      },
                ),
              };
              expect(() => Catalog.fromStored(data)).toThrow(
                expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
              );
              reach.reach(
                corruption === 0
                  ? "dangling type"
                  : corruption === 1
                    ? "duplicate type"
                    : "noncanonical types",
              );
              await execute("DROP TABLE t");
              if (place < 3) await execute("DROP TYPE e");
              if (place === 4) await execute("DROP SEQUENCE s");
            } finally {
              await core.close();
              await pg.exec(
                "DROP TABLE IF EXISTS t; DROP TYPE IF EXISTS public.e; DROP TYPE IF EXISTS other1; DROP TYPE IF EXISTS other2; DROP SEQUENCE IF EXISTS s",
              );
              for (let i = 0; i < padding; i++)
                await pg.exec(`DROP TYPE IF EXISTS pad_${String(i)}`);
            }
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            [0, 0, false, "n", 0],
            [1, 2, true, "a.b", 1],
            [2, 3, false, "Case", 2],
            [3, 1, true, "n", 0],
            [4, 1, true, "n", 1],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
