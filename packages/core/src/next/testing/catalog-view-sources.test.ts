/** PostgreSQL freezes view source schemas at definition, including nested wildcards and NATURAL keys. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_VIEW_SOURCES_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { Catalog } from "../write/catalog.js";
import { decodeCatalogStream } from "../write/catalog-stream.js";
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-catalog-view-sources", [0xb7a4, 0xb7a5]))
  it(`freezes definition-time view bindings seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("view source bindings", CATALOG_VIEW_SOURCES_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 7 }),
          fc.constantFrom("a", "Case", "a.b"),
          fc.boolean(),
          async (shape, name, checkpoint) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            const a = quote(name);
            try {
              for (const sql of [
                `CREATE TABLE t(id INT PRIMARY KEY,unused INT,${a} INT)`,
                "CREATE TABLE u(id INT PRIMARY KEY,b INT)",
              ]) {
                await pg.exec(sql);
                await core.execute(sql);
              }
              await pg.exec(
                `INSERT INTO t VALUES(1,90,7),(2,91,8);INSERT INTO u VALUES(1,10),(2,20)`,
              );
              await core.writeBatch("insertBatch", "t", [
                { id: 1, unused: 90, [name]: 7 },
                { id: 2, unused: 91, [name]: 8 },
              ]);
              await core.writeBatch("insertBatch", "u", [
                { id: 1, b: 10 },
                { id: 2, b: 20 },
              ]);
              const sql = [
                "SELECT * FROM t",
                "SELECT t.*,id+1 AS next FROM t",
                "SELECT * FROM (SELECT * FROM t) q",
                "WITH c AS (SELECT * FROM t) SELECT * FROM c",
                "SELECT t.*,u.b FROM t JOIN u USING(id)",
                "SELECT * FROM t NATURAL JOIN u",
                "SELECT * FROM t",
                "SELECT q.y FROM t q(x,w,y)",
              ][shape];
              if (!sql) throw new Error("missing view shape");
              await pg.exec(`CREATE VIEW v AS ${sql}`);
              await core.execute(`CREATE VIEW v AS ${sql}`);
              if (shape === 6) {
                await pg.exec("CREATE VIEW child AS SELECT * FROM v");
                await core.execute("CREATE VIEW child AS SELECT * FROM v");
              }
              const view = shape === 6 ? "child" : "v";
              const compare = async () => {
                const result = await core.execute(`SELECT * FROM ${view} ORDER BY 1`);
                if (!("batches" in result)) throw new Error("missing view rows");
                const expected = await pg.query(`SELECT * FROM ${view} ORDER BY 1`);
                expect(oracleRows(result)).toEqual(expected.rows);
                const outputs = core.harness.path.catalog.views.find(
                  (v) => v.name === view,
                )?.outputs;
                expect(result.columns.map((c) => c.name)).toEqual(outputs?.map((c) => c.name));
              };
              await compare();
              for (const cmd of [
                "ALTER TABLE t ADD COLUMN extra INT DEFAULT 42",
                ...(shape === 5 ? ["ALTER TABLE t ADD COLUMN b INT DEFAULT 999"] : []),
              ]) {
                await pg.exec(cmd);
                await core.execute(cmd);
              }
              if (shape === 6) {
                await pg.exec("CREATE OR REPLACE VIEW v AS SELECT * FROM t");
                await core.execute("CREATE OR REPLACE VIEW v AS SELECT * FROM t");
              }
              await compare();
              const reached = (
                [
                  "simple",
                  "qualified",
                  "derived",
                  "CTE",
                  "USING",
                  "NATURAL",
                  "nested replacement",
                  "column alias list",
                ] as const
              )[shape];
              if (!reached) throw new Error("missing shape reach");
              reach.reach(reached);
              reach.reach(name === "a" ? "plain name" : "quoted name");
              if (checkpoint) {
                await core.harness.path.recycleLog();
                reach.reach("checkpoint");
              } else reach.reach("WAL");
              await core.reopen();
              await compare();
              reach.reach("reopen");
              const stored = decodeCatalogStream(core.harness.path.catalog.encode());
              const definition = stored.views.find((v) => v.name === "v");
              const source = definition?.sourceSchemas?.[0];
              if (!definition || !source) throw new Error("missing stored source bindings");
              for (const sourceSchemas of [
                [],
                [{ ...source, objectId: 999999 }],
                [{ ...source, columnIds: [...source.columnIds, ...source.columnIds] }],
              ]) {
                expect(() =>
                  Catalog.fromStored({
                    ...stored,
                    views: stored.views.map((v) =>
                      v.id === definition.id ? { ...v, sourceSchemas } : v,
                    ),
                  }),
                ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
              }
              reach.reach("binding corruption refused");
            } finally {
              await core.close();
              await pg.exec(
                "DROP VIEW IF EXISTS child;DROP VIEW IF EXISTS v;DROP TABLE IF EXISTS t,u",
              );
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: Array.from(
            { length: 8 },
            (_, shape) =>
              [shape, shape % 2 ? "a.b" : "a", shape % 2 === 0] as [number, string, boolean],
          ),
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
