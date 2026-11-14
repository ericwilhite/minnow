/** Native ALWAYS identity follows PostgreSQL; the published counter alias stays unchanged. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, IDENTITY_ALWAYS_REACH } from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { StorageCore } from "./storage-core.js";
import { Catalog } from "../write/catalog.js";
import { decodeCatalogStream } from "../write/catalog-stream.js";
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
const state = async (work: () => unknown) => {
  try {
    await work();
    return null;
  } catch (e) {
    if (e && typeof e === "object") {
      if ("sqlState" in e) return e.sqlState;
      if ("code" in e) return e.code;
    }
    throw e;
  }
};
for (const seed of seedsFor("next-identity-always", [0xb7b4, 0xb7b5]))
  it(`retains identity generation mode and assignment rules seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("ALWAYS identity", IDENTITY_ALWAYS_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.constantFrom("smallint", "integer", "bigint"),
          fc.constantFrom("id", "Case", "a.b"),
          fc.boolean(),
          async (always, type, name, checkpoint) => {
            reach.beginHistory();
            const next = new LoweredDatabase(),
              core = await new StorageCore().open(),
              id = quote(name);
            const create = `CREATE TABLE t(${id} ${type} GENERATED ${always ? "ALWAYS" : "BY DEFAULT"} AS IDENTITY PRIMARY KEY,v INT UNIQUE)`;
            try {
              await pg.exec(create);
              next.execute(create);
              await core.execute(create);
              expect(next.catalog.view.tableByName("t")?.columns[0]?.identityAlways).toBe(
                always ? true : undefined,
              );
              for (const sql of [
                "INSERT INTO t(v) VALUES(10),(20)",
                `INSERT INTO t(${id},v) VALUES(DEFAULT,30)`,
                `INSERT INTO t(${id},v) VALUES(50,40)`,
                `INSERT INTO t(${id},v) SELECT 51,41`,
                `UPDATE t SET ${id}=99 WHERE v=20`,
                `UPDATE t SET ${id}=DEFAULT WHERE v=10`,
                `INSERT INTO t(v) VALUES(10) ON CONFLICT(v) DO UPDATE SET ${id}=199`,
                `INSERT INTO t(v) VALUES(10) ON CONFLICT(v) DO UPDATE SET ${id}=DEFAULT`,
                `MERGE INTO t q USING(SELECT 10 AS v)s ON q.v=s.v WHEN MATCHED THEN UPDATE SET ${id}=77`,
                `MERGE INTO t q USING(SELECT 444 AS v)s ON q.v=s.v WHEN NOT MATCHED THEN INSERT(${id},v) VALUES(88,s.v)`,
                `MERGE INTO t q USING(SELECT 445 AS v)s ON q.v=s.v WHEN NOT MATCHED THEN INSERT(${id},v) VALUES(DEFAULT,s.v)`,
              ]) {
                const authority = await state(() => pg.exec(sql));
                expect(await state(() => next.execute(sql)), sql).toBe(authority);
                const query = `SELECT ${id}::text AS id,v FROM t ORDER BY v`;
                expect(next.execute(query), sql).toEqual((await pg.query(query)).rows);
                if (sql.includes("VALUES(50"))
                  reach.reach(always ? "explicit refused" : "explicit allowed");
                if (sql.includes("SELECT 51")) reach.reach("insert select");
                if (sql.startsWith("UPDATE")) reach.reach("update");
                if (sql.includes("ON CONFLICT")) reach.reach("upsert");
                if (sql.startsWith("MERGE")) reach.reach("merge");
                if (sql.includes("DEFAULT")) reach.reach("default allowed");
              }
              const batchSql = `INSERT INTO t(${id},v) VALUES(222,555)`;
              const batchAuthority = await state(() => pg.exec(batchSql));
              expect(
                await state(() =>
                  next.lowerer.lowerWrite(
                    { table: "t", op: "insert", names: [name, "v"] },
                    next.context(),
                  ),
                ),
              ).toBe(always ? batchAuthority : null);
              reach.reach("batch input");
              if (checkpoint) {
                await core.harness.path.recycleLog();
                reach.reach("checkpoint");
              } else reach.reach("WAL");
              await core.reopen();
              expect(core.harness.path.catalog.tableByName("t")?.columns[0]?.identityAlways).toBe(
                always ? true : undefined,
              );
              if (always) {
                const data = decodeCatalogStream(core.harness.path.catalog.encode()),
                  table = data.tables[0],
                  column = table?.columns[0];
                if (!table || !column) throw new Error("missing identity column");
                for (const patch of [
                  { identitySequence: null },
                  { nullable: true },
                  { generatedSql: "1" },
                  { defaultSql: "1" },
                  { autoIncrement: true as const },
                ]) {
                  expect(() =>
                    Catalog.fromStored({
                      ...data,
                      tables: [
                        { ...table, columns: [{ ...column, ...patch }, ...table.columns.slice(1)] },
                      ],
                    }),
                  ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
                }
                reach.reach("invalid mode refused");
              }
              reach.reach(type);
              reach.reach(name === "id" ? "plain" : "quoted");
            } finally {
              await core.close();
              await pg.exec("DROP TABLE IF EXISTS t");
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [true, "smallint", "id", false],
            [false, "integer", "Case", true],
            [true, "bigint", "a.b", true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);

it("resolves identity types before enforcing the integer policy", async () => {
  const pg = new PGlite();
  try {
    for (const mode of ["ALWAYS", "BY DEFAULT"])
      for (const type of ["text", "numeric", "boolean", "uuid", "missing_identity_type"]) {
        const sql = `CREATE TABLE invalid_identity(id ${type} GENERATED ${mode} AS IDENTITY)`;
        const authority = await state(() => pg.exec(sql));
        expect(authority).toBe(type === "missing_identity_type" ? "42704" : "22023");
        expect(await state(() => new LoweredDatabase().execute(sql))).toBe(authority);
      }
  } finally {
    await pg.close();
  }
});
