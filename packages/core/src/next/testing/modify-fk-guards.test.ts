/** New catalog action modes require their actual RI driver, never legacy follow aliases. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_FK_GUARDS_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import { column, INT, tableInput } from "../write/test-support.js";
import { ChunkReader } from "../page/reader.js";
const modes = [
  "NO ACTION",
  "RESTRICT",
  "CASCADE",
  "SET NULL",
  "SET DEFAULT",
  "informational",
  "deleteDefault",
  "follow",
] as const;
it.each(seedsFor("next-modify-fk-guards", [0xb5d4]))(
  "refuses unexecuted RI modes seed %i",
  async (seed) => {
    const reach = createReach("modify FK guards", MODIFY_FK_GUARDS_REACH),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(...modes),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (mode, returning, upsert, raw) => {
            reach.beginHistory();
            const core = await new StorageCore().open();
            try {
              await pg.exec("DROP TABLE IF EXISTS c;DROP TABLE IF EXISTS p");
              await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS c" });
              await shipped.run({ kind: "sql", sql: "DROP TABLE IF EXISTS p" });
              const all = async (sql: string) => {
                await core.execute(sql);
                await pg.exec(sql);
                await shipped.run({ kind: "sql", sql });
              };
              await core.execute(
                "CREATE TABLE p(id INTEGER PRIMARY KEY,k INTEGER UNIQUE,v INTEGER)",
              );
              await pg.exec("CREATE TABLE p(id INTEGER PRIMARY KEY,k INTEGER UNIQUE,v INTEGER)");
              // Shipped references require the primary key; this is a default-follow semantic control.
              await shipped.run({
                kind: "sql",
                sql: "CREATE TABLE p(id INTEGER UNIQUE,k INTEGER PRIMARY KEY,v INTEGER)",
              });
              const def = core.harness.path.catalog.tableByName("p");
              if (!def) throw new Error("p");
              const parentIndex = def.indexes.find(
                (i) => i.columnIds.length === 1 && i.columnIds[0] === def.columns[1]?.id,
              );
              if (!parentIndex) throw new Error("k index");
              const deleting = mode === "deleteDefault",
                info = mode === "informational";
              const action = deleting
                ? "ON DELETE SET DEFAULT"
                : info || mode === "follow"
                  ? ""
                  : "ON UPDATE " + mode;
              if (info)
                await core.change({
                  kind: "createTable",
                  table: tableInput("c", [column("cid", INT, false), column("pid", INT, true)], {
                    foreignKeys: [
                      {
                        name: "info_fk",
                        columns: [1],
                        parentTableId: def.id,
                        parentIndexId: parentIndex.id,
                        enforced: false,
                        onDelete: "noAction",
                      },
                    ],
                  }),
                });
              else
                await core.execute(
                  "CREATE TABLE c(cid INTEGER PRIMARY KEY,pid INTEGER DEFAULT 2 REFERENCES p(k) " +
                    action +
                    ")",
                );
              await pg.exec(
                "CREATE TABLE c(cid INTEGER PRIMARY KEY,pid INTEGER DEFAULT 2 REFERENCES p(k) " +
                  (info || mode === "follow" ? "ON UPDATE CASCADE" : action) +
                  ")",
              );
              await shipped.run({
                kind: "sql",
                sql: "CREATE TABLE c(cid INTEGER PRIMARY KEY,pid INTEGER REFERENCES p(k))",
              });
              await all("INSERT INTO p VALUES(1,1,0),(2,2,0)");
              await all("INSERT INTO c VALUES(1,1)");
              await all("UPDATE p SET v=v+1 WHERE id=1");
              reach.reach("non-key update allowed");
              await all("INSERT INTO p VALUES(1,3,0) ON CONFLICT(id) DO NOTHING");
              reach.reach("DO NOTHING allowed");
              const sql =
                (deleting
                  ? "DELETE FROM p WHERE id=1"
                  : upsert
                    ? "INSERT INTO p VALUES(1,3,0) ON CONFLICT(id) DO UPDATE SET k=EXCLUDED.k"
                    : "UPDATE p SET k=k+2 WHERE id=1") + (returning ? " RETURNING *" : "");
              const direct = raw && mode !== "follow";
              if (direct) {
                const lowered = core.lowerer.lower(sql, core.context());
                if (lowered.kind !== "modify") throw new Error("modify");
                const tx = await core.harness.path.begin(),
                  statement = tx.statement({
                    rules: () => ({ check: () => null, generate: () => undefined }),
                  });
                const execution = core.executor.prepare(lowered.plan).execute({
                  snapshot: null,
                  statement,
                  parameters: [],
                  statementTime: 0n,
                  governor: core.harness.governor,
                  spill: core.spill,
                  queryId: 999991,
                  reads: {
                    open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                  },
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                });
                try {
                  await expect(execution.executed).rejects.toMatchObject({ sqlState: "0A000" });
                  await Promise.allSettled([execution.executed, execution.delivered]);
                } finally {
                  tx.rollback();
                }
              } else if (mode === "follow") await core.execute(sql);
              else await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "0A000" });
              const actual = await core.execute("SELECT * FROM p ORDER BY id"),
                child = await core.execute("SELECT * FROM c");
              if (
                actual.kind !== "rows" ||
                !("batches" in actual) ||
                child.kind !== "rows" ||
                !("batches" in child)
              )
                throw new Error("rows");
              expect(oracleRows(actual)).toEqual([
                { id: 1, k: mode === "follow" ? 3 : 1, v: 1 },
                { id: 2, k: 2, v: 0 },
              ]);
              expect(oracleRows(child)).toEqual([{ cid: 1, pid: mode === "follow" ? 3 : 1 }]);
              if (mode === "NO ACTION" || mode === "RESTRICT")
                await expect(pg.exec(sql)).rejects.toMatchObject({
                  code: mode === "RESTRICT" ? "23001" : "23503",
                });
              else {
                await pg.exec(sql);
                expect((await pg.query("SELECT pid FROM c")).rows).toEqual([
                  { pid: mode === "SET NULL" ? null : deleting || mode === "SET DEFAULT" ? 2 : 3 },
                ]);
              }
              // The shipped omitted-update contract is follow. New explicit actions cannot alias it.
              if (!deleting) {
                await shipped.run({ kind: "sql", sql: "UPDATE p SET k=k+2 WHERE id=1" });
                expect((await shipped.run({ kind: "sql", sql: "SELECT pid FROM c" })).rows).toEqual(
                  [{ pid: 3 }],
                );
              }
              reach.reach(mode);
              reach.reach(deleting ? "delete" : upsert ? "upsert" : "update");
              reach.reach(direct ? "raw guarded" : "owned guarded");
              reach.reach(returning ? "RETURNING" : "plain");
              expect(core.spill.bytesInUse).toBe(0);
            } finally {
              await core.close();
            }
            expect(core.harness.governor.usedBytes).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            ...modes.map((mode, i) => [mode, i % 2 === 0, i % 2 === 1, false]),
            ["NO ACTION", true, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
      await shipped.close();
    }
  },
  60000,
);
