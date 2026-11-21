/** A native trigger statement on a table with a full-text index ends without waiting while its
 * root holds credit. The root's own keyed DELETE is the statement's last write (only a BEFORE
 * trigger runs), so no later body step could have funded the statement-end FTS step: the root's
 * last native step (NativeRootCredits.prepareEnd) must. Run with MINNOW_GOVERNOR_STALL_MS and at
 * MINNOW_TRIGGER_CAP_MIB=32 and 64. Rows are compared with PostgreSQL (which has no FTS index here;
 * the index changes nothing SQL can observe in these tables). */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMAND_FTS_END_REACH } from "../../testing/reach.js";
import { ftsDef } from "../access/fts-def.js";
import { executeNativeCommand } from "../owner/native-command.js";
import { Governor } from "../memory/governor.js";
import { ChunkReader } from "../page/reader.js";
import { autocommit } from "../write/test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;

for (const seed of seedsFor("next-native-command-fts-end", [49761, 49762]))
  it(`ends an FTS-indexed native trigger statement without waiting (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("native command FTS end", NATIVE_COMMAND_FTS_END_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 6 }),
          fc.integer({ min: 0, max: 6 }),
          fc.boolean(),
          async (rows, deleted, published) => {
            reach.beginHistory();
            const governor = new Governor(capBytes),
              core = new StorageCore(64);
            Object.defineProperty(core.harness, "governor", { value: governor });
            await core.open();
            try {
              const values = Array.from(
                { length: rows },
                (_, i) => `(${String(i + 1)}, 'word${String(i)} common text')`,
              ).join(", ");
              const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY, body TEXT)",
                "CREATE TABLE log(id INT PRIMARY KEY, n INT)",
                `INSERT INTO p VALUES ${values}`,
                "INSERT INTO log VALUES (1, 0)",
              ];
              await pg.exec("DROP TABLE IF EXISTS p, log CASCADE; DROP FUNCTION IF EXISTS b()");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                "CREATE FUNCTION b() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE log SET n = n + 1 WHERE id = 1; RETURN OLD; END $$; CREATE TRIGGER b BEFORE DELETE ON p FOR EACH ROW EXECUTE FUNCTION b()",
              );
              for (const sql of setup) await core.execute(sql);
              await core.execute(
                "CREATE TRIGGER b BEFORE DELETE ON p BEGIN UPDATE log SET n = n + 1 WHERE id = 1; END",
              );
              const table = core.harness.path.catalog.tableByName("p"),
                column = table?.columns.find((c) => c.name === "body");
              if (!table || !column) throw new Error("missing FTS column");
              const { id: _id, ...fts } = ftsDef(1, table.id, column.id);
              void _id;
              await autocommit(core.harness.path, (s) =>
                s.changeCatalog({ kind: "createFts", fts }),
              );
              expect(core.harness.path.catalog.fts.length).toBe(1);
              if (published) {
                await core.harness.path.recycleLog();
                await core.harness.path.quiesce();
              }
              const mutation = `DELETE FROM p WHERE id <= ${String(deleted)}`;
              const tx = await core.harness.path.begin();
              const lowered = core.lowerer.lower(mutation, {
                ...core.context(),
                catalog: tx.catalog,
              });
              if (lowered.kind !== "modify") throw new Error("modify plan required");
              const result = await executeNativeCommand({
                transaction: tx,
                rootId: 1,
                plan: lowered.plan,
                parameters: [],
                planner: core.lowerer,
                context: { ...core.context(), catalog: tx.catalog },
                governor,
                signal: new AbortController().signal,
                execution: {
                  reads: {
                    open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options),
                  },
                  spill: core.spill,
                  queryId: 1,
                  statementTime: 0n,
                  sequences: null,
                  sequenceSession: null,
                  returning: "atExecuted",
                },
              }).catch((error: unknown) => {
                tx.rollback();
                throw error;
              });
              await (
                await tx.commit(null)
              ).durable;
              await pg.exec(mutation);
              expect(result.rowCount).toBe(Math.min(rows, deleted));
              for (const sql of ["SELECT * FROM p ORDER BY id", "SELECT * FROM log ORDER BY id"]) {
                const actual = await core.execute(sql);
                if (!("batches" in actual)) throw new Error("rows required");
                expect(oracleRows(actual), sql).toEqual((await pg.query(sql)).rows);
              }
              reach.reach(result.rowCount > 0 ? "keyed delete ended" : "nothing deleted");
              reach.reach(published ? "published source" : "resident source");
            } finally {
              await core.close();
              expect(governor.usedBytes).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [3, 2, false],
            [3, 3, true],
            [1, 0, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 600000);
