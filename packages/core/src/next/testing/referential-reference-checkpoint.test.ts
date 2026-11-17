/** PG RI skips an unchanged key from a committed child tuple, but not from a tuple
 * written in the current transaction. Subcommands must preserve that checkpoint rule. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_REFERENCE_CHECKPOINT_REACH } from "../../testing/reach.js";
import type { ReferentialEntry } from "../access/statement-command.js";
import { batch, ints, locators } from "../write/test-support.js";
import { referentialFixture } from "./referential.test-support.js";
async function code(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (typeof error === "object" && error !== null) {
      if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
      if ("code" in error && typeof error.code === "string") return error.code;
    }
    throw error;
  }
}
for (const seed of seedsFor("next-native-reference-checkpoint", [0xb669, 0xb66a]))
  it(`matches PG unchanged child references before RI seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native reference checkpoints", NATIVE_REFERENCE_CHECKPOINT_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.integer({ min: 0, max: 2 }),
          fc.boolean(),
          fc.integer({ min: 1, max: 99 }),
          async (update, priorKind, published, n) => {
            const fresh = priorKind !== 0;
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY)",
                "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE,v INT)",
                "INSERT INTO p VALUES(1)",
                ...(priorKind !== 1 ? [`INSERT INTO c VALUES(1,1,${String(n)})`] : []),
              ],
              f = await referentialFixture(setup, published, null, 256 << 20, {
                frameCount: 2,
                ...(fresh
                  ? {
                      prior: async (tx) => {
                        const prior = tx.statement({
                          rules: () => ({ generate: () => undefined, check: () => null }),
                        });
                        if (priorKind === 1)
                          await prior.table(2).insert(batch(ints([1]), ints([1]), ints([n])), null);
                        else {
                          const table = prior.catalog.table(2);
                          const valueColumn = table?.columns[2];
                          if (!table || !valueColumn) throw new Error("missing child table");
                          await prior
                            .table(2, { assigned: [valueColumn.id] })
                            .update(batch(locators([0], 2), ints([n + 10])), null);
                        }
                        await prior.end();
                      },
                    }
                  : {}),
              });
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              const event = update ? "UPDATE" : "DELETE";
              await pg.exec(
                `CREATE OR REPLACE FUNCTION reference_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE c SET v=v+1 WHERE pid=OLD.id; RETURN ${update ? "NEW" : "OLD"}; END $$; CREATE TRIGGER "A_before_RI" AFTER ${event} ON p FOR EACH ROW EXECUTE FUNCTION reference_checkpoint()`,
              );
              await pg.exec("BEGIN");
              if (priorKind === 1) await pg.exec(`INSERT INTO c VALUES(1,1,${String(n)})`);
              if (priorKind === 2) await pg.exec("UPDATE c SET v=v+10");
              const expected = await code(() =>
                pg.exec(update ? "UPDATE p SET id=2" : "DELETE FROM p"),
              );
              const childView = f.statement.view.internalTable(2),
                tail = childView.tableView.memtables.at(-1),
                childLocator =
                  priorKind === 2 && tail
                    ? childView.locator(
                        tail.firstRowId + tail.visibleRows(f.statement.view.bound) - 1,
                      )
                    : 0;
              const actual = await code(async () => {
                const parent = await f.capture("p", [0]),
                  child = await f.capture("c", [childLocator]),
                  p = parent.refs[0],
                  c = child.refs[0];
                if (!p || !c) throw new Error("rows");
                if (update) await f.update(parent.tableId, p.id, batch(ints([2])), p.image);
                else
                  await f.rows.write(
                    f.controller.root,
                    parent.tableId,
                    "delete",
                    p.image,
                    p.id,
                    f.controller,
                    f.stop.signal,
                  );
                const command = f.execution.beginCommand(2, 0),
                  table = f.statement.catalog.table(child.tableId);
                const valueColumn = table?.columns[2];
                if (!table || !valueColumn) throw new Error("table");
                const images = await f.images.derive(
                    c.image,
                    "old",
                    [
                      {
                        columnId: valueColumn.id,
                        value: ints([n + (priorKind === 2 ? 10 : 0) + 1]),
                      },
                    ],
                    f.stop.signal,
                  ),
                  pending: ReferentialEntry[] = [];
                await f.rows.write(
                  command,
                  table.id,
                  "update",
                  images,
                  c.id,
                  {
                    beforeRow: async () => undefined,
                    afterRow: async (_c, _t, _e, _i, entries) => {
                      for (const entry of entries) pending.push(entry);
                    },
                  },
                  f.stop.signal,
                );
                await f.execution.verifyCommandUnique(command, f.stop.signal);
                for (const entry of pending)
                  await f.execution.executeReferential(
                    entry,
                    { beforeRow: async () => undefined, afterRow: async () => undefined },
                    f.stop.signal,
                  );
                await f.execution.verifyCommandReferences(command, f.stop.signal);
                await f.end();
              });
              expect(expected).toBe(fresh ? "23503" : null);
              expect(actual).toBe(expected);
              if (actual) await f.rollback();
              await pg.exec(actual ? "ROLLBACK" : "COMMIT");
              reach.reach(update ? "rekey" : "delete");
              reach.reach(
                priorKind === 0
                  ? "committed version"
                  : priorKind === 1
                    ? "current transaction insert"
                    : "current transaction update",
              );
              reach.reach(published ? "published" : "resident");
            } finally {
              await pg.exec("ROLLBACK");
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        { seed, numRuns: 24 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
