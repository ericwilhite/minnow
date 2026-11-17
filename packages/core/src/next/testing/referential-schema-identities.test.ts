/** Native RI captures the current stable-column schema after ALTER, and its accepted-row
 * undo/WAL retains stable index IDs through rollback, later ALTER and ordinary reopen. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_RI_SCHEMA_IDENTITY_REACH } from "../../testing/reach.js";
import { batch, ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { referentialFixture } from "./referential.test-support.js";
for (const seed of seedsFor("next-native-ri-schema-identities", [0xb671, 0xb672]))
  it(`preserves RI identities across ALTER and reopen seed ${String(seed)}`, async () => {
    const reach = createReach("native RI stable schemas", NATIVE_RI_SCHEMA_IDENTITY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        async (first, published, update, rollback) => {
          reach.beginHistory();
          const setup = [
              `CREATE TABLE p(id INT PRIMARY KEY,${first ? "obsolete INT,v INT" : "v INT,obsolete INT"})`,
              `CREATE TABLE c(id INT PRIMARY KEY,${first ? "obsolete INT,pid INT REFERENCES p(id) ON UPDATE CASCADE ON DELETE CASCADE,v INT" : "pid INT REFERENCES p(id) ON UPDATE CASCADE ON DELETE CASCADE,v INT,obsolete INT"})`,
              "CREATE INDEX obsolete_index ON c(obsolete)",
              "CREATE INDEX live_value_index ON c(v)",
              `INSERT INTO p(id,obsolete,v) VALUES(1,91,100),(2,92,200)`,
              `INSERT INTO c(id,pid,obsolete,v) VALUES(1,1,81,100),(2,2,82,200)`,
              "DROP INDEX obsolete_index",
              "ALTER TABLE p DROP COLUMN obsolete",
              "ALTER TABLE c DROP COLUMN obsolete",
            ],
            f = await referentialFixture(setup, published);
          try {
            const input = await f.capture("p", [0, 1]);
            for (const [i, row] of input.refs.entries())
              if (update)
                await f.update(
                  input.tableId,
                  row.id,
                  batch(ints([i + 11]), ints([(i + 1) * 100])),
                  row.image,
                );
              else
                await f.rows.write(
                  f.controller.root,
                  input.tableId,
                  "delete",
                  row.image,
                  row.id,
                  f.controller,
                  f.stop.signal,
                );
            if (rollback) {
              await f.controller.finishRoot();
              await f.rollback();
            } else await f.end();
            await f.retire();
            await f.core.execute("ALTER TABLE c ADD COLUMN extra INT DEFAULT 7");
            await f.core.harness.path.recycleLog();
            await f.core.reopen();
            const result = await f.core.execute("SELECT id,pid,v,extra FROM c ORDER BY id");
            if (!("batches" in result)) throw new Error("missing rows");
            expect(
              result.batches.flatMap((b) =>
                Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
              ),
            ).toEqual(
              !rollback && !update
                ? []
                : [
                    [1, rollback ? 1 : 11, 100, 7],
                    [2, rollback ? 2 : 12, 200, 7],
                  ],
            );
            reach.reach(first ? "drop earlier position" : "drop later position");
            reach.reach(published ? "published" : "resident");
            reach.reach(update ? "rekey" : "delete");
            reach.reach(rollback ? "rollback" : "commit");
            reach.reach("stable index identities");
            reach.reach("add after RI");
            reach.reach("ordinary reopen");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  }, 120000);
