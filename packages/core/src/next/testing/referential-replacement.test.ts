/** Only NO ACTION may use an equivalent parent as an exemption from a named RI action. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_RI_REPLACEMENT_REACH } from "../../testing/reach.js";
import { referentialFixture } from "./referential.test-support.js";
import { batch, ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
for (const seed of seedsFor("next-native-ri-replacement", [0xb64d, 0xb64e]))
  it(`executes actions despite an equivalent replacement parent ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native RI replacement parents", NATIVE_RI_REPLACEMENT_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("CASCADE", "SET NULL", "SET DEFAULT"),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 2, max: 9 }),
          async (mode, update, published, count) => {
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT 1 REFERENCES p(id) ON DELETE ${mode} ON UPDATE ${mode},v INT)`,
                "INSERT INTO p VALUES(1,1),(2,2)",
                `INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String((i % 2) + 1)},1)`).join(",")}`,
              ],
              f = await referentialFixture(setup, published);
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                update
                  ? "WITH u AS(UPDATE p SET id=id+10 RETURNING id-10 AS id,v) INSERT INTO p SELECT id,v FROM u"
                  : "WITH d AS(DELETE FROM p RETURNING *) INSERT INTO p SELECT * FROM d",
              );
              const input = await f.capture("p", [0, 1]);
              for (const [i, row] of input.refs.entries()) {
                if (update)
                  await f.update(
                    input.tableId,
                    row.id,
                    batch(ints([i + 11]), ints([i + 1])),
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
              }
              for (let i = 0; i < 2; i++) {
                const next = await f.images.append(input.tableId, null, {
                  batch: batch(ints([i + 1]), ints([i + 1])),
                  row: 0,
                });
                await f.rows.write(
                  f.controller.root,
                  input.tableId,
                  "insert",
                  next,
                  null,
                  f.controller,
                  f.stop.signal,
                );
              }
              await f.end();
              await f.retire();
              const result = await f.core.execute("SELECT id,pid FROM c ORDER BY id");
              if (!("batches" in result)) throw new Error("Missing replacement child rows");
              expect(
                result.batches.flatMap((batch) =>
                  Array.from({ length: batch.length }, (_, i) =>
                    batch.columns.map((v) => valueAt(v, i)),
                  ),
                ),
              ).toEqual(
                (
                  await pg.query<Record<string, unknown>>("SELECT id,pid FROM c ORDER BY id")
                ).rows.map(Object.values),
              );
              reach.reach(mode);
              reach.reach(update ? "rekey" : "delete");
              reach.reach(published ? "published" : "resident");
              reach.reach("replacement parent");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            ["CASCADE", false, false, 5],
            ["SET NULL", true, true, 4],
            ["SET DEFAULT", false, true, 3],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  });
