/** PG authority for composite RI key order and complete child-row continuations. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_RI_COMPOSITE_REACH } from "../../testing/reach.js";
import { referentialFixture } from "./referential.test-support.js";
import { batch, ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
for (const seed of seedsFor("next-native-ri-composite", [0xb63f, 0xb640]))
  it(`matches PG composite RI with reversed references ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native composite RI", NATIVE_RI_COMPOSITE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("CASCADE", "SET NULL", "SET DEFAULT"),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 2, max: 11 }),
          async (mode, reverse, update, published, count) => {
            reach.beginHistory();
            const setup = [
                `CREATE TABLE p(id INT PRIMARY KEY,a INT,b INT,UNIQUE(${reverse ? "b,a" : "a,b"}))`,
                `CREATE TABLE c(id INT PRIMARY KEY,x INT DEFAULT(90+9),y INT DEFAULT(190+9),FOREIGN KEY(${reverse ? "y,x" : "x,y"}) REFERENCES p(${reverse ? "b,a" : "a,b"}) ON DELETE ${mode} ON UPDATE ${mode})`,
                "INSERT INTO p VALUES(1,1,101),(2,2,102),(99,99,199)",
                `INSERT INTO c VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String((i % 2) + 1)},${String(101 + (i % 2))})`).join(",")}`,
              ],
              f = await referentialFixture(setup, published);
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                update ? "UPDATE p SET a=a+10,b=b+20 WHERE id<3" : "DELETE FROM p WHERE id<3",
              );
              const input = await f.capture("p", [0, 1]);
              for (const [i, row] of input.refs.entries()) {
                if (update)
                  await f.update(
                    input.tableId,
                    row.id,
                    batch(ints([i + 1]), ints([i + 11]), ints([i + 121])),
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
              await f.end();
              const result = await f.core.execute("SELECT id,x,y FROM c ORDER BY id");
              if (!("batches" in result)) throw new Error("Missing native child rows");
              expect(
                result.batches.flatMap((batch) =>
                  Array.from({ length: batch.length }, (_, i) =>
                    batch.columns.map((vector) => valueAt(vector, i)),
                  ),
                ),
              ).toEqual(
                (
                  await pg.query<Record<string, unknown>>("SELECT id,x,y FROM c ORDER BY id")
                ).rows.map(Object.values),
              );
              reach.reach(mode);
              reach.reach(reverse ? "reversed key" : "declared key");
              reach.reach(update ? "rekey" : "delete");
              reach.reach(published ? "published" : "resident");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            ["CASCADE", true, true, true, 4],
            ["SET NULL", false, false, false, 4],
            ["SET DEFAULT", true, true, false, 3],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  });
