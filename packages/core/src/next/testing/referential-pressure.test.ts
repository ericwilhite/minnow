/** Bounded completion/ownership for real RI child payloads under a 64MiB cap. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_RI_PRESSURE_REACH } from "../../testing/reach.js";
import { referentialFixture } from "./referential.test-support.js";
import { batch, ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
for (const seed of seedsFor("next-native-ri-pressure", [0xb641, 0xb642]))
  it(`completes streamed native RI under competing credit ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native RI pressure", NATIVE_RI_PRESSURE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 512, max: 1800 }),
          fc.boolean(),
          fc.boolean(),
          async (count, update, published) => {
            reach.beginHistory();
            const setup = [
              "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
              "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE,v INT)",
              "INSERT INTO p VALUES(1,1),(2,2)",
            ];
            for (let start = 0; start < count; start += 200)
              setup.push(
                `INSERT INTO c VALUES ${Array.from({ length: Math.min(200, count - start) }, (_, i) => `(${String(start + i + 1)},${String(((start + i) % 2) + 1)},${String(start + i)})`).join(",")}`,
              );
            const f = await referentialFixture(setup, published, null, 64 * 1024 * 1024),
              governor = f.core.harness.governor,
              competing = await governor.admit({
                label: "RI competing retained workspace",
                minimumBytes: Math.max(
                  0,
                  governor.capBytes - governor.reserveBytes - governor.usedBytes - 1024 * 1024,
                ),
                spillable: false,
              }),
              creates = vi.spyOn(f.spill, "create");
            expect(competing.allocate(competing.held)).toBe(true);
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(update ? "UPDATE p SET id=id+10" : "DELETE FROM p");
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
              const paid = f.charge.allocated;
              await f.end();
              expect(f.source.allocated).toBe(0);
              expect(f.source.loaned).toBe(0);
              expect(f.charge.allocated).toBeLessThanOrEqual(paid);
              expect(creates.mock.calls.length, "no file per child rowset/row").toBeLessThan(12);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              competing.close();
              await f.retire();
              const actual = await f.core.execute(
                "SELECT CAST(count(*) AS INT) AS n,CAST(sum(pid) AS INT) AS total FROM c",
              );
              if (!("batches" in actual)) throw new Error("Missing child aggregate");
              expect(
                actual.batches.flatMap((batch) =>
                  Array.from({ length: batch.length }, (_, i) =>
                    batch.columns.map((v) => valueAt(v, i)),
                  ),
                ),
              ).toEqual(
                (
                  await pg.query<Record<string, unknown>>(
                    "SELECT count(*)::int AS n,sum(pid)::int AS total FROM c",
                  )
                ).rows.map(Object.values),
              );
              reach.reach(update ? "rekey" : "delete");
              reach.reach(published ? "published" : "resident");
              reach.reach("64MiB");
              reach.reach("competing credit");
              reach.reach("many child segments");
              reach.reach("bounded files");
            } finally {
              creates.mockRestore();
              competing.close();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 4,
          examples: [
            [1024, true, false],
            [512, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
