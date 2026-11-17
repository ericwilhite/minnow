/** Rule generation owns its wrappers; immutable registry borrows are never its targets. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_ROW_BORROW_REACH } from "../../testing/reach.js";
import { referentialFixture } from "./referential.test-support.js";
import { batch, ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
for (const seed of seedsFor("next-native-row-borrows", [0xb64f, 0xb650]))
  it(`generates through owned wrappers with frozen registry leases ${String(seed)}`, async () => {
    const reach = createReach("native row image borrows", NATIVE_ROW_BORROW_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 2, max: 8 }), async (published, count) => {
        reach.beginHistory();
        const f = await referentialFixture(
            [
              "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
              "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id),v INT GENERATED ALWAYS AS(pid+10) STORED)",
              "INSERT INTO p VALUES(1,1),(2,2)",
              `INSERT INTO c(id,pid) VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String((i % 2) + 1)})`).join(",")}`,
            ],
            published,
          ),
          real = f.images.readRowImages.bind(f.images);
        let frozen = 0,
          placeholders = 0;
        const child = f.statement.catalog.tableByName("c"),
          g = child?.columns[2],
          realBefore = f.controller.beforeRow.bind(f.controller);
        if (!child || !g) throw new Error("Missing generated child");
        const before = vi.spyOn(f.controller, "beforeRow").mockImplementation(async (...args) => {
          if (args[1] === child.id) {
            const lease = await f.images.readRowBindings(
              args[3],
              { old: [], new: [g.id] },
              f.stop.signal,
            );
            try {
              const value = lease.new?.[2];
              if (!value || typeof value === "symbol")
                throw new Error("Missing generated placeholder");
              expect(valueAt(value, 0), "BEFORE sees a generated placeholder").toBeNull();
              placeholders++;
            } finally {
              lease.release();
            }
          }
          await realBefore(...args);
        });
        const borrow = vi.spyOn(f.images, "readRowImages").mockImplementation(async (...args) => {
          const lease = await real(...args);
          for (const image of [lease.old, lease.new])
            for (const vector of image?.columns ?? []) {
              Object.freeze(vector);
              frozen++;
            }
          return lease;
        });
        try {
          const input = await f.capture("p", [0, 1]);
          for (const [i, row] of input.refs.entries())
            await f.update(input.tableId, row.id, batch(ints([2 - i]), ints([i + 1])), row.image);
          await f.end();
          expect(frozen).toBeGreaterThan(0);
          expect(placeholders).toBe(count);
          await f.retire();
          const result = await f.core.execute("SELECT id,pid,v FROM c ORDER BY id");
          if (!("batches" in result)) throw new Error("Missing generated children");
          expect(
            result.batches.flatMap((batch) =>
              Array.from({ length: batch.length }, (_, i) =>
                batch.columns.map((v) => valueAt(v, i)),
              ),
            ),
          ).toEqual(Array.from({ length: count }, (_, i) => [i + 1, 2 - (i % 2), 12 - (i % 2)]));
          reach.reach(published ? "published" : "resident");
          reach.reach("frozen leases");
          reach.reach("generated values");
          reach.reach("BEFORE placeholders");
        } finally {
          borrow.mockRestore();
          before.mockRestore();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [false, 3],
          [true, 8],
        ],
      },
    );
    reach.assertReached();
  });
