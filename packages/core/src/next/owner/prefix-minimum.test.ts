/** A point prefix reader funds its actual source with the protected native owner,
 * rather than charging an absent worst-case published decoder at 32 MiB. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREFIX_MINIMUM_REACH } from "../../testing/reach.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { prefixFixture } from "./prefix-images.test-support.js";

for (const seed of seedsFor("next-native-prefix-minimum", [0xb72f, 0xb730]))
  it(`funds actual prefix sources at 32 MiB seed ${String(seed)}`, async () => {
    const reach = createReach("native prefix minimum", NATIVE_PREFIX_MINIMUM_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 6 }),
          fc.constantFrom(0, 20, 24000, 80000),
          fc.boolean(),
          fc.boolean(),
          async (count, width, published, nullable) => {
            reach.beginHistory();
            const f = await prefixFixture(count, width, published, nullable, false, 32 << 20, true),
              readers = f.statement.tx.hooks.shared.readers,
              baseReaders = readers.count,
              borrow = vi.spyOn(f.source, "borrow");
            let preparations = 0;
            f.setPrepare(async () => {
              preparations++;
              expect(readers.count, "funding retains no source pin").toBe(baseReaders);
              expect(f.source.allocated, "funding retains no decoder or recipe").toBe(0);
              expect(f.source.loaned).toBe(0);
            });
            try {
              await pg.exec("DROP TABLE IF EXISTS p;CREATE TABLE p(id INT,payload TEXT)");
              for (let row = 0; row < count; row++)
                await pg.query("INSERT INTO p VALUES($1,$2)", [row + 1, f.payloads[row]]);
              const expected = (
                await pg.query<{ id: number; payload: string | null }>(
                  "SELECT id,payload FROM p ORDER BY id",
                )
              ).rows;
              for (let row = 0; row < count; row++) {
                const id = f.ids[row];
                if (id === undefined) throw new Error("missing native row");
                const ref = await f.reader.capture(f.tableId, id, f.stop.signal),
                  lease = await f.images.readRowImages(ref, f.stop.signal);
                try {
                  const key = lease.old?.columns[0],
                    payload = lease.old?.columns[2];
                  if (!key || !payload) throw new Error("missing native OLD");
                  expect([valueAt(key, 0), valueAt(payload, 0)]).toEqual([
                    expected[row]?.id,
                    expected[row]?.payload,
                  ]);
                } finally {
                  lease.release();
                }
              }
              expect(preparations).toBeGreaterThanOrEqual(count);
              expect(borrow).not.toHaveBeenCalled();
              expect(f.h.governor.peakBytes).toBeLessThanOrEqual(32 << 20);
              reach.reach(published ? "published" : "resident");
              reach.reach(width > 0 ? "payload" : "empty payload");
              reach.reach("protected compound minimum at 32MiB");
              reach.reach("no pin across funding");
              reach.reach("exact PostgreSQL payload");
            } finally {
              borrow.mockRestore();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [1, 0, false, false],
            [6, 80000, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
