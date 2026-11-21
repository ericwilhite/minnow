/** Informational-FK comparison visits only its actual OLD key operands. The root
 * funds that point source after retiring the prior decoder, never a future publication. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREFIX_VISIT_REACH } from "../../testing/reach.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { prefixFixture } from "./prefix-images.test-support.js";
for (const seed of seedsFor("next-native-prefix-visit", [49539, 49540]))
  it(`visits exact selected fields with no image retention (${String(seed)})`, async () => {
    const reach = createReach("native prefix visitor", NATIVE_PREFIX_VISIT_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 6 }),
          fc.constantFrom(0, 20, 24000),
          fc.boolean(),
          fc.boolean(),
          async (count, width, published, nullable) => {
            reach.beginHistory();
            const f = await prefixFixture(count, width, published, nullable, false, 32 << 20, true),
              append = vi.spyOn(f.images, "append"),
              baseline = f.statement.tx.hooks.shared.readers.count;
            f.setPrepare(async () => {
              expect(f.source.allocated).toBe(0);
              expect(f.source.loaned).toBe(0);
              expect(f.statement.tx.hooks.shared.readers.count).toBe(baseline);
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
              const columns = f.statement.catalog.table(f.tableId)?.columns;
              const id = columns?.[0]?.id,
                payload = columns?.[2]?.id;
              if (id === undefined || payload === undefined) throw new Error("Missing field IDs");
              for (const [row, rowId] of f.ids.entries()) {
                const reverse = row % 2 === 1;
                const wanted = reverse
                  ? [expected[row]?.payload, expected[row]?.id]
                  : [expected[row]?.id, expected[row]?.payload];
                let calls = 0;
                expect(
                  await f.reader.visit(
                    f.tableId,
                    rowId,
                    reverse ? [payload, id] : [id, payload],
                    f.stop.signal,
                    (batch, at) => {
                      calls++;
                      expect(
                        batch.columns.map((column) => valueAt(column, at)),
                        "only selected key operands are decoded",
                      ).toEqual(wanted);
                      return row % 2 === 0;
                    },
                  ),
                ).toBe(row % 2 === 0);
                expect(calls).toBe(1);
                expect(f.source.allocated).toBe(0);
                expect(f.source.loaned).toBe(0);
                expect(f.statement.tx.hooks.shared.readers.count).toBe(baseline);
              }
              expect(append).not.toHaveBeenCalled();
              reach.reach(published ? "published" : "resident");
              reach.reach(nullable ? "nullable" : "nonnullable");
              reach.reach("exact selected operands");
              reach.reach("no output image copy");
              reach.reach("no source at funding");
            } finally {
              append.mockRestore();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [1, 0, false, false],
            [6, 24000, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
