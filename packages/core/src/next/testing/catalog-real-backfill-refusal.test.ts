/** Retain the existing explicit runtime-cast limit; an API Float64 shortcut must not
 * approximate SQL REAL constants (overflow/underflow/tie cases need the executor cast).
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_REAL_BACKFILL_REACH, createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";

for (const seed of seedsFor("next-catalog-real-backfill-refusal", [0xb7ee, 0xb7ef]))
  it(`refuses executor-only REAL backfills before catalog mutation seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("REAL backfill boundary", CATALOG_REAL_BACKFILL_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("ordinary", "overflow", "underflow"),
          fc.integer({ min: -1000, max: 1000 }),
          async (kind, n) => {
            reach.beginHistory();
            const value =
              kind === "ordinary"
                ? n + 0.125
                : (n < 0 ? -1 : 1) *
                  (Math.abs(n) + 2) *
                  (kind === "overflow" ? 2 ** 128 : Number.MIN_VALUE);
            const source = String(value),
              core = await new StorageCore().open();
            try {
              let pgState = "rows";
              try {
                await pg.query(`SELECT CAST(${source} AS REAL)`);
              } catch (error) {
                if (!error || typeof error !== "object" || !("code" in error)) throw error;
                pgState = String(error.code);
              }
              expect(pgState).toBe(kind === "ordinary" ? "rows" : "22003");
              await core.execute("CREATE TABLE f(id INTEGER PRIMARY KEY)");
              const before = core.harness.path.catalog.encode();
              await expect(
                core.execute(`ALTER TABLE f ADD COLUMN x REAL DEFAULT ${source}`),
              ).rejects.toMatchObject({ sqlState: "0A000" });
              expect(core.harness.path.catalog.encode()).toEqual(before);
              reach.reach(kind);
              reach.reach("catalog unchanged");
            } finally {
              await core.close();
            }
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("zero resources");
          },
        ),
        {
          seed,
          numRuns: 12,
          examples: [
            ["ordinary", 0],
            ["overflow", 1],
            ["underflow", 1],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
