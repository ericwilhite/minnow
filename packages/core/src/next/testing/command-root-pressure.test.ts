/** Root assembly shares prepaid work with the real compiler/writer under competing credit. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMAND_ROOT_PRESSURE_REACH } from "../../testing/reach.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { commandRootFixture } from "./command-root.test-support.js";
for (const seed of seedsFor("next-native-command-root-pressure", [0xb67b, 0xb67c]))
  it(`completes assembled roots at 64MiB with a competitor seed ${String(seed)}`, async () => {
    const reach = createReach("native assembled root pressure", NATIVE_COMMAND_ROOT_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 16, max: 64 }), fc.boolean(), async (count, published) => {
        reach.beginHistory();
        const payload = "x".repeat(8192),
          setup = [
            "CREATE TABLE audit(id INT)",
            "CREATE TABLE p(id INT PRIMARY KEY,v INT,unused TEXT)",
            `INSERT INTO p VALUES ${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i)},'${payload}')`).join(",")}`,
            "CREATE TRIGGER after_p AFTER UPDATE ON p BEGIN INSERT INTO audit VALUES(NEW.id); END",
          ],
          f = await commandRootFixture(setup, "UPDATE p SET v=v+1", published),
          gov = f.core.harness.governor,
          competitor = await gov.admit({
            label: "assembled root competitor",
            minimumBytes: Math.max(0, gov.capBytes - gov.reserveBytes - gov.usedBytes - (1 << 20)),
            spillable: false,
          }),
          creates = vi.spyOn(f.spill, "create");
        expect(competitor.allocate(competitor.held)).toBe(true);
        try {
          expect(await f.root.run({ slots: [] })).toBe(count);
          expect(f.source.allocated).toBe(0);
          expect(f.source.loaned).toBe(0);
          await f.commit();
          expect(creates.mock.calls.length).toBeLessThan(12);
          expect(gov.peakBytes).toBeLessThanOrEqual(gov.capBytes);
          competitor.close();
          await f.retire();
          const result = await f.core.execute("SELECT id FROM audit ORDER BY id");
          if (!("batches" in result)) throw new Error("audit rows");
          expect(
            result.batches.flatMap((b) =>
              Array.from({ length: b.length }, (_, i) =>
                valueAt(
                  b.columns[0] ??
                    (() => {
                      throw new Error("id");
                    })(),
                  i,
                ),
              ),
            ),
          ).toEqual(Array.from({ length: count }, (_, i) => i + 1));
          reach.reach(published ? "published" : "resident");
          reach.reach("64MiB competing credit");
          reach.reach("wide unrelated payload");
          reach.reach("bounded files");
        } finally {
          creates.mockRestore();
          competitor.close();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [64, true],
          [16, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
