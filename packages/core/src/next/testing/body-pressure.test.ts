/** Competing credit cannot strand a compiled producer/queued body at 64 MiB. All parent
 * images and body inputs share fragmented storage; event count creates no new files. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_PRESSURE_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { referentialFixture } from "./referential.test-support.js";
for (const seed of seedsFor("next-native-body-pressure", [0xb661, 0xb662]))
  it(`completes compiled modifications with bounded files seed ${String(seed)}`, async () => {
    const reach = createReach("native body pressure", NATIVE_BODY_PRESSURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 16, max: 64 }),
        fc.boolean(),
        fc.boolean(),
        async (count, published, wide) => {
          reach.beginHistory();
          const width = wide ? 8192 : 0,
            payload = "x".repeat(width),
            setup = [
              "CREATE TABLE p(id INT PRIMARY KEY,v INT CHECK(v>0),unused TEXT)",
              "CREATE TABLE audit(id INT,v INT)",
            ];
          for (let start = 0; start < count; start += 16)
            setup.push(
              `INSERT INTO p VALUES${Array.from({ length: Math.min(16, count - start) }, (_, i) => `(${String(start + i + 1)},1,'${payload}')`).join(",")}`,
            );
          setup.push(
            "CREATE TRIGGER body AFTER UPDATE ON p BEGIN INSERT INTO audit VALUES(NEW.id,NEW.v); END",
          );
          const f = await referentialFixture(
              setup,
              published,
              null,
              64 << 20,
              "UPDATE p SET v=v+1",
            ),
            gov = f.core.harness.governor,
            competing = await gov.admit({
              label: "compiled body competitor",
              minimumBytes: 4 << 20,
              spillable: false,
            }),
            creates = vi.spyOn(f.spill, "create");
          expect(competing.allocate(competing.held)).toBe(true);
          try {
            await f.nativeRoot();
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            await f.end();
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            expect(creates.mock.calls.length).toBeLessThan(12);
            expect(gov.peakBytes).toBeLessThanOrEqual(gov.capBytes);
            competing.close();
            await f.retire();
            const result = await f.core.execute("SELECT id,v FROM audit ORDER BY id");
            if (!("batches" in result)) throw new Error("rows");
            expect(oracleRows(result)).toEqual(
              Array.from({ length: count }, (_, i) => ({ id: i + 1, v: 2 })),
            );
            reach.reach(published ? "published" : "resident");
            reach.reach(wide ? "wide unused payload" : "small rows");
            reach.reach("64MiB competing credit");
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
          [32, false, true],
          [16, true, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
