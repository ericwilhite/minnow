/** The requested batch target is an upper bound, not a minimum allocation for rows
 * that do not exist. Native hints bound every shifted prefix of the measured source. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREFIX_OUTPUT_BOUND_REACH } from "../../testing/reach.js";
import { boundSource } from "../page/read-bounds.js";
import { prefixFixture } from "./prefix-images.test-support.js";

for (const seed of seedsFor("next-native-prefix-output-bound", [0xb735, 0xb736]))
  it(`bounds actual short-source output seed ${String(seed)}`, async () => {
    const reach = createReach("native prefix output bound", NATIVE_PREFIX_OUTPUT_BOUND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 12 }),
        fc.integer({ min: 0, max: 24000 }),
        fc.constantFrom(16, 4096, 1 << 20),
        fc.boolean(),
        async (count, width, target, retained) => {
          reach.beginHistory();
          const f = await prefixFixture(count, width, false, true, false, 32 << 20),
            pin = f.statement.readOwn(),
            table = pin.table(f.tableId, { budget: f.root }),
            recipe = retained ? table.prepareRead({ budget: f.root }) : null;
          try {
            if (retained && !recipe) throw new Error("missing retained source");
            const source = recipe?.table ?? table,
              segment = source.segments[0],
              columns = source.columns.map((c) => c.id),
              signal = f.stop.signal;
            if (segment?.kind !== "memory") throw new Error("expected native tail");
            const context = { budget: f.root, pages: pin.pages, signal };
            let total = 0,
              shifted = 0;
            for (let row = 0; row < count; row++) {
              const one = await segment.readHint(
                { columns, chunk: 0, startRow: row, maxRows: 1, targetBytes: target },
                context,
              );
              total += one.outputBytes;
              const prefix = await segment.readHint(
                { columns, chunk: 0, startRow: row, maxRows: count - row, targetBytes: target },
                context,
              );
              shifted = Math.max(shifted, prefix.outputBytes);
            }
            const bound = await boundSource(
              { pages: pin.pages, table: () => source },
              {
                id: 1,
                tableId: f.tableId,
                columns,
                types: source.columns.map((c) => c.type),
                windowBytes: 0,
                stagingBytes: 4096,
                rows: null,
              },
              f.h.governor,
              signal,
              target,
            );
            expect(
              bound.progress,
              "every shifted native prefix remains covered",
            ).toBeGreaterThanOrEqual(4 * shifted);
            expect(
              bound.progress,
              "absent rows cannot require the full batch target",
            ).toBeLessThanOrEqual(Math.max(65536, 4 * total));
            expect(bound.bytes).toBeGreaterThanOrEqual(4096 + 2 * shifted);
            expect(bound.valueBytes).toBeLessThanOrEqual(total);
            reach.reach(retained ? "retained" : "publication-stable hint");
            reach.reach(count === 1 ? "one row" : "several rows");
            if (shifted > target) reach.reach("oversized row");
            if (total < target) reach.reach("short source below target");
            reach.reach("all shifted prefixes covered");
          } finally {
            recipe?.close();
            pin.release();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          [1, 0, 1 << 20, true],
          [3, 24000, 16, false],
          [4, 20, 4096, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
