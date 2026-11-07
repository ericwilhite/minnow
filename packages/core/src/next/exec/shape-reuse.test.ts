/** Descriptor planning is shared across scan sizing and reservation (column-target.ts contract).
 * A fresh loaded span needs at most one page lookup until its residency changes.
 * This property intentionally also runs against the pre-optimization implementation.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, SCAN_SHAPE_LOOKUP_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ChunkReader } from "../page/reader.js";
import { defined, writeGroup, UNLIMITED } from "../page/test-support.js";
import { ColumnTarget } from "./column-target.js";

it.each(seedsFor("next-shape-lookup", [0xb2d3, 0xb2d4]))(
  "one descriptor lookup across sizing/reservation (seed %i)",
  async (seed) => {
    const reach = createReach("next-shape-lookup", SCAN_SHAPE_LOOKUP_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 300, max: 800 }), fc.boolean(), async (count, wide) => {
        reach.beginHistory();
        const type = { kind: "int64" } as const;
        const { space, group } = await writeGroup(
          [
            {
              type,
              values: Array.from(
                { length: count },
                (_, row) => BigInt(row) + (wide ? 2n ** 60n : 0n),
              ),
            },
          ],
          { rawPageBytes: 2048 },
        );
        const reader = await ChunkReader.open(space, defined(group.columns[0]), {
          budget: UNLIMITED,
        });
        const target = new ColumnTarget(type, count, {
          take(bytes) {
            throw new Error(`Planning allocated ${String(bytes)} bytes`);
          },
          give(bytes) {
            throw new Error(`Planning freed ${String(bytes)} bytes`);
          },
        });
        try {
          await reader.load(reader.directory.pages.map((_, p) => p));
          const lookup = vi.spyOn(reader, "pagesInRange");
          for (const from of [0, 1, 2]) {
            const spec = { from, to: count };
            ColumnTarget.outputBytes(reader, type, spec, false, true);
            target.growthFor(reader, spec);
            ColumnTarget.outputBytes(reader, type, spec, false, true);
          }
          expect(
            lookup.mock.calls.length,
            "descriptor lookup repeated across sizing and reservation",
          ).toBe(1);
          reach.reach(wide ? "wide integers" : "safe integers");
          reach.reach("same endpoint pages");
          lookup.mockRestore();
        } finally {
          target.close();
          reader.close();
        }
      }),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
    console.log(
      "SCAN_SHAPE_LOOKUP_REACH",
      JSON.stringify({ seed, states: Object.fromEntries(reach.counts()) }),
    );
  },
);
