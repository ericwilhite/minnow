/** A small view cannot smuggle an unbounded backing buffer into the fixed owner. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITER_RESIDENT_BACKING_REACH } from "../../testing/reach.js";
import { countingBudget } from "./space-test-support.js";
import { RETIREMENT_OWNER_BYTES } from "./retirement-inventory.js";
import type { RetirementInventory } from "./retirement-contract.js";
import { makeResidentWriterInput } from "./writer-resident.js";
for (const seed of seedsFor("next-writer-resident-backing", [0xb94d, 0xb94e]))
  it(`refuses oversized resident backing seed ${String(seed)}`, async () => {
    const reach = createReach("resident writer backing bounds", WRITER_RESIDENT_BACKING_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1025, max: 8192 }), async (size) => {
        reach.beginHistory();
        const buffer = new Uint8Array(size),
          bytes = buffer.subarray(0, 16);
        new DataView(bytes.buffer).setFloat64(8, 1, true);
        const budget = countingBudget(RETIREMENT_OWNER_BYTES);
        expect(budget.allocate(RETIREMENT_OWNER_BYTES)).toBe(true);
        let inventory: RetirementInventory | undefined;
        try {
          expect(() => {
            inventory = makeResidentWriterInput(bytes, 1, budget, new Set());
          }).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
          reach.reach("small view with oversized backing refused");
        } finally {
          if (inventory === undefined) budget.free(RETIREMENT_OWNER_BYTES);
          else await inventory.release();
        }
        expect(budget.used).toBe(0);
        reach.reach("no owner credit leak");
      }),
      { seed, numRuns: 4, examples: [[1025], [8192]] },
    );
    reach.assertReached();
  });
