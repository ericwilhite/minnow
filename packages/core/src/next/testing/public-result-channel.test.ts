/** The canonical decoder must preserve the exact metadata across a real MessageChannel. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_RESULT_CHANNEL_REACH } from "../../testing/reach.js";
import { decodeResult } from "../owner/result.js";
import { publicOwner } from "./public-owner.test.fixture.js";

it.each(seedsFor("next-public-result-channel", [0xb734, 0xb735]))(
  "decodes native protocol replies without legacy scalar coercions (seed %i)",
  async (seed) => {
    const owner = await publicOwner();
    const reach = createReach("public Result channel", PUBLIC_RESULT_CHANNEL_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.bigInt({ min: -(1n << 62n), max: (1n << 62n) - 1n }),
          fc.integer({ min: 0, max: 12 }),
          async (value, scale) => {
            reach.beginHistory();
            const raw = await owner.client.query("SELECT $1::bigint n, $2::numeric d", [
              value,
              `1.${"0".repeat(scale)}5`,
            ]);
            const result = decodeResult(raw);
            expect(result.tuples()).toEqual([[value, `1.${"0".repeat(scale)}5`]]);
            expect(result.rows()).toEqual([{ n: value, d: `1.${"0".repeat(scale)}5` }]);
            expect(result.column("n")).toMatchObject({ kind: "int64" });
            reach.reach("MessageChannel native execution");
            reach.reach("numeric display scale");
            reach.reach(
              value >= -9007199254740991n && value <= 9007199254740991n
                ? "safe int8 stays bigint"
                : "wide int8 stays exact",
            );
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            [0n, 0],
            [9007199254740993n, 12],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await owner.close();
      expect(owner.core.harness.governor.usedBytes).toBe(0);
      expect(owner.core.spill.bytesInUse).toBe(0);
    }
  },
  60000,
);
