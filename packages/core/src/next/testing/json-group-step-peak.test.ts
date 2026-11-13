/** PG grouping equality is independent of the number/width of other key columns. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  JSON_GROUP_STEP_REACH,
  JSON_GROUP_STEP_COUNTS_REACH,
} from "../../testing/reach.js";
import { JsonCampaign } from "./json-construct-history.test.fixture.js";
import { stepHistory, retireStepHistories } from "./json-group-step-peak.test.fixture.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterEach(retireStepHistories);
afterAll(async () => {
  await pg.close();
});
describe.each(seedsFor("next-json-group-step-peak", [0xb740, 0xb741]))(
  "step peak seed %i",
  (seed) => {
    for (const family of ["literal", "computed", "jsonb", "array"] as const)
      for (let keys = 1; keys <= (family === "literal" || family === "computed" ? 16 : 4); keys++)
        describe(`${family} ${String(keys)} keys at 16MiB`, () => {
          const countState = JSON_GROUP_STEP_COUNTS_REACH[keys - 1];
          if (!countState) throw new Error("Invalid semantic key-count dimension");
          const reach = createReach<string>("JSON group step peak", [
            ...JSON_GROUP_STEP_REACH,
            countState,
            ...(family === "literal" ? ["single input"] : []),
          ]);
          // Replay the same6-run RNG sequence per bounded body. Exactly ONE indexed
          // history executes; the other5 only advance fast-check's original sequence.
          // Across these6 bodies all4examples+2generated histories remain unchanged.
          for (let slot = 0; slot < 6; slot++)
            it(`history ${String(slot)}`, async () => {
              const campaign = new JsonCampaign();
              let seen = 0,
                executed = 0;
              await fc.assert(
                fc.asyncProperty(
                  fc.array(fc.integer({ min: 512, max: 65536 }), {
                    minLength: keys,
                    maxLength: keys,
                  }),
                  fc.integer({ min: 2, max: 17 }),
                  fc.constantFrom(0, 65536),
                  fc.boolean(),
                  fc.boolean(),
                  fc.boolean(),
                  async (widths, rows, payload, unicode, filter, nullKey) => {
                    if (seen++ !== slot) return;
                    executed++;
                    campaign.guard();
                    reach.beginHistory();
                    const history = await campaign.history(16 << 20);
                    try {
                      const result = await stepHistory(history, pg, {
                        family,
                        widths,
                        rows,
                        payload,
                        unicode,
                        filter,
                        nullKey,
                      });
                      reach.reach(countState);
                      reach.reach("sixteen MiB cap");
                      reach.reach("all competing credit occupied");
                      reach.reach("exact PostgreSQL text");
                      reach.reach(payload ? "wide payload" : "tiny payload");
                      reach.reach(unicode ? "UTF8" : "ASCII");
                      reach.reach(filter ? "FILTER" : "unfiltered");
                      reach.reach(nullKey ? "SQL NULL key" : "nonnull keys");
                      reach.reach(rows === 1 ? "single input" : "multiple inputs");
                      if (result.spilled) reach.reach("observed spill");
                    } finally {
                      await history.close();
                    }
                    expect(history.governor.usedBytes).toBe(0);
                    expect(history.core.spill.bytesInUse).toBe(0);
                    reach.reach("zero resources");
                  },
                ),
                {
                  seed,
                  numRuns: 6,
                  endOnFailure: true,
                  examples: [
                    [
                      Array.from({ length: keys }, () => 65536),
                      family === "literal" ? 1 : 2,
                      0,
                      false,
                      false,
                      false,
                    ],
                    [Array.from({ length: keys }, () => 4681), 2, 65536, true, true, false],
                    [Array.from({ length: keys }, () => 4096), 17, 0, false, true, false],
                    [Array.from({ length: keys }, () => 2048), 3, 65536, false, false, true],
                  ],
                },
              );
              expect(seen).toBe(6);
              expect(executed).toBe(1);
              await campaign.close();
            }, 120000);
          it("Reach across all6histories", () => reach.assertReached());
        });
  },
);

// OPEN-ISSUES.md, "2026-11-13: found by B6's step-peak sweep": B5's
// recycleLog loop, then B2/B4's source admission. Re-enable this exact history
// when those entries close. The general stored sweep retains 1..4 wide keys.
it.skip("known source blocker: six wide stored JSONB keys at 16MiB", async () => {
  const campaign = new JsonCampaign(),
    history = await campaign.history(16 << 20);
  try {
    await stepHistory(history, pg, {
      family: "jsonb",
      storedKeyBudget: 262144,
      widths: Array.from({ length: 6 }, () => 65536),
      rows: 2,
      payload: 0,
      unicode: false,
      filter: false,
      nullKey: false,
    });
  } finally {
    await history.close();
    await campaign.close();
  }
});
