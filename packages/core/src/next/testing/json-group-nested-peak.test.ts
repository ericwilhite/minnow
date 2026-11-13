/** Compare exact results and native peaks with independently measured fitting main controls. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, JSON_GROUP_NESTED_PEAK_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { taskClock, beginTask } from "../task.js";
import { oracleRows } from "./differential-adapter.js";
import { JsonCampaign } from "./json-construct-history.test.fixture.js";
import { nestedPeakCases, nestedPeakSql } from "./json-group-nested-peak.test.fixture.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-group-nested-peak", [0xb746, 0xb747]))(
  "keeps fitting nested-key peaks at or below main (seed %i)",
  async (seed) => {
    const reach = createReach("nested JSON grouping peaks", JSON_GROUP_NESTED_PEAK_REACH);
    const campaign = new JsonCampaign();
    try {
      await fc.assert(
        fc.asyncProperty(fc.constantFrom(...nestedPeakCases), async (data) => {
          campaign.guard();
          reach.beginHistory();
          const sql = nestedPeakSql(data);
          const expected = (await pg.query<{ j: string }>(sql)).rows.map((row) => row.j).sort();
          const history = await campaign.history(16 << 20);
          try {
            const prepare = history.core.executor.prepare.bind(history.core.executor);
            history.core.executor.prepare = (plan) => history.wrap(prepare(plan));
            // Frozen main52722050 controls use the same native task schedule. Wall-clock
            // scheduling may add an independent result slot; it is not a workspace delta.
            history.observe(vi.spyOn(taskClock, "now").mockReturnValue(0));
            beginTask();
            const result = await history.core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected row result");
            const actual = oracleRows(result)
              .map((row) => row.j)
              .sort();
            expect(actual).toEqual(expected);
            if (process.env.MINNOW_TRACE_JSON_STEP_PEAKS)
              console.log(
                JSON.stringify({
                  seed,
                  id: nestedPeakCases.indexOf(data),
                  peak: history.governor.peakBytes,
                  baseline: data.peak,
                }),
              );
            expect(history.governor.peakBytes).toBeLessThanOrEqual(data.peak);
            const widths: readonly number[] = data.widths;
            const widest = widths.indexOf(Math.max(...widths));
            reach.reach(
              data.widths.every((width) => width === data.widths[0])
                ? "uniform widths"
                : widest === 0
                  ? "wide first key"
                  : widest === 1
                    ? "wide middle key"
                    : "wide last key",
            );
            reach.reach(data.unicode ? "UTF8" : "ASCII");
            reach.reach(data.descending ? "descending aggregate" : "ascending aggregate");
            reach.reach(
              data.nullKey === 0
                ? "NULL first key"
                : data.nullKey === 1
                  ? "NULL middle key"
                  : "NULL last key",
            );
            reach.reach("sixteen MiB");
            reach.reach("nested keys");
            reach.reach("exact PostgreSQL text");
            reach.reach("no peak rise");
          } finally {
            await history.close();
            beginTask();
          }
          expect(history.governor.usedBytes).toBe(0);
          expect(history.core.spill.bytesInUse).toBe(0);
          reach.reach("zero resources");
        }),
        { seed, numRuns: 18, endOnFailure: true, examples: nestedPeakCases.map((data) => [data]) },
      );
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  },
  120000,
);
