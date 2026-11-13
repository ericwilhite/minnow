/** Compare exact results and native peaks with independently measured fitting main controls. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { createReach, JSON_GROUP_NESTED_PEAK_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { taskClock, beginTask } from "../task.js";
import { oracleRows } from "./differential-adapter.js";
import { JsonCampaign } from "./json-construct-history.test.fixture.js";
import {
  generatedNestedSql,
  observeNestedLifetime,
  type GeneratedNestedCase,
} from "./json-group-generated-lifetime.test.fixture.js";
import { JSON_GROUP_NESTED_GENERATED_REACH } from "../../testing/reach.js";
import { nestedPeakCases, nestedPeakSql } from "./json-group-nested-peak.test.fixture.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
// Finite historical controls retain their original seeds, 18runs and all12examples.
// The independent campaign below explores widths, key counts, nesting and NULL layouts.
it.each(seedsFor("next-json-group-nested-peak", [0xb746, 0xb747]))(
  "keeps frozen fitting nested-key peaks at or below main (seed %i)",
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

it.each(seedsFor("next-json-group-nested-generated", [0xb74a, 0xb74b]))(
  "generates nested key lifetimes and PG results (seed %i)",
  async (seed) => {
    const reach = createReach("generated nested key lifetimes", JSON_GROUP_NESTED_GENERATED_REACH);
    const campaign = new JsonCampaign();
    const examples: GeneratedNestedCase[] = [
      { widths: [128], depth: 1, nullGroups: 0, unicode: false, descending: false },
      { widths: [43690, 512, 512], depth: 2, nullGroups: 1, unicode: true, descending: true },
      { widths: [1024, 8192], depth: 5, nullGroups: 2, unicode: false, descending: true },
      {
        widths: [512, 1024, 2048, 256, 128, 64],
        depth: 3,
        nullGroups: 3,
        unicode: true,
        descending: false,
      },
      { widths: [8192, 64, 2048, 128], depth: 4, nullGroups: 0, unicode: false, descending: false },
      { widths: [2048, 8192, 512], depth: 1, nullGroups: 2, unicode: true, descending: true },
    ];
    const pinned = new Set(examples.map((data) => JSON.stringify(data)));
    const randomShapes = new Set<string>();
    let seen = 0;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            widths: fc.array(fc.integer({ min: 64, max: 65536 }), { minLength: 1, maxLength: 6 }),
            depth: fc.integer({ min: 1, max: 5 }),
            nullGroups: fc.integer({ min: 0, max: 3 }),
            unicode: fc.boolean(),
            descending: fc.boolean(),
          }),
          async (data) => {
            campaign.guard();
            reach.beginHistory();
            const generated = seen++ >= examples.length;
            const sql = generatedNestedSql(data);
            const expected = (await pg.query<{ j: string }>(sql)).rows.map((row) => row.j).sort();
            const history = await campaign.history(16 << 20);
            try {
              const states = observeNestedLifetime(history);
              const prepare = history.core.executor.prepare.bind(history.core.executor);
              history.core.executor.prepare = (plan) => history.wrap(prepare(plan));
              const result = await history.core.execute(sql);
              if (!("batches" in result)) throw new Error("Expected generated row result");
              expect(
                oracleRows(result)
                  .map((row) => row.j)
                  .sort(),
              ).toEqual(expected);
              expect(states.boundary).toBe(true);
              expect(history.governor.peakBytes).toBeLessThanOrEqual(16 << 20);
              reach.reach(data.widths.length === 1 ? "one key" : "multiple keys");
              if (data.widths.length === 6) reach.reach("six keys");
              if (Math.max(...data.widths) >= 32768) reach.reach("wide widths");
              if (Math.max(...data.widths) <= 2048) reach.reach("narrow widths");
              reach.reach(data.depth === 1 ? "shallow nesting" : "nested containers");
              if (data.depth >= 4) reach.reach("deep nesting");
              reach.reach(data.nullGroups ? "SQL NULL groups" : "no NULL groups");
              if (expected.length >= 3) reach.reach("multiple NULL groups");
              reach.reach(data.unicode ? "UTF8" : "ASCII");
              if (states.cached) reach.reach("observed resident cache");
              if (states.retired) reach.reach("decoded cache retired");
              if (states.growth) reach.reach("unused growth returned");
              if (generated && !pinned.has(JSON.stringify(data))) {
                randomShapes.add(sql);
                reach.reach("new random shape");
              }
              reach.reach("exact PostgreSQL text");
            } finally {
              await history.close();
            }
            expect(history.governor.usedBytes).toBe(0);
            expect(history.core.spill.bytesInUse).toBe(0);
            reach.reach("zero resources");
          },
        ),
        { seed, numRuns: 30, endOnFailure: true, examples: examples.map((data) => [data]) },
      );
      expect(seen).toBe(30);
      expect(randomShapes.size).toBeGreaterThanOrEqual(8);
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  },
  120000,
);
