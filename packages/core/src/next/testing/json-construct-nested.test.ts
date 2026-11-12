/** Large constructor dependencies retire after capture; projected siblings keep their pins. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_CONSTRUCT_NESTED_REACH } from "../../testing/reach.js";
import { JsonCampaign } from "./json-construct-history.test.fixture.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-construction-nested", [0xb6da, 0xb6db]))(
  "keeps nested/sibling/projection values exact without depth times value copies (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON construction dependencies",
      JSON_CONSTRUCT_NESTED_REACH,
    );
    const campaign = new JsonCampaign();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 8, max: 12 }),
          fc.integer({ min: 200000, max: 300000 }),
          fc.boolean(),
          fc.boolean(),
          async (depth, length, binary, sibling) => {
            reach.beginHistory();
            const history = await campaign.history(16 << 20),
              { core, governor } = history,
              fn = binary ? "jsonb_build_object" : "json_build_object";
            let inner = `${fn}('v',$1::text)`;
            for (let i = 1; i < depth; i++) inner = `${fn}('v',${inner})`;
            const sql = `SELECT ${inner}::text j${sibling ? `,${fn}('a',$1::text,'b',${fn}('v',$1::text))::text k` : ""}`;
            // Same seeds, runs, example tuples, depth and six-byte escaping; 1/16 payload.
            // Each dependency still crosses multiple 64 KiB fragments (75..112.5 kB),
            // including live projected sibling pins, without rescanning MiBs per depth.
            const value = "\u0001".repeat(Math.floor(length / 16)),
              expected = (await pg.query(sql, [value])).rows;
            expect(value.length * 6).toBeGreaterThan(65536);
            reach.reach("multi-fragment control expansion");
            history.guard();
            const prepare = core.executor.prepare.bind(core.executor);
            history.observe(
              vi
                .spyOn(core.executor, "prepare")
                .mockImplementation((plan) => history.wrap(prepare(plan))),
            );
            try {
              const actual = await core.execute(sql, [value]);
              if (!("batches" in actual)) throw new Error("Expected rows");
              expect(oracleRows(actual)).toEqual(expected);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              reach.reach(binary ? "JSONB" : "JSON");
              reach.reach("deep dependencies");
              if (sibling) reach.reach("projected sibling pins");
              reach.reach("exact PostgreSQL");
            } finally {
              await history.close();
            }
            expect(governor.usedBytes).toBe(0);
            reach.reach("zero resources");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [10, 200000, false, false],
            [10, 200000, true, false],
            [8, 200000, false, true],
            [8, 200000, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await campaign.close();
    }
  },
  120000,
);
