/** A tiny scalar JSON aggregate must not inherit a multi-MiB fixed workspace. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, JSON_AGGREGATE_SMALL_CAP_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-aggregate-small-cap", [0xb72c, 0xb72d]))(
  "funds tiny aggregates under small complete caps (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON small aggregate admission",
      JSON_AGGREGATE_SMALL_CAP_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4, max: 6 }),
        fc.integer({ min: 0, max: 4 }),
        fc.integer({ min: -99, max: 99 }),
        fc.boolean(),
        async (mib, mode, value, empty) => {
          reach.beginHistory();
          // JSONB object key deduplication has an independent shared-sort recipe.
          // Retain that family at 8..10 MiB; the other three anchor the reviewed
          // 4 MiB fixed-workspace regression. This does not prove a smaller sort minimum.
          const core = await new StorageCore().open(),
            governor = new Governor((mode === 4 ? mib + 8 : mode === 3 ? mib + 4 : mib) << 20);
          const fn =
            ["json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg"][mode] ?? "json_agg";
          const sql =
            mode === 4
              ? `SELECT json_agg(n ORDER BY n)::text j FROM(VALUES(1),(2))t(n)${empty ? " WHERE false" : ""} GROUP BY '1'::jsonb`
              : `SELECT ${fn}(${mode >= 2 ? "'k'," : ""}${String(value)})::text j${empty ? " FROM(VALUES(1))t(n) WHERE false" : ""}`;
          try {
            const expected = (await pg.query<{ j: string | null }>(sql)).rows;
            const p = core.prepare(sql),
              run = core.start({ ...p, execute: (r) => p.execute({ ...r, governor }) });
            const rows: Array<{ j: string | null }> = [];
            try {
              for (;;) {
                const frame = await run.execution.results.next();
                if (!frame) break;
                try {
                  const v = frame.payload.columns[0];
                  if (v?.form !== "flat" || v.data.layout !== "varlen")
                    throw new Error("Expected flat JSON text");
                  const d = v.data;
                  for (let i = 0; i < frame.payload.rowCount; i++) {
                    const valid =
                      v.validity === null || (((v.validity[i >>> 5] ?? 0) >>> (i & 31)) & 1) !== 0;
                    rows.push({
                      j: valid
                        ? new TextDecoder().decode(
                            d.bytes.subarray(d.offsets[i] ?? 0, d.offsets[i + 1] ?? 0),
                          )
                        : null,
                    });
                  }
                } finally {
                  frame.release();
                }
              }
              await run.execution.executed;
              await run.execution.delivered;
              expect(rows, sql).toEqual(expected);
            } catch (error) {
              run.execution.cancel(error);
              await Promise.allSettled([run.execution.executed, run.execution.delivered]);
              throw error;
            } finally {
              run.release();
            }
            reach.reach(fn);
            if (mode === 4) reach.reach("tiny JSONB group key");
            reach.reach(governor.capBytes === 4 << 20 ? "four MiB cap" : "larger small cap");
            reach.reach(empty ? "empty aggregate" : "nonempty aggregate");
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero resources");
          } finally {
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 17,
        examples: [
          [4, 0, 1, false],
          [4, 1, -1, true],
          [4, 2, 0, false],
          [4, 3, 9, false],
          [5, 0, 1, true],
          [4, 4, 0, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
