/** A streamed aggregate's single value fits exactly at cap/2, independent of input size. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_AGGREGATE_CAP_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TABLE cap_values(n INTEGER PRIMARY KEY,v TEXT)");
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-aggregate-cap", [0xb706, 0xb707]))(
  "accepts the exact two-copy boundary and refuses its next byte (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON aggregate exact boundary", JSON_AGGREGATE_CAP_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 16, max: 20 }), fc.boolean(), async (mib, binary) => {
        reach.beginHistory();
        const cap = mib << 20,
          n = 1024,
          governor = new Governor(cap),
          core = await new StorageCore(16).open();
        // [] plus N quoted strings and N-1 separators is exactly N*(length+4) bytes.
        // Small storage chunks keep independent source-read admission below this cap.
        const sql = `SELECT ${binary ? "jsonb_agg" : "json_agg"}(v ORDER BY n)::text j FROM cap_values`;
        const original = core.executor.prepare.bind(core.executor);
        const spy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
          const p = original(plan);
          return { ...p, execute: (r) => p.execute({ ...r, governor }) };
        });
        try {
          const value = "x".repeat(cap / (2 * n) - 4);
          await core.execute("CREATE TABLE cap_values(n INTEGER PRIMARY KEY,v TEXT)");
          await core.writeBatch(
            "insertBatch",
            "cap_values",
            Array.from({ length: n }, (_, i) => ({ n: i, v: value })),
          );
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
          await pg.exec("TRUNCATE cap_values");
          await pg.query(
            "INSERT INTO cap_values SELECT n,$1::text FROM generate_series(0,$2::int-1)t(n)",
            [value, n],
          );
          const expected = (await pg.query<{ j: string }>(sql)).rows;
          expect(expected[0]?.j.length).toBe(cap / 2);
          const actual = await core.execute(sql);
          if (!("batches" in actual)) throw new Error("Expected aggregate");
          expect(oracleRows(actual)).toEqual(expected);
          reach.reach("exact cap divided by two succeeds");
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          await core.execute("CREATE TABLE over_values(n INTEGER PRIMARY KEY,v TEXT)");
          await core.writeBatch(
            "insertBatch",
            "over_values",
            Array.from({ length: n }, (_, i) => ({ n: i, v: i === n - 1 ? value + "y" : value })),
          );
          await core.harness.path.recycleLog();
          await core.harness.path.quiesce();
          await expect(
            core.execute(sql.replace("cap_values", "over_values")),
          ).rejects.toMatchObject({
            sqlState: "53200",
            cause: { requestedBytes: cap + 2, admittedBytes: cap },
          });
          reach.reach("first byte above boundary refuses");
          reach.reach(binary ? "JSONB" : "JSON");
          expect(governor.peakBytes).toBeLessThanOrEqual(cap);
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("small inputs large aggregate");
          reach.reach("zero resources");
        } finally {
          spy.mockRestore();
          await core.close();
        }
      }),
      {
        seed,
        numRuns: 4,
        examples: [
          [16, false],
          [16, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
