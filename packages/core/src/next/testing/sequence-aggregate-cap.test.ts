/** Native serialized output: exact complete delivery admission, never input-sized progress. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SEQUENCE_CAP_REACH } from "../../testing/reach.js";
import { QueryRuntime } from "../exec/run.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
// eslint-disable-next-line @typescript-eslint/unbound-method -- supplied receiver
const restore = QueryRuntime.prototype.restorePhase;
for (const seed of seedsFor("sequence-cap", [47055, 47056]))
  it(`funds exact serialized array/string boundaries seed${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("sequence cap", SEQUENCE_CAP_REACH);
    try {
      await pg.waitReady;
      await pg.exec("CREATE TABLE cap_values(n INTEGER PRIMARY KEY,v TEXT)");
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(16, 20),
          fc.boolean(),
          fc.boolean(),
          async (mib, array, nulls) => {
            reach.beginHistory();
            const cap = mib << 20,
              n = 1024,
              core = await new StorageCore(16).open();
            let governor = new Governor(64 << 20);
            const measuring = governor;
            let exactMinimum = 0;
            const measure = vi
              .spyOn(QueryRuntime.prototype, "restorePhase")
              .mockImplementation(function (this: QueryRuntime, ...args) {
                if (this.request.governor === measuring) exactMinimum = this.minimumBytes(args[0]);
                return restore.apply(this, args);
              });
            const original = core.executor.prepare.bind(core.executor),
              spy = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
                const p = original(plan);
                return {
                  ...p,
                  execute: (r) => p.execute({ ...r, governor, signal: AbortSignal.timeout(30000) }),
                };
              });
            try {
              const isNull = (i: number) => array && nulls && i % 3 === 1;
              const nonnull = Array.from({ length: n }, (_, i) => i).filter(
                (i) => !isNull(i),
              ).length;
              const fixed = array ? 12 + (nulls ? Math.ceil(n / 8) : 0) + 4 * nonnull : n - 1;
              const width = Math.floor((cap / 2 - fixed) / nonnull),
                extra = cap / 2 - fixed - width * nonnull;
              const rows = Array.from({ length: n }, (_, i) => ({
                n: i,
                v: isNull(i) ? null : "x".repeat(width + (i === n - 1 ? extra : 0)),
              }));
              const sql = `SELECT ${array ? "array_agg(v ORDER BY n)" : "string_agg(v,'-' ORDER BY n)"} a FROM cap_values`;
              await core.execute("CREATE TABLE cap_values(n INTEGER PRIMARY KEY,v TEXT)");
              await core.writeBatch("insertBatch", "cap_values", rows);
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              await pg.exec("TRUNCATE cap_values");
              // One parameterized insert per equal-width class, rather than a huge SQL literal.
              await pg.query(
                "INSERT INTO cap_values SELECT n,CASE WHEN $3::boolean AND n%3=1 THEN NULL ELSE $1::text END FROM generate_series(0,$2::int-1)t(n)",
                ["x".repeat(width), n, array && nulls],
              );
              await pg.query("UPDATE cap_values SET v=v||$1::text WHERE n=$2", [
                "x".repeat(extra),
                n - 1,
              ]);
              const expected = (await pg.query(sql)).rows,
                result = await core.execute(sql);
              if (!("batches" in result)) throw new Error("Rows required");
              expect(oracleRows(result)).toEqual(expected);
              const col = result.batches[0]?.columns[0];
              if (col?.form !== "flat" || col.data.layout !== "varlen")
                throw new Error("Expected serialized output");
              expect(col.data.bytes.length).toBe(cap / 2);
              // The native output is adopted: its real complete delivery minimum, not an
              // automatic 2x value rule, defines the cap. No producer or input pins remain.
              expect(exactMinimum).toBeGreaterThan(cap / 2);
              expect(exactMinimum).toBeLessThan(cap);
              governor = new Governor(exactMinimum);
              const fitting = await core.execute(sql);
              if (!("batches" in fitting)) throw new Error("Rows required");
              expect(oracleRows(fitting)).toEqual(expected);
              reach.reach("exact boundary");
              await core.execute("CREATE TABLE over_values(n INTEGER PRIMARY KEY,v TEXT)");
              await core.writeBatch(
                "insertBatch",
                "over_values",
                rows.map((r) => ({ ...r, v: r.n === n - 1 ? (r.v ?? "") + "y" : r.v })),
              );
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
              await expect(
                core.execute(sql.replace("cap_values", "over_values")),
              ).rejects.toMatchObject({
                sqlState: "53200",
                cause: { requestedBytes: exactMinimum + 1, admittedBytes: exactMinimum },
              });
              reach.reach("next byte refuses");
              reach.reach(array ? "array" : "string");
              if (array && nulls) reach.reach("NULL bitmap");
              expect(governor.peakBytes).toBeLessThanOrEqual(exactMinimum);
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero");
            } finally {
              spy.mockRestore();
              measure.mockRestore();
              await core.close();
            }
          },
        ),
        {
          seed,
          numRuns: 4,
          endOnFailure: true,
          examples: [
            [16, false, false],
            [16, true, true],
            [16, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
