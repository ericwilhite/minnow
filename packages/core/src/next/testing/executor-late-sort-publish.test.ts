/** Snapshot/locator authority: access/snapshot.ts R16.3 and vector/locator.ts lifetime. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import type { Snapshot } from "../access/snapshot.js";

it.each(seedsFor("next-executor-late-sort-publish", [0xd109, 0xd10a]))(
  "fetches the pinned winners after their memtable publishes (seed %i)",
  async (seed) => {
    const reach = createReach("late sort publication", [
      "published",
      "repointed",
      "two-chunks",
      "snapshot rows",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2305, max: 4096 }),
        fc.integer({ min: 8, max: 63 }),
        fc.integer({ min: 0, max: 7 }),
        fc.boolean(),
        async (count, limit, offset, descending) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open();
          const names = Array.from({ length: 24 }, (_, i) => `c${String(i)}`);
          const input: Array<Record<string, number>> = Array.from({ length: count }, (_, i) =>
            Object.fromEntries(
              names.map((n, j) => [n, j === 0 ? (i * 7919) % count : i * 1000 + j]),
            ),
          );
          let publications = 0;
          try {
            await core.execute(`CREATE TABLE t(${names.map((n) => `${n} INTEGER`).join(",")})`);
            await core.writeBatch("insertBatch", "t", input);
            const prepare = core.executor.prepare.bind(core.executor);
            core.executor.prepare = (plan) => {
              const prepared = prepare(plan);
              return {
                ...prepared,
                execute: (request) => {
                  const pin = request.snapshot;
                  if (!pin) throw new Error("query pin");
                  let tables = 0;
                  const snapshot: Snapshot = new Proxy(pin, {
                    get(target, name) {
                      if (name === "table")
                        return async (...args: Parameters<Snapshot["table"]>) => {
                          if (++tables === 2) {
                            // The narrow scan has finished; its locators must survive publication,
                            // including the old view's partial tail of this now larger memtable.
                            await core.writeBatch(
                              "insertBatch",
                              "t",
                              Array.from({ length: 17 }, (_, i) =>
                                Object.fromEntries(
                                  names.map((n, j) => [n, 1000000000 + i * 1000 + j]),
                                ),
                              ),
                            );
                            await core.harness.path.recycleLog();
                            await core.harness.path.quiesce();
                            publications++;
                            const repointed = core.harness.path.stats().repointedReaders > 0;
                            expect(repointed).toBe(true);
                          }
                          return target.table(...args);
                        };
                      const value: unknown = Reflect.get(target, name, target);
                      return typeof value === "function" ? (value.bind(target) as unknown) : value;
                    },
                  });
                  return prepared.execute({ ...request, snapshot, statement: null });
                },
              };
            };
            const expected = [...input]
              .sort(
                (a, b) =>
                  (descending ? -1 : 1) * ((a.c0 ?? 0) - (b.c0 ?? 0)) || (a.c1 ?? 0) - (b.c1 ?? 0),
              )
              .slice(offset, offset + limit);
            const result = await core.execute(
              `SELECT * FROM t ORDER BY c0 ${descending ? "DESC" : "ASC"},c1 LIMIT ${String(limit)} OFFSET ${String(offset)}`,
            );
            if (!("batches" in result)) throw new Error("query rows");
            expect(oracleRows(result)).toEqual(expected);
            expect(publications).toBe(1);
            expect(core.lastProfile?.pipelines).toBe(2);
            expect(core.harness.path.stats().repointPins).toBe(0);
            expect(core.harness.path.stats().borrowedChunkBytes).toBe(0);
            reach.reach("published");
            reach.reach("repointed");
            const indices = expected.map((row) => Math.floor((row.c1 ?? 0) / 1000));
            if (indices.some((i) => i < 2048) && indices.some((i) => i >= 2048))
              reach.reach("two-chunks");
            reach.reach("snapshot rows");
            expect(core.spill.bytesInUse).toBe(0);
          } finally {
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("released");
          }
        },
      ),
      { seed, numRuns: 4, examples: [[2305, 63, 3, true]] },
    );
    reach.assertReached();
  },
  120000,
);
