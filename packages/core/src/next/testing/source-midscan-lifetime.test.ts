/** Owner charge survives publication and write-path close after the scan starts. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SOURCE_MIDSCAN_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { QueryCancelledError } from "../exec/errors.js";
import { SnapshotReleasedError } from "../access/snapshot.js";
import type { Execution } from "../exec/executor.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { Memtable } from "../write/memtable.js";
import type { ColumnChunk } from "../write/columns.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const modes = [
  "publish",
  "cancel",
  "snapshot-close",
  "writer-close",
  "writer-close-cancel",
  "compact",
] as const;
it.each(seedsFor("next-source-midscan-lifetime", [0xb748, 0xb749]))(
  "keeps midscan source ownership (seed %i)",
  async (seed) => {
    const reach = createReach("midscan source lifetime", SOURCE_MIDSCAN_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 512, max: 4096 }),
        fc.constantFrom(...modes),
        fc.integer({ min: 0, max: 1 }),
        fc.boolean(),
        async (width, mode, pauseColumn, twoPins) => {
          reach.beginHistory();
          const core = await new StorageCore(4).open(),
            governor = new Governor(16 << 20);
          let execution: Execution | undefined;
          const rows = [
            { n: 1, p: "a".repeat(width) },
            { n: 2, p: "b".repeat(width) },
          ];
          let pinned: ReturnType<typeof core.harness.path.pin> | undefined;
          let otherPin: typeof pinned;
          let transitioned = false;
          const closesWriter = mode.startsWith("writer-close");
          let seen = false,
            reads = 0;
          let retained: readonly ColumnChunk[] = [];
          const charge = () => retained.reduce((n, c) => n + c.chargedBytes, 0);
          try {
            await core.createTable({
              name: "midscan_t",
              uniqueKey: "n",
              columns: [
                { name: "n", type: "number" },
                { name: "p", type: "string" },
              ],
            });
            await core.writeBatch("insertBatch", "midscan_t", rows);
            pinned = core.harness.path.pin();
            if (twoPins) otherPin = core.harness.path.pin();
            const table = pinned.table.bind(pinned);
            pinned.table = (id, options) => {
              const got = table(id, options);
              if (got instanceof Promise) throw new Error("Expected native synchronous table");
              for (const segment of got.segments)
                if (segment.kind === "memory") {
                  const chunk = segment.chunk.bind(segment);
                  segment.chunk = async (...args) => {
                    const result = await chunk(...args);
                    if (!seen && reads++ === pauseColumn) {
                      seen = true;
                      const memtable = (segment as unknown as { memtable: Memtable }).memtable;
                      retained = memtable.chunks.map((c) => {
                        const first = c[0];
                        if (!first) throw new Error("Missing retained column");
                        return first;
                      });
                      expect(charge()).toBeGreaterThan(0);
                      reach.reach(pauseColumn === 0 ? "after first column" : "after second column");
                      await core.writeBatch("insertBatch", "midscan_t", [
                        { n: 3, p: "c" },
                        { n: 4, p: "d" },
                      ]);
                      await core.writeBatch("insertBatch", "midscan_t", [{ n: 5, p: "e" }]);
                      await core.harness.path.quiesce();
                      expect(core.harness.path.stats().borrowedChunkBytes).toBeGreaterThan(0);
                      expect(charge()).toBeGreaterThan(0);
                      reach.reach("publication during scan");
                      if (mode === "compact") {
                        const before = core.harness.path.maintenance.merges;
                        for (let batch = 0; batch < 12; batch++) {
                          await core.writeBatch(
                            "insertBatch",
                            "midscan_t",
                            Array.from({ length: 4 }, (_, i) => ({
                              n: 6 + batch * 4 + i,
                              p: "later",
                            })),
                          );
                          await core.harness.path.quiesce();
                          expect(charge()).toBeGreaterThan(0);
                        }
                        expect(core.harness.path.maintenance.merges).toBeGreaterThan(before);
                        reach.reach("observed compaction");
                      }
                      if (closesWriter) {
                        await core.harness.path.close();
                        expect(pinned?.released).toBe(false);
                        expect(charge()).toBeGreaterThan(0);
                        const owner = pinned?.shared.budget as Grant;
                        expect(owner.allocated).toBeGreaterThanOrEqual(charge());
                        expect(owner.held).toBe(owner.allocated + owner.loaned);
                        expect(core.harness.governor.usedBytes).toBeGreaterThanOrEqual(charge());
                        reach.reach("writer close retains charged source");
                      }
                      if (mode === "snapshot-close") {
                        pinned?.release();
                        reach.reach("snapshot release during scan");
                      }
                      if (mode === "cancel" || mode === "writer-close-cancel") {
                        execution?.cancel(new Error("generated midscan cancellation"));
                        reach.reach("cancellation during scan");
                      }
                    }
                    transitioned ||= seen;
                    return result;
                  };
                }
              return got;
            };
            const prepare = core.executor.prepare.bind(core.executor);
            core.executor.prepare = (plan) => {
              const prepared = prepare(plan);
              return {
                ...prepared,
                execute: (request) => {
                  if (!pinned) throw new Error("Missing pinned snapshot");
                  execution = prepared.execute({
                    ...request,
                    snapshot: pinned,
                    statement: null,
                    governor,
                  });
                  return execution;
                },
              };
            };
            const outcome = await core.execute("SELECT n,p FROM midscan_t ORDER BY n").then(
              (value) => ({ value }),
              (error: unknown) => ({ error }),
            );
            expect(seen).toBe(true);
            if (mode === "cancel" || mode === "writer-close-cancel" || mode === "snapshot-close") {
              if (!("error" in outcome)) throw new Error("Expected midscan cancellation/release");
              expect(outcome.error).toBeInstanceOf(
                mode === "snapshot-close" ? SnapshotReleasedError : QueryCancelledError,
              );
            } else {
              if (!("value" in outcome)) throw outcome.error;
              const expected = (
                await pg.query(
                  "SELECT n,p FROM (VALUES(1::float8,$1::text),(2::float8,$2::text))q(n,p) ORDER BY n",
                  [rows[0]?.p, rows[1]?.p],
                )
              ).rows;
              if (!("batches" in outcome.value)) throw new Error("Expected row result");
              expect(oracleRows(outcome.value)).toEqual(expected);
              reach.reach("exact PostgreSQL rows");
            }
            // Preserve hook errors/incorrect cancellation types before checking completion.
            expect(transitioned).toBe(true);
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            // Native execution has drained and closed its source recipe. The returned
            // borrowed chunk is no longer a reader buffer; the still-live pin owns the
            // remaining row account/pool until its final release.
            const owner = pinned.shared.budget as Grant;
            if (closesWriter) {
              expect(charge()).toBe(0);
              expect(owner.held).toBeGreaterThan(0);
              reach.reach("source recipe returns borrow after execution");
            }
            pinned.release();
            pinned.release();
            if (closesWriter && otherPin) {
              expect(owner.held).toBeGreaterThan(0);
              reach.reach("two reader release fence");
            }
            otherPin?.release();
            otherPin?.release();
            if (closesWriter) {
              expect(charge()).toBe(0);
              expect(owner.held).toBe(0);
              expect(owner.allocated).toBe(0);
              reach.reach("last pin returns retained charge");
            }
            expect(core.harness.path.stats().borrowedChunkBytes).toBe(0);
            reach.reach("zero query resources");
          } finally {
            execution?.cancel(new Error("midscan test cleanup"));
            if (execution) await Promise.allSettled([execution.executed, execution.delivered]);
            pinned?.release();
            otherPin?.release();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 10,
        endOnFailure: true,
        examples: modes.map((mode, i) => [512 + i * 256, mode, i % 2, i % 3 === 0]),
      },
    );
    reach.assertReached();
  },
  120000,
);
