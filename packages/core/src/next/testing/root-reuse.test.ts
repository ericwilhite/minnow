/** Complete roots are parsed once; selected scan facts stay resident, with query-owned, evictable accounting. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, QUERY_ROOT_REUSE_REACH } from "../../testing/reach.js";
import * as codec from "../format/manifest-codec.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
import type { EvictableCacheLike } from "../storage/extents.js";
import { UNLIMITED } from "../page/memory-extents.js";

it.each(seedsFor("next-query-root-reuse", [0xb2e1, 0xb2e2]))(
  "reuses validated root facts across sizing/columns/batches and drains cancellation (seed %i)",
  async (seed) => {
    const reach = createReach("query-local root facts", QUERY_ROOT_REUSE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2049, max: 4097 }),
        fc.integer({ min: 2, max: 11 }),
        async (count, period) => {
          reach.beginHistory();
          const core = await new StorageCore(8192).open();
          const spy = vi.spyOn(codec, "decodeGroupRoot");
          try {
            await core.execute(
              "CREATE TABLE root_reuse (f DOUBLE PRECISION,i INTEGER,t TEXT,unused INTEGER)",
            );
            const rows = Array.from({ length: count }, (_, i) => ({
              f: i / 8,
              i: i - 100,
              t: i % period === 0 ? null : `v${String(i % 13)}`,
              unused: i % period === 0 ? null : i,
            }));
            await core.writeBatch("insertBatch", "root_reuse", rows);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const table = core.harness.path.catalog.tableByName("root_reuse");
            if (!table) throw new Error("missing fixture table");
            const pin = core.harness.path.pin();
            const stored = pin.table(table.id, { budget: UNLIMITED });
            const groups = stored.segments.filter((segment) => segment.kind === "group").length;
            pin.release();
            expect(groups).toBeGreaterThan(0);
            const registrations: Array<{ cache: EvictableCacheLike; closed: boolean }> = [];
            const register = core.harness.governor.registerCache.bind(core.harness.governor);
            const registry = vi.spyOn(core.harness.governor, "registerCache");
            registry.mockImplementation((cache) => {
              const memory = register(cache);
              const record = { cache, closed: false };
              registrations.push(record);
              return {
                ...memory,
                unregister: () => {
                  record.closed = true;
                  memory.unregister();
                },
              };
            });
            const sql = "SELECT f,i,t FROM root_reuse";
            for (let query = 0; query < 2; query++) {
              spy.mockClear();
              const result = await core.execute(sql);
              if (!("batches" in result)) throw new Error("missing query rows");
              expect(oracleRows(result)).toEqual(rows.map(({ f, i, t }) => ({ f, i, t })));
              expect(spy.mock.calls.length).toBe(groups);
              reach.reach("sizing and execution share validated root facts");
              reach.reach("multiple columns and batches");
            }
            reach.reach("fresh query owns its roots");
            const run = core.start(core.prepare(sql));
            const frame = await run.execution.results.next();
            expect(frame).not.toBeNull();
            frame?.release();
            run.execution.cancel(new Error("generated frame-boundary cancel"));
            await Promise.allSettled([run.execution.executed, run.execution.delivered]);
            run.release();
            reach.reach("cancel after output");
            expect(registrations.length).toBe(3);
            expect(registrations.every((entry) => entry.closed)).toBe(true);
            expect(registrations.every((entry) => entry.cache.evictableBytes === 0)).toBe(true);
            registry.mockRestore();
          } finally {
            spy.mockRestore();
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
            reach.reach("zero charges after close");
          }
        },
      ),
      { seed, numRuns: 4, endOnFailure: true },
    );
    reach.assertReached();
  },
);
