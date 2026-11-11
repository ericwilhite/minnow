/** A resident percentile owns its complete workspace only after the FIFO minimum succeeds. */
import fc from "fast-check";
import { PGlite } from "@electric-sql/pglite";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { Execution } from "../exec/executor.js";
import { PercentileOperator } from "../exec/agg/percentile.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-percentile-admission", [0xe157, 0xe158]))(
  "waits without private state and cancels cleanly (seed %i)",
  async (seed) => {
    const core = await new StorageCore(8192).open(),
      pg = new PGlite();
    const reach = createReach("percentile admission", [
      "whole minimum waits",
      "no premature state",
      "holder releases",
      "cancelled wait",
      "PG exact",
      "zero memory",
      "zero spill",
    ]);
    const push = vi.spyOn(PercentileOperator.prototype, "push");
    try {
      const ddl = "CREATE TABLE t(id integer PRIMARY KEY,g text,x double precision)";
      await core.execute(ddl);
      await pg.exec(ddl);
      const rows = Array.from({ length: 128 }, (_, id) => ({
        id,
        g: `g${String(id % 5)}`,
        x: id % 11 === 0 ? null : ((id * 73) % 101) / 8,
      }));
      await core.writeBatch("insertBatch", "t", rows);
      await pg.query(
        "INSERT INTO t VALUES " +
          rows
            .map((_, i) => `($${String(i * 3 + 1)},$${String(i * 3 + 2)},$${String(i * 3 + 3)})`)
            .join(","),
        rows.flatMap((row) => [row.id, row.g, row.x]),
      );
      await core.harness.path.recycleLog();
      await core.harness.path.quiesce();
      const sql =
        "SELECT g,percentile_cont(.5) WITHIN GROUP(ORDER BY x) p FROM t GROUP BY g ORDER BY g";
      const expected = (await pg.query(sql)).rows;
      await fc.assert(
        fc.asyncProperty(fc.boolean(), fc.integer({ min: 64, max: 96 }), async (cancel, cap) => {
          reach.beginHistory();
          push.mockClear();
          const governor = new Governor(cap << 20),
            nativeTry = governor.tryAdmit.bind(governor),
            nativeGroup = governor.admitGroup.bind(governor);
          let holder: Grant | null = null,
            holderReady: Promise<void> | null = null,
            execution: Execution | undefined,
            entered: (() => void) | undefined;
          const waiting = new Promise<void>((resolve) => {
            entered = resolve;
          });
          const prepare = core.executor.prepare.bind(core.executor);
          core.executor.prepare = (plan) => {
            const prepared = prepare(plan);
            return {
              ...prepared,
              execute: (request) => {
                execution = prepared.execute({ ...request, governor });
                return execution;
              },
            };
          };
          governor.tryAdmit = (options) => {
            if (options.label.endsWith("/fixed-buffers") && holderReady === null) {
              expect(governor.usedBytes).toBe(0);
              // A competing execution's COMPLETE minimum may use the shared reserve.
              // Admit it through the same public atomic primitive, leaving only 4 KiB;
              // no assumption about this query's minimum versus reserveBytes is needed.
              holderReady = nativeGroup({
                label: "temporarily pinned competitor",
                includesProgress: true,
                grants: [
                  {
                    label: "complete minimum",
                    minimumBytes: governor.capBytes - 4096,
                    spillable: false,
                  },
                ],
              }).then((grants) => {
                holder = grants[0] ?? null;
                expect(holder).not.toBeNull();
              });
              return null;
            }
            return nativeTry(options);
          };
          governor.admitGroup = (options) => {
            const result = nativeGroup(options);
            if (options.label.endsWith("/fixed-buffers"))
              void (holderReady ?? Promise.resolve()).then(() => entered?.());
            return result;
          };
          const work = core.execute(sql).then(
            (value) => ({ value, error: undefined }),
            (error: unknown) => ({ value: undefined, error }),
          );
          let timer: ReturnType<typeof setTimeout> | undefined;
          const deadline = new Promise<never>((_resolve, reject) => {
            timer = setTimeout(() => reject(new Error("fixed admission was not reached")), 10000);
          });
          try {
            await Promise.race([waiting, deadline]);
            expect(holder).not.toBeNull();
            expect(execution).toBeDefined();
            expect(governor.usedBytes).toBe((holder as Grant | null)?.held);
            expect(push).not.toHaveBeenCalled();
            reach.reach("whole minimum waits");
            reach.reach("no premature state");
            const reason = new Error("cancel percentile minimum");
            if (cancel) {
              execution?.cancel(reason);
              reach.reach("cancelled wait");
            } else {
              (holder as Grant | null)?.close();
              reach.reach("holder releases");
            }
            const result = await work;
            if (cancel)
              expect(result.error).toMatchObject({
                code: "MINNOW_QUERY_CANCELLED",
                sqlState: "57014",
                cause: reason,
              });
            else {
              if (!result.value || !("batches" in result.value))
                throw new Error("Missing percentile rows");
              expect(oracleRows(result.value)).toEqual(expected);
              expect(push).toHaveBeenCalled();
              reach.reach("PG exact");
            }
          } finally {
            clearTimeout(timer);
            execution?.cancel(new Error("test cleanup"));
            await Promise.resolve(holderReady);
            (holder as Grant | null)?.close();
            await work;
            core.executor.prepare = prepare;
          }
          expect(governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("zero memory");
          reach.reach("zero spill");
        }),
        {
          seed,
          numRuns: 4,
          examples: [
            [false, 64],
            [true, 64],
          ],
        },
      );
      reach.assertReached();
    } finally {
      push.mockRestore();
      await core.close();
      await pg.close();
    }
  },
  60000,
);
