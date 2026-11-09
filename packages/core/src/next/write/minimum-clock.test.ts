/** R23.5: one worker clock slices admission work across many small tables, including cancellation. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { taskClock } from "../exec/pipeline.js";
import { createReach, WRITE_MINIMUM_CLOCK_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Harness, autocommit, column, tableInput, INT } from "./test-support.js";

const reach = createReach("write minimum task clock", WRITE_MINIMUM_CLOCK_STATES);
for (const seed of seedsFor("next-write-minimum-clock", [0xb3c4]))
  it(`slices the whole publication minimum and settles close (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({ tables: fc.integer({ min: 8, max: 16 }), cancel: fc.boolean() }),
        async (s) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory");
          const path = await h.open();
          try {
            await autocommit(path, async (st) => {
              for (let i = 0; i < s.tables; i++)
                await st.changeCatalog({
                  kind: "createTable",
                  table: tableInput(`t${String(i)}`, [column("id", INT, false)]),
                });
            });
            let checks = 0,
              taskRan = false;
            // Host speed cannot erase the yield state (same seam as staging-retirement.test.ts).
            const clock = vi.spyOn(taskClock, "due").mockImplementation(() => ++checks % 4 === 0);
            let tx: Awaited<ReturnType<typeof path.begin>> | null = null;
            let timer: ReturnType<typeof setImmediate> | undefined;
            try {
              tx = await path.begin();
              const st = tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              });
              timer = setImmediate(() => {
                taskRan = true;
                if (s.cancel) void path.close();
              });
              const pending = Promise.resolve(
                st.changeCatalog({
                  kind: "createTable",
                  table: tableInput("last", [column("id", INT, false)]),
                }),
              );
              if (s.cancel) {
                await expect(pending).rejects.toMatchObject({ code: "MINNOW_WRITE_PATH_CLOSED" });
                await path.close();
                reach.reach("close drains sliced state pin");
              } else {
                await pending;
                expect(taskRan).toBe(true);
                await st.end();
                await (
                  await tx.commit(null)
                ).durable;
                expect(path.catalog.tables).toHaveLength(s.tables + 1);
                reach.reach("task turn during cumulative small tables");
              }
              reach.reach("shared clock due repeatedly");
              expect(checks).toBeGreaterThanOrEqual(4);
            } finally {
              if (timer !== undefined) clearImmediate(timer);
              clock.mockRestore();
              tx?.rollback();
            }
          } finally {
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("all state credit returned");
        },
      ),
      { seed, numRuns: 16 },
    );
  }, 120000);
it("reaches admission task-clock states", () => reach.assertReached());
