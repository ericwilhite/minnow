/** snapshot.ts: a charged recipe retains existing chunks, never changes locators or visibility. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MIXED_SOURCE_RECIPE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import type { Snapshot } from "../access/snapshot.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-mixed-source-recipe", [0xb5bb, 0xb5bc]))(
  "charges multi-segment retention through publication/cancel (seed %i)",
  async (seed) => {
    const reach = createReach("mixed source recipes", MIXED_SOURCE_RECIPE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.integer({ min: 32, max: 1024 }),
        fc.boolean(),
        fc.boolean(),
        async (n, width, publish, cancel) => {
          reach.beginHistory();
          const core = await new StorageCore(2).open(),
            governor = new Governor(16 << 20);
          const credit = await governor.admit({
            label: "source recipe metadata",
            minimumBytes: 65536,
            spillable: false,
          });
          try {
            await core.execute("CREATE TABLE mixed (id INTEGER PRIMARY KEY,note TEXT)");
            const expected = Array.from({ length: 2 * n + 1 }, (_, id) => ({
              id,
              note: `${String(id)}-${"x".repeat(width)}`,
            }));
            await core.writeBatch("insertBatch", "mixed", expected);
            const pinned = core.harness.path.pin();
            try {
              const id = pinned.catalog.tableByName("mixed")?.id;
              if (id === undefined) throw new Error("table");
              const owner: Snapshot = pinned;
              const table = await owner.table(id, { budget: credit });
              if (!table.prepareRead) throw new Error("Missing native recipe support");
              expect(table.segments.length).toBeGreaterThan(1);
              reach.reach("multiple source segments");
              const before = core.harness.path.stats().borrowedChunkBytes;
              expect(
                table.prepareRead({
                  budget: {
                    allocate: () => false,
                    free: () => {
                      throw new Error("denied budget was freed");
                    },
                  },
                }),
              ).toBeNull();
              expect(core.harness.path.stats().borrowedChunkBytes).toBe(before);
              reach.reach("metadata denied no retain");
              const recipe = table.prepareRead({ budget: credit });
              if (!recipe) throw new Error("Mixed source recipe was refused");
              expect(credit.allocated).toBeGreaterThan(0);
              reach.reach("metadata paid");
              try {
                expect(recipe.table.segments.map((segment) => segment.index)).toEqual(
                  table.segments.map((_, index) => index),
                );
                reach.reach("locators preserved");
                if (publish) {
                  await core.writeBatch("insertBatch", "mixed", [
                    { id: 100, note: null },
                    { id: 101, note: null },
                    { id: 102, note: null },
                  ]);
                  await core.harness.path.quiesce();
                  expect(core.harness.path.stats().borrowedChunkBytes).toBeGreaterThan(0);
                  reach.reach("publication retained");
                }
                const readView: Snapshot = {
                  rootSequence: pinned.rootSequence,
                  released: false,
                  bound: pinned.bound,
                  observedLsn: pinned.observedLsn,
                  catalog: pinned.catalog,
                  pages: pinned.pages,
                  table: () => recipe.table,
                  index: (table, index) => pinned.index(table, index),
                  release: () => undefined,
                };
                const prepare = core.executor.prepare.bind(core.executor);
                const controller = new AbortController();
                if (cancel) controller.abort("cancel retained source");
                core.executor.prepare = (plan) => {
                  const prepared = prepare(plan);
                  return {
                    ...prepared,
                    execute: (request) =>
                      prepared.execute({
                        ...request,
                        snapshot: readView,
                        statement: null,
                        governor,
                        signal: controller.signal,
                      }),
                  };
                };
                if (cancel) {
                  await expect(
                    core.execute("SELECT id,note FROM mixed ORDER BY id+0"),
                  ).rejects.toMatchObject({ sqlState: "57014" });
                  reach.reach("cancelled");
                } else {
                  const actual = await core.execute("SELECT id,note FROM mixed ORDER BY id+0");
                  if (actual.kind !== "rows" || !("batches" in actual)) throw new Error("rows");
                  expect(oracleRows(actual)).toEqual(expected);
                  reach.reach("exact original rows");
                }
              } finally {
                recipe.close();
                recipe.close();
              }
              expect(credit.allocated).toBe(0);
              expect(core.harness.path.stats().borrowedChunkBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
            } finally {
              pinned.release();
            }
          } finally {
            credit.close();
            await core.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("all credit returned");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [2, 128, true, false],
          [3, 512, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
