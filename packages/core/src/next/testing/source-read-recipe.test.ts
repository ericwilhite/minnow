/** A retained source keeps visibility/ownership through publication and phase handoffs. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SOURCE_READ_RECIPE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { LazyGroupRoot, type Memtable } from "../write/memtable.js";
import { ChunkReader } from "../page/reader.js";
import type { Execution } from "../exec/executor.js";
import type { Snapshot } from "../access/snapshot.js";
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
it.each(seedsFor("next-source-read-recipe", [0xb744, 0xb745]))(
  "keeps native source leases across publication/close (seed %i)",
  async (seed) => {
    const reach = createReach("source read recipes", SOURCE_READ_RECIPE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 512, max: 4096 }),
        fc.constantFrom("publish", "cancel", "phases"),
        async (width, mode) => {
          reach.beginHistory();
          const core = await new StorageCore(4).open(),
            governor = new Governor(16 << 20);
          let execution: Execution | undefined, pinned: Snapshot | undefined;
          let competing: Grant | undefined;
          let notify: () => void = () => undefined;
          const entered = new Promise<void>((resolve) => {
            notify = resolve;
          });
          let captured = false,
            sourceOpens = 0;
          const io: number[] = [];
          const rows = [
            { n: 1, p: "A" + "x".repeat(width) },
            { n: 2, p: "B" + "x".repeat(width) },
          ];
          try {
            await core.createTable({
              name: "source_t",
              columns: [
                { name: "n", type: "number" },
                { name: "p", type: "string", nullable: true },
              ],
            });
            await core.writeBatch("insertBatch", "source_t", rows);
            // The owner pin predates every appended row and stays alive across TWO executions.
            pinned = core.harness.path.pin();
            const prepare = core.executor.prepare.bind(core.executor);
            core.executor.prepare = (plan) => {
              const prepared = prepare(plan);
              return {
                ...prepared,
                execute: (request) => {
                  const snapshot = pinned;
                  if (!snapshot) throw new Error("Missing owner snapshot");
                  execution = prepared.execute({
                    ...request,
                    snapshot,
                    statement: null,
                    governor,
                    reads: {
                      open: (pages, chunk, options) => {
                        sourceOpens++;
                        return ChunkReader.open(pages, chunk, options);
                      },
                    },
                  });
                  return execution;
                },
              };
            };
            if (mode === "cancel") {
              const credit = governor.capBytes - governor.reserveBytes - (1 << 20);
              competing = await governor.admit({
                label: "competing execution",
                minimumBytes: credit,
                spillable: false,
              });
              expect(competing.allocate(credit)).toBe(true);
            }
            const immediate = governor.tryAdmit.bind(governor);
            governor.tryAdmit = (options) =>
              options.label.endsWith("/fixed-buffers") ? null : immediate(options);
            const admit = governor.admitGroup.bind(governor);
            governor.admitGroup = async (options) => {
              if (options.label.endsWith("/fixed-buffers")) {
                io.push(
                  options.grants.find((g) => g.label.endsWith("/read-step"))?.minimumBytes ?? 0,
                );
                if (!captured) {
                  captured = true;
                  // Appending fills/publishes the old chunk. Rows3/4 are invisible to pinned.
                  await core.writeBatch("insertBatch", "source_t", [
                    { n: 3, p: null },
                    { n: 4, p: null },
                  ]);
                  await core.writeBatch("insertBatch", "source_t", [{ n: 5, p: null }]);
                  await core.harness.path.quiesce();
                  expect(core.harness.path.stats().borrowedChunkBytes).toBeGreaterThan(0);
                  reach.reach("owner-charged publication retention");
                  notify();
                }
              }
              return admit(options);
            };
            const sql =
              mode === "phases"
                ? "SELECT jsonb_build_object('p',p)::text j FROM source_t EXCEPT SELECT jsonb_build_object('q',p)::text j FROM source_t ORDER BY j"
                : "SELECT n,p FROM source_t WHERE p IS NOT NULL ORDER BY n";
            const authority =
              "WITH source_t(n,p) AS (VALUES($1::float8,$2::text),($3::float8,$4::text)) " + sql;
            const expected = (await pg.query(authority, [1, rows[0]?.p, 2, rows[1]?.p])).rows;
            const outcome = core.execute(sql).then(
              (value) => ({ value }),
              (error: unknown) => ({ error }),
            );
            if (mode === "cancel") {
              await Promise.race([
                entered,
                outcome.then(() => {
                  throw new Error("Execution settled before the injected admission wait");
                }),
              ]);
              if (!execution) throw new Error("Missing pending execution");
              expect(governor.usedBytes).toBe(competing?.held);
              reach.reach("competing credit without partial execution holdings");
              execution.cancel(new Error("cancel queued source admission"));
              expect(await outcome).toHaveProperty("error");
              reach.reach("cancelled admission");
            } else {
              const result = await outcome;
              if ("error" in result) throw result.error;
              if (!("batches" in result.value)) throw new Error("Expected query rows");
              expect(oracleRows(result.value)).toEqual(expected);
              reach.reach("exact PostgreSQL rows");
              if (mode === "publish") {
                expect(sourceOpens).toBe(0);
                expect(io[0]).toBe(0);
                expect(core.harness.path.stats().borrowedChunkBytes).toBe(0);
                // Same old snapshot, now stored: validate real roots/pages and retain ONLY2rows.
                const reader = await governor.admit({
                  label: "explicit complete root",
                  minimumBytes: 65536,
                  spillable: false,
                });
                const id = core.harness.path.catalog.tableByName("source_t")?.id;
                if (id === undefined) throw new Error("Missing source owner");
                const table = await pinned.table(id, { budget: reader });
                const memory = table.segments[0];
                if (memory?.kind !== "memory")
                  throw new Error("Expected re-pointed memory segment");
                const memtable = (memory as unknown as { memtable: Memtable }).memtable;
                const published = memtable.published;
                if (!published) throw new Error("Missing published source");
                memtable.published = {
                  ...published,
                  groupRoot: new LazyGroupRoot(() => {
                    throw new Error("Source used writer-owned root callback");
                  }),
                };
                const recipe = table.prepareRead?.();
                try {
                  const group = recipe?.table.segments[0];
                  if (group?.kind !== "group") throw new Error("Expected stored read recipe");
                  const root = await group.root();
                  expect(root.rowCount).toBe(4);
                  expect(root.columns).toHaveLength(2);
                  expect(reader.allocated).toBeGreaterThan(0);
                  recipe?.close();
                  expect(reader.allocated).toBe(0);
                  reach.reach("caller-funded complete roots");
                  const stored = await core.execute(sql);
                  if (!("batches" in stored)) throw new Error("Expected stored query rows");
                  expect(oracleRows(stored)).toEqual(expected);
                } finally {
                  recipe?.close();
                  reader.close();
                  memtable.published = published;
                }
                expect(sourceOpens).toBeGreaterThan(0);
                expect(io[1]).toBeGreaterThan(0);
                expect(io[1]).toBeLessThan(1 << 20);
                reach.reach("actual published root recipe");
                reach.reach("invisible suffix excluded");
              } else reach.reach("producer handoff");
            }
            expect(core.harness.path.stats().borrowedChunkBytes).toBe(0);
            competing?.close();
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("zero resources before owner pin release");
          } finally {
            execution?.cancel(new Error("source recipe test cleanup"));
            if (execution) await Promise.allSettled([execution.executed, execution.delivered]);
            competing?.close();
            pinned?.release();
            pinned?.release();
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        endOnFailure: true,
        examples: [
          [512, "publish"],
          [1024, "cancel"],
          [2048, "phases"],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
