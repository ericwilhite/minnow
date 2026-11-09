/**
 * R25 (access/write-path.ts PublishDelta): released extents leave root reachability in the
 * same publication. A held index build may retain a dropped table under an older catalog;
 * that table must not keep staging whose rollback this publication retires.
 */
import fc from "fast-check";
import type { PublishDelta } from "../access/write-path.js";
import { need } from "./bytes.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, STAGING_DROP_RETIREMENT_REACH } from "../../testing/reach.js";
import { reachableExtents } from "../format/reachable.js";
import { expect, it } from "vitest";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  ints,
  batch,
  rules,
} from "./test-support.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";

const reach = createReach("next-write-staging-drop-retirement", STAGING_DROP_RETIREMENT_REACH);
for (const seed of seedsFor("next-write-staging-drop-retirement", [27931, 27932, 27933, 27934]))
  it(`a held build cannot resurrect rolled back staging (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 4, max: 10 }),
        fc.integer({ min: 3, max: 6 }),
        fc.boolean(),
        async (width, calls, sim) => {
          reach.beginHistory();
          reach.reach(sim ? "sim" : "memory");

          let armed = false;
          let entered!: () => void;
          let release!: () => void;
          const started = new Promise<void>((r) => {
            entered = r;
          });
          const gate = new Promise<void>((r) => {
            release = r;
          });
          const h = new Harness("strict", sim ? "sim" : "memory", width, undefined, {
            merges: { mode: "off" },
            beforeFlushPublish: async () => {
              if (armed) {
                armed = false;
                entered();
                await gate;
              }
            },
          });
          const listed = new Map<number, number>();
          let pending: PublishDelta | null = null;
          h.extentDeltas = {
            prepare: (_attempt, delta) => {
              pending = delta;
              return null;
            },
            commit: () => {
              if (pending === null) throw new Error("missing delta");
              for (const e of pending.released) {
                expect(listed.delete(e.offset)).toBe(true);
              }
              for (const e of pending.added) {
                expect(listed.has(e.offset)).toBe(false);
                listed.set(e.offset, e.length);
              }
              pending = null;
            },
            abort: () => {
              pending = null;
            },
          };
          const p = await h.open();
          try {
            const tables: number[] = [];
            for (const name of ["victim", "blocker", "witness"]) {
              await autocommit(p, async (s) => {
                tables.push(
                  (
                    await s.changeCatalog({
                      kind: "createTable",
                      table: tableInput(name, [column("id", INT, false)]),
                    })
                  ).id ?? 0,
                );
              });
            }
            await p.quiesce();
            const [victim, blocker, witness] = tables as [number, number, number];
            const stage = async (t: number) => {
              const tx = await p.begin();
              const s = tx.statement({ rules });
              for (let call = 0; call < calls; call++)
                await s
                  .table(t)
                  .insert(
                    batch(ints(Array.from({ length: width }, (_, k) => call * width + k))),
                    null,
                  );
              await s.end();
              return tx;
            };
            const tx = await stage(victim);
            expect(p.maintenance.stages).toBeGreaterThan(0);
            tx.rollback();
            reach.reach("rollback");
            // Build on a nonempty table holds the global checkpoint below the later drop.
            await autocommit(p, (s) => s.table(blocker).insert(batch(ints([100])), null));
            armed = true;
            const col = p.catalog.table(blocker)?.columns[0]?.id ?? 0;
            await autocommit(p, (s) =>
              s.changeCatalog({
                kind: "createIndex",
                index: {
                  tableId: blocker,
                  name: "held",
                  columnIds: [col],
                  unique: false,
                  role: "secondary",
                },
              }),
            );
            // Force the build's live memtable to flush.
            await autocommit(p, (s) =>
              s
                .table(blocker)
                .insert(batch(ints(Array.from({ length: width }, (_, k) => 101 + k))), null),
            );
            await started;
            reach.reach("held build");
            await autocommit(p, (s) => s.changeCatalog({ kind: "dropTable", tableId: victim }));
            const other = await stage(witness);
            const root = decodeManifestRoot(need(h.space.root), {
              allocate: () => true,
              free: () => undefined,
            }).value;
            const kept = root.tables.find((t) => t.tableId === victim);
            expect(kept, "the checkpoint still predates DROP TABLE").toBeDefined();
            expect(kept?.staged).toEqual([]);
            reach.reach("dropped table retained");
            const walked = new Map<number, number>();
            for await (const r of reachableExtents()(need(h.space.root), h.space, {
              allocate: () => true,
              free: () => undefined,
            })) {
              const e = "extent" in r ? r.extent : r;
              if (e.length > 0) walked.set(e.offset, e.length);
            }
            expect(listed).toEqual(walked);
            reach.reach("exact retained root");
            other.rollback();
            release();
            await p.quiesce();
            expect(p.failure).toBeNull();
            await h.space.audit();
            const reopened = await h.reopen();
            expect(reopened.catalog.table(victim)).toBeUndefined();
            expect(reopened.statistics(blocker)?.rowCount).toBe(width + 1);
            reach.reach("reopen");
          } finally {
            release();
            await h.close();
          }
        },
      ),
      { seed, numRuns: 8 },
    );
  });
it("reached the declared staging retirement states", () => {
  reach.assertReached();
});
