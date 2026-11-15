/** Append-only FTS law (approved write seam): any accepted keyed mutation invalidates
 * every binding atomically; native delete undo is authoritative even without a DML hook.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_KEYED_PUBLICATION_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  batch,
  texts,
  locators,
  rules,
  rowsOf,
} from "../write/test-support.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
for (const seed of seedsFor("next-fts-keyed-publication", [0xb84f, 0xb850]))
  it(`invalidates native keyed writes independently of DML notifications ${String(seed)}`, async () => {
    const reach = createReach("FTS keyed publication", FTS_KEYED_PUBLICATION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 1, max: 4 }), async (group, n) => {
        reach.beginHistory();
        for (const kind of ["update", "delete"] as const) {
          const h = new Harness();
          try {
            const path = await setupFts(h);
            const values = Array.from({ length: n }, (_, i) => `alpha ${String(i)}`);
            await autocommit(path, (s) => s.table(1).insert(batch(texts(values)), null));
            if (group) {
              await path.flush();
              await path.quiesce();
              reach.reach("durable group");
            } else reach.reach("memtable");
            const old = path.pin(),
              tx = await path.begin(),
              s = tx.statement({ rules });
            try {
              if (kind === "update")
                await s
                  .table(1, { assigned: [1] })
                  .update(batch(locators([0], 1), texts(["beta"])), null);
              else await s.table(1).delete(batch(locators([0], 1)), null);
              await s.end();
              const receipt = await tx.commit(null);
              await receipt.durable;
              expect(
                (await ftsParts(h)).entries.some(
                  (e) => e.kind === "keyed" && e.tableId === 1 && e.ftsId === 0,
                ),
                "accepted native keyed undo must invalidate every FTS binding",
              ).toBe(true);
              const logged = await lastFtsCommit(h);
              expect(logged.body.effects.filter((e) => e.kind === "fts")).toHaveLength(1);
              expect(logged.body.effects.some((e) => e.kind === "rows" && e.deletes !== null)).toBe(
                true,
              );
              reach.reach(kind);
              reach.reach("one row and FTS commit");
              expect(await rowsOf(old, 1)).toEqual(values.map((v) => [v]));
              reach.reach("older snapshot");
              old.release();
              await path.recycleLog();
              await h.reopen();
              const pin = h.path.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual(
                  (kind === "update" ? [...values.slice(1), "beta"] : values.slice(1)).map((v) => [
                    v,
                  ]),
                );
              } finally {
                pin.release();
              }
              expect((await ftsParts(h)).entries.some((e) => e.kind === "keyed")).toBe(true);
              reach.reach("checkpoint and reopen");
            } finally {
              old.release();
              tx.rollback();
            }
          } finally {
            await h.close();
          }
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  }, 60_000);
