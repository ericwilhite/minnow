import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_STAGED_PUBLICATION_REACH } from "../../testing/reach.js";
import { Harness, batch, texts, rules, rowsOf } from "../write/test-support.js";
import { setupFts, ftsParts, lastFtsCommit } from "./fts-transaction.test-support.js";
for (const seed of seedsFor("next-fts-staged-publication", [0xb84b, 0xb84c]))
  it(`commits pressure-staged rows and missing-delta invalidation together ${String(seed)}`, async () => {
    const reach = createReach("FTS staged publication", FTS_STAGED_PUBLICATION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 4, max: 7 }), async (n) => {
        reach.beginHistory();
        const h = new Harness("strict", "sim", 2);
        try {
          const path = await setupFts(h),
            tx = await path.begin(),
            s = tx.statement({ rules });
          const values = Array.from({ length: n }, (_, i) => "alpha " + String(i));
          for (const value of values) await s.table(1).insert(batch(texts([value])), null);
          await s.end();
          expect(path.maintenance.stages).toBeGreaterThan(0);
          reach.reach("rows actually staged");
          const receipt = await tx.commit(null);
          await receipt.durable;
          const logged = await lastFtsCommit(h);
          expect(logged.body.effects.filter((e) => e.kind === "fts")).toHaveLength(1);
          expect(logged.body.effects.some((e) => e.kind === "publishedRows")).toBe(true);
          reach.reach("one publishedRows and FTS body");
          expect((await ftsParts(h)).entries).toMatchObject([{ kind: "invalidate", ftsId: 1 }]);
          reach.reach("atomic missing-delta invalidation");
          await h.reopen();
          const pin = h.path.pin();
          try {
            expect(await rowsOf(pin, 1)).toEqual(values.map((v) => [v]));
          } finally {
            pin.release();
          }
          reach.reach("replayed rows");
          await h.path.recycleLog();
          expect((await ftsParts(h)).lsn).toBe(receipt.lsn);
          reach.reach("checkpoint");
        } finally {
          await h.close();
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);
