/** Statement completion inspects only its own accepted undo range, not prior statements.
 * Deterministic work bound: no timing threshold or host-load inference.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_STATEMENT_BOUND_REACH } from "../../testing/reach.js";
import { Harness, batch, texts, rules } from "../write/test-support.js";
import { setupFts } from "./fts-transaction.test-support.js";
for (const seed of seedsFor("next-fts-statement-bound", [0xb85d, 0xb85e]))
  it(`never revisits previous statement undo for FTS completion ${String(seed)}`, async () => {
    const reach = createReach("FTS statement bound", FTS_STATEMENT_BOUND_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 3, max: 8 }), async (n) => {
        reach.beginHistory();
        const h = new Harness();
        try {
          const path = await setupFts(h),
            tx = await path.begin(),
            undo = tx.undo;
          let watching = false,
            bound = 0,
            priorReads = 0;
          const observed = new Proxy(undo, {
            get(target, key, receiver) {
              const i = typeof key === "string" ? Number(key) : NaN;
              if (watching && Number.isSafeInteger(i) && i >= 0 && i < bound) priorReads++;
              return Reflect.get(target, key, receiver) as unknown;
            },
          });
          Object.defineProperty(tx, "undo", { value: observed });
          try {
            for (let i = 0; i < n; i++) {
              bound = undo.length;
              priorReads = 0;
              const s = tx.statement({ rules });
              await s.table(1).insert(batch(texts([`alpha ${String(i)}`])), null);
              if (i % 2) {
                s.noteFtsMutation(1, "upsert");
                reach.reach("accepted notice");
              } else reach.reach("ordinary insert");
              watching = true;
              try {
                await s.end();
              } finally {
                watching = false;
              }
              expect(priorReads, "FTS end must not revisit prior statement undo").toBe(0);
              if (bound > 0) reach.reach("prior history excluded");
              reach.reach("current statement checked");
            }
            const receipt = await tx.commit(null);
            await receipt.durable;
            reach.reach("commit");
          } finally {
            watching = false;
            Object.defineProperty(tx, "undo", { value: undo });
            tx.rollback();
          }
        } finally {
          await h.close();
        }
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  }, 60_000);
