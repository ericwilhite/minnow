import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_PUBLICATION_PRESSURE_REACH } from "../../testing/reach.js";
import { Harness, batch, texts, rules, rowsOf } from "../write/test-support.js";
import { setupFts, ftsParts } from "./fts-transaction.test-support.js";
import type { Grant } from "../memory/governor.js";
for (const seed of seedsFor("next-fts-publication-pressure", [0xb849, 0xb84a]))
  it(`keeps accepted rows under generated all-credit FTS preparation pressure ${String(seed)}`, async (context) => {
    const reach = createReach("FTS publication pressure", FTS_PUBLICATION_PRESSURE_REACH);
    let stopped = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      retire?.();
    });
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 6 }),
        fc.shuffledSubarray([null, "", "alpha alpha", "ＦＯＯ 42"], { minLength: 4, maxLength: 4 }),
        async (n, order) => {
          reach.beginHistory();
          for (const pressure of [false, true]) {
            if (stopped) throw new Error("Retired FTS pressure history");
            const h = new Harness();
            let competitor: Grant | undefined;
            let opened: Awaited<ReturnType<typeof setupFts>> | undefined;
            const release = () => {
              if (competitor) {
                competitor.free(competitor.allocated);
                competitor.close();
                competitor = undefined;
              }
            };
            retire = () => {
              release();
              void opened?.close().catch(() => undefined);
            };
            try {
              const path = await setupFts(h);
              opened = path;
              const tx = await path.begin(),
                s = tx.statement({ rules });
              const values = Array.from({ length: n }, (_, i) => order[i % order.length] ?? null);
              await s.table(1).insert(batch(texts(values)), null);
              await s.end();
              if (pressure) {
                competitor = await h.governor.admit({
                  label: "FTS competing native owner",
                  minimumBytes: 0,
                  spillable: true,
                });
                while (competitor.tryGrow(64 * 1024)) {
                  /* Exhaust ordinary growth credit. */
                }
                expect(competitor.allocate(competitor.held)).toBe(true);
              }
              const receipt = await tx.commit(null);
              await receipt.durable;
              const entries = (await ftsParts(h)).entries;
              if (pressure) {
                expect(entries).toMatchObject([{ kind: "invalidate", ftsId: 1 }]);
                reach.reach("pressure invalidation");
              } else {
                expect(entries.filter((e) => e.kind === "insert")).toHaveLength(n);
                reach.reach("funded insert");
              }
              release();
              const pin = path.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual(values.map((v) => [v]));
              } finally {
                pin.release();
              }
              reach.reach("rows preserved");
              if (values.includes(null)) reach.reach("NULL");
              if (values.some((v) => v !== null)) reach.reach("text");
            } finally {
              release();
              await h.close();
              retire = undefined;
            }
          }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  }, 60_000);
