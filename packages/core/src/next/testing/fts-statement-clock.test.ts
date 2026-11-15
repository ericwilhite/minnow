/** Injected shared-clock probe: a large own-undo scan yields and checks abort on resume.
 * Synthetic no-effect undo isolates work/cancellation, not memory or WAL atomicity evidence.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_STATEMENT_CLOCK_REACH } from "../../testing/reach.js";
import { Harness, rules } from "../write/test-support.js";
import { setupFts } from "./fts-transaction.test-support.js";
import { taskClock } from "../task.js";
for (const seed of seedsFor("next-fts-statement-clock", [0xb85f, 0xb860]))
  it(`slices own undo and checks cancellation on resume ${String(seed)}`, async (context) => {
    const reach = createReach("FTS statement clock", FTS_STATEMENT_CLOCK_REACH);
    let stopped = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      retire?.();
    });
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 32768, max: 33000 }), async (n) => {
        reach.beginHistory();
        for (const abort of [false, true]) {
          if (stopped) throw new Error("Retired statement clock probe");
          const h = new Harness(),
            controller = new AbortController();
          const originalNow = taskClock.now;
          let installed = false;
          const restore = () => {
            controller.abort(new Error("FTS statement clock retired"));
            if (installed) {
              installed = false;
              taskClock.now = originalNow;
              taskClock.begin();
            }
          };
          retire = restore;
          try {
            const path = await setupFts(h),
              tx = await path.begin(),
              s = tx.statement({ rules, signal: controller.signal }),
              undo = tx.undo;
            for (let i = 0; i < n; i++) undo.push({ kind: "staged", statement: s.ordinal });
            let time = 0,
              reads = 0;
            const proxy = new Proxy(undo, {
              get(target, key, receiver) {
                const i = typeof key === "string" ? Number(key) : NaN;
                if (Number.isSafeInteger(i) && i >= 0) {
                  time += 0.001;
                  reads++;
                }
                return Reflect.get(target, key, receiver) as unknown;
              },
            });
            Object.defineProperty(tx, "undo", { value: proxy });
            taskClock.now = () => time;
            installed = true;
            taskClock.begin();
            try {
              const done = s.end();
              expect(
                reads,
                "scan must suspend within a clock slice plus one chunk",
              ).toBeLessThanOrEqual(5120);
              expect(reads).toBeGreaterThanOrEqual(1024);
              reach.reach("bounded slice");
              if (abort) {
                controller.abort(new Error("FTS statement probe abort"));
                await expect(done).rejects.toThrow(/probe abort/);
                reach.reach("abort on resume");
              } else {
                await done;
                expect(reads).toBe(n);
                reach.reach("finish");
              }
            } finally {
              restore();
              Object.defineProperty(tx, "undo", { value: undo });
              tx.rollback();
            }
            reach.reach("drained");
          } finally {
            restore();
            await h.close();
            retire = undefined;
          }
        }
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 60_000);
