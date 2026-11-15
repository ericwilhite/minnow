/** Optional FTS work is linear in accepted undo, not inserted-ranges x undo. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_COMMIT_WORK_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  batch,
  texts,
  locators,
  rules,
  rowsOf,
} from "../write/test-support.js";
import { setupFts, lastFtsCommit } from "./fts-transaction.test-support.js";
import { hasFtsInserts } from "../write/fts-prepare.js";
import { taskClock } from "../task.js";
for (const seed of seedsFor("next-fts-commit-work", [0xb87b, 0xb87c]))
  it(`bounds native commit undo reads and eligibility slices seed ${String(seed)}`, async (context) => {
    const reach = createReach("FTS commit work", FTS_COMMIT_WORK_REACH);
    let stopped = false,
      retire: (() => void) | undefined;
    context.onTestFinished(() => {
      stopped = true;
      retire?.();
    });
    const checkActive = () => {
      if (stopped) throw new Error("Retired FTS commit-work history");
    };
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(512, 1000, 2000, 2049), fc.boolean(), async (n, keyed) => {
        checkActive();
        reach.beginHistory();
        const h = new Harness();
        try {
          const path = await setupFts(h);
          await autocommit(path, (s) => s.table(1).insert(batch(texts(["seed"])), null));
          await path.recycleLog();
          const tx = await path.begin(),
            s = tx.statement({ rules });
          const writer = s.table(1);
          for (let i = 0; i < n; i++)
            await writer.insert(batch(texts([`alpha ${String(i)}`])), null);
          if (keyed) await writer.delete(batch(locators([0], 1)), null);
          await s.end();
          const undo = tx.undo,
            entries = undo.length;
          let reads = 0;
          const counted = new Proxy(undo, {
            get(target, key, receiver) {
              if (typeof key === "string" && /^(0|[1-9][0-9]*)$/u.test(key)) reads++;
              return Reflect.get(target, key, receiver) as unknown;
            },
          });
          Object.defineProperty(tx, "undo", { value: counted });
          try {
            const receipt = await tx.commit(null);
            await receipt.durable;
            expect(reads, "commit FTS work must remain linear in undo entries").toBeLessThanOrEqual(
              64 * entries + 1024,
            );
          } finally {
            Object.defineProperty(tx, "undo", { value: undo });
          }
          if (keyed) {
            expect((await lastFtsCommit(h)).body.effects.some((e) => e.kind === "fts")).toBe(false);
            reach.reach("keyed skips payload");
          } else reach.reach("append ranges");
          reach.reach("linear commit reads");
          if (n === 1000) reach.reach("1000 ranges");
          if (n === 2000) reach.reach("2000 ranges");
          const pin = path.pin();
          try {
            expect(await rowsOf(pin, 1)).toEqual([
              ...(keyed ? [] : [["seed"]]),
              ...Array.from({ length: n }, (_, i) => [`alpha ${String(i)}`]),
            ]);
          } finally {
            pin.release();
          }
          reach.reach("exact rows");
          // Isolate scheduling/cancellation from native R9's synchronous visibility install.
          // No-effect synthetic entries represent work only, not a memory/pressure proof.
          for (const abort of [false, true]) {
            checkActive();
            const next = await path.begin(),
              statement = next.statement({ rules });
            const raw = next.undo;
            for (let i = 0; i < 32768; i++)
              raw.push({ kind: "staged", statement: statement.ordinal });
            let clock = 0,
              scanned = 0;
            const controller = new AbortController(),
              now = taskClock.now;
            const proxy = new Proxy(raw, {
              get(target, key, receiver) {
                if (typeof key === "string" && /^(0|[1-9][0-9]*)$/u.test(key)) {
                  scanned++;
                  clock += 0.001;
                }
                return Reflect.get(target, key, receiver) as unknown;
              },
            });
            let installed = false;
            const restore = () => {
              controller.abort(new Error("Retired eligibility probe"));
              if (installed) {
                installed = false;
                taskClock.now = now;
                taskClock.begin();
              }
              Object.defineProperty(next, "undo", { value: raw });
            };
            retire = restore;
            try {
              Object.defineProperty(next, "undo", { value: proxy });
              taskClock.now = () => clock;
              installed = true;
              taskClock.begin();
              const done = hasFtsInserts(next, { signal: controller.signal });
              expect(
                scanned,
                "eligibility must suspend within one shared clock slice",
              ).toBeLessThanOrEqual(4352);
              expect(scanned).toBeGreaterThanOrEqual(256);
              reach.reach("bounded eligibility slice");
              if (abort) {
                controller.abort(new Error("eligibility probe abort"));
                await expect(done).rejects.toThrow("eligibility probe abort");
                reach.reach("eligibility abort on resume");
              } else {
                expect(await done).toBe(false);
                expect(scanned).toBe(32768);
              }
            } finally {
              restore();
              next.rollback();
              retire = undefined;
            }
          }
        } finally {
          retire?.();
          retire = undefined;
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [1000, true],
          [2000, true],
          [512, false],
          [2049, false],
        ],
      },
    );
    reach.assertReached();
  }, 60_000);
