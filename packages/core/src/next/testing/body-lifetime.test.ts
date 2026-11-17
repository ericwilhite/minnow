/** Root close fences a compiled body's borrowed source and copied parameters, including
 * an asynchronous source completion that settles after cancellation. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_LIFETIME_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { yieldTask } from "../task.js";
import type { BoundParameters } from "../exec/executor.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { referentialFixture } from "./referential.test-support.js";
for (const seed of seedsFor("next-native-body-lifetime", [0xb663, 0xb664]))
  it(`fences cancelled compiled source before returning root credit seed ${String(seed)}`, async () => {
    const reach = createReach("compiled body lifetime", NATIVE_BODY_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 99 }),
        fc.boolean(),
        fc.boolean(),
        async (n, published, close) => {
          reach.beginHistory();
          const f = await referentialFixture(
              [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT)",
                `INSERT INTO p VALUES(1,${String(n)})`,
                "CREATE TABLE audit(v INT)",
                "CREATE TRIGGER body AFTER UPDATE ON p BEGIN INSERT INTO audit VALUES(NEW.v); END",
              ],
              published,
              null,
              256 << 20,
              "UPDATE p SET v=v+1",
            ),
            host = f.bodyHost;
          if (!host) throw new Error("host");
          const originalBody = host.spoolBodyInput.bind(host),
            originalAppend = f.images.append.bind(f.images);
          let inBody = false,
            entered!: () => void,
            resume!: () => void,
            parameters: BoundParameters | null = null;
          const suspended = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            });
          const body = vi.spyOn(host, "spoolBodyInput").mockImplementation((request) => {
            inBody = true;
            parameters = request.parameters;
            return originalBody(request).finally(() => {
              inBody = false;
            });
          });
          let held = false;
          const append = vi.spyOn(f.images, "append").mockImplementation(async (...args) => {
            const result = await originalAppend(...args);
            if (inBody && !held) {
              held = true;
              entered();
              await gate;
            }
            return result;
          });
          let finished = false,
            closed = false;
          const running = (async () => {
            await f.nativeRoot();
            await f.end();
          })().finally(() => {
            finished = true;
          });
          void running.catch(() => undefined);
          try {
            await suspended;
            expect(f.source.allocated).toBeGreaterThan(0);
            expect(parameters).not.toBe(null);
            const owned = parameters as BoundParameters | null;
            if (!owned) throw new Error("parameters");
            expect(
              valueAt(
                owned.slots[0] ??
                  (() => {
                    throw new Error("missing binding slot");
                  })(),
                0,
              ),
            ).toBe(n + 1);
            const closing = close
              ? f.statement.closeNativeCommands().then(() => {
                  closed = true;
                })
              : null;
            if (!close) f.stop.abort(new Error("caller cancelled body source"));
            await yieldTask();
            expect(finished).toBe(false);
            expect(closed).toBe(false);
            expect(valueAt(owned.slots[0], 0)).toBe(n + 1);
            resume();
            await expect(running).rejects.toBeInstanceOf(Error);
            await closing;
            expect(() => owned.slots).toThrow("closed");
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            await f.rollback();
            await f.retire();
            for (const [sql, expected] of [
              ["SELECT * FROM p", [{ id: 1, v: n }]],
              ["SELECT * FROM audit", []],
            ] as const) {
              const got = await f.core.execute(sql);
              if (!("batches" in got)) throw new Error("rows");
              expect(oracleRows(got)).toEqual(expected);
            }
            reach.reach(close ? "root close" : "caller cancel");
            reach.reach(published ? "published" : "resident");
            reach.reach("pending producer fence");
            reach.reach("parameter lifetime");
            reach.reach("rollback");
          } finally {
            resume();
            await running.catch(() => undefined);
            body.mockRestore();
            append.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, false, true],
          [2, true, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
