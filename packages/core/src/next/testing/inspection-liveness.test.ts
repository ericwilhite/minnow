/** A queued ordinary reply follows lock transitions, not its original request flag. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, INSPECTION_LIVENESS_REACH } from "../../testing/reach.js";
import { harness, raw } from "./owner-harness.js";
for (const seed of seedsFor("next-inspection-liveness", [46978, 46979]))
  it(`retains only live queued inspection credit (${String(seed)})`, async () => {
    const reach = createReach("inspection liveness", INSPECTION_LIVENESS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 1, max: 8 }),
        async (initialLive, columns) => {
          reach.beginHistory();
          vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
          vi.setSystemTime(0);
          const { server, engine, governor } = harness({ abandonedStreamMs: 100 }),
            port = raw(server);
          try {
            port.deliver(
              port.statement(
                1,
                `CREATE TABLE t(${Array.from({ length: columns }, (_, i) => `c${String(i)} INTEGER`).join(",")})`,
              ),
            );
            expect((await port.reply(1)).t).toBe("done");
            const baseline = governor.usedBytes;
            port.deliver({ t: "liveness", held: initialLive });
            port.deliver({
              t: "admin",
              seq: 2,
              generation: 1,
              sentAtMs: 0,
              resolvedThrough: 0,
              live: false,
              resendable: true,
              retry: false,
              session: 0,
              op: "describeCatalog",
              args: [],
            });
            expect((await port.reply(2)).t).toBe("value");
            expect(governor.usedBytes).toBeGreaterThan(baseline);
            if (!initialLive) {
              await vi.advanceTimersByTimeAsync(50);
              port.deliver({ t: "liveness", held: true });
              await vi.advanceTimersByTimeAsync(250);
              expect(governor.usedBytes).toBeGreaterThan(baseline);
              reach.reach("non-live became live");
            } else {
              await vi.advanceTimersByTimeAsync(300);
              expect(governor.usedBytes).toBeGreaterThan(baseline);
            }
            reach.reach("live reply retained");
            port.deliver({ t: "liveness", held: false });
            await vi.advanceTimersByTimeAsync(100);
            expect(governor.usedBytes).toBe(baseline);
            reach.reach("lost liveness swept");
          } finally {
            const drain = server.quiesce("closing");
            await drain.drained;
            await engine.close();
            drain.finish();
            vi.useRealTimers();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero reply ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [true, 1],
          [false, 8],
        ],
      },
    );
    reach.assertReached();
  });
