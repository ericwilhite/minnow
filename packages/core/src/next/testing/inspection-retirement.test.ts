/** Funding completed before cancellation must not create a reply after retirement. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, INSPECTION_RETIREMENT_REACH } from "../../testing/reach.js";
import { harness, raw } from "./owner-harness.js";
for (const seed of seedsFor("next-inspection-retirement", [46980, 46981]))
  it(`releases a funded inspection racing retirement (${String(seed)})`, async () => {
    const reach = createReach("inspection retirement", INSPECTION_RETIREMENT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 1, max: 8 }),
        async (disconnect, columns) => {
          reach.beginHistory();
          const { server, engine, governor } = harness(),
            port = raw(server);
          let resume: () => void = () => undefined,
            funded: () => void = () => undefined;
          const held = new Promise<void>((resolve) => {
              resume = resolve;
            }),
            ready = new Promise<void>((resolve) => {
              funded = resolve;
            });
          const admin = engine.admin.bind(engine);
          const spy = vi.spyOn(engine, "admin").mockImplementation(async (...args) => {
            const value = await admin(...args);
            funded();
            await held;
            return value;
          });
          try {
            port.deliver(
              port.statement(
                1,
                `CREATE TABLE t(${Array.from({ length: columns }, (_, i) => `c${String(i)} INTEGER`).join(",")})`,
              ),
            );
            expect((await port.reply(1)).t).toBe("done");
            const baseline = governor.usedBytes;
            port.deliver({
              t: "admin",
              seq: 2,
              generation: 1,
              sentAtMs: Date.now(),
              resolvedThrough: 0,
              live: false,
              resendable: true,
              retry: false,
              session: 0,
              op: "describeCatalog",
              args: [],
            });
            await ready;
            expect(governor.usedBytes).toBeGreaterThan(baseline);
            reach.reach("funded before retirement");
            if (disconnect) port.deliver({ t: "bye" });
            else port.deliver({ t: "cancel", target: 2, close: false });
            resume();
            if (!disconnect) expect((await port.reply(2)).t).toBe("error");
            const drain = server.quiesce("closing");
            await drain.drained;
            expect(governor.usedBytes).toBeLessThanOrEqual(baseline);
            reach.reach(
              disconnect ? "closed before reply arrival" : "cancelled before reply arrival",
            );
            drain.finish();
          } finally {
            resume();
            spy.mockRestore();
            const drain = server.quiesce("closing");
            await drain.drained;
            await engine.close();
            drain.finish();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("no late ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 1],
          [true, 8],
        ],
      },
    );
    reach.assertReached();
  });
