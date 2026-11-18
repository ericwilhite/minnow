import { afterAll, expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, LIVE_OBSERVER_REENTRY_REACH } from "../../testing/reach.js";
import { ChangeFeed } from "../access/changes.js";

const reach = createReach("next-live-observer-reentry", LIVE_OBSERVER_REENTRY_REACH);
afterAll(() => reach.assertReached());
const turn = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
for (const seed of seedsFor("next-live-observer-reentry", [8860, 8861])) {
  for (const action of ["subscribe", "unsubscribe", "replace"] as const) {
    it(`dispatches only eligible registrations under ${action} seed=${String(seed)}`, async () => {
      reach.beginHistory();
      let bytes = 0;
      const cap = 64 * 1024;
      const feed = ChangeFeed.create(
        {
          allocate(n) {
            if (bytes + n > cap) return false;
            bytes += n;
            return true;
          },
          free(n) {
            bytes -= n;
            expect(bytes).toBeGreaterThanOrEqual(0);
          },
        },
        0,
      );
      const stops: Array<() => void> = [];
      let primaryCalls = 0;
      let lateCalls = 0;
      let stopPrimary: () => void = () => undefined;
      let stopTarget: () => void = () => undefined;
      const random = mulberry32(seed);
      const replacementBound = 8 + Math.floor(random() * 8);
      const primary = () => {
        primaryCalls++;
        // Bound the intentionally wrong live-Set mutant: failure is an assertion, never timeout.
        if (primaryCalls >= replacementBound) return;
        if (action === "replace") {
          stopPrimary();
          stopPrimary = feed.observe(primary);
          stops.push(stopPrimary);
        } else {
          if (action === "unsubscribe") stopTarget();
          stopTarget = feed.observe(() => {
            lateCalls++;
          });
          stops.push(stopTarget);
        }
      };
      stopPrimary = feed.observe(primary);
      stops.push(stopPrimary);
      if (action === "unsubscribe") {
        stopTarget = feed.observe(() => {
          lateCalls++;
        });
        stops.push(stopTarget);
      }
      const passive = 1 + Math.floor(random() * 5);
      let passiveCalls = 0;
      for (let i = 0; i < passive; i++)
        stops.push(
          feed.observe(() => {
            passiveCalls++;
          }),
        );
      try {
        for (let publication = 1; publication <= 3; publication++) {
          const before = primaryCalls;
          const owner = {};
          feed.prepare(owner, [{ kind: "rows", tableId: publication }]);
          feed.install(owner, publication);
          feed.notify();
          await turn();
          expect(primaryCalls - before, "a replacement must wait for the next publication").toBe(1);
          expect(lateCalls, "a new registration cannot join the dispatch that created it").toBe(
            action === "subscribe" ? (publication * (publication - 1)) / 2 : 0,
          );
          expect(passiveCalls).toBe(passive * publication);
          expect(bytes).toBeLessThanOrEqual(cap);
        }
        reach.reach(action);
        reach.reach("eligible frontier");
        reach.reach("bounded replacement chain");
      } finally {
        for (const stop of stops) stop();
        feed.close();
      }
      expect(bytes).toBe(0);
      reach.reach("zero charges");
    });
  }
}
