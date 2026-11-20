/** Holder priority (GOVERNOR-WAIT-POLICY.md). One holder chain at a time admits a base as a new
 * admission, then grows it several times with `holdsCredit` while holding everything it has, then
 * releases all. New admissions, some sized so they fit only once the chain has released, run
 * concurrently and hold nothing while they wait. Laws: every request is eventually served (the
 * stall guard turns a stuck wait into a failure), nothing at or below the cap is refused, the cap
 * is never exceeded, and a holder's growth never waits behind a new admission that only its own
 * release could serve. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, GOVERNOR_HOLDER_PRIORITY_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "./governor.js";

const KIB = 1024;
const pause = (turns: number) =>
  new Promise<void>((resolve) => {
    let left = turns;
    const next = () => {
      if (left-- <= 0) resolve();
      else setImmediate(next);
    };
    next();
  });

interface Event {
  readonly kind: "holder" | "new";
  readonly queued: number;
  served: number | null;
}

for (const seed of seedsFor("next-governor-holder-priority", [50101, 50102]))
  it(`serves holder growth first and every new admission eventually (${String(seed)})`, async () => {
    const reach = createReach("governor holder priority", GOVERNOR_HOLDER_PRIORITY_REACH);
    const previousStall = Governor.stallAfterMs;
    Governor.stallAfterMs = 2_000;
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 4, max: 32 }),
          fc.array(
            fc.record({
              base: fc.integer({ min: 1, max: 40 }),
              growths: fc.array(fc.integer({ min: 1, max: 30 }), { minLength: 1, maxLength: 4 }),
              progress: fc.boolean(),
              hold: fc.integer({ min: 0, max: 4 }),
            }),
            { minLength: 1, maxLength: 3 },
          ),
          fc.array(
            fc.record({
              percent: fc.integer({ min: 1, max: 100 }),
              delay: fc.integer({ min: 0, max: 6 }),
              hold: fc.integer({ min: 0, max: 4 }),
            }),
            { minLength: 1, maxLength: 6 },
          ),
          async (capMib, chains, clients) => {
            reach.beginHistory();
            const governor = new Governor(capMib << 20),
              ordinary = governor.capBytes - governor.reserveBytes,
              events: Event[] = [];
            let clock = 0,
              peak = 0;
            const tick = () => ++clock;
            const observe = () => {
              peak = Math.max(peak, governor.usedBytes);
            };
            const request = async (
              kind: Event["kind"],
              label: string,
              bytes: number,
              options: { holdsCredit?: boolean; includesProgress?: boolean },
            ): Promise<readonly Grant[]> => {
              const event: Event = { kind, queued: tick(), served: null };
              events.push(event);
              const grants = await governor.admitGroup({
                label,
                grants: [{ label, minimumBytes: bytes, spillable: false }],
                ...options,
              });
              event.served = tick();
              observe();
              return grants;
            };
            // One holder chain at a time: base, then growth while holding everything so far.
            const runChains = async () => {
              for (const [c, chain] of chains.entries()) {
                const capacity = chain.progress ? governor.capBytes : ordinary,
                  parts = [chain.base, ...chain.growths],
                  total = parts.reduce((sum, part) => sum + part, 0),
                  unit = Math.max(1, Math.floor(capacity / Math.max(total, 1) / 2));
                const held: Grant[] = [];
                try {
                  for (const [i, part] of parts.entries()) {
                    held.push(
                      ...(await request(
                        i === 0 ? "new" : "holder",
                        `chain ${String(c)} ${i === 0 ? "base" : `growth ${String(i)}`}`,
                        part * unit,
                        {
                          ...(i === 0 ? {} : { holdsCredit: true }),
                          ...(chain.progress ? { includesProgress: true } : {}),
                        },
                      )),
                    );
                    await pause(chain.hold);
                  }
                } finally {
                  for (const grant of held) grant.close();
                }
              }
            };
            // New admissions: hold nothing while waiting; some need almost all ordinary room.
            const runClient = async (c: number, client: (typeof clients)[number]) => {
              await pause(client.delay);
              const bytes = Math.max(KIB, Math.floor((ordinary * client.percent) / 100));
              const grants = await request("new", `client ${String(c)}`, bytes, {});
              await pause(client.hold);
              for (const grant of grants) grant.close();
            };
            await Promise.all([runChains(), ...clients.map((client, c) => runClient(c, client))]);
            expect(peak).toBeLessThanOrEqual(governor.capBytes);
            expect(governor.usedBytes).toBe(0);
            expect(events.every((event) => event.served !== null)).toBe(true);
            // A holder queued after a still-waiting new admission was served before it.
            for (const holder of events.filter((event) => event.kind === "holder"))
              for (const earlier of events)
                if (
                  earlier.kind === "new" &&
                  earlier.queued < holder.queued &&
                  (earlier.served ?? Infinity) > (holder.served ?? Infinity)
                )
                  reach.reach("holder served ahead of an earlier new admission");
            if (
              events.some((event) => event.kind === "new" && (event.served ?? 0) > event.queued + 1)
            )
              reach.reach("new admission served after waiting");
            if (chains.some((chain) => chain.progress)) reach.reach("holder with progress credit");
            if (chains.some((chain) => !chain.progress))
              reach.reach("holder without progress credit");
            if (clients.some((client) => client.percent >= 90))
              reach.reach("new admission of almost all room");
          },
        ),
        {
          seed,
          numRuns: 60,
          examples: [
            [
              8,
              [{ base: 10, growths: [5, 5], progress: true, hold: 2 }],
              [{ percent: 95, delay: 1, hold: 1 }],
            ],
          ],
        },
      );
      reach.assertReached();
    } finally {
      Governor.stallAfterMs = previousStall;
    }
  }, 120_000);
