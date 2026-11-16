/** Frozen acquisition boundaries still allow compatible takeovers and preserve native fences. */
import "fake-indexeddb/auto";
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROOT_FENCE_BOUNDARY_REACH } from "../../testing/reach.js";
import { SimLocks } from "../storage/sim-disk.js";
import { identityOwnerName, type OwnerIdentity } from "../storage/owner-identity.js";
import { DURABLE_READER_VERSION, DURABLE_WRITER_VERSION } from "../durable-format.js";
import type { OwnerLease } from "../storage/contract.js";
import { deferred, fixture } from "./root-gate.test-support.js";

const initial: OwnerIdentity = {
  epoch: 1,
  generation: 1,
  reader: DURABLE_READER_VERSION,
  writer: DURABLE_WRITER_VERSION,
};
const reach = createReach("native acquisition freeze boundaries", ROOT_FENCE_BOUNDARY_REACH);
afterAll(() => reach.assertReached());
for (const kind of ["IDB", "OPFS"] as const)
  for (const seed of seedsFor("native-fence-boundaries", [0xb2d1, 0xb2d2]))
    it(`${kind}: fences every exposed lease and never blocks on obsolete identities seed ${String(seed)}`, async () => {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 255 }), async (value) => {
          reach.beginHistory();
          for (const cut of [
            "before owner grant",
            "after owner grant",
            "obsolete opener remains frozen",
          ] as const) {
            vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
            const locks = new SimLocks(),
              waiterQueued = deferred(),
              stopped = deferred(),
              resume = deferred(),
              publisherWaiting = deferred();
            let waitingForGate = false,
              armed = false,
              taken = false,
              publishing = false;
            const pause = async () => {
              taken = true;
              stopped.resolve();
              await resume.promise;
            };
            const target = identityOwnerName(
              "gate-race",
              cut === "obsolete opener remains frozen"
                ? initial
                : { ...initial, epoch: 2, generation: 2 },
            );
            const source = await fixture(kind, {
              query: () => locks.query(),
              request: async (name, options) => {
                if (waitingForGate && name.startsWith("minnow-gate:")) waiterQueued.resolve();
                if (publishing && name === `minnow-gate:${identityOwnerName("gate-race", initial)}`)
                  publisherWaiting.resolve();
                const atCut =
                  armed &&
                  !taken &&
                  name === target &&
                  (cut !== "obsolete opener remains frozen" || options.steal === true);
                if (atCut && cut !== "after owner grant") await pause();
                const lock = await locks.request(name, options);
                if (atCut && cut === "after owner grant") await pause();
                return lock;
              },
            });
            const backend = source.backend(),
              owner = await backend.acquire(),
              session = await backend.open(owner);
            let candidate: OwnerLease | undefined,
              successor: OwnerLease | undefined,
              pending: Promise<unknown> | undefined;
            try {
              await session.publish(Uint8Array.of(value));
              await session.appendLog(Uint8Array.of(value));
              await session.flushLog();
              armed = true;
              pending = backend.acquire({ steal: true, timeoutMs: 10_000 }).then(
                (lease) => {
                  candidate = lease;
                  return lease;
                },
                (error: unknown) => error,
              );
              await stopped.promise;
              armed = false;
              if (cut === "obsolete opener remains frozen") {
                publishing = true;
                const published = session.publish(Uint8Array.of(value, value));
                await publisherWaiting.promise;
                await vi.advanceTimersByTimeAsync(1001);
                await published;
                // The older attempt STILL holds E1/G1's gate and E2/G2's future gate.
                // Neither can block a fresh E1/G2 -> E2/G3 takeover.
                successor = await backend.acquire({ steal: true });
                expect(successor.epoch).toBe(2);
                expect(await source.counters()).toEqual({ epoch: 2, generation: 3 });
                const next = await backend.open(successor),
                  before = await source.image();
                resume.resolve();
                expect(await pending).toMatchObject({ code: "MINNOW_OWNERSHIP_LOST" });
                expect(successor.signal.aborted).toBe(false);
                expect(await source.image()).toEqual(before);
                expect(await next.readRoot()).toEqual(Uint8Array.of(value, value));
                await next.publish(Uint8Array.of(value, value, value));
                await next.close();
                reach.reach("fresh takeover while obsolete opener frozen");
              } else {
                // The native fence is already durable although acquire() has not returned.
                expect(await source.counters()).toEqual({ epoch: 2, generation: 2 });
                const before = await source.image();
                await expect(session.publish(Uint8Array.of(0))).rejects.toMatchObject({
                  code: "MINNOW_OWNERSHIP_LOST",
                });
                waitingForGate = true;
                const refused = backend
                  .acquire({ steal: true, timeoutMs: 5 })
                  .catch((error: unknown) => error);
                // Bind the cut to the actual native gate wait, not a number of microtasks.
                await waiterQueued.promise;
                await vi.advanceTimersByTimeAsync(6);
                expect(await refused).toMatchObject({ code: "MINNOW_OWNER_UNRESPONSIVE" });
                expect(await source.image()).toEqual(before);
                resume.resolve();
                await pending;
                expect(candidate?.epoch).toBe(2);
                if (candidate === undefined) throw new Error("no fenced candidate");
                const next = await backend.open(candidate);
                expect(await next.readRoot()).toEqual(Uint8Array.of(value));
                await next.publish(Uint8Array.of(value, value));
                expect(candidate.signal.aborted).toBe(false);
                await next.close();
                reach.reach(cut);
                reach.reach("durable identity before lease exposure");
                reach.reach("cancelled waiter never steals later");
              }
              reach.reach(kind);
              reach.reach("root and WAL preserved at cut");
            } finally {
              resume.resolve();
              await pending;
              await session.close();
              await owner.release();
              await candidate?.release();
              await successor?.release();
              vi.useRealTimers();
            }
          }
        }),
        { seed, numRuns: 2, endOnFailure: true },
      );
    }, 60_000);
