/** A frozen preflight cannot strand a runnable publisher. A resumed attempt targets an
 * obsolete identity even if NO lock-loss notification is delivered until after it resumes.
 */
import "fake-indexeddb/auto";
import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROOT_GATE_LIVENESS_REACH } from "../../testing/reach.js";
import { SimLocks } from "../storage/sim-disk.js";
import { DURABLE_READER_VERSION, DURABLE_WRITER_VERSION } from "../durable-format.js";
import type { OwnerLease } from "../storage/contract.js";
import { deferred, fixture } from "./root-gate.test-support.js";

const nativeSetTimeout = globalThis.setTimeout,
  nativeClearTimeout = globalThis.clearTimeout;
async function completedWithin(work: Promise<void>, ms: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work.then(() => true),
      new Promise<boolean>((resolve) => {
        timer = nativeSetTimeout(() => resolve(false), ms);
      }),
    ]);
  } finally {
    nativeClearTimeout(timer);
  }
}

// Independent contract deadline; the same law runs unchanged against the pre-fix backend.
const PUBLICATION_GATE_TIMEOUT_MS = 1_000;
const identityName = (epoch: number, generation: number) =>
  `minnow-owner:9:gate-race:${String(epoch)}:${String(generation)}:${String(DURABLE_READER_VERSION)}:${String(DURABLE_WRITER_VERSION)}`;
const reach = createReach("frozen opener versus publisher", ROOT_GATE_LIVENESS_REACH);
afterAll(() => reach.assertReached());

for (const kind of ["IDB", "OPFS"] as const)
  for (const seed of seedsFor("root-gate-liveness", [0xb2cd, 0xb2ce]))
    it(`${kind}: bounds publication independently of a suspended opener seed ${String(seed)}`, async () => {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 255 }), async (value) => {
          reach.beginHistory();
          for (const cut of [
            "gate granted",
            "future gate granted",
            "before owner steal",
            "after owner steal",
            "before native fence",
          ] as const)
            for (const delayed of [false, true]) {
              // Immediate notification after an actual live takeover legitimately steps down the
              // former owner. The delayed case additionally tests recovery in that exact interval.
              if ((cut === "after owner steal" || cut === "before native fence") && !delayed)
                continue;
              vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
              const locks = new SimLocks(),
                suspended = deferred(),
                resume = deferred(),
                waiting = deferred();
              const pendingNotifications: Array<() => void> = [];
              let opener = false,
                publishing = false,
                cutTaken = false,
                delayLoss = delayed;
              let ownerName = "",
                gateName = "";
              const futureGate = `minnow-gate:${identityName(2, 2)}`;
              const pause = async () => {
                if (!cutTaken) {
                  cutTaken = true;
                  suspended.resolve();
                  await resume.promise;
                }
              };
              const source = await fixture(
                kind,
                {
                  query: () => locks.query(),
                  request: async (name, options) => {
                    if (publishing && name === gateName) waiting.resolve();
                    const isTarget = opener && name === ownerName && options.steal === true;
                    if (isTarget && cut === "before owner steal") await pause();
                    const raw = await locks.request(name, options);
                    const isOwner = name.startsWith("minnow-owner:") || name === "minnow:gate-race";
                    if (!opener && !publishing && isOwner) {
                      ownerName = name;
                      gateName = `minnow-gate:${name}`;
                    }
                    let held = raw;
                    if (delayed && isOwner) {
                      const lost = new AbortController();
                      raw.lost.addEventListener(
                        "abort",
                        () => {
                          if (delayLoss) pendingNotifications.push(() => lost.abort());
                          else lost.abort();
                        },
                        { once: true },
                      );
                      held = { lost: lost.signal, release: () => raw.release() };
                    }
                    if (
                      opener &&
                      ((cut === "gate granted" && name === gateName) ||
                        (cut === "future gate granted" && name === futureGate) ||
                        (cut === "after owner steal" && isTarget))
                    )
                      await pause();
                    return held;
                  },
                },
                async () => {
                  if (opener && cut === "before native fence") await pause();
                },
              );
              const backend = source.backend(),
                owner = await backend.acquire(),
                session = await backend.open(owner);
              let acquired: OwnerLease | undefined,
                outcome: Promise<unknown> | undefined,
                publication: Promise<void> | undefined;
              try {
                await session.publish(Uint8Array.of(value));
                await session.appendLog(Uint8Array.of(value, value));
                await session.flushLog();
                opener = true;
                outcome = backend.acquire({ steal: true, timeoutMs: 10_000 }).then(
                  (lease) => {
                    acquired = lease;
                    return lease;
                  },
                  (error: unknown) => error,
                );
                await suspended.promise;
                opener = false;
                publishing = true;
                publication = session.publish(Uint8Array.of(value, 0));
                await waiting.promise;
                await vi.advanceTimersByTimeAsync(PUBLICATION_GATE_TIMEOUT_MS + 1);
                // This deadline belongs to the RUNNABLE publisher; the opener remains suspended.
                // A mutant with an unbounded gate wait fails this assertion, not a test timeout.
                expect(await completedWithin(publication, 2_000)).toBe(true);
                expect(owner.signal.aborted).toBe(false);
                expect(owner.epoch).toBe(1);
                expect(await source.counters()).toEqual({ epoch: 1, generation: 2 });
                expect((await locks.query()).some((name) => name === identityName(1, 2))).toBe(
                  true,
                );
                expect(await session.readRoot()).toEqual(Uint8Array.of(value, 0));
                const before = await source.image();
                resume.resolve();
                expect(await outcome).toMatchObject({ code: "MINNOW_OWNERSHIP_LOST" });
                // Deliver the obsolete owner's loss only AFTER its stale attempt has run.
                for (const notify of pendingNotifications.splice(0)) notify();
                expect(owner.signal.aborted).toBe(false);
                expect(await source.image()).toEqual(before);
                await session.appendLog(Uint8Array.of(value));
                await session.flushLog();
                reach.reach(kind);
                reach.reach(cut);
                reach.reach(delayed ? "delayed notification" : "immediate notification");
                reach.reach("publisher completes while opener suspended");
                reach.reach("same epoch higher generation");
                reach.reach("stale opener no writes");
                reach.reach("live owner still writes");
                // Compatible takeover remains supported after recovery. Deliver current loss as
                // it happens here (OPFS must close its exclusive handles before the next opens).
                opener = false;
                pendingNotifications.length = 0;
                delayLoss = false;
                const taking = backend.acquire({ steal: true, timeoutMs: 10_000 });
                acquired = await taking;
                expect(acquired.epoch).toBe(2);
                expect(await source.counters()).toEqual({ epoch: 2, generation: 3 });
                expect(owner.signal.aborted).toBe(true);
                const replacement = await backend.open(acquired);
                expect(await replacement.readRoot()).toEqual(Uint8Array.of(value, 0));
                await replacement.close();
                reach.reach("compatible takeover preserved");
              } finally {
                resume.resolve();
                for (const notify of pendingNotifications.splice(0)) notify();
                await outcome;
                await publication?.catch(() => undefined);
                await session.close();
                await owner.release();
                await acquired?.release();
                vi.useRealTimers();
              }
            }
        }),
        { seed, numRuns: 2, endOnFailure: true },
      );
    }, 60_000);
