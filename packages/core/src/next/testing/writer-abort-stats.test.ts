/** ExtentSpace stats/audit: every allocated byte has exactly one owner during abort. */
import fc from "fast-check";
import { expect, it, onTestFinished } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITER_ABORT_STATS_REACH } from "../../testing/reach.js";
import { yieldTask } from "../task.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { DATA_ALIGN, type DataRange } from "../storage/contract.js";
import { SpaceFailedError } from "../storage/extents.js";
import { countingMemory, openOver } from "../storage/space-test-support.js";
import { idleGarbageOverBound } from "../write/idle-garbage.js";

for (const seed of seedsFor("next-writer-abort-stats", [46500, 46501]))
  it(`accounts every abort await, including partial release failure (seed ${String(seed)})`, async () => {
    const reach = createReach("writer abort stats", WRITER_ABORT_STATS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(0, 1, 65),
        fc.constantFrom(1, 2, 3, 257),
        fc.integer({ min: 0, max: 2 }),
        async (sealed, segments, failAt) => {
          reach.beginHistory();
          const memory = countingMemory(8 << 20);
          let active = false,
            releases = 0,
            observations = 0;
          let firstError: Error | undefined;
          const failure = new Error("injected abort release failure");
          const observe = () => {
            if (!active) return;
            try {
              const stats = space.stats;
              expect(stats.writerBytes).toBeGreaterThanOrEqual(0);
              expect(stats.writerBytes + stats.releasingBytes).toBe(
                stats.dataBytes - stats.freeBytes - stats.untrackedFreeBytes,
              );
              // This is the actual predicate used by #armIdlePublish, not a shadow.
              expect(idleGarbageOverBound(space, 0)).toBe(false);
              observations++;
              if (stats.releasingBytes > 0) {
                reach.reach("stats observed mid-abort");
                reach.reach("idle publish observed mid-abort");
              }
            } catch (error) {
              firstError ??=
                error instanceof Error
                  ? error
                  : new Error("Abort observer failed", { cause: error });
            }
          };
          const opened = await openOver(new MemoryBackend(), {
            memory,
            wrap: (session) =>
              new Proxy(session, {
                get(target, key) {
                  const value: unknown = Reflect.get(target, key);
                  if (typeof value !== "function") return value;
                  if (key === "logLength")
                    return (value as (...args: unknown[]) => unknown).bind(target);
                  if (key === "metadata")
                    return () => {
                      const backing = session.metadata?.();
                      if (backing === null || backing === undefined) return backing;
                      return new Proxy(backing, {
                        get(store, method) {
                          const member: unknown = Reflect.get(store, method);
                          if (typeof member !== "function") return member;
                          const invoke = (member as (...args: unknown[]) => unknown).bind(store);
                          return async (...args: unknown[]) => {
                            observe();
                            try {
                              return await invoke(...args);
                            } finally {
                              observe();
                              if (active) reach.reach("metadata await observed");
                            }
                          };
                        },
                      });
                    };
                  const call = (value as (...args: unknown[]) => unknown).bind(target);
                  return async (...args: unknown[]) => {
                    observe();
                    await Promise.resolve();
                    observe();
                    if (active && key === "releaseData") {
                      const ranges = args[0] as readonly DataRange[];
                      expect(ranges.length).toBeGreaterThan(0);
                      releases++;
                      if (releases === failAt) throw failure;
                    }
                    try {
                      return await call(...args);
                    } finally {
                      observe();
                    }
                  };
                },
              }),
          });
          const space = opened.space;
          const writer = space.beginWrite("abort observer");
          let pending: Promise<void> | undefined,
            audit: Promise<unknown> | undefined,
            observer: Promise<void> | undefined;
          const close = async () => {
            active = false;
            await pending?.catch(() => undefined);
            await observer;
            await audit;
            await opened.space.close();
            await opened.session.close();
          };
          onTestFinished(close);
          try {
            const first = writer.open(DATA_ALIGN);
            for (let i = 1; i < segments; i++) {
              writer.open(DATA_ALIGN); // force a separate physical reservation segment
              expect(first.extend(DATA_ALIGN)).toBe(false);
            }
            for (let i = 0; i < sealed; i++) await writer.write([new Uint8Array([i])]);
            if (segments > 1) reach.reach("fragmented builder");
            if (segments > 256) reach.reach("release batch boundary");
            if (sealed > 64) reach.reach("spilled sealed inventory");
            active = true;
            const observing = () => active;
            observer = (async () => {
              let turns = 0;
              while (observing()) {
                observe();
                if (++turns % 32 === 0) await yieldTask();
                else await Promise.resolve();
              }
            })();
            pending = writer.abort();
            // Inventory capture refuses an aborting owner; audit queues behind its mutation.
            const capture = writer.snapshotExtents?.bind(writer);
            if (capture === undefined) throw new Error("Native writer inventory is required");
            expect(() => capture({ budget: opened.budget })).toThrow(/abort/u);
            audit = space.audit().then(
              () => null,
              (error: unknown) => error,
            );
            await pending;
            observe();
            active = false;
            await observer;
            if (firstError !== undefined) throw firstError;
            expect(observations).toBeGreaterThan(2);
            expect(releases).toBeGreaterThan(0);
            if (failAt !== 0 && releases >= failAt) {
              expect(space.failed).toBe(failure);
              expect(space.stats.writerBytes).toBeGreaterThan(0);
              const refused = await audit;
              expect(refused).toBeInstanceOf(SpaceFailedError);
              expect((refused as SpaceFailedError).cause).toBe(failure);
              reach.reach("failed ranges remain owned");
              if (failAt > 1) reach.reach("partial release failure");
            } else {
              expect(space.failed).toBeNull();
              expect(space.stats.writerBytes).toBe(0);
              expect(space.stats.releasingBytes).toBe(0);
              expect(await audit).toBeNull();
              reach.reach("successful abort audit");
            }
            reach.reach("capture refuses aborting writer");
          } finally {
            await close();
          }
          expect(opened.budget.used).toBe(0);
          expect(memory.used).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [0, 1, 0],
          [1, 2, 1],
          [65, 257, 2],
          [65, 2, 0],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
