/** ReadOptions.retain=false: exact private misses, existing hits, unchanged ordinary reuse. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  EXACT_PACE_REACH,
  EXACT_READ_REACH,
  EXACT_READER_REACH,
} from "../../testing/reach.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { StoreCorruptionError, type StoreSession } from "../storage/contract.js";
import type { BlobRef, ReadLease, ReadOptions } from "../storage/extents.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { PageCache } from "../storage/page-cache.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";

for (const implementation of ["storage", "memory"] as const)
  for (const seed of seedsFor("next-exact-private-read", [0xb301, 0xb302]))
    it(`keeps exact non-retained leases and ordinary cache benefits (${implementation}, ${String(seed)})`, async () => {
      const reach = createReach("exact read", [
        ...EXACT_READ_REACH,
        EXACT_READER_REACH[implementation],
      ]);
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            overlap: fc.boolean(),
            cached: fc.boolean(),
            duplicate: fc.boolean(),
            count: fc.integer({ min: 2, max: 5 }),
            length: fc.integer({ min: 32, max: 256 }),
            corrupt: fc.boolean(),
            closeHeld: fc.boolean(),
          }),
          async (shape) => {
            reach.beginHistory();
            reach.reach(EXACT_READER_REACH[implementation]);
            const data = Uint8Array.from({ length: 65536 }, (_, k) => (k * 17 + 23) % 251);
            let reads = 0;
            const session = {
              async readDataInto(at: number, out: Uint8Array) {
                reads++;
                out.set(data.subarray(at, at + out.length));
              },
            } as unknown as StoreSession;
            const memory = countingMemory(Infinity),
              budget = countingBudget(Infinity);
            const adapter = new MemoryExtentSpace();
            adapter.bytes = data;
            const cache =
              implementation === "storage"
                ? new PageCache(session)
                : {
                    async read(refs: readonly BlobRef[], options: ReadOptions) {
                      const before = adapter.reads;
                      const misses = refs.filter((ref) => adapter.peek(ref) === null).length;
                      try {
                        return await adapter.read(refs, options);
                      } finally {
                        if (adapter.reads > before) reads += misses;
                      }
                    },
                    peek: adapter.peek.bind(adapter),
                    // The adapter has eviction, not the production space's close lifecycle.
                    close: () => adapter.evict(),
                  };
            if (cache instanceof PageCache) cache.attach(memory);
            const refs: BlobRef[] = Array.from({ length: shape.count }, (_, k) => {
              const offset = k * (shape.overlap ? 8 : 8192);
              return {
                extent: { offset: 0, length: data.length },
                offset,
                length: shape.length,
                checksum: xxHash32(data.subarray(offset, offset + shape.length)),
              };
            });
            const first = refs[0],
              last = refs.at(-1);
            if (!first || !last) throw Error("missing ref");
            if (shape.cached) {
              const warm = await cache.read([first], { budget });
              warm.release();
              reach.reach("hit");
            }
            const wanted = shape.duplicate
              ? [...refs, { ...last, extent: { ...last.extent } }]
              : refs;
            const total = refs.reduce((n, r) => n + r.length, 0),
              exact = countingBudget(total);
            const options: ReadOptions & { retain: false } = {
              budget: exact,
              retain: false,
              once: true,
              reuseBuffers: true,
            };
            try {
              const denied = countingBudget(total - 1),
                before = reads;
              await expect(cache.read(wanted, { ...options, budget: denied })).rejects.toThrow(
                "budget",
              );
              expect(denied.used).toBe(0);
              expect(reads).toBe(before);
              reach.reach("refused");
              if (shape.corrupt) {
                data[last.offset + last.length - 1] =
                  (data[last.offset + last.length - 1] ?? 0) ^ 1;
                await expect(cache.read(wanted, options)).rejects.toThrow(StoreCorruptionError);
                expect(exact.used).toBe(0);
                for (const ref of refs.slice(1)) expect(cache.peek(ref, { once: true })).toBeNull();
                reach.reach("error-cleanup");
              } else {
                const lease = await cache.read(wanted, options);
                try {
                  expect(exact.used).toBe(total);
                  for (let k = 0; k < wanted.length; k++) {
                    const ref = wanted[k];
                    if (!ref) throw Error("missing request");
                    expect(lease.bytes[k]).toEqual(
                      data.subarray(ref.offset, ref.offset + ref.length),
                    );
                  }
                  if (shape.cached) expect(lease.bytes[0]).toBe(cache.peek(first, { once: true }));
                  for (const ref of refs.slice(shape.cached ? 1 : 0))
                    expect(cache.peek(ref, { once: true })).toBeNull();
                  reach.reach("miss");
                  reach.reach(shape.overlap ? "overlap" : "gap");
                  if (shape.duplicate) {
                    expect(lease.bytes.at(-1)).toBe(lease.bytes.at(-2));
                    reach.reach("duplicate");
                  }
                  const abort = new AbortController(),
                    cancelledBudget = countingBudget(total);
                  abort.abort(new Error("cancel exact"));
                  await expect(
                    cache
                      .read(wanted, { ...options, signal: abort.signal, budget: cancelledBudget })
                      .then((lease) => {
                        lease.release();
                        return lease;
                      }),
                  ).rejects.toThrow("cancel exact");
                  expect(cancelledBudget.used).toBe(0);
                  reach.reach("cancelled");
                  if (shape.closeHeld) {
                    cache.close();
                    reach.reach("close-held");
                  }
                } finally {
                  lease.release();
                }
                lease.release();
                lease.release();
                expect(exact.used).toBe(0);
                if (!shape.closeHeld) {
                  const ordinary = await cache.read([last], { budget });
                  ordinary.release();
                  const used = reads,
                    warm = await cache.read([last], { budget });
                  warm.release();
                  expect(reads).toBe(used);
                  expect(cache.peek(last)).not.toBeNull();
                  reach.reach("ordinary-warm");
                }
              }
            } finally {
              cache.close();
              expect(memory.used).toBe(0);
              expect(budget.used).toBe(0);
              expect(exact.used).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 32,
          examples: [
            [
              {
                overlap: false,
                cached: true,
                duplicate: true,
                count: 3,
                length: 64,
                corrupt: false,
                closeHeld: false,
              },
            ],
            [
              {
                overlap: true,
                cached: false,
                duplicate: false,
                count: 4,
                length: 96,
                corrupt: false,
                closeHeld: true,
              },
            ],
            [
              {
                overlap: false,
                cached: false,
                duplicate: false,
                count: 3,
                length: 128,
                corrupt: true,
                closeHeld: false,
              },
            ],
          ],
        },
      );
      reach.assertReached();
    });

for (const seed of seedsFor("next-exact-read-pacing", [0xb303, 0xb304]))
  it(`yields exact multi-read continuations and cancels before unstarted IO (${String(seed)})`, async () => {
    const { TaskClock } = await import("../task.js");
    const reach = createReach("exact read pacing", EXACT_PACE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (cancel) => {
        reach.beginHistory();
        let now = 0,
          calls = 0,
          timerSeen = false;
        const controller = new AbortController(),
          clock = new TaskClock(() => now),
          data = new Uint8Array((1 << 20) + 256).fill(42);
        const cache = new PageCache({
          async readDataInto(at: number, out: Uint8Array) {
            calls++;
            out.set(data.subarray(at, at + out.length));
            now += 8;
          },
        } as unknown as StoreSession);
        const memory = countingMemory(Infinity),
          budget = countingBudget(512);
        cache.attach(memory);
        const refs: BlobRef[] = [0, 1 << 20].map((offset) => ({
          extent: { offset: 0, length: data.length },
          offset,
          length: 256,
          checksum: xxHash32(data.subarray(offset, offset + 256)),
        }));
        const task = {
          clock,
          signal: controller.signal,
          yieldTask: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
        };
        let granted: ReadLease | undefined;
        const timer = setTimeout(() => {
          timerSeen = true;
          if (cancel) controller.abort(new Error("cancel between slabs"));
        }, 0);
        try {
          const work = cache
            .read(refs, { budget, retain: false, signal: controller.signal, task })
            .then((lease) => {
              granted = lease;
              return lease;
            });
          if (cancel) {
            await expect(work).rejects.toThrow("cancel between slabs");
            expect(calls).toBe(1);
            reach.reach("cancelled");
          } else {
            const lease = await work;
            try {
              expect(calls).toBe(2);
              expect(lease.bytes[1]).toEqual(new Uint8Array(256).fill(42));
            } finally {
              lease.release();
            }
            reach.reach("completed");
          }
          expect(timerSeen).toBe(true);
          reach.reach("timer");
          expect(budget.used).toBe(0);
        } finally {
          clearTimeout(timer);
          granted?.release();
          cache.close();
          expect(memory.used).toBe(0);
          expect(budget.used).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 8, examples: [[false], [true]] },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-exact-duplicate-budget", [0xb309, 0xb30a]))
  it(`charges semantic aliases once in both readers (${String(seed)})`, async () => {
    const reach = createReach("exact aliases", Object.values(EXACT_READER_REACH));
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 32, max: 512 }),
        fc.integer({ min: 2, max: 6 }),
        async (length, count) => {
          reach.beginHistory();
          const data = Uint8Array.from({ length }, (_, k) => k % 251);
          const ref: BlobRef = {
            extent: { offset: 0, length },
            offset: 0,
            length,
            checksum: xxHash32(data),
          };
          for (const implementation of ["storage", "memory"] as const) {
            const adapter = new MemoryExtentSpace();
            adapter.bytes = data;
            const memory = countingMemory(Infinity),
              budget = countingBudget(length);
            const reader =
              implementation === "memory"
                ? adapter
                : new PageCache({
                    async readDataInto(_at: number, out: Uint8Array) {
                      out.set(data);
                    },
                  } as unknown as StoreSession);
            if (reader instanceof PageCache) reader.attach(memory);
            let lease: ReadLease | undefined;
            try {
              lease = await reader.read(
                Array.from({ length: count }, () => ({ ...ref, extent: { ...ref.extent } })),
                { budget, retain: false },
              );
              expect(budget.used).toBe(length);
              for (const bytes of lease.bytes) {
                expect(bytes).toBe(lease.bytes[0]);
                expect(bytes).toEqual(data);
              }
              reach.reach(EXACT_READER_REACH[implementation]);
            } finally {
              lease?.release();
              if (reader instanceof PageCache) reader.close();
              else reader.evict();
            }
            expect(budget.used).toBe(0);
            expect(memory.used).toBe(0);
          }
        },
      ),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });
