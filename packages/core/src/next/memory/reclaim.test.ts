/** Proposal: grant-or-spill; governor rules 4, 6 and 7, including asynchronous spill holders. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "./governor.js";
const MiB = 1024 * 1024;

it.each(seedsFor("next-memory-reclaim", [0xc401, 0xc402]))(
  "reclaims several breakers after the cache, within the cap (seed %i)",
  async (seed) => {
    const reach = createReach("next-memory-reclaim", [
      "multiple spills",
      "async spill",
      "cache first",
      "exact accounting",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 6 }),
        fc.integer({ min: 2, max: 6 }),
        async (count, size) => {
          reach.beginHistory();
          const gov = new Governor(16 * MiB),
            reader = await gov.admit({ label: "reader", minimumBytes: 0, spillable: true });
          let cached = 65536,
            asked = 0;
          const cache = gov.registerCache({
            get evictableBytes() {
              return cached;
            },
            evict() {
              const n = cached;
              cached = 0;
              cache.release(n);
              return n;
            },
          });
          expect(cache.charge(cached)).toBe(true);
          const grants = [];
          const room = gov.capBytes - gov.reserveBytes - cached;
          for (let i = 0; i < count; i++) {
            const g = await gov.admit({
              label: `breaker${String(i)}`,
              minimumBytes: 0,
              spillable: true,
            });
            grants.push(g);
            expect(g.allocate(Math.floor(room / count))).toBe(true);
            g.onReclaimRequest(async () => {
              expect(cached).toBe(0);
              reach.reach("cache first");
              asked++;
              await Promise.resolve();
              reach.reach("async spill");
              const bytes = g.allocated;
              g.free(bytes);
              g.release(bytes);
            });
          }
          try {
            expect(await reader.makeRoom?.(size * MiB)).toBe(true);
            expect(reader.allocate(size * MiB)).toBe(true);
            expect(asked).toBeGreaterThan(0);
            if (asked > 1) reach.reach("multiple spills");
            expect(gov.usedBytes).toBe(reader.held + grants.reduce((n, g) => n + g.held, 0));
            expect(gov.usedBytes).toBeLessThanOrEqual(gov.capBytes - gov.reserveBytes);
            reach.reach("exact accounting");
          } finally {
            reader.close();
            for (const g of grants) g.close();
            cache.unregister();
            expect(gov.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 50 },
    );
    reach.assertReached();
  },
);

it("returns false only after eligible holders are exhausted, without reclaiming pinned state", async () => {
  const gov = new Governor(8 * MiB),
    pinned = await gov.admit({ label: "pinned", minimumBytes: 7 * MiB, spillable: false }),
    reader = await gov.admit({ label: "reader", minimumBytes: 0, spillable: true });
  let asked = 0;
  pinned.onReclaimRequest(() => {
    asked++;
  });
  expect(await reader.makeRoom?.(1)).toBe(false);
  expect(asked).toBe(0);
  expect(gov.usedBytes).toBe(7 * MiB);
  reader.close();
  pinned.close();
  expect(gov.usedBytes).toBe(0);
});

it("shares an in-flight spill, and cancels one reader without cancelling the other", async () => {
  const gov = new Governor(8 * MiB),
    holder = await gov.admit({ label: "holder", minimumBytes: 7 * MiB, spillable: true }),
    a = await gov.admit({ label: "a", minimumBytes: 0, spillable: true }),
    b = await gov.admit({ label: "b", minimumBytes: 0, spillable: true });
  expect(holder.allocate(7 * MiB)).toBe(true);
  let finish!: () => void,
    calls = 0;
  const gate = new Promise<void>((resolve) => {
    finish = resolve;
  });
  holder.onReclaimRequest(async () => {
    calls++;
    await gate;
    holder.free(7 * MiB);
    holder.release(7 * MiB);
  });
  const cancel = new AbortController(),
    first = a.makeRoom?.(MiB, cancel.signal),
    second = b.makeRoom?.(MiB);
  const error = new Error("reader cancelled");
  cancel.abort(error);
  await expect(first).rejects.toBe(error);
  finish();
  expect(await second).toBe(true);
  expect(calls).toBe(1);
  a.close();
  b.close();
  holder.close();
  expect(gov.usedBytes).toBe(0);
});

it("propagates a spill failure rather than reporting memory refusal", async () => {
  const gov = new Governor(8 * MiB),
    holder = await gov.admit({ label: "holder", minimumBytes: 7 * MiB, spillable: true }),
    reader = await gov.admit({ label: "reader", minimumBytes: 0, spillable: true });
  const error = new Error("spill write failed");
  holder.onReclaimRequest(async () => {
    throw error;
  });
  await expect(reader.makeRoom?.(1)).rejects.toBe(error);
  reader.close();
  holder.close();
  expect(gov.usedBytes).toBe(0);
});

it("reserves a waiting read before another breaker can retake the released bytes", async () => {
  const gov = new Governor(8 * MiB),
    holder = await gov.admit({ label: "holder", minimumBytes: 7 * MiB, spillable: true }),
    reader = await gov.admit({ label: "reader", minimumBytes: 0, spillable: true }),
    competitor = await gov.admit({ label: "competitor", minimumBytes: 0, spillable: true });
  expect(holder.allocate(7 * MiB)).toBe(true);
  let stolen = false;
  holder.onReclaimRequest(() => {
    holder.free(7 * MiB);
    holder.release(7 * MiB);
    stolen = competitor.allocate(7 * MiB);
  });
  try {
    expect(await reader.makeRoom?.(MiB)).toBe(true);
    expect(stolen).toBe(false);
  } finally {
    reader.close();
    holder.close();
    competitor.close();
    expect(gov.usedBytes).toBe(0);
  }
});

it("re-evaluates free room when parallel column reads share a grant", async () => {
  const gov = new Governor(8 * MiB),
    holder = await gov.admit({ label: "holder", minimumBytes: 7 * MiB, spillable: true }),
    reader = await gov.admit({ label: "reader", minimumBytes: 0, spillable: true });
  expect(holder.allocate(7 * MiB)).toBe(true);
  let spills = 0;
  holder.onReclaimRequest(async () => {
    await Promise.resolve();
    if (spills++ === 0) {
      holder.free(MiB);
      holder.release(MiB);
    }
  });
  try {
    expect(await Promise.all([reader.makeRoom?.(MiB), reader.makeRoom?.(MiB)])).toEqual([
      true,
      true,
    ]);
    expect(reader.held).toBe(MiB);
    expect(spills).toBe(1);
  } finally {
    reader.close();
    holder.close();
    expect(gov.usedBytes).toBe(0);
  }
});
