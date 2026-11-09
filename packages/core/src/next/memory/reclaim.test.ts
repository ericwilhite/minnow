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

it.each(seedsFor("next-fresh-read-admission", [0xc41b, 0xc41c]))(
  "returns unused partial holdings before waiting for a complete fresh read (seed %i)",
  async (seed) => {
    const reach = createReach("fresh read admission", [
      "two",
      "more",
      "no-partial-wait",
      "completed",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 4 }),
        fc.integer({ min: 1, max: 1024 }),
        async (count, extra) => {
          reach.beginHistory();
          const part = MiB,
            gov = new Governor(count * part),
            grants = [];
          for (let k = 0; k < count; k++) {
            const parts = await gov.admitGroup({
              label: `fresh${String(k)}`,
              includesProgress: true,
              grants: [{ label: `fresh${String(k)}`, minimumBytes: part, spillable: false }],
            });
            const grant = parts[0];
            if (!grant) throw new Error("missing fresh read minimum");
            grants.push(grant);
          }
          expect(gov.usedBytes).toBe(gov.capBytes);
          let completed = 0;
          const jobs = grants.map(async (grant) => {
            expect(await grant.makeRoom?.(part + extra)).toBe(true);
            expect(grant.allocate(part + extra)).toBe(true);
            grant.free(part + extra);
            grant.release(grant.held);
            completed++;
          });
          void Promise.allSettled(jobs);
          try {
            for (let tick = 0; tick < 32; tick++) await Promise.resolve();
            expect(
              completed,
              "at least one fresh read must progress without a partial reservation cycle",
            ).toBeGreaterThan(0);
            reach.reach("no-partial-wait");
            await Promise.all(jobs);
            expect(completed).toBe(count);
            reach.reach("completed");
            reach.reach(count === 2 ? "two" : "more");
          } finally {
            for (const grant of grants) grant.close();
            await Promise.allSettled(jobs);
            expect(gov.usedBytes).toBe(0);
            reach.reach("released");
          }
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [2, 1],
          [4, 1024],
        ],
      },
    );
    reach.assertReached();
  },
);
