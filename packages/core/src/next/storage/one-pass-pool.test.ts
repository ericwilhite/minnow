/**
 * The one-pass buffer pool (increment 10, the lead's ruling: reuse a small set of buffers once
 * their last view is released, charged as one pooled reservation, so a scan's RSS does not
 * grow with garbage waiting for the collector). A one-pass read with `reuseBuffers` packs its
 * slabs into one pooled buffer of a power-of-two size, charged whole to its lease; on release the
 * buffer returns to the pool, charged to the cache while idle (at most `POOL_IDLE_BYTES`, evicted
 * first, dropped when the governor refuses), and in test builds it is poisoned, so a stale view
 * reads garbage.
 *
 * Law: ReadOptions.reuseBuffers (storage/extents.ts): a lease's bytes are its own until release
 * (never reused while it holds them), its budget pays for the whole buffer, the cache for idle
 * ones, and nothing else changes. The generated property drives random holds and releases,
 * evictions and refusals, and checks every held lease's bytes and every charge after each step.
 * A reader's use of it (its dictionary is never pooled) is in page/export-path.test.ts.
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { StoreSession } from "./contract.js";
import type { BlobRef, ReadLease } from "./extents.js";
import { FIXED_CACHE_BYTES, PageCache, POOL_IDLE_BYTES, POOL_MIN_BUFFER } from "./page-cache.js";
import { countingBudget } from "./space-test-support.js";

const BLOBS = 64;

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("expected a value");
  return value;
}
const data = new Uint8Array(BLOBS * 65_536).map((_, index) => (index * 31 + 7) % 251);
/** Blob k: `lengths[k]` bytes at the start of a 64 KiB stretch (so blobs never share a slab). */
const lengths = Array.from({ length: BLOBS }, (_, k) => 8 * (100 + ((k * 977) % 7000)));
const refs: BlobRef[] = lengths.map((length, k) => ({
  extent: { offset: k * 65_536, length: 65_536 },
  offset: 0,
  length,
  checksum: xxHash32(data.subarray(k * 65_536, k * 65_536 + length)),
}));

function cache(deny: () => boolean = () => false, poolIdleBytes = POOL_IDLE_BYTES) {
  const session = {
    readDataInto(at: number, out: Uint8Array): Promise<void> {
      out.set(data.subarray(at, at + out.length));
      return Promise.resolve();
    },
  } as unknown as StoreSession;
  const pages = new PageCache(session, { poolIdleBytes });
  const memory = {
    used: 0,
    charge(bytes: number): boolean {
      if (deny()) return false;
      memory.used += bytes;
      return true;
    },
    release(bytes: number): void {
      memory.used -= bytes;
    },
  };
  pages.attach(memory);
  return { pages, memory };
}

const reused = { budget: countingBudget(Number.POSITIVE_INFINITY), once: true, reuseBuffers: true };

describe("the one-pass buffer pool", () => {
  it("reuses a released buffer, never a held one, and leaves reads without the option alone", async () => {
    const { pages } = cache();
    const budget = countingBudget(Number.POSITIVE_INFINITY);
    const first = await pages.read([defined(refs[0])], { budget, once: true, reuseBuffers: true });
    const buffer = defined(first.bytes[0]).buffer;
    const second = await pages.read([defined(refs[1])], { budget, once: true, reuseBuffers: true });
    expect(defined(second.bytes[0]).buffer).not.toBe(buffer); // held: not reused
    first.release();
    expect(pages.stats.poolBytes).toBe(POOL_MIN_BUFFER);
    const third = await pages.read([defined(refs[2])], { budget, once: true, reuseBuffers: true });
    expect(defined(third.bytes[0]).buffer).toBe(buffer); // released: reused
    expect(pages.stats.poolBytes).toBe(0);
    const plain = await pages.read([defined(refs[3])], { budget, once: true });
    plain.release();
    expect(pages.stats.poolBytes).toBe(0); // fresh buffers never enter the pool
    second.release();
    third.release();
    expect(budget.used).toBe(0);
  });

  it("charges the lease for its whole buffer, the cache for idle ones, and evicts idle ones first", async () => {
    const { pages, memory } = cache();
    const budget = countingBudget(Number.POSITIVE_INFINITY);
    const lease = await pages.read([defined(refs[4])], { budget, once: true, reuseBuffers: true });
    expect(budget.used).toBe(POOL_MIN_BUFFER);
    expect(memory.used).toBe(FIXED_CACHE_BYTES);
    lease.release();
    expect(budget.used).toBe(0);
    expect(memory.used).toBe(FIXED_CACHE_BYTES + POOL_MIN_BUFFER);
    expect(pages.evictableBytes).toBe(POOL_MIN_BUFFER);
    expect(pages.oldestUnusedAt).toBe(0); // the governor takes it first
    expect(pages.evict(1)).toBe(POOL_MIN_BUFFER);
    expect([memory.used, pages.stats.poolBytes]).toEqual([FIXED_CACHE_BYTES, 0]);
  });

  it("drops a released buffer when the governor refuses, and empties the pool at close", async () => {
    let deny = false;
    const { pages, memory } = cache(() => deny);
    const budget = countingBudget(Number.POSITIVE_INFINITY);
    const a = await pages.read([defined(refs[5])], { budget, once: true, reuseBuffers: true });
    const b = await pages.read([defined(refs[6])], { budget, once: true, reuseBuffers: true });
    deny = true;
    a.release();
    expect(pages.stats.poolBytes).toBe(0);
    deny = false;
    b.release();
    expect(pages.stats.poolBytes).toBe(POOL_MIN_BUFFER);
    pages.close();
    expect(memory.used).toBe(0);
  });

  it("poisons a released buffer in test builds: a stale view reads garbage", async () => {
    const { pages } = cache();
    const lease = await pages.read([defined(refs[7])], reused);
    const view = defined(lease.bytes[0]);
    expect(xxHash32(view)).toBe(defined(refs[7]).checksum);
    lease.release();
    expect(view.every((byte) => byte === 0xa5)).toBe(true);
  });
});

const SEEDS = seedsFor("next-one-pass-pool", [0xb00f, 0x42]);
const reach = createReach("next-one-pass-pool", [
  "a pooled buffer reused",
  "a new buffer while others are held",
  "a larger size class",
  "a released buffer dropped (pool full)",
  "a released buffer dropped (governor refused)",
  "idle buffers evicted",
  "a read without reuse",
]);
afterAll(() => {
  reach.assertReached();
});

describe.each(SEEDS)("the one-pass buffer pool under random use (seed %i)", (seed) => {
  it("never reuses a held buffer and keeps every charge exact", async () => {
    for (let history = 0; history < 40; history++) {
      reach.beginHistory();
      let state = (seed * 1_000_003 + history * 7919) >>> 0 || 1;
      const random = (n: number): number => {
        state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
        return (state >>> 8) % n;
      };
      let deny = false;
      // A small pool, so it fills: at most four 64 KiB buffers idle.
      const limit = 4 * POOL_MIN_BUFFER;
      const { pages, memory } = cache(() => deny, limit);
      const budget = countingBudget(Number.POSITIVE_INFINITY);
      const held: Array<{ lease: ReadLease; ids: number[]; charged: number }> = [];
      const where = (step: number) =>
        `seed ${String(seed)} history ${String(history)} step ${String(step)}`;
      for (let step = 0; step < 120; step++) {
        const op = random(10);
        if (op < 5) {
          // A read of 1–40 blobs (sometimes past the 64 KiB class), pooled or not.
          const count = 1 + random(op === 4 ? 40 : 4);
          const ids = Array.from({ length: count }, () => random(BLOBS));
          const reuse = random(5) !== 0;
          if (!reuse) reach.reach("a read without reuse");
          const poolBefore = pages.stats.poolBytes;
          const usedBefore = budget.used;
          const lease = await pages.read(
            ids.map((id) => defined(refs[id])),
            { budget, once: true, reuseBuffers: reuse },
          );
          const charged = budget.used - usedBefore;
          if (reuse) {
            const size = defined(lease.bytes[0]).buffer.byteLength;
            if (size > POOL_MIN_BUFFER) reach.reach("a larger size class");
            if (pages.stats.poolBytes < poolBefore) reach.reach("a pooled buffer reused");
            else if (held.length > 0) reach.reach("a new buffer while others are held");
            expect(charged, where(step)).toBe(size);
          }
          held.push({ lease, ids, charged });
        } else if (op < 8 && held.length > 0) {
          const [gone] = held.splice(random(held.length), 1);
          if (gone === undefined) continue;
          deny = random(6) === 0;
          const poolBefore = pages.stats.poolBytes;
          gone.lease.release();
          const pooled = gone.lease.bytes[0]?.buffer.byteLength ?? 0;
          if (deny && pages.stats.poolBytes === poolBefore)
            reach.reach("a released buffer dropped (governor refused)");
          if (!deny && pooled > 0 && poolBefore + pooled > limit)
            reach.reach("a released buffer dropped (pool full)");
          deny = false;
        } else if (op === 8 && pages.stats.poolBytes > 0) {
          pages.evict(1 + random(2 * POOL_MIN_BUFFER));
          reach.reach("idle buffers evicted");
        }
        // Every held lease still has its own verified bytes: nothing it holds was reused.
        for (const { lease, ids } of held)
          lease.bytes.forEach((bytes, k) => {
            expect(xxHash32(bytes), where(step)).toBe(defined(refs[ids[k] ?? 0]).checksum);
          });
        // The leases pay for what they hold; the cache for the fixed slabs and idle buffers.
        expect(budget.used, where(step)).toBe(held.reduce((sum, { charged }) => sum + charged, 0));
        expect(memory.used, where(step)).toBe(FIXED_CACHE_BYTES + pages.stats.poolBytes);
        expect(pages.stats.poolBytes).toBeLessThanOrEqual(limit);
      }
      for (const { lease } of held.splice(0)) lease.release();
      pages.close();
      expect([budget.used, memory.used]).toEqual([0, 0]);
    }
  }, 120_000);
});
