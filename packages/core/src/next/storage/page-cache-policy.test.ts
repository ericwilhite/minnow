/**
 * The page cache's eviction policy under one-pass reads (review B2-p2h #1). Law
 * (storage/extents.ts `ReadOptions.once` / `PeekOptions.once`): a one-pass scan's hits are not
 * uses. Its pin must leave the entry's segment and recency exactly as they were, while an
 * ordinary use during that pin still counts.
 *
 * Metamorphic oracle: twin caches over the same blobs receive the same ordinary operations
 * (reads, holds, peeks, evictions); one of them also receives one-pass reads, held or not, at
 * random points, overlapping ordinary uses of the same blob. With no one-pass pin outstanding,
 * the two must keep the same blobs through every eviction, and evict in the same order when
 * drained. (Eviction runs only then: a pinned entry is never evicted, so the twins could not
 * agree on it.) The accounting itself is page-cache-model.test.ts's.
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { StoreSession } from "./contract.js";
import type { ReadLease } from "./extents.js";
import { ENTRY_OVERHEAD, PageCache } from "./page-cache.js";
import { countingBudget } from "./space-test-support.js";

const SEEDS = seedsFor("next-page-cache-policy", [7, 0xbeef]);
const HISTORIES_PER_SEED = 60;
const STEPS = 160;
const BLOB = 32;
const BLOBS = 6;

const data = new Uint8Array(BLOBS * 4096).map((_, index) => (index * 7) % 251);
const refs = Array.from({ length: BLOBS }, (_, index) => ({
  extent: { offset: index * 4096, length: 4096 },
  offset: 0,
  length: BLOB,
  checksum: xxHash32(data.slice(index * 4096, index * 4096 + BLOB)),
}));

function cache(): PageCache {
  const session = {
    readDataInto(at: number, out: Uint8Array): Promise<void> {
      out.set(data.subarray(at, at + out.length));
      return Promise.resolve();
    },
  } as unknown as StoreSession;
  const pages = new PageCache(session);
  pages.attach({ charge: () => true, release: () => undefined });
  return pages;
}

const STATES = [
  "once hit of a reused blob",
  "once hit of a blob used once",
  "ordinary peek during a once pin",
  "ordinary read during a once pin",
  "ordinary hold released while a once pin remains",
  "once pin released after an ordinary use",
  "eviction",
] as const;
const reach = createReach("next-page-cache-policy", STATES);
afterAll(() => {
  reach.assertReached();
});

const resident = (pages: PageCache): boolean[] =>
  refs.map((ref) => pages.peek(ref, { once: true }) !== null);

describe.each(SEEDS)("one-pass reads leave the eviction order alone (seed %i)", (seed) => {
  it("twin caches keep and evict the same blobs with and without one-pass reads", async () => {
    for (let history = 0; history < HISTORIES_PER_SEED; history++) {
      reach.beginHistory();
      let state = (seed * 1_000_003 + history * 7919) >>> 0 || 1;
      const random = (n: number): number => {
        state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
        return (state >>> 8) % n;
      };
      const budget = countingBudget(Number.POSITIVE_INFINITY);
      const scanned = cache();
      const plain = cache();
      const holds: Array<{ id: number; scanned: ReadLease; plain: ReadLease }> = [];
      const onceHolds: Array<{ id: number; lease: ReadLease; usedSince: boolean }> = [];
      /** Ordinary uses per blob since it last entered (the test's view of "reused"). */
      const uses = refs.map(() => 0);
      const where = (step: number): string =>
        `seed ${String(seed)} history ${String(history)} step ${String(step)}`;
      const onceHeld = (id: number) => onceHolds.filter((hold) => hold.id === id);
      const markUsed = (id: number): void => {
        for (const hold of onceHeld(id)) hold.usedSince = true;
        uses[id] = (uses[id] ?? 0) + 1;
      };
      for (let step = 0; step < STEPS; step++) {
        const id = random(BLOBS);
        const ref = refs[id] ?? refs[0];
        if (ref === undefined) throw new Error("no ref");
        const op = random(9);
        if (op === 0) {
          const a = await scanned.read([ref], { budget });
          const b = await plain.read([ref], { budget });
          if (onceHeld(id).length > 0) reach.reach("ordinary read during a once pin");
          markUsed(id);
          a.release();
          b.release();
        } else if (op === 1) {
          if (onceHeld(id).length > 0) reach.reach("ordinary peek during a once pin");
          if (scanned.peek(ref) !== null) markUsed(id);
          plain.peek(ref);
        } else if (op === 2) {
          if (onceHeld(id).length > 0) reach.reach("ordinary read during a once pin");
          holds.push({
            id,
            scanned: await scanned.read([ref], { budget }),
            plain: await plain.read([ref], { budget }),
          });
          markUsed(id);
        } else if (op === 3 && holds.length > 0) {
          const [hold] = holds.splice(random(holds.length), 1);
          if (hold === undefined) continue;
          if (onceHeld(hold.id).length > 0 && !holds.some((other) => other.id === hold.id))
            reach.reach("ordinary hold released while a once pin remains");
          hold.scanned.release();
          hold.plain.release();
        } else if (op === 4 || op === 5) {
          const was = scanned.peek(ref, { once: true }) !== null;
          if (was)
            reach.reach(
              (uses[id] ?? 0) > 1 ? "once hit of a reused blob" : "once hit of a blob used once",
            );
          const lease = await scanned.read([ref], { budget, once: true });
          if (op === 4) lease.release();
          else onceHolds.push({ id, lease, usedSince: false });
        } else if (op === 6 && onceHolds.length > 0) {
          const [hold] = onceHolds.splice(random(onceHolds.length), 1);
          if (hold?.usedSince === true) reach.reach("once pin released after an ordinary use");
          hold?.lease.release();
        } else if ((op === 7 || op === 8) && onceHolds.length === 0) {
          const bytes = (1 + random(2)) * (BLOB + ENTRY_OVERHEAD);
          scanned.evict(bytes);
          plain.evict(bytes);
          reach.reach("eviction");
          expect(resident(scanned), where(step)).toEqual(resident(plain));
          refs.forEach((each, k) => {
            if (plain.peek(each, { once: true }) === null) uses[k] = 0;
          });
        }
      }
      for (const hold of onceHolds.splice(0)) hold.lease.release();
      for (const hold of holds.splice(0)) {
        hold.scanned.release();
        hold.plain.release();
      }
      // Drained one entry at a time: the same blobs go in the same order.
      for (let left = BLOBS; left > 0; left--) {
        scanned.evict(1);
        plain.evict(1);
        expect(resident(scanned), where(STEPS)).toEqual(resident(plain));
      }
      expect(budget.used).toBe(0);
    }
  }, 120_000);
});

describe("one-pass reads leave the eviction order alone: the reviewer's cases", () => {
  it("a once read of a demoted, formerly hot blob does not promote it again (the probe, inverted)", async () => {
    const pages = cache();
    const budget = countingBudget(Number.POSITIVE_INFINITY);
    for (const ref of refs.slice(0, 4)) (await pages.read([ref], { budget })).release();
    for (const ref of refs.slice(0, 4)) pages.peek(ref); // A demoted by the share; B–D protected
    expect(pages.stats.protectedBytes).toBe(3 * BLOB);
    const [a, b] = [refs[0], refs[1]];
    if (a === undefined || b === undefined) throw new Error("no refs");
    (await pages.read([a], { budget, once: true })).release();
    expect(pages.stats.protectedBytes).toBe(3 * BLOB);
    pages.evict(1);
    expect(pages.peek(a, { once: true })).toBeNull();
    expect(pages.peek(b, { once: true })).not.toBeNull();
  });

  it("an ordinary use during a once pin still counts", async () => {
    const pages = cache();
    const budget = countingBudget(Number.POSITIVE_INFINITY);
    const [a, b] = [refs[0], refs[1]];
    if (a === undefined || b === undefined) throw new Error("no refs");
    (await pages.read([a], { budget })).release();
    (await pages.read([b], { budget })).release(); // probation: b, a (a the older)
    const held = await pages.read([a], { budget, once: true });
    pages.peek(a); // a use: a is promoted to protected
    held.release();
    expect(pages.stats.protectedBytes).toBe(BLOB);
    pages.evict(1); // probation first: b, not a
    expect([
      pages.peek(a, { once: true }) === null,
      pages.peek(b, { once: true }) === null,
    ]).toEqual([false, true]);
  });
});
