/**
 * The blob cache's eviction order under one-pass pins (review B2-p2h #2): parsed directories and
 * inflated pages a one-pass scan reuses. Law (format/blob-cache.ts, ChunkReadOptions.once): a
 * one-pass pin is not a use; while every pin of an entry is one, it keeps its place and its
 * `usedAt`, and an ordinary pin meanwhile counts as usual.
 *
 * Metamorphic oracle, as storage/page-cache-policy.test.ts: twin caches receive the same
 * ordinary pins, unpins and evictions; one also receives one-pass pins, held or not. With no
 * one-pass pin outstanding they hold the same entries after every eviction and drain in the same
 * order; and the accounting (evictable bytes, cache charge) agrees. The reader's use of it (a
 * one-pass reader's directory and inflated pins) is in page/review-p2h.test.ts.
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { BlobRef } from "../storage/extents.js";
import { BlobCache } from "./blob-cache.js";

const SEEDS = seedsFor("next-blob-cache-policy", [3, 0xc0de]);
const HISTORIES_PER_SEED = 60;
const STEPS = 160;
const ENTRIES = 6;
const SIZE = 10;

const refs: BlobRef[] = Array.from({ length: ENTRIES }, (_, index) => ({
  extent: { offset: index * 4096, length: 4096 },
  offset: 0,
  length: SIZE,
  checksum: index,
}));

function refAt(id: number): BlobRef {
  const ref = refs[id];
  if (ref === undefined) throw new Error(`no ref ${String(id)}`);
  return ref;
}

function twin() {
  const memory = { used: 0 };
  const cache = new BlobCache<number>(() => ({
    charge: (bytes: number) => ((memory.used += bytes), true),
    release: (bytes: number) => void (memory.used -= bytes),
  }));
  return { cache, memory };
}

const STATES = [
  "once pin of an unpinned entry",
  "ordinary pin during a once pin",
  "ordinary unpin while a once pin remains",
  "eviction",
] as const;
const reach = createReach("next-blob-cache-policy", STATES);
afterAll(() => {
  reach.assertReached();
});

describe.each(SEEDS)("one-pass pins leave the blob cache's order alone (seed %i)", (seed) => {
  it("twin caches keep and evict the same entries with and without one-pass pins", () => {
    for (let history = 0; history < HISTORIES_PER_SEED; history++) {
      reach.beginHistory();
      let state = (seed * 1_000_003 + history * 7919) >>> 0 || 1;
      const random = (n: number): number => {
        state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
        return (state >>> 8) % n;
      };
      const a = twin();
      const b = twin();
      const ordinary: number[] = [];
      const once: number[] = [];
      const where = (step: number) =>
        `seed ${String(seed)} history ${String(history)} step ${String(step)}`;
      const present = (cache: BlobCache<number>): boolean[] =>
        refs.map((ref) => {
          const hit = cache.pin(ref, { once: true });
          if (hit !== null) cache.unpin(ref, { once: true });
          return hit !== null;
        });
      for (let step = 0; step < STEPS; step++) {
        const id = random(ENTRIES);
        const ref = refAt(id);
        const op = random(7);
        if (op === 0) {
          // An ordinary use: pin (admit on a miss) and keep it.
          if (once.includes(id)) reach.reach("ordinary pin during a once pin");
          for (const { cache } of [a, b]) if (cache.pin(ref) === null) cache.admit(ref, id, SIZE);
          ordinary.push(id);
        } else if (op === 1 && ordinary.length > 0) {
          const [gone] = ordinary.splice(random(ordinary.length), 1);
          if (gone === undefined) continue;
          const goneRef = refAt(gone);
          if (once.includes(gone) && !ordinary.includes(gone))
            reach.reach("ordinary unpin while a once pin remains");
          a.cache.unpin(goneRef);
          b.cache.unpin(goneRef);
        } else if (op === 2 || op === 3) {
          const hit = a.cache.pin(ref, { once: true });
          if (hit === null) continue;
          if (!ordinary.includes(id) && !once.includes(id))
            reach.reach("once pin of an unpinned entry");
          if (op === 2) a.cache.unpin(ref, { once: true });
          else once.push(id);
        } else if (op === 4 && once.length > 0) {
          const [gone] = once.splice(random(once.length), 1);
          if (gone !== undefined) a.cache.unpin(refAt(gone), { once: true });
        } else if (op >= 5 && once.length === 0) {
          const bytes = (1 + random(2)) * SIZE;
          a.cache.evict(bytes);
          b.cache.evict(bytes);
          reach.reach("eviction");
          expect(present(a.cache), where(step)).toEqual(present(b.cache));
          expect([a.cache.evictableBytes, a.memory.used], where(step)).toEqual([
            b.cache.evictableBytes,
            b.memory.used,
          ]);
        }
      }
      for (const id of once.splice(0)) a.cache.unpin(refAt(id), { once: true });
      for (const id of ordinary.splice(0)) {
        const ref = refAt(id);
        a.cache.unpin(ref);
        b.cache.unpin(ref);
      }
      for (let left = ENTRIES; left > 0; left--) {
        a.cache.evict(1);
        b.cache.evict(1);
        expect(present(a.cache), where(STEPS)).toEqual(present(b.cache));
      }
      expect([a.memory.used, a.cache.evictableBytes]).toEqual([0, 0]);
    }
  });
});
