/**
 * A randomized state-machine model of the real PageCache's accounting (review B2-p2e-2, the
 * reviewer's model, committed and extended). A shadow tracks each entry generation's residency,
 * pins and descriptor, and the outstanding leases; after every operation the cache's charge must
 * equal the fixed slabs, plus base bytes and overhead of every unpinned resident entry, plus the
 * descriptor of every live entry (resident or pinned), and the lease budget the blob bytes of
 * the outstanding leases. Operations: cold and hot reads (ordinary and one-pass), several leases
 * on one entry, attach
 * pinned and unpinned (granted and refused, and refused for a copy of the entry's bytes), peek, last and non-last release (re-admission
 * granted and refused), full eviction, drop while pinned and unpinned, close with outstanding
 * leases, and two concurrent cold reads of one blob whose second platform read is held until the
 * first has attached and released (the interleaving the sequential model could not reach).
 */
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { StoreSession } from "./contract.js";
import { ENTRY_OVERHEAD, FIXED_CACHE_BYTES, PageCache } from "./page-cache.js";
import { countingBudget } from "./space-test-support.js";

const SEEDS = seedsFor("next-page-cache-model", [1, 0x5eed]);
const HISTORIES_PER_SEED = 50;
const STEPS = 200;
const DESCRIPTOR = 768;
const BLOB = 32;

const data = new Uint8Array(16_384).map((_, index) => index % 251);
const refs = Array.from({ length: 4 }, (_, index) => ({
  extent: { offset: index * 4096, length: 4096 },
  offset: 0,
  length: BLOB,
  checksum: xxHash32(data.slice(index * 4096, index * 4096 + BLOB)),
}));

interface Gate {
  calls: number;
  entered: () => void;
  open: Promise<void>;
}

function setup() {
  let gate: Gate | null = null;
  const session = {
    async readDataInto(at: number, out: Uint8Array): Promise<void> {
      if (gate !== null && ++gate.calls === 2) {
        gate.entered();
        await gate.open;
      }
      out.set(data.subarray(at, at + out.length));
    },
  } as unknown as StoreSession;
  const cache = new PageCache(session);
  const memory = {
    used: 0,
    deny: false,
    charge(bytes: number): boolean {
      if (memory.deny) return false;
      memory.used += bytes;
      return true;
    },
    release(bytes: number): void {
      memory.used -= bytes;
    },
  };
  cache.attach(memory);
  return {
    cache,
    memory,
    budget: countingBudget(Number.POSITIVE_INFINITY),
    arm: (): { entered: Promise<void>; unblock: () => void } => {
      let entered!: () => void;
      let unblock!: () => void;
      const reached = new Promise<void>((resolve) => (entered = resolve));
      const open = new Promise<void>((resolve) => (unblock = resolve));
      gate = { calls: 0, entered, open };
      return {
        entered: reached,
        unblock: () => {
          unblock();
          gate = null;
        },
      };
    },
  };
}

const STATES = [
  "read",
  "once-hit",
  "once-miss",
  "multiple-leases",
  "attach-pinned-true",
  "attach-pinned-false",
  "attach-unpinned-true",
  "attach-unpinned-false",
  "attach-foreign-false",
  "release-true",
  "release-false",
  "drop-pinned",
  "drop-unpinned",
  "drop-pinned-descriptor",
  "evict",
  "peek",
  "close-pinned-descriptor",
  "concurrent-cold-released-first",
  "concurrent-cold-held-first",
  "concurrent-cold-descriptor",
] as const;
const reach = createReach("next-page-cache-model", STATES);
afterAll(() => {
  reach.assertReached();
});

describe.each(SEEDS)("the page cache's accounting model (seed %i)", (seed) => {
  it("keeps every charge exact through random operation sequences", async () => {
    for (let history = 0; history < HISTORIES_PER_SEED; history++) {
      reach.beginHistory();
      let state = (seed * 1_000_003 + history * 7919) >>> 0 || 1;
      const random = (n: number): number => {
        state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
        return (state >>> 8) % n;
      };
      const { cache, memory, budget, arm } = setup();
      interface Shadow {
        id: number;
        pins: number;
        descriptor: number;
        resident: boolean;
      }
      const entries: Shadow[] = [];
      const leases: Array<{ entry: Shadow; release: () => void }> = [];
      let closed = false;
      const where = (): string => `seed ${String(seed)} history ${String(history)}`;
      /** `pending`: reads started (their budget charged at once) whose lease is not back yet. */
      const check = (pending = 0): void => {
        const want =
          (closed ? 0 : FIXED_CACHE_BYTES) +
          entries.reduce(
            (sum, entry) =>
              sum +
              (entry.resident || entry.pins > 0 ? entry.descriptor : 0) +
              (entry.resident && entry.pins === 0 ? BLOB + ENTRY_OVERHEAD : 0),
            0,
          );
        expect(memory.used, where()).toBe(want);
        expect(budget.used, where()).toBe((leases.length + pending) * BLOB);
      };
      for (let step = 0; step < STEPS; step++) {
        const id = random(4);
        const op = random(8);
        const ref = refs[id] ?? refs[0];
        if (ref === undefined) throw new Error("no ref");
        let entry = entries.find((each) => each.id === id && each.resident);
        if (op === 0) {
          // A third of the reads are one-pass (review B2-p2h #1): a hit pins the entry where it
          // is listed, a miss stays private to its lease and is never resident.
          const once = random(3) === 0;
          const lease = await cache.read([ref], { budget, once });
          if (once) reach.reach(entry === undefined ? "once-miss" : "once-hit");
          if (entry === undefined) {
            entry = { id, pins: 0, descriptor: 0, resident: !once };
            entries.push(entry);
          }
          entry.pins++;
          if (entry.pins > 1) reach.reach("multiple-leases");
          leases.push({ entry, release: () => lease.release() });
          reach.reach("read");
        } else if (op === 1) {
          memory.deny = random(2) === 0;
          // A descriptor of the entry's own bytes, or of an equal private copy (a one-pass
          // read's, review B2-p2g #3), which must never ride on the entry.
          const foreign = random(4) === 0;
          const own = cache.peek(ref, { once: true });
          const of = foreign || own === null ? data.slice(id * 4096, id * 4096 + BLOB) : own;
          const ok = cache.attachParsed(ref, {}, DESCRIPTOR, of);
          expect(ok, where()).toBe(entry?.descriptor === 0 && !memory.deny && !foreign);
          if (ok && entry !== undefined) entry.descriptor = DESCRIPTOR;
          if (entry !== undefined && foreign) reach.reach("attach-foreign-false");
          else if (entry !== undefined)
            reach.reach(
              `attach-${entry.pins > 0 ? "pinned" : "unpinned"}-${String(ok)}` as (typeof STATES)[number],
            );
          memory.deny = false;
        } else if (op === 2 && leases.length > 0) {
          const lease = leases.splice(random(leases.length), 1)[0];
          if (lease === undefined) throw new Error("no lease");
          memory.deny = random(2) === 0;
          // A refused re-admission may recycle probation: evict the unpinned first so refusal is
          // unambiguous.
          if (memory.deny) {
            cache.evict(Number.POSITIVE_INFINITY);
            for (const each of entries)
              if (each.pins === 0) {
                each.resident = false;
                each.descriptor = 0;
              }
          }
          lease.release();
          lease.release(); // idempotent
          lease.entry.pins--;
          if (lease.entry.pins === 0 && (memory.deny || !lease.entry.resident)) {
            lease.entry.resident = false;
            lease.entry.descriptor = 0;
          }
          reach.reach(`release-${String(memory.deny)}` as (typeof STATES)[number]);
          memory.deny = false;
        } else if (op === 3) {
          cache.dropExtent(ref.extent.offset);
          if (entry !== undefined) {
            if (entry.pins > 0 && entry.descriptor > 0) reach.reach("drop-pinned-descriptor");
            reach.reach(entry.pins > 0 ? "drop-pinned" : "drop-unpinned");
            entry.resident = false;
            if (entry.pins === 0) entry.descriptor = 0;
          }
        } else if (op === 4) {
          cache.evict(Number.POSITIVE_INFINITY);
          for (const each of entries)
            if (each.pins === 0) {
              each.resident = false;
              each.descriptor = 0;
            }
          reach.reach("evict");
        } else if (op === 5) {
          expect(cache.peek(ref) !== null, where()).toBe(entry !== undefined);
          reach.reach("peek");
        } else if (op === 6 && entry === undefined) {
          // Two cold reads of one absent blob; the second's platform read waits until the first
          // has (maybe) attached a descriptor and (maybe) released its lease.
          const gate = arm();
          const one = cache.read([ref], { budget });
          const two = cache.read([ref], { budget });
          const first = await one;
          await gate.entered;
          const fresh: Shadow = { id, pins: 1, descriptor: 0, resident: true };
          entries.push(fresh);
          if (random(2) === 0 && cache.attachParsed(ref, {}, DESCRIPTOR, first.bytes[0] ?? data)) {
            fresh.descriptor = DESCRIPTOR;
            reach.reach("concurrent-cold-descriptor");
          }
          const releaseFirst = random(2) === 0;
          if (releaseFirst) {
            first.release();
            fresh.pins--;
            reach.reach("concurrent-cold-released-first");
          } else {
            leases.push({ entry: fresh, release: () => first.release() });
            reach.reach("concurrent-cold-held-first");
          }
          check(1);
          gate.unblock();
          const second = await two;
          fresh.pins++;
          leases.push({ entry: fresh, release: () => second.release() });
        }
        check();
      }
      if (entries.some((each) => each.pins > 0 && each.descriptor > 0))
        reach.reach("close-pinned-descriptor");
      cache.close();
      closed = true;
      for (const each of entries) {
        each.resident = false;
        if (each.pins === 0) each.descriptor = 0;
      }
      check();
      while (leases.length > 0) {
        const lease = leases.pop();
        if (lease === undefined) break;
        lease.release();
        lease.entry.pins--;
        if (lease.entry.pins === 0) lease.entry.descriptor = 0;
        check();
      }
      expect(memory.used, where()).toBe(0);
    }
  }, 120_000);
});

describe("the page cache's accounting: the reviewer's race", () => {
  it("keeps a descriptor charged when a concurrent cold read pins the entry the first one admitted", async () => {
    const { cache, memory, budget, arm } = setup();
    const ref = refs[0];
    if (ref === undefined) throw new Error("no ref");
    const gate = arm();
    const one = cache.read([ref], { budget });
    const two = cache.read([ref], { budget });
    const first = await one;
    await gate.entered;
    expect(cache.attachParsed(ref, {}, DESCRIPTOR, first.bytes[0] ?? data)).toBe(true);
    first.release();
    gate.unblock();
    const second = await two;
    expect(cache.parsed(ref)).not.toBeNull();
    expect(memory.used).toBe(FIXED_CACHE_BYTES + DESCRIPTOR);
    second.release();
    expect(memory.used).toBe(FIXED_CACHE_BYTES + DESCRIPTOR + BLOB + ENTRY_OVERHEAD);
    cache.close();
    expect(memory.used).toBe(0);
  });
});
