/** Approved lossy-map law: allocate only tracked holes or EOF; forgotten holes are wasted, never reused. */
import { expect, it, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, BOUNDED_FREE_ARENA_REACH } from "../../testing/reach.js";
import {
  BoundedFreeSpaceMap,
  MIN_RUN_ARENA_BYTES,
  MAX_RUN_ARENA_BYTES,
  runArenaBytes,
} from "./bounded-free-space.js";
import { countingBudget } from "./space-test-support.js";
import { ReadDeniedError } from "./extents.js";

const reach = createReach("bounded free arena", BOUNDED_FREE_ARENA_REACH);
for (const seed of seedsFor("bounded-free-space", [1501, 1502, 1503, 1504]))
  it(`never allocates forgotten or live cells (${String(seed)})`, () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      budget = countingBudget(MIN_RUN_ARENA_BYTES);
    const map = BoundedFreeSpaceMap.create(64, MIN_RUN_ARENA_BYTES, budget);
    const cells = map.capacity * 3 + 7;
    const live = new Set(Array.from({ length: cells }, (_, i) => i));
    expect(map.allocate(cells * 64)).toBe(0);
    const check = () => {
      const tracked = new Set<number>();
      let previousEnd = 0;
      for (let at = 0; at < map.runCount; at++) {
        const run = { offset: map.runOffset(at), length: map.runLength(at) };
        if (run.offset < previousEnd) throw new Error("Tracked runs regress or overlap");
        previousEnd = run.offset + run.length;
        for (let i = run.offset / 64; i < previousEnd / 64; i++) {
          if (live.has(i) || tracked.has(i)) throw new Error("Tracked bytes are live or repeated");
          tracked.add(i);
        }
      }
      let free = 0;
      for (let i = 0; i < map.end / 64; i++) if (!live.has(i)) free++;
      expect(map.freeBytes + map.untrackedFreeBytes).toBe(free * 64);
      expect(map.runCount).toBeLessThanOrEqual(map.capacity);
      expect(budget.used).toBe(MIN_RUN_ARENA_BYTES);
      return tracked;
    };
    try {
      for (let i = 1; i < cells - 1; i += 2) {
        live.delete(i);
        map.releaseKnownAllocated(i * 64, 64);
      }
      expect(map.untrackedFreeBytes).toBeGreaterThan(0);
      reach.reach("overflow");
      const tracked = check();
      const forgotten = Array.from({ length: cells }, (_, i) => i).find(
        (i) => !live.has(i) && !tracked.has(i),
      );
      if (forgotten === undefined) throw new Error("No forgotten generated hole");
      expect(map.allocateAt(forgotten * 64, 64)).toBe(false);
      expect(map.allocationState(forgotten * 64, 64)).toBe("unknown");
      reach.reach("unknown");
      for (let step = 0; step < 96; step++) {
        const size = 1 + Math.floor(random() * 4);
        const expected = map.runs().find((run) => run.length >= size * 64)?.offset ?? map.end;
        const at = map.allocate(size * 64);
        expect(at).toBe(expected);
        for (let i = at / 64; i < at / 64 + size; i++) {
          if (live.has(i) || (i < cells && !tracked.has(i)))
            throw new Error("Allocated live or untracked bytes");
          live.add(i);
        }
        reach.reach("tracked first fit");
        check();
      }
      // Trusted release proofs come from live ownership, never from the lossy complement.
      for (const i of [...live].sort((a, b) => a - b)) {
        map.releaseKnownAllocated(i * 64, 64);
        live.delete(i);
      }
      reach.reach("coalesce");
      check();
      // Reopen from empty kept-root reachability: all old holes, including the forgotten ones,
      // are now implicit EOF space. With unchanged extreme fragmentation debt could remain.
      map.close();
      const reopened = BoundedFreeSpaceMap.create(64, MIN_RUN_ARENA_BYTES, budget);
      try {
        expect(reopened.untrackedFreeBytes).toBe(0);
        expect(reopened.allocate(cells * 64)).toBe(0);
        reach.reach("reopen");
      } finally {
        reopened.close();
      }
    } finally {
      map.close();
    }
    expect(budget.used).toBe(0);
    reach.reach("closed");
  });
it("reaches lossy-map histories", () => reach.assertReached());

it("admits the entire arena before allocation and never allocates backing during mutation", () => {
  const budget = countingBudget(MIN_RUN_ARENA_BYTES);
  const Native = Float64Array;
  let allocations = 0;
  const spy = vi.spyOn(globalThis, "Float64Array").mockImplementation(function (
    this: unknown,
    n: number | ArrayBufferLike,
  ) {
    if (typeof n !== "number") throw new Error("Unexpected constructor");
    allocations++;
    expect(budget.used).toBeGreaterThanOrEqual(n * 8 + 320);
    return new Native(n);
  } as unknown as typeof Float64Array);
  try {
    expect(() =>
      BoundedFreeSpaceMap.create(64, MIN_RUN_ARENA_BYTES, countingBudget(MIN_RUN_ARENA_BYTES - 1)),
    ).toThrow(ReadDeniedError);
    expect(allocations).toBe(0);
    const map = BoundedFreeSpaceMap.create(64, MIN_RUN_ARENA_BYTES, budget);
    try {
      map.allocate((map.capacity * 3 + 1) * 128);
      for (let i = 0; i < map.capacity * 3; i++) map.releaseKnownAllocated(i * 128 + 64, 64);
      expect(allocations).toBe(1);
      expect(map.untrackedFreeBytes).toBeGreaterThan(0);
    } finally {
      map.close();
    }
    expect(budget.used).toBe(0);
  } finally {
    spy.mockRestore();
  }
});

it("keeps larger holes, handles an interior split at capacity, and needs no slot for a tail", () => {
  const budget = countingBudget(MIN_RUN_ARENA_BYTES);
  const map = BoundedFreeSpaceMap.create(64, MIN_RUN_ARENA_BYTES, budget);
  try {
    const total = (map.capacity * 2 + 40) * 64;
    map.allocate(total);
    for (let i = 0; i < map.capacity; i++) map.releaseKnownAllocated((i * 2 + 1) * 64, 64);
    const large = (map.capacity * 2 + 2) * 64;
    map.releaseKnownAllocated(large, 16 * 64);
    expect(map.untrackedFreeBytes).toBe(64);
    expect(map.allocateAt(64, 64)).toBe(false);
    expect(map.largestRun).toBe(16 * 64);
    expect(map.allocateAt(large + 128, 64)).toBe(true);
    expect(map.untrackedFreeBytes).toBe(128);
    expect(map.runCount).toBe(map.capacity);
    const lost = map.untrackedFreeBytes;
    map.releaseKnownAllocated(total - 64, 64);
    expect(map.end).toBe(total - 64);
    expect(map.untrackedFreeBytes).toBe(lost);
    expect(budget.used).toBe(MIN_RUN_ARENA_BYTES);
  } finally {
    map.close();
  }
  expect(budget.used).toBe(0);
});

it("sizes once from the run count with bounded headroom", () => {
  expect(runArenaBytes(0)).toBe(MIN_RUN_ARENA_BYTES);
  expect(runArenaBytes(1024)).toBeGreaterThanOrEqual(2 * 1024 * 16 + 320);
  expect(runArenaBytes(Number.MAX_SAFE_INTEGER)).toBe(MAX_RUN_ARENA_BYTES);
  expect(() => runArenaBytes(-1)).toThrow(RangeError);
});
