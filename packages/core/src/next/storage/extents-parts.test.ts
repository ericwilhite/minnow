/**
 * The behaviour under the extent space's interface (extents.ts): the free map, two-generation
 * reclaim under pins, and read coalescing. The space itself arrives in phase 2.
 */
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { FreeSpaceMap } from "./free-space.js";
import { ReclaimLedger } from "./reclaim.js";
import { MAX_SLAB, MAX_SLAB_GAP, planSlabs } from "./slabs.js";

const A = 4096;

describe("free space map", () => {
  it("allocates lowest-address first fit, grows at the end, and rounds to the alignment", () => {
    const map = new FreeSpaceMap(A);
    expect(map.allocate(1)).toBe(0);
    expect(map.allocate(A + 1)).toBe(A);
    expect(map.allocate(A)).toBe(3 * A);
    expect(map.end).toBe(4 * A);
    map.free(A, A + 1);
    expect(map.runs()).toEqual([{ offset: A, length: 2 * A }]);
    expect(map.allocate(A)).toBe(A);
    expect(map.allocate(3 * A)).toBe(4 * A);
    expect(map.allocate(A)).toBe(2 * A);
    expect(map.freeBytes).toBe(0);
  });

  it("coalesces frees and returns a free tail by lowering the end", () => {
    const map = new FreeSpaceMap(A);
    for (let index = 0; index < 4; index++) map.allocate(A);
    map.free(A, A);
    map.free(3 * A, A);
    expect(map.end).toBe(3 * A);
    expect(map.runs()).toEqual([{ offset: A, length: A }]);
    map.free(2 * A, A);
    expect(map.end).toBe(A);
    expect(map.runCount).toBe(0);
  });

  it("refuses double frees, frees past the end and unaligned offsets", () => {
    const map = new FreeSpaceMap(A);
    map.allocate(4 * A);
    map.free(A, A);
    expect(() => map.free(A, A)).toThrow(/Double free/u);
    expect(() => map.free(0, 2 * A)).toThrow(/Double free/u);
    expect(() => map.free(4 * A, A)).toThrow(/past the end/u);
    expect(() => map.free(7, A)).toThrow(/Unaligned/u);
    expect(map.runs()).toEqual([{ offset: A, length: A }]);
  });

  it("places exactly where asked when that space is free, to extend a sequential run", () => {
    const map = new FreeSpaceMap(A);
    map.allocate(4 * A);
    map.free(A, 2 * A);
    expect(map.allocateAt(A, A)).toBe(true);
    expect(map.allocateAt(2 * A, 2 * A)).toBe(false);
    expect(map.allocateAt(2 * A, A)).toBe(true);
    expect(map.allocateAt(4 * A, A)).toBe(true);
    expect(map.allocateAt(7 * A, A)).toBe(true);
    expect(map.runs()).toEqual([{ offset: 5 * A, length: 2 * A }]);
    expect(map.end).toBe(8 * A);
  });

  it("is rebuilt from the extents a root reaches; shared extents count once, overlaps refuse", () => {
    const map = FreeSpaceMap.fromAllocated(A, 10 * A, [
      { offset: 5 * A, length: A },
      { offset: A, length: 10 },
      { offset: A, length: 10 },
      { offset: 2 * A, length: 2 * A },
    ]);
    expect(map.runs()).toEqual([
      { offset: 0, length: A },
      { offset: 4 * A, length: A },
    ]);
    expect(map.end).toBe(6 * A);
    expect(() =>
      FreeSpaceMap.fromAllocated(A, 10 * A, [
        { offset: 0, length: 2 * A },
        { offset: A, length: A },
      ]),
    ).toThrow(/overlaps/u);
    expect(() => FreeSpaceMap.fromAllocated(A, 2 * A, [{ offset: 2 * A, length: 1 }])).toThrow(
      /past the file/u,
    );
  });

  it.each(seedsFor("next-free-space", [0xf5ee, 7]))(
    "never hands out space twice, under random allocate/free (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      const map = new FreeSpaceMap(A);
      const live: Array<{ offset: number; length: number }> = [];
      for (let step = 0; step < 2_000; step++) {
        if (live.length > 0 && random() < 0.45) {
          const [extent] = live.splice(Math.floor(random() * live.length), 1);
          if (extent !== undefined) map.free(extent.offset, extent.length);
        } else {
          const length = 1 + Math.floor(random() * 6 * A);
          live.push({ offset: map.allocate(length), length });
        }
        const owned = live
          .map((extent) => ({
            start: extent.offset,
            end: extent.offset + map.sizeOf(extent.length),
          }))
          .concat(map.runs().map((run) => ({ start: run.offset, end: run.offset + run.length })))
          .sort((a, b) => a.start - b.start);
        // Live extents and free runs tile [0, end) exactly: no overlap, no leak.
        let cursor = 0;
        for (const range of owned) {
          expect(range.start).toBe(cursor);
          cursor = range.end;
        }
        expect(cursor).toBe(map.end);
      }
    },
  );
});

describe("reclaim ledger (two-generation reclaim, ruling R8)", () => {
  it("keeps retired space until the next publish is durable, even with no pins", () => {
    const freed: string[][] = [];
    const ledger = new ReclaimLedger<string>((items) => freed.push([...items]));
    expect(ledger.advance(["a"])).toBe(1);
    // Generation 0's root is still in the other slot: a reopen may fall back to it.
    expect(freed).toEqual([]);
    expect(ledger.waiting).toBe(1);
    expect(ledger.advance([])).toBe(2);
    expect(freed).toEqual([["a"]]);
  });

  it("also waits for every pin older than the retiring generation", () => {
    const freed: string[] = [];
    const ledger = new ReclaimLedger<string>((items) => freed.push(...items));
    const zero = ledger.pin("query 1");
    ledger.advance(["x"]);
    const one = ledger.pin("query 2");
    ledger.advance(["y"]);
    const two = ledger.pin("merge");
    ledger.advance(["z"]);
    ledger.advance([]);
    expect(ledger.oldestPinned).toBe(0);
    one.release();
    one.release();
    expect(freed).toEqual([]);
    zero.release();
    // x (retired by 1) and y (retired by 2) are free; z (retired by 3) waits for the pin of 2.
    expect(freed).toEqual(["x", "y"]);
    two.release();
    expect(freed).toEqual(["x", "y", "z"]);
    expect(ledger.pinCount).toBe(0);
    expect(ledger.waiting).toBe(0);
  });

  it("holds what only the previous root reaches at open until the first publish", () => {
    const freed: string[] = [];
    const ledger = new ReclaimLedger<string>((items) => freed.push(...items));
    ledger.retireAtOpen(["previous only"]);
    const pin = ledger.pin();
    expect(freed).toEqual([]);
    ledger.advance([]);
    // A pin of the current root never reaches the previous root's extents.
    expect(freed).toEqual(["previous only"]);
    pin.release();
    expect(() => ledger.retireAtOpen(["late"])).toThrow(TypeError);
  });

  it("reports the oldest live pin for backpressure", () => {
    let now = 1_000;
    const ledger = new ReclaimLedger<string>(
      () => undefined,
      () => now,
    );
    expect(ledger.oldestPin).toBeNull();
    const first = ledger.pin("slow reader");
    now = 1_500;
    ledger.advance([]);
    ledger.pin("fresh");
    now = 2_000;
    expect(ledger.oldestPin).toEqual({ label: "slow reader", ageMs: 1_000 });
    first.release();
    expect(ledger.oldestPin).toEqual({ label: "fresh", ageMs: 500 });
  });
});

describe("read coalescing", () => {
  it("merges near ranges into one slab and keeps far ones apart", () => {
    const slabs = planSlabs([
      { offset: 300_000, length: 10 },
      { offset: 0, length: 100 },
      { offset: 100 + MAX_SLAB_GAP, length: 100 },
      { offset: 50, length: 20 },
    ]);
    expect(slabs).toEqual([
      { offset: 0, length: 200 + MAX_SLAB_GAP, members: [1, 3, 2] },
      { offset: 300_000, length: 10, members: [0] },
    ]);
  });

  it.each(seedsFor("next-slabs", [0x51ab, 3]))(
    "covers every range with exactly one slab of at most MAX_SLAB when no range is longer (seed %i)",
    (seed) => {
      const random = mulberry32(seed);
      for (let round = 0; round < 200; round++) {
        const ranges = Array.from({ length: 1 + Math.floor(random() * 40) }, () => ({
          offset: Math.floor(random() * 8 * MAX_SLAB),
          length: Math.floor(random() * (random() < 0.1 ? MAX_SLAB + 1 : 64 * 1024)),
        }));
        const slabs = planSlabs(ranges);
        const covered = new Array<number>(ranges.length).fill(0);
        for (const slab of slabs) {
          expect(slab.length).toBeLessThanOrEqual(MAX_SLAB);
          for (const member of slab.members) {
            const range = ranges[member];
            if (range === undefined) throw new Error("bad member");
            expect(range.offset).toBeGreaterThanOrEqual(slab.offset);
            expect(range.offset + range.length).toBeLessThanOrEqual(slab.offset + slab.length);
            covered[member] = (covered[member] ?? 0) + 1;
          }
        }
        expect(covered.every((count) => count === 1)).toBe(true);
      }
    },
  );

  it("bounds a slab at MAX_SLAB, and reads a longer range alone", () => {
    const pages = Array.from({ length: 80 }, (_, index) => ({
      offset: index * 32_768,
      length: 32_768,
    }));
    const slabs = planSlabs(pages);
    expect(slabs.map((slab) => slab.length)).toEqual([MAX_SLAB, MAX_SLAB, 16 * 32_768]);
    const big = planSlabs([
      { offset: 0, length: 3 * MAX_SLAB },
      { offset: 10, length: 10 },
      { offset: 3 * MAX_SLAB + 1, length: 1 },
    ]);
    expect(big.map((slab) => slab.members)).toEqual([[0, 1], [2]]);
  });
});
