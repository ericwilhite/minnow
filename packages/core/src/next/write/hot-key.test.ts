/** Repeated non-unique keys are unbounded chains; ordering is linear or O(n log n), sliced on R23.5's task clock. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_HOT_KEY_STATES } from "../../testing/reach.js";
import * as pipeline from "../exec/pipeline.js";
import { KeyDelta } from "./key-hash.js";

const reach = createReach("long non-unique key chains", WRITE_HOT_KEY_STATES);
for (const seed of seedsFor("next-write-hot-key", [0xb3b1]))
  it(`orders generated long hot-key chains (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.constantFrom(4096, 16384, 65536),
          shape: fc.constantFrom("ascending", "descending", "mixed"),
          duplicate: fc.boolean(),
          bound: fc.constantFrom(2, Infinity),
          filter: fc.boolean(),
        }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.shape);
          const delta = new KeyDelta();
          const key = Uint8Array.of(9, 3);
          for (let k = 0; k < s.count; k++) {
            const n =
              s.shape === "ascending"
                ? k
                : s.shape === "descending"
                  ? s.count - k - 1
                  : (k * 8191) % s.count;
            delta.add(
              key,
              0,
              key.length,
              s.duplicate ? (n >>> 1) + 1 : n + 1,
              (k % 4) + 1,
              s.duplicate && k % 2 === 0,
            );
          }
          if (s.duplicate) reach.reach("row-id ties");
          if (s.count === 65536) reach.reach("full memtable chain");
          if (s.bound !== Infinity || s.filter) reach.reach("filtered chain");
          const keep = s.filter
            ? (rowId: number, tombstone: boolean) => rowId % 3 !== 0 || tombstone
            : null;
          const expected = delta
            .sorted(s.bound)
            .flatMap(({ entries }) =>
              entries.filter((e) => keep === null || keep(e.rowId, e.tombstone)),
            );
          const actual: Array<{ rowId: number; tombstone: boolean }> = [];
          await delta.forEachSorted(
            s.bound,
            (_arena, _start, _end, rowId, tombstone) => actual.push({ rowId, tombstone }),
            keep,
          );
          expect(actual).toEqual(expected);
          delta.releaseCharge();
        },
      ),
      { seed, numRuns: 24 },
    );
  }, 120_000);

it.each([0, 6])(
  "honors prior task work and yields during a long hot-key walk (prior %i ms)",
  async (prior) => {
    const delta = new KeyDelta(),
      key = Uint8Array.of(1);
    for (let k = 0; k < 65536; k++) delta.add(key, 0, 1, k + 1, 1, false);
    let now = 0,
      visited = 0,
      yields = 0,
      timerRan = false;
    const original = pipeline.yieldTask;
    const timer = setTimeout(() => {
      timerRan = true;
    }, 0);
    vi.spyOn(performance, "now").mockReturnValue(0);
    vi.spyOn(pipeline.taskClock, "now").mockImplementation(() => {
      now += 0.25;
      return now;
    });
    pipeline.beginTask();
    const epoch = pipeline.taskClock.epoch;
    now += prior;
    vi.spyOn(pipeline, "yieldTask").mockImplementation(async () => {
      yields++;
      await original();
    });
    let pending: ReturnType<KeyDelta["forEachSorted"]> = undefined;
    try {
      pending = delta.forEachSorted(Infinity, () => {
        visited++;
      });
      if (prior > 0) {
        expect(pending).toBeInstanceOf(Promise);
        expect(visited).toBe(0);
        expect(pipeline.taskClock.epoch).toBe(epoch);
      }
      await pending;
      expect(visited).toBe(65536);
      expect(yields).toBeGreaterThan(1);
      expect(timerRan).toBe(true);
      reach.reach("task yields");
    } finally {
      if (pending instanceof Promise) await pending.catch(() => undefined);
      clearTimeout(timer);
      vi.restoreAllMocks();
      pipeline.beginTask();
      delta.releaseCharge();
    }
  },
);

it("carries the clock across many short traversals in one task", async () => {
  const deltas = Array.from({ length: 32 }, () => {
    const delta = new KeyDelta();
    delta.add(Uint8Array.of(1), 0, 1, 1, 1, false);
    return delta;
  });
  let now = 0,
    visited = 0;
  const offsets: number[] = [],
    original = pipeline.yieldTask;
  vi.spyOn(performance, "now").mockReturnValue(0);
  vi.spyOn(pipeline.taskClock, "now").mockImplementation(() => now);
  vi.spyOn(pipeline, "yieldTask").mockImplementation(async () => {
    offsets.push(visited);
    await original();
  });
  pipeline.beginTask();
  try {
    for (const delta of deltas) {
      const pending = delta.forEachSorted(Infinity, () => {
        visited++;
        now += 0.25;
      });
      if (pending instanceof Promise) await pending;
    }
    expect(visited).toBe(32);
    expect(offsets[0]).toBe(16);
    reach.reach("short traversals accumulate");
  } finally {
    vi.restoreAllMocks();
    pipeline.beginTask();
    for (const delta of deltas) delta.releaseCharge();
  }
});

it("keeps overlapping suspended traversals independent and cancels at a task boundary", async () => {
  const delta = new KeyDelta(),
    key = Uint8Array.of(1);
  const expected: number[][] = [[], []];
  for (let k = 0; k < 8192; k++) {
    const rowId = ((k * 2053) % 8192) + 1,
      seq = (k % 4) + 1;
    delta.add(key, 0, 1, rowId, seq, false);
    expected[0]?.push(rowId);
    if (seq <= 2) expected[1]?.push(rowId);
  }
  const actual: number[][] = [[], []];
  let now = 0;
  vi.spyOn(pipeline.taskClock, "now").mockImplementation(() => ++now);
  pipeline.beginTask();
  now += 6;
  try {
    const first = delta.forEachSorted(Infinity, (_a, _s, _e, rowId) => actual[0]?.push(rowId));
    const second = delta.forEachSorted(2, (_a, _s, _e, rowId) => actual[1]?.push(rowId));
    expect(first).toBeInstanceOf(Promise);
    expect(second).toBeInstanceOf(Promise);
    await Promise.all([first, second]);
    expect(actual).toEqual(expected.map((rows) => rows.sort((a, b) => a - b)));
    const stopped = new Error("traversal cancelled"),
      controller = new AbortController();
    now += 6;
    const cancelled = delta.forEachSorted(
      Infinity,
      () => undefined,
      null,
      () => controller.signal.throwIfAborted(),
    );
    controller.abort(stopped);
    await expect(cancelled).rejects.toBe(stopped);
  } finally {
    vi.restoreAllMocks();
    pipeline.beginTask();
    delta.releaseCharge();
  }
});
it("reaches hot-key traversal states", () => reach.assertReached());
