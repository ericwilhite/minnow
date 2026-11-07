import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import { TaskClock } from "../task.js";
import {
  BoundedFreeSpaceMap,
  MAX_RUN_ARENA_BYTES,
  MIN_RUN_ARENA_BYTES,
} from "../storage/bounded-free-space.js";
import { countingBudget, countingMemory } from "../storage/space-test-support.js";
import { ReadDeniedError } from "../storage/extents.js";

it("transfers one prepaid arena at full ordinary capacity without a second charge or loan", async () => {
  const governor = new Governor(4 << 20),
    startup = await governor.admit({ label: "startup", minimumBytes: 3 << 20, spillable: false });
  const arena = BoundedFreeSpaceMap.create(4096, MIN_RUN_ARENA_BYTES, startup);
  try {
    expect(governor.usedBytes).toBe(3 << 20);
    arena.transfer((owner) => governor.registerCache(owner));
    expect(startup.allocated).toBe(0);
    expect(startup.held).toBe((3 << 20) - MIN_RUN_ARENA_BYTES);
    expect(arena.bytes).toBe(MIN_RUN_ARENA_BYTES);
    expect(governor.usedBytes).toBe(3 << 20);
    expect(() => arena.transfer((owner) => governor.registerCache(owner))).toThrow(TypeError);
    startup.close();
    expect(arena.allocate(4096)).toBe(0);
  } finally {
    arena.close();
    startup.close();
  }
  expect(governor.usedBytes).toBe(0);
});

it("keeps a refused transfer with its source and supports structural test governors", () => {
  const budget = countingBudget(MIN_RUN_ARENA_BYTES),
    arena = BoundedFreeSpaceMap.create(64, MIN_RUN_ARENA_BYTES, budget);
  const denied = countingMemory(0),
    paid = countingMemory(MIN_RUN_ARENA_BYTES);
  try {
    expect(() => arena.transfer(() => denied)).toThrow(ReadDeniedError);
    expect(budget.used).toBe(MIN_RUN_ARENA_BYTES);
    expect(denied.used).toBe(0);
    arena.transfer(() => paid);
    expect(budget.used).toBe(0);
    expect(paid.used).toBe(MIN_RUN_ARENA_BYTES);
  } finally {
    arena.close();
  }
  expect(paid.used).toBe(0);
});

it("cancels during bounded arena copying and retries without another reservation", async () => {
  const backing = new Uint8Array(MAX_RUN_ARENA_BYTES),
    source = BoundedFreeSpaceMap.borrow(64, backing);
  source.startRebuild(100_000 * 128);
  for (let at = 0; at < 100_000; at++) source.releaseKnownAllocated(at * 128, 64);
  const budget = countingBudget(MAX_RUN_ARENA_BYTES),
    target = BoundedFreeSpaceMap.create(64, MAX_RUN_ARENA_BYTES, budget);
  const controller = new AbortController(),
    reason = new Error("cancel arena copy");
  let ticks = 0,
    turns = 0,
    copied = 0;
  const originalSet = Reflect.get(Float64Array.prototype, "set");
  const copying = vi.spyOn(Float64Array.prototype, "set").mockImplementation(function (
    this: Float64Array,
    input: ArrayLike<number>,
    offset?: number,
  ) {
    copied += input.length;
    originalSet.call(this, input, offset);
  });
  try {
    await expect(
      target.copyFrom(source, {
        clock: new TaskClock(() => (ticks += 5)),
        signal: controller.signal,
        yieldTask: async () => {
          await new Promise<void>((resolve) => setImmediate(resolve));
          expect(copied).toBeLessThanOrEqual(256);
          if (++turns === 2) controller.abort(reason);
        },
      }),
    ).rejects.toBe(reason);
    expect(turns).toBe(2);
    expect(target.runCount).toBe(0);
    expect(budget.used).toBe(MAX_RUN_ARENA_BYTES);
    copying.mockRestore();
    await target.copyFrom(source);
    expect(target.runs()).toEqual(source.runs());
    expect(target.untrackedFreeBytes).toBe(source.untrackedFreeBytes);
  } finally {
    copying.mockRestore();
    source.close();
    target.close();
  }
  expect(budget.used).toBe(0);
});
