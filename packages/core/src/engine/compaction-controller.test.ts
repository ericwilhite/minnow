import { afterEach, expect, it, vi } from "vitest";
import { CompactionController } from "./compaction-controller.js";
import type { TableRecord } from "../storage/types.js";
const table = { id: "t", name: "table" } as TableRecord;
afterEach(() => vi.useRealTimers());
it("owns retry and debounce timers and drains admitted checks before disposal", async () => {
  vi.useFakeTimers();
  const checks: string[] = [];
  const report = vi.fn();
  const controller = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run: async () => false,
    check: async (id) => {
      checks.push(id);
    },
    yield: async () => undefined,
    report,
  });
  controller.committed(["t"]);
  controller.backOff("t", 48);
  await vi.advanceTimersByTimeAsync(25);
  expect(checks).toEqual(["t"]);
  await vi.advanceTimersByTimeAsync(225);
  expect(checks).toEqual(["t", "t"]);
  controller.committed(["t"]);
  controller.backOff("t", 48);
  controller.stop();
  await controller.drain();
  await vi.advanceTimersByTimeAsync(60_000);
  expect(checks).toEqual(["t", "t"]);
  expect(vi.getTimerCount()).toBe(0);
});
it("coalesces overlapping table folds, reports failures, and forgets dropped-table retries", async () => {
  vi.useFakeTimers();
  let reject!: (error: Error) => void;
  const run = vi.fn(
    () =>
      new Promise<boolean>((_resolve, refuse) => {
        reject = refuse;
      }),
  );
  const check = vi.fn(async () => undefined);
  const report = vi.fn();
  const controller = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run,
    check,
    yield: async () => undefined,
    report,
  });
  controller.schedule(table, 48);
  controller.schedule(table, 48);
  const failure = new Error("I/O");
  reject(failure);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(1);
  expect(report).toHaveBeenCalledWith(failure, "automatic compaction for table");
  expect(check).toHaveBeenCalledTimes(1);
  expect(controller.retryPending("t")).toBe(true);
  controller.forget("t");
  expect(controller.retryPending("t")).toBe(false);
  await vi.advanceTimersByTimeAsync(60_000);
  expect(check).toHaveBeenCalledTimes(1);
  controller.stop();
});

it("does not recreate dropped-table retries when an admitted fold fails later", async () => {
  vi.useFakeTimers();
  let reject!: (error: Error) => void;
  const report = vi.fn();
  const check = vi.fn(async () => undefined);
  const controller = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run: () =>
      new Promise<boolean>((_resolve, refuse) => {
        reject = refuse;
      }),
    check,
    yield: async () => undefined,
    report,
  });
  controller.schedule(table, 48);
  controller.forget(table.id);
  const failure = new Error("I/O after drop");
  reject(failure);
  await controller.drain();
  expect(report).toHaveBeenCalledWith(failure, "automatic compaction for table");
  expect(controller.retryPending(table.id)).toBe(false);
  await vi.advanceTimersByTimeAsync(60_000);
  expect(check).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(0);
  controller.stop();
});
