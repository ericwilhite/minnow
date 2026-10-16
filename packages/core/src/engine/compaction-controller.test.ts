import { afterEach, expect, it, vi } from "vitest";
import { CompactionController, QUIET_COMPACTION_FAILURE_LIMIT } from "./compaction-controller.js";
import {
  CompactionJobConflictError,
  OpfsUncertainOutcomeError,
  SchemaConflictError,
  StorageUnresponsiveError,
  TransactionRecordConflictError,
  type TableRecord,
} from "../storage/types.js";
import { TransactionClosedError } from "../transactions/index.js";
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

it("retries a fold lost with its OPFS leader without reporting it, but reports a wedged store", async () => {
  vi.useFakeTimers();
  const lost = new OpfsUncertainOutcomeError("updateCompactionJob");
  const wedged = new StorageUnresponsiveError("indexeddb", "app", 30_000);
  const run = vi.fn().mockRejectedValueOnce(lost).mockRejectedValueOnce(wedged);
  const report = vi.fn();
  const controller = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run,
    check: async () => undefined,
    yield: async () => undefined,
    report,
  });
  controller.schedule(table, 48);
  await controller.drain();
  expect(report).not.toHaveBeenCalled();
  expect(controller.retryPending("t")).toBe(true);
  await vi.advanceTimersByTimeAsync(250);
  controller.schedule(table, 48);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(2);
  expect(report).toHaveBeenCalledExactlyOnceWith(wedged, "automatic compaction for table");
  expect(controller.retryPending("t")).toBe(true);
  controller.stop();
});

it("replans a fold that kept losing to schema changes on its retry, without reporting it", async () => {
  vi.useFakeTimers();
  const run = vi
    .fn()
    .mockRejectedValueOnce(new SchemaConflictError(18, 19))
    .mockResolvedValueOnce(true);
  const report = vi.fn();
  const controller = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run,
    check: async () => undefined,
    yield: async () => undefined,
    report,
  });
  controller.schedule(table, 48);
  await controller.drain();
  expect(report).not.toHaveBeenCalled();
  expect(controller.retryPending("t")).toBe(true);
  await vi.advanceTimersByTimeAsync(250);
  controller.schedule(table, 48);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(2);
  expect(report).not.toHaveBeenCalled();
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

it("retries a failed fold on time alone but holds a declined one until the table grows", async () => {
  vi.useFakeTimers();
  let outcome: "fail" | "decline" = "fail";
  const run = vi.fn(async () => {
    if (outcome === "fail") throw new Error("transient read");
    return false;
  });
  const visible = { t: 48 };
  const controller: CompactionController = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run,
    check: async () => {
      controller.schedule(table, visible.t);
    },
    yield: async () => undefined,
    report: vi.fn(),
  });

  // A failure retries once its delay passes, though the table has not grown at all: an idle
  // table must not wait for writes that may never come.
  controller.schedule(table, visible.t);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(1);
  expect(controller.retryPending("t")).toBe(true);
  await vi.advanceTimersByTimeAsync(250);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(2);

  // A declined fold is held until the table doubles, whatever the timer says.
  outcome = "decline";
  await vi.advanceTimersByTimeAsync(500);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(3);
  await vi.advanceTimersByTimeAsync(60_000);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(3);
  visible.t = 96;
  controller.schedule(table, visible.t);
  await controller.drain();
  expect(run).toHaveBeenCalledTimes(4);
  controller.stop();
});

it("retries folds that lose the shared job to another connection quietly, unless they never stop", async () => {
  vi.useFakeTimers();
  const races = [
    new CompactionJobConflictError("job", 3, 4),
    new TransactionRecordConflictError("txn", 5, 6),
    new TransactionClosedError("txn"),
  ];
  let attempt = 0;
  let outcome: "race" | "fold" = "race";
  const run = vi.fn(async () => {
    if (outcome === "fold") return true;
    const race = races[attempt % races.length] ?? new Error("no race fixture");
    attempt += 1;
    throw race;
  });
  const report = vi.fn();
  const controller: CompactionController = new CompactionController({
    enabled: true,
    maximumLevelZeroSegments: 256,
    dropping: () => false,
    run,
    check: async () => {
      controller.schedule(table, 48);
    },
    yield: async () => undefined,
    report,
  });
  // One backoff period: exactly the next retry timer fires.
  const retry = async () => {
    await vi.advanceTimersToNextTimerAsync();
    await controller.drain();
  };

  const losses = async (count: number) => {
    controller.schedule(table, 48);
    await controller.drain();
    for (let index = 1; index < count; index += 1) await retry();
  };

  // Another connection advanced the job each time: retried on the timer, nothing reported.
  await losses(QUIET_COMPACTION_FAILURE_LIMIT - 1);
  expect(run).toHaveBeenCalledTimes(QUIET_COMPACTION_FAILURE_LIMIT - 1);
  expect(report).not.toHaveBeenCalled();

  // A fold in between proves progress and starts the count again.
  outcome = "fold";
  await retry();
  expect(run).toHaveBeenCalledTimes(QUIET_COMPACTION_FAILURE_LIMIT);
  expect(controller.retryPending("t")).toBe(false);
  outcome = "race";
  await losses(QUIET_COMPACTION_FAILURE_LIMIT - 1);
  expect(report).not.toHaveBeenCalled();

  // The limit-th loss in a row is a job that is not making progress: the app hears of it.
  await retry();
  expect(report).toHaveBeenCalledExactlyOnceWith(
    races[(attempt - 1) % races.length],
    `automatic compaction for table (${String(QUIET_COMPACTION_FAILURE_LIMIT)} attempts in a row without progress)`,
  );
  expect(controller.retryPending("t")).toBe(true);
  controller.stop();
});
