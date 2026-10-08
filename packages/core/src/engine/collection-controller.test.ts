import { afterEach, expect, it, vi } from "vitest";
import { OpfsUncertainOutcomeError, StorageUnresponsiveError } from "../storage/types.js";
import { CollectionController } from "./collection-controller.js";
import { MaintenanceBacklogError } from "./errors.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}
function controller(
  overrides: Partial<ConstructorParameters<typeof CollectionController>[0]> = {},
) {
  const run = vi.fn(async () => ({ moreWork: false, reclaimed: true }));
  const report = vi.fn();
  const collection = new CollectionController({
    enabled: true,
    debtLimit: 3,
    now: Date.now,
    run,
    report,
    yield: async () => undefined,
    durableDebt: async () => 0,
    isShutdownRefusal: () => false,
    ...overrides,
  });
  return { collection, run, report };
}
afterEach(() => vi.useRealTimers());

it("coalesces triggers during a run and drains the continuation before disposal", async () => {
  const first = deferred<{ moreWork: boolean; reclaimed: boolean }>();
  const yielding = deferred<undefined>();
  const run = vi.fn(() => first.promise);
  const { collection } = controller({ run, yield: () => yielding.promise });
  collection.schedule();
  collection.schedule();
  collection.schedule();
  first.resolve({ moreWork: false, reclaimed: true });
  await vi.waitFor(() => expect(collection.status().collectionRunning).toBe(false));
  expect(collection.status().collectionRequested).toBe(true);
  collection.stop();
  let drained = false;
  const closing = collection.drain().then(() => {
    drained = true;
  });
  await Promise.resolve();
  expect(drained).toBe(false);
  yielding.resolve(undefined);
  await closing;
  expect(run).toHaveBeenCalledTimes(1);
  expect(collection.status().collectionRequested).toBe(false);
});

it("retries I/O failures without a new commit and cancels every timer on stop", async () => {
  vi.useFakeTimers();
  const failure = new Error("disk unavailable");
  const run = vi
    .fn()
    .mockRejectedValueOnce(failure)
    .mockResolvedValue({ moreWork: false, reclaimed: true });
  const { collection, report } = controller({ run });
  collection.start();
  collection.schedule();
  await collection.drain();
  expect(report).toHaveBeenCalledWith(failure, "auto collection");
  expect(collection.status().consecutiveFailures).toBe(1);
  await vi.advanceTimersByTimeAsync(1000);
  await collection.drain();
  expect(run).toHaveBeenCalledTimes(2);
  expect(collection.status().consecutiveFailures).toBe(0);
  collection.stop();
  await vi.advanceTimersByTimeAsync(120_000);
  expect(run).toHaveBeenCalledTimes(2);
  expect(vi.getTimerCount()).toBe(0);
});

it("retries a step lost with its OPFS leader without reporting it, but reports a wedged store", async () => {
  vi.useFakeTimers();
  const lost = new OpfsUncertainOutcomeError("updateGarbageCollectionPlanning");
  const wedged = new StorageUnresponsiveError("indexeddb", "app", 30_000);
  const run = vi
    .fn()
    .mockRejectedValueOnce(lost)
    .mockRejectedValueOnce(wedged)
    .mockResolvedValue({ moreWork: false, reclaimed: true });
  const { collection, report } = controller({ run });
  collection.schedule();
  await collection.drain();
  expect(report).not.toHaveBeenCalled();
  expect(collection.status()).toMatchObject({
    consecutiveFailures: 1,
    lastError: { name: "OpfsUncertainOutcomeError" },
  });
  expect(collection.status().nextRetryAt).not.toBeNull();
  await vi.advanceTimersByTimeAsync(1000);
  await collection.drain();
  expect(report).toHaveBeenCalledExactlyOnceWith(wedged, "auto collection");
  await vi.advanceTimersByTimeAsync(2000);
  await collection.drain();
  expect(run).toHaveBeenCalledTimes(3);
  expect(collection.status().consecutiveFailures).toBe(0);
  collection.stop();
});

it("keeps manual backpressure across reopen until a successful explicit collection", async () => {
  const { collection, run } = controller({ enabled: false, durableDebt: async () => 100 });
  await expect(collection.assist()).rejects.toBeInstanceOf(MaintenanceBacklogError);
  expect(run).not.toHaveBeenCalled();
  collection.collectedManually();
  await collection.assist();
  collection.committed();
  collection.committed();
  collection.committed();
  await expect(collection.assist()).rejects.toBeInstanceOf(MaintenanceBacklogError);
  collection.stop();
});

it("reports an unexpected cooperative-yield failure without an unhandled promise", async () => {
  const failure = new Error("yield failed");
  const { collection, report } = controller({
    run: async () => ({ moreWork: true, reclaimed: false }),
    yield: async () => {
      throw failure;
    },
  });
  collection.schedule();
  await collection.drain();
  expect(report).toHaveBeenCalledWith(failure, "auto collection continuation");
  collection.stop();
});

it("serializes explicit steps and releases the queue after a failed step", async () => {
  const first = deferred<undefined>();
  const order: number[] = [];
  const { collection } = controller();
  const a = collection.step(async () => {
    order.push(1);
    await first.promise;
    throw new Error("refused");
  });
  const rejected = expect(a).rejects.toThrow("refused");
  const b = collection.step(async () => {
    order.push(2);
    return 2;
  });
  await Promise.resolve();
  expect(order).toEqual([1]);
  first.resolve(undefined);
  await rejected;
  expect(await b).toBe(2);
  expect(order).toEqual([1, 2]);
  collection.stop();
});

it("reports opaque I/O failures and retains the retry schedule", async () => {
  vi.useFakeTimers();
  const failure = {
    toString() {
      throw new Error("conversion refused");
    },
  };
  const { collection, report } = controller({
    run: async () => {
      // Thrown values from adapters need not be Error instances.
      // eslint-disable-next-line @typescript-eslint/only-throw-error
      throw failure;
    },
  });
  collection.schedule();
  await collection.drain();
  expect(report).toHaveBeenCalledWith(failure, "auto collection");
  expect(collection.status().lastError?.message).toBe("[unprintable error]");
  expect(collection.status().nextRetryAt).not.toBeNull();
  collection.stop();
});
