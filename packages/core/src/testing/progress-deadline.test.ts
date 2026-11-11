import { afterEach, expect, it, vi } from "vitest";
import { withProgressDeadline } from "./progress-deadline.test-support.js";

afterEach(() => vi.useRealTimers());

function pending() {
  let advance: (marker: string) => void = () => undefined;
  let settle: () => void = () => undefined;
  let error: unknown;
  let settled = false;
  const promise = withProgressDeadline(
    "fold",
    (report) => {
      advance = report;
      return new Promise<void>((resolve) => {
        settle = resolve;
      });
    },
    { idleMs: 100, totalMs: 300, now: () => Date.now() },
  );
  const observed = promise.then(
    () => {
      settled = true;
    },
    (reason: unknown) => {
      settled = true;
      error = reason;
    },
  );
  return {
    advance: (key: string) => advance(key),
    settle: () => settle(),
    observed,
    settled: () => settled,
    error: () => error,
  };
}

it("fails a stalled await without waiting for the suite timeout, and retains diagnostics", async () => {
  vi.useFakeTimers();
  const work = pending();
  await vi.advanceTimersByTimeAsync(100);
  expect(work.settled()).toBe(true);
  expect(work.error()).toBeInstanceOf(Error);
  expect((work.error() as Error).message).toContain("no progress");
  await work.observed;
  expect(vi.getTimerCount()).toBe(0);
});

it("does not count identical cursor observations as progress", async () => {
  vi.useFakeTimers();
  const work = pending();
  work.advance("job:writing:row10");
  await vi.advanceTimersByTimeAsync(90);
  work.advance("job:writing:row10");
  await vi.advanceTimersByTimeAsync(10);
  expect(work.settled()).toBe(true);
  expect(work.error()).toBeInstanceOf(Error);
  expect((work.error() as Error).message).toContain("job:writing:row10");
  await work.observed;
});

it("allows advancing cursors but imposes an absolute cap", async () => {
  vi.useFakeTimers();
  const work = pending();
  for (let row = 0; row < 3; row++) {
    await vi.advanceTimersByTimeAsync(90);
    expect(work.settled()).toBe(false);
    work.advance(`row${String(row)}`);
  }
  await vi.advanceTimersByTimeAsync(30);
  expect(work.settled()).toBe(true);
  expect(work.error()).toBeInstanceOf(Error);
  expect((work.error() as Error).message).toContain("total deadline");
  await work.observed;
});

it("clears its watchdog on success and ignores late observations", async () => {
  vi.useFakeTimers();
  const work = pending();
  work.settle();
  await work.observed;
  expect(work.error()).toBeUndefined();
  expect(vi.getTimerCount()).toBe(0);
  expect(() => work.advance("late native update")).not.toThrow();
});

it("checks the cap at completion even when the timer could not run", async () => {
  vi.useFakeTimers();
  let clock = 0;
  await expect(
    withProgressDeadline(
      "busy phase",
      async () => {
        clock = 1_000;
        return 1;
      },
      { idleMs: 100, totalMs: 300, now: () => clock },
    ),
  ).rejects.toThrow("total deadline");
  expect(vi.getTimerCount()).toBe(0);
});
