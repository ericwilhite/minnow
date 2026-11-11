import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_WIDE_DEADLINE_REACH } from "../../testing/reach.js";
import { jsonbByteProgress, jsonbWideDeadline } from "./jsonb-wide-deadline.test.fixture.js";
import type { SlicedJob } from "../exec/slices.js";

it.each(seedsFor("next-jsonb-wide-deadline", [0xb6b0, 0xb6b1]))(
  "bounds silence while preserving productive byte work (seed %i)",
  async (seed) => {
    const reach = createReach("JSONB wide progress deadline", JSONB_WIDE_DEADLINE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1000, max: 29999 }), async (interval) => {
        reach.beginHistory();
        vi.useFakeTimers();
        const activity = new Set<() => void>();
        let complete: () => void = () => undefined;
        const work = new Promise<void>((resolve) => {
          complete = resolve;
        });
        const cancel = vi.fn();
        const result = { value: "pending" };
        const waiting = jsonbWideDeadline(work, activity, cancel).then(
          () => {
            result.value = "complete";
          },
          () => {
            result.value = "stalled";
          },
        );
        try {
          for (let i = 0; i < 5; i++) {
            await vi.advanceTimersByTimeAsync(interval);
            for (const advance of activity) advance();
          }
          expect(result.value).toBe("pending");
          complete();
          await waiting;
          expect(result.value).toBe("complete");
          expect(cancel).not.toHaveBeenCalled();
          reach.reach("productive work");
          if (5 * interval > 120000) reach.reach("past old total deadline");
          expect(activity.size).toBe(0);

          const failure = { error: undefined as unknown };
          const stalled = jsonbWideDeadline(
            new Promise<never>(() => undefined),
            activity,
            cancel,
          ).catch((error: unknown) => {
            failure.error = error;
          });
          await vi.advanceTimersByTimeAsync(29999);
          expect(failure.error).toBeUndefined();
          await vi.advanceTimersByTimeAsync(1);
          expect(failure.error).toEqual(new Error("JSONB wide made no progress for 30000 ms"));
          await stalled;
          expect(cancel).toHaveBeenCalledExactlyOnceWith(failure.error);
          expect(activity.size).toBe(0);
          reach.reach("stalled work cancelled");
          expect(vi.getTimerCount()).toBe(0);
          reach.reach("returned timer owners");
        } finally {
          complete();
          await waiting;
          vi.useRealTimers();
        }
      }),
      { seed, numRuns: 12, examples: [[29000], [1000]] },
    );
    reach.assertReached();
  },
);

it("does not treat empty generator yields as byte progress and closes an interrupted job", () => {
  let position = 0,
    closed = 0;
  const advance = vi.fn();
  function* scan(): SlicedJob<void> {
    try {
      yield;
      yield;
      position = 12;
      yield;
      yield;
    } finally {
      closed++;
    }
  }
  const job = jsonbByteProgress(scan(), () => position, advance);
  job.next();
  job.next();
  expect(advance).not.toHaveBeenCalled();
  job.next();
  expect(advance).toHaveBeenCalledTimes(1);
  job.next();
  expect(advance).toHaveBeenCalledTimes(1);
  job.return(undefined);
  expect(closed).toBe(1);
});
