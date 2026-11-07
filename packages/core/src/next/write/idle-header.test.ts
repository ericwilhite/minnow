/** Idle acknowledgement scheduling includes time spent awaiting the initial flush.
 * The 100 ms target assumes timely timer dispatch and storage I/O (access/write-path.ts).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryBackend } from "../storage/memory-store.js";
import { IDLE_ARM_MS, IDLE_FLUSH_MS, LogAppender } from "./log.js";

/** The worst timer latency under continuous execution: ~50 ms plus one 5 ms slice. */
const LATE_TIMER_MS = 55;

afterEach(() => {
  vi.useRealTimers();
});

describe("the idle flush budgets from the last append", () => {
  it("reserves 55 ms after the timer budget for scheduling and I/O", () => {
    expect(IDLE_ARM_MS + LATE_TIMER_MS).toBeLessThanOrEqual(IDLE_FLUSH_MS);
  });

  it.each([true, false].flatMap((strict) => [0, 70].map((delay) => ({ strict, delay }))))(
    "schedules from append with strict=$strict and first flush delay=$delay",
    async ({ strict, delay }) => {
      vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] });
      const backend = new MemoryBackend();
      const lease = await backend.acquire();
      const session = await backend.open(lease);
      const headers: number[] = [];
      const write = session.writeLogHeader.bind(session);
      session.writeLogHeader = (slot, header) => {
        headers.push(performance.now());
        return write(slot, header);
      };
      const flush = session.flushLog.bind(session);
      let first = true;
      session.flushLog = async () => {
        if (first) {
          first = false;
          await new Promise<void>((resolve) => setTimeout(resolve, delay));
        }
        await flush();
      };
      const log = new LogAppender(
        session,
        {
          salt: 1,
          chain: 1,
          end: 0,
          lastLsn: 0,
          generation: 0,
          nextSlot: 0,
          acked: { end: 0, chain: 1, lsn: 0 },
          rootSequence: 1,
        },
        strict,
        () => undefined,
      );
      const start = performance.now();
      const lsn = await log.append(new Uint8Array([1, 2, 3]));
      await vi.advanceTimersByTimeAsync(IDLE_FLUSH_MS);
      expect(headers).toHaveLength(2);
      const idle = (headers[1] ?? Infinity) - start;
      expect(idle).toBeGreaterThanOrEqual(IDLE_ARM_MS);
      // A slow initial flush consumes the timer budget, rather than starting it again.
      expect(idle).toBeLessThanOrEqual(Math.max(IDLE_ARM_MS, delay + (strict ? 0 : 5)) + 1);
      await log.durable(lsn);
      await log.close();
      await session.close();
      await lease.release();
    },
  );
});
