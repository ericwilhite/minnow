/**
 * The idle flush's header (write/log.ts; access/write-path.ts "Commit" step 4): due within
 * IDLE_FLUSH_MS of the last commit even when timers run late. Under continuous query execution a
 * due timer can wait up to ~50 ms plus one slice (the executor's yieldTask timer turn), so the
 * timer is armed IDLE_ARM_MS after the commit's flush (joint review R10/idle-flush note).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryBackend } from "../storage/memory-store.js";
import { IDLE_ARM_MS, IDLE_FLUSH_MS, LogAppender } from "./log.js";

/** The worst timer latency under continuous execution: ~50 ms plus one 5 ms slice. */
const LATE_TIMER_MS = 55;

afterEach(() => {
  vi.useRealTimers();
});

describe("the idle flush acknowledges the last commit in time", () => {
  it("is armed early enough that a timer ~50 ms late still meets the 100 ms bound", () => {
    expect(IDLE_ARM_MS + LATE_TIMER_MS).toBeLessThanOrEqual(IDLE_FLUSH_MS);
  });

  it("writes the acknowledging header IDLE_ARM_MS after the commit's flush", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const backend = new MemoryBackend();
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    const headers: number[] = [];
    const write = session.writeLogHeader.bind(session);
    session.writeLogHeader = (slot, header) => {
      headers.push(Date.now());
      return write(slot, header);
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
      true,
      () => undefined,
    );
    const lsn = log.append(new Uint8Array([1, 2, 3]));
    await log.durable(lsn);
    // The commit's own flush wrote one header (acknowledging nothing new yet).
    const afterFlush = headers.length;
    await vi.advanceTimersByTimeAsync(IDLE_ARM_MS - 1);
    expect(headers.length, "not before IDLE_ARM_MS").toBe(afterFlush);
    await vi.advanceTimersByTimeAsync(1);
    expect(headers.length, "the idle header, at IDLE_ARM_MS").toBe(afterFlush + 1);
    vi.useRealTimers();
    await log.close();
    await session.close();
    await lease.release();
  });
});
