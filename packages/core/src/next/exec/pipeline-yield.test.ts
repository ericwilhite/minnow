/**
 * `yieldTask` (exec/pipeline.ts `createYieldTask`) in a browser-like host: an event loop that
 * services message tasks strictly before timers (the worst case a user agent may choose; WebKit
 * was seen to starve timers behind messages, review B5-p2d-1). Law: the pipeline contract
 * (exec/pipeline.ts header, ruling R23): long work yields so the owner's other tasks, timers
 * included, run within about a slice; here a due timer must run within `TIMER_TURN_MS` plus a
 * slice of becoming due, however long the query runs.
 */
import { describe, expect, it } from "vitest";
import { TIMER_TURN_MS, YIELD_INTERVAL_MS, createYieldTask, type TaskHost } from "./pipeline.js";

/** A virtual-time event loop: messages first (FIFO), then due timers (by due time, FIFO). */
class ModelLoop {
  now = 0;
  readonly #messages: Array<() => void> = [];
  readonly #timers: Array<{ due: number; order: number; run: () => void }> = [];
  #order = 0;
  messagesRun = 0;
  timersRun = 0;
  readonly host: TaskHost = {
    setTimeout: (run, ms) => {
      this.#timers.push({ due: this.now + Math.max(0, ms), order: this.#order++, run });
      return 0;
    },
    MessageChannel: ((loop: ModelLoop) =>
      class {
        readonly port1: { onmessage: (() => void) | null } = { onmessage: null };
        readonly port2 = {
          postMessage: () => {
            loop.#messages.push(() => this.port1.onmessage?.());
          },
        };
      })(this) as unknown as new () => MessageChannel,
    now: () => this.now,
  };

  /** Runs one task (a message if any, else the earliest due timer); false when idle. */
  step(): boolean {
    const message = this.#messages.shift();
    if (message !== undefined) {
      this.messagesRun += 1;
      message();
      return true;
    }
    if (this.#timers.length === 0) return false;
    this.#timers.sort((a, b) => a.due - b.due || a.order - b.order);
    const timer = this.#timers.shift();
    if (timer === undefined) return false;
    this.now = Math.max(this.now, timer.due);
    this.timersRun += 1;
    timer.run();
    return true;
  }
}

/** Lets every microtask the last task queued run (a real macrotask turn). */
const drain = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

/**
 * A query of `slices` slices of `YIELD_INTERVAL_MS` virtual work, yielding between them, and an
 * owner timer due at `timerAt`: when that timer ran, and how many yields there were.
 */
async function run(slices: number, timerAt: number): Promise<{ firedAt: number; loop: ModelLoop }> {
  const loop = new ModelLoop();
  const yieldTask = createYieldTask(loop.host);
  const state = { firedAt: -1, done: false };
  loop.host.setTimeout(() => {
    state.firedAt = loop.now;
  }, timerAt);
  void (async () => {
    for (let s = 0; s < slices; s++) {
      loop.now += YIELD_INTERVAL_MS;
      await yieldTask();
    }
    state.done = true;
  })();
  await drain();
  while (!state.done || state.firedAt < 0) {
    if (!loop.step()) break;
    await drain();
  }
  return { firedAt: state.firedAt, loop };
}

describe("yieldTask takes timer turns where messages could starve timers", () => {
  it("a due timer runs within TIMER_TURN_MS plus a slice while a long query yields", async () => {
    // 4 s of virtual work in 4 ms slices; timers due early, mid-query and late.
    for (const timerAt of [0, 10, 1_234, 3_000]) {
      const { firedAt } = await run(1_000, timerAt);
      expect(firedAt, `timer due at ${String(timerAt)}`).toBeGreaterThanOrEqual(timerAt);
      expect(firedAt - timerAt).toBeLessThanOrEqual(TIMER_TURN_MS + YIELD_INTERVAL_MS);
    }
  });

  it("still yields mostly through messages (no timer clamp on every slice)", async () => {
    const { loop } = await run(1_000, 1e9);
    // About one timer turn per TIMER_TURN_MS of work: 4,000 ms / 50 ms = 80, of 1,000 yields.
    expect(loop.timersRun).toBeLessThanOrEqual(
      Math.ceil((1_000 * YIELD_INTERVAL_MS) / TIMER_TURN_MS) + 1,
    );
    expect(loop.messagesRun).toBeGreaterThan(900);
  });

  it("uses setImmediate where it exists (Node: timers run first in each loop turn)", async () => {
    let immediates = 0;
    let timeouts = 0;
    const yieldTask = createYieldTask({
      setImmediate: (callback) => {
        immediates += 1;
        setImmediate(callback);
      },
      setTimeout: (callback, ms) => {
        timeouts += 1;
        return setTimeout(callback, ms);
      },
      now: () => 1e9,
    });
    await yieldTask();
    await yieldTask();
    expect([immediates, timeouts]).toEqual([2, 0]);
  });
});
