/**
 * The executor's task yields (src/next/exec/pipeline.ts) in a real dedicated worker: long work
 * that yields every 4 ms, beside a 100 ms interval timer standing in for the owner's timers
 * (liveness, idle flush, transaction timeouts). Driven by next-yield-run.ts.
 *
 * - `yieldTask`: the shipped yield, in a loop of 4 ms busy slices.
 * - `drive`: the shipped driver over a source whose batches each take 4 ms (it yields between
 *   them by itself).
 * - `messages`: the control, message-channel yields only (`createYieldTask` with its clock held,
 *   so it never takes a timer turn): what the shipped yield was before the timer-turn fix.
 */
import { createYieldTask, drive, yieldTask, type Operator } from "../src/next/exec/pipeline.js";

export type YieldKind = "yieldTask" | "drive" | "messages";

export interface YieldResult {
  readonly kind: YieldKind;
  /** Interval ticks while the work ran, and when (ms from its start). */
  readonly ticks: readonly number[];
  readonly elapsed: number;
  readonly slices: number;
}

const WORK_MS = 1_500;
const SLICE_MS = 4;
const TICK_MS = 100;

function busy(ms: number): void {
  const until = performance.now() + ms;
  while (performance.now() < until) {
    // Synchronous work: nothing else runs on this worker meanwhile.
  }
}

const messagesOnly = createYieldTask({
  setTimeout: (callback, ms) => setTimeout(callback, ms),
  MessageChannel,
  now: () => 0,
});

async function run(kind: YieldKind): Promise<YieldResult> {
  const start = performance.now();
  const ticks: number[] = [];
  let working = true;
  const interval = setInterval(() => {
    if (working) ticks.push(performance.now() - start);
  }, TICK_MS);
  let slices = 0;
  if (kind === "drive") {
    const sink: Operator = {
      push: () => "ready",
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    await drive(
      {
        next: () => {
          if (performance.now() - start >= WORK_MS) return null;
          busy(SLICE_MS);
          slices += 1;
          return { batch: { length: 0, columns: [], selection: null } };
        },
        abort: () => undefined,
      },
      sink,
    );
  } else {
    const next = kind === "yieldTask" ? yieldTask : messagesOnly;
    while (performance.now() - start < WORK_MS) {
      busy(SLICE_MS);
      slices += 1;
      await next();
    }
  }
  working = false;
  const elapsed = performance.now() - start;
  clearInterval(interval);
  return { kind, ticks, elapsed, slices };
}

addEventListener("message", (event: MessageEvent<YieldKind>) => {
  void run(event.data).then((result) => {
    postMessage(result);
  });
});
postMessage("ready");
