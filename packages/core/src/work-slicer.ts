/** Synchronous work the engine runs between turns it hands the event loop. */
export const WORK_SLICE_MS = 8;

/**
 * When the thread last came back from a turn this module handed the event loop. One clock for
 * every caller: a statement runs phase after phase (planning, an overlay build, a scan, a
 * commit) and a caller may await statement after statement, all without real I/O, so a clock
 * per phase would let each phase stay under the slice while together they hold the thread for
 * many slices.
 */
let sliceStart = performance.now();

/**
 * A macrotask boundary, so long work lets queued queries, writes, and rendering run between its
 * steps. An awaited promise that is already settled resumes as a microtask, which the event loop
 * runs before anything else: a loop of such awaits holds the thread exactly as a synchronous loop
 * would.
 */
export async function yieldToEventLoop(): Promise<void> {
  await macrotask();
  sliceStart = performance.now();
}

/**
 * Hands the event loop a turn once the thread has run for `WORK_SLICE_MS` since its last one,
 * so a query arriving in the middle of a fold plan, a scan, or an index build waits for one
 * slice, not for the whole job. Cheap enough to call every few hundred rows.
 */
export async function maybeYieldToEventLoop(): Promise<void> {
  if (performance.now() - sliceStart < WORK_SLICE_MS) return;
  await yieldToEventLoop();
}

function macrotask(): Promise<void> {
  // A timer is clamped to roughly 1–4 ms in the runtimes Minnow targets. Maintenance can yield
  // once per bounded page, so using timers turns a healthy thousand-page cleanup into seconds of
  // artificial latency. MessageChannel is still a genuine task boundary (rendering, timers, and
  // other clients can run) without the timer clamp.
  // Vitest/Sinon fake timers expose a `clock` marker and intentionally expect cooperative work
  // to remain timer-driven. Production timers have no such property.
  if (typeof MessageChannel !== "undefined" && !Reflect.has(setTimeout, "clock")) {
    return new Promise((resolve) => {
      const channel = new MessageChannel();
      channel.port1.onmessage = () => {
        channel.port1.close();
        channel.port2.close();
        resolve();
      };
      channel.port2.postMessage(undefined);
    });
  }
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** Runs a step generator to completion without yielding: its synchronous form. */
export function runSteps<T>(steps: Generator<void, T>): T {
  let step = steps.next();
  while (step.done !== true) step = steps.next();
  return step.value;
}

/** Runs a step generator, offering the event loop a turn between steps once a slice has run. */
export async function runStepsSliced<T>(steps: Generator<void, T>): Promise<T> {
  let step = steps.next();
  while (step.done !== true) {
    await maybeYieldToEventLoop();
    step = steps.next();
  }
  return step.value;
}
