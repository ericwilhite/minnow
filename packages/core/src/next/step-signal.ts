/**
 * Step signals: a short-lived abort signal that follows long-lived ones, without composites.
 *
 * `AbortSignal.any` makes the composite a dependant of every source, released only by garbage
 * collection: a statement that derives several composites per row from the same long-lived signal
 * (its scope's, its writer's) pays for each one on every abort check and refresh. A native
 * trigger UPDATE derived 57 per row (C1's profile, 2026-11-22: `refreshCompositeSignal` alone was
 * a tenth of its time).
 *
 * Here a long-lived signal with open steps has one fan-out: one `abort` listener and the set of
 * its open steps. A step is a plain `AbortController` registered in its parents' fan-outs;
 * `close()` removes it, and must be called when the step ends (in a `finally`). When a parent's
 * last step closes, its listener is removed: nothing stays registered on a long-lived signal
 * once its operations have settled. A parent's abort closes and aborts every open step with the
 * parent's reason, as `AbortSignal.any` would; a step opened under an aborted parent is aborted at
 * once.
 */

export interface StepSignal {
  readonly signal: AbortSignal;
  /** Ends the step: its parents stop holding it. Idempotent. */
  close(): void;
}

/** A long-lived signal's one listener and its open steps (each with the step's close). */
interface Fanout {
  readonly steps: Map<AbortController, () => void>;
  readonly listener: () => void;
}
const fanouts = new WeakMap<AbortSignal, Fanout>();
/** Registrations parents hold: one per open step and parent. */
let held = 0;
/** Test builds only: told the site of every step as it opens (null in production). */
let observer: ((site: string) => void) | null = null;

function join(parent: AbortSignal, controller: AbortController, close: () => void): void {
  let fanout = fanouts.get(parent);
  if (fanout === undefined) {
    const steps = new Map<AbortController, () => void>();
    const listener = () => {
      for (const [step, closeStep] of [...steps]) {
        closeStep();
        step.abort(parent.reason);
      }
    };
    fanout = { steps, listener };
    fanouts.set(parent, fanout);
    parent.addEventListener("abort", listener, { once: true });
  }
  if (fanout.steps.has(controller)) return;
  fanout.steps.set(controller, close);
  held++;
}

function leave(parent: AbortSignal, controller: AbortController): void {
  const fanout = fanouts.get(parent);
  if (fanout?.steps.delete(controller) !== true) return;
  held--;
  if (fanout.steps.size > 0) return;
  // The last step closed: the long-lived signal keeps no listener.
  parent.removeEventListener("abort", fanout.listener);
  fanouts.delete(parent);
}

/**
 * A step signal aborted when any of `parents` aborts. Close it when the step ends. `site` names
 * the step for tests (`observeStepSignals`).
 */
export function stepSignal(site: string, ...parents: readonly AbortSignal[]): StepSignal {
  const controller = new AbortController();
  const joined: AbortSignal[] = [];
  let closed = false;
  const close = (): void => {
    if (closed) return;
    closed = true;
    for (const parent of joined) leave(parent, controller);
    joined.length = 0;
  };
  for (const parent of parents) {
    if (parent.aborted) {
      close();
      controller.abort(parent.reason);
      return { signal: controller.signal, close };
    }
    join(parent, controller, close);
    joined.push(parent);
  }
  observer?.(site);
  return { signal: controller.signal, close };
}

/** Test builds only: observe every step as it opens, by site; null stops observing. */
export function observeStepSignals(observe: ((site: string) => void) | null): void {
  observer = observe;
}

/** Step registrations parents still hold (tests: a step never closed stays held). */
export function heldStepSignals(): number {
  return held;
}
