/**
 * Step signals: a short-lived abort signal that follows long-lived ones, without per-step
 * composites.
 *
 * `AbortSignal.any` makes a composite whose `aborted`, `reason` and `throwIfAborted` re-read its
 * sources on every access (Node's `refreshCompositeSignal`): a native trigger UPDATE derived 57
 * composites per row and checked them constantly (C1's profile, 2026-11-22: a tenth of its time).
 * A step is a plain `AbortController`, so checking it costs nothing.
 *
 * It aborts exactly as `AbortSignal.any(parents)` would: the DOM's two-phase "signal abort". When
 * a source aborts, every dependent takes that source's reason at once (step 4), then the source's
 * own listeners run (step 5), and only then do the dependents' abort events fire (step 6). So:
 * - Each parent with open steps gets one sentinel, `AbortSignal.any([parent])`, created once per
 *   parent and never read. Its abort event fires at step 6, where a composite's would, and the
 *   parent's fan-out listens there: steps abort after every listener of the parent, in the order
 *   they joined.
 * - The first source to abort decides a step's reason. A fan-out passes over a step that has
 *   another aborted parent whose own fan-out has not finished: that parent's abort encloses this
 *   one (a later abort would have been dispatched completely before returning here), so it aborted
 *   first and its fan-out aborts the step.
 * - A parent that is itself a step signal is replaced by that step's parents, as `AbortSignal.any`
 *   replaces a composite source by its sources.
 * A step under an already-aborted parent aborts at once with the first such parent's reason.
 *
 * `close()` ends a step (in a `finally`, including when the work it guards refuses to start): its
 * parents stop holding it, and a parent's sentinel listener is removed with its last step.
 */

export interface StepSignal {
  readonly signal: AbortSignal;
  /** Ends the step: its parents stop holding it. Idempotent. */
  close(): void;
}

/** A step its owner can also abort itself (`abortableStep`). */
export interface AbortableStep extends StepSignal {
  /** Aborts the step with `reason`, as its own source would; it then ends. */
  abort(reason?: unknown): void;
}

interface Step {
  /** Its sources: the parents it joined, step parents replaced by their own. */
  readonly parents: readonly AbortSignal[];
  close(): void;
}

/** A parent's sentinel, its open steps and the listener that fans its abort out to them. */
interface Fanout {
  readonly sentinel: AbortSignal;
  readonly steps: Map<AbortController, Step>;
  readonly listener: () => void;
  listening: boolean;
}

const fanouts = new WeakMap<AbortSignal, Fanout>();
/** Parents whose abort has been fanned out to every step that was theirs to abort. */
const dispatched = new WeakSet<AbortSignal>();
/** Step signals and the sources they follow (for flattening). */
const stepSources = new WeakMap<AbortSignal, readonly AbortSignal[]>();
/** Registrations parents hold: one per open step and parent. */
let held = 0;
/** Test builds only: told the site of every step as it opens (null in production). */
let observer: ((site: string) => void) | null = null;

function fanoutOf(parent: AbortSignal): Fanout {
  const existing = fanouts.get(parent);
  if (existing !== undefined) return existing;
  const steps = new Map<AbortController, Step>();
  const fanout: Fanout = {
    sentinel: AbortSignal.any([parent]),
    steps,
    listening: false,
    listener: () => {
      fanout.listening = false;
      try {
        for (const [controller, step] of [...steps]) {
          if (controller.signal.aborted) continue;
          // An enclosing abort of another parent decides this step: its fan-out aborts it.
          if (
            step.parents.some(
              (other) => other !== parent && other.aborted && !dispatched.has(other),
            )
          )
            continue;
          step.close();
          controller.abort(parent.reason);
        }
      } finally {
        dispatched.add(parent);
      }
    },
  };
  fanouts.set(parent, fanout);
  return fanout;
}

function join(parent: AbortSignal, controller: AbortController, step: Step): void {
  const fanout = fanoutOf(parent);
  if (fanout.steps.has(controller)) return;
  fanout.steps.set(controller, step);
  held++;
  if (!fanout.listening) {
    fanout.listening = true;
    fanout.sentinel.addEventListener("abort", fanout.listener, { once: true });
  }
}

function leave(parent: AbortSignal, controller: AbortController): void {
  const fanout = fanouts.get(parent);
  if (fanout?.steps.delete(controller) !== true) return;
  held--;
  if (fanout.steps.size > 0 || !fanout.listening) return;
  // The last step closed: nothing stays registered for this parent.
  fanout.listening = false;
  fanout.sentinel.removeEventListener("abort", fanout.listener);
}

/**
 * A step signal aborted when any of `parents` aborts, with the reason `AbortSignal.any(parents)`
 * would have. Close it when the step ends. `site` names the step for tests
 * (`observeStepSignals`).
 */
export function stepSignal(site: string, ...parents: readonly AbortSignal[]): StepSignal {
  return open(site, parents, true);
}

/**
 * A step signal its owner can also abort (instead of following a fresh controller of its own,
 * which would be one more parent per step). Its signal is a source of its own: a step that
 * follows it follows it, not its parents.
 */
export function abortableStep(site: string, ...parents: readonly AbortSignal[]): AbortableStep {
  const step = open(site, parents, false);
  return {
    ...step,
    abort: (reason?: unknown) => {
      step.close();
      step.controller.abort(reason);
    },
  };
}

function open(
  site: string,
  parents: readonly AbortSignal[],
  flattened: boolean,
): StepSignal & { readonly controller: AbortController } {
  const controller = new AbortController();
  const sources: AbortSignal[] = [];
  for (const parent of parents)
    for (const source of stepSources.get(parent) ?? [parent])
      if (!sources.includes(source)) sources.push(source);
  const joined: AbortSignal[] = [];
  let closed = false;
  const close = (): void => {
    if (closed) return;
    closed = true;
    for (const parent of joined) leave(parent, controller);
    joined.length = 0;
  };
  const step: Step = { parents: sources, close };
  for (const source of sources)
    if (source.aborted) {
      close();
      controller.abort(source.reason);
      return { signal: controller.signal, close, controller };
    }
  for (const source of sources) {
    join(source, controller, step);
    joined.push(source);
  }
  // Only a step that nothing but its parents can abort may stand for them.
  if (flattened) stepSources.set(controller.signal, sources);
  observer?.(site);
  return { signal: controller.signal, close, controller };
}

/** Test builds only: observe every step as it opens, by site; null stops observing. */
export function observeStepSignals(observe: ((site: string) => void) | null): void {
  observer = observe;
}

/** Step registrations parents still hold (tests: a step never closed stays held). */
export function heldStepSignals(): number {
  return held;
}
