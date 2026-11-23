/**
 * Step signals: a short-lived abort signal that follows long-lived ones, without per-step
 * composites.
 *
 * `AbortSignal.any` makes a composite whose `aborted`, `reason` and `throwIfAborted` re-read its
 * sources on every access (Node's `refreshCompositeSignal`): a native trigger UPDATE derived 57
 * composites per row and checked them constantly (C1's profile, 2026-11-22: a tenth of its time).
 * A step is a plain `AbortController`, so checking it costs nothing.
 *
 * ## Contract
 *
 * `stepSignal(site, ...parents)` behaves as `AbortSignal.any(parents)`, and `abortableStep(site,
 * ...parents)` as `AbortSignal.any([...parents, own.signal])` whose `abort(reason)` is
 * `own.abort(reason)`: **every step it creates matches that composite in reason, final aborted
 * state and abort events.**
 *
 * One open is refused instead (`StepSignalInvariantError`, `MINNOW_STEP_SIGNAL_INVARIANT`, SQLSTATE
 * XX000; nothing registered): a step over a step whose abort
 * two nested sources are both still dispatching, opened inside one of those dispatches. Natively
 * it is born aborted with the reason of whichever source aborted first, and nothing the platform
 * exposes orders those aborts before their dispatch ends, short of a composite per step (the cost
 * this module removes). Engine code opens no step inside an abort dispatch of one of its sources:
 * test builds check that in every project that runs next/ code (`checkStepOpensDuringAbort`).
 *
 * A closed step is over: it no longer follows its parents, unless its abort was already decided
 * when it closed (then it ends aborted with that reason), and a direct abort after a close does
 * nothing.
 *
 * ## How
 *
 * The DOM's two-phase "signal abort": when a source aborts, every dependent takes that source's
 * reason at once (step 4), then the source's own listeners run (step 5), and only then do the
 * dependents' abort events fire (step 6). So:
 * - Each parent with open steps gets one sentinel, `AbortSignal.any([parent])`, created once per
 *   parent and never read. Its abort event fires at step 6, where a composite's would, and the
 *   parent's fan-out listens there: steps abort after every listener of the parent, in the order
 *   they joined.
 * - The first source to abort decides a step's reason. A fan-out passes over a step that another
 *   aborted source has decided but not yet fanned out: that abort encloses this one (a later
 *   abort would have been dispatched completely before returning here), so it came first and its
 *   fan-out aborts the step.
 * - A parent that is itself a step stands for its sources (an abortable step also for its own
 *   signal), as `AbortSignal.any` replaces a composite source by its sources. An aborted parent
 *   gives its own recorded reason; a parent step whose abort one pending source decides gives that
 *   source's reason at once.
 * - A close, or a direct abort, while the step's abort is decided but pending waits for that
 *   fan-out.
 *
 * `close()` ends a step (in a `finally`, including when the work it guards refuses to start): its
 * parents stop holding it, and a parent's sentinel listener is removed with its last step.
 */

/**
 * The contract's one refused open (an engine invariant, SQLSTATE XX000): a step opened inside an
 * abort dispatch of one of its sources, over a step whose abort two nested sources decide. Engine
 * code never does this (`checkStepOpensDuringAbort`); a caller that does sees this rule named.
 */
export class StepSignalInvariantError extends Error {
  readonly code = "MINNOW_STEP_SIGNAL_INVARIANT";
  readonly sqlState = "XX000";
  constructor(readonly site: string) {
    super(
      `Step signal "${site}": a step may not be opened inside an abort dispatch of one of its ` +
        "sources (step-signal.ts, Contract)",
    );
    this.name = "StepSignalInvariantError";
  }
}

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
  /** Leaves every parent (the fan-out that aborts it, or a close). */
  release(): void;
}

/** A parent's sentinel, its open steps and the listener that fans its abort out to them. */
interface Fanout {
  readonly sentinel: AbortSignal;
  readonly steps: Map<AbortController, Step>;
  readonly listener: () => void;
  listening: boolean;
}

const fanouts = new WeakMap<AbortSignal, Fanout>();
/** Parents whose fan-out is running now. */
const fanningOut = new WeakSet<AbortSignal>();
/**
 * Each step signal's sources, and what a step that follows it follows instead: a plain step's
 * sources, an abortable step's sources and its own signal (its direct aborts), as
 * `AbortSignal.any` replaces a composite source by its sources.
 */
const records = new WeakMap<
  AbortSignal,
  {
    readonly sources: readonly AbortSignal[];
    readonly stands: readonly AbortSignal[];
    /** Aborted by its owner (`abortableStep`): its signal is then a source of its own. */
    direct: boolean;
  }
>();
/** Registrations parents hold: one per open step and parent. */
let held = 0;
/** Opens refused: over a step whose abort several pending sources decide (the contract). */
let refused = 0;
/** Test builds only: the sites of steps opened while a source's abort was in dispatch. */
let opensDuringAbort: string[] | null = null;
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
      fanningOut.add(parent);
      try {
        for (const [controller, step] of [...steps]) {
          if (controller.signal.aborted) continue;
          // An enclosing abort of another parent decides this step: its fan-out aborts it.
          if (step.parents.some((other) => other !== parent && pending(other, parent))) continue;
          step.release();
          controller.abort(parent.reason);
        }
      } finally {
        fanningOut.delete(parent);
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
 * which would be one more parent per step): as `AbortSignal.any([...parents, own.signal])` with a
 * private `own` controller. Its signal is a source of its own: a step that follows it follows it,
 * not its parents.
 */
export function abortableStep(site: string, ...parents: readonly AbortSignal[]): AbortableStep {
  return open(site, parents, false);
}

/**
 * Whether an abort of these sources is already decided but not yet fanned out: one is aborted
 * and its fan-out has not run. Natively the composite is aborted then (its reason was fixed when
 * that source aborted); here the pending fan-out applies it.
 */
function decided(sources: readonly AbortSignal[]): boolean {
  return sources.some((source) => pending(source));
}

/**
 * An aborted source whose abort is still being dispatched to its steps: its fan-out is armed (it
 * fires at the end of the source's dispatch) or running. Once the source's `abort()` has returned,
 * this is false, whether or not it ever had a fan-out.
 */
function inDispatch(signal: AbortSignal): boolean {
  return fanouts.get(signal)?.listening === true || fanningOut.has(signal);
}

/**
 * A source aborted whose fan-out has not run, or a step not yet aborted that follows one
 * (`except`: the source whose fan-out is asking).
 */
function pending(signal: AbortSignal, except: AbortSignal | null = null): boolean {
  if (signal === except) return false;
  const record = records.get(signal);
  // A step aborted through its sources is not a source in time of its own: look through it.
  if (signal.aborted && (record === undefined || record.direct)) return inDispatch(signal);
  return record?.sources.some((source) => pending(source, except)) ?? false;
}

/** The aborted sources behind `signal` whose fan-outs have not run. */
function pendingSources(signal: AbortSignal, into: AbortSignal[]): AbortSignal[] {
  const record = records.get(signal);
  if (signal.aborted && (record === undefined || record.direct)) {
    if (inDispatch(signal) && !into.includes(signal)) into.push(signal);
  } else for (const source of record?.sources ?? []) pendingSources(source, into);
  return into;
}

function open(
  site: string,
  parents: readonly AbortSignal[],
  flattened: boolean,
): StepSignal & { abort(reason?: unknown): void } {
  const controller = new AbortController();
  let released = false,
    closing = false;
  const joined: AbortSignal[] = [];
  const release = (): void => {
    if (released) return;
    released = true;
    for (const parent of joined) leave(parent, controller);
    joined.length = 0;
  };
  if (opensDuringAbort !== null && parents.some((parent) => pending(parent)))
    opensDuringAbort.push(site);
  const sources: AbortSignal[] = [];
  const step: Step = { parents: sources, release };
  const handle = {
    signal: controller.signal,
    // Closing while an abort of the step is decided but pending waits for that fan-out: the step
    // still ends aborted with that reason, as a composite would.
    close: (): void => {
      if (released || closing) return;
      if (!controller.signal.aborted && decided(sources)) closing = true;
      else release();
    },
    // A direct abort under a decided, pending abort changes nothing: natively the composite took
    // the parent's reason first.
    // After a close the step is over: a direct abort changes nothing.
    abort: (reason?: unknown): void => {
      if (controller.signal.aborted || released || closing || decided(sources)) return;
      release();
      const record = records.get(controller.signal);
      if (record !== undefined) record.direct = true;
      controller.abort(reason);
    },
  };
  for (const parent of parents) {
    // As AbortSignal.any: the first parent already aborted, in order, decides at once (a step
    // parent by its own recorded reason, never re-derived from its parents).
    if (parent.aborted) {
      released = true;
      controller.abort(parent.reason);
      observer?.(site);
      return handle;
    }
    const record = records.get(parent);
    const decidedBy = record === undefined ? [] : pendingSources(parent, []);
    if (decidedBy.length === 1) {
      // A step whose abort is decided but pending is aborted natively, with the reason of the one
      // source whose fan-out has not run (any other aborted source has run: it passed over the
      // step for this one, which aborted first). As AbortSignal.any: this step is aborted at once.
      released = true;
      controller.abort(decidedBy[0]?.reason);
      observer?.(site);
      return handle;
    }
    if (decidedBy.length > 1) {
      // Natively this step is born aborted with the reason of whichever of those sources aborted
      // first; nothing the platform exposes orders them before their fan-outs run, short of a
      // composite per step (the cost this module removes). Refused rather than guessed: engine
      // code never opens a step inside an abort dispatch (checkStepOpensDuringAbort).
      refused++;
      throw new StepSignalInvariantError(site);
    }
    for (const source of record?.stands ?? [parent])
      if (!sources.includes(source)) sources.push(source);
  }
  for (const source of sources) {
    join(source, controller, step);
    joined.push(source);
  }
  records.set(controller.signal, {
    sources,
    stands: flattened ? sources : [...sources, controller.signal],
    direct: false,
  });
  observer?.(site);
  return handle;
}

/** Test builds only: observe every step as it opens, by site; null stops observing. */
export function observeStepSignals(observe: ((site: string) => void) | null): void {
  observer = observe;
}

/** Opens refused by the contract's one exclusion (tests). */
export function refusedStepOpens(): number {
  return refused;
}

/**
 * Test builds only: records (when `on`) every step opened while an abort of one of its sources is
 * in dispatch, and returns the sites recorded since the last call. Engine code opens none
 * (step-signal-checks.setup.ts asserts it after every test file).
 */
export function checkStepOpensDuringAbort(on: boolean): readonly string[] {
  const seen = opensDuringAbort ?? [];
  opensDuringAbort = on ? [] : null;
  return seen;
}

/** Step registrations parents still hold (tests: a step never closed stays held). */
export function heldStepSignals(): number {
  return held;
}
