/**
 * Step signals (step-signal.ts) replace per-row `AbortSignal.any` composites on a native statement's
 * hot path without weakening cancellation.
 *
 * Laws:
 * - A step aborts exactly as `AbortSignal.any(parents)` would (the DOM's two-phase "signal abort":
 *   the first source to abort fixes every dependent's reason before any listener runs, and
 *   dependents fire after the source's own listeners), including under reentrant listeners that
 *   abort other parents: a fast-check differential against the native composite.
 * - A long-lived signal carries at most one listener however many steps follow it, and none once
 *   they close; a closed step is held by no parent: a 2,000-row trigger UPDATE ends with exactly
 *   the registrations it began with.
 * - The statement derives (almost) no composites per row that it reads: C1's profile counted 57
 *   per updated row before (`refreshCompositeSignal`, run on every read of a composite, was a
 *   tenth of the statement's time). The only composites left are step-signal sentinels, one per
 *   parent signal a caller creates per call, which nothing reads (13 per updated row).
 * - Cancelling the statement while any converted site holds a step is prompt: the call rejects
 *   with the caller's reason (owner/api.ts `CallOptions.signal`), the engine releases every step
 *   within 2 s of the abort, nothing is written, and the connection still answers.
 */
import fc from "fast-check";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createReach, STEP_SIGNAL_DIFFERENTIAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import {
  abortableStep,
  refusedStepOpens,
  StepSignalInvariantError,
  checkStepOpensDuringAbort,
  heldStepSignals,
  observeStepSignals,
  stepSignal,
} from "../step-signal.js";
import { publicOwner } from "./public-owner.test.fixture.js";

afterEach(() => {
  observeStepSignals(null);
  // This file opens steps inside abort dispatches on purpose: clear them from the suite-wide
  // check (step-signal-checks.setup.ts), and keep checking for the next test.
  checkStepOpensDuringAbort(true);
});

describe("stepSignal", () => {
  it("aborts with the parent's reason, and at once under an aborted parent", () => {
    const parent = new AbortController(),
      other = new AbortController();
    const step = stepSignal("test", parent.signal, other.signal);
    expect(step.signal.aborted).toBe(false);
    const reason = new Error("stop");
    other.abort(reason);
    expect(step.signal.aborted).toBe(true);
    expect(step.signal.reason).toBe(reason);
    const late = stepSignal("test", parent.signal, other.signal);
    expect(late.signal.aborted).toBe(true);
    expect(late.signal.reason).toBe(reason);
    step.close();
    late.close();
  });

  it("keeps at most one listener per parent, and none once its steps close", () => {
    const parent = new AbortController();
    const active = new Set<unknown>();
    let most = 0;
    vi.spyOn(AbortSignal.prototype, "addEventListener").mockImplementation(function (
      this: AbortSignal,
      type,
      listener,
      options,
    ) {
      if (type === "abort") most = Math.max(most, active.add(listener).size);
      EventTarget.prototype.addEventListener.call(this, type, listener, options);
    });
    vi.spyOn(AbortSignal.prototype, "removeEventListener").mockImplementation(function (
      this: AbortSignal,
      type,
      listener,
      options,
    ) {
      if (type === "abort") active.delete(listener);
      EventTarget.prototype.removeEventListener.call(this, type, listener, options);
    });
    try {
      const before = heldStepSignals();
      const open = Array.from({ length: 3 }, () => stepSignal("test", parent.signal));
      expect(active.size, "one listener for three open steps").toBe(1);
      expect(heldStepSignals()).toBe(before + 3);
      for (const step of open) step.close();
      for (let k = 0; k < 1000; k++) {
        const step = stepSignal("test", parent.signal);
        step.close();
        step.close();
      }
      expect(heldStepSignals()).toBe(before);
      expect(active.size, "no listener once every step closed").toBe(0);
      expect(most).toBe(1);
    } finally {
      vi.restoreAllMocks();
    }
  });

  // Astra's review of #55: the first source to abort decides the reason, as for AbortSignal.any.
  it("keeps the first parent's reason when that parent's own listener aborts another parent", () => {
    const a = new AbortController(),
      b = new AbortController();
    const first = new Error("caller cancelled"),
      second = new Error("cleanup stopped");
    a.signal.addEventListener("abort", () => b.abort(second));
    const native = AbortSignal.any([a.signal, b.signal]);
    const before = heldStepSignals();
    const step = stepSignal("review", a.signal, b.signal);
    a.abort(first);
    expect(native.reason).toBe(first);
    expect(step.signal.reason).toBe(first);
    expect(heldStepSignals()).toBe(before);
  });

  it("keeps the first parent's reason when a sibling step's listener aborts another parent", () => {
    const p = new AbortController(),
      q = new AbortController();
    const first = new Error("caller cancelled"),
      second = new Error("cleanup stopped");
    const nativeA = AbortSignal.any([p.signal]),
      nativeB = AbortSignal.any([p.signal, q.signal]);
    const a = stepSignal("a", p.signal),
      b = stepSignal("b", p.signal, q.signal);
    // A's listener aborts Q while P's abort is being fanned out, before P reaches B.
    nativeA.addEventListener("abort", () => q.abort(second));
    a.signal.addEventListener("abort", () => q.abort(second));
    p.abort(first);
    expect(nativeB.reason).toBe(first);
    expect(b.signal.reason).toBe(first);
    a.close();
    b.close();
  });

  it("records a step opened inside an abort dispatch of one of its sources (the suite check)", () => {
    checkStepOpensDuringAbort(true);
    const p = new AbortController(),
      q = new AbortController();
    const a = stepSignal("outside", p.signal);
    p.signal.addEventListener("abort", () => {
      stepSignal("inside a source's abort", p.signal, q.signal).close();
      stepSignal("inside, over a decided step", a.signal).close();
      stepSignal("inside, unrelated", q.signal).close();
    });
    p.abort("stop");
    a.close();
    expect(checkStepOpensDuringAbort(true)).toEqual([
      "inside a source's abort",
      "inside, over a decided step",
    ]);
  });

  it("records no open over a source whose abort has returned, fan-out or not", () => {
    checkStepOpensDuringAbort(true);
    const bare = new AbortController();
    bare.abort("finished");
    stepSignal("after a bare abort", bare.signal).close();
    const followed = new AbortController(),
      step = stepSignal("follows", followed.signal);
    followed.abort("finished");
    stepSignal("after a fanned-out abort", followed.signal).close();
    step.close();
    expect(checkStepOpensDuringAbort(true)).toEqual([]);
  });

  // Astra's re-check of #55 (probes.mjs): each compared with AbortSignal.any.
  it("takes an aborted step's own reason, never re-derived from its parents", () => {
    const p = new AbortController(),
      q = new AbortController();
    const nativeA = AbortSignal.any([p.signal, q.signal]),
      a = stepSignal("a", p.signal, q.signal);
    q.abort("caller");
    p.abort("cleanup");
    const nativeB = AbortSignal.any([nativeA]),
      b = stepSignal("b", a.signal);
    expect([nativeA.reason, nativeB.reason]).toEqual(["caller", "caller"]);
    expect([a.signal.reason, b.signal.reason]).toEqual(["caller", "caller"]);
  });

  it("opens over a pending step born aborted with its reason when one source decides it", () => {
    // Astra's probe, with one of A's sources mid-dispatch: exact (born aborted, no event).
    const run = (open: (...parents: AbortSignal[]) => AbortSignal) => {
      const p = new AbortController(),
        q = new AbortController();
      const a = open(p.signal);
      let b: AbortSignal | undefined,
        born = false;
      q.signal.addEventListener("abort", () => p.abort("cleanup"));
      p.signal.addEventListener("abort", () => {
        b = open(a, q.signal);
        born = b.aborted;
      });
      q.abort("caller");
      return { a: a.reason as unknown, b: b?.reason as unknown, born };
    };
    const native = run((...parents) => AbortSignal.any(parents));
    expect(native).toEqual({ a: "cleanup", b: "cleanup", born: true });
    expect(run((...parents) => stepSignal("probe", ...parents).signal)).toEqual(native);
  });

  // Why the exclusion is refused, not built on a composite of A's sources at that point: once
  // both have aborted, AbortSignal.any takes the first in array order, not the first in time.
  it.each([false, true])(
    "a composite made after two nested aborts orders them by array, not time (reversed: %s)",
    (reversed) => {
      const p = new AbortController(),
        q = new AbortController();
      const sources = reversed ? [q.signal, p.signal] : [p.signal, q.signal];
      const a = AbortSignal.any(sources);
      let late: AbortSignal | undefined, native: AbortSignal | undefined;
      p.signal.addEventListener("abort", () => q.abort("inner"));
      q.signal.addEventListener("abort", () => {
        native = AbortSignal.any([a]);
        late = AbortSignal.any(sources);
      });
      p.abort("outer");
      expect(native?.reason).toBe("outer");
      expect(late?.reason).toBe(reversed ? "inner" : "outer");
    },
  );

  it("refuses Astra's nested probe, where two of A's sources are mid-dispatch", () => {
    // A = step(P, Q); Q's listener aborts P; P's listener opens B over A. Natively B is born
    // aborted with "caller" (Q aborted first): the contract's exclusion.
    const p = new AbortController(),
      q = new AbortController();
    const a = stepSignal("a", p.signal, q.signal);
    let opened: unknown;
    q.signal.addEventListener("abort", () => p.abort("cleanup"));
    p.signal.addEventListener("abort", () => {
      try {
        opened = stepSignal("b", a.signal);
      } catch (error) {
        opened = error;
      }
    });
    q.abort("caller");
    expect(a.signal.reason).toBe("caller");
    expect(opened).toBeInstanceOf(StepSignalInvariantError);
    a.close();
  });

  it("keeps the caller's reason when a parent's listener aborts an abortable step directly", () => {
    const p = new AbortController(),
      stop = new AbortController();
    const native = AbortSignal.any([p.signal, stop.signal]);
    const step = abortableStep("own reentrant", p.signal);
    p.signal.addEventListener("abort", () => {
      stop.abort("cleanup");
      step.abort("cleanup");
    });
    p.abort("caller");
    expect(native.reason).toBe("caller");
    expect(step.signal.reason).toBe("caller");
  });

  it("still aborts a step closed inside its parent's abort, as a composite would", () => {
    const p = new AbortController();
    const native = AbortSignal.any([p.signal]),
      step = stepSignal("closed in listener", p.signal);
    const before = heldStepSignals();
    p.signal.addEventListener("abort", () => step.close());
    p.abort("caller");
    expect(native.reason).toBe("caller");
    expect(step.signal.reason).toBe("caller");
    expect(heldStepSignals()).toBe(before - 1);
  });

  /**
   * The contract's one exclusion: a step opened, inside an abort dispatch, over a step whose abort
   * two nested sources are both still dispatching. Natively it is born aborted with the reason of
   * the source that aborted first, which nothing orders yet: it is refused, with nothing
   * registered.
   */
  const nested = (open: (...parents: AbortSignal[]) => AbortSignal) => {
    const p0 = new AbortController(),
      p1 = new AbortController();
    const s0 = open(p1.signal, p0.signal);
    let opened: AbortSignal | Error | undefined;
    p0.signal.addEventListener("abort", () => p1.abort("p1"));
    p1.signal.addEventListener("abort", () => {
      try {
        opened = open(s0);
      } catch (error) {
        opened = error as Error;
      }
    });
    p0.abort("p0");
    return { s0: s0.reason as unknown, opened };
  };

  it("refuses to open over a step whose abort two nested pending sources decide", () => {
    const native = nested((...parents) => AbortSignal.any(parents));
    expect(native.s0).toBe("p0");
    expect((native.opened as AbortSignal).reason).toBe("p0");
    const held = heldStepSignals(),
      refused = refusedStepOpens();
    const ours = nested((...parents) => stepSignal("nested", ...parents).signal);
    expect(ours.s0).toBe("p0");
    expect(ours.opened).toBeInstanceOf(StepSignalInvariantError);
    expect(ours.opened).toMatchObject({
      code: "MINNOW_STEP_SIGNAL_INVARIANT",
      sqlState: "XX000",
      message: expect.stringContaining(
        "a step may not be opened inside an abort dispatch of one of its sources",
      ) as unknown,
    });
    expect(refusedStepOpens()).toBe(refused + 1);
    expect(heldStepSignals(), "nothing registered").toBe(held);
  });

  // Astra's final check of #55 (adversarial.mjs): with the open refused, nothing fires that a
  // composite would not, so an independent step its listener would abort stays unaborted.
  it.each([false, true])(
    "aborts no independent step through a refused open (closed inside dispatch: %s)",
    (close) => {
      const run = (native: boolean) => {
        const open = (...parents: AbortSignal[]) => {
          if (native) {
            const own = new AbortController();
            return {
              signal: AbortSignal.any([...parents, own.signal]),
              abort: (r: unknown) => own.abort(r),
              close: () => undefined,
            };
          }
          return abortableStep("adversarial", ...parents);
        };
        const p = new AbortController(),
          q = new AbortController();
        const a = open(p.signal, q.signal),
          victim = open();
        p.signal.addEventListener("abort", () => q.abort("inner"));
        q.signal.addEventListener("abort", () => {
          try {
            const b = open(a.signal);
            b.signal.addEventListener("abort", () => victim.abort("extra event"));
            if (close) b.close();
          } catch {
            // Refused (step signals only).
          }
        });
        p.abort("outer");
        a.close();
        return { a: a.signal.reason as unknown, victim: victim.signal.aborted };
      };
      expect(run(true)).toEqual({ a: "outer", victim: false });
      expect(run(false)).toEqual(run(true));
    },
  );

  it("releases a sibling closed during an abort, and aborts a late step at once", () => {
    const parent = new AbortController();
    const before = heldStepSignals();
    stepSignal("old", parent.signal).close();
    const a = stepSignal("a", parent.signal),
      b = stepSignal("b", parent.signal);
    a.signal.addEventListener("abort", () => b.close());
    const reason = new Error("stop");
    parent.abort(reason);
    expect(a.signal.reason).toBe(reason);
    expect(heldStepSignals()).toBe(before);
    const late = stepSignal("late", parent.signal);
    expect(late.signal.reason).toBe(reason);
    expect(heldStepSignals()).toBe(before);
  });

  it("releases a step aborted through one parent from the others", () => {
    const a = new AbortController(),
      b = new AbortController();
    const before = heldStepSignals();
    stepSignal("test", a.signal, b.signal);
    expect(heldStepSignals()).toBe(before + 2);
    a.abort();
    expect(heldStepSignals()).toBe(before);
  });
});

/**
 * A random world of parents and steps, built twice: with native composites (a step is
 * `AbortSignal.any(parents)`, an abortable step `AbortSignal.any([...parents, own.signal])` and
 * its direct abort `own.abort`), and with step signals. Listeners on parents and steps abort
 * parents, abort abortable steps, open steps (over parents and existing steps), close steps or
 * throw; every abort carries its own reason, so the order of aborts shows in the reasons.
 *
 * A step closed before any abort of it was decided is over: in both worlds it then reads
 * "closed", runs no listener and is followed by no new step (a native composite has no close; the
 * model freezes it there). A step closed after its abort was decided (inside that abort's
 * dispatch) reads that abort's reason, as a composite would.
 */
type Ref = { parent: number } | { step: number };
type Action =
  | { do: "abort parent"; parent: number }
  | { do: "abort step"; step: number }
  | { do: "open"; abortable: boolean; parents: readonly Ref[] }
  | { do: "close"; step: number }
  | { do: "throw" };
interface World {
  readonly parents: number;
  /** Steps opened before any abort. */
  readonly steps: ReadonlyArray<{ abortable: boolean; parents: readonly Ref[] }>;
  /** `before`: on a parent, registered before the steps open. */
  readonly listeners: ReadonlyArray<{ on: Ref; before: boolean; action: Action }>;
  /** Done from outside, in order. */
  readonly outside: readonly Action[];
}

const ref = (parents: number, steps: number): fc.Arbitrary<Ref> =>
  fc.oneof(
    fc.integer({ min: 0, max: parents - 1 }).map((parent) => ({ parent })),
    fc.integer({ min: 0, max: steps - 1 }).map((step) => ({ step })),
  );
const action = (parents: number, steps: number): fc.Arbitrary<Action> =>
  fc.oneof(
    {
      weight: 4,
      arbitrary: fc
        .integer({ min: 0, max: parents - 1 })
        .map((parent) => ({ do: "abort parent" as const, parent })),
    },
    {
      weight: 2,
      arbitrary: fc
        .integer({ min: 0, max: steps - 1 })
        .map((step) => ({ do: "abort step" as const, step })),
    },
    {
      weight: 2,
      arbitrary: fc.record({
        do: fc.constant("open" as const),
        abortable: fc.boolean(),
        parents: fc.array(ref(parents, steps), { minLength: 1, maxLength: 3 }),
      }),
    },
    {
      weight: 1,
      arbitrary: fc
        .integer({ min: 0, max: steps - 1 })
        .map((step) => ({ do: "close" as const, step })),
    },
    { weight: 1, arbitrary: fc.constant({ do: "throw" as const }) },
  );
const world: fc.Arbitrary<World> = fc
  .record({ parents: fc.integer({ min: 2, max: 4 }), steps: fc.integer({ min: 1, max: 4 }) })
  .chain(({ parents, steps }) =>
    fc.record({
      parents: fc.constant(parents),
      steps: fc.tuple(
        ...Array.from({ length: steps }, (_, k) =>
          fc.record({
            abortable: fc.boolean(),
            parents: fc.array(
              k === 0 ? ref(parents, 1).filter((r) => "parent" in r) : ref(parents, k),
              {
                minLength: 1,
                maxLength: 3,
              },
            ),
          }),
        ),
      ),
      // Refs to steps may name steps opened later (inside listeners): unknown ones are skipped.
      listeners: fc.array(
        fc.record({
          on: ref(parents, steps + 2),
          before: fc.boolean(),
          action: action(parents, steps + 2),
        }),
        { maxLength: 6 },
      ),
      outside: fc.array(action(parents, steps + 2), { minLength: 1, maxLength: 3 }),
    }),
  );

/** How one world makes and drives its steps. */
interface Engine {
  readonly native: boolean;
  open(
    parents: readonly AbortSignal[],
    abortable: boolean,
  ): { signal: AbortSignal; abort(reason: unknown): void; close(): void };
}
const nativeEngine: Engine = {
  native: true,
  open(parents, abortable) {
    const own = new AbortController();
    const signal = AbortSignal.any(abortable ? [...parents, own.signal] : [...parents]);
    return { signal, abort: (reason) => own.abort(reason), close: () => undefined };
  },
};
const stepEngine: Engine = {
  native: false,
  open(parents, abortable) {
    if (abortable) {
      const step = abortableStep("differential", ...parents);
      return {
        signal: step.signal,
        abort: (reason) => step.abort(reason),
        close: () => step.close(),
      };
    }
    const step = stepSignal("differential", ...parents);
    return { signal: step.signal, abort: () => undefined, close: () => step.close() };
  },
};

/**
 * Each parent's and step's final state: its reason's label, "closed", or "-" when untouched.
 * `refused` holds the indices (in order of attempt) of opens the step world refused (the
 * contract's one exclusion): the step world records them, the native world skips them.
 */
function play(w: World, engine: Engine, seen: Set<string>, refused: Set<number>): string[] {
  let attempts = 0,
    throws = 0;
  const controllers = Array.from({ length: w.parents }, () => new AbortController());
  const labels = new Map<unknown, string>();
  let next = 0;
  const reason = (label: string) => {
    const value = new Error(label);
    labels.set(value, `${label} ${String(next++)}`);
    return value;
  };
  const steps: Array<{
    signal: AbortSignal;
    abort(reason: unknown): void;
    close(): void;
    abortable: boolean;
    closed: string | null;
  }> = [];
  const signalOf = (r: Ref) =>
    "parent" in r ? controllers[r.parent]?.signal : steps[r.step]?.signal;
  const label = (signal: AbortSignal) =>
    signal.aborted ? (labels.get(signal.reason) ?? "?") : "-";
  let dispatching = 0;
  const pendingListeners: Array<{ on: Ref; action: Action }> = [];
  const run = (act: Action, source: string) => {
    if (act.do === "abort parent") {
      if (dispatching > 0) seen.add("reentrant abort of another parent");
      controllers[act.parent]?.abort(reason(`${source} aborts parent ${String(act.parent)}`));
    } else if (act.do === "abort step") {
      const step = steps[act.step];
      if (!step?.abortable || step.closed !== null) return;
      seen.add(
        dispatching > 0
          ? "direct abort inside a parent's abort"
          : "direct abort of an abortable step",
      );
      step.abort(reason(`${source} aborts step ${String(act.step)}`));
    } else if (act.do === "open") {
      // A closed step is over: nothing may follow it.
      if (act.parents.some((r) => "step" in r && steps[r.step]?.closed != null)) return;
      const parents = act.parents.map(signalOf);
      if (parents.some((p) => p === undefined)) return;
      if (dispatching > 0) seen.add("step created inside a listener");
      if (act.parents.some((r) => "step" in r && steps[r.step]?.signal.aborted))
        seen.add("step created over an aborted step");
      open(parents as AbortSignal[], act.abortable);
    } else if (act.do === "close") {
      const step = steps[act.step];
      if (step?.closed !== null) return;
      if (dispatching > 0) seen.add("close inside an abort");
      step.close();
      // A native composite has no close: its state at the close is the model's verdict (an abort
      // already decided stays; otherwise the step is over). A step signal is judged at the end.
      step.closed = engine.native ? label(step.signal) : "pending";
    } else {
      seen.add("throwing listener");
      throws++;
      throw new Error("listener threw");
    }
  };
  const listen = (index: number, on: Ref, act: Action) =>
    signalOf(on)?.addEventListener("abort", () => {
      // A step closed before its abort was decided is over (a native composite would fire on).
      if ("step" in on && steps[on.step]?.closed === "-") return;
      dispatching++;
      try {
        if ("step" in on && act.do === "abort parent") seen.add("step listener aborted a parent");
        run(act, `listener ${String(index)}`);
      } finally {
        dispatching--;
      }
    });
  const open = (parents: AbortSignal[], abortable: boolean) => {
    const attempt = attempts++;
    if (engine.native && refused.has(attempt)) return null;
    let opened: ReturnType<Engine["open"]>;
    try {
      opened = engine.open(parents, abortable);
    } catch (error) {
      if (engine.native || !(error instanceof StepSignalInvariantError)) throw error;
      refused.add(attempt);
      return null;
    }
    steps.push({ ...opened, abortable, closed: null });
    const index = steps.length - 1;
    // Listeners naming this step attach as it opens.
    for (const [k, l] of w.listeners.entries())
      if ("step" in l.on && l.on.step === index) listen(k, l.on, l.action);
    return opened;
  };
  void pendingListeners;
  for (const [k, l] of w.listeners.entries())
    if (l.before && "parent" in l.on) {
      seen.add("parent listener registered before the steps");
      listen(k, l.on, l.action);
    }
  for (const step of w.steps) {
    const parents = step.parents.map(signalOf);
    if (parents.some((p) => p === undefined)) continue;
    if (step.parents.some((r) => "step" in r)) seen.add("nested step");
    open(parents as AbortSignal[], step.abortable);
  }
  for (const [k, l] of w.listeners.entries())
    if (!l.before && "parent" in l.on) listen(k, l.on, l.action);
  for (const [k, act] of w.outside.entries()) {
    try {
      run(act, `outside ${String(k)}`);
    } catch {
      // An outside "throw" is not a listener: nothing to compare.
    }
  }
  const verdict = (step: (typeof steps)[number]) => {
    if (step.closed === null) return label(step.signal);
    const state = step.closed === "pending" ? label(step.signal) : step.closed;
    return state === "-" ? "closed" : state;
  };
  for (const step of steps) if (verdict(step) === "-") seen.add("step left unaborted");
  return [
    ...controllers.map((c) => label(c.signal)),
    ...steps.map(verdict),
    `listeners threw ${String(throws)}`,
  ];
}

const RUNS = 400;

it.each(seedsFor("next-step-signal-differential", [0x5751, 0x5752]))(
  "aborts exactly as AbortSignal.any under reentrant listeners (seed %i)",
  async (seed) => {
    const reach = createReach(
      "step signals against AbortSignal.any",
      STEP_SIGNAL_DIFFERENTIAL_REACH,
    );
    // A throwing listener's error is reported, not thrown: each world counts its own throws (in
    // the states it returns); the reports are swallowed here.
    const report = () => undefined;
    process.on("uncaughtException", report);
    try {
      await fc.assert(
        fc.asyncProperty(world, async (w) => {
          reach.beginHistory();
          const seen = new Set<string>(),
            refused = new Set<number>();
          // Step signals first: the opens they refuse (the contract's exclusion) are skipped in
          // the native world too, and every world is compared.
          const ours = play(w, stepEngine, new Set(), refused);
          const native = play(w, nativeEngine, seen, refused);
          await new Promise((resolve) => setImmediate(resolve));
          expect(ours).toEqual(native);
          for (const state of seen)
            reach.reach(state as (typeof STEP_SIGNAL_DIFFERENTIAL_REACH)[number]);
          const stepStates = native.slice(w.parents);
          if (
            w.steps.some(
              (step, k) =>
                stepStates[k] !== "-" &&
                step.parents.filter((r) => "parent" in r && native[r.parent] !== "-").length >= 2,
            )
          )
            reach.reach("step with several aborted parents");
        }),
        { seed, numRuns: RUNS },
      );
    } finally {
      process.off("uncaughtException", report);
    }
    reach.assertReached();
  },
  120_000,
);

const ROWS = 2000;
/** Every site the trigger UPDATE below opens steps at (step-signal.ts callers). */
const SITES = [
  "native write step",
  "native root funding",
  "native root admission",
  "native query workspace",
  "native query admission",
  "memory read",
  "row image read",
  "command checkpoint",
  "command checks",
  "following",
] as const;

async function triggerTable(rows: number) {
  const owner = await publicOwner({ capBytes: 64 << 20 });
  const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
  await db.execute("CREATE TABLE t(id INT PRIMARY KEY, v TEXT)");
  await db.execute("CREATE TABLE mirror(id INT PRIMARY KEY, v TEXT)");
  await db.execute(`INSERT INTO mirror SELECT g, 'm' FROM generate_series(1, ${String(rows)}) g`);
  await db.execute(
    `INSERT INTO t SELECT g, 'x' || g::text FROM generate_series(1, ${String(rows)}) g`,
  );
  await db.execute(
    "CREATE TRIGGER follow AFTER UPDATE ON t FOR EACH ROW BEGIN UPDATE mirror SET v = NEW.v WHERE id = NEW.id; END",
  );
  return { owner, db };
}
const UPDATE = "UPDATE t SET v = 'y' || (id * 7)::text WHERE id % 2 = 0";

it("a 2,000-row trigger UPDATE derives no read composite per row and leaves no step held", async () => {
  const { owner, db } = await triggerTable(ROWS);
  const sites = new Set<string>();
  const any = AbortSignal.any.bind(AbortSignal);
  let composites = 0,
    sentinels = 0;
  AbortSignal.any = (signals: AbortSignal[]) => {
    // A sentinel (step-signal.ts `fanoutOf`) is created per parent and never read.
    if (((new Error().stack ?? "").split("\n")[2] ?? "").includes("step-signal.ts")) sentinels++;
    else composites++;
    return any(signals);
  };
  try {
    observeStepSignals((site) => sites.add(site));
    const held = heldStepSignals();
    await db.execute(UPDATE);
    const updated = ROWS / 2;
    expect(heldStepSignals(), "every step released").toBe(held);
    expect(composites / updated, "composites per updated row").toBeLessThanOrEqual(0.1);
    // One sentinel per parent a caller creates per call (measured 13.03 per updated row).
    expect(sentinels / updated, "sentinels per updated row").toBeLessThanOrEqual(13.5);
    expect([...sites].sort()).toEqual([...SITES].sort());
    expect((await db.query("SELECT count(*) AS n FROM mirror WHERE v LIKE 'y%'")).rows()).toEqual([
      { n: updated },
    ]);
  } finally {
    AbortSignal.any = any;
    await db.close();
    await owner.close();
  }
}, 120_000);

it.each(SITES)(
  "cancelling while %s holds a step is prompt and writes nothing",
  async (site) => {
    const { owner, db } = await triggerTable(400);
    try {
      const held = heldStepSignals();
      const controller = new AbortController(),
        reason = new Error(`cancel at ${site}`);
      let cancelledAt = 0;
      observeStepSignals((opened) => {
        if (opened !== site || cancelledAt !== 0) return;
        cancelledAt = performance.now();
        controller.abort(reason);
      });
      const outcome: unknown = await db.execute(UPDATE, [], { signal: controller.signal }).then(
        () => "written",
        (error: unknown) => error,
      );
      observeStepSignals(null);
      expect(cancelledAt, `${site} opened a step`).toBeGreaterThan(0);
      expect(outcome, "the call rejects with the caller's reason").toBe(reason);
      // The engine stops on its own schedule: every step it held is released promptly.
      while (heldStepSignals() !== held && performance.now() - cancelledAt < 2000)
        await new Promise((resolve) => setTimeout(resolve, 5));
      expect(heldStepSignals(), "every step released within 2 s").toBe(held);
      expect(
        (await db.query("SELECT count(*) AS n FROM mirror WHERE v LIKE 'y%'")).rows(),
        "nothing written",
      ).toEqual([{ n: 0 }]);
    } finally {
      observeStepSignals(null);
      await db.close();
      await owner.close();
    }
  },
  60_000,
);
