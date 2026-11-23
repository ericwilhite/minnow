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
  heldStepSignals,
  observeStepSignals,
  stepSignal,
  type StepSignal,
} from "../step-signal.js";
import { publicOwner } from "./public-owner.test.fixture.js";

afterEach(() => {
  observeStepSignals(null);
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
 * A random world of parents, steps (some following other steps) and reentrant listeners, built
 * twice: once with `AbortSignal.any` composites, once with step signals. Each listener aborts a
 * parent with its own reason, so the order of aborts shows in the reasons.
 */
interface World {
  readonly parents: number;
  /** Each step's parents: a parent index, or `-1 - k` for step k (an earlier step). */
  readonly steps: ReadonlyArray<readonly number[]>;
  /** `on`: a parent index, or `-1 - k` for step k; `before`: registered before the steps. */
  readonly listeners: ReadonlyArray<{ on: number; before: boolean; aborts: number }>;
  /** Parents aborted from outside, in order. */
  readonly aborts: readonly number[];
}

const world: fc.Arbitrary<World> = fc
  .record({ parents: fc.integer({ min: 2, max: 4 }), steps: fc.integer({ min: 1, max: 4 }) })
  .chain(({ parents, steps }) =>
    fc.record({
      parents: fc.constant(parents),
      steps: fc.tuple(
        ...Array.from({ length: steps }, (_, k) =>
          fc.array(fc.integer({ min: -k, max: parents - 1 }), {
            minLength: 1,
            maxLength: 3,
          }),
        ),
      ),
      listeners: fc.array(
        fc.record({
          on: fc.integer({ min: -steps, max: parents - 1 }),
          before: fc.boolean(),
          aborts: fc.integer({ min: 0, max: parents - 1 }),
        }),
        { maxLength: 6 },
      ),
      aborts: fc.array(fc.integer({ min: 0, max: parents - 1 }), { minLength: 1, maxLength: 3 }),
    }),
  );

/** Each step's (and parent's) final state: its reason's label, or "-" when not aborted. */
function play(w: World, derive: (parents: readonly AbortSignal[]) => AbortSignal): string[] {
  const controllers = Array.from({ length: w.parents }, () => new AbortController());
  const reasons = new Map<unknown, string>();
  const reason = (label: string) => {
    const value = new Error(label);
    reasons.set(value, label);
    return value;
  };
  const signals: AbortSignal[] = [];
  const target = (on: number) => (on >= 0 ? controllers[on]?.signal : signals[-1 - on]);
  const listen = (index: number) => {
    const { on, aborts } = w.listeners[index] ?? { on: 0, aborts: 0 };
    target(on)?.addEventListener("abort", () =>
      controllers[aborts]?.abort(reason(`listener ${String(index)}`)),
    );
  };
  for (const [index, listener] of w.listeners.entries())
    if (listener.before && listener.on >= 0) listen(index);
  for (const parents of w.steps)
    signals.push(
      derive(
        parents
          .map((i) => (i >= 0 ? controllers[i]?.signal : signals[-1 - i]))
          .filter((signal): signal is AbortSignal => signal !== undefined),
      ),
    );
  for (const [index, listener] of w.listeners.entries())
    if (!listener.before || listener.on < 0) listen(index);
  for (const [k, index] of w.aborts.entries())
    controllers[index]?.abort(reason(`outside ${String(k)}`));
  return [...controllers.map((c) => c.signal), ...signals].map((signal) =>
    signal.aborted ? (reasons.get(signal.reason) ?? "?") : "-",
  );
}

it.each(seedsFor("next-step-signal-differential", [0x5751, 0x5752]))(
  "aborts exactly as AbortSignal.any under reentrant listeners (seed %i)",
  (seed) => {
    const reach = createReach(
      "step signals against AbortSignal.any",
      STEP_SIGNAL_DIFFERENTIAL_REACH,
    );
    fc.assert(
      fc.property(world, (w) => {
        reach.beginHistory();
        const steps: StepSignal[] = [];
        const native = play(w, (parents) => AbortSignal.any([...parents]));
        const ours = play(w, (parents) => {
          const step = stepSignal("differential", ...parents);
          steps.push(step);
          return step.signal;
        });
        expect(ours).toEqual(native);
        for (const step of steps) step.close();
        const listenerKinds = w.listeners.map((l) => l.on);
        if (w.listeners.some((l) => l.before && l.on >= 0))
          reach.reach("parent listener registered before the steps");
        if (listenerKinds.some((on) => on < 0)) reach.reach("step listener aborted a parent");
        if (w.listeners.length > 0 && native.filter((r) => r.startsWith("listener")).length > 0)
          reach.reach("reentrant abort of another parent");
        if (w.steps.some((parents) => parents.some((i) => i < 0))) reach.reach("nested step");
        const stepStates = native.slice(w.parents);
        if (
          w.steps.some(
            (parents, k) =>
              stepStates[k] !== "-" &&
              parents.filter((i) => i >= 0 && native[i] !== "-").length >= 2,
          )
        )
          reach.reach("step with several aborted parents");
        if (stepStates.includes("-")) reach.reach("step left unaborted");
      }),
      { seed, numRuns: 400 },
    );
    reach.assertReached();
  },
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
