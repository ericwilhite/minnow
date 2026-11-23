/**
 * Step signals (step-signal.ts) replace per-row `AbortSignal.any` composites on a native statement's
 * hot path without weakening cancellation.
 *
 * Laws:
 * - A step aborts with its parent's reason when any parent aborts, at once when one already has
 *   (`AbortSignal.any`'s contract).
 * - A long-lived signal carries at most one listener however many steps follow it, and none once
 *   they close; a closed step is held by no parent: a 2,000-row trigger UPDATE ends with exactly
 *   the registrations it began with.
 * - The statement derives (almost) no composites per row: C1's profile counted 57 per updated row
 *   before (`refreshCompositeSignal` alone was a tenth of the statement's time).
 * - Cancelling the statement while any converted site holds a step is prompt: the call rejects
 *   with the caller's reason (owner/api.ts `CallOptions.signal`), the engine releases every step
 *   within 2 s of the abort, nothing is written, and the connection still answers.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { heldStepSignals, observeStepSignals, stepSignal } from "../step-signal.js";
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

  it("keeps at most one listener on a parent, and none once its steps close", () => {
    const parent = new AbortController();
    let listening = 0,
      most = 0;
    const add = parent.signal.addEventListener.bind(parent.signal),
      remove = parent.signal.removeEventListener.bind(parent.signal);
    vi.spyOn(parent.signal, "addEventListener").mockImplementation((type, listener, options) => {
      if (type === "abort") most = Math.max(most, ++listening);
      add(type, listener, options);
    });
    vi.spyOn(parent.signal, "removeEventListener").mockImplementation((type, listener, options) => {
      if (type === "abort") listening--;
      remove(type, listener, options);
    });
    const before = heldStepSignals();
    const open = Array.from({ length: 3 }, () => stepSignal("test", parent.signal));
    expect(listening, "one listener for three open steps").toBe(1);
    expect(heldStepSignals()).toBe(before + 3);
    for (const step of open) step.close();
    for (let k = 0; k < 1000; k++) {
      const step = stepSignal("test", parent.signal);
      step.close();
      step.close();
    }
    expect(heldStepSignals()).toBe(before);
    expect(listening, "no listener once every step closed").toBe(0);
    expect(most).toBe(1);
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

it("a 2,000-row trigger UPDATE derives no composite per row and leaves no step held", async () => {
  const { owner, db } = await triggerTable(ROWS);
  const sites = new Set<string>();
  const any = AbortSignal.any.bind(AbortSignal);
  let composites = 0;
  AbortSignal.any = (signals: AbortSignal[]) => {
    composites++;
    return any(signals);
  };
  try {
    observeStepSignals((site) => sites.add(site));
    const held = heldStepSignals();
    await db.execute(UPDATE);
    const updated = ROWS / 2;
    expect(heldStepSignals(), "every step released").toBe(held);
    expect(composites / updated, "composites per updated row").toBeLessThanOrEqual(0.1);
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
