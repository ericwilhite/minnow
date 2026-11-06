/**
 * Review B3-p2b-2 B2: merges that keep failing at the level-0 stall bound. Writers must never
 * wait for a merge that cannot run (they get the slot), never busy-wait (the wait yields tasks),
 * keep writing while the fault persists, and the debt merges once it is gone.
 */
import { expect, it } from "vitest";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import { autocommit, batch, column, Harness, INT, ints, tableInput } from "./test-support.js";

async function setUp(): Promise<{
  harness: Harness;
  path: WritePathImpl;
  t: number;
  fault: { failing: boolean };
}> {
  const harness = new Harness("strict", "memory", 4, undefined, {
    merges: { fanIn: 2, stallRuns: 2 },
  });
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  // Every read a merge makes (its grant's label) fails while the fault is on.
  const fault = { failing: true };
  const read = harness.space.read.bind(harness.space);
  harness.space.read = (refs, options) => {
    const label = (options.budget as { label?: string }).label ?? "";
    if (fault.failing && label.startsWith("merge"))
      return Promise.reject(new Error("injected read fault"));
    return read(refs, options);
  };
  return { harness, path, t, fault };
}

/** begin() with a guard: a busy-wait would read `failure` without end; 1,000 reads stop it. */
async function guardedBegin(path: WritePathImpl): Promise<unknown> {
  let checks = 0;
  const stop = new Error("guard: 1,000 failure checks without the slot");
  Object.defineProperty(path, "failure", {
    configurable: true,
    get: () => (++checks >= 1000 ? stop : null),
  });
  try {
    const tx = await path.begin();
    tx.rollback();
    return null;
  } catch (error) {
    return error;
  } finally {
    Reflect.deleteProperty(path, "failure");
  }
}

it("a writer gets the slot after two failed merges at the stall bound (the reviewer's probe)", async () => {
  const { harness, path, t, fault } = await setUp();
  for (let from = 0; from < 12; from += 4)
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(Array.from({ length: 4 }, (_, k) => from + k))), null),
    );
  await path.quiesce();
  expect(path.failure).toBeNull();
  expect(path.maintenance.mergeFailures).toBeGreaterThanOrEqual(2);
  expect(path.maintenance.merges).toBe(0);
  const primary = need(path.catalog.table(t)?.primaryKey);
  expect(path.state().tables.get(t)?.runs.get(primary)?.length).toBeGreaterThanOrEqual(2);
  fault.failing = false;
  expect(await guardedBegin(path)).toBeNull();
  await harness.close();
}, 120_000);

it("writing continues while merges keep failing, and the debt merges once the fault is gone", async () => {
  const { harness, path, t, fault } = await setUp();
  let id = 0;
  const insert = () => {
    const ids = Array.from({ length: 4 }, () => id++);
    return autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
  };
  // Twelve flushes under a persistent fault: every begin() gets the slot; failures stay bounded
  // (a try and a retry per new level-0 state).
  for (let flush = 0; flush < 12; flush++) {
    expect(await guardedBegin(path)).toBeNull();
    await insert();
    await path.quiesce();
  }
  expect(path.failure).toBeNull();
  expect(path.maintenance.merges).toBe(0);
  expect(path.maintenance.mergeFailures).toBeLessThanOrEqual(2 * 13);
  // Debt no merge can reduce is not reported as backpressure.
  expect(path.stats().backpressure).toBe(false);
  const primary = need(path.catalog.table(t)?.primaryKey);
  const before = path.state().tables.get(t)?.runs.get(primary)?.length ?? 0;
  expect(before).toBeGreaterThan(2);
  // The fault goes; the next flush changes level 0, and its merge runs.
  fault.failing = false;
  await insert();
  await insert();
  await path.quiesce();
  expect(path.failure).toBeNull();
  expect(path.maintenance.merges).toBeGreaterThan(0);
  expect(path.state().tables.get(t)?.runs.get(primary)?.length ?? 0).toBeLessThan(before);
  expect(path.stats().backpressure).toBe(false);
  await harness.space.audit();
  await harness.close();
}, 120_000);
