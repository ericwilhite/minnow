/** Decoded semantic trees belong to one charged column pair and one consumer. */
import { expect, it, vi } from "vitest";
import { Governor } from "../memory/governor.js";
import { Charge, Pool } from "./spill-pool.js";
import { SemanticStep, semanticStepBytes } from "./semantic-step.js";
import { batchesOf } from "./join-fixtures.js";
import type { PhysicalType } from "../types.js";
import type { SortColumn } from "./sort-keys.js";
const type: PhysicalType = { kind: "json", binary: true };
const key = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});
it("reuses one pair across wide equal prefix columns, charging before decode", async () => {
  const governor = new Governor(16 << 20);
  const grant = await governor.admit({ label: "pair", minimumBytes: 1 << 20, spillable: true });
  const pool = new Pool(new Charge(grant), 1 << 20),
    signal = new AbortController();
  const step = new SemanticStep(pool, grant, signal.signal),
    owner = {};
  try {
    const types = Array.from({ length: 16 }, () => type),
      keys = types.map((_, i) => key(i));
    const rows = Array.from({ length: 2 }, (_, r) =>
      types.map((_, i) => JSON.stringify(i === 15 ? r : "x".repeat(8192))),
    );
    const batch = batchesOf(types, rows)[0];
    if (!batch) throw new Error("Missing semantic input batch");
    const bytes = semanticStepBytes(types, keys, batch.columns, 0, 2);
    expect(bytes).toBeLessThan(500000);
    await step.prepare(owner, bytes);
    expect(pool.used).toBe(bytes);
    expect(() => step.prepare({}, bytes)).toThrow("retained across");
    expect(step.compare(owner, types, keys, batch.columns, 0, batch.columns, 1)).toBe(-1);
    expect(pool.used).toBe(bytes);
    step.release(owner);
    step.release(owner);
    expect(pool.used).toBe(0);
  } finally {
    await step.close();
    pool.close();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});
for (const cancel of [false, true])
  it(`close drains pending pair admission (cancel=${String(cancel)})`, async () => {
    const governor = new Governor(8 << 20),
      controller = new AbortController();
    const grant = await governor.admit({
      label: "pair lifecycle",
      minimumBytes: 4096,
      spillable: true,
    });
    const pool = new Pool(new Charge(grant), 4096),
      step = new SemanticStep(pool, grant, controller.signal);
    const bytes = governor.capBytes - governor.reserveBytes - governor.usedBytes;
    const blocker = await governor.admit({
      label: "competing pair lifecycle",
      minimumBytes: bytes,
      spillable: true,
    });
    expect(blocker.allocate(bytes)).toBe(true);
    let resume!: () => void, admitted!: () => void;
    const gate = new Promise<void>((done) => {
      resume = done;
    });
    const ready = new Promise<void>((done) => {
      admitted = done;
    });
    const need = pool.need.bind(pool);
    const spy = vi.spyOn(pool, "need").mockImplementation(async (...args) => {
      await need(...args);
      admitted();
      await gate;
    });
    const reason = new Error("cancel suspended pair");
    const work = step.prepare({}, 65536).catch((error: unknown) => error);
    let closing: Promise<void> | undefined;
    try {
      await ready;
      // This is an actual progress loan, not merely an ordinary-growth suspension.
      expect(governor.loanedBytes).toBeGreaterThan(0);
      let closed = false;
      closing = step.close().then(() => {
        closed = true;
      });
      if (cancel) controller.abort(reason);
      await Promise.resolve();
      expect(closed).toBe(false);
      resume();
      expect(await work).toBeInstanceOf(Error);
      await closing;
      expect(pool.used).toBe(0);
      expect(governor.loanedBytes).toBe(0);
    } finally {
      resume();
      await work;
      await closing;
      await step.close();
      spy.mockRestore();
      pool.close();
      grant.close();
      blocker.close();
    }
    expect(governor.usedBytes).toBe(0);
  });
