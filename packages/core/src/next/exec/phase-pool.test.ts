import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { acquirePhasePool } from "./phase-pool.js";
it("shares one idle account, closes after the last phase, and cancels creation", async () => {
  const governor = new Governor(8 << 20),
    signal = new AbortController().signal;
  const [a, b] = await Promise.all([
    acquirePhasePool(governor, signal),
    acquirePhasePool(governor, signal),
  ]);
  expect(a.pool).toBe(b.pool);
  const source = await governor.admit({
    label: "old remaining plan",
    minimumBytes: 1024,
    spillable: false,
  });
  expect(governor.transferCredit(source, a.pool, 1024)).toBe(true);
  source.close();
  a.release();
  expect(governor.transferCredit(b.pool, b.pool, 0)).toBe(true);
  expect(governor.usedBytes).toBe(1024);
  b.release();
  b.release();
  expect(governor.transferCredit(b.pool, b.pool, 0)).toBe(false);
  expect(governor.usedBytes).toBe(0);
  const stop = new AbortController(),
    cancelled = acquirePhasePool(governor, stop.signal);
  stop.abort(new Error("stop phase"));
  await expect(cancelled).rejects.toThrow("stop phase");
  const c = await acquirePhasePool(governor, signal);
  expect(c.pool).not.toBe(a.pool);
  c.release();
  expect(governor.usedBytes).toBe(0);
});
