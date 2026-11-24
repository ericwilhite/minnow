/**
 * Per-governor state (the phase idle pool) is found through every facade: a preparation facade
 * over a session's wrapper resolves to the governor itself.
 *
 * Found by #68's review (Neon, P3): a wrapper (a Proxy adding options to a session's admissions)
 * under a preparation facade was not unwrapped, so each wrapped query made its own
 * "executor/phase-idle" pool instead of sharing the governor's.
 *
 * Law: exec/phase-pool.ts, "one shared idle account per governor".
 */
import { expect, it } from "vitest";
import { Governor, type MemoryGovernor } from "../memory/governor.js";
import { acquirePhasePool } from "./phase-pool.js";
import { baseGovernor, preparationGovernor } from "./preparation.js";

/** A facade adding nothing, naming the governor it fronts. */
const wrapper = (governor: MemoryGovernor): MemoryGovernor =>
  new Proxy(governor, {
    get(target, key) {
      if (key === "fronts") return target;
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === "function" ? (value as () => unknown).bind(target) : value;
    },
  });

it("resolves a preparation facade over a wrapper to the governor itself", () => {
  const governor = new Governor(8 * 1024 * 1024);
  const preparation = preparationGovernor(wrapper(governor));
  if (preparation === null) throw new Error("the reference governor has a preparation facade");
  expect(baseGovernor(preparation)).toBe(governor);
  expect(baseGovernor(wrapper(wrapper(governor)))).toBe(governor);
  preparation.close();
});

it("shares one phase idle pool between queries behind different wrappers", async () => {
  const governor = new Governor(8 * 1024 * 1024);
  const signal = new AbortController().signal;
  const first = await acquirePhasePool(wrapper(governor), signal);
  const second = await acquirePhasePool(wrapper(governor), signal);
  expect(second.pool).toBe(first.pool);
  first.release();
  second.release();
});
