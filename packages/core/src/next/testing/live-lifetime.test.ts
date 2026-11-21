/**
 * Review 10 of #11 (GOVERNOR-WAIT-POLICY, lifetime holdings): lazily created structures that live
 * until close are registered when they are created. The change feed (base and retained history)
 * belongs to the write engine; a live-query runtime (base and its feed listener) to the owner. With
 * either created and idle, the whole usable cap is served, and at that grant every other holding is
 * exactly the registered lifetime holdings.
 */
import { expect, it } from "vitest";
import { CHANGE_FEED_BYTES, CHANGE_LISTENER_BYTES } from "../access/changes.js";
import { LiveRuntime } from "../live/runtime.js";
import { Governor } from "../memory/governor.js";
import { Harness } from "../write/test-support.js";
import { setup } from "./live-runtime.test-support.js";

const PENDING = Symbol("pending");
const settleWithin = <T>(work: Promise<T>, ms: number) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

/** The whole usable cap is served, with no slack at the grant (exact both ways). */
async function exact(governor: Governor): Promise<{ served: unknown; slack: number | null }> {
  let slack: number | null = null;
  const served = await settleWithin(
    governor
      .admit({
        label: "whole usable cap",
        minimumBytes: governor.usableCapacity(),
        spillable: false,
      })
      .then((grant) => {
        slack = governor.capBytes - governor.reserveBytes - governor.usedBytes;
        grant.close();
        return "served";
      }),
    4_000,
  );
  return { served, slack };
}

it("registers the change feed when it is created", async () => {
  const h = new Harness();
  Object.defineProperty(h, "governor", { value: new Governor(32 << 20) });
  const path = await h.open();
  try {
    const before = h.governor.permanentBytes;
    void path.changes;
    expect(h.governor.permanentBytes - before).toBe(CHANGE_FEED_BYTES);
    // Subscribed and unsubscribed: the listener cell was its subscriber's, the base stays.
    const stop = path.changes.observe(() => undefined);
    stop();
    expect(await exact(h.governor)).toEqual({ served: "served", slack: 0 });
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

it("registers a live-query runtime until it closes", async () => {
  const active = await setup(32 << 20);
  const governor = active.h.governor;
  try {
    void active.h.path.changes;
    const before = governor.permanentBytes;
    const runtime = new LiveRuntime(active.engine, governor, active.h.path.changes, {
      pollIntervalMs: 60_000,
    });
    expect(governor.permanentBytes - before).toBe(512 + CHANGE_LISTENER_BYTES);
    await active.h.path.quiesce();
    expect(await exact(governor)).toEqual({ served: "served", slack: 0 });
    await runtime.close();
    expect(governor.permanentBytes).toBe(before);
  } finally {
    await active.close();
  }
  expect(governor.usedBytes).toBe(0);
});
