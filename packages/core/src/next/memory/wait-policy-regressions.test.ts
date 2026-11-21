/** Review 7 of #11 (GOVERNOR-WAIT-POLICY): deterministic regressions for the governor's wait
 * policy. The generated schedules are in holder-priority.test.ts. */
import { describe, expect, it, vi } from "vitest";
import {
  Governor,
  GovernorStallError,
  GrowthObserverReentryError,
  MemoryAdmissionError,
  type Grant,
} from "./governor.js";
import { WriteCredits } from "../write/credits.js";

const tick = () =>
  new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
const within = async <T>(work: Promise<T>, ms: number): Promise<T | "pending"> =>
  Promise.race([
    work,
    new Promise<"pending">((resolve) => {
      setTimeout(() => {
        resolve("pending");
      }, ms);
    }),
  ]);

it("serves the write owner minimum ahead of an earlier new admission while owner holdings stay held", async () => {
  const governor = new Governor(32 << 20),
    ordinary = governor.capBytes - governor.reserveBytes;
  // The owner's scratch control, kept through the writer's open (not registered permanent here:
  // this checks the holder mark itself).
  const held = await governor.admit({
    label: "owner scratch control",
    minimumBytes: 64 << 10,
    spillable: false,
  });
  const stop = new AbortController();
  const foreign = governor
    .admit({
      label: "foreign whole",
      minimumBytes: ordinary,
      spillable: false,
      signal: stop.signal,
    })
    .catch((error: unknown) => error);
  const opening = WriteCredits.open(
    governor,
    { rows: 1 << 20, metadata: 1 << 20, commits: 1 << 20, merge: 1 << 20 },
    undefined,
    () => held.held,
  );
  const credits = await within(opening, 2_000);
  expect(credits, "the write owner minimum waited behind the foreign request").not.toBe("pending");
  stop.abort(new Error("done"));
  await foreign;
  if (credits !== "pending") credits.close();
  held.close();
  expect(governor.usedBytes).toBe(0);
});

it("re-checks priority after a reclaim callout: a holder queued during it is served first", async () => {
  const governor = new Governor(32 << 20),
    ordinary = governor.capBytes - governor.reserveBytes;
  const base = await governor.admit({ label: "base", minimumBytes: ordinary, spillable: true });
  const order: string[] = [];
  let holder: Promise<Grant> | undefined;
  base.onReclaimRequest(() => {
    if (holder) return;
    holder = governor
      .admit({ label: "holder", minimumBytes: 1, spillable: false, holdsCredit: true })
      .then((grant) => {
        order.push("holder");
        return grant;
      });
    base.release(1);
  });
  const fresh = governor
    .admit({ label: "new", minimumBytes: 1, spillable: false })
    .then((grant) => {
      order.push("new");
      return grant;
    });
  const first = await within(holder as unknown as Promise<Grant>, 2_000);
  expect(order, "the new admission took the byte a queued holder needed").toEqual(["holder"]);
  if (first === "pending") throw new Error("the queued holder was not served");
  first.close();
  (await fresh).close();
  expect(order).toEqual(["holder", "new"]);
  base.close();
  expect(governor.usedBytes).toBe(0);
});

it("refuses at once an admission only a permanent holding's release could serve", async () => {
  const governor = new Governor(32 << 20),
    ordinary = governor.capBytes - governor.reserveBytes;
  const scratch = await governor.admit({
    label: "owner scratch control",
    minimumBytes: 64 << 10,
    spillable: false,
  });
  const unregister = governor.registerPermanent(() => scratch.held);
  // Above the cap less permanent holdings: never servable, so 53200 now (both entry points).
  const settle = <T>(work: Promise<T>) =>
    within(
      work.then(
        () => "served" as const,
        (error: unknown) => error,
      ),
      2_000,
    );
  expect(
    await settle(governor.admit({ label: "whole", minimumBytes: ordinary, spillable: false })),
  ).toMatchObject({ sqlState: "53200", capacity: ordinary - (64 << 10) });
  expect(
    await settle(
      governor.admitGroup({
        label: "whole group",
        grants: [{ label: "whole group", minimumBytes: ordinary, spillable: false }],
      }),
    ),
  ).toBeInstanceOf(MemoryAdmissionError);
  // At the usable cap: served.
  (
    await governor.admit({ label: "fits", minimumBytes: ordinary - (64 << 10), spillable: false })
  ).close();
  unregister();
  scratch.close();
  expect(governor.usedBytes).toBe(0);
});

it("re-checks queued admissions when a permanent holding registers", async () => {
  const governor = new Governor(32 << 20),
    ordinary = governor.capBytes - governor.reserveBytes;
  const blocker = await governor.admit({ label: "blocker", minimumBytes: 1, spillable: false });
  const waiting = governor.admit({ label: "whole", minimumBytes: ordinary, spillable: false }).then(
    () => "served",
    (error: unknown) => error,
  );
  await tick();
  // An owner registers a lifetime holding (here a constant; normally its grant's held bytes).
  const unregister = governor.registerPermanent(() => 64 << 10);
  expect(await within(waiting, 2_000)).toMatchObject({ sqlState: "53200" });
  unregister();
  blocker.close();
  expect(governor.usedBytes).toBe(0);
});

it("serves an admission within the usable cap once the holder chain ahead of it finishes", async () => {
  const governor = new Governor(32 << 20),
    ordinary = governor.capBytes - governor.reserveBytes;
  const persistent = await governor.admit({ label: "floor", minimumBytes: 1, spillable: false });
  const unregister = governor.registerPermanent(() => persistent.held);
  // A holder chain: its base, then repeated growth while holding it.
  const chainBase = await governor.admit({
    label: "chain base",
    minimumBytes: 1,
    spillable: false,
  });
  let served = false;
  const waiting = governor
    .admit({ label: "usable", minimumBytes: ordinary - 1, spillable: false })
    .then((grant) => {
      served = true;
      return grant;
    });
  for (let i = 0; i < 1_000; i++) {
    const growth = await governor.admit({
      label: "growth",
      minimumBytes: 1,
      spillable: false,
      holdsCredit: true,
    });
    growth.close();
  }
  expect(served).toBe(false);
  // The chain finishes and releases: the admission within the usable cap is served.
  chainBase.close();
  const grant = await waiting;
  expect(served).toBe(true);
  grant.close();
  unregister();
  persistent.close();
  expect(governor.usedBytes).toBe(0);
});

// Merge of #31 (preparation credit) with holder priority, lifetime registration and the stall
// guard (GOVERNOR-WAIT-POLICY).
describe("preparation credit interactions", () => {
  it("never reports an admission as possible at once past queued holder growth", async () => {
    const governor = new Governor(8 << 20);
    const ordinary = governor.capBytes - governor.reserveBytes;
    const owner = await governor.admit({
      label: "owner",
      minimumBytes: ordinary - (1 << 20),
      spillable: false,
    });
    const holder = governor.admit({
      label: "holder growth",
      minimumBytes: 2 << 20,
      spillable: false,
      holdsCredit: true,
    });
    // 1 MiB is free, but a holder is queued: a preparation step must not be told "at once".
    expect(governor.canAdmitNow(64 << 10, false)).toBe(false);
    owner.close();
    (await holder).close();
    expect(governor.canAdmitNow(64 << 10, false)).toBe(true);
    expect(governor.usedBytes).toBe(0);
  });

  it("refuses #11's mutators inside a growth observer", async () => {
    const governor = new Governor(8 << 20);
    const grant = await governor.admit({ label: "observed", minimumBytes: 0, spillable: true });
    const seen: unknown[] = [];
    governor.observeGrowth(grant, () => {
      for (const mutate of [
        () => governor.registerPermanent(() => 0),
        () => governor.observeGrants(() => undefined),
      ])
        try {
          mutate();
          seen.push("allowed");
        } catch (error) {
          seen.push(error);
        }
    });
    expect(grant.tryGrow(4096)).toBe(true);
    expect(seen).toHaveLength(2);
    for (const error of seen) expect(error).toBeInstanceOf(GrowthObserverReentryError);
    governor.observeGrowth(grant, null);
    grant.close();
    expect(governor.permanentBytes).toBe(0);
    expect(governor.usedBytes).toBe(0);
  });

  it("counts carving, adoption and credit returned to a parent as holder activity", async () => {
    const governor = new Governor(8 << 20);
    const parent = await governor.admit({
      label: "credit",
      minimumBytes: 1 << 20,
      spillable: false,
    });
    const step = (work: () => void) => {
      const before = governor.activity;
      work();
      return governor.activity - before;
    };
    let carved: ReturnType<Governor["carve"]> = null;
    expect(
      step(() => {
        carved = governor.carve(parent, "carved", false, 4096);
      }),
    ).toBeGreaterThan(0);
    const child = await governor.admit({ label: "child", minimumBytes: 4096, spillable: false });
    expect(step(() => expect(governor.adopt(parent, child)).toBe(true))).toBeGreaterThan(0);
    expect(step(() => child.release(4096))).toBeGreaterThan(0);
    expect(step(() => carved?.close())).toBeGreaterThan(0);
    child.close();
    parent.close();
    expect(governor.usedBytes).toBe(0);
  });
});

// Review 10 P3: unrelated grant activity cannot mask a stalled waiter indefinitely. Two holders'
// growth waits on credit the other holds while an unrelated grant allocates and frees one byte a
// second (progress for the no-progress rule): the hard deadline reports the waiter, naming the rule.
it("reports a waiter past its hard deadline despite unrelated activity", async () => {
  vi.useFakeTimers();
  const previous = Governor.stallAfterMs;
  Governor.stallAfterMs = 10_000;
  const governor = new Governor(32 << 20);
  const room = governor.capBytes - governor.reserveBytes;
  const a = await governor.admit({
    label: "A",
    minimumBytes: Math.floor(room / 2),
    spillable: false,
  });
  const b = await governor.admit({
    label: "B",
    minimumBytes: room - Math.floor(room / 2) - 1024,
    spillable: false,
  });
  const unrelated = await governor.admit({
    label: "unrelated",
    minimumBytes: 1024,
    spillable: false,
  });
  expect(a.allocate(a.held)).toBe(true);
  expect(b.allocate(b.held)).toBe(true);
  const outcomes: unknown[] = ["pending", "pending"];
  const waits = outcomes.map((_, i) =>
    governor
      .admitGroup({
        label: `growth ${String(i)}`,
        holdsCredit: true,
        grants: [{ label: "growth", minimumBytes: 2048, spillable: false }],
      })
      .then(
        (grants) => {
          for (const grant of grants) grant.close();
          outcomes[i] = "served";
        },
        (error: unknown) => {
          outcomes[i] = error;
        },
      ),
  );
  try {
    // Activity every second for 70 s: the no-progress rule never fires; the deadline (60 s) does.
    for (let second = 0; second < 70; second++) {
      expect(unrelated.allocate(1)).toBe(true);
      unrelated.free(1);
      await vi.advanceTimersByTimeAsync(1_000);
      if (second < 55) expect(outcomes[0], `reported at ${String(second)} s`).toBe("pending");
    }
    expect(outcomes[0]).toBeInstanceOf(GovernorStallError);
    expect((outcomes[0] as GovernorStallError).rule).toBe("deadline");
    expect((outcomes[0] as GovernorStallError).message).toContain("Governor stall (deadline)");
  } finally {
    a.free(a.allocated);
    b.free(b.allocated);
    a.close();
    b.close();
    await vi.advanceTimersByTimeAsync(100_000);
    await Promise.all(waits);
    unrelated.close();
    Governor.stallAfterMs = previous;
    vi.useRealTimers();
  }
  expect(governor.usedBytes).toBe(0);
});
