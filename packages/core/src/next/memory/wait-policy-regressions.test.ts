/** Review 7 of #11 (GOVERNOR-WAIT-POLICY): deterministic regressions for the governor's wait
 * policy. The generated schedules are in holder-priority.test.ts. */
import { expect, it } from "vitest";
import { Governor, MemoryAdmissionError, type Grant } from "./governor.js";
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
