/**
 * The preparation credit (exec/preparation.ts). Laws (memory governor contract,
 * memory/governor.ts): no hold-and-wait under FIFO admission, and a typed 53200 only above the cap.
 * A would-wait releases everything, waits holding nothing for a credit that grows geometrically,
 * so a query restarts at most ceil(log2(capacity / first request)) + 1 times. An attempt that
 * holds nothing waits in place (already a wait holding nothing).
 */
import { afterEach, expect, it, vi } from "vitest";
import {
  Governor,
  GrowthObserverReentryError,
  MemoryAdmissionError,
  type Grant,
} from "../memory/governor.js";
import { PreparationGovernor, PreparationWouldWait } from "./preparation.js";

afterEach(() => {
  vi.restoreAllMocks();
});

/** One preparation: admit `sizes` in order; on a would-wait, release all, regrow, start over. */
async function prepare(
  facade: PreparationGovernor,
  sizes: readonly number[],
  signal: AbortSignal,
  onRegrow?: (wait: PreparationWouldWait) => void,
): Promise<Grant[]> {
  for (;;) {
    const grants: Grant[] = [];
    try {
      for (const [i, minimumBytes] of sizes.entries())
        grants.push(
          await facade.admit({ label: `part#${String(i)}`, minimumBytes, spillable: false }),
        );
      facade.finish();
      return grants;
    } catch (error) {
      for (const grant of grants) grant.close();
      if (!(error instanceof PreparationWouldWait)) throw error;
      // Attempts are bounded; a preparation that keeps restarting fails here (an unbounded
      // loop of resolved promises would starve the test's own timeout).
      if (facade.attempts > 64)
        throw new Error(`preparation did not terminate: ${String(facade.attempts)} attempts`, {
          cause: error,
        });
      onRegrow?.(error);
      await facade.regrow(error, signal);
    }
  }
}

/** Every immediate admission is refused, as under constant contention; credit waits still work. */
function contended(governor: Governor): void {
  vi.spyOn(governor, "tryAdmit").mockReturnValue(null);
}

it("grows the credit geometrically: restarts are bounded by log2(capacity / first request) + 1", async () => {
  const governor = new Governor(64 << 20);
  const capacity = governor.capBytes - governor.reserveBytes;
  const parts = 16,
    size = Math.floor(capacity / parts);
  contended(governor);
  const facade = new PreparationGovernor(governor);
  const needs: number[] = [];
  let first = 0;
  const grants = await prepare(
    facade,
    Array.from({ length: parts }, () => size),
    new AbortController().signal,
    (wait) => {
      if (first === 0) first = wait.needed;
      needs.push(facade.needBytes);
    },
  );
  expect(grants.map((grant) => grant.held)).toEqual(Array.from({ length: parts }, () => size));
  const restarts = facade.attempts - 1;
  // The first part waits in place (nothing held); the first restart asks for two parts. Doubling
  // reaches all 16 parts in log2(16 / 2) + 1 restarts.
  expect(first).toBe(2 * size);
  expect(restarts).toBeLessThanOrEqual(Math.ceil(Math.log2(capacity / first)) + 1);
  expect(restarts).toBe(4);
  // Strictly growing requests (needs[k] is the request before the k-th regrow).
  for (let k = 1; k < needs.length; k++) expect(needs[k] ?? 0).toBeGreaterThan(needs[k - 1] ?? 0);
  for (const grant of grants) grant.close();
  facade.close();
  expect(governor.usedBytes).toBe(0);
});

it("reuses freed preparation bytes across phases: the credit needs only the peak (review repro)", async () => {
  // Recipe cap/8, then scratch 3cap/4 (closed), then the reservation 3cap/4: the peak is 7cap/8.
  // A FIFO competitor asks for one byte more than the free room at every attempt.
  const governor = new Governor(16 << 20);
  const usable = governor.capBytes - governor.reserveBytes;
  const facade = new PreparationGovernor(governor);
  const needs: number[] = [];
  let first = 0,
    done = false;
  for (let attempt = 0; attempt < 12 && !done; attempt++) {
    const owned: Grant[] = [];
    let competitor: Promise<Grant> | undefined;
    try {
      owned.push(
        await facade.admit({ label: "recipe", minimumBytes: usable / 8, spillable: false }),
      );
      competitor = governor.admit({
        label: "competitor",
        minimumBytes: usable - governor.usedBytes + 1,
        spillable: false,
      });
      const scratch = await facade.admit({
        label: "scratch",
        minimumBytes: (usable * 3) / 4,
        spillable: false,
      });
      scratch.close();
      owned.push(
        await facade.admit({
          label: "reservation",
          minimumBytes: (usable * 3) / 4,
          spillable: false,
        }),
      );
      facade.finish();
      done = true;
    } catch (error) {
      if (!(error instanceof PreparationWouldWait)) throw error;
      for (const grant of owned.splice(0)) grant.close();
      if (first === 0) first = error.needed;
      const next = facade.regrow(error, new AbortController().signal);
      if (competitor !== undefined) (await competitor).close();
      await next;
      needs.push(facade.needBytes);
      continue;
    }
    for (const grant of owned) grant.close();
    (await competitor).close();
  }
  expect(done, `needs ${JSON.stringify(needs)}`).toBe(true);
  expect(first).toBe((usable * 7) / 8);
  expect(facade.attempts - 1).toBeLessThanOrEqual(Math.ceil(Math.log2(usable / first)) + 1);
  expect(facade.attempts - 1).toBe(1);
  facade.close();
  expect(governor.usedBytes).toBe(0);
});

it("never asks for more than the usable capacity: lifetime holdings cap the doubling (review repro)", async () => {
  const MiB = 1 << 20;
  const governor = new Governor(16 * MiB);
  // Lifetime holdings (never returned while the owner lives) leave usable = ordinary - 4 MiB.
  const lifetime = governor.tryAdmit({
    label: "lifetime",
    minimumBytes: 4 * MiB,
    spillable: false,
  });
  if (lifetime === null) throw new Error("lifetime");
  // Lifetime holdings, as #11 will register them: the usable capacity is 4 MiB smaller.
  const facade = new PreparationGovernor(governor);
  vi.spyOn(facade, "usableCapacity").mockImplementation(
    (progress = false) =>
      (progress ? governor.capBytes : governor.capBytes - governor.reserveBytes) - 4 * MiB,
  );
  expect(facade.usableCapacity()).toBe(10 * MiB);
  const wait = (needed: number) =>
    new PreparationWouldWait("probe", MiB, false, needed - MiB, [
      { label: "probe", minimumBytes: MiB, spillable: false },
    ]);
  const controller = new AbortController();
  const signal = controller.signal;
  await facade.regrow(wait(6 * MiB), signal);
  // The next true need is 7 MiB; doubling (12 MiB) is capped at the usable 10 MiB and fits now.
  const next = facade.regrow(wait(7 * MiB), signal);
  const outcome = await Promise.race([
    next.then(() => "admitted"),
    new Promise((resolve) => setTimeout(() => resolve("still waiting"), 500)),
  ]);
  if (outcome !== "admitted") controller.abort(new Error("still waiting"));
  await next.catch(() => undefined);
  expect(outcome, `credit request ${String(facade.needBytes)} with 10 MiB usable`).toBe("admitted");
  expect(facade.needBytes).toBe(10 * MiB);
  expect(facade.credit?.held).toBe(10 * MiB);
  // Above the usable capacity: the typed refusal, not a wait.
  await expect(facade.regrow(wait(10 * MiB + 1), signal)).rejects.toMatchObject({
    sqlState: "53200",
  });
  facade.close();
  lifetime.close();
  expect(governor.usedBytes).toBe(0);
});

it("counts growth after admission in the peak need (review repro)", async () => {
  // Retain 1 MiB, admit 1 MiB of spillable scratch, grow it to 7 MiB, close it, then block on a
  // 1 MiB admission: the attempt's peak was 8 MiB, so that is the next request.
  const MiB = 1 << 20;
  const governor = new Governor(16 * MiB);
  const facade = new PreparationGovernor(governor);
  const recipe = await facade.admit({ label: "recipe", minimumBytes: MiB, spillable: false });
  const scratch = await facade.admit({ label: "scratch", minimumBytes: MiB, spillable: true });
  expect(scratch.allocate(7 * MiB)).toBe(true);
  expect(facade.heldBytes).toBe(8 * MiB);
  scratch.free(7 * MiB);
  scratch.close();
  const competitor = governor.admit({
    label: "competitor",
    minimumBytes: governor.capBytes - governor.reserveBytes,
    spillable: false,
  });
  const blocked = facade.admit({ label: "next", minimumBytes: MiB, spillable: false });
  await expect(blocked).rejects.toBeInstanceOf(PreparationWouldWait);
  const wait = await blocked.catch((error: unknown) => error);
  expect(wait).toMatchObject({ held: MiB, requested: MiB, needed: 8 * MiB });
  recipe.close();
  (await competitor).close();
  facade.close();
  expect(governor.usedBytes).toBe(0);
});

it("counts a loan served from pooled idle credit in the peak, once it is complete (review repro)", async () => {
  const MiB = 1 << 20;
  const governor = new Governor(16 * MiB);
  const facade = new PreparationGovernor(governor);
  const recipe = await facade.admit({ label: "recipe", minimumBytes: MiB, spillable: false });
  const scratch = await facade.admit({ label: "scratch", minimumBytes: MiB, spillable: true });
  const pool = governor.tryAdmit({ label: "idle", minimumBytes: MiB, spillable: true });
  if (pool === null) throw new Error("pool");
  await governor.admitGroup({ label: "idle", idleCredit: pool, grants: [] });
  // The loan is served from the idle pool: the grant's holdings grow by the whole loan.
  const loan = await scratch.borrow(MiB);
  const peak = facade.heldBytes;
  expect(peak).toBe(3 * MiB);
  loan.repay();
  scratch.close();
  pool.close();
  const competitor = governor.admit({
    label: "competitor",
    minimumBytes: governor.capBytes - governor.reserveBytes,
    spillable: false,
  });
  const wait = await facade
    .admit({ label: "next", minimumBytes: MiB, spillable: false })
    .catch((error: unknown) => error);
  expect(wait).toBeInstanceOf(PreparationWouldWait);
  expect(wait).toMatchObject({ needed: peak });
  recipe.close();
  (await competitor).close();
  facade.close();
  expect(governor.usedBytes).toBe(0);
});

it("notifies growth once per complete operation, including an adopted child's return", () => {
  const governor = new Governor(16 << 20);
  const take = (minimumBytes: number) => {
    const grant = governor.tryAdmit({ label: "test", minimumBytes, spillable: true });
    if (grant === null) throw new Error("grant");
    return grant;
  };
  const parent = take(1024),
    child = take(1024);
  const seen: Array<{ held: number; allocated: number }> = [];
  governor.observeGrowth(parent, () =>
    seen.push({ held: parent.held, allocated: parent.allocated }),
  );
  // Growth inside allocate: one notification, after the allocation is recorded.
  expect(parent.allocate(4096)).toBe(true);
  expect(seen).toEqual([{ held: 4096, allocated: 4096 }]);
  // An adopted child's returned bytes raise the parent's holdings: one notification.
  expect(governor.adopt(parent, child)).toBe(true);
  child.close();
  expect(seen).toEqual([
    { held: 4096, allocated: 4096 },
    { held: 5120, allocated: 4096 },
  ]);
  parent.free(4096);
  parent.close();
  expect(governor.usedBytes).toBe(0);
});

it("refuses a growth observer that changes state, leaving the grant consistent", () => {
  const governor = new Governor(16 << 20);
  const grant = governor.tryAdmit({ label: "test", minimumBytes: 1024, spillable: true });
  if (grant === null) throw new Error("grant");
  governor.observeGrowth(grant, () => grant.close());
  expect(() => grant.allocate(2048)).toThrow(GrowthObserverReentryError);
  // The refused close changed nothing: the completed allocation stands and is charged.
  expect(grant).toMatchObject({ closed: false, held: 2048, allocated: 2048 });
  expect(governor.usedBytes).toBe(2048);
  governor.observeGrowth(grant, null);
  grant.free(2048);
  grant.close();
  expect(governor.usedBytes).toBe(0);
});

it("waits for each credit holding nothing", async () => {
  const governor = new Governor(64 << 20);
  contended(governor);
  const facade = new PreparationGovernor(governor);
  const admitGroup = governor.admitGroup.bind(governor);
  let waits = 0;
  vi.spyOn(governor, "admitGroup").mockImplementation((options) => {
    if (options.grants.some((part) => part.label.endsWith("/preparation-credit"))) {
      waits++;
      expect(facade.credit, "the old credit was released before waiting").toBeNull();
      expect(facade.heldBytes, "the attempt's grants were released before waiting").toBe(0);
      expect(governor.usedBytes, "nothing of this query is held during the wait").toBe(0);
    }
    return admitGroup(options);
  });
  const grants = await prepare(facade, [4096, 1 << 20, 3 << 20], new AbortController().signal);
  expect(waits).toBe(facade.attempts - 1);
  expect(waits).toBeGreaterThan(0);
  for (const grant of grants) grant.close();
  facade.close();
  expect(governor.usedBytes).toBe(0);
});

it("is the typed 53200 only when the attempt needs more than the cap at once", async () => {
  const governor = new Governor(16 << 20);
  const capacity = governor.capBytes - governor.reserveBytes;
  contended(governor);
  // Exactly the capacity in two parts: admitted (never refused at or below the cap).
  const fits = new PreparationGovernor(governor);
  const half = capacity / 2;
  expect(Number.isInteger(half)).toBe(true);
  const grants = await prepare(fits, [half, half], new AbortController().signal);
  for (const grant of grants) grant.close();
  fits.close();
  // One byte more than the capacity, in total: typed refusal, and nothing remains held.
  const over = new PreparationGovernor(governor);
  const refused = prepare(over, [half, half + 1], new AbortController().signal);
  await expect(refused).rejects.toBeInstanceOf(MemoryAdmissionError);
  await expect(refused).rejects.toMatchObject({ sqlState: "53200" });
  over.close();
  expect(governor.usedBytes).toBe(0);
});

it("an attempt holding nothing waits in place, without a restart", async () => {
  const governor = new Governor(16 << 20);
  const capacity = governor.capBytes - governor.reserveBytes;
  const blocker = governor.tryAdmit({ label: "blocker", minimumBytes: capacity, spillable: false });
  if (blocker === null) throw new Error("blocker");
  const facade = new PreparationGovernor(governor);
  const waiting = prepare(facade, [4096], new AbortController().signal);
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(governor.usedBytes, "the waiting attempt holds nothing").toBe(capacity);
  blocker.close();
  const grants = await waiting;
  expect(facade.attempts).toBe(1);
  for (const grant of grants) grant.close();
  facade.close();
  expect(governor.usedBytes).toBe(0);
});

it("cancelling during the credit wait leaves no credit behind", async () => {
  const governor = new Governor(16 << 20);
  const capacity = governor.capBytes - governor.reserveBytes;
  // Another holder leaves room for the first part only, so the credit request queues.
  const blocker = governor.tryAdmit({
    label: "blocker",
    minimumBytes: capacity - 4096,
    spillable: false,
  });
  if (blocker === null) throw new Error("blocker");
  const facade = new PreparationGovernor(governor);
  const controller = new AbortController();
  const waiting = prepare(facade, [4096, 8192], controller.signal);
  // Let the attempt fail and its credit request queue.
  for (let i = 0; i < 10 && facade.attempts === 1; i++) await Promise.resolve();
  expect(facade.attempts).toBe(2);
  controller.abort(new Error("cancelled"));
  await expect(waiting).rejects.toThrow("cancelled");
  blocker.close();
  // The cancelled request left the queue: nothing is admitted to the facade afterwards.
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(facade.credit).toBeNull();
  expect(governor.usedBytes).toBe(0);
  facade.close();
  // A later admission is not stuck behind a zombie request.
  const later = governor.tryAdmit({ label: "later", minimumBytes: capacity, spillable: false });
  expect(later).not.toBeNull();
  later?.close();
});

it("a credit admitted after cancellation is returned at once", async () => {
  const governor = new Governor(16 << 20);
  const facade = new PreparationGovernor(governor);
  const controller = new AbortController();
  const admitGroup = governor.admitGroup.bind(governor);
  vi.spyOn(governor, "admitGroup").mockImplementation(async (options) => {
    const grants = await admitGroup(options);
    // Cancellation lands between the admission and the facade adopting the credit.
    if (options.grants.some((part) => part.label.endsWith("/preparation-credit")))
      controller.abort(new Error("late cancel"));
    return grants;
  });
  await expect(
    facade.regrow(
      new PreparationWouldWait("probe", 4096, false, 0, [
        { label: "probe", minimumBytes: 4096, spillable: false },
      ]),
      controller.signal,
    ),
  ).rejects.toThrow("late cancel");
  expect(facade.credit).toBeNull();
  expect(governor.usedBytes).toBe(0);
  facade.close();
});
