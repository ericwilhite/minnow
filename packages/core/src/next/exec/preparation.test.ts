/**
 * The preparation credit (exec/preparation.ts). Laws (memory governor contract,
 * memory/governor.ts): no hold-and-wait under FIFO admission, and a typed 53200 only above the cap.
 * A would-wait releases everything, waits holding nothing for a credit that grows geometrically,
 * so a query restarts at most ceil(log2(capacity / first request)) + 1 times. An attempt that
 * holds nothing waits in place (already a wait holding nothing).
 */
import { afterEach, expect, it, vi } from "vitest";
import { Governor, MemoryAdmissionError, type Grant } from "../memory/governor.js";
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
      if (first === 0) first = wait.held + wait.requested;
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
