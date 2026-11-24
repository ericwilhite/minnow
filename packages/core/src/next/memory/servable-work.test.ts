/**
 * The owner's queued work in `servableNow`: the safety order, and its record leaving in the drain
 * that serves it.
 *
 * Law (lead ruling 2026-11-24, admission-ordering council R*): from what is available now (the
 * class's capacity less everything held: the statement's holdings, every queued item's held
 * credit, everyone else's), repeatedly take any queued item whose remaining ask fits, count its
 * held credit as returned and add it to what is available; stop when none fits. Returned credit
 * is not kept; an item never reached blocks the statement (its requests queue behind it).
 * Judging each item alone beside the statement counts two items that fit only one at a time
 * (a false "servable", then a stall).
 */
import { expect, it } from "vitest";
import { Governor, type QueuedWork } from "./governor.js";

const MiB = 1 << 20;

async function setting() {
  const governor = new Governor(20 * MiB);
  // The statement holds 8 MiB; the queued work items hold 5 MiB between them.
  const statement = await governor.admit({
    label: "statement",
    minimumBytes: 8 * MiB,
    spillable: false,
  });
  const work = await governor.admit({ label: "work", minimumBytes: 5 * MiB, spillable: false });
  return { governor, statement, work };
}

const item = (ask: number, held: number): QueuedWork => ({
  ask: ask * MiB,
  progress: true,
  held: () => held * MiB,
});

it("releases only the items the safety order reaches: two that fit alone, not together", async () => {
  const { governor, statement, work } = await setting();
  try {
    // Available now: 20 − (8 + 5) = 7. A asks 7: it fits, finishes and returns 2 (available 9).
    // B asks 10: it never fits, though beside the statement alone (20 − 8 = 12) each would.
    const holdings = {
      grants: [statement],
      lent: { grants: [], retained: () => 5 * MiB, work: () => [item(7, 2), item(10, 3)] },
    };
    expect(governor.servableNow(holdings, true), "B blocks the statement").toBeLessThanOrEqual(0);
  } finally {
    statement.close();
    work.close();
  }
});

it("releases an item reached once an earlier one returns its credit", async () => {
  const { governor, statement, work } = await setting();
  try {
    // A (7 ≤ 7) returns 2: available 9. B asks 9: it fits now and returns 3. Both returned: the
    // statement keeps its 8 and the writer's 5 − 5.
    const holdings = {
      grants: [statement],
      lent: { grants: [], retained: () => 5 * MiB, work: () => [item(9, 3), item(7, 2)] },
    };
    expect(governor.servableNow(holdings, true)).toBe(20 * MiB - 8 * MiB);
  } finally {
    statement.close();
    work.close();
  }
});

it("removes a served request's record in the drain that serves it, before it settles", async () => {
  const governor = new Governor(8 * MiB);
  const full = await governor.admit({
    label: "full",
    minimumBytes: governor.usableCapacity(false),
    spillable: false,
  });
  let served = 0;
  const queued = governor.admitGroup({
    label: "queued work",
    grants: [{ label: "queued work", minimumBytes: MiB, spillable: false }],
    onServed: () => {
      served += 1;
    },
  });
  expect(served, "waiting").toBe(0);
  full.close();
  // Synchronously, in the drain the release ran: no microtask between serving and the record.
  expect(served, "served in the same drain").toBe(1);
  for (const grant of await queued) grant.close();
});
