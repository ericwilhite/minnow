/**
 * GOVERNOR-WAIT-POLICY point 5 (`MemoryGovernor.servableNow`, `NativeGrantGroupOptions.servable`):
 * the most a request could ever be served while its statement waits is the usable capacity less
 * what that statement keeps held (its own grants, and what its writer retains while it holds the
 * turn). A waiting group above it is refused with 53200, on its first drain or later when the
 * holdings become known, and never before the reclaim that drain asked for has completed. A group
 * at or below it waits for other holders. And a stall report names every byte `usedBytes` counts:
 * grants, caches and lifetime holdings.
 */
import { afterEach, expect, it } from "vitest";
import { Governor, GovernorStallError, reserveFor, type WaitingHoldings } from "./governor.js";

const MiB = 1 << 20;
const CAP = 32 * MiB;
const USABLE = CAP - reserveFor(CAP);
const PENDING = Symbol("pending");
const within = <T>(work: Promise<T>, ms = 200) =>
  Promise.race([
    work,
    new Promise<typeof PENDING>((resolve) => {
      setTimeout(() => {
        resolve(PENDING);
      }, ms);
    }),
  ]);

afterEach(() => {
  Governor.stallAfterMs = null;
});

/** A group carrying its statement's `servable` bound; resolves "served" or the error. */
function group(governor: Governor, bytes: number, holdings: WaitingHoldings) {
  return governor
    .admitGroup({
      label: "q/fixed-buffers",
      grants: [{ label: "part", minimumBytes: bytes, spillable: false }],
      holdings: () => holdings,
    })
    .then(
      (grants) => {
        for (const grant of grants) grant.close();
        return "served" as const;
      },
      (error: unknown) => error,
    );
}

it("counts the statement's grants and the writer's retained bytes, not its whole holdings", async () => {
  const governor = new Governor(CAP);
  const own = await governor.admit({ label: "own", minimumBytes: 2 * MiB, spillable: false });
  const writer = await governor.admit({ label: "writer", minimumBytes: 9 * MiB, spillable: true });
  const holdings = { grants: [own], lent: { grants: [writer], retained: () => 3 * MiB } };
  expect(governor.servableNow(holdings)).toBe(USABLE - 5 * MiB);
  // A complete step with private progress may use the whole cap.
  expect(governor.servableNow(holdings, true)).toBe(CAP - 5 * MiB);
  // Registered lifetime holdings never release.
  const unregister = governor.registerPermanent(() => 6 * MiB, "lifetime");
  expect(governor.servableNow(holdings)).toBe(USABLE - 11 * MiB);
  unregister();
  own.close();
  writer.close();
  expect(governor.usedBytes).toBe(0);
});

it("decides only after the reclaim its drain asked for: the statement's idle credit returns first", async () => {
  const governor = new Governor(CAP);
  const own = await governor.admit({ label: "own", minimumBytes: 10 * MiB, spillable: true });
  expect(own.allocate(2 * MiB)).toBe(true);
  let asked = 0;
  own.onReclaimRequest(async () => {
    asked++;
    await Promise.resolve();
    own.release(own.held - own.allocated);
  });
  // Beside the 10 MiB held it could never fit; beside the 2 MiB in use it fits now.
  expect(await within(group(governor, USABLE - 2 * MiB, { grants: [own] }))).toBe("served");
  expect(asked).toBe(1);
  own.free(2 * MiB);
  own.close();
  expect(governor.usedBytes).toBe(0);
});

it("refuses at once what never fits beside the statement, though another holder could release", async () => {
  const governor = new Governor(CAP);
  const own = await governor.admit({ label: "own", minimumBytes: 6 * MiB, spillable: false });
  const other = await governor.admit({ label: "other", minimumBytes: 4 * MiB, spillable: false });
  const outcome = await within(group(governor, USABLE - 6 * MiB + 1, { grants: [own] }));
  expect(outcome).toMatchObject({
    name: "MemoryAdmissionError",
    sqlState: "53200",
    label: "q/fixed-buffers",
  });
  other.close();
  own.close();
  expect(governor.usedBytes).toBe(0);
});

it("waits while another holder could release, and is served when it does", async () => {
  const governor = new Governor(CAP);
  const own = await governor.admit({ label: "own", minimumBytes: 2 * MiB, spillable: false });
  const other = await governor.admit({ label: "other", minimumBytes: 8 * MiB, spillable: false });
  const waiting = group(governor, USABLE - 4 * MiB, { grants: [own] });
  expect(await within(waiting)).toBe(PENDING);
  other.close();
  expect(await within(waiting)).toBe("served");
  own.close();
  expect(governor.usedBytes).toBe(0);
});

it("decides a waiting group when the writer's holdings become known", async () => {
  const governor = new Governor(CAP);
  const writer = await governor.admit({
    label: "writer",
    minimumBytes: 10 * MiB,
    spillable: false,
  });
  expect(writer.allocate(10 * MiB)).toBe(true);
  // While the writer runs work of its own it counts nothing (its holdings are not yet known).
  let known = false;
  const lent = { grants: [writer], retained: () => (known ? 10 * MiB : 0) };
  const waiting = group(governor, USABLE - 5 * MiB, { grants: [], lent });
  expect(await within(waiting)).toBe(PENDING);
  known = true;
  // The writer wakes the queue when its work ends (`notifyReclaimable`).
  writer.release(0);
  expect(await within(waiting)).toMatchObject({ name: "MemoryAdmissionError", sqlState: "53200" });
  writer.free(10 * MiB);
  writer.close();
  expect(governor.usedBytes).toBe(0);
});

it("names caches and lifetime holdings in a stall report, leaving nothing unattributed", async () => {
  Governor.stallAfterMs = 100;
  const governor = new Governor(CAP);
  const scratch = await governor.admit({ label: "scratch", minimumBytes: MiB, spillable: false });
  const unregister = governor.registerPermanent(() => scratch.held, "native scratch control");
  const cache = governor.registerCache({ label: "test cache", evictableBytes: 0, evict: () => 0 });
  expect(cache.charge(2 * MiB)).toBe(true);
  const holder = await governor.admit({
    label: "holder",
    minimumBytes: 20 * MiB,
    spillable: false,
  });
  const error: unknown = await governor
    .admit({ label: "waiter", minimumBytes: 8 * MiB, spillable: false })
    .then(
      () => null,
      (reason: unknown) => reason,
    );
  expect(error).toBeInstanceOf(GovernorStallError);
  const stall = error as GovernorStallError;
  expect(stall.accounts.caches).toEqual([{ label: "test cache", held: 2 * MiB, evictable: 0 }]);
  expect(stall.accounts.permanent).toEqual([{ label: "native scratch control", bytes: MiB }]);
  expect(stall.accounts.unattributed).toBe(0);
  expect(stall.message).toContain("test cache 2097152B held");
  expect(stall.message).toContain("native scratch control 1048576B");
  cache.release(2 * MiB);
  holder.close();
  unregister();
  scratch.close();
  expect(governor.usedBytes).toBe(0);
});

it("asks a reclaimer again after its owner's wake, so a covered release is not missed", async () => {
  const governor = new Governor(CAP);
  const writer = await governor.admit({ label: "writer", minimumBytes: 4 * MiB, spillable: true });
  expect(writer.allocate(4 * MiB)).toBe(true);
  // The reclaimer frees nothing until its state allows (a publication covers its entries).
  let covered = false,
    asked = 0;
  writer.onReclaimRequest(async () => {
    asked++;
    await Promise.resolve();
    if (!covered) return;
    writer.free(4 * MiB);
    writer.release(4 * MiB);
  });
  const waiting = governor
    .admitGroup({
      label: "read",
      grants: [{ label: "read", minimumBytes: USABLE - MiB, spillable: false }],
    })
    .then((grants) => {
      for (const grant of grants) grant.close();
      return "served" as const;
    });
  expect(await within(waiting)).toBe(PENDING);
  expect(asked).toBe(1);
  covered = true;
  // The owner's wake (`notifyReclaimable`): queued admissions ask its reclaimer once more.
  writer.release(0);
  expect(await within(waiting)).toBe("served");
  expect(asked).toBe(2);
  writer.close();
  expect(governor.usedBytes).toBe(0);
});

it("refuses a group above its bound behind another waiter, then serves the waiter its release frees", async () => {
  const governor = new Governor(CAP);
  const writer = await governor.admit({
    label: "writer",
    minimumBytes: 10 * MiB,
    spillable: false,
  });
  // A foreign admission at the head needs the writer's credit back.
  const foreign = governor
    .admit({ label: "foreign", minimumBytes: USABLE, spillable: false })
    .then((grant) => {
      grant.close();
      return "served" as const;
    });
  // The writer's statement asks, behind it, for more than could ever fit beside its writer.
  const lent = { grants: [writer], retained: () => writer.held };
  const mine = group(governor, USABLE - 10 * MiB + 1, { grants: [], lent });
  expect(await within(mine)).toMatchObject({ name: "MemoryAdmissionError", sqlState: "53200" });
  // The refused statement ends and its writer's credit returns: the foreign waiter is served.
  expect(await within(foreign)).toBe(PENDING);
  writer.close();
  expect(await within(foreign)).toBe("served");
  expect(governor.usedBytes).toBe(0);
});

it("keeps FIFO for a group within its bound behind another waiter", async () => {
  const governor = new Governor(CAP);
  const other = await governor.admit({ label: "other", minimumBytes: 10 * MiB, spillable: false });
  const order: string[] = [];
  const first = governor
    .admit({ label: "first", minimumBytes: USABLE - 5 * MiB, spillable: false })
    .then((grant) => {
      order.push("first");
      grant.close();
    });
  const second = group(governor, USABLE - 5 * MiB, { grants: [] }).then((outcome) => {
    order.push("second");
    return outcome;
  });
  expect(await within(second)).toBe(PENDING);
  other.close();
  await first;
  expect(await within(second)).toBe("served");
  expect(order).toEqual(["first", "second"]);
  expect(governor.usedBytes).toBe(0);
});

it("counts the statement's own spillable holding once its reclaim returned what it could", async () => {
  const governor = new Governor(CAP);
  // Spillable, but its reclaim returns nothing (all of it is in use).
  const own = await governor.admit({ label: "own", minimumBytes: 10 * MiB, spillable: true });
  expect(own.allocate(10 * MiB)).toBe(true);
  let asked = 0;
  own.onReclaimRequest(async () => {
    asked++;
    await Promise.resolve();
  });
  // Before its reclaim is asked it may yet return everything; after, the 10 MiB stays.
  const outcome = await within(group(governor, USABLE - 5 * MiB, { grants: [own] }));
  expect(asked).toBe(1);
  expect(outcome).toMatchObject({ name: "MemoryAdmissionError", sqlState: "53200" });
  own.free(10 * MiB);
  own.close();
  expect(governor.usedBytes).toBe(0);
});

it("serves a waiter that was behind a group refused for its bound", async () => {
  const governor = new Governor(CAP);
  const own = await governor.admit({ label: "own", minimumBytes: 8 * MiB, spillable: false });
  const other = await governor.admit({ label: "other", minimumBytes: 4 * MiB, spillable: false });
  // At the head, a group that never fits beside its statement's 8 MiB: refused.
  const refused = group(governor, USABLE - 8 * MiB + 1, { grants: [own] });
  // Behind it, a plain admission that fits once `other` releases.
  const behind = governor
    .admit({ label: "behind", minimumBytes: USABLE - 8 * MiB, spillable: false })
    .then((grant) => {
      grant.close();
      return "served" as const;
    });
  expect(await within(refused)).toMatchObject({ sqlState: "53200" });
  other.close();
  expect(await within(behind)).toBe("served");
  own.close();
  expect(governor.usedBytes).toBe(0);
});
