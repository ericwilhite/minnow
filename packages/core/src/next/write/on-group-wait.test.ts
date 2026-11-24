/**
 * `onGroupWait` (write/test-support.ts) tells an admission that waited behind another holder from
 * one served at once, one refused at once, and one queued only until the caller's own release.
 *
 * Law (memory/governor.ts): `admitGroup` serves a fitting request inside the call that queues it
 * and a queued one inside the release that makes room, reporting each grant as it makes it
 * (`observeGrants`); it is async, so a refusal (above the usable capacity, an aborted signal) is
 * a rejected promise, never a throw. A refusal made inside the queue (`holdings` above the bound,
 * an abort while queued) settles only after the governor's own `await`, and later still through
 * an async wrapper such as `watchGrants`, the composition write/directory.test.ts uses.
 */
import { expect, it } from "vitest";
import { Governor, type WaitingHoldings } from "../memory/governor.js";
import { onGroupWait, watchGrants } from "./test-support.js";

const MiB = 1024 * 1024;
const LABEL = "group under test";
const group = (bytes: number, signal?: AbortSignal, holdings?: () => WaitingHoldings) => ({
  label: LABEL,
  grants: [{ label: "part", minimumBytes: bytes, spillable: false }],
  ...(signal === undefined ? {} : { signal }),
  ...(holdings === undefined ? {} : { holdings }),
});

/** A holder of all but `free` bytes of the usable capacity. */
const holdAllBut = (governor: Governor, free: number) =>
  governor.admit({
    label: "holder",
    minimumBytes: governor.usableCapacity() - free,
    spillable: false,
  });

it("counts a fitting admission as immediate", async () => {
  const governor = new Governor(32 * MiB);
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => waited++);
  const [grant] = await governor.admitGroup(group(MiB));
  expect(counts).toEqual({ waits: 0, immediate: 1, refused: 0 });
  expect(waited).toBe(0);
  grant?.close();
});

it("counts an admission refused at once as refused, never as a wait", async () => {
  const governor = new Governor(32 * MiB);
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => waited++);
  // Above the usable capacity: a rejected promise, not a throw.
  await expect(governor.admitGroup(group(64 * MiB))).rejects.toThrow();
  const aborted = new AbortController();
  aborted.abort();
  await expect(governor.admitGroup(group(MiB, aborted.signal))).rejects.toThrow();
  expect(counts).toEqual({ waits: 0, immediate: 0, refused: 2 });
  expect(waited).toBe(0);
});

it("counts an admission still queued behind another holder as a wait", async () => {
  const governor = new Governor(32 * MiB);
  const holder = await governor.admit({
    label: "holder",
    minimumBytes: governor.capBytes - governor.reserveBytes,
    spillable: false,
  });
  const counts = onGroupWait(governor, LABEL, () => {
    holder.close();
  });
  const [grant] = await governor.admitGroup(group(MiB));
  expect(counts).toEqual({ waits: 1, immediate: 0, refused: 0 });
  grant?.close();
});

it("does not count an admission its caller's own release serves in the same task", async () => {
  const governor = new Governor(32 * MiB);
  const own = await governor.admit({
    label: "caller's own holding",
    minimumBytes: governor.capBytes - governor.reserveBytes,
    spillable: false,
  });
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => waited++);
  const admitted = governor.admitGroup(group(MiB));
  own.close(); // as the harness returns its startup credit right after starting the open
  const [grant] = await admitted;
  expect(counts).toEqual({ waits: 0, immediate: 1, refused: 0 });
  expect(waited).toBe(0);
  grant?.close();
});

it("counts a refusal made inside the queue (holdings above the bound) as refused", async () => {
  const governor = new Governor(32 * MiB);
  // The statement's own 8 MiB stays held while it waits: 16 MiB more can never be served beside
  // it once the holder leaves only 8 MiB free, so the queue refuses it (`#refuseAboveBound`).
  const own = await holdAllBut(governor, 8 * MiB);
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => waited++);
  const admitted = governor.admitGroup(group(16 * MiB, undefined, () => ({ grants: [own] })));
  await expect(admitted).rejects.toMatchObject({ sqlState: "53200" });
  expect(counts).toEqual({ waits: 0, immediate: 0, refused: 1 });
  expect(waited).toBe(0);
  own.close();
});

it("counts the same way beneath watchGrants' async wrapper", async () => {
  const governor = new Governor(32 * MiB);
  const holder = await holdAllBut(governor, 8 * MiB);
  const observed: number[] = [];
  // Installed first, as write/directory.test.ts does: onGroupWait wraps its async wrapper.
  watchGrants(governor, (grant) => observed.push(grant.held));
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => {
    waited++;
    holder.close();
  });
  await expect(governor.admitGroup(group(64 * MiB)), "above the capacity").rejects.toThrow();
  const aborted = new AbortController();
  aborted.abort();
  await expect(governor.admitGroup(group(MiB, aborted.signal)), "aborted").rejects.toThrow();
  await expect(
    governor.admitGroup(group(16 * MiB, undefined, () => ({ grants: [holder] }))),
    "above its holdings' bound",
  ).rejects.toMatchObject({ sqlState: "53200" });
  expect(counts).toEqual({ waits: 0, immediate: 0, refused: 3 });
  const [fits] = await governor.admitGroup(group(MiB));
  expect(counts).toEqual({ waits: 0, immediate: 1, refused: 3 });
  fits?.close();
  // 16 MiB queues behind the holder until onWait releases it.
  const [queued] = await governor.admitGroup(group(16 * MiB));
  expect(counts).toEqual({ waits: 1, immediate: 1, refused: 3 });
  expect(waited).toBe(1);
  expect(observed).toEqual([MiB, 16 * MiB]);
  queued?.close();
});

it("counts an admission its caller aborts in the same task as refused", async () => {
  const governor = new Governor(32 * MiB);
  const holder = await holdAllBut(governor, 0);
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => waited++);
  const abort = new AbortController();
  const admitted = governor.admitGroup(group(MiB, abort.signal));
  abort.abort(); // queued behind the holder, then cancelled before the caller's task ends
  await expect(admitted).rejects.toThrow();
  expect(counts).toEqual({ waits: 0, immediate: 0, refused: 1 });
  expect(waited, "the competing holder is not released for a cancelled admission").toBe(0);
  holder.close();
});

it("counts an admission another release serves after its caller's task as a wait", async () => {
  const governor = new Governor(32 * MiB);
  const holder = await holdAllBut(governor, 0);
  let waited = 0;
  const counts = onGroupWait(governor, LABEL, () => waited++);
  const admitted = governor.admitGroup(group(MiB));
  // Another task's release, two microtask turns later: the admission waited for it.
  queueMicrotask(() => {
    queueMicrotask(() => {
      holder.close();
    });
  });
  const [grant] = await admitted;
  expect(counts).toEqual({ waits: 1, immediate: 0, refused: 0 });
  expect(waited).toBe(1);
  grant?.close();
});
