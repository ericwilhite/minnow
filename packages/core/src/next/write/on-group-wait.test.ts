/**
 * `onGroupWait` (write/test-support.ts) tells an admission that waited behind another holder from
 * one served at once, one refused at once, and one queued only until the caller's own release.
 *
 * Law (memory/governor.ts): `admitGroup` serves a fitting request inside the call that queues it
 * and a queued one inside the release that makes room, reporting each grant as it makes it
 * (`observeGrants`); it is async, so a refusal (above the usable capacity, an aborted signal) is
 * a rejected promise, never a throw.
 */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { onGroupWait } from "./test-support.js";

const MiB = 1024 * 1024;
const LABEL = "group under test";
const group = (bytes: number, signal?: AbortSignal) => ({
  label: LABEL,
  grants: [{ label: "part", minimumBytes: bytes, spillable: false }],
  ...(signal === undefined ? {} : { signal }),
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
