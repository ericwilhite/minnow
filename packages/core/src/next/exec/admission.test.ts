import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { ReservedGrant } from "./admission.js";

it("protects a later probe's fixed buffers while a hash build exhausts growth", async () => {
  const governor = new Governor(8 * 1024 * 1024);
  const a = await governor.admit({ label: "build/extra", minimumBytes: 0, spillable: true });
  const b = await governor.admit({ label: "probe/extra", minimumBytes: 0, spillable: true });
  const fixed = await governor.admit({
    label: "fixed",
    minimumBytes: 2 * 1024 * 1024,
    spillable: true,
  });
  const build = new ReservedGrant(fixed, a, 1024 * 1024),
    probe = new ReservedGrant(fixed, b, 1024 * 1024);
  expect(build.allocate(6 * 1024 * 1024)).toBe(true);
  expect(build.allocate(1)).toBe(false);
  expect(probe.allocate(1024 * 1024)).toBe(true);
  expect(governor.usedBytes).toBe(7 * 1024 * 1024);
  build.close();
  probe.close();
  fixed.close();
  expect(governor.usedBytes).toBe(0);
});

it("keeps progress loans independent and frees both accounts exactly", async () => {
  const governor = new Governor(8 * 1024 * 1024);
  const a = await governor.admit({ label: "a/extra", minimumBytes: 0, spillable: true });
  const b = await governor.admit({ label: "b/extra", minimumBytes: 0, spillable: true });
  const fixed = await governor.admit({ label: "fixed", minimumBytes: 200, spillable: true });
  const first = new ReservedGrant(fixed, a, 100),
    second = new ReservedGrant(fixed, b, 100);
  const one = await first.borrow(1000),
    two = await second.borrow(2000);
  expect(first.allocate(1100)).toBe(true);
  expect(second.allocate(2100)).toBe(true);
  first.free(1100);
  second.free(2100);
  one.repay();
  two.repay();
  first.release(100);
  second.close();
  first.close();
  fixed.close();
  expect(governor.usedBytes).toBe(0);
});
