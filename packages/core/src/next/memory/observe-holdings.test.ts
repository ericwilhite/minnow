/**
 * `Governor.observeHoldings` reports exactly a grant's holdings.
 *
 * Law (the `Grant` contract, memory/governor.ts): `held` is "minimum + growth + loan"; an
 * outstanding loan is part of `held` (`loaned` only says how much of it must leave through
 * `repay`). So the sum of the reported deltas equals `held` after every operation, including a
 * loan's issue and repayment. Suites that measure peak holdings (testing/wide-row-read-need) sum
 * these deltas; a loan reported once as `held` and again as `loaned` would double it.
 */
import { afterEach, expect, it } from "vitest";
import { Governor, type Grant } from "./governor.js";

const MiB = 1024 * 1024;

afterEach(() => {
  Governor.observeHoldings = null;
});

it("reports a grant's held bytes exactly, through admission, loan, growth and close", async () => {
  const reported = new Map<Grant, number>();
  Governor.observeHoldings = (grant, delta) => {
    reported.set(grant, (reported.get(grant) ?? 0) + delta);
  };
  const governor = new Governor(16 * MiB);
  const grant = governor.tryAdmit({ label: "probe", minimumBytes: 1024, spillable: true });
  if (grant === null) throw new Error("admission did not fit");
  const observed = () => reported.get(grant) ?? 0;
  expect(observed(), "admitted").toBe(1024);

  const loan = await grant.borrow(1024);
  expect(grant.loaned).toBe(1024);
  expect(grant.held, "a loan is part of held").toBe(2048);
  expect(observed(), "admitted and borrowed: exactly held, the loan once").toBe(grant.held);

  expect(grant.allocate(2048)).toBe(true);
  expect(observed(), "allocating moves no credit").toBe(2048);
  grant.free(1024);
  loan.repay();
  expect(grant.held).toBe(1024);
  expect(observed(), "repaid").toBe(grant.held);

  expect(grant.tryGrow(4096)).toBe(true);
  expect(observed(), "grown").toBe(grant.held);
  grant.free(1024);
  grant.release(4096);
  expect(observed(), "released").toBe(grant.held);

  await grant.borrow(512);
  grant.close();
  expect(grant.held).toBe(0);
  expect(observed(), "closed with a loan outstanding").toBe(0);
});

it("reports nothing for a grant created while the hook is unset", () => {
  const governor = new Governor(16 * MiB);
  const grant = governor.tryAdmit({ label: "plain", minimumBytes: 1024, spillable: true });
  if (grant === null) throw new Error("admission did not fit");
  let calls = 0;
  Governor.observeHoldings = () => {
    calls++;
  };
  expect(grant.tryGrow(1024)).toBe(true);
  grant.close();
  expect(calls).toBe(0);
});
