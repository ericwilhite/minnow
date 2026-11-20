import { describe, expect, it } from "vitest";
import {
  balancedOrders,
  bracketRepeats,
  holm,
  signTestP,
  targetVerdict,
  MIN_EFFECT,
} from "./next-claim.mts";

describe("next/ speed-claim protocol arithmetic", () => {
  it("computes the exact two-sided sign test", () => {
    expect(signTestP(9, 0)).toBeCloseTo(2 / 512, 12);
    expect(signTestP(7, 2)).toBeCloseTo((2 * (36 + 9 + 1)) / 512, 12);
    expect(signTestP(0, 0)).toBe(1);
    // Nine pairs can never survive a 45-cell correction; fifteen can.
    expect(signTestP(9, 0) * 45).toBeGreaterThan(0.05);
    expect(signTestP(15, 0) * 45).toBeLessThan(0.05);
  });

  it("applies Holm's step-down adjustment", () => {
    expect(holm([0.01, 0.04, 0.03])).toEqual([0.03, 0.06, 0.06]);
    expect(holm([0.5])).toEqual([0.5]);
  });

  it("orders pairs as a balanced, seeded shuffle", () => {
    const orders = balancedOrders(15, 47);
    expect(orders).toHaveLength(15);
    expect(orders.filter((o) => o[0] === "control")).toHaveLength(8);
    expect(balancedOrders(15, 47)).toEqual(orders);
    expect(balancedOrders(15, 48)).not.toEqual(orders);
  });

  it("sizes brackets to at least a second or ten queries", () => {
    expect(bracketRepeats(5)).toBe(10);
    expect(bracketRepeats(250)).toBe(4);
    expect(bracketRepeats(4000)).toBe(1);
  });

  it("requires the minimum effect on top of the paired rule", () => {
    const base = {
      cell: "c",
      repeats: 2,
      controlMedianMs: 100,
      acceptance: {
        medianDeltaMs: -2 * 100 * MIN_EFFECT,
        iqrDeltaMs: 1,
        favoursCandidate: 9,
        favoursControl: 0,
        acceptedGain: true,
        significantRegression: false,
      },
    };
    expect(targetVerdict(base)).toBe("gain");
    expect(targetVerdict({ ...base, acceptance: { ...base.acceptance, medianDeltaMs: -1 } })).toBe(
      "none",
    );
  });
});
