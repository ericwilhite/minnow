import { describe, expect, it } from "vitest";
import { summarizePairDeltas } from "./write-bench-pairs.mts";

describe("write-load paired acceptance", () => {
  it("accepts seven wins using the IQR while retaining an outlier in the full range", () => {
    const deltas = [-30, -10, 60, 80, 100, 110, 120, 130, 400];
    expect(summarizePairDeltas(deltas)).toMatchObject({
      deltas,
      medianGainMs: 100,
      iqrMs: 60,
      fullRangeMs: 430,
      wins: 7,
      losses: 2,
      accepted: true,
      regression: false,
    });
    expect(summarizePairDeltas(deltas.map((delta) => -delta))).toMatchObject({
      medianGainMs: -100,
      iqrMs: 60,
      fullRangeMs: 430,
      wins: 2,
      losses: 7,
      accepted: false,
      regression: true,
    });
  });

  it("requires a strict gain above the IQR and does not call ties improvement", () => {
    expect(summarizePairDeltas([-30, -10, 10, 20, 30, 35, 40, 50, 100])).toMatchObject({
      medianGainMs: 30,
      iqrMs: 30,
      wins: 7,
      accepted: false,
      regression: false,
    });
    expect(summarizePairDeltas(Array<number>(9).fill(0))).toMatchObject({
      accepted: false,
      regression: false,
      wins: 0,
      losses: 0,
    });
  });

  it("refuses missing pairs and non-finite measurements", () => {
    expect(() => summarizePairDeltas(Array<number>(8).fill(100))).toThrow("nine finite deltas");
    expect(() => summarizePairDeltas([...Array<number>(8).fill(100), NaN])).toThrow(
      "nine finite deltas",
    );
  });
});
