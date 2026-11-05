import { describe, expect, it } from "vitest";
import {
  isVisible,
  MAX_LSN,
  MAX_STATEMENTS,
  PENDING_BASE,
  pendingSeq,
  readBound,
  statementBound,
} from "./snapshot.js";

describe("visibility bounds", () => {
  it("keeps every pending change above every LSN", () => {
    expect(MAX_LSN).toBeLessThan(PENDING_BASE);
    expect(pendingSeq(1)).toBeGreaterThan(readBound(MAX_LSN));
    expect(Number.isSafeInteger(pendingSeq(MAX_STATEMENTS))).toBe(true);
  });

  it("lets a statement see committed changes and earlier statements, not its own", () => {
    const bound = statementBound(3);
    expect(isVisible(MAX_LSN, bound)).toBe(true);
    expect(isVisible(pendingSeq(2), bound)).toBe(true);
    expect(isVisible(pendingSeq(3), bound)).toBe(false);
    expect(isVisible(pendingSeq(1), statementBound(1))).toBe(false);
  });

  it("lets a reader see commits through its LSN and nothing pending", () => {
    expect(isVisible(10, readBound(10))).toBe(true);
    expect(isVisible(11, readBound(10))).toBe(false);
    expect(isVisible(pendingSeq(1), readBound(MAX_LSN))).toBe(false);
  });

  it("refuses bounds outside the ranges", () => {
    expect(() => pendingSeq(0)).toThrow(RangeError);
    expect(() => readBound(MAX_LSN + 1)).toThrow(RangeError);
    expect(() => readBound(-1)).toThrow(RangeError);
  });
});
