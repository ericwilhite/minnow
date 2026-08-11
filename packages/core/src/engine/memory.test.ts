import { describe, expect, it } from "vitest";
import { QueryMemoryBudgetError, QueryMemoryContext } from "./memory.js";

describe("query memory context", () => {
  it("tracks shared child reservations and peak bytes", () => {
    const root = new QueryMemoryContext(16);
    const retained = root.reserve(4, "retained vectors");
    const child = root.createChild();
    const temporary = child.reserve(8, "scan batch");
    expect(root.usage).toEqual({ budgetBytes: 16, usedBytes: 12, peakBytes: 12 });

    temporary.release();
    expect(root.usage).toEqual({ budgetBytes: 16, usedBytes: 4, peakBytes: 12 });
    retained.release();
    expect(root.usage).toEqual({ budgetBytes: 16, usedBytes: 0, peakBytes: 12 });
  });

  it("fails atomically at the exact budget boundary", () => {
    const context = new QueryMemoryContext(8);
    context.reserve(8, "exact");
    let error: unknown;
    try {
      context.reserve(1, "overflow");
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(QueryMemoryBudgetError);
    expect(error).toMatchObject({
      name: "QueryMemoryBudgetError",
      label: "overflow",
      requestedBytes: 1,
      usedBytes: 8,
      budgetBytes: 8,
    });
    expect(context.usage).toEqual({ budgetBytes: 8, usedBytes: 8, peakBytes: 8 });
  });

  it("recursively releases children and treats close and release as idempotent", () => {
    const root = new QueryMemoryContext(32);
    const child = root.createChild();
    const grandchild = child.createChild();
    const reservation = grandchild.reserve(7, "nested");
    root.close();
    root.close();
    reservation.release();
    expect(root.usage).toEqual({ budgetBytes: 32, usedBytes: 0, peakBytes: 7 });
    expect(() => child.reserve(1, "closed")).toThrow("Query memory context is closed");
  });

  it("rejects invalid budgets and reservation sizes", () => {
    for (const value of [-1, 0.5, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => new QueryMemoryContext(value)).toThrow(RangeError);
    }
    const context = new QueryMemoryContext();
    for (const value of [-1, 0.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => context.reserve(value, "invalid")).toThrow(RangeError);
    }
  });
});
