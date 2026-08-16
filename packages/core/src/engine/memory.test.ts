import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { QueryMemoryBudgetError, QueryMemoryContext } from "./memory.js";
import { MemoryBlockStore } from "../storage/index.js";

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

  it("tallies bytes without reservation objects and releases them on close", () => {
    const root = new QueryMemoryContext(16);
    const child = root.createChild();
    child.tally(4, "result row");
    child.tally(6, "result row");
    expect(root.usage).toEqual({ budgetBytes: 16, usedBytes: 10, peakBytes: 10 });

    let error: unknown;
    try {
      child.tally(7, "overflow row");
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(QueryMemoryBudgetError);
    expect(error).toMatchObject({ requestedBytes: 7, usedBytes: 10, budgetBytes: 16 });
    expect(root.usage).toEqual({ budgetBytes: 16, usedBytes: 10, peakBytes: 10 });

    child.close();
    expect(root.usage).toEqual({ budgetBytes: 16, usedBytes: 0, peakBytes: 10 });
    expect(() => child.tally(1, "closed")).toThrow("Query memory context is closed");
    root.close();
  });

  /**
   * A block vector's retained size is computed once and reused for every later scan of that
   * block, because the vector is immutable and shared by reference. This pins the invariant the
   * reuse depends on: the size a scan reserves must account for the whole string dictionary and
   * must not drift between the run that computes it and the runs that reuse it.
   */
  it("accounts a string block's dictionary identically on first and repeated scans", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "data",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    const rows = 20_000;
    await database.insertBatch("data", {
      columns: {
        id: Array.from({ length: rows }, (_, index) => index + 1),
        // Every label distinct, so the dictionary dominates the block's retained size.
        label: Array.from({ length: rows }, (_, index) => `label-${String(index)}`),
      },
    });
    const peaks: number[] = [];
    for (let run = 0; run < 3; run += 1) {
      await database.query("SELECT id, label FROM data WHERE id = 12345", {
        memoize: false,
        onStats: (stats) => peaks.push(stats.peakMemoryBytes),
      });
    }
    expect(peaks).toHaveLength(3);
    // Stable across runs: a stale or recomputed size would show up as a differing peak.
    expect(new Set(peaks).size).toBe(1);
    // And large enough to include the dictionary's characters rather than the codes alone.
    expect(peaks[0]).toBeGreaterThan(rows * 16);
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
