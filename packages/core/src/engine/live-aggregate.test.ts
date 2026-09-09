import { describe, expect, it, vi } from "vitest";
import { LiveAggregate } from "./live-aggregate.js";
import { compileQuery, type QueryResult } from "./query.js";

function seed(count: number) {
  const plan = LiveAggregate.plan(compileQuery("SELECT COUNT(*) AS n, SUM(x) AS s FROM t"), "t.id");
  if (plan === undefined) throw new Error("Expected maintainable aggregate");
  const columns = plan.inputPlan.select.map((item) => item.alias);
  const input = (ids: number[], values = ids): QueryResult => ({
    columns,
    columnDomains: columns.map(() => null),
    rows: ids.map((id, index) => ({
      [columns[0] ?? "key"]: id,
      [columns[1] ?? "count"]: 1,
      [columns[2] ?? "sum"]: values[index] ?? null,
    })),
  });
  const aggregate = plan.patch(
    input(Array.from({ length: count }, (_, index) => index)),
    new Set(),
    String,
  );
  aggregate.accept();
  return { aggregate, input };
}

describe("staged aggregate contributions", () => {
  it("leaves accepted state intact on rejected, failed and obsolete candidates", () => {
    const { aggregate, input } = seed(3);
    const original = aggregate.result();
    const bytes = aggregate.retainedBytes;
    const rejected = aggregate.patch(input([0], [10]), new Set(["0"]), String);
    expect(rejected.result().rows).toEqual([{ n: 3, s: 13 }]);
    expect(aggregate.result()).toEqual(original);
    expect(aggregate.retainedBytes).toBe(bytes);
    expect(() => aggregate.patch(input([0, 1], [10, 0.5]), new Set(["0", "1"]), String)).toThrow(
      "Floating-point",
    );
    const accepted = aggregate.patch(input([0], [20]), new Set(["0"]), String);
    accepted.accept();
    expect(() => rejected.accept()).toThrow("Stale");
    expect(() => aggregate.patch(input([]), new Set(), String)).toThrow("stale");
    const removed = accepted.patch(input([]), new Set(["0"]), String);
    removed.accept();
    expect(removed.result().rows).toEqual([{ n: 2, s: 3 }]);
    const restored = removed.patch(input([0], [0]), new Set(), String);
    restored.accept();
    expect(restored.result()).toEqual(original);
    expect(restored.retainedBytes).toBe(bytes);
    expect(() => restored.patch(input([1, 1]), new Set(["1"]), String)).toThrow("Duplicate");
  });

  it("does not enumerate the retained input for a one-key patch or a byte measurement", () => {
    const { aggregate, input } = seed(10000);
    const original = Map.prototype[Symbol.iterator];
    const enumeration = vi.spyOn(Map.prototype, Symbol.iterator).mockImplementation(function (
      this: Map<unknown, unknown>,
    ) {
      if (this.size > 1000) throw new Error("Enumerated the retained input");
      return original.call(this);
    });
    try {
      const next = aggregate.patch(input([0], [10]), new Set(["0"]), String);
      expect(next.retainedBytes).toBeGreaterThan(0);
      expect(next.result().rows).toEqual([{ n: 10000, s: 49995010 }]);
      next.accept();
    } finally {
      enumeration.mockRestore();
    }
  });
});
