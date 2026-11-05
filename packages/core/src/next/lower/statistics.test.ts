import { describe, expect, it } from "vitest";
import {
  REPLAN_GROWTH_FACTOR,
  REPLAN_MIN_ROWS,
  statisticsDrifted,
  type PlanningStatistics,
} from "./statistics.js";

function rows(counts: Record<number, number>): PlanningStatistics {
  return {
    table: (tableId) => {
      const rowCount = counts[tableId];
      return rowCount === undefined ? null : { rowCount, column: () => null };
    },
  };
}

describe("statisticsDrifted", () => {
  const basis = [{ tableId: 1, rowCount: 10_000 }];

  it("keeps a plan while each table stays within the growth factor", () => {
    expect(statisticsDrifted(basis, rows({ 1: 10_000 * REPLAN_GROWTH_FACTOR }))).toBe(false);
    expect(statisticsDrifted(basis, rows({ 1: 10_000 / REPLAN_GROWTH_FACTOR }))).toBe(false);
  });

  it("re-plans when a table grows or shrinks past it", () => {
    expect(statisticsDrifted(basis, rows({ 1: 20_001 }))).toBe(true);
    expect(statisticsDrifted(basis, rows({ 1: 4_999 }))).toBe(true);
  });

  it("ignores small tables and tables without statistics", () => {
    const small = [{ tableId: 1, rowCount: 0 }];
    expect(statisticsDrifted(small, rows({ 1: REPLAN_MIN_ROWS * REPLAN_GROWTH_FACTOR }))).toBe(
      false,
    );
    expect(statisticsDrifted(small, rows({ 1: REPLAN_MIN_ROWS * REPLAN_GROWTH_FACTOR + 1 }))).toBe(
      true,
    );
    expect(statisticsDrifted(basis, rows({}))).toBe(false);
  });

  it("checks every table the plan was costed on", () => {
    const two = [
      { tableId: 1, rowCount: 10_000 },
      { tableId: 2, rowCount: 50_000 },
    ];
    expect(statisticsDrifted(two, rows({ 1: 10_000, 2: 200_000 }))).toBe(true);
  });
});
