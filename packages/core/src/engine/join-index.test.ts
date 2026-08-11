import { describe, expect, it } from "vitest";
import { ByteJoinIndex } from "./join-index.js";
import { QueryMemoryBudgetError, QueryMemoryContext } from "./memory.js";

describe("byte join index", () => {
  it("distinguishes scalar types and preserves duplicate row order", () => {
    const memory = new QueryMemoryContext(2_048);
    const index = new ByteJoinIndex(memory, 10);
    index.add(null, 0);
    index.add(Number.NaN, 1);
    index.add(1, 2);
    index.add("1", 3);
    index.add(false, 4);
    index.add(-0, 5);
    index.add(0, 6);
    index.add(Number.POSITIVE_INFINITY, 7);
    index.add(new Date("2025-01-01T00:00:00.000Z"), 8);
    index.add(new Date("2025-01-01T00:00:00.000Z"), 9);

    expect(index.firstRow(null)).toBe(-1);
    expect(index.firstRow(Number.NaN)).toBe(-1);
    expect(index.firstRow(1)).toBe(2);
    expect(index.firstRow("1")).toBe(3);
    expect(index.firstRow(false)).toBe(4);
    expect(index.firstRow(0)).toBe(5);
    expect(index.nextRow(5)).toBe(6);
    expect(index.nextRow(6)).toBe(-1);
    expect(index.firstRow(Number.POSITIVE_INFINITY)).toBe(7);
    expect(index.firstRow(new Date("2025-01-01T00:00:00.000Z"))).toBe(8);
    expect(index.nextRow(8)).toBe(9);
    expect(index.unique).toBe(false);
    memory.close();
    expect(memory.usage.usedBytes).toBe(0);
  });

  it("verifies encoded bytes when distinct keys share an FNV hash", () => {
    const memory = new QueryMemoryContext(512);
    const index = new ByteJoinIndex(memory, 2);
    const first = "R>3B~a/}09~w";
    const second = "Mq,vUpsu9 )k";
    index.add(first, 0);
    index.add(second, 1);
    expect(index.firstRow(first)).toBe(0);
    expect(index.firstRow(second)).toBe(1);
    memory.close();
  });

  it("reserves growth before mutation and cleans up a failed growth", () => {
    const measuredMemory = new QueryMemoryContext();
    const measured = new ByteJoinIndex(measuredMemory, 20);
    for (let row = 0; row < 20; row += 1) measured.add(`key-${String(row)}`, row);
    const exactBytes = measuredMemory.usage.peakBytes;
    expect(exactBytes).toBeGreaterThan(measuredMemory.usage.usedBytes);
    measuredMemory.close();

    const belowMemory = new QueryMemoryContext(exactBytes - 1);
    const below = new ByteJoinIndex(belowMemory, 20);
    expect(() => {
      for (let row = 0; row < 20; row += 1) below.add(`key-${String(row)}`, row);
    }).toThrow(QueryMemoryBudgetError);
    belowMemory.close();
    expect(belowMemory.usage.usedBytes).toBe(0);

    const exactMemory = new QueryMemoryContext(exactBytes);
    const exact = new ByteJoinIndex(exactMemory, 20);
    for (let row = 0; row < 20; row += 1) exact.add(`key-${String(row)}`, row);
    expect(exact.firstRow("key-19")).toBe(19);
    expect(exactMemory.usage.peakBytes).toBe(exactBytes);
    exactMemory.close();
  });
});
