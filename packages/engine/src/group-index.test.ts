import { describe, expect, it } from "vitest";
import { ByteGroupIndex } from "./group-index.js";
import { QueryMemoryBudgetError, QueryMemoryContext } from "./memory.js";

describe("byte group index", () => {
  it("distinguishes typed and compound keys while preserving insertion order", () => {
    const memory = new QueryMemoryContext(1_024);
    const index = new ByteGroupIndex<string>(memory);
    index.setEmpty("empty");
    index.setOne(null, "null");
    index.setOne(false, "false");
    index.setOne(true, "true");
    index.setOne(0, "zero");
    index.setOne(-0, "updated-zero");
    index.setOne(1, "number");
    index.setOne("1", "string");
    index.set(["a", "bc"], "first-compound");
    index.set(["ab", "c"], "second-compound");

    expect(index.getEmpty()).toBe("empty");
    expect(index.getOne(-0)).toBe("updated-zero");
    expect(index.getOne(1)).toBe("number");
    expect(index.getOne("1")).toBe("string");
    expect(index.get(["a", "bc"])).toBe("first-compound");
    expect(index.get(["ab", "c"])).toBe("second-compound");
    expect(index.values()).toEqual([
      "empty",
      "null",
      "false",
      "true",
      "updated-zero",
      "number",
      "string",
      "first-compound",
      "second-compound",
    ]);
    expect(index.size).toBe(9);
    memory.close();
    expect(memory.usage.usedBytes).toBe(0);
  });

  it("reserves typed growth before mutation and cleans up a failed growth", () => {
    const measuredMemory = new QueryMemoryContext();
    const measured = new ByteGroupIndex<number>(measuredMemory);
    for (let index = 0; index < 20; index += 1) measured.setOne(`key-${String(index)}`, index);
    const exactBytes = measuredMemory.usage.peakBytes;
    expect(exactBytes).toBeGreaterThan(measuredMemory.usage.usedBytes);
    measuredMemory.close();

    const belowMemory = new QueryMemoryContext(exactBytes - 1);
    const below = new ByteGroupIndex<number>(belowMemory);
    expect(() => {
      for (let index = 0; index < 20; index += 1) below.setOne(`key-${String(index)}`, index);
    }).toThrow(QueryMemoryBudgetError);
    belowMemory.close();
    expect(belowMemory.usage.usedBytes).toBe(0);

    const exactMemory = new QueryMemoryContext(exactBytes);
    const exact = new ByteGroupIndex<number>(exactMemory);
    for (let index = 0; index < 20; index += 1) exact.setOne(`key-${String(index)}`, index);
    expect(exact.values()).toEqual(Array.from({ length: 20 }, (_, index) => index));
    expect(exactMemory.usage.peakBytes).toBe(exactBytes);
    exactMemory.close();
  });

  it("verifies encoded bytes when distinct keys share an FNV hash", () => {
    const memory = new QueryMemoryContext(512);
    const index = new ByteGroupIndex<number>(memory);
    const first = "R>3B~a/}09~w";
    const second = "Mq,vUpsu9 )k";
    index.setOne(first, 1);
    index.setOne(second, 2);
    expect(index.getOne(first)).toBe(1);
    expect(index.getOne(second)).toBe(2);
    expect(index.values()).toEqual([1, 2]);
    memory.close();
  });

  it("creates a missing value once and returns it without replacing insertion order", () => {
    const memory = new QueryMemoryContext(512);
    const index = new ByteGroupIndex<object>(memory);
    const first = {};
    let creates = 0;
    expect(
      index.getOrInsertOne("key", () => {
        creates += 1;
        return first;
      }),
    ).toBe(first);
    expect(
      index.getOrInsertOne("key", () => {
        creates += 1;
        return {};
      }),
    ).toBe(first);
    expect(creates).toBe(1);
    expect(index.values()).toEqual([first]);
    memory.close();
  });
});
