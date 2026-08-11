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

  it("keys strings by their UTF-8 bytes across every code point width", () => {
    const memory = new QueryMemoryContext();
    const index = new ByteGroupIndex<string>(memory);
    // One representative per UTF-8 width, plus a key long enough to grow the scratch arena.
    const distinct = ["a", "é", "€", "\u{1D11E}", "日本語", "\u{1F389}".repeat(400)];
    distinct.forEach((key) => index.setOne(key, key));

    distinct.forEach((key) => expect(index.getOne(key)).toBe(key));
    expect(index.size).toBe(distinct.length);

    // A compound key must not collide with the concatenation of its parts.
    index.set(["a", "bc"], "split-left");
    index.set(["ab", "c"], "split-right");
    expect(index.get(["a", "bc"])).toBe("split-left");
    expect(index.get(["ab", "c"])).toBe("split-right");
    memory.close();
  });

  it("folds unpaired surrogates to U+FFFD the way TextEncoder does", () => {
    const memory = new QueryMemoryContext();
    const index = new ByteGroupIndex<string>(memory);
    // A lone high or low surrogate encodes as the replacement character, so each is the same key.
    index.setOne("\uD800", "high");
    expect(index.getOne("\uDFFF")).toBe("high");
    expect(index.getOne("�")).toBe("high");
    expect(index.size).toBe(1);

    // A well-formed pair stays distinct from the replacement character it surrounds.
    index.setOne("𝄞", "paired");
    expect(index.getOne("\u{1D11E}")).toBe("paired");
    expect(index.size).toBe(2);
    memory.close();
  });

  it("sheds oversized scratch capacity after a pathological key", () => {
    const memory = new QueryMemoryContext();
    const index = new ByteGroupIndex<string>(memory);
    // A multi-megabyte key grows the shared scratch arena past its retention cap; the next
    // encoding reclaims it, and both keys must remain resolvable afterwards.
    const huge = "k".repeat(2 * 1024 * 1024);
    index.setOne(huge, "huge");
    index.setOne("small", "small");
    expect(index.getOne(huge)).toBe("huge");
    expect(index.getOne("small")).toBe("small");
    expect(index.size).toBe(2);
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
