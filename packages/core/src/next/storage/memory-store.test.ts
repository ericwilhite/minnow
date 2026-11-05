import { describe, expect, it } from "vitest";
import { storeConformanceCases } from "./conformance.js";
import { FaultInjectingStore } from "./fault-store.js";
import { MemoryStore } from "./memory-store.js";
import { chooseRoot, decodeRootSlot, encodeRootSlot } from "./root-slot.js";

describe("memory store conformance", () => {
  for (const testCase of storeConformanceCases)
    it(testCase.name, () => testCase.run(async () => new MemoryStore()));
});

describe("fault store conformance (no crash planned)", () => {
  for (const testCase of storeConformanceCases)
    it(testCase.name, () => testCase.run(async () => new FaultInjectingStore({ seed: 1 })));
});

describe("two-slot root publication", () => {
  const root = (text: string) => new TextEncoder().encode(text);

  it("round-trips a root and refuses a torn or foreign slot", () => {
    const slot = encodeRootSlot(2 ** 40 + 3, root("hello"));
    expect(decodeRootSlot(slot)).toEqual({ sequence: 2 ** 40 + 3, root: root("hello") });
    for (let cut = 0; cut < slot.length; cut++)
      expect(decodeRootSlot(slot.slice(0, cut)), `cut at ${String(cut)}`).toBeNull();
    for (let byte = 0; byte < slot.length; byte++) {
      const flipped = slot.slice();
      flipped[byte] = (flipped[byte] ?? 0) ^ 0x10;
      expect(decodeRootSlot(flipped), `flip at ${String(byte)}`).toBeNull();
    }
    expect(decodeRootSlot(null)).toBeNull();
  });

  it("chooses the newest valid slot and overwrites the other", () => {
    expect(chooseRoot(null, null)).toEqual({ current: null, nextSlot: 0, nextSequence: 1 });
    const one = encodeRootSlot(1, root("one"));
    const two = encodeRootSlot(2, root("two"));
    expect(chooseRoot(one, null)).toMatchObject({ nextSlot: 1, nextSequence: 2 });
    expect(chooseRoot(one, two).current?.root).toEqual(root("two"));
    expect(chooseRoot(one, two).nextSlot).toBe(0);
    // A torn write of the next root leaves the previous one current.
    const torn = encodeRootSlot(3, root("three")).slice(0, 10);
    expect(chooseRoot(torn, two).current?.sequence).toBe(2);
    expect(chooseRoot(torn, two).nextSlot).toBe(0);
  });
});
