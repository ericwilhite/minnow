import { describe, expect, it } from "vitest";
import { physicalTypeOf } from "../types.js";
import {
  MAX_SAFE,
  int64At,
  narrowToSafe,
  provenSafe,
  safeAdd,
  safeMultiply,
  sumStaysSafe,
  widenToInt64,
} from "./int64.js";
import {
  activeRow,
  allocateSelection,
  allocateValidity,
  andValidity,
  countValid,
  isValid,
  selectValid,
  setAllValid,
  setValid,
} from "./validity.js";
import { INT128_MAX, INT128_MIN, addInt128, int128At, setInt128 } from "./int128.js";
import { locatorPosition, locatorSegment, packLocator } from "./locator.js";
import {
  VarlenBuilder,
  compareBytes,
  decodeUtf8,
  utf8Length,
  valueLength,
  varlenEqual,
} from "./varlen.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  allocateData,
  allocateFlat,
  batchRowsFor,
  dataBytes,
  newDictionaryId,
} from "./vector.js";

describe("validity bitmaps", () => {
  it("sets, reads and counts bits across word boundaries", () => {
    const validity = allocateValidity(BATCH_ROWS);
    expect(validity.length).toBe(BATCH_ROWS / 32);
    for (const row of [0, 31, 32, 63, 2047]) setValid(validity, row, true);
    expect([0, 1, 31, 32, 63, 64, 2047].map((row) => isValid(validity, row))).toEqual([
      true,
      false,
      true,
      true,
      true,
      false,
      true,
    ]);
    expect(countValid(validity, BATCH_ROWS)).toBe(5);
    expect(countValid(validity, 33)).toBe(3);
    setValid(validity, 31, false);
    expect(countValid(validity, BATCH_ROWS)).toBe(4);
    expect(isValid(null, 5)).toBe(true);
    expect(countValid(null, 17)).toBe(17);
  });

  it("marks exactly a prefix valid", () => {
    const validity = allocateValidity(100);
    validity.fill(0xffff_ffff);
    for (const length of [0, 1, 31, 32, 33, 64, 100]) {
      setAllValid(validity, length);
      expect(countValid(validity, 128), String(length)).toBe(length);
    }
  });

  it("ANDs two bitmaps, and allocates nothing when both are all-valid", () => {
    const left = allocateValidity(64);
    const right = allocateValidity(64);
    const target = allocateValidity(64);
    setAllValid(left, 64);
    setValid(left, 3, false);
    setAllValid(right, 64);
    setValid(right, 40, false);
    const result = andValidity(left, right, target, 64);
    expect(result).toBe(target);
    expect(countValid(target, 64)).toBe(62);
    expect(isValid(target, 3) || isValid(target, 40)).toBe(false);
    expect(andValidity(null, null, target, 64)).toBeNull();
    expect(countValid(andValidity(null, right, target, 64), 64)).toBe(63);
  });

  it("narrows a selection to its valid rows", () => {
    const validity = allocateValidity(8);
    setAllValid(validity, 8);
    setValid(validity, 2, false);
    setValid(validity, 5, false);
    const all = selectValid(null, 8, validity, allocateSelection(8));
    expect([...all.rows.subarray(0, all.count)]).toEqual([0, 1, 3, 4, 6, 7]);
    const some = allocateSelection(8);
    some.rows.set([1, 2, 5, 6]);
    some.count = 4;
    const narrowed = selectValid(some, 8, validity, allocateSelection(8));
    expect([...narrowed.rows.subarray(0, narrowed.count)]).toEqual([1, 6]);
    expect(activeRow(narrowed, 1)).toBe(6);
    expect(activeRow(null, 1)).toBe(1);
  });
});

describe("exact int64", () => {
  it("proves the safe representation from exact bounds only", () => {
    expect(provenSafe(-(2n ** 53n) + 1n, 2n ** 53n - 1n)).toBe(true);
    expect(provenSafe(0n, 2n ** 53n)).toBe(false);
    expect(provenSafe(-(2n ** 53n), 0n)).toBe(false);
  });

  it("detects every result that leaves the safe range", () => {
    expect(safeAdd(MAX_SAFE, 0)).toBe(MAX_SAFE);
    expect(safeAdd(MAX_SAFE, 1)).toBeNaN();
    expect(safeAdd(-MAX_SAFE, -1)).toBeNaN();
    // 2^53 + 1 rounds to 2^53 in f64; still detected.
    expect(safeAdd(2 ** 52, 2 ** 52 + 1)).toBeNaN();
    expect(safeMultiply(2 ** 26, 2 ** 26)).toBe(2 ** 52);
    expect(safeMultiply(2 ** 27, 2 ** 26)).toBeNaN();
    expect(Object.is(safeMultiply(-1, 0), 0)).toBe(true);
    expect(sumStaysSafe(0, 2 ** 40, 2 ** 12)).toBe(true);
    expect(sumStaysSafe(0, 2 ** 40, 2 ** 13)).toBe(false);
  });

  it("widens and narrows without loss", () => {
    const safe = new Float64Array([1, -MAX_SAFE, MAX_SAFE, 0]);
    const wide = new BigInt64Array(4);
    widenToInt64(safe, 4, null, wide);
    expect([...wide]).toEqual([1n, -(2n ** 53n) + 1n, 2n ** 53n - 1n, 0n]);
    const back = new Float64Array(4);
    expect(narrowToSafe(wide, 4, null, back)).toBe(true);
    expect([...back]).toEqual([...safe]);
    wide[2] = 2n ** 53n;
    expect(narrowToSafe(wide, 4, null, back)).toBe(false);
    expect(int64At({ repr: "int64", values: wide }, 2)).toBe(2n ** 53n);
    expect(int64At({ repr: "safe", values: safe }, 1)).toBe(-(2n ** 53n) + 1n);
  });

  it("never throws on garbage under a null slot (review probe: NaN under a null)", () => {
    const validity = allocateValidity(2);
    setValid(validity, 0, true);
    const target = new BigInt64Array(2);
    widenToInt64(new Float64Array([1, Number.NaN]), 2, validity, target);
    expect([...target]).toEqual([1n, 0n]);
    const wide = new BigInt64Array([5n, 2n ** 60n]);
    const back = new Float64Array(2);
    expect(narrowToSafe(wide, 2, validity, back)).toBe(true);
    expect([...back]).toEqual([5, 0]);
  });
});

describe("128-bit decimal tier", () => {
  it("round-trips and adds exactly, reporting overflow", () => {
    const limbs = new Uint32Array(8);
    const values = [0n, 1n, -1n, 2n ** 64n + 7n, -(2n ** 100n), INT128_MAX, INT128_MIN];
    for (const value of values) {
      setInt128(limbs, 1, value);
      expect(int128At(limbs, 1)).toBe(value);
    }
    for (const [a, b] of [
      [2n ** 64n - 1n, 1n],
      [-(2n ** 90n), 2n ** 89n],
      [INT128_MAX, 1n],
      [INT128_MIN, -1n],
      [INT128_MAX, INT128_MIN],
    ] as const) {
      setInt128(limbs, 0, a);
      setInt128(limbs, 1, b);
      const fits = addInt128(limbs, 0, limbs, 1);
      const exact = a + b;
      expect(fits).toBe(exact >= INT128_MIN && exact <= INT128_MAX);
      if (fits) expect(int128At(limbs, 0)).toBe(exact);
    }
    expect(() => setInt128(limbs, 0, INT128_MAX + 1n)).toThrow(RangeError);
  });
});

describe("byte-bounded batches and locators", () => {
  it("shrinks rows as rows widen, never below one", () => {
    expect(batchRowsFor(8)).toBe(BATCH_ROWS);
    expect(batchRowsFor(200 * 8)).toBe(640);
    expect(batchRowsFor(500 * 8) * 500 * 8).toBeLessThanOrEqual(BATCH_TARGET_BYTES);
    expect(batchRowsFor(BATCH_TARGET_BYTES * 3)).toBe(1);
    expect(batchRowsFor(BATCH_TARGET_BYTES / 10)).toBe(10);
  });

  it("packs locators exactly and refuses out-of-range parts", () => {
    const locator = packLocator(2 ** 36 + 5, 65_535);
    expect(locatorSegment(locator)).toBe(2 ** 36 + 5);
    expect(locatorPosition(locator)).toBe(65_535);
    expect(() => packLocator(0, 65_536)).toThrow(RangeError);
    expect(() => packLocator(2 ** 37, 0)).toThrow(RangeError);
  });
});

/** A grant stand-in: a byte budget that records what it has handed out. */
function budget(limit: number) {
  return {
    held: 0,
    allocate(bytes: number) {
      if (this.held + bytes > limit) return false;
      this.held += bytes;
      return true;
    },
    free(bytes: number) {
      this.held -= bytes;
    },
  };
}

describe("varlen data", () => {
  it("builds, compares and decodes UTF-8 values, reusing its buffers", () => {
    const charge = budget(1 << 20);
    const builder = new VarlenBuilder(4, charge, 1024, 16);
    expect(builder.appendString("abc")).toBe("ok");
    expect(builder.appendString("é😀")).toBe("ok");
    expect(builder.appendEmpty()).toBe("ok");
    expect(builder.appendBytes(new TextEncoder().encode("abd"))).toBe("ok");
    const data = builder.data();
    expect(builder.rows).toBe(4);
    expect([0, 1, 2, 3].map((i) => valueLength(data, i))).toEqual([3, 6, 0, 3]);
    expect([0, 1, 2, 3].map((i) => decodeUtf8(data, i))).toEqual(["abc", "é😀", "", "abd"]);
    expect(varlenEqual(data, 0, data, 0)).toBe(true);
    expect(varlenEqual(data, 0, data, 3)).toBe(false);
    const at = (i: number) => [data.offsets[i] ?? 0, data.offsets[i + 1] ?? 0] as const;
    expect(compareBytes(data.bytes, ...at(0), data.bytes, ...at(3))).toBe(-1);
    expect(compareBytes(data.bytes, ...at(2), data.bytes, ...at(0))).toBe(-1);
    expect(compareBytes(data.bytes, ...at(1), data.bytes, ...at(0))).toBe(1);
    expect(compareBytes(data.bytes, ...at(0), data.bytes, ...at(0))).toBe(0);
    expect(builder.appendEmpty()).toBe("full");
    expect(charge.held).toBe(builder.retainedBytes);
    builder.reset();
    expect(builder.appendString("x")).toBe("ok");
    expect(decodeUtf8(builder.data(), 0)).toBe("x");
    builder.close();
    expect(charge.held).toBe(0);
  });

  it("stops at the byte target, admits one oversized value alone, and shrinks after it", () => {
    const charge = budget(1 << 20);
    const builder = new VarlenBuilder(8, charge, 64, 16);
    expect(builder.appendBytes(new Uint8Array(40))).toBe("ok");
    expect(builder.appendBytes(new Uint8Array(40))).toBe("full");
    builder.reset();
    expect(builder.appendBytes(new Uint8Array(500))).toBe("ok");
    expect(builder.retainedBytes).toBeGreaterThanOrEqual(500);
    expect(charge.held).toBe(builder.retainedBytes);
    expect(builder.appendBytes(new Uint8Array(1))).toBe("full");
    builder.reset();
    expect(builder.retainedBytes).toBe(16 + 9 * 4);
    expect(charge.held).toBe(builder.retainedBytes);
  });

  it("asks before growing and reports a refusal instead of allocating", () => {
    const charge = budget(16 + 3 * 4 + 20);
    const builder = new VarlenBuilder(2, charge, 1024, 16);
    expect(builder.appendBytes(new Uint8Array(40))).toBe("denied");
    expect(builder.rows).toBe(0);
    expect(charge.held).toBe(builder.retainedBytes);
    expect(() => new VarlenBuilder(2, budget(10))).toThrow(RangeError);
    expect(utf8Length("aé😀\ud800")).toBe(1 + 2 + 4 + 3);
  });

  it("refuses invalid UTF-8 at the output boundary", () => {
    const builder = new VarlenBuilder(1, budget(1 << 20));
    builder.appendBytes(new Uint8Array([0xc3, 0x28]));
    expect(() => decodeUtf8(builder.data(), 0)).toThrow(TypeError);
  });
});

describe("vector allocation", () => {
  it("allocates the layout of every physical type at batch capacity", () => {
    const layouts = [
      physicalTypeOf({ type: "boolean" }),
      physicalTypeOf({ type: "number", integer: true, declaredTypeName: "INTEGER" }),
      physicalTypeOf({ type: "number", integer: true }),
      physicalTypeOf({ type: "number" }),
      physicalTypeOf({ type: "string", sqlDomain: { kind: "numeric", precision: 12, scale: 2 } }),
      physicalTypeOf({ type: "string", sqlDomain: { kind: "date" } }),
      physicalTypeOf({ type: "datetime" }),
      physicalTypeOf({ type: "string", sqlDomain: { kind: "interval" } }),
      physicalTypeOf({ type: "string" }),
      physicalTypeOf({ type: "string", sqlDomain: { kind: "uuid" } }),
    ].map((type) => allocateData(type, BATCH_ROWS).layout);
    expect(layouts).toEqual([
      "bool",
      "i32",
      "int64",
      "f64",
      "decimal",
      "i32",
      "int64",
      "interval",
      "varlen",
      "fixed16",
    ]);
    const decimal = allocateData({ kind: "decimal", precision: 12, scale: 2 }, 8);
    expect(decimal.layout === "decimal" && decimal.scale).toBe(2);
    expect(allocateData({ kind: "float32" }, 4).layout).toBe("f32");
    expect(allocateData({ kind: "locator", tableId: 1 }, 4).layout).toBe("f64");
    expect(dataBytes({ kind: "float32" }, 4)).toBe(16);
    expect(allocateData({ kind: "decimal", precision: 5, scale: -2 }, 1).layout === "decimal").toBe(
      true,
    );
    const negative = allocateData({ kind: "decimal", precision: 5, scale: -2 }, 1);
    expect(negative.layout === "decimal" && negative.scale).toBe(0);
    const free = allocateData({ kind: "decimal", precision: null, scale: null }, 3);
    expect(free.layout === "decimal" && free.scale === null && free.scales?.length).toBe(3);
    const flat = allocateFlat({ kind: "text" });
    expect(flat.data.layout === "varlen" && flat.data.offsets.length).toBe(BATCH_ROWS + 1);
    expect(newDictionaryId()).not.toBe(newDictionaryId());
  });
});
