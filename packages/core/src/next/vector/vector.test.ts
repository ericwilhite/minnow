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
import { VarlenBuilder, compareBytes, decodeUtf8, valueLength, varlenEqual } from "./varlen.js";
import { BATCH_ROWS, allocateData, allocateFlat, newDictionaryId } from "./vector.js";

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
    widenToInt64(safe, 4, wide);
    expect([...wide]).toEqual([1n, -(2n ** 53n) + 1n, 2n ** 53n - 1n, 0n]);
    const back = new Float64Array(4);
    expect(narrowToSafe(wide, 4, back)).toBe(true);
    expect([...back]).toEqual([...safe]);
    wide[2] = 2n ** 53n;
    expect(narrowToSafe(wide, 4, back)).toBe(false);
    expect(int64At({ repr: "int64", values: wide }, 2)).toBe(2n ** 53n);
    expect(int64At({ repr: "safe", values: safe }, 1)).toBe(-(2n ** 53n) + 1n);
  });
});

describe("varlen data", () => {
  it("builds, compares and decodes UTF-8 values, reusing its buffers", () => {
    const builder = new VarlenBuilder(4, 16);
    builder.appendString("abc");
    builder.appendString("é😀");
    builder.appendEmpty();
    builder.appendBytes(new TextEncoder().encode("abd"));
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
    expect(() => builder.appendEmpty()).toThrow(RangeError);
    const retained = builder.retainedBytes;
    builder.reset();
    builder.appendString("x");
    expect(decodeUtf8(builder.data(), 0)).toBe("x");
    expect(builder.retainedBytes).toBe(retained);
  });

  it("refuses invalid UTF-8 at the output boundary", () => {
    const builder = new VarlenBuilder(1);
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
    const flat = allocateFlat({ kind: "text" });
    expect(flat.data.layout === "varlen" && flat.data.offsets.length).toBe(BATCH_ROWS + 1);
    expect(newDictionaryId()).not.toBe(newDictionaryId());
  });
});
