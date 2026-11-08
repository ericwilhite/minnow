import { vectorOf } from "./join-fixtures.js";
import { expect, it } from "vitest";
import { windowInputLayout, remapWindowFunction, literalWindowAmount } from "./window-input.js";
import type { ScalarExpr } from "../ir/ir.js";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
const int = { kind: "int32", bits: 32 } as const;
const col = (index: number): ScalarExpr => ({ op: "column", index, type: int });
it("compacts proven aliases and restores all logical slots and function references", () => {
  const projection = [col(0), col(1), col(2), col(0), col(2), col(1)];
  const layout = windowInputLayout(
    projection.map((_, index) => col(index)),
    projection,
  );
  expect(layout.exprs).toHaveLength(3);
  expect(layout.restore).toEqual([0, 1, 2, 0, 2, 1]);
  expect(
    remapWindowFunction(
      { fn: "lag", arg: 4, frame: null, type: int, amount: { column: 3 } },
      layout.index,
    ),
  ).toEqual({ fn: "lag", arg: 2, frame: null, type: int, amount: { column: 0 } });
  expect(() => layout.index(9)).toThrow("outside its input layout");
});
it("keeps computed expressions and distinct physical types separate", () => {
  const value = vectorOf(int, [1]);
  if (value.form !== "flat") throw new Error("expected flat literal");
  const literal: ScalarExpr = { op: "literal", value, type: int };
  const wide = { op: "column", index: 0, type: { kind: "int64" } } as const;
  const exprs = [col(0), literal, literal, wide];
  expect(windowInputLayout(exprs, null).restore).toEqual([0, 1, 2, 3]);
  expect(windowInputLayout([col(0), col(0)], null, false).restore).toEqual([0, 1]);
});
for (const seed of seedsFor("window-input-layout", [0xb710, 0xb711]))
  it(`preserves positional values/validity under generated aliases, seed ${String(seed)}`, () => {
    const random = mulberry32(seed);
    for (let run = 0; run < 128; run++) {
      const projection = Array.from({ length: 3 + Math.floor(random() * 12) }, () =>
        col(Math.floor(random() * 3)),
      );
      const exprs = projection.map((_, index) => col(index));
      const layout = windowInputLayout(exprs, projection);
      const values = [null, -(2n ** 62n), 2n ** 62n];
      const logical = projection.map((expr) =>
        expr.op === "column" ? values[expr.index] : undefined,
      );
      const physical = layout.exprs.map((expr) =>
        expr.op === "column" ? logical[expr.index] : undefined,
      );
      expect(layout.restore.map((index) => physical[index])).toEqual(logical);
      expect(layout.exprs.length).toBeLessThanOrEqual(3);
    }
  });

it("keeps exact int4 amounts scalar but retains NULL, computed and wider arguments", () => {
  const literal = (amount: number | null): ScalarExpr => {
    const value = vectorOf(int, [amount]);
    if (value.form !== "flat") throw new Error("flat literal expected");
    return { op: "literal", type: int, value };
  };
  for (const amount of [-2147483648, -1, 0, 1, 3, 2147483647])
    expect(literalWindowAmount(literal(amount))).toBe(amount);
  expect(literalWindowAmount(literal(null))).toBeUndefined();
  expect(literalWindowAmount(col(0))).toBeUndefined();
  expect(literalWindowAmount(undefined)).toBeUndefined();
  const big = { kind: "int64" } as const;
  const value = vectorOf(big, [2n ** 62n]);
  if (value.form !== "flat") throw new Error("flat literal expected");
  expect(literalWindowAmount({ op: "literal", type: big, value })).toBeUndefined();
});
