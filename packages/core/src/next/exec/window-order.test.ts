/** Order proofs respect expression bindings, scalar identity, collation and volatility. */
import { expect, it } from "vitest";
import type { Rel, ScalarExpr, SortKey } from "../ir/ir.js";
import { windowOrderSource } from "./window-order.js";
import type { PhysicalType } from "../types.js";
import { INT, DBL, vectorOf } from "./join-fixtures.js";
const col = (index: number, type: PhysicalType = INT): ScalarExpr => ({
  op: "column",
  index,
  type,
});
const key = (index: number, type: PhysicalType = INT): SortKey => ({
  expr: col(index, type),
  direction: "asc",
  nulls: "last",
  collation: null,
});
const literal = (value: number): ScalarExpr => {
  const vector = vectorOf(DBL, [value]);
  if (vector.form !== "flat") throw Error("literal");
  return { op: "literal", type: DBL, value: vector };
};
const base: Rel = {
  op: "values",
  id: 1,
  rows: [],
  output: [{ name: "x", type: INT, nullable: true }],
};
function window(a: ScalarExpr, b: ScalarExpr): Extract<Rel, { op: "window" }> {
  const exprs = [a, b, col(0)];
  const output = exprs.map((expr, index) => ({
    name: String(index),
    type: expr.type,
    nullable: true,
  }));
  return {
    op: "window",
    id: 3,
    input: { op: "project", id: 2, input: base, exprs, output },
    partitionBy: [1],
    orderBy: [key(2)],
    functions: [],
    output,
  };
}
it("proves immutable repeated expressions and exact binding aliases", () => {
  const mod = (): ScalarExpr => ({
    op: "call",
    fn: "mod",
    args: [col(0), { op: "param", slot: 0, type: INT }],
    type: INT,
  });
  const w = window(mod(), mod());
  expect(windowOrderSource(w, [key(0), key(2)])).toBe(w);
  const alias: Rel = {
    op: "project",
    id: 4,
    input: w,
    exprs: [col(2), col(0)],
    output: w.output.slice(0, 2),
  };
  expect(windowOrderSource(alias, [key(1), key(0)])).toBe(w);
  for (const changed of [
    { ...key(0), direction: "desc" as const },
    { ...key(0), nulls: "first" as const },
    { ...key(0), collation: "de" },
  ])
    expect(windowOrderSource(w, [changed])).toBeNull();
  expect(windowOrderSource(w, [{ ...key(0), collation: "C" }])).toBe(w);
  expect(windowOrderSource(w, [key(2)])).toBeNull();
});
it("does not equate volatile expression copies, different params or literal bits", () => {
  const random = (): ScalarExpr => ({ op: "call", fn: "random", args: [], type: DBL });
  const w = window(random(), random());
  expect(windowOrderSource(w, [key(0, DBL)])).toBeNull();
  expect(windowOrderSource(w, [key(1, DBL)])).toBe(w); // one already materialized value
  const params = window({ op: "param", slot: 0, type: INT }, { op: "param", slot: 1, type: INT });
  expect(windowOrderSource(params, [key(0)])).toBeNull();
  for (const [a, b] of [
    [NaN, Infinity],
    [0, -0],
    [1, 2],
  ])
    expect(windowOrderSource(window(literal(a ?? 0), literal(b ?? 0)), [key(0, DBL)])).toBeNull();
});
