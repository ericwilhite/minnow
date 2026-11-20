/** The demand walker reads through every expression kind; an unclassified kind reads everything. */
import { expect, it } from "vitest";
import { columnsRead } from "./columns-read.js";
import type { ScalarExpr } from "./ir.js";
import type { PhysicalType } from "../types.js";

const F64: PhysicalType = { kind: "float64" };
const BOOL: PhysicalType = { kind: "bool" };
const col = (index: number): ScalarExpr => ({ op: "column", index, type: F64 });
const param: ScalarExpr = { op: "param", slot: 0, type: F64 };

it("collects columns from every expression kind, however deeply nested", () => {
  const kinds: ReadonlyArray<readonly [ScalarExpr, readonly number[]]> = [
    [col(3), [3]],
    [param, []],
    [{ op: "call", fn: "add", args: [col(1), col(2)], type: F64 }, [1, 2]],
    [{ op: "cast", input: col(4), type: F64 }, [4]],
    [{ op: "compare", cmp: "<", left: col(1), right: col(5), collation: null, type: BOOL }, [1, 5]],
    [{ op: "distinctFrom", negated: false, left: col(6), right: param, type: BOOL }, [6]],
    [{ op: "and", args: [col(7), col(8)], type: BOOL }, [7, 8]],
    [{ op: "not", arg: col(9), type: BOOL }, [9]],
    [{ op: "isNull", negated: true, arg: col(10), type: BOOL }, [10]],
    [
      {
        op: "case",
        branches: [{ when: col(11), then: col(12) }],
        otherwise: col(13),
        type: F64,
      },
      [11, 12, 13],
    ],
    [
      {
        op: "inList",
        negated: false,
        arg: col(14),
        list: [param, col(15)],
        collation: null,
        type: BOOL,
      },
      [14, 15],
    ],
    [
      {
        op: "like",
        mode: "like",
        negated: false,
        arg: col(16),
        pattern: col(17),
        escape: null,
        type: BOOL,
      },
      [16, 17],
    ],
  ];
  for (const [expr, columns] of kinds) {
    expect([...(columnsRead([expr]) ?? [])].sort((a, b) => a - b)).toEqual(columns);
    // The same expression inside a CASE arm inside a call still counts.
    const nested: ScalarExpr = {
      op: "call",
      fn: "coalesce",
      args: [{ op: "case", branches: [{ when: param, then: expr }], otherwise: null, type: F64 }],
      type: F64,
    };
    expect([...(columnsRead([nested]) ?? [])].sort((a, b) => a - b)).toEqual(columns);
  }
});

it("demands every column when it meets an unclassified expression", () => {
  const unknown = { op: "mystery", arg: col(1), type: F64 } as unknown as ScalarExpr;
  expect(columnsRead([unknown])).toBeNull();
  expect(columnsRead([col(0), { op: "call", fn: "f", args: [unknown], type: F64 }])).toBeNull();
});
