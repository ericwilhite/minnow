/**
 * `compileTableRules` (exec/rules.ts): the expressions lowering hands over must match the table
 * exactly, and a table without rules gets free no-op rules. Evaluation is Phase 2.
 */
import { describe, expect, it } from "vitest";
import type { ColumnDef, TableDef } from "../access/catalog.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import { allocateFlat } from "../vector/vector.js";
import { RuleCompileError, compileTableRules, type TableRuleExpressions } from "./rules.js";

const INT: PhysicalType = { kind: "int32", bits: 32 };
const BOOL: PhysicalType = { kind: "bool" };

function column(id: number, generatedSql: string | null = null): ColumnDef {
  return {
    id,
    name: `c${String(id)}`,
    type: INT,
    declared: { type: "number", integer: true, declaredTypeName: "INTEGER" },
    nullable: true,
    defaultSql: null,
    generatedSql,
    identitySequence: null,
    missing: null,
  };
}

function table(columns: ColumnDef[], checks: TableDef["checks"] = []): TableDef {
  return {
    id: 4,
    name: "t",
    schemaVersion: 1,
    columns,
    primaryKey: null,
    indexes: [],
    foreignKeys: [],
    checks,
  };
}

const col = (index: number): ScalarExpr => ({ op: "column", index, type: INT });
const positive = (index: number): ScalarExpr => ({
  op: "compare",
  cmp: ">",
  left: col(index),
  right: col(index),
  collation: null,
  type: BOOL,
});
const check = { id: 9, name: "positive", conditionSql: "c1 > 0", columnIds: [1] };
const empty: TableRuleExpressions = { checks: [], generated: [] };

describe("compileTableRules", () => {
  it("gives a table without rules free no-op rules", () => {
    const rules = compileTableRules(table([column(1)]), 3, empty);
    expect(rules.check({ length: 1, columns: [], selection: null }, null)).toBeNull();
    expect(() => {
      rules.generate({ length: 1, columns: [], selection: null }, [1]);
    }).not.toThrow();
  });

  it("accepts expressions that match the table", () => {
    const def = table([column(1), column(2, "c1 * 2")], [check]);
    expect(() =>
      compileTableRules(def, 3, {
        checks: [{ checkId: 9, condition: positive(0) }],
        generated: [{ columnId: 2, expr: col(0) }],
      }),
    ).not.toThrow();
  });

  it("skips checks that read none of the written columns", () => {
    const def = table([column(1), column(2)], [check]);
    const rules = compileTableRules(def, 3, {
      checks: [{ checkId: 9, condition: positive(0) }],
      generated: [],
    });
    expect(rules.check({ length: 1, columns: [], selection: null }, [2])).toBeNull();
  });

  it.each<[string, TableDef, TableRuleExpressions]>([
    ["a missing check", table([column(1)], [check]), empty],
    [
      "an extra check",
      table([column(1)]),
      { checks: [{ checkId: 9, condition: positive(0) }], generated: [] },
    ],
    [
      "a non-boolean check",
      table([column(1)], [check]),
      { checks: [{ checkId: 9, condition: col(0) }], generated: [] },
    ],
    [
      "a column out of range",
      table([column(1)], [check]),
      { checks: [{ checkId: 9, condition: positive(5) }], generated: [] },
    ],
    [
      "a parameter",
      table([column(1)], [check]),
      {
        checks: [
          {
            checkId: 9,
            condition: { ...positive(0), right: { op: "param", slot: 0, type: INT } } as ScalarExpr,
          },
        ],
        generated: [],
      },
    ],
    ["a missing generated column", table([column(1), column(2, "c1")]), empty],
    [
      "a generated column reading another",
      table([column(1, "1"), column(2, "c1")]),
      {
        checks: [],
        generated: [
          { columnId: 1, expr: { op: "literal", value: allocateFlat(INT, 1), type: INT } },
          { columnId: 2, expr: col(0) },
        ],
      },
    ],
    [
      "a generated expression of the wrong type",
      table([column(1), column(2, "c1 > 0")]),
      { checks: [], generated: [{ columnId: 2, expr: positive(0) }] },
    ],
  ])("refuses %s", (_name, def, expressions) => {
    expect(() => compileTableRules(def, 3, expressions)).toThrow(RuleCompileError);
  });
});
