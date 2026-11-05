import { describe, expect, it } from "vitest";
import { compileStatement, compileWithDeclaredTypes } from "../../engine/query.js";
import { declaredColumnType, type FrontEndColumn } from "./columns.js";
import { UnsupportedPlanError } from "../errors.js";

function columns(sql: string, declared = true): Record<string, FrontEndColumn> {
  const statement = declared
    ? compileWithDeclaredTypes(() => compileStatement(sql))
    : compileStatement(sql);
  if (statement.kind !== "create-table") throw new Error("Expected CREATE TABLE");
  return Object.fromEntries(statement.columns.map((column) => [column.name, column]));
}

const type = (column: FrontEndColumn | undefined) => {
  if (column === undefined) throw new Error("Missing column");
  return declaredColumnType(column, `t.${column.name}`).type;
};

describe("declaredColumnType (DECISIONS.md C5)", () => {
  it("maps every declared width to its exact physical type", () => {
    const t = columns(
      "CREATE TABLE t (a SMALLINT, b INTEGER, c BIGINT, d REAL, e DOUBLE PRECISION, f FLOAT(20), " +
        "g SERIAL, h BIGSERIAL, i SMALLSERIAL, j NUMERIC(12, 3), k TIMESTAMP, l TIMESTAMPTZ, " +
        "m TEXT, n BOOLEAN, o INT8[], p DATE, q JSONB)",
    );
    expect(type(t.a)).toEqual({ kind: "int32", bits: 16 });
    expect(type(t.b)).toEqual({ kind: "int32", bits: 32 });
    expect(type(t.c)).toEqual({ kind: "int64" });
    expect(type(t.d)).toEqual({ kind: "float32" });
    expect(type(t.e)).toEqual({ kind: "float64" });
    expect(type(t.f)).toEqual({ kind: "float32" });
    expect(type(t.g)).toEqual({ kind: "int32", bits: 32 });
    expect(type(t.h)).toEqual({ kind: "int64" });
    expect(type(t.i)).toEqual({ kind: "int32", bits: 16 });
    expect(type(t.j)).toEqual({ kind: "decimal", precision: 12, scale: 3 });
    expect(type(t.k)).toEqual({ kind: "timestamp", zoned: false });
    expect(type(t.l)).toEqual({ kind: "timestamp", zoned: true });
    expect(type(t.m)).toEqual({ kind: "text" });
    expect(type(t.n)).toEqual({ kind: "bool" });
    expect(type(t.o)).toEqual({ kind: "array", element: { kind: "int64" } });
    expect(type(t.p)).toEqual({ kind: "date" });
    expect(type(t.q)).toEqual({ kind: "json", binary: true });
  });

  it("keeps the declared descriptor for the catalog", () => {
    const t = columns("CREATE TABLE t (a INT2 NOT NULL)");
    if (t.a === undefined) throw new Error("Missing column");
    expect(declaredColumnType(t.a, "t.a").declared).toEqual({
      type: "number",
      integer: true,
      declaredTypeName: "smallint",
    });
  });

  it("refuses a numeric column compiled without its declared width", () => {
    const t = columns("CREATE TABLE t (a INTEGER)", false);
    expect(() => type(t.a)).toThrow("Column t.a has no declared SQL type name");
  });

  it("refuses what the new catalog cannot declare yet", () => {
    const t = columns("CREATE TABLE t (a mood)");
    expect(() => type(t.a)).toThrow(UnsupportedPlanError);
    expect(() => type(t.a)).toThrow("(enumType)");
  });
});
