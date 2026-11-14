import { describe, expect, it } from "vitest";
import {
  layoutOf,
  physicalTypeOf,
  physicalTypeOfName,
  requireDeclaredWidths,
  samePhysicalType,
  type SqlTypeDescriptor,
} from "./types.js";

describe("SQL → physical type mapping", () => {
  it.each<[string, SqlTypeDescriptor, unknown]>([
    ["BOOLEAN", { type: "boolean" }, { kind: "bool" }],
    [
      "SMALLINT",
      { type: "number", integer: true, declaredTypeName: "smallint" },
      { kind: "int32", bits: 16 },
    ],
    [
      "INTEGER",
      { type: "number", integer: true, declaredTypeName: "INT4" },
      { kind: "int32", bits: 32 },
    ],
    ["BIGINT", { type: "number", integer: true, declaredTypeName: "BIGINT" }, { kind: "int64" }],
    ["integer without a declared name", { type: "number", integer: true }, { kind: "int64" }],
    ["DOUBLE / number", { type: "number" }, { kind: "float64" }],
    ["REAL", { type: "number", declaredTypeName: "real" }, { kind: "float32" }],
    ["FLOAT4", { type: "number", declaredTypeName: "FLOAT4" }, { kind: "float32" }],
    [
      "DOUBLE PRECISION",
      { type: "number", declaredTypeName: "DOUBLE PRECISION" },
      { kind: "float64" },
    ],
    [
      "NUMERIC(12,2)",
      { type: "string", sqlDomain: { kind: "numeric", precision: 12, scale: 2 } },
      { kind: "decimal", precision: 12, scale: 2 },
    ],
    [
      "NUMERIC",
      { type: "string", sqlDomain: { kind: "numeric" } },
      { kind: "decimal", precision: null, scale: null },
    ],
    ["DATE", { type: "string", sqlDomain: { kind: "date" } }, { kind: "date" }],
    ["TIME", { type: "string", sqlDomain: { kind: "time" } }, { kind: "time" }],
    ["TIMESTAMPTZ", { type: "datetime" }, { kind: "timestamp", zoned: true }],
    ["TIMESTAMP", { type: "datetime", withoutTimeZone: true }, { kind: "timestamp", zoned: false }],
    ["INTERVAL", { type: "string", sqlDomain: { kind: "interval" } }, { kind: "interval" }],
    ["TEXT", { type: "string" }, { kind: "text" }],
    ["JSON", { type: "string", sqlDomain: { kind: "json" } }, { kind: "json", binary: false }],
    ["JSONB", { type: "string", sqlDomain: { kind: "jsonb" } }, { kind: "json", binary: true }],
    ["UUID", { type: "string", sqlDomain: { kind: "uuid" } }, { kind: "uuid" }],
    [
      "enum type",
      { type: "string", sqlDomain: { kind: "enum", name: "mood", values: ["sad", "ok"] } },
      { kind: "enum", name: "mood", labels: ["sad", "ok"] },
    ],
    [
      "string with enumValues",
      { type: "string", enumValues: ["a", "b"] },
      { kind: "enum", name: "", labels: ["a", "b"] },
    ],
    [
      "INTEGER[]",
      { type: "string", sqlDomain: { kind: "array", element: "integer" } },
      { kind: "array", element: { kind: "int32", bits: 32 } },
    ],
  ])("maps %s", (_name, descriptor, expected) => {
    expect(physicalTypeOf(descriptor)).toEqual(expected);
  });

  it("requires declared widths before lowering (C5)", () => {
    expect(() => requireDeclaredWidths({ type: "number", integer: true }, "t.a")).toThrow(
      TypeError,
    );
    expect(() => requireDeclaredWidths({ type: "number" }, "t.b")).toThrow(TypeError);
    requireDeclaredWidths({ type: "number", integer: true, declaredTypeName: "BIGINT" }, "t.c");
    requireDeclaredWidths({ type: "string", sqlDomain: { kind: "numeric" } }, "t.d");
    expect(physicalTypeOfName("real")).toEqual({ kind: "float32" });
  });

  it("refuses an unknown array element type", () => {
    expect(() => physicalTypeOfName("geometry")).toThrow(TypeError);
  });

  it("gives every type a layout and compares types structurally", () => {
    expect(layoutOf({ kind: "timestamp", zoned: false })).toBe("int64");
    expect(layoutOf({ kind: "enum", name: "e", labels: [] })).toBe("varlen");
    expect(samePhysicalType({ kind: "int32", bits: 16 }, { kind: "int32", bits: 32 })).toBe(false);
    expect(
      samePhysicalType(
        { kind: "array", element: { kind: "text" } },
        { kind: "array", element: { kind: "text" } },
      ),
    ).toBe(true);
    expect(
      samePhysicalType(
        { kind: "enum", name: "e", labels: ["a"] },
        { kind: "enum", name: "e", labels: ["b"] },
      ),
    ).toBe(false);
  });
});

describe("text widths (ruling R17.4)", () => {
  it("keeps VARCHAR(n) and CHAR(n) widths and compares them", () => {
    const varchar = physicalTypeOf({
      type: "string",
      declaredLength: { length: 20, fixed: false },
    });
    expect(varchar).toEqual({ kind: "text", maxLength: 20 });
    expect(physicalTypeOf({ type: "string", declaredLength: { length: 3, fixed: true } })).toEqual({
      kind: "text",
      maxLength: 3,
      fixed: true,
    });
    expect(physicalTypeOf({ type: "string" })).toEqual({ kind: "text" });
    expect(samePhysicalType(varchar, { kind: "text", maxLength: 20 })).toBe(true);
    expect(samePhysicalType(varchar, { kind: "text" })).toBe(false);
    expect(samePhysicalType(varchar, { kind: "text", maxLength: 20, fixed: true })).toBe(false);
  });

  it("names every array element spelling the parser accepts", () => {
    expect(physicalTypeOfName("FLOAT")).toEqual({ kind: "float64" });
    expect(physicalTypeOfName("char")).toEqual({ kind: "text" });
    expect(physicalTypeOfName("STRING")).toEqual({ kind: "text" });
    expect(physicalTypeOfName("DATETIME")).toEqual({ kind: "timestamp", zoned: false });
  });
});

it("maps an explicit native BYTEA descriptor without treating TEXT as bytes", () => {
  expect(physicalTypeOf({ type: "string", declaredTypeName: "BYTEA" })).toEqual({ kind: "bytes" });
  expect(physicalTypeOf({ type: "string", declaredTypeName: "TEXT" })).toEqual({ kind: "text" });
});
