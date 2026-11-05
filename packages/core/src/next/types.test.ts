import { describe, expect, it } from "vitest";
import {
  layoutOf,
  physicalTypeOf,
  physicalTypeOfName,
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
    ["DOUBLE / REAL / number", { type: "number" }, { kind: "float64" }],
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
