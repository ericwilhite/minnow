import { describe, expect, it } from "vitest";
import type { PhysicalType } from "../types.js";
import { renderOf, resultColumn, sqlTypeName } from "./result.js";

const int64: PhysicalType = { kind: "int64" };
const exactSum: PhysicalType = { kind: "decimal", precision: null, scale: 0 };

describe("sqlTypeName", () => {
  it("prints PostgreSQL's format_type names", () => {
    const cases: Array<[PhysicalType, string]> = [
      [{ kind: "bool" }, "boolean"],
      [{ kind: "int32", bits: 16 }, "smallint"],
      [{ kind: "int32", bits: 32 }, "integer"],
      [int64, "bigint"],
      [{ kind: "float32" }, "real"],
      [{ kind: "float64" }, "double precision"],
      [{ kind: "decimal", precision: null, scale: null }, "numeric"],
      [{ kind: "decimal", precision: 10, scale: 2 }, "numeric(10,2)"],
      [{ kind: "decimal", precision: 5, scale: 0 }, "numeric(5,0)"],
      [{ kind: "date" }, "date"],
      [{ kind: "time" }, "time without time zone"],
      [{ kind: "timestamp", zoned: false }, "timestamp without time zone"],
      [{ kind: "timestamp", zoned: true }, "timestamp with time zone"],
      [{ kind: "interval" }, "interval"],
      [{ kind: "text" }, "text"],
      [{ kind: "bytes" }, "bytea"],
      [{ kind: "json", binary: false }, "json"],
      [{ kind: "json", binary: true }, "jsonb"],
      [{ kind: "uuid" }, "uuid"],
      [{ kind: "enum", name: "mood", labels: ["sad", "ok"] }, "mood"],
      [{ kind: "array", element: { kind: "int32", bits: 32 } }, "integer[]"],
      [{ kind: "array", element: { kind: "decimal", precision: 8, scale: 3 } }, "numeric(8,3)[]"],
    ];
    for (const [type, name] of cases) expect(sqlTypeName(type)).toBe(name);
  });

  it("refuses the internal locator type", () => {
    expect(() => sqlTypeName({ kind: "locator", tableId: 1 })).toThrow(TypeError);
  });
});

describe("renderOf (DECISIONS.md D2, ruling R16)", () => {
  it("renders BIGINT values and SUM(bigint) as bigint", () => {
    expect(renderOf(int64, "value")).toBe("bigint");
    expect(renderOf(exactSum, "sumBigint")).toBe("bigint");
  });

  it("renders COUNT, SUM(int2/int4) and row numbering as number", () => {
    expect(renderOf(int64, "count")).toBe("number");
    expect(renderOf(int64, "sumNarrow")).toBe("number");
    expect(renderOf(int64, "rank")).toBe("number");
  });

  it("has no choice to make for other types", () => {
    expect(renderOf({ kind: "int32", bits: 32 }, "value")).toBeNull();
    expect(renderOf({ kind: "decimal", precision: 10, scale: 2 }, "value")).toBeNull();
    expect(renderOf({ kind: "timestamp", zoned: true }, "value")).toBeNull();
    expect(renderOf({ kind: "float64" }, "count")).toBeNull();
  });

  it("builds the owner's column metadata", () => {
    expect(resultColumn({ name: "n", type: int64, nullable: false }, "count")).toEqual({
      name: "n",
      type: int64,
      nullable: false,
      sqlType: "bigint",
      render: "number",
    });
  });
});
