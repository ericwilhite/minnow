import { describe, expect, it } from "vitest";
import { samePhysicalType } from "../types.js";
import {
  FUNCTIONS,
  IMPLICIT_CASTS,
  NON_STRICT_FUNCTIONS,
  VOLATILE_FUNCTIONS,
  physicalTypeOfSql,
  sqlTypeOf,
  type SqlType,
} from "./functions.js";

const SQL_TYPES: readonly SqlType[] = [
  "bool",
  "int2",
  "int4",
  "int8",
  "float4",
  "float8",
  "numeric",
  "text",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "interval",
  "json",
  "jsonb",
  "uuid",
  "bytea",
];

describe("the IR function table (ruling R17.5)", () => {
  it("maps every SQL type to a physical type and back", () => {
    for (const type of SQL_TYPES) expect(sqlTypeOf(physicalTypeOfSql(type))).toBe(type);
  });

  it("has no two overloads of one function with the same argument types", () => {
    for (const [fn, overloads] of Object.entries(FUNCTIONS)) {
      const keys = overloads.map((overload) => overload.args.join(","));
      expect(new Set(keys).size, fn).toBe(keys.length);
    }
  });

  it("uses a type variable only with a result of that variable or a fixed type", () => {
    for (const [fn, overloads] of Object.entries(FUNCTIONS))
      for (const overload of overloads) {
        if (overload.result === "T") expect(overload.args, fn).toContain("T");
        if (overload.variadic === true) expect(overload.args.length, fn).toBeGreaterThan(0);
      }
  });

  it("names only listed functions as non-strict or volatile", () => {
    for (const fn of [...NON_STRICT_FUNCTIONS, ...VOLATILE_FUNCTIONS])
      expect(FUNCTIONS[fn], fn).toBeDefined();
  });

  it("lists implicit casts as a ladder that never narrows", () => {
    for (const [from, targets] of Object.entries(IMPLICIT_CASTS))
      for (const to of targets ?? []) {
        expect(from).not.toBe(to);
        expect(samePhysicalType(physicalTypeOfSql(from as SqlType), physicalTypeOfSql(to))).toBe(
          false,
        );
      }
    expect(IMPLICIT_CASTS.int8).not.toContain("int4");
    expect(IMPLICIT_CASTS.text).toBeUndefined();
  });
});
