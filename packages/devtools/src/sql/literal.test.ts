import { describe, expect, it } from "vitest";
import {
  isExactlyComparable,
  isSortable,
  sqlColumn,
  sqlLiteral,
  sqlOrderColumn,
} from "./literal.js";

describe("sqlLiteral", () => {
  it("doubles a quote instead of ending the string early", () => {
    expect(sqlLiteral("O'Hara", "string")).toBe("'O''Hara'");
    expect(sqlLiteral("'; DROP TABLE people --", "string")).toBe("'''; DROP TABLE people --'");
  });

  it("writes plain strings unchanged", () => {
    expect(sqlLiteral("west", "string")).toBe("'west'");
    expect(sqlLiteral("", "string")).toBe("''");
  });

  it("writes numbers, and refuses the ones SQL has no spelling for", () => {
    expect(sqlLiteral(42, "number")).toBe("42");
    expect(sqlLiteral(-0.5, "number")).toBe("-0.5");
    expect(() => sqlLiteral(Number.NaN, "number")).toThrow(TypeError);
    expect(() => sqlLiteral(Number.POSITIVE_INFINITY, "number")).toThrow(TypeError);
    expect(() => sqlLiteral("twelve", "number")).toThrow(TypeError);
  });

  it("writes booleans as keywords", () => {
    expect(sqlLiteral(true, "boolean")).toBe("TRUE");
    expect(sqlLiteral(false, "boolean")).toBe("FALSE");
  });

  it("writes a datetime as the engine's only date literal, which is day-granular", () => {
    expect(sqlLiteral(new Date("2026-08-12T18:40:00Z"), "datetime")).toBe("DATE '2026-08-12'");
    expect(() => sqlLiteral("not a date", "datetime")).toThrow(TypeError);
  });

  it("writes NULL for every type", () => {
    for (const type of ["string", "number", "boolean", "datetime"] as const) {
      expect(sqlLiteral(null, type)).toBe("NULL");
    }
  });
});

describe("sqlColumn", () => {
  it("qualifies, so a keyword-named column still parses in WHERE", () => {
    expect(sqlColumn("people", "when")).toBe("people.when");
  });

  it("refuses a name that is not a bare identifier", () => {
    expect(() => sqlColumn("people", "a b")).toThrow(TypeError);
    expect(() => sqlColumn("people", "x; DROP TABLE y")).toThrow(TypeError);
    expect(() => sqlColumn("people p", "x")).toThrow(TypeError);
    expect(() => sqlColumn("people", "")).toThrow(TypeError);
  });
});

describe("sqlOrderColumn", () => {
  it("leaves the name bare, because ORDER BY resolves against output aliases", () => {
    expect(sqlOrderColumn("name")).toBe("name");
  });

  it("still refuses anything that is not an identifier", () => {
    expect(() => sqlOrderColumn("name DESC, id")).toThrow(TypeError);
  });
});

describe("isSortable", () => {
  it("rejects names the expression parser claims for itself", () => {
    expect(isSortable("case")).toBe(false);
    expect(isSortable("NOT")).toBe(false);
    expect(isSortable("null")).toBe(false);
    expect(isSortable("exists")).toBe(false);
  });

  it("allows names that are only special with a paren or a string after them", () => {
    // ORDER BY never supplies either, so these parse as plain columns.
    expect(isSortable("count")).toBe(true);
    expect(isSortable("date")).toBe(true);
    expect(isSortable("when")).toBe(true);
    expect(isSortable("order")).toBe(true);
  });
});

describe("isExactlyComparable", () => {
  it("excludes datetimes, whose literals carry no time of day", () => {
    expect(isExactlyComparable("datetime")).toBe(false);
    expect(isExactlyComparable("number")).toBe(true);
    expect(isExactlyComparable("string")).toBe(true);
  });
});
