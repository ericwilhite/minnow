import { describe, expect, it } from "vitest";
import { sqlColumn, sqlIdentifier, sqlLiteral } from "./literal.js";

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

  it("writes a datetime to the millisecond, in UTC", () => {
    expect(sqlLiteral(new Date("2026-08-12T18:40:00.250Z"), "datetime")).toBe(
      "TIMESTAMP '2026-08-12T18:40:00.250Z'",
    );
    expect(sqlLiteral("2026-08-12", "datetime")).toBe("TIMESTAMP '2026-08-12T00:00:00.000Z'");
    expect(() => sqlLiteral("not a date", "datetime")).toThrow(TypeError);
  });

  it("writes NULL for every type", () => {
    for (const type of ["string", "number", "boolean", "datetime"] as const) {
      expect(sqlLiteral(null, type)).toBe("NULL");
    }
  });
});

describe("sqlIdentifier", () => {
  it("leaves a bare identifier alone, keywords included", () => {
    for (const name of ["people", "case", "NOT", "null", "_x1", "order"]) {
      expect(sqlIdentifier(name)).toBe(name);
    }
  });

  it("quotes anything else, doubling quotes inside", () => {
    expect(sqlIdentifier("order date")).toBe('"order date"');
    expect(sqlIdentifier("a-b")).toBe('"a-b"');
    expect(sqlIdentifier('quo"te')).toBe('"quo""te"');
    expect(sqlIdentifier("x; DROP TABLE y")).toBe('"x; DROP TABLE y"');
    expect(sqlIdentifier("1st")).toBe('"1st"');
  });

  it("refuses an empty name, which no quoting can express", () => {
    expect(() => sqlIdentifier("")).toThrow(TypeError);
  });
});

describe("sqlColumn", () => {
  it("qualifies, so a keyword-named column still parses in WHERE", () => {
    expect(sqlColumn("people", "when")).toBe("people.when");
  });

  it("quotes either side when it has to", () => {
    expect(sqlColumn("people", "a b")).toBe('people."a b"');
    expect(sqlColumn("people p", "x")).toBe('"people p".x');
    expect(() => sqlColumn("people", "")).toThrow(TypeError);
  });
});
