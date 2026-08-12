import { describe, expect, it } from "vitest";
import { describeValue, formatForInput, inputHint, parseInput } from "./values.js";

describe("parseInput", () => {
  it("reads blank and NULL as null, but only where the column allows it", () => {
    expect(parseInput("", "string", true)).toEqual({ ok: true, value: null });
    expect(parseInput("  ", "number", true)).toEqual({ ok: true, value: null });
    expect(parseInput("null", "string", true)).toEqual({ ok: true, value: null });
    expect(parseInput("", "string", false)).toEqual({
      ok: false,
      message: "This column cannot be null.",
    });
  });

  it("parses numbers and refuses text", () => {
    expect(parseInput("42.5", "number", false)).toEqual({ ok: true, value: 42.5 });
    expect(parseInput("-3", "number", false)).toEqual({ ok: true, value: -3 });
    expect(parseInput("twelve", "number", false)).toEqual({
      ok: false,
      message: "Not a number: twelve",
    });
    expect(parseInput("Infinity", "number", false).ok).toBe(false);
  });

  it("accepts the spellings people actually type for booleans", () => {
    for (const text of ["true", "TRUE", "1", "yes"]) {
      expect(parseInput(text, "boolean", false)).toEqual({ ok: true, value: true });
    }
    for (const text of ["false", "FALSE", "0", "no"]) {
      expect(parseInput(text, "boolean", false)).toEqual({ ok: true, value: false });
    }
    expect(parseInput("maybe", "boolean", false).ok).toBe(false);
  });

  it("keeps a datetime's full precision, unlike a SQL literal", () => {
    const parsed = parseInput("2026-08-12T09:14:00Z", "datetime", false);
    expect(parsed.ok).toBe(true);
    expect(parsed.ok && (parsed.value as Date).toISOString()).toBe("2026-08-12T09:14:00.000Z");
    expect(parseInput("last tuesday", "datetime", false).ok).toBe(false);
  });

  it("keeps a string as typed, trimmed", () => {
    expect(parseInput("  Ada  ", "string", false)).toEqual({ ok: true, value: "Ada" });
  });
});

describe("formatForInput", () => {
  it("round-trips through parseInput", () => {
    const date = new Date("2026-08-12T09:14:00Z");
    expect(parseInput(formatForInput(date), "datetime", false)).toEqual({ ok: true, value: date });
    expect(parseInput(formatForInput(42), "number", false)).toEqual({ ok: true, value: 42 });
    expect(parseInput(formatForInput(false), "boolean", false)).toEqual({ ok: true, value: false });
  });

  it("shows an empty box for null", () => {
    expect(formatForInput(null)).toBe("");
  });
});

describe("describeValue", () => {
  it("keeps NULL distinguishable from the text 'NULL'", () => {
    expect(describeValue(null)).toBe("NULL");
    expect(describeValue("NULL")).toBe("'NULL'");
    expect(describeValue(0)).toBe("0");
    expect(describeValue(new Date("2026-08-12T09:14:00Z"))).toBe("2026-08-12T09:14:00.000Z");
  });
});

describe("inputHint", () => {
  it("shows the shape rather than the column name", () => {
    expect(inputHint("datetime", false)).toBe("2026-08-12T09:14:00Z");
    expect(inputHint("boolean", true)).toBe("true / false or NULL");
    expect(inputHint("string", false)).toBe("string");
  });
});
