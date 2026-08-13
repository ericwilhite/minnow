import { describe, expect, it } from "vitest";
import { spaced } from "./editor.js";

describe("spaced", () => {
  it("separates a name from a word that already ends the text", () => {
    // Clicking two rail entries in a row must not produce `peoplepeople`.
    expect(spaced("SELECT people.name", "people")).toBe(" people");
    expect(spaced("SELECT 1", "people")).toBe(" people");
  });

  it("leaves punctuation that legitimately abuts a name alone", () => {
    expect(spaced("SELECT ", "name")).toBe("name");
    expect(spaced("SELECT COUNT(", "name")).toBe("name");
    expect(spaced("SELECT a,", "name")).toBe("name");
    expect(spaced("SELECT people.", "name")).toBe("name");
    expect(spaced("SELECT a\n", "name")).toBe("name");
  });

  it("adds nothing at the very start", () => {
    expect(spaced("", "people")).toBe("people");
  });
});
