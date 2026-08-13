import { describe, expect, it } from "vitest";
import { diagnose } from "./diagnostics.js";

describe("diagnose", () => {
  it("says nothing about SQL that compiles", () => {
    expect(diagnose("SELECT * FROM people")).toEqual([]);
    expect(diagnose("INSERT INTO people (name) VALUES ('a')")).toEqual([]);
    expect(diagnose("DELETE FROM people WHERE name = 'a'")).toEqual([]);
  });

  it("says nothing about an empty document, which is not a mistake", () => {
    expect(diagnose("")).toEqual([]);
    expect(diagnose("   \n  ")).toEqual([]);
  });

  it("marks the token the compiler failed on", () => {
    const sql = "SELECT * FROM people WHERE name = 'oops";
    const [diagnostic] = diagnose(sql);
    expect(diagnostic?.message).toBe("Unterminated string literal");
    expect(diagnostic?.severity).toBe("error");
    expect(sql.slice(diagnostic?.from, diagnostic?.to)).toBe("'oops");
  });

  it("gives a zero-width failure something to underline", () => {
    // A query that ends early has no width to mark, and a zero-length range renders nothing.
    const [diagnostic] = diagnose("SELECT * FROM");
    expect(diagnostic).toBeDefined();
    expect((diagnostic?.to ?? 0) - (diagnostic?.from ?? 0)).toBeGreaterThan(0);
  });

  it("never points past the end of the document", () => {
    for (const sql of ["SELECT", "SELECT * FROM", "SELECT * FROM people WHERE"]) {
      const [diagnostic] = diagnose(sql);
      expect(diagnostic?.to).toBeLessThanOrEqual(sql.length);
      expect(diagnostic?.from).toBeGreaterThanOrEqual(0);
    }
  });

  it("appends what the matrix knows, when it knows something", () => {
    const sql = "SELECT SUM(a) OVER (ORDER BY a GROUPS UNBOUNDED PRECEDING) AS s FROM t";
    const explain = (message: string): string | undefined =>
      message.includes("GROUPS") ? "Use ROWS or RANGE frames." : undefined;
    const [explained] = diagnose(sql, explain);
    expect(explained?.message).toContain("GROUPS window frames are not supported");
    expect(explained?.message).toContain("Use ROWS or RANGE frames.");

    // Nothing to add leaves the compiler's own message alone.
    const [plain] = diagnose(sql, () => undefined);
    expect(plain?.message).toBe("GROUPS window frames are not supported");
  });
});
