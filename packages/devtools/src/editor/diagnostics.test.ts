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
    const sql =
      "SELECT SUM(a) OVER (ORDER BY a RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t";
    const explain = (message: string): string | undefined =>
      message.includes("RANGE") ? "Use ROWS frames for row distances." : undefined;
    const [explained] = diagnose(sql, explain);
    expect(explained?.message).toContain("RANGE frames take only UNBOUNDED and CURRENT ROW");
    expect(explained?.message).toContain("Use ROWS frames for row distances.");

    // Nothing to add leaves the compiler's own message alone.
    const [plain] = diagnose(sql, () => undefined);
    expect(plain?.message).toBe(
      "RANGE frames take only UNBOUNDED and CURRENT ROW bounds; use ROWS",
    );
  });
});
