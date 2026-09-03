import { describe, expect, it } from "vitest";
import { splitStatements, withAppendedClause } from "./split.js";

const texts = (script: string): string[] => splitStatements(script).map((s) => s.sql);

describe("splitStatements", () => {
  it("splits at semicolons and drops empty statements", () => {
    expect(texts("SELECT 1; SELECT 2;; ;")).toEqual(["SELECT 1", "SELECT 2"]);
    expect(texts("  SELECT 1  ")).toEqual(["SELECT 1"]);
    expect(texts("")).toEqual([]);
  });

  it("records where each statement sits, for pointing at an error", () => {
    const parts = splitStatements("SELECT 1;\n  SELECT 2;");
    expect(parts).toEqual([
      { sql: "SELECT 1", from: 0, to: 8 },
      { sql: "SELECT 2", from: 12, to: 20 },
    ]);
  });

  it("leaves semicolons inside strings, identifiers, and comments alone", () => {
    expect(texts("SELECT 'a;b' AS x; SELECT \"c;d\" FROM t")).toEqual([
      "SELECT 'a;b' AS x",
      'SELECT "c;d" FROM t',
    ]);
    expect(texts("SELECT 'it''s; here'; SELECT 2")).toEqual(["SELECT 'it''s; here'", "SELECT 2"]);
    expect(texts("SELECT 1 -- ; not here\n; SELECT 2 /* ; nor here */; SELECT 3")).toEqual([
      "SELECT 1 -- ; not here",
      "SELECT 2 /* ; nor here */",
      "SELECT 3",
    ]);
  });

  it("keeps a trigger body together, CASE expressions included", () => {
    const trigger =
      "CREATE TRIGGER t AFTER INSERT ON orders FOR EACH ROW BEGIN " +
      "UPDATE totals SET n = CASE WHEN n IS NULL THEN 1 ELSE n + 1 END; " +
      "INSERT INTO log (msg) VALUES ('x'); END";
    expect(texts(`${trigger}; SELECT 1`)).toEqual([trigger, "SELECT 1"]);
  });

  it("treats a leading BEGIN as the transaction statement, not a block", () => {
    expect(texts("BEGIN; INSERT INTO t VALUES (1); COMMIT")).toEqual([
      "BEGIN",
      "INSERT INTO t VALUES (1)",
      "COMMIT",
    ]);
    expect(texts("begin;\nselect 1;\nrollback;")).toEqual(["begin", "select 1", "rollback"]);
  });

  it("runs an unterminated quote to the end rather than splitting inside it", () => {
    expect(texts("SELECT 'open; SELECT 2")).toEqual(["SELECT 'open; SELECT 2"]);
  });
});

describe("withAppendedClause", () => {
  it("drops a trailing semicolon and puts the clause on its own line", () => {
    expect(withAppendedClause("SELECT 1;", "LIMIT 5")).toBe("SELECT 1\nLIMIT 5");
    expect(withAppendedClause("SELECT 1 ;  \n", "LIMIT 5")).toBe("SELECT 1\nLIMIT 5");
  });

  it("survives a trailing line comment", () => {
    expect(withAppendedClause("SELECT 1 -- all", "LIMIT 5")).toBe("SELECT 1 -- all\nLIMIT 5");
  });
});
