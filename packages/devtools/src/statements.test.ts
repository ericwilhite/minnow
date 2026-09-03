import { SqlCompileError } from "@minnowdb/core";
import { describe, expect, it } from "vitest";
import {
  changesCatalog,
  changesData,
  classifyStatement,
  needsConfirmation,
  previewQuery,
  summarize,
} from "./statements.js";

describe("classifyStatement", () => {
  it("reads the operation off the compiled plan", () => {
    expect(classifyStatement("SELECT * FROM people")).toEqual({ kind: "select", limited: false });
    expect(classifyStatement("INSERT INTO people (name, score) VALUES ('a', 1), ('b', 2)")).toEqual(
      {
        kind: "insert",
        table: "people",
        columns: ["name", "score"],
        rowCount: 2,
      },
    );
    expect(classifyStatement("UPDATE people SET score = 1 WHERE name = 'a'")).toEqual({
      kind: "update",
      table: "people",
      columns: ["score"],
      filtered: true,
    });
    expect(classifyStatement("DELETE FROM people WHERE score < 0")).toEqual({
      kind: "delete",
      table: "people",
      filtered: true,
    });
  });

  it("is not fooled by keywords inside string literals", () => {
    // Matching text would call this a delete; the compiler knows it only selects.
    expect(classifyStatement("SELECT * FROM people WHERE name = 'DELETE FROM people'")).toEqual({
      kind: "select",
      limited: false,
    });
  });

  it("reports an unfiltered statement as such", () => {
    expect(classifyStatement("DELETE FROM people")).toMatchObject({ filtered: false });
    expect(classifyStatement("UPDATE people SET score = 0")).toMatchObject({ filtered: false });
  });

  it("routes DDL and transaction statements through execute with an exact review", () => {
    expect(
      classifyStatement("CREATE UNIQUE INDEX people_lookup ON people (city ASC, score DESC)"),
    ).toMatchObject({
      kind: "execute",
      operation: "create-index",
      summary: {
        title: "Create unique index people_lookup",
        facts: [
          ["index", "people_lookup"],
          ["table", "people"],
          ["columns", "city ASC, score DESC"],
        ],
        confirmLabel: "Create index",
      },
    });
    expect(classifyStatement("DROP TABLE people")).toMatchObject({
      kind: "execute",
      operation: "drop-table",
      summary: { destructive: true },
    });
    expect(classifyStatement("BEGIN")).toMatchObject({
      kind: "execute",
      operation: "transaction",
      summary: { confirmLabel: "Begin" },
      confirm: false,
    });
    expect(classifyStatement("ROLLBACK")).toMatchObject({
      kind: "execute",
      operation: "transaction",
      summary: { confirmLabel: "Rollback", destructive: true },
      confirm: true,
    });
    expect(classifyStatement("ALTER TABLE people ADD COLUMN note VARCHAR")).toMatchObject({
      kind: "execute",
      operation: "add-column",
    });
    expect(classifyStatement("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")).toMatchObject({
      kind: "execute",
      operation: "create-enum",
      summary: {
        title: "Create enum type mood",
        facts: [
          ["type", "mood"],
          ["values", "sad, ok, happy"],
        ],
        confirmLabel: "Create type",
      },
    });
    expect(classifyStatement("CREATE SEQUENCE order_number")).toMatchObject({
      kind: "execute",
      operation: "create-sequence",
      summary: {
        title: "Create sequence order_number",
        facts: [["sequence", "order_number"]],
        confirmLabel: "Create sequence",
      },
    });
  });

  it("treats session settings as neither a query nor a write", () => {
    expect(classifyStatement("SET search_path = public")).toEqual({
      kind: "session",
      operation: "set",
    });
    expect(classifyStatement("RESET search_path")).toEqual({ kind: "session", operation: "set" });
    expect(classifyStatement("SHOW search_path")).toEqual({ kind: "session", operation: "show" });
  });

  it("passes the located compile error through", () => {
    const sql = "SELECT * FROM people WHERE name = 'oops";
    expect(() => classifyStatement(sql)).toThrow(SqlCompileError);
    try {
      classifyStatement(sql);
    } catch (error) {
      const located = error as SqlCompileError;
      expect(sql.slice(located.offset, located.offset + located.length)).toBe("'oops");
    }
  });
});

describe("changesData", () => {
  it("is the gate every write passes through", () => {
    expect(changesData({ kind: "select", limited: false })).toBe(false);
    expect(changesData(classifyStatement("DELETE FROM people"))).toBe(true);
    expect(changesData(classifyStatement("UPDATE people SET score = 0"))).toBe(true);
    expect(changesData(classifyStatement("INSERT INTO people (name) VALUES ('a')"))).toBe(true);
    expect(changesData(classifyStatement("CREATE INDEX by_name ON people(name)"))).toBe(true);
    expect(changesData(classifyStatement("BEGIN"))).toBe(true);
    expect(changesData(classifyStatement("COMMIT"))).toBe(true);
    expect(changesData(classifyStatement("ROLLBACK"))).toBe(true);
  });

  it("lets session settings through even when writes are off", () => {
    expect(changesData(classifyStatement("SET search_path = public"))).toBe(false);
    expect(changesData(classifyStatement("RESET search_path"))).toBe(false);
    expect(changesData(classifyStatement("SHOW search_path"))).toBe(false);
  });
});

describe("needsConfirmation", () => {
  it("asks before every data and schema change", () => {
    expect(needsConfirmation(classifyStatement("DELETE FROM people"))).toBe(true);
    expect(needsConfirmation(classifyStatement("INSERT INTO people (name) VALUES ('a')"))).toBe(
      true,
    );
    expect(needsConfirmation(classifyStatement("CREATE INDEX by_name ON people(name)"))).toBe(true);
    expect(needsConfirmation(classifyStatement("DROP TABLE people"))).toBe(true);
  });

  it("asks before a rollback but not before begin or commit", () => {
    // Each statement inside the transaction is confirmed on its own; COMMIT only keeps what was
    // already approved, while ROLLBACK throws it away.
    expect(needsConfirmation(classifyStatement("ROLLBACK"))).toBe(true);
    expect(needsConfirmation(classifyStatement("BEGIN"))).toBe(false);
    expect(needsConfirmation(classifyStatement("COMMIT"))).toBe(false);
  });

  it("never asks for queries or session settings", () => {
    expect(needsConfirmation({ kind: "select", limited: false })).toBe(false);
    expect(needsConfirmation(classifyStatement("SET search_path = public"))).toBe(false);
    expect(needsConfirmation(classifyStatement("RESET search_path"))).toBe(false);
    expect(needsConfirmation(classifyStatement("SHOW search_path"))).toBe(false);
  });
});

describe("summarize", () => {
  it("says what an insert will do", () => {
    const summary = summarize(classifyStatement("INSERT INTO people (name) VALUES ('a')"));
    expect(summary.title).toBe("Insert 1 row into people");
    expect(summary.warning).toBeUndefined();
  });

  it("warns when an update or delete has no filter", () => {
    const update = summarize(classifyStatement("UPDATE people SET score = 0"));
    expect(update.title).toBe("Update every row in people");
    expect(update.warning).toContain("every row");

    const remove = summarize(classifyStatement("DELETE FROM people"));
    expect(remove.title).toBe("Delete every row from people");
    expect(remove.warning).toContain("every row");
  });

  it("drops the warning once a filter is present", () => {
    expect(
      summarize(classifyStatement("DELETE FROM people WHERE score < 0")).warning,
    ).toBeUndefined();
  });

  it("explains destructive schema changes before they run", () => {
    const summary = summarize(classifyStatement("DROP INDEX by_name"));
    expect(summary.title).toBe("Drop index by_name");
    expect(summary.warning).toContain("scan more data");
    expect(summary.destructive).toBe(true);
  });
});

describe("limited", () => {
  it("says whether a SELECT bounds itself, so the console knows when to cap it", () => {
    expect(classifyStatement("SELECT 1")).toEqual({ kind: "select", limited: false });
    expect(classifyStatement("SELECT 1 LIMIT 5")).toEqual({ kind: "select", limited: true });
    expect(classifyStatement("SELECT 1 FETCH FIRST 3 ROWS ONLY")).toEqual({
      kind: "select",
      limited: true,
    });
  });
});

describe("changesCatalog", () => {
  it("is true for the statements the rail has to reread after", () => {
    for (const sql of [
      "CREATE TABLE t (id INTEGER PRIMARY KEY)",
      "DROP TABLE t",
      "CREATE INDEX i ON t (id)",
      "CREATE VIEW v AS SELECT 1 AS x",
      "ALTER TABLE t ADD COLUMN c TEXT",
    ]) {
      expect(changesCatalog(classifyStatement(sql))).toBe(true);
    }
  });

  it("is false for data changes and reads", () => {
    expect(changesCatalog(classifyStatement("INSERT INTO t (id) VALUES (1)"))).toBe(false);
    expect(changesCatalog(classifyStatement("SELECT 1"))).toBe(false);
    expect(changesCatalog(classifyStatement("SET search_path = x"))).toBe(false);
  });
});

describe("previewQuery", () => {
  it("counts the rows an UPDATE or DELETE with a WHERE clause will touch", () => {
    const update = "UPDATE people SET score = 1 WHERE city = 'x' AND score > (SELECT 2)";
    expect(previewQuery(update, classifyStatement(update))).toBe(
      "SELECT COUNT(*) AS row_count FROM people WHERE city = 'x' AND score > (SELECT 2)",
    );
    const remove = "DELETE FROM people WHERE name = 'where; returning' RETURNING id";
    expect(previewQuery(remove, classifyStatement(remove))).toBe(
      "SELECT COUNT(*) AS row_count FROM people WHERE name = 'where; returning'",
    );
  });

  it("counts the whole table for an unfiltered statement", () => {
    const sql = "DELETE FROM people";
    expect(previewQuery(sql, classifyStatement(sql))).toBe(
      "SELECT COUNT(*) AS row_count FROM people",
    );
  });

  it("ignores WHERE inside a subquery's parentheses and words inside comments", () => {
    const sql =
      "UPDATE people SET score = (SELECT MAX(s) FROM x WHERE y = 1) -- where\n WHERE id = 3;";
    expect(previewQuery(sql, classifyStatement(sql))).toBe(
      "SELECT COUNT(*) AS row_count FROM people WHERE id = 3",
    );
  });

  it("has nothing to say for other statements", () => {
    expect(previewQuery("SELECT 1", classifyStatement("SELECT 1"))).toBeUndefined();
    const insert = "INSERT INTO people (id) VALUES (1)";
    expect(previewQuery(insert, classifyStatement(insert))).toBeUndefined();
  });
});
