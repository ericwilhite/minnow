import { SqlCompileError } from "@minnowdb/core";
import { describe, expect, it } from "vitest";
import { changesData, classifyStatement, summarize } from "./statements.js";

describe("classifyStatement", () => {
  it("reads the operation off the compiled plan", () => {
    expect(classifyStatement("SELECT * FROM people")).toEqual({ kind: "select" });
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
    expect(changesData({ kind: "select" })).toBe(false);
    expect(changesData(classifyStatement("DELETE FROM people"))).toBe(true);
    expect(changesData(classifyStatement("UPDATE people SET score = 0"))).toBe(true);
    expect(changesData(classifyStatement("INSERT INTO people (name) VALUES ('a')"))).toBe(true);
    expect(changesData(classifyStatement("CREATE INDEX by_name ON people(name)"))).toBe(true);
    expect(changesData(classifyStatement("COMMIT"))).toBe(true);
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
