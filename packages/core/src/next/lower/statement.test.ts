import { describe, expect, it } from "vitest";
import { compileStatement } from "../../engine/query.js";
import { classifyStatement } from "./statement.js";

const classify = (sql: string) => classifyStatement(compileStatement(sql));

describe("classifyStatement", () => {
  it("names queries, DML and their PostgreSQL command tags", () => {
    expect(classify("SELECT 1")).toEqual({
      kind: "query",
      command: "SELECT",
      controlsTransaction: false,
    });
    expect(classify("INSERT INTO t (a) VALUES (1)").command).toBe("INSERT");
    expect(classify("UPDATE t SET a = 1").command).toBe("UPDATE");
    expect(classify("DELETE FROM t").command).toBe("DELETE");
    expect(classify("TRUNCATE t")).toEqual({
      kind: "modify",
      command: "DELETE",
      controlsTransaction: false,
    });
    expect(classify("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE").kind).toBe(
      "modify",
    );
  });

  it("marks every transaction-control statement", () => {
    for (const [sql, command] of [
      ["BEGIN", "BEGIN"],
      ["START TRANSACTION READ ONLY", "BEGIN"],
      ["COMMIT", "COMMIT"],
      ["END", "COMMIT"],
      ["ROLLBACK", "ROLLBACK"],
      ["SAVEPOINT a", "SAVEPOINT"],
      ["ROLLBACK TO SAVEPOINT a", "ROLLBACK"],
      ["RELEASE SAVEPOINT a", "RELEASE"],
    ] as const)
      expect(classify(sql), sql).toEqual({
        kind: "transactionControl",
        command,
        controlsTransaction: true,
      });
  });

  it("classifies DDL and utilities", () => {
    expect(classify("CREATE TABLE t (a INT)").command).toBe("CREATE TABLE");
    expect(classify("CREATE TABLE u AS SELECT 1 AS a")).toEqual({
      kind: "ddl",
      command: "SELECT",
      controlsTransaction: false,
    });
    expect(classify("ALTER TABLE t ADD COLUMN b INT").command).toBe("ALTER TABLE");
    expect(classify("CREATE INDEX i ON t (a)").command).toBe("CREATE INDEX");
    expect(classify("DROP TABLE t").command).toBe("DROP TABLE");
    expect(classify("SET search_path TO public")).toEqual({
      kind: "utility",
      command: "SET",
      controlsTransaction: false,
    });
    expect(classify("SHOW timezone").command).toBe("SHOW");
  });
});
