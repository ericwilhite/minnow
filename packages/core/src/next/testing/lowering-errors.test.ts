/**
 * Every failure of `Lowerer.lower` carries PostgreSQL's SQLSTATE (lower/errors.ts): syntax errors
 * (42601), unknown relations (42P01), columns (42703), functions (42883) and types (42704), and
 * the other compile-error classes the front end raises. Each case runs in PGlite and through
 * lowering (pglite-oracle.ts), and the codes must match.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PlanningError } from "../lower/errors.js";
import { check } from "./pglite-oracle.js";
import { LoweredDatabase } from "./lowering-harness.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const SETUP = [
  "CREATE TABLE a (id integer, v integer)",
  "CREATE TABLE b (id integer, v integer)",
  "CREATE VIEW va AS SELECT id FROM a",
  "CREATE SEQUENCE s",
];

describe("lowering's errors carry PostgreSQL's SQLSTATE", () => {
  it("syntax errors are 42601", async () => {
    await check(pg, SETUP, [
      { sql: "SELEC 1" },
      { sql: "SELECT FROM WHERE" },
      { sql: "SELECT (1" },
      { sql: "SELECT 1 +" },
      { sql: "SELECT 'abc" },
      { sql: "INSERT INTO a VALUES (1, 2" },
      { sql: "CREATE TABLE (id int)" },
      { sql: "SELECT id FROM a UNION SELECT id, v FROM b" },
    ]);
  });

  it("unknown relations are 42P01, wherever they are named", async () => {
    await check(pg, SETUP, [
      { sql: "SELECT * FROM missing" },
      { sql: "SELECT id FROM missing" },
      { sql: "SELECT * FROM a JOIN missing m ON m.id = a.id" },
      { sql: "SELECT x.id FROM a" },
      { sql: "SELECT a.id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND z.v = 1)" },
      { sql: "INSERT INTO missing VALUES (1)" },
      { sql: "UPDATE missing SET id = 1" },
      { sql: "DELETE FROM missing" },
      { sql: "DROP TABLE missing" },
      { sql: "DROP VIEW missing" },
      { sql: "CREATE INDEX i ON missing (id)" },
      { sql: "ALTER TABLE missing ADD COLUMN c integer" },
      { sql: "SELECT nextval('missing')" },
    ]);
  });

  it("unknown columns, functions and types: 42703, 42883, 42704", async () => {
    await check(pg, SETUP, [
      { sql: "SELECT nope FROM a" },
      { sql: "SELECT a.nope FROM a" },
      { sql: "UPDATE a SET nope = 1" },
      { sql: "INSERT INTO a (nope) VALUES (1)" },
      { sql: "SELECT id FROM a ORDER BY nope" },
      { sql: "CREATE INDEX i ON a (nope)" },
      { sql: "SELECT nofunc(1)" },
      { sql: "SELECT abs('x', 1)" },
      { sql: "SELECT length(1, 2, 3)" },
      { sql: "SELECT sum(id, id) FROM a" },
      { sql: "SELECT CAST(1 AS notatype)" },
      { sql: "SELECT 1::notatype" },
      { sql: "CREATE TABLE t (c notatype)" },
    ]);
  });

  it("the other compile-error classes", async () => {
    await check(pg, SETUP, [
      { sql: "SELECT id FROM a, b" },
      { sql: "CREATE TABLE t (c integer, c integer)" },
      { sql: "CREATE TABLE a (id integer)" },
      { sql: "CREATE VIEW va AS SELECT 1" },
      { sql: "CREATE SEQUENCE s" },
      { sql: "SELECT id FROM a GROUP BY v" },
      { sql: "SELECT sum(sum(id)) FROM a" },
      { sql: "SELECT id FROM a WHERE sum(id) > 1" },
      { sql: "SELECT 'x'::integer" },
      { sql: "SELECT id FROM a WHERE id = 'x'" },
    ]);
  });

  it("keeps the parser's position and the original error, and refuses with 0A000", () => {
    const next = new LoweredDatabase();
    for (const sql of SETUP) next.execute(sql);
    const failure = (sql: string): PlanningError => {
      try {
        next.lower(sql);
      } catch (error) {
        if (error instanceof PlanningError) return error;
        throw error;
      }
      throw new Error(`${sql} lowered`);
    };
    const syntax = failure("SELEC 1");
    expect([syntax.sqlState, syntax.position]).toEqual(["42601", { offset: 0, length: 5 }]);
    expect((syntax.cause as Error).name).toBe("SqlCompileError");
    // Shapes the front end refuses though PostgreSQL accepts them: feature not supported.
    expect(
      failure(
        "SELECT id FROM a GROUP BY id HAVING max(v) > (SELECT max(v) FROM b WHERE b.id = a.id)",
      ).sqlState,
    ).toBe("0A000");
    expect(failure("SELECT id FROM a WHERE id IN (SELECT a.id FROM b)").sqlState).toBe("0A000");
  });
});

describe("BEGIN READ ONLY (ruling R11.2)", () => {
  it("lowers the access mode", () => {
    const next = new LoweredDatabase();
    const mode = (sql: string) => {
      const lowered = next.lower(sql);
      if (lowered.kind !== "transactionControl") throw new Error(`${sql}: ${lowered.kind}`);
      return [lowered.action, lowered.readOnly];
    };
    expect(mode("BEGIN READ ONLY")).toEqual(["begin", true]);
    expect(mode("START TRANSACTION READ ONLY")).toEqual(["begin", true]);
    expect(mode("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")).toEqual([
      "begin",
      true,
    ]);
    expect(mode("BEGIN")).toEqual(["begin", false]);
    expect(mode("BEGIN READ WRITE")).toEqual(["begin", false]);
    expect(mode("BEGIN ISOLATION LEVEL READ COMMITTED")).toEqual(["begin", false]);
    expect(mode("COMMIT")).toEqual(["commit", false]);
  });

  it("ignores READ ONLY inside a comment", () => {
    const next = new LoweredDatabase();
    const line = next.lower("BEGIN -- READ ONLY\n");
    expect(line.kind === "transactionControl" && line.readOnly).toBe(false);
    const block = next.lower("BEGIN /* READ ONLY */ READ WRITE");
    expect(block.kind === "transactionControl" && block.readOnly).toBe(false);
    const after = next.lower("BEGIN /* mode */ READ ONLY");
    expect(after.kind === "transactionControl" && after.readOnly).toBe(true);
  });
});
