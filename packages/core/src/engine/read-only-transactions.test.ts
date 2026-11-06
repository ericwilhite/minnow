/**
 * READ ONLY transactions, against PostgreSQL (PGlite): a write, DDL or sequence change in a
 * read-only transaction raises 25006 (read_only_sql_transaction), and READ WRITE cannot be set
 * once the transaction has run a statement (25001). Every way to make a transaction read-only
 * (or not) × every kind of statement runs in both; the outcomes must match.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach, REFUSAL_STATES } from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const SETUP = [
  "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
  "CREATE TABLE u (id INTEGER PRIMARY KEY, v INTEGER)",
  "CREATE SEQUENCE s",
  "INSERT INTO t VALUES (1, 10), (2, 20)",
];

/** Statements that open a transaction and set (or leave) its access mode. */
const PREFIXES: ReadonlyArray<{
  readonly statements: readonly string[];
  readonly readOnly: boolean;
}> = [
  { statements: ["BEGIN READ ONLY"], readOnly: true },
  { statements: ["START TRANSACTION READ ONLY"], readOnly: true },
  { statements: ["BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY"], readOnly: true },
  { statements: ["BEGIN READ WRITE, READ ONLY"], readOnly: true },
  { statements: ["BEGIN", "SET TRANSACTION READ ONLY"], readOnly: true },
  { statements: ["BEGIN", "SELECT 1 AS one", "SET TRANSACTION READ ONLY"], readOnly: true },
  { statements: ["BEGIN READ ONLY", "SET TRANSACTION READ WRITE"], readOnly: false },
  { statements: ["BEGIN READ ONLY, READ WRITE"], readOnly: false },
  { statements: ["BEGIN READ WRITE"], readOnly: false },
  { statements: ["BEGIN"], readOnly: false },
];

/** Statements run inside; `inWriteTransaction` marks those the current engine runs in any one. */
const STATEMENTS: ReadonlyArray<{ readonly sql: string; readonly inWriteTransaction: boolean }> = [
  { sql: "INSERT INTO t VALUES (3, 30)", inWriteTransaction: true },
  { sql: "UPDATE t SET v = v + 1 WHERE id = 1", inWriteTransaction: true },
  { sql: "DELETE FROM t WHERE id = 2", inWriteTransaction: true },
  { sql: "SELECT nextval('s') AS n", inWriteTransaction: true },
  { sql: "SELECT count(*) AS n FROM t", inWriteTransaction: true },
  {
    sql: "MERGE INTO u USING t ON u.id = t.id WHEN NOT MATCHED THEN INSERT VALUES (t.id, t.v)",
    inWriteTransaction: false,
  },
  { sql: "TRUNCATE u", inWriteTransaction: false },
  { sql: "CREATE TABLE w (id INTEGER)", inWriteTransaction: false },
  { sql: "DROP TABLE u", inWriteTransaction: false },
];

/** An outcome class: ok, or PostgreSQL's code (the current engine's error, by its message). */
function minnowClass(error: unknown): string {
  const message = String(error);
  if (message.includes("in a read-only transaction")) return "25006";
  if (message.includes("read-write mode must be set before any query")) return "25001";
  return `error ${message}`;
}

describe("READ ONLY transactions", () => {
  it("refuse writes, DDL and sequence changes exactly as PostgreSQL", async () => {
    const mismatches: string[] = [];
    const reach = createReach<string>("read-only transactions", REFUSAL_STATES);
    reach.beginHistory();
    for (const prefix of PREFIXES)
      for (const statement of STATEMENTS) {
        if (!prefix.readOnly && !statement.inWriteTransaction) continue;
        const database = new MinnowDatabase(new MemoryBlockStore());
        await pg.exec("DROP SCHEMA IF EXISTS ro CASCADE; CREATE SCHEMA ro; SET search_path TO ro");
        for (const sql of SETUP) {
          await database.execute(sql);
          await pg.exec(sql);
        }
        let expected = "ok";
        let actual = "ok";
        try {
          for (const sql of [...prefix.statements, statement.sql]) await pg.query(sql);
        } catch (error) {
          expected = String((error as { code?: string }).code);
        } finally {
          await pg.exec("ROLLBACK");
        }
        try {
          for (const sql of [...prefix.statements, statement.sql]) await database.execute(sql);
        } catch (error) {
          actual = minnowClass(error);
        } finally {
          await database.execute("ROLLBACK").catch(() => undefined);
          await database.close();
        }
        const label = `${prefix.statements.join("; ")}; ${statement.sql}`;
        if (actual === expected) reach.reach(expected === "ok" ? "accepted" : "refused");
        else mismatches.push(`${label}\n  pglite: ${expected}\n  minnow: ${actual}`);
      }
    expect(mismatches.join("\n")).toBe("");
    reach.assertReached();
  });

  it("refuses READ WRITE after the transaction's first statement (25001), as PostgreSQL", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await database.execute("BEGIN READ ONLY");
    await database.execute("SELECT count(*) AS n FROM t");
    await expect(database.execute("SET TRANSACTION READ WRITE")).rejects.toThrow(
      "transaction read-write mode must be set before any query",
    );
    await database.execute("ROLLBACK");
    // A read-only transaction ends with it: the next one writes.
    await database.execute("BEGIN");
    await database.execute("INSERT INTO t VALUES (1)");
    await database.execute("COMMIT");
    expect((await database.query("SELECT count(*) AS n FROM t")).rows).toEqual([{ n: 1 }]);
    await database.close();
  });
});
