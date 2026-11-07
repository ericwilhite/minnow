/**
 * READ ONLY transactions, against PostgreSQL (PGlite): a write, DDL or sequence change in a
 * read-only transaction raises 25006 (read_only_sql_transaction), and READ WRITE cannot be set
 * once the transaction has run a statement (25001). Every way to make a transaction read-only
 * (or not) × every kind of statement runs in both; the outcomes must match.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import {
  createReach,
  REFUSAL_STATES,
  ROW_LOCK_TARGET_STATES,
  ROW_LOCK_VIEW_STATES,
} from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";
import { compileQuery, compileStatement } from "./query.js";

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
  // currval reads the session's last nextval: defined before any transaction.
  "SELECT nextval('s') AS n",
  // Views that lock, at one and two levels, and one that does not (review B6 stream A 3, 3).
  "CREATE VIEW vl AS SELECT * FROM t FOR UPDATE",
  "CREATE VIEW vs AS SELECT * FROM t FOR SHARE",
  "CREATE VIEW vp AS SELECT * FROM t",
  "CREATE VIEW vv AS SELECT * FROM vl",
  "CREATE VIEW vc AS SELECT 1 AS n",
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
  if (message.includes("cannot set transaction read-write mode inside a read-only transaction"))
    return "25001";
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

/** Every public way to run a read inside an open transaction (review B6 stream A 2, finding 1). */
const ENTRY_POINTS: ReadonlyArray<
  readonly [name: string, run: (database: MinnowDatabase, sql: string) => Promise<unknown>]
> = [
  ["execute", (database, sql) => database.execute(sql)],
  ["query", (database, sql) => database.query(sql)],
  ["run", (database, sql) => database.run({ kind: "typed-query", plan: compileQuery(sql) })],
  [
    "queryCursor",
    async (database, sql) => {
      for await (const batch of database.queryCursor(sql)) void batch;
    },
  ],
];

/** Reads a read-only transaction refuses, or runs (review B6 stream A 2, findings 1 and 3). */
const LOCK_TARGETS = [
  ["unused CTE", "WITH x AS (SELECT * FROM t FOR UPDATE) SELECT 1 AS n"],
  ["unused CTE", "WITH x AS (SELECT * FROM vl) SELECT 1 AS n"],
  ["constant derived", "SELECT * FROM (SELECT 1 AS n) s FOR UPDATE"],
  ["CTE", "WITH x AS (SELECT * FROM t) SELECT * FROM x FOR UPDATE"],
  ["table derived", "SELECT * FROM (SELECT * FROM t) s FOR UPDATE"],
  ["table derived", "SELECT * FROM (SELECT * FROM t) s FOR UPDATE OF s"],
  ["OF constant", "SELECT t.id FROM t, (SELECT 1 AS n) s FOR UPDATE OF s"],
  ["OF table", "SELECT t.id FROM t, (SELECT 1 AS n) s FOR UPDATE OF t"],
  ["series", "SELECT * FROM generate_series(1, 2) s FOR UPDATE"],
  ["empty table", "SELECT * FROM t WHERE false FOR UPDATE"],
] as const;
const READS = [
  "SELECT count(*) AS n FROM t",
  "SELECT nextval('s') AS n",
  // Review B6 stream A 3, finding 2: only an executed nextval writes.
  "SELECT currval('s') AS n",
  "SELECT CASE WHEN false THEN nextval('s') END AS n",
  "SELECT COALESCE(1, nextval('s')) AS n",
  "SELECT nextval('s') AS n WHERE false",
  "SELECT nextval('s') AS n LIMIT 0",
  "SELECT CASE WHEN true THEN nextval('s') END AS n",
  "SELECT * FROM t FOR UPDATE",
  // A lock written in a view counts, at any depth, wherever the view is read.
  "SELECT * FROM vl",
  "SELECT * FROM vv",
  "SELECT * FROM vs",
  "SELECT * FROM vp",
  "SELECT * FROM vp FOR UPDATE",
  "SELECT t.id FROM t JOIN vs ON vs.id = t.id",
  "SELECT * FROM t FOR NO KEY UPDATE",
  "SELECT * FROM t FOR SHARE",
  "SELECT * FROM t FOR KEY SHARE",
  "SELECT * FROM t FOR SHARE FOR UPDATE",
  "SELECT * FROM t FOR KEY SHARE FOR NO KEY UPDATE",
  "SELECT * FROM t FOR UPDATE OF t NOWAIT",
  "SELECT * FROM t FOR UPDATE SKIP LOCKED",
  "SELECT * FROM (SELECT * FROM t FOR UPDATE) s",
  "SELECT * FROM t WHERE id IN (SELECT id FROM u FOR SHARE)",
  "WITH c AS (SELECT * FROM t FOR UPDATE) SELECT * FROM c",
  "SELECT 1 AS one FOR UPDATE",
  ...LOCK_TARGETS.map(([, sql]) => sql),
];

/**
 * Statement sequences where a read is the transaction's first statement (finding 1), and that
 * move the access mode around savepoints and COMMIT (finding 2).
 */
const SEQUENCES: ReadonlyArray<readonly string[]> = [
  [
    "BEGIN READ WRITE",
    "SELECT 1 AS one",
    "SET TRANSACTION READ WRITE",
    "INSERT INTO t VALUES (9, 9)",
  ],
  ["BEGIN READ ONLY", "SELECT count(*) AS n FROM t", "SET TRANSACTION READ WRITE"],
  ["BEGIN READ ONLY", "SET TRANSACTION READ WRITE", "INSERT INTO t VALUES (9, 9)"],
  // A released savepoint's mode ends with it, back to the mode its parent level had.
  [
    "BEGIN",
    "SAVEPOINT x",
    "SET TRANSACTION READ ONLY",
    "SAVEPOINT y",
    "RELEASE y",
    "INSERT INTO t VALUES (9, 9)",
  ],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SAVEPOINT y",
    "SET TRANSACTION READ ONLY",
    "RELEASE y",
    "INSERT INTO t VALUES (9, 9)",
  ],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SET TRANSACTION READ ONLY",
    "ROLLBACK TO x",
    "INSERT INTO t VALUES (9, 9)",
  ],
  ["BEGIN", "SAVEPOINT x", "SET TRANSACTION READ ONLY", "RELEASE x", "INSERT INTO t VALUES (9, 9)"],
  ["BEGIN READ ONLY", "SAVEPOINT x", "SET TRANSACTION READ WRITE"],
  [
    "BEGIN READ ONLY",
    "SAVEPOINT x",
    "RELEASE x",
    "SET TRANSACTION READ WRITE",
    "INSERT INTO t VALUES (9, 9)",
  ],
  ["BEGIN READ ONLY", "SAVEPOINT x", "ROLLBACK TO x", "SET TRANSACTION READ WRITE"],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SET TRANSACTION READ ONLY",
    "SAVEPOINT y",
    "ROLLBACK TO y",
    "INSERT INTO t VALUES (9, 9)",
  ],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SAVEPOINT y",
    "SET TRANSACTION READ ONLY",
    "ROLLBACK TO x",
    "INSERT INTO t VALUES (9, 9)",
  ],
  ["BEGIN", "SAVEPOINT x", "SET TRANSACTION READ ONLY", "SET TRANSACTION READ WRITE"],
  ["BEGIN", "SET TRANSACTION READ ONLY", "SAVEPOINT x", "SET TRANSACTION READ WRITE"],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SELECT 1 AS one",
    "SET TRANSACTION READ ONLY",
    "ROLLBACK TO x",
    "SET TRANSACTION READ WRITE",
    "INSERT INTO t VALUES (9, 9)",
  ],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SET TRANSACTION READ ONLY",
    "ROLLBACK TO x",
    "SAVEPOINT y",
    "SET TRANSACTION READ ONLY",
    "RELEASE y",
    "INSERT INTO t VALUES (9, 9)",
  ],
  // COMMIT ends the transaction and its mode: the next one starts read-write.
  ["BEGIN READ ONLY", "SAVEPOINT x", "COMMIT", "BEGIN", "INSERT INTO t VALUES (9, 9)"],
  [
    "BEGIN",
    "SAVEPOINT x",
    "SET TRANSACTION READ ONLY",
    "COMMIT",
    "BEGIN",
    "INSERT INTO t VALUES (9, 9)",
  ],
];

/**
 * PostgreSQL's outcome of `statements` in a fresh schema: ok, or the first SQLSTATE, with the
 * message for 25006. Minnow's declared diagnostic names locking reads SELECT, independently of
 * the strength PostgreSQL names; only the PG SELECT tag is mapped to that contract. Minnow's
 * messages are never normalized, and writes/nextval keep their exact PG comparison.
 */
async function pgRun(statements: readonly string[]): Promise<string> {
  await pg.exec("DROP SCHEMA IF EXISTS ro CASCADE; CREATE SCHEMA ro; SET search_path TO ro");
  for (const sql of SETUP) await pg.exec(sql);
  try {
    for (const sql of statements) await pg.query(sql);
    return "ok";
  } catch (error) {
    const { code, message } = error as { code?: string; message?: string };
    const diagnostic = String(message).replace(
      /^cannot execute SELECT FOR (?:KEY SHARE|SHARE|NO KEY UPDATE|UPDATE) in a read-only transaction$/u,
      "cannot execute SELECT in a read-only transaction",
    );
    return code === "25006" ? `25006 ${diagnostic}` : String(code);
  } finally {
    await pg.exec("ROLLBACK");
  }
}

/** The same through Minnow, each SELECT through `entry` and every other statement execute(). */
async function minnowRun(
  statements: readonly string[],
  entry: (database: MinnowDatabase, sql: string) => Promise<unknown>,
  compiledControl = false,
): Promise<string> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  for (const sql of SETUP) await database.execute(sql);
  try {
    for (const sql of statements)
      if (sql.startsWith("SELECT") || sql.startsWith("WITH")) await entry(database, sql);
      else if (
        compiledControl &&
        (sql.startsWith("BEGIN") ||
          sql.startsWith("START") ||
          sql.startsWith("SET ") ||
          sql.startsWith("SAVEPOINT") ||
          sql.startsWith("RELEASE") ||
          sql.startsWith("ROLLBACK") ||
          sql.startsWith("COMMIT"))
      )
        await database.runStatement(compileStatement(sql));
      else await database.execute(sql);
    return "ok";
  } catch (error) {
    const state = minnowClass(error);
    return state === "25006" ? `25006 ${(error as Error).message}` : state;
  } finally {
    await database.execute("ROLLBACK").catch(() => undefined);
    await database.close();
  }
}

describe("READ ONLY across entry points, row locks and savepoints (review B6 stream A 2)", () => {
  it("refuses sequence changes and row locks through every read entry point, as PostgreSQL", async () => {
    const mismatches: string[] = [];
    const reach = createReach<string>("read-only reads", REFUSAL_STATES);
    const targets = createReach<string>("row lock targets", ROW_LOCK_TARGET_STATES);
    targets.beginHistory();
    for (const begin of ["BEGIN READ ONLY", "BEGIN", "BEGIN READ WRITE"])
      for (const read of READS) {
        const expected = await pgRun([begin, read]);
        for (const [name, entry] of ENTRY_POINTS) {
          reach.beginHistory();
          const actual = await minnowRun([begin, read], entry);
          if (actual === expected) {
            reach.reach(expected === "ok" ? "accepted" : "refused");
            for (const [shape, sql] of LOCK_TARGETS) if (sql === read) targets.reach(shape);
          } else
            mismatches.push(
              `${name}: ${begin}; ${read}\n  pglite: ${expected}\n  minnow: ${actual}`,
            );
        }
      }
    expect(mismatches.join("\n")).toBe("");
    reach.assertReached();
    targets.assertReached();
  });

  it("treats a constant view as a relation target in READ ONLY", async () => {
    const reach = createReach<string>("constant view lock target", ROW_LOCK_VIEW_STATES);
    for (const begin of ["BEGIN READ ONLY", "BEGIN", "BEGIN READ WRITE"])
      for (const clause of ["UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"])
        for (const [, entry] of ENTRY_POINTS) {
          reach.beginHistory();
          const sql = `SELECT * FROM vc FOR ${clause}`;
          const expected = await pgRun([begin, sql]);
          const actual = await minnowRun([begin, sql], entry);
          // PG and Minnow both use generic SELECT for a constant view. The diagnostic campaign
          // separately requires that wording for every locking read, including expanded views.
          expect(expected === "ok" ? "ok" : expected.split(" ")[0], sql).toBe(
            begin === "BEGIN READ ONLY" ? "25006" : "ok",
          );
          expect(actual === "ok" ? "ok" : actual.split(" ")[0], sql).toBe(
            begin === "BEGIN READ ONLY" ? "25006" : "ok",
          );
          reach.reach(begin === "BEGIN READ ONLY" ? "read only refused" : "read write allowed");
        }
    reach.assertReached();
  });

  it("preserves compiled transaction modes and the first-statement rule", async () => {
    const mismatches: string[] = [];
    const reach = createReach<string>("compiled transaction modes", REFUSAL_STATES);
    reach.beginHistory();
    const sequences = [
      ...PREFIXES.map(({ statements }) => [...statements, "INSERT INTO t VALUES (9, 9)"]),
      ...SEQUENCES,
    ];
    for (const statements of sequences) {
      const expected = await pgRun(statements);
      const actual = await minnowRun(statements, (db, sql) => db.query(sql), true);
      if (actual === expected) reach.reach(expected === "ok" ? "accepted" : "refused");
      else mismatches.push(`${statements.join("; ")}: ${expected} / ${actual}`);
    }
    expect(mismatches.join("\n")).toBe("");
    reach.assertReached();
  });

  it("scopes the access mode to savepoints and counts a read from any entry point as the first statement", async () => {
    const mismatches: string[] = [];
    const reach = createReach<string>("first statement", REFUSAL_STATES);
    for (const statements of SEQUENCES) {
      const expected = await pgRun(statements);
      for (const [name, entry] of ENTRY_POINTS) {
        reach.beginHistory();
        const actual = await minnowRun(statements, entry);
        if (actual === expected) reach.reach(expected === "ok" ? "accepted" : "refused");
        else
          mismatches.push(
            `${name}: ${statements.join("; ")}\n  pglite: ${expected}\n  minnow: ${actual}`,
          );
      }
    }
    expect(mismatches.join("\n")).toBe("");
    reach.assertReached();
  });

  it("keeps table-method writes out of a SQL-begun transaction", async () => {
    // insert() does not join the open BEGIN: it waits for the transaction to end and commits on
    // its own, so a read-only transaction neither refuses it nor rolls it back.
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    await database.execute("BEGIN READ ONLY");
    let settled = false;
    const inserted = database.insert("t", { id: 1, v: 10 }).finally(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(settled).toBe(false);
    await database.execute("ROLLBACK");
    await inserted;
    expect((await database.query("SELECT count(*) AS n FROM t")).rows).toEqual([{ n: 1 }]);
    await database.close();
  });
});

describe("READ ONLY binds live subscriptions (review B6 stream A 3, finding 1)", () => {
  /** A live subscription's outcome and the durable sequence position after it. */
  async function liveCase(
    begin: string,
    read: string,
    typed: boolean,
    phase: "init" | "refresh",
  ): Promise<{ outcome: string; before: number; after: number }> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    for (const sql of SETUP) await database.execute(sql);
    const live = database.liveQueries();
    const input = typed ? { kind: "typed-query" as const, plan: compileQuery(read) } : read;
    let errors = 0;
    let deliveries = 0;
    const options = {
      onChange: () => {
        deliveries += 1;
      },
      onError: () => {
        errors += 1;
      },
    };
    const position = async () =>
      Number(
        (await database.query("SELECT currval('s') AS n").catch(() => ({ rows: [{ n: 0 }] })))
          .rows[0]?.n ?? 0,
      );
    try {
      if (phase === "refresh") {
        await live.subscribe(input, options).catch(() => {
          errors += 1;
        });
        errors = 0;
        deliveries = 0;
      }
      const before = await position();
      await database.execute(begin);
      if (phase === "init")
        await live.subscribe(input, options).catch(() => {
          errors += 1;
        });
      else
        await live.refresh().catch(() => {
          errors += 1;
        });
      await database.execute("ROLLBACK");
      live.close();
      // The durable position: the next nextval outside any transaction is one past it.
      const next = Number((await database.query("SELECT nextval('s') AS n")).rows[0]?.n);
      return {
        outcome: errors > 0 ? "refused" : deliveries > 0 ? "ran" : "silent",
        before,
        after: next - 1,
      };
    } finally {
      await database.close();
    }
  }

  it("refuses nextval and row locks in live runs inside READ ONLY, and moves no sequence", async () => {
    const mismatches: string[] = [];
    const reach = createReach<string>("read-only live runs", REFUSAL_STATES);
    for (const begin of ["BEGIN READ ONLY", "BEGIN"])
      for (const read of [
        "SELECT nextval('s') AS n",
        "SELECT * FROM t FOR UPDATE",
        "SELECT * FROM vl",
        "SELECT count(*) AS n FROM t",
      ])
        for (const typed of [false, true])
          for (const phase of ["init", "refresh"] as const) {
            reach.beginHistory();
            const { outcome, before, after } = await liveCase(begin, read, typed, phase);
            const refuses = begin === "BEGIN READ ONLY" && !read.startsWith("SELECT count");
            const label = `${typed ? "typed" : "sql"} ${phase}: ${begin}; ${read}`;
            // PostgreSQL: a refused read leaves the sequence where it was.
            // A refresh with nothing invalidated runs nothing ("silent"); that refuses nothing
            // and moves nothing, which is all read-only asks of it.
            const refused = outcome === "refused" || (phase === "refresh" && outcome === "silent");
            if (refuses && (!refused || after !== before))
              mismatches.push(
                `${label}: ${outcome}, sequence ${String(before)} → ${String(after)}`,
              );
            else if (!refuses && outcome === "refused") mismatches.push(`${label}: refused`);
            else reach.reach(refuses ? "refused" : "accepted");
          }
    expect(mismatches.join("\n")).toBe("");
    reach.assertReached();
  });
});
