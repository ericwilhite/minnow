/**
 * A statement takes its transaction's snapshot exactly when PostgreSQL's would, except where
 * PostgreSQL rejects the text before taking one (divergence `syntax-error-snapshot`).
 *
 * The law next/ follows (lead ruling 2026-11-23, after Neon's review of #59):
 * - A statement that lowers: its planned kind decides (owner/compose.ts `#guard`).
 * - A statement that fails to lower: next/'s parser reads a subset of PostgreSQL's grammar, so its
 *   failure says nothing about PostgreSQL's raw parse. It counts as raw-parsed, and takes the
 *   snapshot unless its first word starts a statement that takes none (lower/snapshot-rule.ts):
 *   whitespace, leading semicolons and both comment forms (a `--` comment ends at `\n` or `\r`)
 *   skipped first.
 * - So a raw syntax error, or several statements in one request, takes the snapshot in next/ and
 *   none in PostgreSQL, unless its first word takes none.
 *
 * Law: PostgreSQL 18 takes the transaction snapshot once a statement's raw parse succeeds:
 * `analyze_requires_snapshot` for queries and data statements, before analysis; utility
 * statements when they run, unless `PlannedStmtRequiresSnapshot` (utility.c) refuses them
 * (transaction control, LOCK, SET, RESET, SHOW, SET CONSTRAINTS, FETCH, MOVE, LISTEN, NOTIFY,
 * UNLISTEN, CHECKPOINT). Observed in PGlite and in next/ the same way: after `BEGIN ISOLATION
 * LEVEL REPEATABLE READ READ ONLY; SAVEPOINT s; <statement>; ROLLBACK TO s; RELEASE s`, changing
 * the isolation level is refused with 25001 exactly when the statement took the snapshot.
 * `pg.query` is PostgreSQL's extended protocol, as next/'s requests are: one statement each.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { createReach, SNAPSHOT_STATEMENT_KIND_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";

const SETUP = [
  "CREATE TABLE t (id integer PRIMARY KEY, v integer)",
  "CREATE TABLE u (id integer PRIMARY KEY, v integer)",
  "INSERT INTO t VALUES (1, 1)",
];

/**
 * Statements by kind; each kind's snapshot is PostgreSQL's (probed), and next/'s follows it
 * except for `syntax` and `several`, the divergence.
 */
const STATEMENTS = {
  // PostgreSQL's raw parse fails; the first word takes a snapshot: next/ takes one.
  syntax: [
    "SELEC 1",
    "SELECT FROM WHERE",
    "SELECT (1",
    "SELECT 1 +",
    "SELECT 'abc",
    "INSERT INTO t VALUES (1, 2",
    "SELECT 1 LIMIT",
    "DELETE t",
    "SELECT 1 1",
    "UPDATE t SET v + 1",
    "SELECT * FROM t JOIN u",
    "SELECT 1 WINDOW w",
    "SELECT CASE END",
    "CREATE VIEW v AS",
    "CREATE VIEW v",
    "SELECT count(DISTINCT) FROM t",
    // Keywords fold ASCII letters only: `ſelect` is no SELECT, and no statement in PostgreSQL.
    "ſelect 1",
  ],
  // Several statements: PostgreSQL's extended protocol refuses them before any snapshot (42601,
  // "cannot insert multiple commands into a prepared statement"); next/ refuses them too, and
  // the first word decides.
  several: ["SELECT 1; SELECT 2", "SELECT 1; SELEC 2", "VALUES (1); SET search_path = public"],
  // PostgreSQL's raw parse fails, and the first word takes none: neither takes one.
  "control syntax": ["SHOW", "SET", "LOCK", "SAVEPOINT", "SET search_path = public; SELECT 1"],
  // Parsed, then refused by PostgreSQL's parse analysis (or run): both take one.
  analysis: [
    "SELECT *",
    "SELECT missing_column FROM t",
    "SELECT * FROM missing_table",
    "SELECT no_such_function(1)",
    "SELECT CASE WHEN v = 1 THEN 1 ELSE true END FROM t",
    "SELECT v FROM t GROUP BY id",
    "INSERT INTO missing_table VALUES (1)",
    "CREATE VIEW v (a) AS SELECT 1",
    "SELECT round(1, 2, 3)",
    "CALL no_such()",
    "DROP TABLE missing_table",
  ],
  // Parsed and run by PostgreSQL; refused by next/ after it read them, or before (it does not
  // read them): both take one. Neon's review of #59 lists the second group: valid PostgreSQL
  // that #59's token rules claimed PostgreSQL rejects.
  valid: [
    "SELECT id FROM t WHERE id IN (SELECT t.id FROM u)",
    "SELECT EXISTS (VALUES (1))",
    "SELECT CAST(1 AS text(1.5))",
    "SELECT (ARRAY[1, 2, 3])[1:2]",
    "SELECT substr(string := 'abc', start := 2)",
    "SELECT 1 AS café",
    "SELECT 1 AS a$b",
    "SELECT 0x7fffffffffffffff",
    "SELECT 1_000.",
    "SELECT (1 -- a comment\r)",
    "CREATE TABLE trigger (id integer)",
    "SELECT v FROM t ORDER BY v USING ~<~",
    "SELECT 1 case",
    "SELECT (SELECT 1 distinct)",
    "CREATE TABLE z (a integer UNIQUE NULLS NOT DISTINCT)",
    "WITH int (a) AS (SELECT 1) TABLE int",
    "WITH c AS (SELECT 1 FROM t AS rows WHERE true) TABLE c",
    "CREATE TABLE coalesce ()",
    "VALUES (***1)",
    "EXPLAIN SELECT 1",
    "DECLARE c CURSOR FOR SELECT 1",
    "DISCARD ALL",
    "DEALLOCATE ALL",
    "CLOSE ALL",
    "VACUUM",
    "PREPARE p AS SELECT 1",
    // TRANSACTION is an unreserved keyword: a prepared statement may be named so (gram.y).
    "PREPARE transaction AS SELECT 1",
    "PREPARE transaction (integer) AS SELECT $1",
    "/* c */ PREPARE /* c */ transaction AS SELECT 1",
    "DO $$ BEGIN END $$",
  ],
  // The statements PostgreSQL takes no snapshot for, run or failing: neither takes one.
  "no snapshot": [
    "SHOW search_path",
    "SET search_path = public",
    "RESET search_path",
    "SET CONSTRAINTS ALL DEFERRED",
    "LOCK TABLE t IN ACCESS SHARE MODE",
    "LOCK TABLE missing_table",
    "LISTEN channel",
    "UNLISTEN channel",
    "UNLISTEN *",
    "NOTIFY channel",
    "NOTIFY channel, 'payload'",
    "FETCH 1 FROM missing_cursor",
    "MOVE 1 FROM missing_cursor",
    "CHECKPOINT",
    "COMMIT PREPARED 'x'",
    "ROLLBACK PREPARED 'x'",
  ],
  // No statement at all: an empty query takes none.
  empty: ["", ";", ";;", "-- only a comment"],
} as const;
type Kind = keyof typeof STATEMENTS;

/** Kinds where PostgreSQL takes the snapshot. */
const POSTGRES_TAKES: ReadonlySet<Kind> = new Set<Kind>(["analysis", "valid"]);
/** Kinds where next/ takes it although PostgreSQL does not (divergence syntax-error-snapshot). */
const DIVERGES: ReadonlySet<Kind> = new Set<Kind>(["syntax", "several"]);

/** Text before the statement that PostgreSQL's scanner skips. */
const PREFIXES = [
  "",
  "-- a comment\n",
  "-- a comment\r",
  "/* a comment */ ",
  "/* outer /* nested */ still a comment */ ",
  "\n\t/* x */ -- y\n  ",
  "; ",
  ";; /* x */ ; ",
] as const;

/** One session: runs a statement, resolving with the SQLSTATE it failed with or "00000". */
type Run = (sql: string) => Promise<string>;

/** Whether `statement` took the transaction's snapshot: the 25001 probe. */
async function tookSnapshot(
  run: Run,
  statement: string,
): Promise<{ took: boolean; state: string }> {
  expect(await run("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")).toBe("00000");
  expect(await run("SAVEPOINT s")).toBe("00000");
  const state = await run(statement);
  expect(await run("ROLLBACK TO s")).toBe("00000");
  expect(await run("RELEASE s")).toBe("00000");
  const changed = await run("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
  expect(["00000", "25001"], `${statement}: the isolation change`).toContain(changed);
  expect(await run("ROLLBACK")).toBe("00000");
  return { took: changed === "25001", state };
}

let pg: PGlite;
let postgres: Run;
let next: Run;
let close: () => Promise<void>;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  postgres = async (sql) => {
    try {
      await pg.query(sql);
      return "00000";
    } catch (error) {
      return (error as { code?: string }).code ?? String(error);
    }
  };
  const core = await new StorageCore().open();
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "snapshot-kind", epoch: 1, build: "test" });
  const { port1, port2 } = new MessageChannel();
  server.accept(port1);
  const client = new ProtocolClient();
  await client.connect(port2);
  next = async (sql) => {
    try {
      await client.query(sql);
      return "00000";
    } catch (error) {
      return (error as { sqlState?: string }).sqlState ?? String(error);
    }
  };
  for (const sql of SETUP) {
    expect(await postgres(sql)).toBe("00000");
    expect(await next(sql)).toBe("00000");
  }
  close = async () => {
    client.close();
    port1.close();
    port2.close();
    const closing = server.quiesce("closing");
    await closing.drained;
    closing.finish();
    await engine.close();
    await core.close();
  };
}, 60_000);

afterAll(async () => {
  await close();
  await pg.close();
});

async function compare(
  kind: Kind,
  statement: string,
  reach?: ReturnType<typeof createReach>,
): Promise<void> {
  const authority = await tookSnapshot(postgres, statement);
  const ours = await tookSnapshot(next, statement);
  const context = `${kind}: ${JSON.stringify(statement)} (PostgreSQL ${authority.state}, next/ ${ours.state})`;
  // The kind is PostgreSQL's, probed: a kind that is wrong here is a wrong test, not a pass.
  expect(authority.took, `${context}: PostgreSQL's snapshot`).toBe(POSTGRES_TAKES.has(kind));
  expect(ours.took, `${context}: the snapshot it took`).toBe(DIVERGES.has(kind) || authority.took);
  reach?.reach(kind);
}

for (const kind of Object.keys(STATEMENTS) as Kind[])
  it(`takes the snapshot as PostgreSQL does: ${kind}`, async () => {
    for (const statement of STATEMENTS[kind]) await compare(kind, statement);
  });

it("takes no snapshot for PREPARE TRANSACTION (PostgreSQL's ends the transaction)", async () => {
  // PostgreSQL ends the transaction (55000, then 25P01), so its probe cannot see this one:
  // `PlannedStmtRequiresSnapshot` refuses TransactionStmt (utility.c) is the authority.
  expect((await tookSnapshot(next, "PREPARE TRANSACTION 'x'")).took).toBe(false);
  expect((await tookSnapshot(next, "PREPARE p AS SELECT 1")).took).toBe(true);
});

for (const seed of seedsFor("next-snapshot-statement-kind", [0x6601, 0x6602]))
  it(`a statement takes the snapshot as PostgreSQL's does, behind any prefix (seed ${String(seed)})`, async () => {
    const reach = createReach("snapshot statement kind", SNAPSHOT_STATEMENT_KIND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc
          .constantFrom(...(Object.keys(STATEMENTS) as Kind[]))
          .chain((kind) => fc.tuple(fc.constant(kind), fc.constantFrom(...STATEMENTS[kind]))),
        fc.constantFrom(...PREFIXES),
        async ([kind, statement], prefix) => {
          reach.beginHistory();
          if (prefix.includes("\r")) reach.reach("behind a carriage-return comment");
          else if (prefix.includes(";")) reach.reach("behind semicolons");
          else if (prefix !== "") reach.reach("behind a comment");
          await compare(kind, `${prefix}${statement}`, reach);
        },
      ),
      { seed, numRuns: 120 },
    );
    reach.assertReached();
  }, 300_000);
