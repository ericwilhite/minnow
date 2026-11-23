/**
 * A failed statement takes its transaction's snapshot exactly when PostgreSQL's would: a statement
 * whose raw parse fails takes none; one that parsed takes one by its kind, before its analysis
 * or planning can fail.
 *
 * Astra's re-check of #59: the first `Lowerer.takesSnapshot` decided a failed statement from its
 * SQLSTATE (42601 taken for a raw parse failure) and its text (a leading-keyword pattern). Both
 * are wrong: `SELECT *` without FROM is 42601 from PostgreSQL's parse analysis, after the
 * snapshot; `/* c *\/ SELECT no_such_function(1)` starts with a comment. It now decides from the
 * front end's own boundary: a grammar failure (engine/grammar-failure.ts `grammarFailure`, marked where
 * the tokenizer or parser cannot read on) is the raw parse failing; any other refusal came after
 * the statement was read, and its kind, from its first word as the tokenizer reads it, decides.
 *
 * Law: PostgreSQL's exec_simple_query raw-parses, then takes the transaction snapshot for each
 * statement analyze_requires_snapshot accepts, before parse analysis; a utility statement takes
 * it when it runs. Observed in PGlite and in next/ the same way: after `BEGIN ISOLATION LEVEL
 * REPEATABLE READ READ ONLY; SAVEPOINT s; <statement>; ROLLBACK TO s; RELEASE s`, changing the
 * isolation level is refused with 25001 exactly when the statement took the snapshot.
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

/** Statements by how they fail; each kind's snapshot effect is PostgreSQL's (probed). */
const STATEMENTS = {
  // The raw parse fails (and next/'s front end cannot read on before any other refusal).
  parse: [
    "SELEC 1",
    "SELECT FROM WHERE",
    "SELECT (1",
    "SELECT 1 +",
    "SELECT 'abc",
    "INSERT INTO t VALUES (1, 2",
    "DELETE t",
    "SELECT 1 1",
    "SELECT v w x FROM t",
    "UPDATE SET v = 1",
    "UPDATE t SET v + 1",
    "SELECT * FROM t JOIN u",
    "SELECT 1 LIMIT",
  ],
  // Parsed, then refused by PostgreSQL's parse analysis.
  analysis: [
    "SELECT *",
    "SELECT missing_column FROM t",
    "SELECT * FROM missing_table",
    "SELECT no_such_function(1)",
    "SELECT CASE WHEN v = 1 THEN 1 ELSE true END FROM t",
    "SELECT v FROM t GROUP BY id",
    "INSERT INTO missing_table VALUES (1)",
  ],
  // Parsed and run by PostgreSQL; refused by next/ after it read them (0A000).
  planning: [
    "SELECT id FROM t WHERE id IN (SELECT t.id FROM u)",
    "SELECT id FROM t GROUP BY id HAVING max(v) > (SELECT max(v) FROM u WHERE u.id = t.id)",
  ],
} as const;
type Kind = keyof typeof STATEMENTS;

/**
 * The audited grammar-failure classes (scripts/grammar-failure-audit.test.ts), each with
 * statements PostgreSQL's raw parser rejects and, where one exists, a neighbour it reads. Not here:
 * text next/'s grammar reads but PostgreSQL's rejects (`SELECT 1e` is `1 AS e` to next/; a CAST
 * to `varchar(1, 2)`; a `?` placeholder): next/ reads them, so they take the snapshot.
 */
const AUDITED: ReadonlyArray<readonly [string, Kind, string]> = [
  ["lexical", "parse", "SELECT 'abc"],
  ["lexical", "parse", "SELECT 1 /* open"],
  ["lexical", "parse", 'SELECT "abc'],
  ["lexical", "parse", "SELECT 0x"],
  ["lexical", "parse", 'SELECT ""'],
  ["token", "parse", "SELECT 1 FROM t WHERE v IN"],
  ["token", "parse", "SELECT v FROM t ORDER"],
  ["fixed arity", "parse", "SELECT COALESCE()"],
  ["fixed arity", "parse", "SELECT NULLIF(1)"],
  ["fixed arity", "parse", "SELECT GREATEST()"],
  ["fixed arity", "analysis", "SELECT round(1, 2, 3)"],
  ["empty list", "parse", "SELECT CASE END"],
  ["empty list", "parse", "SELECT 1 FROM t GROUP BY ROLLUP()"],
  ["empty list", "parse", "WITH x() AS (SELECT 1) SELECT 1"],
  ["empty list", "parse", "INSERT INTO t VALUES (1, 1) ON CONFLICT () DO NOTHING"],
  ["empty list", "parse", "MERGE INTO t USING u ON t.id = u.id"],
  ["missing body", "parse", "CREATE VIEW v AS"],
  ["missing body", "parse", "SELECT 1 WINDOW w"],
  ["missing body", "parse", "CREATE TABLE z (a integer DEFAULT)"],
  ["type modifier", "parse", "CREATE TABLE z (a varchar(1, 2))"],
  ["type modifier", "parse", "SELECT CAST(1 AS varchar(1.5))"],
  ["bare wildcard", "parse", "VALUES (*)"],
  ["bare wildcard", "parse", "SELECT 1 IN (*)"],
  ["bare wildcard", "parse", "SELECT count(DISTINCT *) FROM t"],
  ["bare wildcard", "analysis", "SELECT count(DISTINCT v, id) FROM t"],
  ["frame bounds", "parse", "SELECT sum(v) OVER (ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM t"],
  ["frame bounds", "analysis", "SELECT sum(v) OVER (GROUPS 1 PRECEDING) FROM t"],
  ["statement shape", "parse", "SHOW"],
  ["statement shape", "parse", "CREATE TYPE"],
  ["statement shape", "parse", "CREATE TYPE e AS ENUM (1)"],
  ["statement shape", "parse", "CREATE SEQUENCE"],
  ["placeholders", "parse", "SELECT 1, $1, ?"],
  ["operator character", "analysis", "SELECT 1 ` 2"],
];

const PREFIXES = [
  "",
  "-- a comment\n",
  "/* a comment */ ",
  "/* outer /* nested */ still a comment */ ",
  "\n\t/* x */ -- y\n  ",
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
  // The kind is PostgreSQL's: a raw parse failure takes none; a parsed statement takes one.
  expect(authority.took, `${context}: PostgreSQL's snapshot`).toBe(kind !== "parse");
  expect(ours.took, `${context}: the snapshot it took`).toBe(authority.took);
  reach?.reach(`${kind} error`);
}

it("takes no snapshot for the grammar failures Astra's audit request named", async () => {
  // Re-check of #59 at e827b24c5: unmarked grammar checks took a snapshot.
  for (const prefix of PREFIXES) {
    await compare("parse", `${prefix}SELECT 1 WINDOW w`);
    await compare("parse", `${prefix}SELECT CASE END`);
    await compare("parse", `${prefix}CREATE VIEW v AS`);
  }
});

it("takes the snapshot for Astra's two statements, with and without comments", async () => {
  // Review of #59: `SELECT *` is 42601 from analysis; a leading comment must not hide the kind.
  for (const prefix of PREFIXES) {
    await compare("analysis", `${prefix}SELECT *`);
    await compare("analysis", `${prefix}SELECT no_such_function(1)`);
    await compare("parse", `${prefix}SELEC 1`);
  }
});

for (const seed of seedsFor("next-snapshot-statement-kind", [0x6601, 0x6602]))
  it(`a failed statement takes the snapshot as PostgreSQL's does (seed ${String(seed)})`, async () => {
    const reach = createReach("snapshot statement kind", SNAPSHOT_STATEMENT_KIND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc
            .constantFrom<Kind>("parse", "analysis", "planning")
            .chain((kind) =>
              fc.tuple(
                fc.constant("listed"),
                fc.constant(kind),
                fc.constantFrom(...STATEMENTS[kind]),
              ),
            ),
          fc.constantFrom(...AUDITED),
        ),
        fc.constantFrom(...PREFIXES),
        async ([group, kind, statement], prefix) => {
          reach.beginHistory();
          if (prefix !== "") reach.reach("behind a comment");
          if (group !== "listed") reach.reach("an audited class");
          await compare(kind, `${prefix}${statement}`, reach);
        },
      ),
      { seed, numRuns: 80 },
    );
    reach.assertReached();
  }, 300_000);
