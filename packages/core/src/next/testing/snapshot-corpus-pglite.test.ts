/**
 * Every statement of a seeded corpus takes its transaction's snapshot in next/ exactly when it does
 * in PostgreSQL, except where PostgreSQL rejects the text before taking one (divergence
 * `syntax-error-snapshot`, testing/divergences.ts).
 *
 * Why: #59 first decided a failed statement's snapshot from token rules meant to tell where
 * PostgreSQL's raw parser rejects text. Neon's review found a dozen rule groups that reject valid
 * PostgreSQL, and a corpus whose space-joined mutations could not show them. The rules are gone
 * (lead ruling 2026-11-23): a statement that lowers decides by its planned kind, and one that
 * fails to lower counts as raw-parsed and decides by its first words (lower/snapshot-rule.ts).
 * This differential compares the decision itself, through the 25001 probe, on both engines, for
 * every statement of the corpus, whatever next/ does with it.
 *
 * Law: PostgreSQL 18 (PGlite), extended protocol (`pg.query`, one statement per request, as
 * next/'s requests are). After `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SAVEPOINT s;
 * <statement>; ROLLBACK TO s; RELEASE s`, changing the isolation level is refused with 25001
 * exactly when the statement took the snapshot. Where they differ, next/ must have taken one and
 * PostgreSQL must have rejected the text before its snapshot: its raw parse fails (`SELECT 1/0;\n
 * <statement>` through the simple protocol fails with anything but 22012, since exec_simple_query
 * raw-parses the whole string first), or it holds several statements (42601, "cannot insert
 * multiple commands into a prepared statement").
 *
 * Not probed: transaction control that ends or nests the probe's own transaction (BEGIN, START,
 * COMMIT, END, ROLLBACK, ABORT, SAVEPOINT, RELEASE, PREPARE TRANSACTION); snapshot-statement-kind
 * and read-only-snapshot-timing cover them.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { leadingWords } from "../lower/snapshot-rule.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import {
  generateInteractionPlan,
  runInteractionPlan,
  type SimulatedConnection,
  type SimulationDriver,
} from "../../testing/interaction-simulator.js";
import { createReach, SNAPSHOT_CORPUS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { SYNTAX_ERROR_SNAPSHOT } from "./divergences.js";
import { drawCorpus, repositorySources, type CorpusSource } from "./grammar-corpus.js";
import { createNextDriver } from "./simulator-driver.js";
import { StorageCore } from "./storage-core.js";

/** Mutated or original statements drawn per seed, besides every non-SQLLogicTest original. */
const DRAWS = 6000;

const SETUP = [
  "CREATE TABLE t (id integer PRIMARY KEY, v integer, s text)",
  "CREATE TABLE u (id integer PRIMARY KEY, v integer, s text)",
  "INSERT INTO t VALUES (1, 1, 'a')",
];

/** First words of the statements the probe cannot run inside its own transaction. */
const PROBE_BREAKING = new Set(
  "BEGIN START COMMIT END ROLLBACK ABORT SAVEPOINT RELEASE".split(" "),
);

/** One session: runs a statement, resolving with the SQLSTATE and message it failed with. */
type Run = (sql: string) => Promise<{ state: string; message: string }>;

let pg: PGlite;
let postgres: Run;
let next: Run;
let close: () => Promise<void>;
let statements = 0;

/**
 * PGlite 0.5.5 faults after about 1,470 errors in one instance ("function signature mismatch",
 * then 54001 on every statement): a fresh instance every 100 statements (each probe can raise
 * several), and a probe PGlite faulted in runs again once on a fresh instance.
 */
async function freshPostgres(): Promise<void> {
  await pg.close().catch(() => undefined);
  pg = new PGlite();
  await pg.waitReady;
  for (const sql of SETUP) expect((await postgres(sql)).state, sql).toBe("00000");
}

/** PGlite's fault: no SQLSTATE of the statement's own (see `freshPostgres`). */
const faulted = (state: string) => state === "54001" || !/^[0-9A-Z]{5}$/u.test(state);

/** PostgreSQL's 25001 probe, on a fresh instance once if PGlite faulted in it. */
async function probePostgres(statement: string) {
  for (let attempt = 0; ; attempt++) {
    const result = await probe(postgres, statement, "PostgreSQL", attempt === 0).catch(
      (error: unknown) => {
        if (attempt === 0) return null;
        throw error;
      },
    );
    if (result !== null && !(attempt === 0 && faulted(result.state))) return result;
    await freshPostgres();
  }
}

/** Whether `statement` took the transaction's snapshot: the 25001 probe. */
async function probe(run: Run, statement: string, engine: string, retryable = false) {
  const step = async (sql: string) => {
    const { state, message } = await run(sql);
    if (retryable && state !== "00000" && faulted(state)) throw new Error(`faulted: ${message}`);
    expect(state, `${engine}: ${sql} around ${JSON.stringify(statement)}: ${message}`).toBe(
      "00000",
    );
  };
  await step("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  await step("SAVEPOINT s");
  const outcome = await run(statement);
  await step("ROLLBACK TO s");
  await step("RELEASE s");
  const changed = await run("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
  expect(["00000", "25001"], `${engine}: ${statement}: the isolation change`).toContain(
    changed.state,
  );
  await step("ROLLBACK");
  return { took: changed.state === "25001", ...outcome };
}

/** PostgreSQL's raw parse of `sql` (simple protocol): false when it rejects the text. */
async function postgresRawParses(sql: string): Promise<boolean> {
  for (let attempt = 0; ; attempt++) {
    try {
      await pg.exec(`SELECT 1/0;\n${sql}`);
      throw new Error(`ran past division by zero: ${sql}`);
    } catch (error) {
      const code = (error as { code?: string }).code;
      if ((code === undefined || code === "54001") && attempt === 0) {
        await freshPostgres();
        continue;
      }
      if (code === undefined) throw error;
      return code === "22012";
    }
  }
}

/** The statements one interaction plan over next/ sends, recorded at the driver. */
async function simulatorStatements(seed: number): Promise<CorpusSource> {
  const recorded: string[] = [];
  const driver = await createNextDriver({ transactionIdleTimeoutMs: 2000 });
  const record = (connection: SimulatedConnection): SimulatedConnection => ({
    ...connection,
    reopen: () => connection.reopen(),
    execute: (sql, params) => {
      recorded.push(sql);
      return connection.execute(sql, params);
    },
    query: (sql, params) => {
      recorded.push(sql);
      return connection.query(sql, params);
    },
  });
  const recording: SimulationDriver = {
    ...driver,
    open: async (connection) => record(await driver.open(connection)),
  };
  const plan = generateInteractionPlan(seed, {
    length: 60,
    connections: 2,
    tables: 2,
    keySpace: 16,
    constraints: true,
    schemaChanges: true,
  });
  // Only the text matters here: a plan an open finding stops still recorded what it sent.
  await runInteractionPlan(plan, recording).catch(() => undefined);
  return { name: "interaction simulator", statements: [...new Set(recorded)] };
}

beforeAll(async () => {
  postgres = async (sql) => {
    try {
      await pg.query(sql);
      return { state: "00000", message: "" };
    } catch (error) {
      const e = error as { code?: string; message?: string };
      return { state: e.code ?? String(error), message: e.message ?? "" };
    }
  };
  pg = new PGlite();
  await pg.waitReady;
  for (const sql of SETUP) expect((await postgres(sql)).state, sql).toBe("00000");
  const core = await new StorageCore().open();
  const engine = composeEngine({
    planner: core.lowerer,
    writePath: core.harness.path,
    governor: core.harness.governor,
    spill: core.spill,
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "snapshot-corpus", epoch: 1, build: "test" });
  const { port1, port2 } = new MessageChannel();
  server.accept(port1);
  const client = new ProtocolClient();
  await client.connect(port2);
  next = async (sql) => {
    try {
      await client.query(sql);
      return { state: "00000", message: "" };
    } catch (error) {
      const e = error as { sqlState?: string; message?: string };
      return { state: e.sqlState ?? String(error), message: e.message ?? "" };
    }
  };
  for (const sql of SETUP) expect((await next(sql)).state, sql).toBe("00000");
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

it("reads PostgreSQL's rejection before its snapshot as the probe does", async () => {
  // The divergence's condition, calibrated: text whose raw parse fails, or several statements,
  // take no snapshot in PostgreSQL; text that raw-parses and needs one takes it.
  for (const [sql, rawParses, took] of [
    ["SELECT 1", true, true],
    ["SELECT missing_column FROM t", true, true],
    ["SELEC 1", false, false],
    ["SELECT (1", false, false],
    ["SELECT 1; SELECT 2", true, false],
    ["SHOW search_path", true, false],
  ] as const) {
    expect(await postgresRawParses(sql), `${sql}: raw parse`).toBe(rawParses);
    expect((await probePostgres(sql)).took, `${sql}: snapshot`).toBe(took);
  }
});

for (const seed of seedsFor("next-snapshot-corpus", [0x6701, 0x6702]))
  it(`takes the snapshot as PostgreSQL does for every corpus statement (seed ${String(seed)})`, async () => {
    const reach = createReach("snapshot corpus", SNAPSHOT_CORPUS_REACH);
    const sources = [...repositorySources(), await simulatorStatements(seed)];
    const corpus = [
      // Every original but the SQLLogicTest profile's 10,000, which are drawn.
      ...sources
        .filter((source) => source.name !== "SQLLogicTest profile")
        .flatMap((source) =>
          source.statements.map((sql) => ({ sql, source: source.name, mutation: null })),
        ),
      ...drawCorpus(sources, seed, DRAWS),
    ];
    const seen = new Set<string>();
    const wrong: string[] = [];
    let probed = 0;
    let notProbed = 0;
    let rejectedBefore = 0;
    let diverged = 0;
    for (const entry of corpus) {
      if (seen.has(entry.sql)) continue;
      seen.add(entry.sql);
      reach.beginHistory();
      reach.reach(entry.mutation ?? "original");
      if (entry.source === "interaction simulator") reach.reach("simulator statement");
      const [first, second] = leadingWords(entry.sql, 2);
      if (
        (first !== undefined && PROBE_BREAKING.has(first)) ||
        (first === "PREPARE" && second === "TRANSACTION")
      ) {
        notProbed++;
        reach.reach("transaction control, not probed");
        continue;
      }
      if (++statements % 100 === 0) await freshPostgres();
      probed++;
      const theirs = await probePostgres(entry.sql);
      const ours = await probe(next, entry.sql, "next/");
      reach.reach(ours.state === "00000" ? "next/ ran it" : "next/ refused it");
      const before =
        !theirs.took &&
        (theirs.message.includes("cannot insert multiple commands") ||
          !(await postgresRawParses(entry.sql)));
      if (before) rejectedBefore++;
      if (theirs.took === ours.took) {
        reach.reach(ours.took ? "both take it" : "neither takes it");
        continue;
      }
      if (ours.took && before) {
        diverged++;
        reach.reach(
          theirs.message.includes("cannot insert multiple commands")
            ? "several statements taken"
            : "syntax error taken",
        );
        continue;
      }
      wrong.push(
        `${JSON.stringify(entry.sql)}: PostgreSQL ${theirs.took ? "took" : "took none"} (${theirs.state}), next/ ${ours.took ? "took" : "took none"} (${ours.state} ${ours.message})`,
      );
    }
    console.log(
      `snapshot corpus, seed ${String(seed)}: ${String(seen.size)} statements, ${String(probed)} probed, ${String(notProbed)} transaction control not probed; PostgreSQL rejected ${String(rejectedBefore)} before its snapshot, next/ took one for ${String(diverged)} of them (${SYNTAX_ERROR_SNAPSHOT.id}); ${String(wrong.length)} wrong`,
    );
    expect(wrong, "next/'s snapshot differs from PostgreSQL's outside the divergence").toEqual([]);
    reach.assertReached();
  }, 1_800_000);
