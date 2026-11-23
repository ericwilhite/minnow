/**
 * The front end claims a grammar failure only where PostgreSQL's raw parser rejects the text, over
 * a seeded corpus of the repository's SQL and mutations of it (grammar-corpus.ts).
 *
 * Why: a failed statement takes its transaction's snapshot exactly when PostgreSQL's would
 * (next/ `Lowerer.takesSnapshot`): PostgreSQL takes it for every statement its raw parser
 * (gram.y, scan.l) accepts. next/'s parser reads a subset of PostgreSQL's grammar, so failing to
 * read on proves nothing: an unmarked failure counts as parsed, and `rawParseRejects`
 * (lower/raw-parse.ts) claims a rejection only where gram.y has no alternative. Astra's re-checks of
 * #59 found such claims on valid PostgreSQL (`EXISTS (VALUES (1))`, `CAST(1 AS text(1.5))`,
 * `CREATE TRIGGER ... EXECUTE FUNCTION f()`); a hand audit of the claims did not converge, so the
 * corpus is the gate:
 * - A: PostgreSQL parses, next/ claims a rejection. Never: such a statement would skip the
 *   snapshot PostgreSQL takes.
 * - B: PostgreSQL rejects, next/ counts the statement as read (its parser refused something
 *   earlier, or reads an extension). The documented divergence `late-syntax-error-snapshot`
 *   (testing/divergences.ts): such a statement takes the snapshot PostgreSQL would not. Its share
 *   of the statements PostgreSQL's raw parser rejects stays at or under B_CEILING, which may only
 *   go down.
 *
 * Law: PostgreSQL 18 (PGlite). Its raw-parse verdict: `SELECT 1/0;\n<statement>` fails with 22012
 * (division by zero) exactly when the whole text raw-parsed, since exec_simple_query raw-parses
 * the whole string before running its first statement; a raw parse failure is any other error.
 * The first test calibrates that verdict against the snapshot probe itself.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { rawParseRejects } from "../lower/raw-parse.js";
import {
  generateInteractionPlan,
  runInteractionPlan,
  type SimulatedConnection,
  type SimulationDriver,
} from "../../testing/interaction-simulator.js";
import { createReach, GRAMMAR_VERDICT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { createLowerer } from "../lower/create.js";
import { ctasDataClause } from "../lower/ctas.js";
import { parseAny } from "../lower/frontend.js";
import { LATE_SYNTAX_ERROR_SNAPSHOT } from "./divergences.js";
import { drawCorpus, repositorySources, type CorpusSource } from "./grammar-corpus.js";
import { createNextDriver } from "./simulator-driver.js";

/**
 * The highest share, of the statements PostgreSQL's raw parser rejects, that next/ reads (B),
 * measured over the default seeds (LATE_SYNTAX_ERROR_SNAPSHOT). Lower it as the front end claims
 * more rejections; never raise it.
 */
const B_CEILING = LATE_SYNTAX_ERROR_SNAPSHOT.ceiling;
/** Mutated or original statements drawn per seed, besides every non-SQLLogicTest original. */
const DRAWS = 6000;

let pg: PGlite;
let statements = 0;

/**
 * PGlite 0.5.5 faults after about 1,470 raw parse errors in one instance ("function signature
 * mismatch", then 54001 on every statement): a fresh instance every 300 statements, and one
 * retry on a fresh instance for a verdict that is not a SQLSTATE of the statement.
 */
async function fresh(): Promise<void> {
  await pg.close().catch(() => undefined);
  pg = new PGlite();
  await pg.waitReady;
}

/** PostgreSQL's raw-parse verdict on `sql`, with its error when it rejects. */
async function postgresParses(sql: string): Promise<{ parses: boolean; error: string }> {
  if (++statements % 300 === 0) await fresh();
  for (let attempt = 0; ; attempt++) {
    try {
      await pg.exec(`SELECT 1/0;\n${sql}`);
      throw new Error(`ran past division by zero: ${sql}`);
    } catch (error) {
      const e = error as { code?: string; message?: string };
      if ((e.code === undefined || e.code === "54001") && attempt === 0) {
        await fresh();
        continue;
      }
      if (e.code === undefined) throw error;
      return { parses: e.code === "22012", error: `${e.code} ${e.message ?? ""}` };
    }
  }
}

const lowerer = createLowerer();

/** next/'s verdict, as `takesSnapshot` reaches it: whether it counts the text as parsed. */
function nextReads(sql: string): { reads: boolean; error: string } {
  const ctas = ctasDataClause(sql);
  try {
    parseAny(ctas === null ? sql : sql.slice(0, ctas.index));
    return { reads: true, error: "" };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const reads = !rawParseRejects(sql);
    // takesSnapshot decides from the same verdict: a claimed rejection takes no snapshot.
    if (!reads) expect(lowerer.takesSnapshot(sql), `${sql}: takes no snapshot`).toBe(false);
    return { reads, error: message };
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
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

it("reads PostgreSQL's raw-parse verdict as the snapshot probe does", async () => {
  // The verdict is PostgreSQL's whether a statement took the snapshot (snapshot-statement-kind):
  // a statement that raw-parses takes it, one that does not takes none.
  const probe = async (sql: string): Promise<boolean> => {
    await pg.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SAVEPOINT s");
    await pg.query(sql).catch(() => undefined);
    await pg.exec("ROLLBACK TO s; RELEASE s");
    const changed = await pg.exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").then(
      () => "00000",
      (error: unknown) => (error as { code?: string }).code,
    );
    await pg.exec("ROLLBACK");
    return changed === "25001";
  };
  for (const sql of [
    "SELECT 1",
    "SELECT *",
    "SELECT EXISTS (VALUES (1))",
    "SELECT CAST(1 AS text(1.5))",
    "SELECT no_such_function(1)",
    "SELEC 1",
    "SELECT (1",
    "SELECT 1 +",
    "CREATE VIEW v",
    "SELECT count(DISTINCT)",
  ]) {
    const { parses } = await postgresParses(sql);
    expect(await probe(sql), sql).toBe(parses);
  }
});

const rates: number[] = [];

for (const seed of seedsFor("next-grammar-verdict", [0x6701, 0x6702]))
  it(`claims a grammar failure only where PostgreSQL rejects the text (seed ${String(seed)})`, async () => {
    const reach = createReach("grammar verdict", GRAMMAR_VERDICT_REACH);
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
    const claimedOnValid: string[] = [];
    let readPastError = 0;
    let rejected = 0;
    // Of B, the statements next/ runs: an extension of its grammar (`?` placeholders, MATCH ...
    // AGAINST), not a late refusal.
    let extensions = 0;
    for (const entry of corpus) {
      if (seen.has(entry.sql)) continue;
      seen.add(entry.sql);
      reach.beginHistory();
      reach.reach(entry.mutation ?? "original");
      if (entry.source === "interaction simulator") reach.reach("simulator statement");
      const postgres = await postgresParses(entry.sql);
      const next = nextReads(entry.sql);
      reach.reach(postgres.parses ? "PostgreSQL parses" : "PostgreSQL rejects");
      if (!postgres.parses) rejected++;
      if (!next.reads) reach.reach("claimed rejection");
      if (postgres.parses && !next.reads)
        claimedOnValid.push(`${JSON.stringify(entry.sql)}: ${next.error}`);
      if (!postgres.parses && next.reads) {
        readPastError++;
        if (next.error === "") extensions++;
        reach.reach("read past a syntax error");
      }
    }
    const rate = readPastError / rejected;
    rates.push(rate);
    console.log(
      `grammar verdict, seed ${String(seed)}: ${String(seen.size)} statements, ${String(rejected)} rejected by PostgreSQL; A ${String(claimedOnValid.length)}; B ${String(readPastError)} (${rate.toFixed(4)} of the rejected; ${String(extensions)} run as extensions), ceiling ${String(B_CEILING)}`,
    );
    expect(
      claimedOnValid,
      "PostgreSQL parses these; next/ claims its grammar rejects them",
    ).toEqual([]);
    expect(rate, "share of PostgreSQL's rejections next/ reads (B)").toBeLessThanOrEqual(B_CEILING);
    reach.assertReached();
  }, 300_000);

it("keeps the B ceiling no looser than the corpus needs", () => {
  // The ceiling is a ratchet: when the front end claims more rejections, it comes down with them.
  if ((process.env.MINNOW_SEED ?? "").trim() !== "" || rates.length === 0) return;
  expect(B_CEILING - Math.max(...rates), "lower B_CEILING to the measured rate").toBeLessThan(0.01);
});
