/**
 * Planning a statement never takes its transaction's snapshot; running it does, and so does a
 * prepare where PostgreSQL's Parse does.
 *
 * Neon's review of #59 (P2): `ComposedSession.plan` pinned the snapshot for every caller,
 * including those that never run the statement: an explain (`admin("explain")`), a request the
 * ledger did not admit (a refused retry), and a failed prepare. Each then fixed the isolation
 * level (25001) of a transaction PostgreSQL leaves untouched.
 *
 * Law: PostgreSQL 18 takes the snapshot when a statement runs (exec_simple_query, or Execute) or
 * when an extended-protocol Parse analyses it (exec_parse_message, `analyze_requires_snapshot`):
 * a query or data statement and CREATE TABLE AS, parsed or failing analysis; not a schema change,
 * not a raw syntax error. Checked here against PGlite's Parse alone (no Bind, no Execute). An
 * explain without running is not a statement PostgreSQL sees at all (EXPLAIN without ANALYZE is
 * one, and takes the snapshot; `admin("explain")` is next/'s inspection request).
 *
 * The probe: after `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`, changing the isolation
 * level is refused with 25001 exactly when something took the snapshot.
 */
import { PGlite, protocol } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { FAILED_PREPARE_KEEPS_TRANSACTION } from "./divergences.js";
import { harness, raw } from "./owner-harness.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TABLE t (id integer PRIMARY KEY, v integer)");
}, 60_000);

afterAll(async () => {
  await pg.close();
});

/** Whether PostgreSQL's Parse of `sql`, alone, takes the snapshot, and whether it failed. */
async function postgresParse(sql: string): Promise<{ took: boolean; failed: boolean }> {
  await pg.exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SAVEPOINT s");
  const bytes = new Uint8Array([
    ...protocol.serialize.parse({ text: sql }),
    ...protocol.serialize.sync(),
  ]);
  const result = await pg.execProtocol(bytes, { throwOnError: false });
  const failed = result.messages.some((message) => message.name === "error");
  await pg.exec("ROLLBACK TO s; RELEASE s");
  const changed = await pg.exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").then(
    () => "00000",
    (error: unknown) => (error as { code?: string }).code ?? String(error),
  );
  await pg.exec("ROLLBACK");
  return { took: changed === "25001", failed };
}

/** next/ over the owner harness, one raw connection, session 1 in a read-only transaction. */
async function session() {
  const { server, connect } = harness();
  const setup = await connect();
  await setup.query("CREATE TABLE t (id integer PRIMARY KEY, v integer)");
  const port = raw(server);
  let seq = 0;
  const header = () => ({
    seq: ++seq,
    sentAtMs: Date.now(),
    resolvedThrough: 0,
    live: false,
    resendable: true,
    retry: false,
    generation: 1,
  });
  /** Runs `sql` on session 1: its SQLSTATE, or "00000". */
  const run = async (sql: string): Promise<string> => {
    const message = { ...port.statement(++seq, sql), session: 1 };
    port.deliver(message);
    const reply = await port.reply(message.seq);
    if (reply.t !== "error") return "00000";
    const state = reply.error.props?.sqlState;
    return typeof state === "string" ? state : reply.error.name;
  };
  expect(await run("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")).toBe("00000");
  /** Whether the snapshot is taken: the 25001 probe, which ends the transaction. */
  const took = async (): Promise<boolean> => {
    const changed = await run("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    expect(["00000", "25001"]).toContain(changed);
    await run("ROLLBACK");
    return changed === "25001";
  };
  return { port, header, run, took };
}

it("an explain plans without taking the snapshot", async () => {
  for (const sql of ["SELECT count(*) FROM t", "SELECT missing_column FROM t"]) {
    const { port, header, took } = await session();
    const request = { ...header(), t: "admin", session: 1, op: "explain", args: [sql] };
    port.deliver(request);
    await port.reply(request.seq);
    expect(await took(), `explain ${sql}: the snapshot`).toBe(false);
  }
});

it("a request the ledger does not admit takes no snapshot", async () => {
  // A non-resendable retry this owner never saw is refused before it runs (SessionLostError).
  for (const sql of ["SELECT count(*) FROM t", "SELECT missing_column FROM t"]) {
    const { port, header, took } = await session();
    const request = {
      ...port.statement(header().seq, sql),
      session: 1,
      resendable: false,
      retry: true,
    };
    port.deliver(request);
    expect(await port.reply(request.seq)).toMatchObject({
      t: "error",
      error: { name: "SessionLostError" },
    });
    expect(await took(), `refused retry ${sql}: the snapshot`).toBe(false);
  }
});

it("a prepare takes the snapshot exactly where PostgreSQL's Parse does", async () => {
  for (const sql of [
    // Parsed and analysed: taken.
    "SELECT v FROM t WHERE id = 1",
    "INSERT INTO t VALUES (1, 1)",
    "CREATE TABLE z AS SELECT 1 AS a",
    // Analysis fails after the snapshot: taken (and the transaction aborts: 25P02 in
    // PostgreSQL; next/ keeps it open, `FAILED_PREPARE_KEEPS_TRANSACTION`).
    "SELECT missing_column FROM t",
    "SELECT * FROM missing_table",
    "UPDATE missing_table SET v = 1",
    // Not analysed at Parse: none.
    "CREATE TABLE z (a integer)",
    "DROP TABLE missing_table",
    "SET search_path = public",
    "SHOW search_path",
    // A raw syntax error: none.
    "SET",
  ]) {
    const postgres = await postgresParse(sql);
    const { port, header, took } = await session();
    const request = { ...header(), t: "prepare", session: 1, id: 7, text: sql };
    port.deliver(request);
    const reply = await port.reply(request.seq);
    // Where each fails may differ (next/ reads the catalog when it prepares a DROP; PostgreSQL
    // when it runs one): the snapshot may not.
    const context = `prepare ${sql} (PostgreSQL ${postgres.failed ? "failed" : "parsed"}, next/ ${reply.t}; ${FAILED_PREPARE_KEEPS_TRANSACTION.id} for the abort)`;
    expect(await took(), `${context}: the snapshot`).toBe(postgres.took);
  }
});

it("a statement that fails to plan takes the snapshot only when it is admitted and runs", async () => {
  // Control for the two above: the same text, admitted, takes it (an analysis failure).
  const { run, took } = await session();
  expect(await run("SAVEPOINT s")).toBe("00000");
  expect(await run("SELECT missing_column FROM t")).toBe("42703");
  expect(await run("ROLLBACK TO s")).toBe("00000");
  expect(await run("RELEASE s")).toBe("00000");
  expect(await took(), "an admitted failure: the snapshot").toBe(true);
});
