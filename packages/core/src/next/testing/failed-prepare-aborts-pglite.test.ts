/**
 * A prepare (PostgreSQL's extended-protocol Parse) that fails inside a transaction aborts it, as a
 * failed statement does: later statements and prepares are refused 25P02 until ROLLBACK (or
 * ROLLBACK TO a savepoint, which recovers it).
 *
 * Found while fixing #59 (2026-11-23), registered as the divergence
 * `failed-prepare-keeps-transaction`: next/'s failed prepare request answered the error and left
 * the transaction open, so the next statement ran. A prepare inside an already aborted
 * transaction was planned and answered as usual.
 *
 * Law: PostgreSQL 18 (exec_parse_message), probed here in PGlite with a Parse message alone:
 * - outside a transaction, a failed Parse changes nothing;
 * - inside one (read-write, read-only, after a savepoint), a failed Parse, a syntax error or an
 *   analysis error alike, aborts it: the next statement and the next Parse are 25P02;
 * - inside an aborted transaction a Parse is treated as a statement is: a raw syntax error is
 *   still 42601, COMMIT, ROLLBACK and ROLLBACK TO are accepted, anything else is 25P02;
 * - ROLLBACK TO a savepoint taken before the failure recovers the transaction.
 * next/ cannot tell a raw syntax error from an analysis error, and counts a text that fails to
 * lower as raw-parsed (lead ruling for #59, divergence `syntax-error-snapshot`): inside an
 * aborted transaction it refuses either 25P02 unless the first word ends the transaction, where
 * PostgreSQL answers a raw syntax error 42601. That one difference is applied to PostgreSQL's
 * answers below (`withRuling`), stated, not hidden.
 */
import { PGlite, protocol } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";
import { createReach, FAILED_PREPARE_REACH } from "../../testing/reach.js";

type Step = { readonly prepare: string } | { readonly run: string };

const STATES: Record<string, readonly string[]> = {
  none: [],
  open: ["BEGIN"],
  savepoint: ["BEGIN", "SAVEPOINT s"],
  "read-only": ["BEGIN READ ONLY"],
};
const PREPARES = ["SELEC 1", "SELECT missing FROM t", "SELECT 1"];
const FOLLOW: ReadonlyArray<readonly Step[]> = [
  [{ run: "SELECT 1" }],
  [{ prepare: "SELECT 2" }],
  [{ prepare: "SELECT missing FROM t" }],
  [{ prepare: "SELEC 2" }],
  [{ run: "SELECT missing FROM t" }],
  [{ run: "SELEC 2" }],
  [{ prepare: "ROLLBACK" }, { run: "SELECT 1" }],
  [{ run: "ROLLBACK TO s" }, { run: "SELECT 1" }, { prepare: "SELECT 3" }],
  [{ run: "SAVEPOINT x" }],
  [{ run: "COMMIT" }, { run: "SELECT 1" }],
  [{ run: "ROLLBACK" }, { run: "SELECT 1" }],
];

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("CREATE TABLE t (id integer PRIMARY KEY)");
});
afterAll(async () => {
  await pg.close();
});

const state = (error: unknown) =>
  (error as { sqlState?: string; code?: string }).sqlState ??
  (error as { code?: string }).code ??
  String(error);

async function postgres(steps: readonly Step[]): Promise<string[]> {
  const out: string[] = [];
  for (const step of steps) {
    if ("run" in step) {
      out.push(await pg.query(step.run).then(() => "00000", state));
      continue;
    }
    const result = await pg.execProtocol(
      new Uint8Array([
        ...protocol.serialize.parse({ text: step.prepare }),
        ...protocol.serialize.sync(),
      ]),
      { throwOnError: false },
    );
    const error = result.messages.find((message) => message.name === "error") as
      { code?: string } | undefined;
    out.push(error === undefined ? "00000" : String(error.code));
  }
  await pg.exec("ROLLBACK").catch(() => undefined);
  return out;
}

async function minnow(steps: readonly Step[]): Promise<string[]> {
  const h = new Harness("strict", "memory");
  await h.open();
  const engine = composeEngine({
    planner: createLowerer(),
    writePath: h.path,
    governor: h.governor,
    spill: new MemorySpillService(),
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "failed-prepare", epoch: 1, build: "test" });
  const { port1, port2 } = new MessageChannel();
  server.accept(port1);
  const client = new ProtocolClient();
  await client.connect(port2);
  const out: string[] = [];
  try {
    await client.query("CREATE TABLE t (id integer PRIMARY KEY)");
    for (const step of steps)
      out.push(
        await (
          "run" in step ? client.query(step.run) : client.prepareTask(step.prepare, 0).promise
        ).then(() => "00000", state),
      );
  } finally {
    client.close();
    const stop = server.quiesce("closing");
    await stop.drained;
    stop.finish();
    port1.close();
    port2.close();
    await engine.close();
    await h.close();
  }
  return out;
}

/** PostgreSQL's answers with the ruling applied: a raw syntax error (a misspelt first keyword,
 * `SELEC`) inside an aborted transaction is 25P02 in next/. */
function withRuling(steps: readonly Step[], answers: readonly string[]): string[] {
  let inTransaction = false,
    aborted = false;
  return steps.map((step, k) => {
    const text = "run" in step ? step.run : step.prepare;
    let answer = answers[k] ?? "";
    if (aborted && answer === "42601" && text.startsWith("SELEC ")) answer = "25P02";
    if ("run" in step && text.startsWith("BEGIN")) inTransaction = true;
    else if ("run" in step && (text === "COMMIT" || text === "ROLLBACK")) {
      inTransaction = false;
      aborted = false;
    } else if ("run" in step && text === "ROLLBACK TO s" && answer === "00000") aborted = false;
    else if (inTransaction && answer !== "00000") aborted = true;
    return answer;
  });
}

for (const [name, begin] of Object.entries(STATES))
  it(`a failed prepare in a transaction (${name}) aborts it as PostgreSQL's Parse does`, async () => {
    // ROLLBACK TO recovers only where a savepoint was taken.
    const reach = createReach(
      `failed prepare (${name})`,
      name === "savepoint"
        ? FAILED_PREPARE_REACH
        : FAILED_PREPARE_REACH.filter((state) => state !== "savepoint recovered"),
    );
    for (const prepare of PREPARES)
      for (const follow of FOLLOW) {
        if (
          name !== "savepoint" &&
          follow.some((step) => "run" in step && step.run === "ROLLBACK TO s")
        )
          continue;
        reach.beginHistory();
        const steps: Step[] = [...begin.map((sql) => ({ run: sql })), { prepare }, ...follow];
        const expected = withRuling(steps, await postgres(steps));
        const actual = await minnow(steps);
        expect(actual, JSON.stringify(steps)).toEqual(expected);
        if (expected.includes("25P02")) reach.reach("aborted");
        if (expected[begin.length] === "00000") reach.reach("prepared");
        else reach.reach("prepare failed");
        if (follow.some((step) => "run" in step && step.run === "ROLLBACK TO s"))
          reach.reach("savepoint recovered");
      }
    if (name === "none") return;
    reach.assertReached();
  }, 120_000);
