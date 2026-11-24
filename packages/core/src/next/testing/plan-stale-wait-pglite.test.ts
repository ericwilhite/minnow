/**
 * Writes that wait for the writer turn while another connection's transaction changes the
 * schema of the table they write answer as PostgreSQL answers them: generated histories, each
 * compared with PGlite.
 *
 * A history: connection A runs BEGIN and one schema change of t (or none), then one to three
 * waiters, each on its own connection, send a write of t (INSERT, INSERT … SELECT, UPDATE,
 * DELETE, INSERT … ON CONFLICT), prepared or not, with literals or `$n` parameters, with no
 * RETURNING, a computed one or `RETURNING *`. Some are cancelled while they wait. Then A commits
 * or rolls back, and every waiter's answer (its columns and rows, or its SQLSTATE) and the final
 * rows of t and audit are compared with PostgreSQL's.
 *
 * Law: PostgreSQL analyses a write after locking its target, and each change here takes a lock
 * that conflicts with the write's (ALTER TABLE, DROP TABLE and DROP TRIGGER: ACCESS EXCLUSIVE;
 * CREATE TRIGGER: SHARE ROW EXCLUSIVE; CREATE INDEX: SHARE). So each waiter runs after A's
 * transaction ends, analysed on the catalog it left: the same answer as the waiter sent after A
 * ended, which is what PGlite (one connection) runs. A prepared statement is analysed again on
 * that catalog with the parameter types it was prepared with, and is refused 0A000 ("cached plan
 * must not change result type") while its result columns differ (plancache.c
 * RevalidateCachedQuery). The waiters write disjoint rows, so their order among themselves
 * changes nothing (PostgreSQL grants their non-conflicting locks together).
 *
 * Outside what one PGlite connection can run, and pinned in plan-stale-wait.test.ts instead: a
 * write naming a table A created and has not committed is refused 42P01 at once (it is not
 * found, so there is no lock to wait on), and a cancelled waiter is answered 57014 (here it is
 * only checked to be, and is left out of PostgreSQL's history).
 *
 * Shapes the generator leaves out, each a limit next/ has with no one waiting:
 * - RETURNING beside a trigger (native modification RETURNING is refused 0A000);
 * - a secondary index beside a trigger (native trigger indexed-domain preflight);
 * - a non-key value as a parameter of an unprepared write: the wire value's type is the
 *   client's, and a re-plan may infer another (PostgreSQL's text-format parameters do not care);
 * - an integer literal written to a text column, and `text = integer` comparisons (separate
 *   defects, found here: 0A000 and 42804 where PostgreSQL serves and answers 42883);
 * - a prepared INSERT … ON CONFLICT (k) with no unique index on k (a separate defect, found here:
 *   next/ refuses it 42P10 at prepare; PostgreSQL raises that in its planner, so its PREPARE
 *   succeeds and EXECUTE answers whatever analysis finds first, such as 42703).
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { Governor } from "../memory/governor.js";
import { ProtocolClient } from "../owner/client.js";
import { CancelledError } from "../owner/errors.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";
import { createReach, PLAN_STALE_WAIT_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";

type Reach = ReturnType<typeof createReach<(typeof PLAN_STALE_WAIT_REACH)[number]>>;

const BASE = "CREATE TABLE t (id integer PRIMARY KEY, k integer, v text)";
const ROWS = "INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'a'), (3, 3, 'a'), (4, 4, 'a')";
const MINNOW_TRIGGER =
  "CREATE TRIGGER t_bump AFTER INSERT ON t FOR EACH ROW BEGIN UPDATE audit SET n = n + 1 WHERE id = 1; END";
const POSTGRES_TRIGGER =
  "CREATE TRIGGER t_bump AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION bump()";

interface Change {
  readonly minnow: readonly string[];
  readonly postgres: readonly string[];
  /** t has the trigger before A's transaction. */
  readonly trigger?: true;
}
const same = (...sql: string[]): Change => ({ minnow: sql, postgres: sql });

/** A's schema change of t, inside its transaction while the waiters wait. */
const CHANGES: Record<string, Change> = {
  none: same(),
  "add column": same("ALTER TABLE t ADD COLUMN c integer"),
  "rename column": same("ALTER TABLE t RENAME COLUMN v TO c"),
  "drop column": same("ALTER TABLE t DROP COLUMN v"),
  "recreate identical": same("DROP TABLE t", BASE, ROWS),
  // Nullability is no part of a result type: a prepared statement survives it.
  "recreate k not null": same(
    "DROP TABLE t",
    "CREATE TABLE t (id integer PRIMARY KEY, k integer NOT NULL, v text)",
    ROWS,
  ),
  "recreate v integer": same(
    "DROP TABLE t",
    "CREATE TABLE t (id integer PRIMARY KEY, k integer, v integer)",
    "INSERT INTO t VALUES (1, 1, 0), (2, 2, 0), (3, 3, 0), (4, 4, 0)",
  ),
  "unique index": same("CREATE UNIQUE INDEX t_k ON t (k)"),
  "other table": same("CREATE TABLE u (id integer PRIMARY KEY)"),
  "add trigger": { minnow: [MINNOW_TRIGGER], postgres: [POSTGRES_TRIGGER] },
  "drop trigger": {
    minnow: ["DROP TRIGGER t_bump"],
    postgres: ["DROP TRIGGER t_bump ON t"],
    trigger: true,
  },
};
const TRIGGERED = new Set(["add trigger", "drop trigger"]);

interface Waiter {
  readonly verb: "insert" | "insert select" | "update" | "delete" | "upsert";
  /** The non-key column it names: t's own v, or c (added or renamed by some changes). */
  readonly column: "v" | "c";
  readonly parameters: "none" | "key" | "key and value";
  readonly returning: "none" | "computed" | "star";
  readonly prepared: boolean;
  readonly cancel: boolean;
}

interface History {
  readonly change: string;
  readonly end: "COMMIT" | "ROLLBACK";
  readonly waiters: readonly Waiter[];
}

const waiter = (change: string): fc.Arbitrary<Waiter> =>
  fc
    .record({
      verb: fc.constantFrom<Waiter["verb"]>(
        "insert",
        "insert select",
        "update",
        "delete",
        "upsert",
      ),
      column: fc.constantFrom<Waiter["column"]>("v", "c"),
      parameters: fc.constantFrom<Waiter["parameters"]>("none", "key", "key and value"),
      returning: fc.constantFrom<Waiter["returning"]>("none", "computed", "star"),
      prepared: fc.boolean(),
      cancel: fc.constantFrom(false, false, false, true),
    })
    .map((w): Waiter => ({
      ...w,
      // Header: a non-key parameter only where its type is fixed (prepared), no prepared
      // upsert, no RETURNING beside a trigger.
      parameters: w.parameters === "key and value" && !w.prepared ? "key" : w.parameters,
      prepared: w.prepared && w.verb !== "upsert",
      returning: TRIGGERED.has(change) ? "none" : w.returning,
    }));

const history: fc.Arbitrary<History> = fc.constantFrom(...Object.keys(CHANGES)).chain((change) =>
  fc.record({
    change: fc.constant(change),
    end: fc.constantFrom<History["end"]>("COMMIT", "COMMIT", "ROLLBACK"),
    waiters: fc.array(waiter(change), { minLength: 1, maxLength: 3 }),
  }),
);

/** Waiter `i`'s SQL and parameters. Waiters write disjoint rows (header). */
function render(w: Waiter, i: number): { sql: string; params: Array<number | string> } {
  const params: Array<number | string> = [];
  const bind = (value: number | string, literal: string): string => {
    if (w.parameters === "none" || (typeof value === "string" && w.parameters !== "key and value"))
      return literal;
    params.push(value);
    return `$${String(params.length)}`;
  };
  const key = (n: number) => bind(n, String(n));
  const value = () => bind("9", "'9'");
  const c = w.column;
  const returning =
    w.returning === "none"
      ? ""
      : w.returning === "computed"
        ? " RETURNING id * 2 AS d"
        : " RETURNING *";
  const body = (() => {
    switch (w.verb) {
      case "insert":
        return `INSERT INTO t (id, k, ${c}) VALUES (${key(100 + i)}, '${String(50 + i)}', ${value()})`;
      case "insert select":
        return `INSERT INTO t (id, k, ${c}) SELECT id + 200, k + 200, ${c} FROM t WHERE id = ${key(i + 1)}`;
      case "update":
        return `UPDATE t SET ${c} = ${value()} WHERE id = ${key(i + 1)}`;
      case "delete":
        return `DELETE FROM t WHERE id = ${key(i + 1)} AND ${c} IS NOT NULL`;
      case "upsert":
        return `INSERT INTO t (id, k, ${c}) VALUES (${key(100 + i)}, '${String(i + 1)}', ${value()}) ON CONFLICT (k) DO NOTHING`;
    }
  })();
  return { sql: body + returning, params };
}

/** An answer as compared: the result's column names and rows, or the SQLSTATE. */
type Answer = string;
const served = (columns: readonly string[], rows: unknown) =>
  `ok ${JSON.stringify(columns)} ${JSON.stringify(rows)}`;

let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec(
    "CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE audit SET n = n + 1 WHERE id = 1; RETURN NULL; END $$",
  );
});
afterAll(async () => {
  await pg.close();
});

async function postgres(h: History): Promise<{
  arrival: Answer[];
  answers: Array<Answer | null>;
  state: string;
}> {
  await pg.exec("DROP TABLE IF EXISTS t, u, audit CASCADE; DEALLOCATE ALL");
  const change = CHANGES[h.change];
  if (change === undefined) throw new Error(h.change);
  const code = (error: unknown) => String((error as { code?: unknown }).code);
  const answer = async (sql: string, params: readonly unknown[] = []) =>
    pg.query<unknown[]>(sql, params as unknown[], { rowMode: "array" }).then(
      (result) =>
        served(
          result.fields.map((field) => field.name),
          result.rows,
        ),
      code,
    );
  await pg.exec(`${BASE}; ${ROWS}; CREATE TABLE audit (id integer PRIMARY KEY, n integer)`);
  await pg.exec("INSERT INTO audit VALUES (1, 0)");
  if (change.trigger === true) await pg.exec(POSTGRES_TRIGGER);
  const rendered = h.waiters.map((w, i) => render(w, i));
  // What each would answer on arrival (before A): tells "planned after the turn" for reach.
  const arrival: Answer[] = [];
  for (const { sql, params } of rendered) {
    await pg.exec("BEGIN");
    arrival.push(await answer(sql, params));
    await pg.exec("ROLLBACK");
  }
  const prepared: Array<Answer | null> = [];
  for (const [i, w] of h.waiters.entries())
    prepared.push(
      w.prepared
        ? await pg.exec(`PREPARE w${String(i)} AS ${rendered[i]?.sql ?? ""}`).then(() => null, code)
        : null,
    );
  await pg.exec("BEGIN");
  for (const sql of change.postgres) await pg.exec(sql);
  await pg.exec(h.end);
  const answers: Array<Answer | null> = [];
  for (const [i, w] of h.waiters.entries()) {
    const { sql, params } = rendered[i] ?? { sql: "", params: [] };
    if (prepared[i] !== null && prepared[i] !== undefined) answers.push(prepared[i]);
    else if (w.cancel) answers.push(null);
    else if (w.prepared) {
      const args = params.map((p) => (typeof p === "number" ? String(p) : `'${p}'`)).join(", ");
      answers.push(await answer(`EXECUTE w${String(i)}${params.length === 0 ? "" : `(${args})`}`));
    } else answers.push(await answer(sql, params));
  }
  const state = [await answer("SELECT * FROM t ORDER BY id"), await answer("SELECT n FROM audit")];
  return { arrival, answers, state: state.join("\n") };
}

const minnowState = (error: unknown) =>
  String((error as { sqlState?: unknown }).sqlState ?? (error as { name?: unknown }).name);

async function minnow(h: History): Promise<{ answers: Array<Answer | null>; state: string }> {
  const change = CHANGES[h.change];
  if (change === undefined) throw new Error(h.change);
  Governor.stallAfterMs = 5_000;
  const harness = new Harness("strict", "memory");
  await harness.open();
  const engine = composeEngine({
    planner: createLowerer(),
    writePath: harness.path,
    governor: harness.governor,
    spill: new MemorySpillService(),
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "plan-stale-gen", epoch: 1, build: "test" });
  const clients: ProtocolClient[] = [];
  const ports: MessagePort[] = [];
  for (let k = 0; k <= h.waiters.length; k++) {
    const { port1, port2 } = new MessageChannel();
    ports.push(port1, port2);
    server.accept(port1);
    const client = new ProtocolClient();
    await client.connect(port2);
    clients.push(client);
  }
  const clientAt = (k: number): ProtocolClient => {
    const client = clients[k];
    if (client === undefined) throw new Error(`no connection ${String(k)}`);
    return client;
  };
  const a = clientAt(0);
  const answer = (result: { columns: ReadonlyArray<{ name: string }>; rows(): unknown[][] }) =>
    served(
      result.columns.map((column) => column.name),
      result.rows(),
    );
  try {
    for (const sql of [BASE, ROWS, "CREATE TABLE audit (id integer PRIMARY KEY, n integer)"])
      await a.query(sql);
    await a.query("INSERT INTO audit VALUES (1, 0)");
    if (change.trigger === true) await a.query(MINNOW_TRIGGER);
    const rendered = h.waiters.map((w, i) => render(w, i));
    const prepared = await Promise.all(
      h.waiters.map(async (w, i) =>
        w.prepared
          ? clientAt(i + 1)
              .prepare(rendered[i]?.sql ?? "")
              .then((info) => ({ id: info.id }), minnowState)
          : null,
      ),
    );
    await a.query("BEGIN");
    for (const sql of change.minnow) await a.query(sql);
    const aborts = h.waiters.map(() => new AbortController());
    let settledEarly = 0;
    const answers = h.waiters.map((_, i): Promise<Answer> | Answer => {
      const handle = prepared[i];
      if (typeof handle === "string") return handle;
      const client = clientAt(i + 1);
      const { sql, params } = rendered[i] ?? { sql: "", params: [] };
      const signal = aborts[i]?.signal;
      const sent =
        handle === null || handle === undefined
          ? client.query(sql, params, signal === undefined ? {} : { signal })
          : client.execute(handle.id, params, 0, -1, signal === undefined ? {} : { signal });
      return sent.then(answer, minnowState).finally(() => {
        settledEarly++;
      });
    });
    // Observed, not timed: every sent waiter waits for the writer turn (queued for it, or
    // waiting for A to end before it plans again) or has been answered.
    const sent = answers.filter((x) => typeof x !== "string").length;
    const deadline = Date.now() + 5_000;
    while (settledEarly + harness.path.waitingWriters < sent) {
      if (Date.now() > deadline) throw new Error("the waiters never queued behind A");
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
    // None is answered while A's transaction holds the turn: each waits to be analysed on the
    // catalog A leaves (as PostgreSQL's waits for its lock).
    expect(settledEarly, "waiters answered while A's transaction was open").toBe(0);
    for (const [i, w] of h.waiters.entries()) {
      const pending = answers[i];
      if (!w.cancel || typeof pending === "string" || pending === undefined) continue;
      // The client answers a sent request's cancellation with the signal's reason once the
      // owner has answered it cancelled (client.ts `#fail`).
      aborts[i]?.abort(new CancelledError());
      expect(await pending, `waiter ${String(i)} cancelled while it waited`).toBe("57014");
    }
    await a.query(h.end);
    const settled: Array<Answer | null> = [];
    for (const [i, w] of h.waiters.entries()) {
      const pending = answers[i];
      settled.push(w.cancel && typeof pending !== "string" ? null : ((await pending) ?? null));
    }
    const state = [
      answer(await a.query("SELECT * FROM t ORDER BY id")),
      answer(await a.query("SELECT n FROM audit")),
    ];
    return { answers: settled, state: state.join("\n") };
  } finally {
    Governor.stallAfterMs = null;
    for (const client of clients) client.close();
    const stop = server.quiesce("closing");
    await stop.drained;
    stop.finish();
    for (const port of ports) port.close();
    await engine.close();
    await harness.close();
  }
}

function noteReach(h: History, expected: Awaited<ReturnType<typeof postgres>>, reach: Reach): void {
  const committed = h.end === "COMMIT";
  if (!committed) reach.reach("rolled back");
  if (h.waiters.length === 3) reach.reach("three waiters");
  if (committed) {
    if (h.change.startsWith("recreate")) reach.reach("table recreated");
    if (h.change === "rename column") reach.reach("column renamed");
    if (h.change === "drop column") reach.reach("column dropped");
    if (h.change === "add trigger") reach.reach("trigger added");
    if (h.change === "drop trigger") reach.reach("trigger dropped");
  }
  for (const [i, w] of h.waiters.entries()) {
    const answer = expected.answers[i];
    if (w.cancel && answer === null) reach.reach("cancelled while waiting");
    if (answer === null || answer === undefined) continue;
    const ok = answer.startsWith("ok ");
    if (ok) reach.reach("served");
    reach.reach(w.prepared ? "prepared" : "unprepared");
    if (w.parameters !== "none") reach.reach("parameters");
    if (w.returning === "computed") reach.reach("returning computed");
    if (w.returning === "star") reach.reach("returning star");
    const arrival = expected.arrival[i] ?? "";
    if (!w.prepared && (arrival === "42703" || arrival === "42P10"))
      reach.reach(ok ? "planned after the turn, served" : "planned after the turn, refused");
    if (!w.prepared && arrival.startsWith("ok ") && answer === "42703")
      reach.reach("re-planned, refused");
    if (w.prepared && answer === "0A000") reach.reach("prepared result shape refused");
    if (w.prepared && w.parameters === "key and value" && answer === "42804")
      reach.reach("prepared parameter type kept");
    if (
      !w.prepared &&
      ok &&
      w.returning === "star" &&
      arrival.startsWith("ok ") &&
      arrival.split(" ")[1] !== answer.split(" ")[1]
    )
      reach.reach("result shape changed, unprepared");
  }
}

/** Cases the review named, as the generator writes them. */
const PINNED: readonly History[] = [
  {
    change: "add column",
    end: "COMMIT",
    waiters: [
      {
        verb: "insert",
        column: "v",
        parameters: "key",
        returning: "star",
        prepared: true,
        cancel: false,
      },
      {
        verb: "insert",
        column: "c",
        parameters: "key",
        returning: "star",
        prepared: false,
        cancel: false,
      },
      {
        verb: "upsert",
        column: "v",
        parameters: "none",
        returning: "star",
        prepared: false,
        cancel: true,
      },
    ],
  },
  {
    change: "recreate v integer",
    end: "COMMIT",
    waiters: [
      {
        verb: "update",
        column: "v",
        parameters: "key and value",
        returning: "none",
        prepared: true,
        cancel: false,
      },
    ],
  },
  {
    change: "unique index",
    end: "ROLLBACK",
    waiters: [
      {
        verb: "upsert",
        column: "v",
        parameters: "key",
        returning: "computed",
        prepared: false,
        cancel: false,
      },
    ],
  },
  {
    // Unprepared, it is served with the columns of the plan it runs (found by the generator:
    // the rows had four values and the client was told three columns).
    change: "add column",
    end: "COMMIT",
    waiters: [
      {
        verb: "insert",
        column: "v",
        parameters: "none",
        returning: "star",
        prepared: false,
        cancel: false,
      },
    ],
  },
];

for (const seed of seedsFor("next-plan-stale-wait", [0x69a1, 0x69a2]))
  it(`writes waiting behind A's schema change answer as PostgreSQL's (seed ${String(seed)})`, async () => {
    const reach = createReach("plan-stale wait", PLAN_STALE_WAIT_REACH);
    await fc.assert(
      fc.asyncProperty(history, async (h) => {
        reach.beginHistory();
        const expected = await postgres(h);
        const actual = await minnow(h);
        const context = JSON.stringify(h);
        expect(actual.answers, `${context}: the waiters' answers`).toEqual(expected.answers);
        expect(actual.state, `${context}: t and audit`).toBe(expected.state);
        noteReach(h, expected, reach);
      }),
      { seed, numRuns: 60, examples: PINNED.map((h) => [h]) },
    );
    reach.assertReached();
  }, 900_000);
