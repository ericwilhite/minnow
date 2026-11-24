/**
 * An autocommit write that waits for the writer turn while another connection's transaction
 * changes the catalog runs against the catalog it executes on.
 *
 * Found 2026-11-23 by #68's generator, pure SQL on main 0911b2f0. Connection A: `BEGIN; CREATE
 * INDEX t_k ON t (k)`. Connection B: `INSERT INTO t …`, which waits for the writer turn A's
 * transaction holds. A: `COMMIT`. B was answered `PlanStaleError` 0A000 ("The plan was lowered at
 * catalog version 2, the snapshot is at 4"): it was planned when it arrived and executed after the
 * DDL committed. The same for UPDATE, DELETE, INSERT … SELECT and RETURNING, after any committed
 * DDL (DROP INDEX, ADD COLUMN, even CREATE TABLE of another table); after DROP TABLE it was an
 * untyped `TypeError: Missing reachable table` (a DDL statement: `CatalogChangeError`).
 *
 * Law: exec/errors.ts's contract for `PlanStaleError` ("Not a user error: the owner re-lowers
 * the statement and executes it again (nothing ran)"), and PostgreSQL, which plans a statement
 * after taking its locks: B runs once A's transaction ends, against A's committed catalog. A new
 * index holds B's row (a unique one refuses a duplicate of it), a new trigger fires for it, and
 * after DROP TABLE B is refused 42P01 (relation does not exist). An unprepared B learns its
 * columns with its result (the simple protocol): it is served with the new ones. A prepared B
 * keeps the parameter types it was prepared with and is refused while its result columns differ,
 * on every execute (plancache.c: 0A000, "cached plan must not change result type"). A B that
 * names a column or a conflict target A is adding waits for A and is analysed on its catalog,
 * as PostgreSQL's B waits for its lock on t; a table A created is not found (42P01) at once.
 *
 * Generated histories against PGlite: plan-stale-wait-pglite.test.ts.
 */
import { afterEach, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { Governor } from "../memory/governor.js";
import { ProtocolClient, preparedShape } from "../owner/client.js";
import type { PreparedShape, WireValue } from "../owner/protocol.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";
import { CancelledError } from "../owner/errors.js";

afterEach(() => {
  Governor.stallAfterMs = null;
});

const describe = (error: unknown) =>
  error instanceof Error
    ? `${error.name} ${(error as { sqlState?: string }).sqlState ?? ""} ${error.message}`
    : String(error);

/** Resolves once `count` writers wait for the writer turn (observed, not timed). */
async function untilWaiting(h: Harness, count = 1): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (h.path.waitingWriters < count) {
    if (Date.now() > deadline) throw new Error("B never waited for the writer turn");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

async function sessions(
  body: (
    a: ProtocolClient,
    b: ProtocolClient,
    h: Harness,
    reconnect: (client: ProtocolClient) => Promise<void>,
  ) => Promise<void>,
) {
  Governor.stallAfterMs = 5_000;
  const h = new Harness("strict", "memory");
  const governor = new Governor(256 * (1 << 20));
  Object.defineProperty(h, "governor", { value: governor });
  await h.open();
  const engine = composeEngine({
    planner: createLowerer(),
    writePath: h.path,
    governor,
    spill: new MemorySpillService(),
    reads: chunkReads,
  });
  const server = new OwnerServer(engine, { ownerId: "plan-stale", epoch: 1, build: "test" });
  const clients: ProtocolClient[] = [];
  const ports: MessagePort[] = [];
  for (let k = 0; k < 2; k++) {
    const { port1, port2 } = new MessageChannel();
    ports.push(port1, port2);
    server.accept(port1);
    const client = new ProtocolClient();
    await client.connect(port2);
    clients.push(client);
  }
  const [a, b] = clients as [ProtocolClient, ProtocolClient];
  try {
    await a.query("CREATE TABLE t (id integer PRIMARY KEY, k integer, v text)");
    await a.query("CREATE INDEX t_v ON t (v)");
    await a.query("INSERT INTO t VALUES (1, 1, 'a'), (2, 2, 'a')");
    await a.query("CREATE TABLE audit (id integer PRIMARY KEY, n integer)");
    await a.query("INSERT INTO audit VALUES (1, 0)");
    await body(a, b, h, async (client) => {
      const { port1, port2 } = new MessageChannel();
      ports.push(port1, port2);
      server.accept(port1);
      await client.reconnect(port2);
    });
  } finally {
    for (const client of clients) client.close();
    const stop = server.quiesce("closing");
    await stop.drained;
    stop.finish();
    for (const port of ports) port.close();
    await engine.close();
    await h.close();
  }
}

/** A's DDL, inside its transaction while B waits. */
const DDL: Record<string, readonly string[]> = {
  "create index": ["CREATE INDEX t_k ON t (k)"],
  "create unique index": ["CREATE UNIQUE INDEX t_k ON t (k)"],
  "drop index": ["DROP INDEX t_v"],
  "add column": ["ALTER TABLE t ADD COLUMN c integer"],
  "drop table": ["DROP TABLE t"],
  "create other table": ["CREATE TABLE u (id integer PRIMARY KEY)"],
  // ON DELETE SET DEFAULT runs only on the native command path (statement-reach.ts): B must be
  // dispatched there on the catalog it executes on, not on the one it was planned on.
  "create set-default child": [
    "DROP INDEX t_v",
    "CREATE TABLE child (id integer PRIMARY KEY, tid integer DEFAULT 1 REFERENCES t (id) ON DELETE SET DEFAULT)",
    "INSERT INTO child VALUES (1, 2)",
  ],
  // Without t_v here and below: a native command on a table with a secondary index is refused
  // even alone (0A000 "native trigger indexed-domain preflight"), a separate limit.
  "create trigger": [
    "DROP INDEX t_v",
    "CREATE TRIGGER t_bump AFTER INSERT ON t FOR EACH ROW BEGIN UPDATE audit SET n = n + 1 WHERE id = 1; END",
  ],
};

interface Write {
  readonly sql: string;
  /** In B's own transaction (BEGIN waits for the turn; the statement is planned after it). */
  readonly own?: true;
  /** The k values B's statement leaves in t, and the rows it inserts. */
  readonly keys: readonly number[];
  readonly inserted: number;
  readonly returns?: readonly unknown[][];
}

/** B's statement, sent while A's transaction holds the writer turn. */
const WRITES: Record<string, Write> = {
  insert: { sql: "INSERT INTO t (id, k, v) VALUES (100, 7, 'b')", keys: [7], inserted: 1 },
  "insert select": {
    sql: "INSERT INTO t (id, k, v) SELECT id + 200, k + 20, v FROM t WHERE id <= 2",
    keys: [21, 22],
    inserted: 2,
  },
  update: { sql: "UPDATE t SET k = k + 10 WHERE id = 1", keys: [11], inserted: 0 },
  delete: { sql: "DELETE FROM t WHERE id = 2", keys: [], inserted: 0 },
  "insert returning": {
    sql: "INSERT INTO t (id, k, v) VALUES (100, 7, 'b') RETURNING id, k, v",
    keys: [7],
    inserted: 1,
    returns: [[100, 7, "b"]],
  },
  "insert in B's transaction": {
    sql: "INSERT INTO t (id, k, v) VALUES (100, 7, 'b')",
    own: true,
    keys: [7],
    inserted: 1,
  },
  "create index": { sql: "CREATE INDEX t_k2 ON t (k)", keys: [], inserted: 0 },
  "add column": { sql: "ALTER TABLE t ADD COLUMN d integer", keys: [], inserted: 0 },
  "drop table": { sql: "DROP TABLE t", keys: [], inserted: 0 },
};

type Outcome = { readonly rows: unknown } | { readonly error: string };

/** What B's statement answered and what t and audit hold afterwards. */
async function play(
  ddl: string,
  write: string,
  end: "COMMIT" | "ROLLBACK",
  waiting: boolean,
  check?: (a: ProtocolClient, outcome: Outcome) => Promise<void>,
): Promise<string> {
  const statements = DDL[ddl] ?? [];
  const w = WRITES[write];
  if (w === undefined) throw new Error(write);
  let observed = "";
  await sessions(async (a, b, h) => {
    await a.query("BEGIN");
    for (const sql of statements) await a.query(sql);
    const send = () => {
      const begin = w.own === true ? b.query("BEGIN") : null;
      const outcome = b.query(w.sql).then(
        (result): Outcome => ({ rows: result.rows() }),
        (error: unknown): Outcome => ({ error: describe(error) }),
      );
      return { begin, outcome };
    };
    // Waiting: B is sent while A's transaction holds the writer turn. Alone: after A ended.
    const sent = waiting ? send() : null;
    if (sent !== null) await untilWaiting(h);
    await a.query(end);
    const { begin, outcome } = sent ?? send();
    await begin;
    const settled = await outcome;
    if (w.own === true) await b.query("error" in settled ? "ROLLBACK" : "COMMIT");
    const state = async (sql: string) =>
      a.query(sql).then((result) => JSON.stringify(result.rows()), describe);
    observed = [
      JSON.stringify(settled),
      await state("SELECT * FROM t ORDER BY id"),
      await state("SELECT n FROM audit"),
      await state("SELECT * FROM child ORDER BY id"),
    ].join("\n");
    await check?.(a, settled);
  });
  return observed;
}

async function history(ddl: string, write: string, end: "COMMIT" | "ROLLBACK") {
  const w = WRITES[write];
  if (w === undefined) throw new Error(write);
  const context = `${ddl} | ${write} | ${end}`;
  const committed = end === "COMMIT";
  const waiting = await play(ddl, write, end, true, async (a, settled) => {
    if (committed && ddl === "drop table") {
      // PostgreSQL: the relation is gone once B holds its lock.
      expect("error" in settled ? settled.error : "served", context).toMatch(
        /^PlanningError 42P01 (relation|table) "t" does not exist/u,
      );
      return;
    }
    // B is served, but where PostgreSQL refuses it too (a table a new foreign key references
    // cannot be dropped: 2BP01) or for a limit B meets alone (the comparison below checks that).
    const refused =
      committed && ddl === "create set-default child" && write === "drop table"
        ? /^CatalogChangeError 2BP01 /u
        : committed && ddl === "create trigger" && write === "insert returning"
          ? /^UnsupportedPlanError 0A000 .*native modification RETURNING/u
          : null;
    if (refused !== null) {
      expect("error" in settled ? settled.error : "served", context).toMatch(refused);
      return;
    }
    if ("error" in settled) throw new Error(`${context}: B was refused: ${settled.error}`);
    if (write === "drop table") return;
    // ON DELETE SET DEFAULT ran: the child points at the default parent.
    if (committed && ddl === "create set-default child" && write === "delete")
      expect((await a.query("SELECT * FROM child")).rows(), context).toEqual([[1, 1]]);
    if (w.returns !== undefined) expect(settled.rows, context).toEqual(w.returns);
    // B's rows are in every index A created: a lookup by k finds them, and a unique index
    // refuses a duplicate of each.
    for (const k of w.keys) {
      const found = await a.query("SELECT count(*) FROM t WHERE k = $1", [k]);
      expect(found.rows(), `${context}: k = ${String(k)}`).toEqual([[1]]);
      if (committed && ddl === "create unique index") {
        const duplicate = await a
          .query("INSERT INTO t (id, k, v) VALUES (999, $1, 'd')", [k])
          .then(() => "inserted", describe);
        expect(duplicate, `${context}: the unique index holds k = ${String(k)}`).toMatch(
          /^UniqueViolationError 23505/u,
        );
      }
    }
    // A trigger A created fires for B's rows.
    if (committed && ddl === "create trigger")
      expect((await a.query("SELECT n FROM audit")).rows(), `${context}: trigger`).toEqual([
        [w.inserted],
      ]);
    // A column A added is in B's rows (null: B named its columns).
    if (committed && ddl === "add column" && w.inserted > 0) {
      const added = await a.query("SELECT count(*) FROM t WHERE id >= 100 AND c IS NULL");
      expect(added.rows(), `${context}: added column`).toEqual([[w.inserted]]);
    }
  });
  // Waiting for A changes nothing B would see sent after A ended: its answer and the rows.
  expect(waiting, `${context}: B waiting behind A, as B sent after A`).toBe(
    await play(ddl, write, end, false),
  );
  if (!(committed && ddl === "drop table"))
    expect(waiting.split("\n")[0], `${context}: B's answer`).not.toMatch(/PlanStale|TypeError/u);
}

for (const ddl of Object.keys(DDL))
  it(`a write waiting for the writer turn runs on the catalog after A's ${ddl}`, async () => {
    for (const write of Object.keys(WRITES))
      for (const end of ["COMMIT", "ROLLBACK"] as const) await history(ddl, write, end);
  }, 120_000);

const outcome = (pending: Promise<unknown>) => pending.then(() => "served", describe);
const settledYet = async (pending: Promise<string>) =>
  Promise.race([pending, new Promise((resolve) => setTimeout(resolve, 20, "pending"))]);

it("serves an unprepared write re-planned with other result columns with them", async () => {
  await sessions(async (a, b, h) => {
    await a.query("BEGIN");
    await a.query("ALTER TABLE t ADD COLUMN c integer");
    const result = b.query("INSERT INTO t (id, k, v) VALUES (100, 7, 'b') RETURNING *");
    await untilWaiting(h);
    await a.query("COMMIT");
    const served = await result;
    expect(served.columns.map((column) => column.name)).toEqual(["id", "k", "v", "c"]);
    expect(served.rows()).toEqual([[100, 7, "b", null]]);
  });
}, 60_000);

it("refuses a prepared write whose result columns changed on every execute, waiting or not", async () => {
  await sessions(async (a, b, h) => {
    const sql = "INSERT INTO t (id, k, v) VALUES ($1, 7, 'b') RETURNING *";
    const handle = await b.prepare(sql);
    await a.query("BEGIN");
    await a.query("ALTER TABLE t ADD COLUMN c integer");
    const queued = outcome(b.execute(handle.id, [100]));
    await untilWaiting(h);
    await a.query("COMMIT");
    const refused = /^PlanShapeChangedError 0A000 cached plan must not change result type/u;
    expect(await queued, "waiting for the turn").toMatch(refused);
    expect(await outcome(b.execute(handle.id, [101])), "planned on arrival").toMatch(refused);
    expect(await outcome(b.execute(handle.id, [102])), "again").toMatch(refused);
    expect((await a.query("SELECT count(*) FROM t WHERE id >= 100")).rows()).toEqual([[0]]);
    // Prepared again, it describes and returns the new columns.
    b.unprepare(handle.id);
    const again = await b.prepare(sql);
    expect(again.columns.map((column) => column.name)).toEqual(["id", "k", "v", "c"]);
    expect((await b.execute(again.id, [100])).rows()).toEqual([[100, 7, "b", null]]);
  });
}, 60_000);

it("re-plans a prepared write with the parameter types it was prepared with", async () => {
  await sessions(async (a, b, h) => {
    // $1 is text (v's type) when prepared; v becomes an integer. PostgreSQL keeps $1 text, and
    // text is not assignable to integer: 42804. Sent unprepared, $1 would be an integer.
    const handle = await b.prepare("UPDATE t SET v = $1 WHERE id = 1");
    expect(handle.parameters.map((type) => type.kind)).toEqual(["text"]);
    await a.query("BEGIN");
    await a.query("DROP TABLE t");
    await a.query("CREATE TABLE t (id integer PRIMARY KEY, k integer, v integer)");
    await a.query("INSERT INTO t VALUES (1, 1, 0)");
    const queued = outcome(b.execute(handle.id, ["9"]));
    await untilWaiting(h);
    await a.query("COMMIT");
    expect(await queued).toMatch(/ 42804 /u);
    expect(await outcome(b.execute(handle.id, ["9"]))).toMatch(/ 42804 /u);
    expect((await a.query("SELECT v FROM t")).rows()).toEqual([[0]]);
  });
}, 60_000);

it("serves a prepared write whose columns only changed nullability", async () => {
  await sessions(async (a, b, h) => {
    const handle = await b.prepare("INSERT INTO t (id, k, v) VALUES ($1, 7, 'b') RETURNING *");
    await a.query("BEGIN");
    await a.query("DROP TABLE t");
    await a.query("CREATE TABLE t (id integer PRIMARY KEY, k integer NOT NULL, v text)");
    const queued = b.execute(handle.id, [100]);
    await untilWaiting(h);
    await a.query("COMMIT");
    expect((await queued).rows()).toEqual([[100, 7, "b"]]);
  });
}, 60_000);

/**
 * A handle the owner no longer holds (evicted from a bounded cache, or a new connection) plans its
 * fallback text as the handle would: with the parameter types and against the result columns its
 * client was told at prepare (`StatementRequest.preparedShape`), as PostgreSQL's plancache does
 * for the statement it still holds.
 */
async function fallback(client: ProtocolClient, sql: string, params: readonly WireValue[] = []) {
  const handle = await client.prepare(sql);
  // The owner drops the handle; the client still holds what it was told.
  client.unprepare(handle.id);
  const run = (
    values: readonly WireValue[] = params,
    shape: PreparedShape | null = preparedShape(handle),
  ) =>
    outcome(
      client.send({
        t: "statement",
        session: 0,
        text: sql,
        prepared: handle.id,
        ...(shape === null ? {} : { preparedShape: shape }),
        knownColumnsVersion: -1,
        params: values,
        result: { windowBytes: 1 << 20, frameRows: null, wantRows: true },
      }).promise,
    );
  return { handle, run };
}

it("refuses a prepared fallback whose result columns changed, waiting or not", async () => {
  await sessions(async (a, b, h) => {
    const refused = /^PlanShapeChangedError 0A000 cached plan must not change result type/u;
    const read = await fallback(b, "SELECT * FROM t ORDER BY id");
    const write = await fallback(
      b,
      "INSERT INTO t (id, k, v) VALUES ($1, 7, 'b') RETURNING *",
      [100],
    );
    expect(await read.run(), "unchanged").toBe("served");
    await a.query("BEGIN");
    await a.query("ALTER TABLE t ADD COLUMN c integer");
    const queued = write.run();
    await untilWaiting(h);
    await a.query("COMMIT");
    expect(await queued, "waiting for the turn").toMatch(refused);
    expect(await write.run([101]), "planned on arrival").toMatch(refused);
    expect(await read.run(), "a query").toMatch(refused);
    expect(await read.run(), "again").toMatch(refused);
    expect((await a.query("SELECT count(*) FROM t WHERE id >= 100")).rows()).toEqual([[0]]);
    // Without what its client was told, a fallback cannot keep the rule: refused, nothing ran.
    expect(await write.run([102], null)).toMatch(/^ProtocolError .*shape its client was told/u);
    expect((await a.query("SELECT count(*) FROM t WHERE id >= 100")).rows()).toEqual([[0]]);
  });
}, 60_000);

it("plans a prepared fallback with the parameter types it was prepared with", async () => {
  await sessions(async (a, b) => {
    // As "re-plans a prepared write with the parameter types it was prepared with": $1 stays text.
    const update = await fallback(b, "UPDATE t SET v = $1 WHERE id = 1", ["9"]);
    expect(update.handle.parameters.map((type) => type.kind)).toEqual(["text"]);
    const insert = await fallback(b, "INSERT INTO t (id, k) VALUES ($1, 7) RETURNING *", [100]);
    await a.query("DROP TABLE t");
    await a.query("CREATE TABLE t (id integer PRIMARY KEY, k integer, v integer)");
    await a.query("INSERT INTO t VALUES (1, 1, 0)");
    expect(await update.run()).toMatch(/ 42804 /u);
    expect((await a.query("SELECT v FROM t")).rows()).toEqual([[0]]);
    // v's type changed, so the INSERT's result columns did too.
    expect(await insert.run()).toMatch(/ 0A000 /u);
    expect((await a.query("SELECT count(*) FROM t")).rows()).toEqual([[1]]);
  });
}, 60_000);

it("serves a prepared fallback whose columns only changed nullability", async () => {
  await sessions(async (a, b) => {
    const insert = await fallback(
      b,
      "INSERT INTO t (id, k, v) VALUES ($1, 7, 'b') RETURNING *",
      [100],
    );
    await a.query("DROP TABLE t");
    await a.query("CREATE TABLE t (id integer PRIMARY KEY, k integer NOT NULL, v text)");
    expect(await insert.run()).toBe("served");
    expect((await a.query("SELECT id, k, v FROM t")).rows()).toEqual([[100, 7, "b"]]);
  });
}, 60_000);

/**
 * #69 re-review P1 (exactly-once): a write planned on its turn is admitted like any write, in the
 * ledger and in the owner's running requests, so a reconnect or a close while it waits ends it
 * as it ends a write waiting for the turn (`MINNOW_SESSION_LOST`, nothing written). Before, it
 * waited outside both: the resent request was answered 42703 and the orphaned original
 * committed when A did.
 */
for (const named of ["c", "v"] as const)
  it(`ends a write waiting for its turn exactly once on a reconnect (names ${named})`, async () => {
    await sessions(async (a, b, h, reconnect) => {
      await a.query("BEGIN");
      await a.query("ALTER TABLE t ADD COLUMN c integer");
      // c: A's new column (its arrival plan fails: planned on its turn); v: plans at arrival.
      const told = outcome(b.query(`INSERT INTO t (id, k, ${named}) VALUES (10, 10, 5)`));
      await untilWaiting(h);
      await reconnect(b);
      expect(await settledYet(told), "answered for the old connection").not.toBe("pending");
      const answer = await told;
      await a.query("COMMIT");
      await new Promise((resolve) => setTimeout(resolve, 20));
      const rows = (await a.query("SELECT id FROM t ORDER BY id")).rows();
      // Never an error for a write that committed, nor a success for one that did not.
      expect(answer).toMatch(/MINNOW_SESSION_LOST/u);
      expect(rows).toEqual([[1], [2]]);
    });
  }, 60_000);

for (const named of ["c", "v"] as const)
  it(`ends a write waiting for its turn on its connection's close, and writes nothing (names ${named})`, async () => {
    await sessions(async (a, b, h) => {
      await a.query("BEGIN");
      await a.query("ALTER TABLE t ADD COLUMN c integer");
      const told = outcome(b.query(`INSERT INTO t (id, k, ${named}) VALUES (10, 10, 5)`));
      await untilWaiting(h);
      b.close();
      expect(await told).not.toBe("served");
      // The close reaches the owner on its own port: once it does, the write no longer waits.
      const deadline = Date.now() + 5_000;
      while (h.path.waitingWriters > 0) {
        if (Date.now() > deadline) throw new Error("the closed connection's write still waits");
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      await a.query("COMMIT");
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect((await a.query("SELECT id FROM t ORDER BY id")).rows()).toEqual([[1], [2]]);
      expect(h.path.waitingWriters).toBe(0);
    });
  }, 60_000);

it("answers at once a write that cannot plan behind a transaction that changed no catalog", async () => {
  await sessions(async (a, b, h) => {
    await a.query("BEGIN");
    await a.query("INSERT INTO t VALUES (3, 3, 'a')");
    const told = outcome(b.query("INSERT INTO t (id, k, nope) VALUES (10, 10, 5)"));
    expect(await settledYet(told), "no wait: nothing can change its answer").toMatch(/ 42703 /u);
    expect(h.path.waitingWriters).toBe(0);
    await a.query("COMMIT");
  });
}, 60_000);

it("plans a write that names a column A is adding once A ends", async () => {
  for (const end of ["COMMIT", "ROLLBACK"] as const)
    await sessions(async (a, b, h) => {
      await a.query("BEGIN");
      await a.query("ALTER TABLE t ADD COLUMN c integer");
      const pending = outcome(b.query("INSERT INTO t (id, k, v, c) VALUES (100, 7, 'b', 5)"));
      await untilWaiting(h);
      expect(await settledYet(pending), `${end}: B waits for A`).toBe("pending");
      await a.query(end);
      if (end === "COMMIT") {
        expect(await pending).toBe("served");
        expect((await a.query("SELECT c FROM t WHERE id = 100")).rows()).toEqual([[5]]);
      } else expect(await pending).toMatch(/^PlanningError 42703 /u);
    });
}, 60_000);

it("plans an upsert whose conflict target A is creating once A ends", async () => {
  await sessions(async (a, b, h) => {
    await a.query("BEGIN");
    await a.query("CREATE UNIQUE INDEX t_k ON t (k)");
    const pending = outcome(
      b.query("INSERT INTO t (id, k, v) VALUES (100, 1, 'b') ON CONFLICT (k) DO NOTHING"),
    );
    await untilWaiting(h);
    expect(await settledYet(pending)).toBe("pending");
    await a.query("COMMIT");
    expect(await pending).toBe("served");
    expect((await a.query("SELECT count(*) FROM t")).rows()).toEqual([[2]]);
  });
}, 60_000);

it("refuses at once a write to a table A created and has not committed (42P01)", async () => {
  await sessions(async (a, b, h) => {
    await a.query("BEGIN");
    await a.query("CREATE TABLE u (id integer PRIMARY KEY)");
    // PostgreSQL does not find the table, so there is no lock to wait on.
    const pending = outcome(b.query("INSERT INTO u VALUES (1)"));
    const first = await Promise.race([
      pending,
      untilWaiting(h).then(
        () => "waiting for A",
        () => "never waited",
      ),
    ]);
    expect(first).toMatch(/^PlanningError 42P01 /u);
    await a.query("COMMIT");
    await b.query("INSERT INTO u VALUES (1)");
  });
}, 60_000);

it("cancels a write waiting for A to end before it plans", async () => {
  await sessions(async (a, b, h) => {
    await a.query("BEGIN");
    await a.query("ALTER TABLE t ADD COLUMN c integer");
    const abort = new AbortController();
    const pending = b
      .query("INSERT INTO t (id, k, v, c) VALUES (100, 7, 'b', 5)", [], { signal: abort.signal })
      .then(() => "served", describe);
    await untilWaiting(h);
    // The client answers with the signal's reason once the owner answered it cancelled.
    abort.abort(new CancelledError());
    expect(await pending).toMatch(/^CancelledError 57014 /u);
    expect(h.path.waitingWriters).toBe(0);
    await a.query("COMMIT");
    expect((await a.query("SELECT count(*) FROM t WHERE id = 100")).rows()).toEqual([[0]]);
  });
}, 60_000);

it("creates a table from a query (CREATE TABLE … AS), autocommit, waiting and in a transaction", async () => {
  await sessions(async (a, b, h) => {
    await b.query("CREATE TABLE t2 AS SELECT id, k FROM t");
    expect((await a.query("SELECT * FROM t2 ORDER BY id")).rows()).toEqual([
      [1, 1],
      [2, 2],
    ]);
    await b.query("BEGIN");
    await b.query("CREATE TABLE t3 AS SELECT id FROM t WHERE id = 2");
    expect((await b.query("SELECT * FROM t3")).rows()).toEqual([[2]]);
    await b.query("COMMIT");
    expect((await a.query("SELECT * FROM t3")).rows()).toEqual([[2]]);
    // Waiting behind A's ADD COLUMN, it is planned on the catalog A committed.
    await a.query("BEGIN");
    await a.query("ALTER TABLE t ADD COLUMN c integer");
    const pending = outcome(b.query("CREATE TABLE t4 AS SELECT * FROM t"));
    await untilWaiting(h);
    await a.query("COMMIT");
    expect(await pending).toBe("served");
    const t4 = await a.query("SELECT * FROM t4 ORDER BY id");
    expect(t4.columns.map((column) => column.name)).toEqual(["id", "k", "v", "c"]);
    expect(t4.rows()).toEqual([
      [1, 1, "a", null],
      [2, 2, "a", null],
    ]);
  });
}, 60_000);
