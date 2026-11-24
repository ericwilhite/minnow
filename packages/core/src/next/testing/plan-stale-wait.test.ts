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
 * after DROP TABLE B is refused 42P01 (relation does not exist). A re-plan that would change
 * what B's client was told (its result columns) is refused as PostgreSQL refuses a cached plan
 * whose result type changed (0A000, "cached plan must not change result type").
 */
import { afterEach, expect, it } from "vitest";
import { chunkReads } from "../exec/scan.js";
import { createLowerer } from "../lower/create.js";
import { Governor } from "../memory/governor.js";
import { ProtocolClient } from "../owner/client.js";
import { composeEngine } from "../owner/compose.js";
import { OwnerServer } from "../owner/server.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { Harness } from "../write/test-support.js";
import { PLAN_SHAPE_CHANGED_REFUSAL } from "./divergences.js";

afterEach(() => {
  Governor.stallAfterMs = null;
});

const describe = (error: unknown) =>
  error instanceof Error
    ? `${error.name} ${(error as { sqlState?: string }).sqlState ?? ""} ${error.message}`
    : String(error);

async function sessions(body: (a: ProtocolClient, b: ProtocolClient) => Promise<void>) {
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
    await body(a, b);
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
  await sessions(async (a, b) => {
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
    if (waiting) await new Promise((resolve) => setTimeout(resolve, 50));
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

it("refuses, typed and with nothing written, a re-plan that changes B's result columns", async () => {
  // Divergence PLAN_SHAPE_CHANGED_REFUSAL (divergences.ts): PostgreSQL's simple protocol would
  // describe the new shape; next/ told the client the old columns when it planned, so it refuses
  // as PostgreSQL refuses a cached plan (0A000).
  expect(PLAN_SHAPE_CHANGED_REFUSAL.id).toBe("plan-shape-changed-refusal");
  await sessions(async (a, b) => {
    await a.query("BEGIN");
    await a.query("ALTER TABLE t ADD COLUMN c integer");
    const outcome = b
      .query("INSERT INTO t (id, k, v) VALUES (100, 7, 'b') RETURNING *")
      .then(() => "served", describe);
    await new Promise((resolve) => setTimeout(resolve, 50));
    await a.query("COMMIT");
    expect(await outcome).toMatch(
      /^PlanShapeChangedError 0A000 cached plan must not change result type/u,
    );
    expect((await a.query("SELECT count(*) FROM t WHERE id = 100")).rows()).toEqual([[0]]);
    // Sent again, it is planned on the new catalog and served with the new shape.
    const again = await b.query("INSERT INTO t (id, k, v) VALUES (100, 7, 'b') RETURNING *");
    expect(again.rows()).toEqual([[100, 7, "b", null]]);
  });
}, 60_000);
