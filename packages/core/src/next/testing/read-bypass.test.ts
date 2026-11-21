/**
 * engine/transactions.mdx ("Conflicts"; reads through query() do not take turns and never wait
 * behind a callback) and next/README.md ("One writer at a time"): a session's read-only query
 * does not queue behind that session's own autocommit write while the write waits for the writer
 * turn. Outside that case the session's requests keep their order: inside a transaction, behind
 * DDL or transaction control, and for anything that is not a plain read (row locks, volatile
 * functions such as nextval, execute()). The bypassing read runs on the same session and sees
 * every write the session has had acknowledged, none it is still waiting on.
 */
import { afterEach, expect, it, vi } from "vitest";
import { ProtocolClient } from "../owner/client.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import type { EngineSession } from "../owner/engine.js";
import { publicOwner } from "./public-owner.test.fixture.js";

type Owner = Awaited<ReturnType<typeof publicOwner>>;
const owners: Owner[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const owner of owners.splice(0)) await owner.close();
});

async function setup(sessions?: Map<EngineSession, string[]>) {
  const owner = await publicOwner({ transactionIdleTimeoutMs: 20_000 });
  owners.push(owner);
  if (sessions !== undefined) {
    const open = owner.engine.openSession.bind(owner.engine);
    vi.spyOn(owner.engine, "openSession").mockImplementation((...args) => {
      const session = open(...args);
      const run = session.run.bind(session);
      sessions.set(session, []);
      session.run = (statement, params, context) => {
        sessions.get(session)?.push(statement.text);
        return run(statement, params, context);
      };
      return session;
    });
  }
  const connect = async () => {
    const channel = new MessageChannel();
    const protocol = new ProtocolClient();
    owner.server.accept(channel.port2);
    await protocol.connect(channel.port1);
    return new MinnowDatabaseClient(protocol, { ready: Promise.resolve() });
  };
  const a = await connect(),
    b = await connect();
  await a.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
  return { owner, a, b };
}

/** Whether `promise` settles within `ms` (a bound on the bypass, far below any idle timeout). */
const settlesWithin = (promise: Promise<unknown>, ms: number): Promise<boolean> =>
  Promise.race([
    promise.then(
      () => true,
      () => true,
    ),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), ms)),
  ]);
const ids = async (db: MinnowDatabaseClient, sql = "SELECT id FROM t ORDER BY id") =>
  (await db.query(sql)).rows().map((row) => Number((row as { id: unknown }).id));

it("a read does not wait behind its session's write queued for the turn, and sees what was acknowledged", async () => {
  const { a, b } = await setup();
  await b.execute("INSERT INTO t VALUES (0, 'acknowledged')");
  await a.execute("BEGIN");
  await a.execute("INSERT INTO t VALUES (1, 'staged')");
  const queued = b.execute("INSERT INTO t VALUES (2, 'queued')");
  expect(await settlesWithin(queued, 200)).toBe(false);
  const read = b.query("SELECT id FROM t ORDER BY id");
  expect(await settlesWithin(read, 1000)).toBe(true);
  // B's acknowledged row; not A's staged row, not B's own write still waiting for the turn.
  expect((await read).rows().map((row) => Number((row as { id: unknown }).id))).toEqual([0]);
  expect(await settlesWithin(queued, 50)).toBe(false);
  await a.execute("COMMIT");
  await queued;
  expect(await ids(b)).toEqual([0, 1, 2]);
});

it("a bare BEGIN holds the turn: the queued write waits, the read does not", async () => {
  const { a, b } = await setup();
  await a.execute("BEGIN");
  const queued = b.execute("INSERT INTO t VALUES (2, 'queued')");
  expect(await settlesWithin(queued, 200)).toBe(false);
  expect(await settlesWithin(b.query("SELECT id FROM t"), 1000)).toBe(true);
  await a.execute("COMMIT");
  await queued;
  expect(await ids(b)).toEqual([2]);
});

it.each([
  ["a row-locking read", (b: MinnowDatabaseClient) => b.query("SELECT id FROM t FOR UPDATE")],
  ["a volatile read (nextval)", (b: MinnowDatabaseClient) => b.query("SELECT nextval('s') AS v")],
  ["a SELECT through execute()", (b: MinnowDatabaseClient) => b.execute("SELECT id FROM t")],
])("%s keeps its place behind the queued write", async (_name, read) => {
  const { a, b } = await setup();
  await a.execute("CREATE SEQUENCE s");
  await a.execute("BEGIN");
  const queued = b.execute("INSERT INTO t VALUES (2, 'queued')");
  const kept = read(b);
  kept.catch(() => undefined);
  expect(await settlesWithin(kept, 300)).toBe(false);
  await a.execute("COMMIT");
  await queued;
  await kept.catch(() => undefined);
});

it("DDL queued ahead keeps a later read in its place", async () => {
  const { a, b } = await setup();
  await a.execute("BEGIN");
  const ddl = b.execute("CREATE TABLE u(id INTEGER PRIMARY KEY)");
  const read = b.query("SELECT id FROM t");
  expect(await settlesWithin(read, 300)).toBe(false);
  await a.execute("COMMIT");
  await ddl;
  expect((await read).rows()).toEqual([]);
});

it("inside an open transaction the session's reads stay in order and see its staged rows", async () => {
  const { b } = await setup();
  await b.execute("BEGIN");
  const insert = b.execute("INSERT INTO t SELECT g, 'x' FROM generate_series(1, 60000) g");
  const count = b.query("SELECT count(*) AS n FROM t");
  await insert;
  expect(Number(((await count).rows()[0] as { n: unknown }).n)).toBe(60000);
  await b.execute("ROLLBACK");
});

it("writes keep their order: a later write never passes an earlier one", async () => {
  const { a, b } = await setup();
  await a.execute("BEGIN");
  const first = b.execute("INSERT INTO t VALUES (5, 'first')");
  const second = b.execute("INSERT INTO t VALUES (5, 'second')");
  second.catch(() => undefined);
  await a.execute("COMMIT");
  await first;
  await expect(second).rejects.toMatchObject({ sqlState: "23505" });
  expect((await b.query("SELECT v FROM t WHERE id = 5")).rows()).toEqual([{ v: "first" }]);
});

it("cancelling the queued write after a bypassing read cancels only that write", async () => {
  const { a, b } = await setup();
  await a.execute("BEGIN");
  const controller = new AbortController();
  const queued = b.execute("INSERT INTO t VALUES (7, 'cancelled')", [], {
    signal: controller.signal,
  });
  queued.catch(() => undefined);
  expect(await ids(b)).toEqual([]);
  controller.abort(new Error("cancel the queued write"));
  await expect(queued).rejects.toThrow();
  await a.execute("COMMIT");
  await b.execute("INSERT INTO t VALUES (8, 'after')");
  expect(await ids(b)).toEqual([8]);
});

it("closing a connection after a bypassed read still drains its queued write in order", async () => {
  const { a, b } = await setup();
  await a.execute("BEGIN");
  const queued = b.execute("INSERT INTO t VALUES (9, 'queued')");
  queued.catch(() => undefined);
  expect(await ids(b)).toEqual([]);
  // Close drains the session: the write still waits for the turn, and close waits for it.
  const closed = b.close();
  expect(await settlesWithin(closed, 200)).toBe(false);
  await a.execute("COMMIT");
  expect(await settlesWithin(closed, 2000)).toBe(true);
  await queued;
  expect(await ids(a)).toEqual([9]);
});

it("the bypassing read runs on its own session, and a prepared read bypasses too", async () => {
  const sessions = new Map<EngineSession, string[]>();
  const { a, b } = await setup(sessions);
  const prepared = await b.prepare("SELECT id FROM t ORDER BY id");
  await b.execute("INSERT INTO t VALUES (3, 'b')");
  await a.execute("BEGIN");
  const queued = b.execute("INSERT INTO t VALUES (4, 'queued')");
  expect(await settlesWithin(queued, 200)).toBe(false);
  const read = prepared.query();
  expect(await settlesWithin(read, 1000)).toBe(true);
  expect((await read).rows()).toEqual([{ id: 3 }]);
  await a.execute("COMMIT");
  await queued;
  // One engine session served B's write, its queued write (which started and waits inside for
  // the turn) and its bypassing read: never a second session with its own state.
  const owner = [...sessions].find(([, texts]) => texts.includes("INSERT INTO t VALUES (3, 'b')"));
  expect(owner?.[1]).toEqual([
    "INSERT INTO t VALUES (3, 'b')",
    "INSERT INTO t VALUES (4, 'queued')",
    "SELECT id FROM t ORDER BY id",
  ]);
});
