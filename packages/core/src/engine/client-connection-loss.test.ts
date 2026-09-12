/**
 * What a client's callers hear when the worker is lost or a call is abandoned: live routes end
 * with `onError` then `onComplete`, typed live queries reach the error state, pending mutations
 * report an unknown outcome, and a cancelled scope read does not fail the scope that follows.
 */
import { expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabaseClient } from "./client.js";
import { MinnowDatabase } from "./database.js";
import {
  DatabaseWorkerFailedError,
  DatabaseWorkerOutcomeUnknownError,
  DatabaseWorkerTimeoutError,
} from "./errors.js";
import { createLiveQueryManager } from "./typed-live.js";
import { attachDatabaseWorker, exposeDatabase } from "./worker-host.js";
import { createBoundary, faultyStore, settled } from "./client-audit-harness.js";

const transportError = {
  message: "boom",
  filename: "w.js",
  lineno: 1,
  colno: 1,
  error: new Error("boom"),
};

async function seeded(client: MinnowDatabaseClient): Promise<void> {
  await client.createTable({
    name: "t",
    columns: [
      { name: "id", type: "number" },
      { name: "v", type: "string" },
    ],
    uniqueKey: "id",
  });
  await client.insert("t", { id: 1, v: "x" });
}

async function until(assertion: () => void, attempts = 50): Promise<void> {
  let failure: unknown;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      assertion();
      return;
    } catch (error) {
      failure = error;
      await settled(10);
    }
  }
  throw failure;
}

/**
 * A store whose block reads park on a gate while `armed` is set, so a scope read can be
 * cancelled while the worker is still inside it.
 */
function gatedStore() {
  let gate: (() => void) | undefined;
  let armed = false;
  const { store } = faultyStore(async (method, _args, run) => {
    if (armed && method.startsWith("getBlock")) {
      await new Promise<void>((resolve) => {
        gate = resolve;
      });
    }
    return run();
  });
  return {
    store,
    arm: () => {
      armed = true;
    },
    release: () => {
      armed = false;
      gate?.();
    },
  };
}

it("ends every live subscription with onError then onComplete when the transport fails", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "memory" },
    onWorkerError: () => undefined,
  });
  await seeded(client);
  const live = client.liveQueries();
  const events: string[] = [];
  const sub = await live.subscribe("SELECT id FROM t", {
    onChange: () => events.push("change"),
    onError: (e) => events.push(`error:${(e as Error).name}`),
    onComplete: () => events.push("complete"),
  });
  expect(events).toEqual(["change"]);
  boundary.emitTransport("error", transportError);
  await settled(50);
  expect(events).toEqual(["change", "error:DatabaseWorkerFailedError", "complete"]);
  // The subscription already ended, so closing it is a no-op rather than a failed call...
  await expect(sub.close()).resolves.toBeUndefined();
  // ...while the client itself stays dead, and the ended route is never delivered to again.
  await expect(client.listTables()).rejects.toBeInstanceOf(DatabaseWorkerFailedError);
  await settled(20);
  expect(events).toHaveLength(3);
});

it("moves a typed live query to the error state when the connection is lost", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "memory" },
    onWorkerError: () => undefined,
  });
  await client.createTable({
    name: "t",
    columns: [{ name: "id", type: "number" }],
    uniqueKey: "id",
  });
  await client.insert("t", { id: 1 });
  const manager = createLiveQueryManager(client);
  const sql = "SELECT id FROM t ORDER BY id";
  const query = manager.watch<{ id: number }>({
    query: sql,
    execute: async (signal) =>
      (await client.query(sql, signal === undefined ? {} : { signal })).rows as Array<{
        id: number;
      }>,
  });
  const unsubscribe = query.subscribe(() => undefined);
  await until(() => expect(query.getSnapshot().status).toBe("ready"));
  expect(query.getSnapshot().rows).toEqual([{ id: 1 }]);
  boundary.emitTransport("error", transportError);
  await until(() => expect(query.getSnapshot().status).toBe("error"));
  const snapshot = query.getSnapshot();
  expect(snapshot.status === "error" && snapshot.error).toBeInstanceOf(DatabaseWorkerFailedError);
  unsubscribe();
  query.close();
  await manager.close().catch(() => undefined);
});

it("settles every call when the worker dies mid-scope: mutations report an unknown outcome", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, {
    store: { kind: "memory" },
    requestTimeoutMs: 200,
  });
  await seeded(client);
  const scope = client.write(async (s) => {
    await s.insertBatch("t", [{ id: 2, v: "y" }]);
    boundary.sever();
    await s.insertBatch("t", [{ id: 3, v: "z" }]);
  });
  const direct = client.insert("t", { id: 4, v: "w" });
  const [scopeError, directError] = await Promise.all([
    scope.catch((e: unknown) => e),
    direct.catch((e: unknown) => e),
  ]);
  expect(scopeError).toBeInstanceOf(DatabaseWorkerTimeoutError);
  expect(directError).toBeInstanceOf(DatabaseWorkerOutcomeUnknownError);
  expect((directError as Error).cause).toBeInstanceOf(DatabaseWorkerTimeoutError);
});

it("logs no deserialization diagnostics for ordinary traffic across a typed boundary", async () => {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  const logged = vi.spyOn(console, "error").mockImplementation(() => undefined);
  try {
    const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
    await seeded(client);
    await client.listTables();
    await settled(20);
    await client.close();
    expect(logged.mock.calls).toEqual([]);
  } finally {
    logged.mockRestore();
  }
});

it("fails only the call whose result cannot be cloned, not the connection", async () => {
  const boundary = createBoundary();
  const database = new MinnowDatabase(new MemoryBlockStore());
  // Poison one root method so its result carries a function.
  (database as unknown as { listTables: () => Promise<unknown> }).listTables = async () => [
    { f: () => 1 },
  ];
  exposeDatabase(database, boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { onWorkerError: () => undefined });
  await client.ready();
  const error = await client.listTables().catch((e: unknown) => e);
  expect(error).toBeInstanceOf(Error);
  expect((error as Error).name).toBe("DataCloneError");
  await expect(client.storageStats()).resolves.toBeDefined();
  await client.close();
});

it("runs the next scope statement after a cancelled read once that read winds down", async () => {
  const { store, arm, release } = gatedStore();
  const boundary = createBoundary();
  exposeDatabase(new MinnowDatabase(store), boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { requestTimeoutMs: 5_000 });
  await seeded(client);
  const { result } = await client.write(async (s) => {
    arm();
    const controller = new AbortController();
    const read = s.query("SELECT * FROM t", { signal: controller.signal });
    await settled(30);
    controller.abort();
    const readError = await read.catch((e: unknown) => e);
    // The worker is still inside the abandoned read; the next call waits for it to finish.
    const next = s.execute("INSERT INTO t (id, v) VALUES (9, 'y')");
    await settled(30);
    release();
    await next;
    const { rows } = await s.query("SELECT COUNT(*) AS n FROM t");
    return { readError: (readError as Error).name, n: rows[0]?.n };
  });
  expect(result).toEqual({ readError: "AbortError", n: 2 });
  expect((await client.readTable("t")).length).toBe(2);
  await client.close();
});

it("commits a scope that cancelled a read and returned while the read was still running", async () => {
  const { store, arm, release } = gatedStore();
  const boundary = createBoundary();
  exposeDatabase(new MinnowDatabase(store), boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide, { requestTimeoutMs: 5_000 });
  await seeded(client);
  const { result } = await client.write(async (s) => {
    await s.insertBatch("t", [{ id: 2, v: "staged" }]);
    arm();
    const controller = new AbortController();
    const read = s.query("SELECT * FROM t", { signal: controller.signal });
    await settled(30);
    controller.abort();
    await read.catch(() => undefined);
    setTimeout(release, 50); // the worker is still inside the cancelled read at commit time
    return "done";
  });
  expect(result).toBe("done");
  expect((await client.readTable("t")).length).toBe(2);
  await client.close();
});
