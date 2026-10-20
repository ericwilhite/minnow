import { describe, expect, it, vi } from "vitest";
import { createBoundary } from "./client-audit-harness.js";
import { MinnowDatabaseClient } from "./client.js";
import { attachDatabaseWorker } from "./worker-host.js";
import { exposeDatabase } from "./worker-server.js";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/index.js";
import { LiveQuerySet } from "./live.js";

function connection() {
  const boundary = createBoundary();
  attachDatabaseWorker(boundary.workerSide);
  return {
    boundary,
    client: new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } }),
  };
}

describe("client key changes", () => {
  it("retries a failed forward read before settling query results", async () => {
    let version = 1,
      fail = false;
    const order: string[] = [];
    const live = new LiveQuerySet({
      currentProbe: async () => ({
        manifestVersion: version,
        catalogEpoch: version,
        schemaEpoch: 0,
      }),
      manifestPage: async () => ({ records: [], nextCursor: null }),
      dependencyTableIds: async () => new Set(["t"]),
      execute: async () => ({ columns: ["id"], columnDomains: [null], rows: [{ id: version }] }),
      keyChanges: async (previous, _probe, report) => {
        if (fail) throw new Error("forward read failed");
        return {
          state: version,
          changes:
            previous === undefined || previous.value === version || report === false
              ? []
              : [{ table: "t", from: 1, to: 2 }],
        };
      },
    });
    const errors: unknown[] = [];
    await live.subscribe("SELECT id FROM t", {
      onChange: () => order.push("row"),
      onError: (error) => errors.push(error),
    });
    await live.subscribeKeyChanges(() => order.push("key"));
    order.length = 0;
    fail = true;
    version = 2;
    await live.refresh();
    expect(order).toEqual([]);
    expect(errors).toHaveLength(1);
    fail = false;
    await live.refresh();
    expect(order).toEqual(["key", "row"]);
    live.close();
  });

  it("recovers missed cross-connection hints while live sets exist", async () => {
    const store = new MemoryBlockStore();
    const engine = new MinnowDatabase(store),
      writer = new MinnowDatabase(store);
    const boundary = createBoundary();
    exposeDatabase(engine, boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide);
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO t VALUES (-1)");
    const live = client.liveQueries();
    await live.subscribe("SELECT COUNT(*) AS n FROM t", { onChange: () => undefined });
    const seen: unknown[] = [];
    const unsubscribe = await client.onKeyChanges((changes) => seen.push(...changes));
    // A different engine over a memory store sends this client no local or broadcast hint.
    await writer.execute("UPDATE t SET id = 10 WHERE id = -1");
    await vi.waitFor(() => expect(seen).toEqual([{ table: "t", from: -1, to: 10 }]), {
      timeout: 3000,
    });
    await unsubscribe();
    await live.close();
    await client.close();
    await writer.close();
  });

  it("starts at registration and supports simultaneous listeners", async () => {
    const { client } = connection();
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO t VALUES (-1)");
    const live = client.liveQueries();
    await live.subscribe("SELECT COUNT(*) AS n FROM t", { onChange: () => undefined });
    await client.execute("UPDATE t SET id = 10 WHERE id = -1");
    const a: unknown[] = [],
      b: unknown[] = [];
    const [offA, offB] = await Promise.all([
      client.onKeyChanges((changes) => a.push(...changes)),
      client.onKeyChanges((changes) => b.push(...changes)),
    ]);
    await client.execute("UPDATE t SET id = 20 WHERE id = 10");
    await live.refresh();
    expect(a).toEqual([{ table: "t", from: 10, to: 20 }]);
    expect(b).toEqual(a);
    await offA();
    await offB();
    await live.close();
    await client.close();
  });

  it("reports rekeys without queries and releases an unsubscribed listener", async () => {
    const { client } = connection();
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO t VALUES (-1)");
    const seen: unknown[] = [];
    const unsubscribe = await client.onKeyChanges((changes) => seen.push(changes));
    await client.execute("UPDATE t SET id = 10 WHERE id = -1");
    await vi.waitFor(() => expect(seen).toEqual([[{ table: "t", from: -1, to: 10 }]]));
    await unsubscribe();
    await unsubscribe();
    await client.execute("UPDATE t SET id = 11 WHERE id = 10");
    expect(seen).toHaveLength(1);
    await client.close();
  });

  it("reports unrelated and unchanged-result rekeys, once across overlapping sets", async () => {
    const { client } = connection();
    await client.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)");
    await client.execute("CREATE TABLE lines (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO orders VALUES (1)");
    await client.execute("INSERT INTO lines VALUES (-7)");
    const a = client.liveQueries(),
      b = client.liveQueries();
    await a.subscribe("SELECT * FROM orders", { onChange: () => undefined });
    await b.subscribe("SELECT COUNT(*) AS n FROM lines", { onChange: () => undefined });
    const seen: unknown[] = [];
    const unsubscribe = await client.onKeyChanges((changes) => seen.push(...changes));
    await client.execute("UPDATE lines SET id = 500 WHERE id = -7");
    await Promise.all([a.refresh(), b.refresh()]);
    expect(seen).toEqual([{ table: "lines", from: -7, to: 500 }]);
    await Promise.all([a.refresh(), b.refresh()]);
    expect(seen).toHaveLength(1);
    await a.close();
    await b.close();
    await client.execute("UPDATE lines SET id = 501 WHERE id = 500");
    await vi.waitFor(() => expect(seen).toHaveLength(2));
    expect(seen[1]).toEqual({ table: "lines", from: 500, to: 501 });
    await unsubscribe();
    await client.close();
  });

  it("delivers key notices before affected results in the same worker batch", async () => {
    const { boundary, client } = connection();
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO t VALUES (-1)");
    const seen: string[] = [];
    const unsubscribe = await client.onKeyChanges(() => seen.push("key"));
    const live = client.liveQueries();
    await live.subscribe("SELECT * FROM t", { onChange: () => seen.push("row") });
    seen.length = 0;
    boundary.sentByWorker.length = 0;
    await client.execute("UPDATE t SET id = 10 WHERE id = -1");
    await live.refresh();
    expect(seen).toEqual(["key", "row"]);
    const batches = boundary.sentByWorker.filter(
      (frame): frame is { event: string; payload: { events: Array<{ event: string }> } } =>
        (frame as { event?: string }).event === "batch",
    );
    expect(
      batches.some(
        (frame) =>
          frame.payload.events[0]?.event === "keyChanges" &&
          frame.payload.events.some((event) => event.event === "change"),
      ),
    ).toBe(true);
    await unsubscribe();
    await live.close();
    await client.close();
  });

  it("isolates throwing listeners and reports ordered chains", async () => {
    const { client } = connection();
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO t VALUES (-1)");
    const error = vi.fn();
    const bad = await client.onKeyChanges(
      () => {
        throw new Error("listener failed");
      },
      { onError: error },
    );
    const seen: unknown[] = [];
    const good = await client.onKeyChanges((changes) => seen.push(...changes));
    await client.write(async (session) => {
      await session.execute("UPDATE t SET id = 10 WHERE id = -1");
      await session.execute("UPDATE t SET id = 11 WHERE id = 10");
    });
    await vi.waitFor(() =>
      expect(seen).toEqual([
        { table: "t", from: -1, to: 10 },
        { table: "t", from: 10, to: 11 },
      ]),
    );
    expect(error).toHaveBeenCalled();
    await bad();
    await good();
    await client.close();
  });
});
