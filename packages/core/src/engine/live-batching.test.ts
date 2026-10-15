/**
 * Batched live delivery and key changes (0.15.0), held to what the live-delivery audit found:
 * a failing sweep still hands out what it settled, one subscriber cannot stall another's batch,
 * a failed worker post resets rather than corrupts, rekey pairing never crosses tables, and key
 * changes neither go missing when deliveries coalesce nor linger from a stale baseline.
 */
import { describe, expect, it } from "vitest";
import {
  MemoryBlockStore,
  OpfsCoordinationError,
  type Manifest,
  type StoragePage,
} from "../storage/index.js";
import { createBoundary } from "./client-audit-harness.js";
import { MinnowDatabaseClient } from "./client.js";
import { MinnowDatabase } from "./database.js";
import { KeyedLiveQuery } from "./keyed-live.js";
import { LiveQuerySet, type LiveQueryInput } from "./live.js";
import type { QueryResult } from "./query.js";
import { createLiveQueryManager } from "./typed-live.js";
import { attachDatabaseWorker } from "./worker-host.js";

const settle = (ms = 0): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

describe("batched live delivery", () => {
  it("hands out a failing sweep's settled results before it retries the rest", async () => {
    let version = 1;
    let failing = false;
    const manifests: Manifest[] = [];
    const host = {
      currentProbe: () =>
        Promise.resolve({ manifestVersion: version, catalogEpoch: version, schemaEpoch: 0 }),
      manifestPage: (after: number | null): Promise<StoragePage<Manifest, number>> =>
        Promise.resolve({
          records: manifests.filter((manifest) => manifest.version > (after ?? -1)),
          nextCursor: null,
        }),
      dependencyTableIds: () => Promise.resolve(new Set(["t"])),
      execute: (query: LiveQueryInput): Promise<QueryResult> => {
        if (failing && query === "second") {
          return Promise.reject(new OpfsCoordinationError("leader-unavailable"));
        }
        return Promise.resolve({ columns: ["v"], columnDomains: [null], rows: [{ v: version }] });
      },
    };
    const live = new LiveQuerySet(host);
    const seen = { first: [] as unknown[], second: [] as unknown[] };
    await live.subscribe("first", { onChange: (result) => seen.first.push(result.rows[0]?.v) });
    await live.subscribe("second", { onChange: (result) => seen.second.push(result.rows[0]?.v) });
    version = 2;
    manifests.push({
      version: 2,
      previousVersion: 1,
      createdAt: "",
      liveBlockCount: 0,
      liveBlockBytes: 0,
      changedTableIds: ["t"],
    });
    failing = true;
    await live.refresh();
    expect(seen.first).toEqual([1, 2]);
    failing = false;
    await settle(150);
    await live.refresh();
    expect(seen.second).toEqual([1, 2]);
    live.close();
  });

  it("delivers the rest of a client batch when one subscriber's callback throws", async () => {
    const boundary = createBoundary();
    attachDatabaseWorker(boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide, {
      store: { kind: "memory" },
      onWorkerError: () => undefined,
    });
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
    await client.execute("INSERT INTO t VALUES (1, 1), (2, 2)");
    const live = client.liveQueries();
    let calls = 0;
    await live.subscribe("SELECT count(*) AS n FROM t", {
      onChange: () => {
        calls += 1;
        if (calls > 1) throw new Error("renderer failed");
      },
    });
    const sums: unknown[] = [];
    await live.subscribe("SELECT sum(x) AS s FROM t", {
      onChange: (result) => sums.push(result.rows[0]?.s),
    });
    await client.execute("INSERT INTO t VALUES (3, 3)");
    await live.refresh();
    await settle(20);
    expect(sums).toEqual([3, 6]);
    await live.close();
    await client.close();
  });

  it("starts a subscriber over after a delivery the worker could not post", async () => {
    const boundary = createBoundary();
    let refuse = false;
    const scope = boundary.workerSide;
    const post = scope.postMessage.bind(scope);
    scope.postMessage = (message: unknown, options?: { transfer: ArrayBuffer[] }) => {
      const frame = message as { event?: string; handleId?: string };
      if (refuse && (frame.event === "batch" || frame.event === "patch")) {
        throw new DOMException("could not clone", "DataCloneError");
      }
      post(message, options);
    };
    attachDatabaseWorker(scope);
    const client = new MinnowDatabaseClient(boundary.clientSide, {
      store: { kind: "memory" },
      onWorkerError: () => undefined,
    });
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
    await client.execute("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)");
    const live = client.liveQueries();
    let rows: QueryResult["rows"] = [];
    const errors: unknown[] = [];
    await live.subscribePatches("SELECT id, x FROM t ORDER BY id", {
      onPatch: (patch) => {
        if (patch.type === "reset") rows = patch.result.rows;
        else {
          const changed = new Map(patch.changedRows.map(({ index, row }) => [index, row]));
          rows = Array.from(patch.retained, (was, index) => {
            const row = was >= 0 ? rows[was] : changed.get(index);
            if (row === undefined) throw new Error("Patch did not provide a row");
            return row;
          });
        }
      },
      onError: (error) => errors.push(error),
    });
    refuse = true;
    await client.execute("UPDATE t SET x = 20 WHERE id = 2");
    await live.refresh();
    await settle(20);
    refuse = false;
    await client.execute("UPDATE t SET x = 30 WHERE id = 3");
    await live.refresh();
    await settle(20);
    expect(errors).toHaveLength(1);
    expect(rows).toEqual([
      { id: 1, x: 1 },
      { id: 2, x: 20 },
      { id: 3, x: 30 },
    ]);
    await live.close();
    await client.close();
  });
});

describe("key changes on live deliveries", () => {
  async function tables(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE projects (id INTEGER PRIMARY KEY)");
    await database.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, label TEXT)");
    await database.execute("INSERT INTO projects VALUES (5), (-1)");
    await database.execute("INSERT INTO tasks VALUES (5, 'old task'), (-1, 'offline task')");
    return database;
  }

  function keyedTasks(database: MinnowDatabase, table?: string) {
    const manager = createLiveQueryManager(database);
    const sql = "SELECT id, label FROM tasks ORDER BY label";
    const query = manager.watch<{ id: number; label: string }>({
      query: sql,
      execute: async () =>
        (await database.query(sql)).rows as unknown as Array<{ id: number; label: string }>,
      decode: (result) => result.rows as unknown as Array<{ id: number; label: string }>,
    });
    const keyed = new KeyedLiveQuery(query, {
      key: "id",
      ...(table === undefined ? {} : { table }),
    });
    const unsubscribe = keyed.subscribe(() => undefined);
    return { manager, keyed, unsubscribe };
  }

  it("never pairs rows of a table other than the one the key belongs to", async () => {
    const database = await tables();
    const { manager, keyed, unsubscribe } = keyedTasks(database, "tasks");
    await keyed.refresh();
    await database.write(async (session) => {
      await session.execute("UPDATE projects SET id = 9 WHERE id = 5");
      await session.execute("DELETE FROM tasks WHERE id = 5");
      await session.execute("INSERT INTO tasks VALUES (9, 'brand new task')");
    });
    await keyed.refresh();
    const snapshot = keyed.getSnapshot();
    const types = snapshot.status === "ready" ? snapshot.changes.map(({ type }) => type) : [];
    expect(types).not.toContain("rekey");
    unsubscribe();
    await manager.close();
  });

  it("pairs a table's own rekey when told the table, and only then", async () => {
    const database = await tables();
    const named = keyedTasks(database, "tasks");
    const unnamed = keyedTasks(database);
    await named.keyed.refresh();
    await unnamed.keyed.refresh();
    await database.write(async (session) => {
      await session.execute("UPDATE projects SET id = 101 WHERE id = -1");
      await session.execute("UPDATE tasks SET id = 205 WHERE id = -1");
    });
    await named.keyed.refresh();
    await unnamed.keyed.refresh();
    const changes = (keyed: KeyedLiveQuery<{ id: number; label: string }, "id">) => {
      const snapshot = keyed.getSnapshot();
      return snapshot.status === "ready" ? snapshot.changes : [];
    };
    expect(changes(named.keyed)).toMatchObject([{ type: "rekey", from: -1, key: 205 }]);
    expect(changes(unnamed.keyed).map(({ type }) => type)).not.toContain("rekey");
    named.unsubscribe();
    unnamed.unsubscribe();
    await named.manager.close();
    await unnamed.manager.close();
  });

  it("keeps every coalesced delivery's key changes on the next snapshot", async () => {
    const database = await tables();
    const manager = createLiveQueryManager(database);
    const sql = "SELECT id, label FROM tasks ORDER BY label";
    let hold: Promise<void> | undefined;
    const query = manager.watch<{ id: number; label: string }>({
      query: sql,
      execute: async () => {
        await hold;
        return (await database.query(sql)).rows as unknown as Array<{
          id: number;
          label: string;
        }>;
      },
    });
    const keyed = new KeyedLiveQuery(query, { key: "id", table: "tasks" });
    const unsubscribe = keyed.subscribe(() => undefined);
    await keyed.refresh();
    // Re-execution waits until both commits have invalidated the query, so they coalesce.
    let release = (): void => undefined;
    hold = new Promise<void>((resolve) => (release = resolve));
    await database.execute("UPDATE tasks SET id = 205 WHERE id = -1");
    await settle(20);
    await database.execute("UPDATE tasks SET label = 'renamed' WHERE id = 5");
    await settle(20);
    release();
    hold = undefined;
    await keyed.refresh();
    await settle(20);
    const snapshot = keyed.getSnapshot();
    expect(snapshot.status === "ready" ? snapshot.keyChanges : undefined).toEqual([
      { table: "tasks", from: -1, to: 205 },
    ]);
    expect(snapshot.status === "ready" ? snapshot.changes.map(({ type }) => type) : []).toContain(
      "rekey",
    );
    unsubscribe();
    await manager.close();
  });

  it("does not report a rekey from before a set's current subscriptions", async () => {
    const database = await tables();
    const live = database.liveQueries();
    const first = await live.subscribe("SELECT id FROM tasks ORDER BY id", {
      onChange: () => undefined,
    });
    first.close();
    await database.execute("UPDATE tasks SET id = 100 WHERE id = -1");
    const reported: unknown[] = [];
    await live.subscribe("SELECT id FROM tasks ORDER BY id", {
      onChange: (_result, delivery) => reported.push(delivery.keyChanges),
    });
    await database.execute("UPDATE tasks SET id = 200 WHERE id = 5");
    await live.refresh();
    expect(reported.at(-1)).toEqual([{ table: "tasks", from: 5, to: 200 }]);
    live.close();
  });
});
