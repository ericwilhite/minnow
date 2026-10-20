import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { createBoundary, faultyStore } from "./client-audit-harness.js";
import { MinnowDatabaseClient } from "./client.js";
import { attachDatabaseWorker } from "./worker-host.js";

describe("batched key resolution", () => {
  it("evaluates retention once even when the clock advances during the batch", async () => {
    let now = Date.parse("2026-10-20T12:00:00Z"),
      ticking = false;
    const { store } = faultyStore((_method, _args, run) => run());
    const db = new MinnowDatabase(store, {
      now: () => new Date(ticking ? now++ : now),
      keyForwardRetentionMs: 1000,
    });
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await db.execute("INSERT INTO t VALUES (-1)");
    await db.execute("UPDATE t SET id = 900 WHERE id = -1");
    ticking = true;
    expect(new Set(await db.resolveKeys("t", new Array<number>(2000).fill(-1)))).toEqual(
      new Set([900]),
    );
    await db.close();
  });

  it("preserves order, duplicate and unknown keys, and follows chains", async () => {
    const { store, calls } = faultyStore((_method, _args, run) => run());
    const db = new MinnowDatabase(store);
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await db.execute("INSERT INTO t VALUES (-1), (-2)");
    await db.execute("UPDATE t SET id = 100 WHERE id = -1");
    await db.execute("UPDATE t SET id = 200 WHERE id = 100");
    const keys = [-2, -1, 200, -1, 999];
    calls.length = 0;
    expect(await db.resolveKeys("t", keys)).toEqual([-2, 200, 200, 200, 999]);
    expect(
      calls.filter((call) => call.method === "getTableByName" && call.args[0] === "t"),
    ).toHaveLength(1);
    expect(keys).toEqual([-2, -1, 200, -1, 999]);
    expect(await db.resolveKeys("t", [])).toEqual([]);
    await db.close();
  });

  it("accepts composite keys, owns its input, and rejects wrong arity", async () => {
    const { store } = faultyStore((_method, _args, run) => run());
    const db = new MinnowDatabase(store);
    await db.execute("CREATE TABLE t (shop INTEGER, id INTEGER, PRIMARY KEY (shop, id))");
    await db.execute("INSERT INTO t VALUES (1, -1), (2, -1)");
    await db.execute("UPDATE t SET id = 900 WHERE shop = 1 AND id = -1");
    const input = [
      [1, -1],
      [2, -1],
      [1, -1],
    ];
    const pending = db.resolveKeys("t", input);
    const first = input[0];
    if (first === undefined) throw new Error("Missing fixture key");
    first[0] = 99;
    expect(await pending).toEqual([
      [1, 900],
      [2, -1],
      [1, 900],
    ]);
    await expect(db.resolveKeys("t", [1])).rejects.toThrow(/primary-key/);
    await db.close();
  });

  it("honors retention and returns domain values", async () => {
    let now = Date.parse("2026-10-20T12:00:00Z");
    const { store } = faultyStore((_method, _args, run) => run());
    const db = new MinnowDatabase(store, { now: () => new Date(now), keyForwardRetentionMs: 1000 });
    await db.execute("CREATE TABLE t (id TIMESTAMPTZ PRIMARY KEY)");
    await db.execute("INSERT INTO t VALUES ('2026-01-01T00:00:00Z')");
    await db.execute("UPDATE t SET id = '2026-01-02T00:00:00Z'");
    const old = new Date("2026-01-01T00:00:00Z");
    expect(await db.resolveKeys("t", [old, old])).toEqual([
      new Date("2026-01-02T00:00:00Z"),
      new Date("2026-01-02T00:00:00Z"),
    ]);
    now += 1001;
    expect(await db.resolveKeys("t", [old])).toEqual([old]);
    await db.close();
  });

  it("uses one worker request for a batch", async () => {
    const boundary = createBoundary();
    attachDatabaseWorker(boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await client.execute("INSERT INTO t VALUES (-1)");
    await client.execute("UPDATE t SET id = 900 WHERE id = -1");
    boundary.sentByClient.length = 0;
    expect(await client.resolveKeys("t", [-1, 900, -2, -1])).toEqual([900, 900, -2, 900]);
    expect(boundary.sentByClient).toHaveLength(1);
    expect(boundary.sentByClient[0]).toMatchObject({ method: "resolveKeys" });
    await client.close();
  });
});
