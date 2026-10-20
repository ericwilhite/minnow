import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { MissingKeyError, UniqueConstraintError } from "./errors.js";
import { createBoundary, faultyStore } from "./client-audit-harness.js";
import { MinnowDatabaseClient } from "./client.js";
import { attachDatabaseWorker } from "./worker-host.js";
import { KeyedLiveQuery } from "./keyed-live.js";
import { createLiveQueryManager } from "./typed-live.js";

async function seed(db: MinnowDatabase) {
  await db.execute(
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT, extra TEXT DEFAULT 'retained')",
  );
  await db.execute(
    "CREATE TABLE lines (id INTEGER PRIMARY KEY, orderId INTEGER REFERENCES orders(id))",
  );
  await db.execute("INSERT INTO orders (id, note) VALUES (-1, 'offline'), (900, 'server')");
  await db.execute("INSERT INTO lines VALUES (1, -1), (2, 900)");
}

describe("explicit key merging", () => {
  it("keeps the existing target in a keyed diff instead of rekeying onto it", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await seed(db);
    const manager = createLiveQueryManager(db);
    const sql = "SELECT id, note FROM orders ORDER BY id";
    interface Row {
      id: number;
      note: string;
    }
    const query = manager.watch<Row>({
      query: sql,
      execute: async () => (await db.query(sql)).rows as unknown as Row[],
      decode: (result) => result.rows as unknown as Row[],
    });
    const keyed = new KeyedLiveQuery(query, { key: "id", table: "orders" });
    const unsubscribe = keyed.subscribe(() => undefined);
    await keyed.refresh();
    await db.mergeKey("orders", -1, 900);
    await keyed.refresh();
    const snapshot = keyed.getSnapshot();
    expect(
      snapshot.status === "ready" ? snapshot.changes.map((change) => change.type) : [],
    ).toContain("delete");
    expect(
      snapshot.status === "ready" ? snapshot.changes.map((change) => change.type) : [],
    ).not.toContain("rekey");
    expect(snapshot.status === "ready" ? snapshot.rows : []).toEqual([{ id: 900, note: "server" }]);
    unsubscribe();
    await manager.close();
    await db.close();
  });

  it("preserves the target, redirects references and held keys, and allows retries", async () => {
    const store = new MemoryBlockStore();
    const db = new MinnowDatabase(store);
    await seed(db);
    await expect(db.execute("UPDATE orders SET id = 900 WHERE id = -1")).rejects.toBeInstanceOf(
      UniqueConstraintError,
    );
    const notices: unknown[] = [];
    const live = db.liveQueries();
    await live.subscribe("SELECT * FROM orders ORDER BY id", {
      onChange: (_rows, delivery) => notices.push(delivery.keyChanges),
    });
    await db.mergeKey("orders", -1, 900);
    await live.refresh();
    expect(notices.at(-1)).toEqual([{ table: "orders", from: -1, to: 900 }]);
    expect((await db.query("SELECT * FROM orders")).rows).toEqual([
      { id: 900, note: "server", extra: "retained" },
    ]);
    expect((await db.query("SELECT * FROM lines ORDER BY id")).rows).toEqual([
      { id: 1, orderId: 900 },
      { id: 2, orderId: 900 },
    ]);
    expect(await db.resolveKey("orders", -1)).toBe(900);
    await db.mergeKey("orders", -1, 900);
    await db.mergeKey("orders", 900, 900);
    const reopened = new MinnowDatabase(store);
    expect(await reopened.resolveKey("orders", -1)).toBe(900);
    await reopened.execute("UPDATE orders SET note = 'updated' WHERE id = -1");
    expect((await reopened.query("SELECT note FROM orders WHERE id = 900")).rows).toEqual([
      { note: "updated" },
    ]);
    live.close();
    await reopened.close();
    await db.close();
  });

  it("does not run source or target DELETE triggers or delete actions", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await seed(db);
    await db.execute("CREATE TABLE audit (value INTEGER)");
    await db.execute(
      "CREATE TRIGGER deleted AFTER DELETE ON orders BEGIN INSERT INTO audit VALUES (OLD.id); END",
    );
    await db.mergeKey("orders", -1, 900);
    expect((await db.query("SELECT * FROM audit")).rows).toEqual([]);
    expect((await db.query("SELECT COUNT(*) AS n FROM lines")).rows).toEqual([{ n: 2 }]);
    await db.close();
  });

  it("refuses missing rows and wrong key shapes without publishing changes", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await seed(db);
    await expect(db.mergeKey("orders", -1, 99)).rejects.toBeInstanceOf(MissingKeyError);
    await expect(db.mergeKey("orders", 99, 900)).rejects.toBeInstanceOf(MissingKeyError);
    await expect(db.mergeKey("orders", [-1, 2], 900)).rejects.toThrow(/primary-key/);
    expect((await db.query("SELECT COUNT(*) AS n FROM orders")).rows).toEqual([{ n: 2 }]);
    expect(await db.resolveKey("orders", -1)).toBe(-1);
    await db.close();
  });

  it("rekeys composite child identities and refuses a child collision atomically", async () => {
    for (const collision of [false, true]) {
      const db = new MinnowDatabase(new MemoryBlockStore());
      await db.execute("CREATE TABLE p (id INTEGER PRIMARY KEY)");
      await db.execute(
        "CREATE TABLE c (parent INTEGER REFERENCES p(id), n INTEGER, PRIMARY KEY (parent, n))",
      );
      await db.execute(
        "CREATE TABLE grandchild (id INTEGER PRIMARY KEY, parent INTEGER, n INTEGER, FOREIGN KEY (parent, n) REFERENCES c(parent, n))",
      );
      await db.execute("INSERT INTO p VALUES (-1), (900)");
      await db.execute("INSERT INTO c VALUES (-1, 1)");
      await db.execute("INSERT INTO grandchild VALUES (1, -1, 1)");
      if (collision) await db.execute("INSERT INTO c VALUES (900, 1)");
      if (collision) {
        await expect(db.mergeKey("p", -1, 900)).rejects.toBeInstanceOf(UniqueConstraintError);
        expect(await db.resolveKey("p", -1)).toBe(-1);
        expect((await db.query("SELECT parent FROM grandchild")).rows).toEqual([{ parent: -1 }]);
      } else {
        await db.mergeKey("p", -1, 900);
        expect(await db.resolveKey("c", [-1, 1])).toEqual([900, 1]);
        expect((await db.query("SELECT parent FROM grandchild")).rows).toEqual([{ parent: 900 }]);
      }
      await db.close();
    }
  });

  it("supports composite source and target keys", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await db.execute(
      "CREATE TABLE t (shop INTEGER, id INTEGER, note TEXT, PRIMARY KEY (shop, id))",
    );
    await db.execute("INSERT INTO t VALUES (1, -1, 'offline'), (1, 900, 'server')");
    await db.mergeKey("t", [1, -1], [1, 900]);
    expect((await db.query("SELECT * FROM t")).rows).toEqual([
      { shop: 1, id: 900, note: "server" },
    ]);
    expect(await db.resolveKey("t", [1, -1])).toEqual([1, 900]);
    await db.close();
  });

  it("rolls back a merge with its containing scope", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await seed(db);
    await expect(
      db.write(async (session) => {
        await session.mergeKey("orders", -1, 900);
        throw new Error("abort");
      }),
    ).rejects.toThrow("abort");
    expect(await db.resolveKey("orders", -1)).toBe(-1);
    expect((await db.query("SELECT orderId FROM lines WHERE id = 1")).rows).toEqual([
      { orderId: -1 },
    ]);
    await db.close();
  });

  it.each(["before", "after"])(
    "reconciles interruption %s transaction publication",
    async (timing) => {
      let armed = false;
      const { store } = faultyStore(async (method, _args, run) => {
        if (armed && method === "writeTransaction") {
          armed = false;
          if (timing === "before") throw new Error("publication interrupted");
          await run();
          throw new Error("reply lost");
        }
        return run();
      });
      const db = new MinnowDatabase(store);
      await seed(db);
      armed = true;
      await db.mergeKey("orders", -1, 900).catch(() => undefined);
      expect(armed).toBe(false);
      const reopened = new MinnowDatabase(store);
      const merged = (await reopened.resolveKey("orders", -1)) === 900;
      expect((await reopened.query("SELECT orderId FROM lines WHERE id = 1")).rows).toEqual([
        { orderId: merged ? 900 : -1 },
      ]);
      expect((await reopened.query("SELECT COUNT(*) AS n FROM orders")).rows).toEqual([
        { n: merged ? 1 : 2 },
      ]);
      await reopened.mergeKey("orders", -1, 900);
      expect(await reopened.resolveKey("orders", -1)).toBe(900);
      await reopened.close();
      await db.close();
    },
  );

  it("works through root and scoped worker APIs", async () => {
    const boundary = createBoundary();
    attachDatabaseWorker(boundary.workerSide);
    const client = new MinnowDatabaseClient(boundary.clientSide, { store: { kind: "memory" } });
    await client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)");
    await client.execute(
      "INSERT INTO t VALUES (-1, 'offline'), (900, 'server'), (-2, 'other'), (901, 'target')",
    );
    await client.mergeKey("t", -1, 900);
    await client.write(async (session) => {
      await session.mergeKey("t", -2, 901);
    });
    expect(await client.resolveKey("t", -1)).toBe(900);
    expect(await client.resolveKey("t", -2)).toBe(901);
    await client.close();
  });
});
