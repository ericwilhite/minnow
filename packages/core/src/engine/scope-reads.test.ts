/**
 * Reads and engine self-checks inside a write scope see the folded write set — joins,
 * secondary-index lookups, point reads, CHECK pre-images, FK proofs, triggers, generated
 * columns, composite keys, domain columns, INSERT … SELECT, UPDATE … FROM, DELETE … USING,
 * and RETURNING.
 *
 * Most cases compare the scope's own view and the committed result against the same statements
 * run one autocommit at a time on a twin database: the per-statement path never buffers, so it
 * is the oracle.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { type ExecuteResult, MinnowDatabase, type WriteSession } from "./database.js";
import { pointReadTestHooks } from "./point-read.js";

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

type Statement = string | [string, unknown[]];

/** The RETURNING rows of a mutation result. */
function returnedRows(result: ExecuteResult): unknown[] | undefined {
  return "returnedRows" in result ? result.returnedRows : undefined;
}

async function twin(
  create: () => Promise<BlockStore>,
  ddl: string[],
  scoped: Statement[],
  probes: string[],
): Promise<{ scope: string[][]; committed: string[]; oracle: string[] }> {
  const run = async (db: MinnowDatabase, sql: Statement): Promise<unknown> =>
    typeof sql === "string" ? db.execute(sql) : db.execute(sql[0], sql[1] as never);
  const runTx = async (tx: WriteSession, sql: Statement): Promise<unknown> =>
    typeof sql === "string" ? tx.execute(sql) : tx.execute(sql[0], sql[1] as never);
  const read = async (q: (sql: string) => Promise<{ rows: unknown[] }>): Promise<string[]> => {
    const out: string[] = [];
    for (const probe of probes) out.push(JSON.stringify((await q(probe)).rows));
    return out;
  };
  const scopedDb = new MinnowDatabase(await create(), { rowsPerBlock: 8 });
  const oracleDb = new MinnowDatabase(await create(), { rowsPerBlock: 8 });
  for (const sql of ddl) {
    await scopedDb.execute(sql);
    await oracleDb.execute(sql);
  }
  const scopeViews: string[][] = [];
  await scopedDb.write(async (tx) => {
    for (const sql of scoped) {
      await runTx(tx, sql);
      scopeViews.push(await read((p) => tx.query(p)));
    }
  });
  for (const sql of scoped) await run(oracleDb, sql);
  const committed = await read((p) => scopedDb.query(p));
  const oracle = await read((p) => oracleDb.query(p));
  await scopedDb.close();
  await oracleDb.close();
  return { scope: scopeViews, committed, oracle };
}

describe.each(implementations)("scope reads see the write set on $name", ({ create }) => {
  it("joins, GROUP BY, and index lookups over pending inserts, updates, and deletes", async () => {
    const { scope, committed, oracle } = await twin(
      create,
      [
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier INTEGER NOT NULL)",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, total INTEGER NOT NULL)",
        "CREATE INDEX orders_customer ON orders (customer_id)",
        "CREATE INDEX customers_tier ON customers (tier)",
        "INSERT INTO customers (id, name, tier) VALUES (1, 'ann', 1), (2, 'bob', 2), (3, 'cy', 1)",
        "INSERT INTO orders (id, customer_id, total) VALUES (10, 1, 5), (11, 2, 7), (12, 3, 9)",
      ],
      [
        "INSERT INTO customers (id, name, tier) VALUES (4, 'di', 2)",
        "INSERT INTO orders (id, customer_id, total) VALUES (13, 4, 11)",
        "UPDATE customers SET tier = 2 WHERE id = 1",
        "UPDATE orders SET total = 50 WHERE id = 10",
        "DELETE FROM orders WHERE id = 11",
        "DELETE FROM customers WHERE id = 3",
        "DELETE FROM orders WHERE id = 12",
        "INSERT INTO customers (id, name, tier) VALUES (3, 'cy2', 3)",
        "UPDATE customers SET name = 'ann2' WHERE id = 1",
      ],
      [
        "SELECT c.id, c.name, c.tier, o.id AS order_id, o.total FROM customers c JOIN orders o ON o.customer_id = c.id ORDER BY c.id, o.id",
        "SELECT c.id, c.name, o.id AS order_id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id ORDER BY c.id, o.id",
        "SELECT tier, COUNT(*) AS n, SUM(id) AS s FROM customers GROUP BY tier ORDER BY tier",
        "SELECT id, total FROM orders WHERE customer_id = 1 ORDER BY id",
        "SELECT id, total FROM orders WHERE customer_id = 4 ORDER BY id",
        "SELECT id, name FROM customers WHERE tier = 2 ORDER BY id",
        "SELECT id, name FROM customers WHERE tier = 1 ORDER BY id",
        "SELECT id, name FROM customers WHERE tier IN (2, 3) ORDER BY id",
        "SELECT COUNT(*) AS n FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 2)",
        "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.total > 10) ORDER BY c.id",
      ],
    );
    expect(committed).toEqual(oracle);
    // The scope's final view equals the committed one.
    expect(scope.at(-1)).toEqual(oracle);
  });

  it("point reads inside a scope replay pending and staged history per key", async () => {
    pointReadTestHooks.attempted = 0;
    pointReadTestHooks.served = 0;
    const { scope, committed, oracle } = await twin(
      create,
      [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, label TEXT NOT NULL)",
        "INSERT INTO items (id, amount, label) VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')",
      ],
      [
        "UPDATE items SET amount = 11 WHERE id = 1",
        "DELETE FROM items WHERE id = 2",
        "INSERT INTO items (id, amount, label) VALUES (2, 22, 'b2')",
        "INSERT INTO items (id, amount, label) VALUES (4, 40, 'd')",
        "UPDATE items SET label = 'd2' WHERE id = 4",
        "INSERT INTO items (id, amount, label) VALUES (3, 33, 'c3') ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount, label = EXCLUDED.label",
        "UPDATE items SET amount = 34 WHERE id = 3",
        "DELETE FROM items WHERE id = 4",
        "UPDATE items SET label = 'a3' WHERE id = 1",
      ],
      [
        "SELECT id, amount, label FROM items WHERE id = 1",
        "SELECT id, amount, label FROM items WHERE id = 2",
        "SELECT id, amount, label FROM items WHERE id = 3",
        "SELECT id, amount, label FROM items WHERE id = 4",
        "SELECT amount FROM items WHERE id = 2",
      ],
    );
    expect(committed).toEqual(oracle);
    expect(scope.at(-1)).toEqual(oracle);
    // Each statement's view equals what the oracle would show at that point — spot-check the
    // delete→insert step: id 2 must read as the new row, never the committed one.
    expect(scope[2]?.[1]).toBe(JSON.stringify([{ id: 2, amount: 22, label: "b2" }]));
    expect(scope[1]?.[1]).toBe("[]");
    expect(scope[7]?.[3]).toBe("[]");
    expect(pointReadTestHooks.served).toBeGreaterThan(0);
  });

  it("point reads after hundreds of interleaved reads and writes stay correct", async () => {
    const db = new MinnowDatabase(await create(), { rowsPerBlock: 8 });
    await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
    await db.execute("INSERT INTO items (id, amount) VALUES (1, 0), (2, 0), (3, 0)");
    const expected = new Map<number, number | undefined>([
      [1, 0],
      [2, 0],
      [3, 0],
    ]);
    await db.write(async (tx) => {
      for (let step = 0; step < 300; step += 1) {
        const id = 1 + (step % 3);
        // A read every step forces a flush, so the history becomes a segment per step.
        const seen = (await tx.query("SELECT amount FROM items WHERE id = $1", { params: [id] }))
          .rows;
        expect(
          seen.map((row) => row.amount),
          `step ${String(step)}`,
        ).toEqual(expected.get(id) === undefined ? [] : [expected.get(id)]);
        if (step % 7 === 3) {
          await tx.execute("DELETE FROM items WHERE id = $1", [id]);
          expected.set(id, undefined);
        } else if (expected.get(id) === undefined) {
          await tx.execute("INSERT INTO items (id, amount) VALUES ($1, $2)", [id, step]);
          expected.set(id, step);
        } else {
          await tx.execute("UPDATE items SET amount = $1 WHERE id = $2", [step, id]);
          expected.set(id, step);
        }
      }
    });
    for (const [id, amount] of expected) {
      const rows = (await db.query("SELECT amount FROM items WHERE id = $1", { params: [id] }))
        .rows;
      expect(rows.map((row) => row.amount)).toEqual(amount === undefined ? [] : [amount]);
    }
    await db.close();
  });

  it("CHECK pre-images, generated columns, and defaults come from the folded set", async () => {
    const { committed, oracle, scope } = await twin(
      create,
      [
        "CREATE TABLE acct (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL DEFAULT 5, floor_ INTEGER NOT NULL DEFAULT 0, total INTEGER GENERATED ALWAYS AS (balance + floor_) STORED, CHECK (balance >= floor_))",
        "INSERT INTO acct (id, balance, floor_) VALUES (1, 10, 0)",
      ],
      [
        "INSERT INTO acct (id) VALUES (2)",
        "UPDATE acct SET floor_ = 3 WHERE id = 2",
        "UPDATE acct SET balance = 4 WHERE id = 2",
        "UPDATE acct SET floor_ = 8 WHERE id = 1",
        "UPDATE acct SET balance = 9 WHERE id = 1",
        "DELETE FROM acct WHERE id = 1",
        "INSERT INTO acct (id, balance) VALUES (1, 1)",
        "UPDATE acct SET balance = balance + 1 WHERE id = 1",
      ],
      ["SELECT id, balance, floor_, total FROM acct ORDER BY id"],
    );
    expect(committed).toEqual(oracle);
    expect(scope.at(-1)).toEqual(oracle);
  });

  it("refuses a CHECK violation against the folded pre-image, on the statement", async () => {
    const db = new MinnowDatabase(await create(), {});
    await db.execute(
      "CREATE TABLE acct (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL, floor_ INTEGER NOT NULL, CHECK (balance >= floor_))",
    );
    await db.execute("INSERT INTO acct (id, balance, floor_) VALUES (1, 10, 0)");
    await db.write(async (tx) => {
      await tx.execute("UPDATE acct SET floor_ = 8 WHERE id = 1");
      // balance 7 < folded floor 8: must fail even though the committed floor is 0.
      await expect(tx.execute("UPDATE acct SET balance = 7 WHERE id = 1")).rejects.toThrow(
        /check/i,
      );
      await tx.execute("UPDATE acct SET balance = 9 WHERE id = 1");
    });
    expect((await db.query("SELECT balance, floor_ FROM acct WHERE id = 1")).rows).toEqual([
      { balance: 9, floor_: 8 },
    ]);
    await db.close();
  });

  it("FK proofs see parents the scope inserted or deleted, without encoding the parent set", async () => {
    const store = await create();
    const db = new MinnowDatabase(store, {});
    await db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    await db.execute(
      "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent (id))",
    );
    await db.execute("INSERT INTO parent (id, name) VALUES (1, 'committed')");
    await db.write(async (tx) => {
      for (let id = 2; id < 50; id += 1) {
        await tx.execute("INSERT INTO parent (id, name) VALUES ($1, 'p')", [id]);
        await tx.execute("INSERT INTO child (id, parent_id) VALUES ($1, $2)", [id, id]);
      }
      await tx.execute("DELETE FROM child WHERE id = 10");
      await tx.execute("DELETE FROM parent WHERE id = 10");
      await expect(
        tx.execute("INSERT INTO child (id, parent_id) VALUES (100, 10)"),
      ).rejects.toThrow(/foreign key/i);
      await expect(
        tx.execute("INSERT INTO child (id, parent_id) VALUES (101, 999)"),
      ).rejects.toThrow(/foreign key/i);
      await tx.execute("INSERT INTO child (id, parent_id) VALUES (102, 1)");
      await tx.execute("INSERT INTO child (id, parent_id) VALUES (103, 49)");
    });
    const parent = await store.getTableByName("parent");
    const child = await store.getTableByName("child");
    if (parent === undefined || child === undefined) throw new Error("tables");
    const parentSegments = (await store.listTableSegmentPage(parent.id, null, 1_000)).records;
    const childSegments = (await store.listTableSegmentPage(child.id, null, 1_000)).records;
    // Alternating parent/child inserts still coalesce: the FK proof answers from the key
    // overlay, not from a read that would encode the parent set.
    expect(parentSegments.map((s) => s.kind).sort()).toEqual(["delete", "insert", "insert"]);
    expect(childSegments.length).toBeLessThanOrEqual(3);
    expect((await db.query("SELECT COUNT(*) AS n FROM child")).rows[0]?.n).toBe(49);
    await db.close();
  });

  it("delete triggers see the folded row as OLD, and the audit rows fold too", async () => {
    const { committed, oracle } = await twin(
      create,
      [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "CREATE TABLE audit (action TEXT NOT NULL, item_id INTEGER NOT NULL, amount INTEGER NOT NULL)",
        "CREATE TRIGGER items_del AFTER DELETE ON items BEGIN INSERT INTO audit (action, item_id, amount) VALUES ('del', OLD.id, OLD.amount); END",
        "INSERT INTO items (id, amount) VALUES (1, 10)",
      ],
      [
        "INSERT INTO items (id, amount) VALUES (2, 20)",
        "UPDATE items SET amount = 21 WHERE id = 2",
        "UPDATE items SET amount = 11 WHERE id = 1",
        "DELETE FROM items WHERE id = 2",
        "DELETE FROM items WHERE id = 1",
        "INSERT INTO items (id, amount) VALUES (1, 12)",
        "DELETE FROM items WHERE id IN (1, 2, 3)",
      ],
      [
        "SELECT id, amount FROM items ORDER BY id",
        "SELECT action, item_id, amount FROM audit ORDER BY item_id, amount",
      ],
    );
    expect(committed).toEqual(oracle);
  });

  it("composite keys, datetime keys, and domain columns fold like scalar keys", async () => {
    const { committed, oracle, scope } = await twin(
      create,
      [
        "CREATE TABLE receipts (shop INTEGER NOT NULL, receipt INTEGER NOT NULL, note TEXT NOT NULL, PRIMARY KEY (shop, receipt))",
        "CREATE TABLE stamps (at TIMESTAMP PRIMARY KEY, note TEXT NOT NULL)",
        "CREATE TABLE prices (id INTEGER PRIMARY KEY, price NUMERIC(10, 2) NOT NULL, qty SMALLINT NOT NULL)",
        "INSERT INTO receipts (shop, receipt, note) VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')",
        "INSERT INTO stamps (at, note) VALUES ('2026-01-01T00:00:00Z', 'x'), ('2026-01-02T00:00:00Z', 'y')",
        "INSERT INTO prices (id, price, qty) VALUES (1, 1.50, 1), (2, 2.25, 2)",
      ],
      [
        "INSERT INTO receipts (shop, receipt, note) VALUES (2, 2, 'd')",
        "UPDATE receipts SET note = 'a2' WHERE shop = 1 AND receipt = 1",
        "DELETE FROM receipts WHERE shop = 1 AND receipt = 2",
        "INSERT INTO receipts (shop, receipt, note) VALUES (1, 2, 'b2')",
        "UPDATE receipts SET note = 'd2' WHERE shop = 2 AND receipt = 2",
        "INSERT INTO stamps (at, note) VALUES ('2026-01-03T00:00:00Z', 'z')",
        "UPDATE stamps SET note = 'x2' WHERE at = '2026-01-01T00:00:00Z'",
        "DELETE FROM stamps WHERE at = '2026-01-02T00:00:00Z'",
        "UPDATE stamps SET note = 'z2' WHERE at = '2026-01-03T00:00:00Z'",
        "UPDATE prices SET price = 9.99 WHERE id = 1",
        "UPDATE prices SET qty = 7 WHERE id = 1",
        "INSERT INTO prices (id, price, qty) VALUES (3, 0.10, 3)",
        "UPDATE prices SET price = price * 2 WHERE id = 3",
        "DELETE FROM prices WHERE id = 2",
        "INSERT INTO prices (id, price, qty) VALUES (2, 5.55, 5)",
      ],
      [
        "SELECT shop, receipt, note FROM receipts ORDER BY shop, receipt",
        "SELECT at, note FROM stamps ORDER BY at",
        "SELECT id, price, qty FROM prices ORDER BY id",
        "SELECT note FROM receipts WHERE shop = 1 AND receipt = 2",
        "SELECT SUM(price) AS s FROM prices",
      ],
    );
    expect(committed).toEqual(oracle);
    expect(scope.at(-1)).toEqual(oracle);
  });

  it("INSERT … SELECT, upsert-from-select, UPDATE … FROM, and DELETE … USING read the folded set", async () => {
    const { committed, oracle, scope } = await twin(
      create,
      [
        "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "CREATE TABLE copies (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "INSERT INTO items (id, amount) VALUES (1, 10), (2, 20)",
      ],
      [
        "INSERT INTO items (id, amount) VALUES (3, 30)",
        "UPDATE items SET amount = 21 WHERE id = 2",
        "DELETE FROM items WHERE id = 1",
        "INSERT INTO copies (id, amount) SELECT id, amount FROM items",
        "UPDATE items SET amount = 5 WHERE id = 3",
        "INSERT INTO items (id, amount) SELECT id + 100, amount FROM items",
        // MERGE is refused inside a scope by design; the same rows via upsert.
        "INSERT INTO copies (id, amount) SELECT id, amount FROM items WHERE TRUE ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount",
        "UPDATE copies SET amount = copies.amount + s.amount FROM (SELECT id, amount FROM items) s WHERE s.id = copies.id",
        "INSERT INTO items (id, amount) VALUES (7, 70)",
        "DELETE FROM copies USING (SELECT id FROM items WHERE amount > 60) gone WHERE gone.id = copies.id",
        "DELETE FROM items WHERE id = 7",
        "DELETE FROM copies USING (SELECT id FROM items WHERE amount > 60) gone WHERE gone.id = copies.id",
      ],
      ["SELECT id, amount FROM items ORDER BY id", "SELECT id, amount FROM copies ORDER BY id"],
    );
    expect(committed).toEqual(oracle);
    expect(scope.at(-1)).toEqual(oracle);
  });

  it("RETURNING inside a scope reflects the folded row", async () => {
    const db = new MinnowDatabase(await create(), {});
    await db.execute(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, note TEXT NOT NULL DEFAULT 'n')",
    );
    await db.execute("INSERT INTO items (id, amount) VALUES (1, 10)");
    const returned: unknown[] = [];
    await db.write(async (tx) => {
      returned.push(
        returnedRows(
          await tx.execute(
            "INSERT INTO items (id, amount) VALUES (2, 20) RETURNING id, amount, note",
          ),
        ),
      );
      await tx.execute("UPDATE items SET amount = 21 WHERE id = 2");
      returned.push(
        returnedRows(
          await tx.execute("UPDATE items SET note = 'x' WHERE id = 2 RETURNING id, amount, note"),
        ),
      );
      await tx.execute("UPDATE items SET amount = 11 WHERE id = 1");
      returned.push(
        returnedRows(
          await tx.execute(
            "UPDATE items SET amount = amount + 1 WHERE id = 1 RETURNING id, amount, note",
          ),
        ),
      );
      await tx.execute("DELETE FROM items WHERE id = 1");
      await tx.execute("INSERT INTO items (id, amount) VALUES (1, 100)");
      returned.push(
        returnedRows(await tx.execute("DELETE FROM items WHERE id = 1 RETURNING id, amount, note")),
      );
      returned.push(returnedRows(await tx.execute("DELETE FROM items WHERE id = 1 RETURNING id")));
    });
    expect(returned).toEqual([
      [{ id: 2, amount: 20, note: "n" }],
      [{ id: 2, amount: 21, note: "x" }],
      [{ id: 1, amount: 12, note: "n" }],
      [{ id: 1, amount: 100, note: "n" }],
      [],
    ]);
    expect((await db.query("SELECT id, amount, note FROM items ORDER BY id")).rows).toEqual([
      { id: 2, amount: 21, note: "x" },
    ]);
    await db.close();
  });
});
