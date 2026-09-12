/**
 * Statement-level failure inside a write scope.
 *
 * The contract (engine/transactions.mdx): each statement validates, registers its unique keys,
 * and proves its foreign keys on the spot — a failure fails that statement and leaves the scope
 * usable — and a unique violation fails on the statement whether the conflicting row was
 * committed before the transaction or staged earlier in it. Each case runs a statement that
 * must be refused, then keeps writing in the same scope and checks what committed.
 *
 * Also pinned here: a read-first statement that fails validation after earlier statements
 * buffered work (the read used to move the staged-work counter before the throw, which
 * poisoned the scope), and tx.query / tx.execute externalizing NUMERIC like db.query does.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { UniqueConstraintError } from "./errors.js";

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

async function fixture(create: () => Promise<BlockStore>): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(await create(), {});
  await db.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, code INTEGER)",
  );
  await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");
  await db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
  await db.execute(
    "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parent (id))",
  );
  await db.execute(
    "INSERT INTO items (id, amount, code) VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3)",
  );
  await db.execute("INSERT INTO parent (id) VALUES (1)");
  return db;
}

describe.each(implementations)("scope failure semantics on $name", ({ create }) => {
  it("a read-first statement that fails validation after buffered statements leaves the scope usable", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      // Keyed constant update: joins the write set without a read.
      await tx.execute("UPDATE items SET amount = 12 WHERE id = 1");
      // General path: reads first (staging the pending set), then fails validation.
      await expect(tx.execute("UPDATE items SET amount = 'abc' WHERE id = 2")).rejects.toThrow(
        /safe integer/,
      );
      await tx.execute("UPDATE items SET amount = 13 WHERE id = 3");
    });
    expect((await db.query("SELECT id, amount FROM items ORDER BY id")).rows).toEqual([
      { id: 1, amount: 12 },
      { id: 2, amount: 20 },
      { id: 3, amount: 13 },
    ]);
    await db.close();
  });

  it("an expression UPDATE that fails at evaluation after a pending insert leaves the scope usable", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (4, 4, NULL)");
      await expect(
        tx.execute("UPDATE items SET amount = amount + 'x' WHERE id = 1"),
      ).rejects.toThrow();
      await tx.execute("UPDATE items SET amount = 13 WHERE id = 3");
    });
    expect((await db.query("SELECT id, amount FROM items ORDER BY id")).rows).toEqual([
      { id: 1, amount: 10 },
      { id: 2, amount: 20 },
      { id: 3, amount: 13 },
      { id: 4, amount: 4 },
    ]);
    await db.close();
  });

  it("a FOREIGN KEY violation fails its statement and leaves the scope usable", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await expect(tx.execute("INSERT INTO child (id, parent_id) VALUES (1, 999)")).rejects.toThrow(
        /foreign key/i,
      );
      await tx.execute("INSERT INTO child (id, parent_id) VALUES (2, 1)");
    });
    expect((await db.query("SELECT id FROM child")).rows).toEqual([{ id: 2 }]);
    await db.close();
  });

  it("tx.insertBatch of a committed duplicate key fails on the statement, not at COMMIT", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await expect(
        tx.insertBatch("items", [{ id: 1, amount: 5, code: null }]),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await tx.insertBatch("items", [{ id: 8, amount: 5, code: null }]);
    });
    expect(
      (await db.query("SELECT id, amount FROM items WHERE id IN (1, 8) ORDER BY id")).rows,
    ).toEqual([
      { id: 1, amount: 10 },
      { id: 8, amount: 5 },
    ]);
    await db.close();
  });

  it("an INSERT whose key or UNIQUE term is held by a row the scope inserted fails on the statement", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (4, 4, 6)");
      await expect(
        tx.execute("INSERT INTO items (id, amount, code) VALUES (4, 44, NULL)"),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await expect(tx.insertBatch("items", [{ id: 5, amount: 5, code: 6 }])).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (5, 5, 7)");
    });
    expect(
      (await db.query("SELECT id, amount, code FROM items WHERE id IN (4, 5) ORDER BY id")).rows,
    ).toEqual([
      { id: 4, amount: 4, code: 6 },
      { id: 5, amount: 5, code: 7 },
    ]);
    await db.close();
  });

  it("a secondary UNIQUE conflict with a committed row fails on the statement, not at COMMIT", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await expect(
        tx.execute("INSERT INTO items (id, amount, code) VALUES (9, 5, 1)"),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (8, 5, 8)");
    });
    expect((await db.query("SELECT id FROM items WHERE code IN (1, 8) ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 8 },
    ]);
    await db.close();
  });

  it("an in-batch duplicate UNIQUE term fails its statement and leaves the scope usable", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await expect(
        tx.upsertBatch("items", [
          { id: 4, amount: 5, code: 6 },
          { id: 5, amount: 5, code: 6 },
        ]),
      ).rejects.toThrow(/UNIQUE index items_code has a duplicate key/);
      await tx.insertBatch("items", [{ id: 8, amount: 5, code: 8 }]);
    });
    expect((await db.query("SELECT id FROM items WHERE id >= 4 ORDER BY id")).rows).toEqual([
      { id: 8 },
    ]);
    await db.close();
  });

  it("tx.query and tx.execute externalize NUMERIC values like db.query does", async () => {
    const db = new MinnowDatabase(await create(), {});
    await db.execute("CREATE TABLE prices (id INTEGER PRIMARY KEY, price NUMERIC(10, 2) NOT NULL)");
    await db.execute("INSERT INTO prices (id, price) VALUES (1, 1.50)");
    const seen: unknown[] = [];
    await db.write(async (tx) => {
      seen.push((await tx.query("SELECT price FROM prices WHERE id = 1")).rows[0]?.price);
      seen.push((await tx.query("SELECT SUM(price) AS s FROM prices")).rows[0]?.s);
      const executed = await tx.execute("SELECT price FROM prices WHERE id = 1");
      seen.push(executed.kind === "rows" ? executed.result.rows[0]?.price : executed);
    });
    expect(seen).toEqual(["1.50", "1.50", "1.50"]);
    expect((await db.query("SELECT price FROM prices WHERE id = 1")).rows[0]?.price).toBe("1.50");
    await db.close();
  });
});
