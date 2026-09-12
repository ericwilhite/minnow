/**
 * An UPDATE inside a write scope that moves a row onto a UNIQUE index term another row holds
 * fails on that statement and leaves the scope usable, whether the other row is committed or
 * staged earlier in the scope. A swap inside one statement, and a move onto a term the scope
 * itself retired, are not conflicts. Commit still re-validates atomically.
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
  await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code INTEGER)");
  await db.execute("CREATE UNIQUE INDEX items_code ON items (code)");
  await db.execute("INSERT INTO items (id, code) VALUES (1, 1), (2, 2), (3, 3)");
  return db;
}

describe.each(implementations)("scoped UNIQUE updates on $name", ({ create }) => {
  it("an update onto a committed row's term fails the statement, not the scope", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await expect(tx.execute("UPDATE items SET code = 2 WHERE id = 1")).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
      await expect(
        tx.updateBatch("items", { keys: [3], changes: { code: [1] } }),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await tx.execute("UPDATE items SET code = 30 WHERE id = 3");
    });
    expect((await db.query("SELECT id, code FROM items ORDER BY id")).rows).toEqual([
      { id: 1, code: 1 },
      { id: 2, code: 2 },
      { id: 3, code: 30 },
    ]);
    await db.close();
  });

  it("an update onto a term another statement of the scope took fails the statement", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await tx.execute("INSERT INTO items (id, code) VALUES (4, 40)");
      await expect(tx.execute("UPDATE items SET code = 40 WHERE id = 1")).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
      await tx.execute("UPDATE items SET code = 41 WHERE id = 1");
    });
    expect((await db.query("SELECT id, code FROM items ORDER BY id")).rows).toEqual([
      { id: 1, code: 41 },
      { id: 2, code: 2 },
      { id: 3, code: 3 },
      { id: 4, code: 40 },
    ]);
    await db.close();
  });

  it("a swap within one statement and a move onto a term the scope retired both commit", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await tx.updateBatch("items", { keys: [1, 2], changes: { code: [2, 1] } });
      await tx.execute("UPDATE items SET code = NULL WHERE id = 3");
      await tx.execute("INSERT INTO items (id, code) VALUES (5, 3)");
    });
    expect((await db.query("SELECT id, code FROM items ORDER BY id")).rows).toEqual([
      { id: 1, code: 2 },
      { id: 2, code: 1 },
      { id: 3, code: null },
      { id: 5, code: 3 },
    ]);
    await db.close();
  });

  it("a batch that repeats a term fails the statement and leaves the scope usable", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await expect(
        tx.updateBatch("items", { keys: [1, 2], changes: { code: [9, 9] } }),
      ).rejects.toThrow(/duplicate key/);
      await tx.execute("UPDATE items SET code = 9 WHERE id = 1");
    });
    expect((await db.query("SELECT code FROM items WHERE id = 1")).rows).toEqual([{ code: 9 }]);
    await db.close();
  });
});
