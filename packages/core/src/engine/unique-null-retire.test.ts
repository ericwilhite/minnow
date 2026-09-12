/**
 * Setting a UNIQUE-indexed column to NULL retires the row's index term.
 *
 * The post-image term of a unique mutation used to fall through to the old value on an explicit
 * NULL, so the term was removed and re-added under the same row: no other row could take the
 * value afterwards, and inside one scope a valid sequence was refused at COMMIT. These pin the
 * fixed behaviour on both stores, through SQL, the batch API, one scope, and an upsert.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

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
  await db.execute(
    "INSERT INTO items (id, amount, code) VALUES (1, 10, 1), (2, 20, 2), (3, 30, 3)",
  );
  return db;
}

describe.each(implementations)("NULL assignment retires a UNIQUE term ($name)", ({ create }) => {
  it("autocommit SQL: after SET code = NULL, another row can take the old value", async () => {
    const db = await fixture(create);
    await db.execute("UPDATE items SET code = NULL WHERE id = 3");
    expect((await db.query("SELECT id FROM items WHERE code = 3")).rows).toEqual([]);
    await db.execute("INSERT INTO items (id, amount, code) VALUES (9, 1, 3)");
    expect((await db.query("SELECT id, code FROM items WHERE code = 3")).rows).toEqual([
      { id: 9, code: 3 },
    ]);
    await db.close();
  });

  it("autocommit batch: after updateBatch to NULL, another row can take the old value", async () => {
    const db = await fixture(create);
    await db.updateBatch("items", { keys: [3], changes: { code: [null] } });
    await db.insertBatch("items", [{ id: 9, amount: 1, code: 3 }]);
    expect((await db.query("SELECT id, code FROM items WHERE code = 3")).rows).toEqual([
      { id: 9, code: 3 },
    ]);
    await db.close();
  });

  it("in one scope: NULL the value, give it to another row, then change the first row again", async () => {
    const db = await fixture(create);
    await db.write(async (tx) => {
      await tx.execute("UPDATE items SET code = NULL WHERE id = 3");
      await tx.execute("INSERT INTO items (id, amount, code) VALUES (9, 1, 3)");
      await tx.execute("UPDATE items SET code = 30 WHERE id = 3");
    });
    expect(
      (await db.query("SELECT id, code FROM items WHERE id IN (3, 9) ORDER BY id")).rows,
    ).toEqual([
      { id: 3, code: 30 },
      { id: 9, code: 3 },
    ]);
    await db.close();
  });

  it("upsert to NULL via EXCLUDED also retires the term", async () => {
    const db = await fixture(create);
    await db.execute(
      "INSERT INTO items (id, amount, code) VALUES (3, 30, NULL) ON CONFLICT (id) DO UPDATE SET code = EXCLUDED.code",
    );
    await db.execute("INSERT INTO items (id, amount, code) VALUES (9, 1, 3)");
    expect((await db.query("SELECT id FROM items WHERE code = 3")).rows).toEqual([{ id: 9 }]);
    await db.close();
  });
});
