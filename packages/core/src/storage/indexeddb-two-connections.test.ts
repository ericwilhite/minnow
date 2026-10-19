/**
 * Two connections over one IndexedDB database and the adapter's in-memory caches (unique-key
 * membership, level-zero counts, journal, table names): no duplicate primary key or UNIQUE
 * value is admitted across interleaved writes, rekeys, racing identical inserts, and a table
 * dropped and recreated from the other connection; and an open statement transaction on one
 * connection makes the other's write wait rather than interleave or disappear.
 */
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import { ENGINE_OPTIONS, openEngine } from "./indexeddb-audit-helpers.js";
import { IndexedDbBlockStore } from "./indexeddb.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

async function ids(db: MinnowDatabase): Promise<number[]> {
  const result = await db.query("SELECT id FROM items ORDER BY id", { memoize: false });
  return (result.rows as Array<{ id: number }>).map((row) => row.id);
}

async function refuses(run: Promise<unknown>): Promise<string | undefined> {
  return run.then(
    () => undefined,
    (error: unknown) => (error as Error).name,
  );
}

describe("two IndexedDB connections", () => {
  it("never admit a duplicate primary key or UNIQUE value across connections", async () => {
    const a = await openEngine();
    const storeB = await IndexedDbBlockStore.open({ name: a.name, indexedDB: a.indexedDB });
    const b = new MinnowDatabase(storeB, ENGINE_OPTIONS);
    await a.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
    await a.db.execute("CREATE UNIQUE INDEX items_name ON items(name)");
    // Warm A's unique-key cache with a bulk load, then interleave.
    await a.db.insertBatch(
      "items",
      Array.from({ length: 200 }, (_, index) => ({ id: index, name: `n${String(index)}`, qty: 0 })),
    );
    await a.db.insertBatch("items", [{ id: 200, name: "n200", qty: 0 }]);
    // B inserts a key A has never seen; A's cache must not admit it again.
    await b.insertBatch("items", [{ id: 300, name: "n300", qty: 1 }]);
    expect(await refuses(a.db.insertBatch("items", [{ id: 300, name: "other", qty: 2 }]))).toMatch(
      /Unique/,
    );
    expect(await refuses(a.db.insertBatch("items", [{ id: 301, name: "n300", qty: 2 }]))).toMatch(
      /Unique/,
    );
    // B deletes, A may insert, then B is refused.
    await b.execute("DELETE FROM items WHERE id = 300");
    await a.db.insertBatch("items", [{ id: 300, name: "n300", qty: 3 }]);
    expect(await refuses(b.insertBatch("items", [{ id: 300, name: "n300", qty: 4 }]))).toMatch(
      /Unique/,
    );
    // A commits to another table (its cache carries over); B rekeys a row in items meanwhile.
    await a.db.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)");
    await b.execute("UPDATE items SET id = 999 WHERE id = 5");
    await a.db.execute("INSERT INTO other (id) VALUES (1)");
    expect(await refuses(a.db.insertBatch("items", [{ id: 999, name: "dup999", qty: 1 }]))).toMatch(
      /Unique/,
    );
    // A retired key stays reserved by its forward (see rekeying), on both connections.
    expect(await refuses(a.db.insertBatch("items", [{ id: 5, name: "n5b", qty: 1 }]))).toMatch(
      /Unique/,
    );
    expect(await refuses(b.insertBatch("items", [{ id: 5, name: "n5c", qty: 1 }]))).toMatch(
      /Unique/,
    );
    expect(
      (await a.db.query("SELECT id FROM items WHERE id = 5", { memoize: false })).rows,
    ).toEqual([{ id: 999 }]);
    // Racing identical inserts from both connections: exactly one wins.
    const race = await Promise.allSettled([
      a.db.insertBatch("items", [{ id: 400, name: "race", qty: 1 }]),
      b.insertBatch("items", [{ id: 400, name: "race", qty: 2 }]),
    ]);
    expect(race.filter((entry) => entry.status === "fulfilled")).toHaveLength(1);
    const fromA = await ids(a.db);
    expect(await ids(b)).toEqual(fromA);
    expect(new Set(fromA).size).toBe(fromA.length);
    expect(
      (
        await a.db.query("SELECT COUNT(*) AS n, COUNT(DISTINCT name) AS d FROM items", {
          memoize: false,
        })
      ).rows,
    ).toEqual([{ n: fromA.length, d: fromA.length }]);
    expect((await a.store.checkIntegrity()).issues).toEqual([]);
    // Drop and recreate the table from B; A's stale name and key caches must not leak old keys.
    await b.execute("DROP TABLE items");
    await b.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
    await a.db.insertBatch("items", [{ id: 5, name: "fresh", qty: 1 }]);
    await a.db.insertBatch("items", [{ id: 300, name: "n300", qty: 1 }]);
    expect(await ids(b)).toEqual([5, 300]);
    expect(await refuses(b.insertBatch("items", [{ id: 5, name: "x", qty: 1 }]))).toMatch(/Unique/);
    expect((await a.store.checkIntegrity()).issues).toEqual([]);
    await a.db.close();
    await b.close();
    a.store.close();
    storeB.close();
  });

  it("an open statement transaction on one connection makes the other's write wait, not vanish", async () => {
    const a = await openEngine();
    const storeB = await IndexedDbBlockStore.open({ name: a.name, indexedDB: a.indexedDB });
    const b = new MinnowDatabase(storeB, ENGINE_OPTIONS);
    await a.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)");
    await a.db.insertBatch(
      "items",
      Array.from({ length: 20 }, (_, index) => ({ id: index, name: `n${String(index)}`, qty: 0 })),
    );
    await a.db.execute("BEGIN");
    await a.db.execute("INSERT INTO items (id, name, qty) VALUES (100, 'a', 1)");
    const pendingB = b.execute("INSERT INTO items (id, name, qty) VALUES (200, 'b', 1)");
    await new Promise((resolve) => setTimeout(resolve, 100));
    await a.db.execute("UPDATE items SET qty = 7 WHERE id = 3");
    await a.db.execute("COMMIT");
    await pendingB;
    const expected = [...Array.from({ length: 20 }, (_, index) => index), 100, 200];
    expect(await ids(a.db)).toEqual(expected);
    expect(await ids(b)).toEqual(expected);
    expect((await b.query("SELECT qty FROM items WHERE id = 3", { memoize: false })).rows).toEqual([
      { qty: 7 },
    ]);
    expect((await a.store.checkIntegrity()).issues).toEqual([]);
    await a.db.close();
    await b.close();
    a.store.close();
    storeB.close();
  });
});
