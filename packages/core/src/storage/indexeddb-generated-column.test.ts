/**
 * A table with a stored generated column persists on IndexedDB: the catalog validator accepts
 * the column's `generatedValue`, so the table can be created, written, read, and reopened.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { IndexedDbBlockStore } from "./indexeddb.js";

describe("IndexedDB generated columns", () => {
  it("creates, writes, reads, and reopens a table with a stored generated column", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = async (database: MinnowDatabase) =>
      (await database.query("SELECT a, b FROM t ORDER BY a", { memoize: false })).rows;

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, { autoCompact: false });
    await database.execute(
      "CREATE TABLE t (a INTEGER PRIMARY KEY, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    );
    await database.execute("INSERT INTO t (a) VALUES (1), (2)");
    expect(await rows(database)).toEqual([
      { a: 1, b: 2 },
      { a: 2, b: 4 },
    ]);
    expect((await store.checkIntegrity()).issues).toEqual([]);
    await database.close();
    store.close();

    const reopened = await IndexedDbBlockStore.open({ name, indexedDB });
    const reopenedDatabase = new MinnowDatabase(reopened, { autoCompact: false });
    expect(await rows(reopenedDatabase)).toEqual([
      { a: 1, b: 2 },
      { a: 2, b: 4 },
    ]);
    await reopenedDatabase.execute("INSERT INTO t (a) VALUES (3)");
    expect(await rows(reopenedDatabase)).toEqual([
      { a: 1, b: 2 },
      { a: 2, b: 4 },
      { a: 3, b: 6 },
    ]);
    expect((await reopened.checkIntegrity()).issues).toEqual([]);
    await reopenedDatabase.close();
    reopened.close();
  });
});
