import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { UniqueConstraintError } from "./errors.js";
import { column, schema, table } from "./schema.js";

/**
 * Plain inserts inside a write scope coalesce into one segment per table instead of one per
 * statement, so a loop of single-row inserts never exhausts the transaction's block budget and
 * costs what one batch costs — while every read, savepoint, upsert, and unique check inside the
 * scope still sees exactly what the scope inserted.
 */

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
];

const WIDTH = 100;

function wideColumns(): Record<string, ReturnType<typeof column.string>> {
  const columns: Record<string, ReturnType<typeof column.string>> = {};
  for (let index = 0; index < WIDTH; index += 1) columns[`c${String(index)}`] = column.string();
  return columns;
}

function wideRow(id: number): Record<string, string | number> {
  const row: Record<string, string | number> = { id };
  for (let index = 0; index < WIDTH; index += 1) row[`c${String(index)}`] = `v${String(id)}`;
  return row;
}

function insertSql(row: Record<string, unknown>): { sql: string; params: unknown[] } {
  const names = Object.keys(row);
  return {
    sql: `INSERT INTO "wide" (${names.map((name) => `"${name}"`).join(", ")}) VALUES (${names
      .map((_, index) => `$${String(index + 1)}`)
      .join(", ")})`,
    params: Object.values(row),
  };
}

describe.each(implementations)("scope insert coalescing on $name", ({ create }) => {
  it("stages hundreds of single-row inserts on a wide table as one segment", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("wide", { id: column.number().unique(), ...wideColumns() })]),
    );
    const staged: Array<string | null> = [];
    await database.write(async (tx) => {
      for (let id = 0; id < 150; id += 1) {
        const { sql, params } = insertSql(wideRow(id));
        await tx.execute(sql, params as never);
      }
      for (let id = 150; id < 200; id += 1) {
        staged.push((await tx.insertBatch("wide", [wideRow(id)])).segmentId);
      }
    });
    expect(staged.every((segmentId) => segmentId === null)).toBe(true);
    expect((await database.query('SELECT count(*) AS n FROM "wide"')).rows[0]?.n).toBe(200);
    const record = await store.getTableByName("wide");
    if (record === undefined) throw new Error("Expected the wide table");
    const segments = await store.listTableSegmentPage(record.id, null, 10);
    expect(segments.records).toHaveLength(1);
    await database.close();
  });

  it("lets reads, updates, deletes, and upserts inside the scope see buffered rows", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([
        table("items", { id: column.number().unique(), value: column.string() }),
        table("log", { note: column.string() }),
      ]),
    );
    await database.write(async (tx) => {
      for (let id = 1; id <= 5; id += 1) {
        await tx.insertBatch("items", [{ id, value: `v${String(id)}` }]);
      }
      // A keyless table takes the pre-stage read fast path unless buffers are accounted for.
      await tx.insertBatch("log", [{ note: "a" }, { note: "b" }]);
      expect((await tx.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(5);
      expect((await tx.query('SELECT count(*) AS n FROM "log"')).rows[0]?.n).toBe(2);
      await tx.insertBatch("items", [{ id: 6, value: "v6" }]);
      await tx.execute('UPDATE "items" SET "value" = \'six\' WHERE "id" = 6');
      await tx.execute('DELETE FROM "items" WHERE "id" = 1');
      await tx.insertBatch("items", [{ id: 7, value: "v7" }]);
      await tx.upsertBatch("items", [{ id: 7, value: "seven" }]);
      await tx.insertBatch("items", [{ id: 1, value: "one again" }]);
    });
    const rows = (await database.query('SELECT "id", "value" FROM "items" ORDER BY "id"')).rows;
    expect(rows).toEqual([
      { id: 1, value: "one again" },
      { id: 2, value: "v2" },
      { id: 3, value: "v3" },
      { id: 4, value: "v4" },
      { id: 5, value: "v5" },
      { id: 6, value: "six" },
      { id: 7, value: "seven" },
    ]);
    await database.close();
  });

  it("fails a duplicate key on the statement that caused it, without staging anything", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.insert("items", { id: 1, value: "committed" });
    const outcome = await database
      .write(async (tx) => {
        await tx.insertBatch("items", [{ id: 2, value: "staged" }]);
        await expect(
          tx.execute('INSERT INTO "items" ("id", "value") VALUES (2, \'dup\')'),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        await expect(
          tx.execute('INSERT INTO "items" ("id", "value") VALUES (1, \'dup\')'),
        ).rejects.toBeInstanceOf(UniqueConstraintError);
        await expect(tx.insertBatch("items", [{ id: 2, value: "dup" }])).rejects.toBeInstanceOf(
          UniqueConstraintError,
        );
        // A key the scope deleted is free again.
        await tx.execute('DELETE FROM "items" WHERE "id" = 1');
        await tx.execute('INSERT INTO "items" ("id", "value") VALUES (1, \'reused\')');
        return "done";
      })
      .catch((error: unknown) => error);
    // Each refusal came before the statement registered anything, so the scope stayed usable.
    expect(outcome).toMatchObject({ result: "done" });
    expect((await database.query('SELECT "id", "value" FROM "items" ORDER BY "id"')).rows).toEqual([
      { id: 1, value: "reused" },
      { id: 2, value: "staged" },
    ]);
    await database.close();
  });

  it("keeps rows inserted before a savepoint and drops those after a rollback to it", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.execute("BEGIN");
    await database.execute('INSERT INTO "items" ("id", "value") VALUES (1, \'kept\')');
    await database.execute('INSERT INTO "items" ("id", "value") VALUES (2, \'kept\')');
    await database.execute("SAVEPOINT s");
    await database.execute('INSERT INTO "items" ("id", "value") VALUES (3, \'dropped\')');
    await database.execute('INSERT INTO "items" ("id", "value") VALUES (4, \'dropped\')');
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(4);
    await database.execute("ROLLBACK TO SAVEPOINT s");
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(2);
    await database.execute('INSERT INTO "items" ("id", "value") VALUES (3, \'after\')');
    await database.execute("COMMIT");
    expect((await database.query('SELECT "id", "value" FROM "items" ORDER BY "id"')).rows).toEqual([
      { id: 1, value: "kept" },
      { id: 2, value: "kept" },
      { id: 3, value: "after" },
    ]);
    await database.close();
  });

  it("publishes nothing from a scope that throws after buffering", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await expect(
      database.write(async (tx) => {
        for (let id = 1; id <= 20; id += 1) await tx.insertBatch("items", [{ id, value: "x" }]);
        throw new Error("abandon");
      }),
    ).rejects.toThrow("abandon");
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(0);
    await database.close();
  });
});
