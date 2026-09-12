import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { OpfsBlockStore } from "../storage/opfs/index.js";
import { MinnowDatabase } from "./database.js";
import { column, schema, table } from "./schema.js";

/**
 * A write that stages more blocks than one storage batch holds: the stager flushes the first
 * batch into the transaction's local deferred set, and the single-shot commit must carry those
 * blocks too. An update of 100 columns used to fail with "references block outside its
 * transaction" while the same update inside a scope worked.
 */

const implementations = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    create: async (): Promise<BlockStore> =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

const WIDTH = 100;

describe.each(implementations)("writes wider than one storage batch on $name", ({ create }) => {
  it("commits an autocommit update, upsert, and delete touching every column", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    const columns: Record<string, ReturnType<typeof column.string>> = {};
    for (let index = 0; index < WIDTH; index += 1) columns[`c${String(index)}`] = column.string();
    await database.migrate(schema([table("wide", { id: column.number().unique(), ...columns })]));
    const row = (id: number, value: string): Record<string, string | number> => {
      const values: Record<string, string | number> = { id };
      for (let index = 0; index < WIDTH; index += 1) values[`c${String(index)}`] = value;
      return values;
    };
    await database.insertBatch("wide", [row(1, "a"), row(2, "a"), row(3, "a")]);
    const changes: Record<string, string[]> = {};
    for (let index = 0; index < WIDTH; index += 1) changes[`c${String(index)}`] = ["b", "b"];
    const updated = await database.updateBatch("wide", { keys: [1, 2], changes });
    expect(updated.updatedRowCount).toBe(2);
    const upserted = await database.upsertBatch("wide", [row(2, "c"), row(4, "c")]);
    expect(upserted).toMatchObject({ insertedRowCount: 1, updatedRowCount: 1 });
    await database.deleteBatch("wide", { keys: [3] });
    const rows = (
      await database.query('SELECT "id", "c0", "c99" FROM "wide" ORDER BY "id"', {
        memoize: false,
      })
    ).rows;
    expect(rows).toEqual([
      { id: 1, c0: "b", c99: "b" },
      { id: 2, c0: "c", c99: "c" },
      { id: 4, c0: "c", c99: "c" },
    ]);
    await database.close();
    store.close();
  });
});

describe("wide tables with wide histories stay inside the query budget", () => {
  it("reads every column after full-width update and upsert passes", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    const columns: Record<string, ReturnType<typeof column.string>> = {};
    for (let index = 0; index < WIDTH; index += 1) columns[`c${String(index)}`] = column.string();
    await database.migrate(schema([table("wide", { id: column.number().unique(), ...columns })]));
    const rows = (tag: string): Array<Record<string, string | number>> =>
      Array.from({ length: 5_000 }, (_, id) => {
        const values: Record<string, string | number> = { id };
        for (let index = 0; index < WIDTH; index += 1) {
          values[`c${String(index)}`] = `${tag}-${String(id)}-${String(index)}`;
        }
        return values;
      });
    await database.insertBatch("wide", rows("a"));
    const updated = rows("b");
    const changes: Record<string, string[]> = {};
    for (let index = 0; index < WIDTH; index += 1) {
      changes[`c${String(index)}`] = updated.map((row) => row[`c${String(index)}`] as string);
    }
    await database.updateBatch("wide", { keys: updated.map((row) => row.id as number), changes });
    await database.upsertBatch("wide", rows("c"));
    await database.upsertBatch("wide", rows("d"));
    const keys = Array.from({ length: 1_024 }, (_, index) => index);
    const placeholders = keys.map((_, index) => `$${String(index + 1)}`).join(", ");
    const inList = await database.query(`SELECT * FROM "wide" WHERE "id" IN (${placeholders})`, {
      params: keys,
      memoize: false,
    });
    expect(inList.rows).toHaveLength(1_024);
    expect(inList.rows[7]).toMatchObject({ id: 7, c0: "d-7-0", c99: "d-7-99" });
    const scanned = await database.query('SELECT * FROM "wide" WHERE "c1" <> \'never\'', {
      memoize: false,
    });
    expect(scanned.rows).toHaveLength(5_000);
    expect(scanned.rows.every((row) => String(row.c50).startsWith("d-"))).toBe(true);
    await database.close();
  });
});
