import { IDBFactory } from "fake-indexeddb";
import { expect, it } from "vitest";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { MinnowDatabase } from "./database.js";

it("creates a sequence with valid durable key metadata in IndexedDB", async () => {
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  try {
    await expect(database.execute("CREATE SEQUENCE order_ids")).resolves.toEqual({
      kind: "create-sequence",
      name: "order_ids",
    });
    await expect(database.query("SELECT NEXTVAL('order_ids') AS id")).resolves.toMatchObject({
      rows: [{ id: 1 }],
    });

    const sequence = (await store.listTables()).find(
      (table) => table.sequence?.name === "order_ids",
    );
    expect(sequence?.uniqueKeyColumnId).toBe(sequence?.sequence?.columnId);
  } finally {
    await database.close();
    store.close();
  }
});

it("normalizes the legacy sequence record shape without a unique-key field", async () => {
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const columnId = "legacy-sequence-column";
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  try {
    await store.addTable({
      id: "legacy-sequence-table",
      name: "\u0000minnow_sequence:legacy_ids",
      columns: [
        {
          id: columnId,
          name: "value",
          type: "number",
          integer: true,
          nullable: false,
          hidden: true,
          defaultValue: { kind: "autoincrement" },
        },
      ],
      managed: false,
      revision: 0,
      sequence: { name: "legacy_ids", start: 1, columnId },
      createdAt: "2026-01-01T00:00:00.000Z",
    });

    expect((await store.getTable("legacy-sequence-table"))?.uniqueKeyColumnId).toBe(columnId);
    await expect(database.query("SELECT NEXTVAL('legacy_ids') AS id")).resolves.toMatchObject({
      rows: [{ id: 1 }],
    });
  } finally {
    await database.close();
    store.close();
  }
});
