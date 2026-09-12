import { IDBFactory } from "fake-indexeddb";
import { afterEach, describe, expect, it } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { MissingKeyError, UniqueConstraintError } from "./errors.js";
import { column, schema, table } from "./schema.js";
import { pointReadTestHooks } from "./point-read.js";
import { scopeWriteSetTestHooks } from "./scope-write-set.js";

/**
 * Every plain mutation inside a write scope joins a per-table write set instead of encoding a
 * segment of its own: inserts, updates, deletes, and unguarded upserts of the same key fold
 * into one net effect, and the set becomes at most a handful of segments per table when the
 * scope reads the table, takes a savepoint, or commits. Keyed lookups the engine makes for its
 * own checks — upsert classification, pre-images for CHECK constraints — answer from the write
 * set and the committed snapshot, so they never force the encoding either.
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

async function segmentKinds(store: BlockStore, tableName: string): Promise<string[]> {
  const record = await store.getTableByName(tableName);
  if (record === undefined) throw new Error(`Expected the ${tableName} table`);
  const page = await store.listTableSegmentPage(record.id, null, 1_000);
  return page.records.map((segment) => segment.kind).sort();
}

afterEach(() => {
  scopeWriteSetTestHooks.budgetBytes = scopeWriteSetTestHooks.defaultBudgetBytes;
});

describe.each(implementations)("scope write set on $name", ({ create }) => {
  it("stages hundreds of single-row updates on a wide table as one update segment", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("wide", { id: column.number().unique(), ...wideColumns() })]),
    );
    await database.insertBatch(
      "wide",
      Array.from({ length: 300 }, (_, id) => wideRow(id)),
    );
    const staged: Array<string | null> = [];
    await database.write(async (tx) => {
      for (let id = 0; id < 150; id += 1) {
        await tx.execute('UPDATE "wide" SET "c7" = $1 WHERE "id" = $2', [`u${String(id)}`, id]);
      }
      for (let id = 150; id < 300; id += 1) {
        staged.push(
          (await tx.updateBatch("wide", { keys: [id], changes: { c7: [`u${String(id)}`] } }))
            .segmentId,
        );
      }
    });
    expect(staged.every((segmentId) => segmentId === null)).toBe(true);
    // Two hundred committed base rows, and one update segment carrying every key.
    expect(await segmentKinds(store, "wide")).toEqual(["insert", "update"]);
    const rows = (await database.query('SELECT "id", "c7", "c8" FROM "wide" ORDER BY "id"')).rows;
    expect(rows).toHaveLength(300);
    expect(
      rows.every((row) => row.c7 === `u${String(row.id)}` && row.c8 === `v${String(row.id)}`),
    ).toBe(true);
    await database.close();
  });

  it("stages a loop of single-key deletes as one delete segment", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.insertBatch(
      "items",
      Array.from({ length: 200 }, (_, id) => ({ id, value: `v${String(id)}` })),
    );
    await database.write(async (tx) => {
      for (let id = 0; id < 100; id += 1) {
        await tx.execute('DELETE FROM "items" WHERE "id" = $1', [id]);
      }
      for (let id = 100; id < 150; id += 1) await tx.deleteBatch("items", { keys: [id] });
      // A key that never existed deletes nothing and does not fail the statement.
      const missing = await tx.execute('DELETE FROM "items" WHERE "id" = $1', [9_999]);
      expect(missing).toMatchObject({ kind: "delete", rowCount: 0 });
    });
    expect(await segmentKinds(store, "items")).toEqual(["delete", "insert"]);
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(50);
    await database.close();
  });

  it("folds inserts, updates, deletes, and upserts of one key into its net effect", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.insertBatch("items", [
      { id: 1, value: "committed" },
      { id: 2, value: "committed" },
      { id: 3, value: "committed" },
    ]);
    await database.write(async (tx) => {
      // Inserted, then patched twice: one insert row with the final values.
      await tx.insertBatch("items", [{ id: 10, value: "a" }]);
      await tx.updateBatch("items", { keys: [10], changes: { value: ["b"] } });
      await tx.execute('UPDATE "items" SET "value" = $1 WHERE "id" = $2', ["c", 10]);
      // Inserted then deleted: nothing survives.
      await tx.insertBatch("items", [{ id: 11, value: "gone" }]);
      await tx.deleteBatch("items", { keys: [11] });
      // Deleted then inserted again: the committed row is replaced in place.
      await tx.deleteBatch("items", { keys: [1] });
      await tx.insertBatch("items", [{ id: 1, value: "replaced" }]);
      // Updated then deleted: only the delete remains.
      await tx.updateBatch("items", { keys: [2], changes: { value: ["never seen"] } });
      await tx.deleteBatch("items", { keys: [2] });
      // Upserted over a committed row, then patched.
      await tx.upsertBatch("items", [{ id: 3, value: "upserted" }]);
      await tx.updateBatch("items", { keys: [3], changes: { value: ["patched"] } });
      // A deleted key cannot be updated, and the scope stays usable after the refusal.
      await expect(
        tx.updateBatch("items", { keys: [2], changes: { value: ["x"] } }),
      ).rejects.toBeInstanceOf(MissingKeyError);
      await tx.insertBatch("items", [{ id: 12, value: "after refusal" }]);
    });
    expect((await database.query('SELECT "id", "value" FROM "items" ORDER BY "id"')).rows).toEqual([
      { id: 1, value: "replaced" },
      { id: 3, value: "patched" },
      { id: 10, value: "c" },
      { id: 12, value: "after refusal" },
    ]);
    // Base rows, then one insert, one upsert, and one delete segment for the whole scope.
    expect(await segmentKinds(store, "items")).toEqual(["delete", "insert", "insert", "upsert"]);
    await database.close();
  });

  it("classifies guarded upserts against the write set without encoding it", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([
        table("stock", {
          sku: column.string().unique(),
          qty: column.number(),
          revision: column.number(),
        }),
      ]),
    );
    await database.insertBatch("stock", [
      { sku: "old", qty: 1, revision: 5 },
      { sku: "fresh", qty: 1, revision: 9 },
    ]);
    const results: Array<{ rowCount: number; skippedRowCount: number; segmentId: string | null }> =
      [];
    await database.write(async (tx) => {
      for (let index = 0; index < 100; index += 1) {
        results.push(
          await tx.upsertBatch("stock", [{ sku: `s${String(index)}`, qty: index, revision: 1 }], {
            conflictWhere: { column: "revision", operator: "<", value: 7 },
          }),
        );
      }
      // The guard reads the scope's own staged revision, not the committed one.
      results.push(
        await tx.upsertBatch("stock", [{ sku: "s5", qty: 500, revision: 2 }], {
          conflictWhere: { column: "revision", operator: "<", value: 2 },
        }),
      );
      results.push(
        await tx.upsertBatch(
          "stock",
          [
            { sku: "old", qty: 2, revision: 6 },
            { sku: "fresh", qty: 2, revision: 10 },
          ],
          { conflictWhere: { column: "revision", operator: "<", value: 7 } },
        ),
      );
    });
    expect(results.every((result) => result.segmentId === null)).toBe(true);
    expect(results.at(-2)).toMatchObject({ rowCount: 1, skippedRowCount: 0 });
    expect(results.at(-1)).toMatchObject({ rowCount: 1, skippedRowCount: 1 });
    expect(await segmentKinds(store, "stock")).toEqual(["insert", "upsert"]);
    const rows = (
      await database.query(
        'SELECT "sku", "qty", "revision" FROM "stock" WHERE "sku" IN ($1, $2, $3) ORDER BY "sku"',
        { params: ["fresh", "old", "s5"] },
      )
    ).rows;
    expect(rows).toEqual([
      { sku: "fresh", qty: 1, revision: 9 },
      { sku: "old", qty: 2, revision: 6 },
      { sku: "s5", qty: 500, revision: 2 },
    ]);
    await database.close();
  });

  it("keeps coalescing after a read of the table, answering pre-images from the mirror", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.execute(
      'CREATE TABLE "accounts" ("id" INTEGER PRIMARY KEY, "balance" INTEGER CHECK ("balance" >= 0), "note" TEXT)',
    );
    await database.insertBatch(
      "accounts",
      Array.from({ length: 100 }, (_, id) => ({ id, balance: 10, note: "" })),
    );
    await database.write(async (tx) => {
      // CHECK constraints need each row's pre-image; with nothing staged that is a committed read.
      for (let id = 0; id < 50; id += 1) {
        await tx.updateBatch("accounts", { keys: [id], changes: { balance: [id + 1] } });
      }
      // A read of the table encodes what waits; later lookups see those rows through the mirror.
      const seen = await tx.query('SELECT "balance" FROM "accounts" WHERE "id" = 7');
      expect(seen.rows[0]?.balance).toBe(8);
      for (let id = 0; id < 100; id += 1) {
        await tx.updateBatch("accounts", { keys: [id], changes: { note: ["touched"] } });
      }
      // A post-image the constraint refuses fails the statement that wrote it.
      await expect(
        tx.updateBatch("accounts", { keys: [3], changes: { balance: [-1] } }),
      ).rejects.toThrow(/check/i);
    });
    // Base rows, the segment the read forced, and one segment for the second loop.
    expect(await segmentKinds(store, "accounts")).toEqual(["insert", "update", "update"]);
    const rows = (
      await database.query('SELECT "id", "balance", "note" FROM "accounts" ORDER BY "id"')
    ).rows;
    expect(rows[7]).toEqual({ id: 7, balance: 8, note: "touched" });
    expect(rows[99]).toEqual({ id: 99, balance: 10, note: "touched" });
    await database.close();
  });

  it("fails a duplicate key on the insert that caused it and leaves the scope usable", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.write(async (tx) => {
      await tx.insertBatch("items", [{ id: 1, value: "first" }]);
      await expect(tx.insertBatch("items", [{ id: 1, value: "again" }])).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
      await expect(
        tx.execute('INSERT INTO "items" ("id", "value") VALUES (1, \'sql\')'),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await tx.insertBatch("items", [{ id: 2, value: "second" }]);
    });
    expect((await database.query('SELECT "id", "value" FROM "items" ORDER BY "id"')).rows).toEqual([
      { id: 1, value: "first" },
      { id: 2, value: "second" },
    ]);
    await database.close();
  });

  it("drops the mirror after a savepoint rollback and still answers lookups correctly", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.execute(
      'CREATE TABLE "accounts" ("id" INTEGER PRIMARY KEY, "balance" INTEGER, "floor" INTEGER, CHECK ("balance" >= "floor"))',
    );
    await database.execute("BEGIN");
    await database.execute('INSERT INTO "accounts" ("id", "balance", "floor") VALUES (1, 10, 0)');
    await database.execute("SAVEPOINT s");
    // Both take the keyed path; the first patches the buffered row, the second is a new key.
    await database.execute('UPDATE "accounts" SET "floor" = 5 WHERE "id" = 1');
    await database.execute('INSERT INTO "accounts" ("id", "balance", "floor") VALUES (2, 1, 0)');
    await database.execute("ROLLBACK TO SAVEPOINT s");
    // The pre-image behind this CHECK must be the rolled-back floor of 0, not the mirrored 5.
    await database.execute('UPDATE "accounts" SET "balance" = 3 WHERE "id" = 1');
    // The rolled-back key is free again, and the key that stayed is still taken.
    await database.execute('INSERT INTO "accounts" ("id", "balance", "floor") VALUES (2, 2, 0)');
    await expect(
      database.execute('INSERT INTO "accounts" ("id", "balance", "floor") VALUES (1, 9, 0)'),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await database.execute("COMMIT");
    expect(
      (await database.query('SELECT "id", "balance", "floor" FROM "accounts" ORDER BY "id"')).rows,
    ).toEqual([
      { id: 1, balance: 3, floor: 0 },
      { id: 2, balance: 2, floor: 0 },
    ]);
    await database.close();
  });

  it("falls back to the overlay read once the retained write set exceeds its budget", async () => {
    scopeWriteSetTestHooks.budgetBytes = 4 * 1024;
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([
        table("items", {
          id: column.number().unique(),
          value: column.string(),
          revision: column.number(),
        }),
      ]),
    );
    await database.write(async (tx) => {
      for (let id = 0; id < 200; id += 1) {
        await tx.insertBatch("items", [{ id, value: "x".repeat(64), revision: 1 }]);
      }
      const result = await tx.upsertBatch(
        "items",
        [
          { id: 5, value: "guarded", revision: 2 },
          { id: 500, value: "new", revision: 2 },
        ],
        { conflictWhere: { column: "revision", operator: "<", value: 2 } },
      );
      expect(result).toMatchObject({ rowCount: 2, skippedRowCount: 0 });
      await tx.updateBatch("items", { keys: [6], changes: { value: ["patched"] } });
    });
    const rows = (
      await database.query(
        'SELECT "id", "value", "revision" FROM "items" WHERE "id" IN (5, 6, 500) ORDER BY "id"',
      )
    ).rows;
    expect(rows).toEqual([
      { id: 5, value: "guarded", revision: 2 },
      { id: 6, value: "patched", revision: 1 },
      { id: 500, value: "new", revision: 2 },
    ]);
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(201);
    await database.close();
  });

  it("stages per statement on a trigger-backed table and still classifies its upserts", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([
        table("items", {
          id: column.number().unique(),
          value: column.string(),
          revision: column.number(),
        }),
        table("log", { note: column.string() }),
      ]),
    );
    await database.execute(
      'CREATE TRIGGER "audit" AFTER UPDATE ON "items" FOR EACH ROW BEGIN INSERT INTO "log" ("note") VALUES (NEW."value"); END',
    );
    await database.insertBatch("items", [{ id: 1, value: "a", revision: 1 }]);
    await database.write(async (tx) => {
      for (let step = 0; step < 5; step += 1) {
        await tx.updateBatch("items", { keys: [1], changes: { value: [`v${String(step)}`] } });
      }
      const result = await tx.upsertBatch("items", [{ id: 1, value: "guarded", revision: 2 }], {
        conflictWhere: { column: "revision", operator: "<", value: 2 },
      });
      expect(result).toMatchObject({ rowCount: 1, skippedRowCount: 0 });
    });
    expect((await database.query('SELECT count(*) AS n FROM "log"')).rows[0]?.n).toBe(6);
    expect(
      (await database.query('SELECT "value" FROM "items" WHERE "id" = 1')).rows[0]?.value,
    ).toBe("guarded");
    // Five per-statement update segments plus the upsert: triggers see each statement's rows.
    expect(await segmentKinds(store, "items")).toEqual([
      "insert",
      "update",
      "update",
      "update",
      "update",
      "update",
      "upsert",
    ]);
    await database.close();
  });

  it("stages a bulk statement directly and still orders it after what the set held", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.write(async (tx) => {
      // Small statements wait in the set; the bulk insert must land after them.
      await tx.insertBatch("items", [{ id: 1, value: "small" }]);
      await tx.deleteBatch("items", { keys: [1] });
      const bulk = await tx.insertBatch(
        "items",
        Array.from({ length: 5_000 }, (_, index) => ({ id: index, value: "bulk" })),
      );
      expect(bulk.segmentId).not.toBeNull();
      // Later keyed work still classifies correctly, now through the overlay read.
      const result = await tx.upsertBatch("items", [{ id: 7, value: "guarded" }], {
        conflictWhere: { column: "value", operator: "=", value: "bulk" },
      });
      expect(result).toMatchObject({ rowCount: 1, skippedRowCount: 0 });
      await expect(tx.insertBatch("items", [{ id: 8, value: "dup" }])).rejects.toBeInstanceOf(
        UniqueConstraintError,
      );
    });
    const rows = (
      await database.query('SELECT "id", "value" FROM "items" WHERE "id" IN (1, 7) ORDER BY "id"')
    ).rows;
    expect(rows).toEqual([
      { id: 1, value: "bulk" },
      { id: 7, value: "guarded" },
    ]);
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(5_000);
    await database.close();
  });

  it("encodes a buffered delete before a per-statement insert of the same key", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([
        table("items", { id: column.number().unique(), value: column.string() }),
        table("log", { note: column.string() }),
      ]),
    );
    // Inserts stage per statement here; deletes have no trigger and wait in the set.
    await database.execute(
      'CREATE TRIGGER "audit" AFTER INSERT ON "items" BEGIN INSERT INTO "log" ("note") VALUES (NEW."value"); END',
    );
    await database.insertBatch("items", [{ id: 1, value: "old" }]);
    await database.write(async (tx) => {
      await tx.deleteBatch("items", { keys: [1] });
      await tx.insertBatch("items", [{ id: 1, value: "new" }]);
    });
    expect((await database.query('SELECT "value" FROM "items" WHERE "id" = 1')).rows).toEqual([
      { value: "new" },
    ]);
    await database.close();
  });

  it("serves keyed reads inside a scope from the point-read path, however deep the staging", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.insertBatch(
      "items",
      Array.from({ length: 400 }, (_, id) => ({ id, value: `v${String(id)}` })),
    );
    await database.write(async (tx) => {
      // Each read encodes the previous upsert, so the scope stages one segment per cycle:
      // more than the replay cap that applies outside a scope.
      for (let id = 0; id < 300; id += 1) {
        await tx.upsertBatch("items", [{ id, value: `u${String(id)}` }]);
        await tx.query('SELECT "value" FROM "items" WHERE "id" = $1', { params: [id + 1] });
      }
      await tx.deleteBatch("items", { keys: [5] });
      await tx.updateBatch("items", { keys: [6], changes: { value: ["patched"] } });
      await tx.insertBatch("items", [{ id: 400, value: "new" }]);
      const before = pointReadTestHooks.served;
      const read = async (id: number): Promise<unknown> =>
        (await tx.query('SELECT "value" FROM "items" WHERE "id" = $1', { params: [id] })).rows;
      expect(await read(4)).toEqual([{ value: "u4" }]);
      expect(await read(5)).toEqual([]);
      expect(await read(6)).toEqual([{ value: "patched" }]);
      expect(await read(350)).toEqual([{ value: "v350" }]);
      expect(await read(400)).toEqual([{ value: "new" }]);
      expect(await read(401)).toEqual([]);
      expect(pointReadTestHooks.served - before).toBe(6);
    });
    expect((await database.query('SELECT count(*) AS n FROM "items"')).rows[0]?.n).toBe(400);
    await database.close();
  });

  it("encodes buffered rows once when reads of the table run concurrently", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([table("items", { id: column.number().unique(), value: column.string() })]),
    );
    await database.write(async (tx) => {
      for (let id = 0; id < 3; id += 1) await tx.insertBatch("items", [{ id, value: "v" }]);
      const counts = await Promise.all([
        tx.query('SELECT count(*) AS n FROM "items"'),
        tx.query('SELECT count(*) AS n FROM "items"'),
        tx.query('SELECT "value" FROM "items" WHERE "id" = 1'),
      ]);
      expect(counts[0].rows).toEqual([{ n: 3 }]);
      expect(counts[1].rows).toEqual([{ n: 3 }]);
      expect(counts[2].rows).toEqual([{ value: "v" }]);
    });
    expect((await database.query('SELECT "id" FROM "items" ORDER BY "id"')).rows).toEqual([
      { id: 0 },
      { id: 1 },
      { id: 2 },
    ]);
    expect(await segmentKinds(store, "items")).toEqual(["insert"]);
    await database.close();
  });

  it("coalesces inserts into a keyless table and folds nothing else there", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(schema([table("log", { note: column.string() })]));
    await database.write(async (tx) => {
      for (let index = 0; index < 50; index += 1) {
        await tx.execute('INSERT INTO "log" ("note") VALUES ($1)', [`n${String(index)}`]);
      }
      expect((await tx.query('SELECT count(*) AS n FROM "log"')).rows).toEqual([{ n: 50 }]);
      await tx.insertBatch("log", [{ note: "after the read" }]);
    });
    expect((await database.query('SELECT count(*) AS n FROM "log"')).rows).toEqual([{ n: 51 }]);
    expect(await segmentKinds(store, "log")).toEqual(["insert", "insert"]);
    await database.close();
  });

  it("takes the keyed SQL path only for constant assignments of the column's own type", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, {});
    await database.migrate(
      schema([
        table("items", {
          id: column.number().unique(),
          qty: column.number(),
          label: column.string(),
        }),
      ]),
    );
    await database.insertBatch("items", [
      { id: 1, qty: 1, label: "one" },
      { id: 2, qty: 2, label: "two" },
      { id: 3, qty: 3, label: "three" },
    ]);
    await database.write(async (tx) => {
      // Expressions over the row still read it; the answer must be the same either way.
      await tx.execute('UPDATE "items" SET "qty" = "qty" + 10 WHERE "id" = 1');
      await tx.execute('UPDATE "items" SET "qty" = $1 WHERE "id" IN ($2, $3)', [7, 2, 3]);
      await tx.execute('UPDATE "items" SET "label" = $1 WHERE "id" = $2', ["seven", 2]);
      // A string constant for a number column is read in the column's type on the general path.
      await tx.execute('UPDATE "items" SET "qty" = \'42\' WHERE "id" = 3');
      const missing = await tx.execute('UPDATE "items" SET "qty" = 0 WHERE "id" = 99');
      expect(missing).toMatchObject({ kind: "update", rowCount: 0 });
    });
    expect(
      (await database.query('SELECT "id", "qty", "label" FROM "items" ORDER BY "id"')).rows,
    ).toEqual([
      { id: 1, qty: 11, label: "one" },
      { id: 2, qty: 7, label: "seven" },
      { id: 3, qty: 42, label: "three" },
    ]);
    await database.close();
  });
});
