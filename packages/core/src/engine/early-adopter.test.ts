import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";

import { IndexedDbBlockStore, MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { UnknownTableError } from "./errors.js";
import { column, schema, table } from "./schema.js";

describe("early-adopter engine contracts", () => {
  it("reports missing tables with a stable typed error", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const error = await database.insert("missing", { id: 1 }).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(UnknownTableError);
    expect(error).toBeInstanceOf(TypeError);
    expect(error).toMatchObject({ name: "UnknownTableError", tableName: "missing" });
    await database.close();
  });

  it("publishes declared and ad-hoc JSON domains and implements zoneless DATE", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const events = table("events", {
      id: column.integer().unique(),
      day: column.date(),
      payload: column.jsonb(),
    });
    await database.migrate(schema([events]));
    await database.insert("events", {
      id: 1,
      day: "2024-02-29",
      payload: JSON.stringify({ ok: true }),
    });

    const result = await database.query(
      "SELECT day, payload, JSON_OBJECT('day', day) AS summary FROM events",
    );
    expect(result).toEqual({
      columns: ["day", "payload", "summary"],
      columnDomains: [{ kind: "date" }, { kind: "jsonb" }, { kind: "json" }],
      rows: [
        {
          day: "2024-02-29",
          payload: '{"ok":true}',
          summary: '{"day":"2024-02-29"}',
        },
      ],
    });
    await expect(
      database.insert("events", { id: 2, day: "2025-02-29", payload: "{}" }),
    ).rejects.toThrow("Invalid DATE value");

    await database.execute("CREATE TABLE sql_dates (id INTEGER PRIMARY KEY, day DATE)");
    await database.execute("INSERT INTO sql_dates VALUES (1, DATE '2026-01-31')");
    await expect(
      database.query(
        "SELECT day, day + INTERVAL '1 month' AS next_month, CURRENT_DATE AS today FROM sql_dates",
      ),
    ).resolves.toMatchObject({
      columnDomains: [{ kind: "date" }, { kind: "date" }, { kind: "date" }],
      rows: [{ day: "2026-01-31", next_month: "2026-02-28" }],
    });
    const current = await database.query("SELECT CURRENT_DATE AS today");
    expect(current.rows[0]?.today).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(current.rows[0]?.today).not.toBeInstanceOf(Date);
    await database.execute("CREATE TABLE timestamped (id INTEGER PRIMARY KEY, joined TIMESTAMP)");
    await database.execute("INSERT INTO timestamped VALUES (1, TIMESTAMP '2026-01-02')");
    await expect(
      database.query("SELECT id FROM timestamped WHERE joined >= DATE '2026-01-01'"),
    ).resolves.toMatchObject({ rows: [{ id: 1 }] });
    await database.close();
  });

  it("guards columnar upserts and reports actual versus skipped work", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { compression: "raw" });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "value", type: "string" },
        { name: "_synced", type: "boolean" },
      ],
    });
    await database.insertBatch("items", [
      { id: 1, value: "local", _synced: false },
      { id: 2, value: "old", _synced: true },
    ]);

    const result = await database.upsertBatch(
      "items",
      [
        { id: 1, value: "must-not-win", _synced: true },
        { id: 2, value: "remote", _synced: true },
        { id: 3, value: "new", _synced: true },
      ],
      { conflictWhere: { column: "_synced", operator: "=", value: true } },
    );
    expect(result).toMatchObject({
      requestedRowCount: 3,
      rowCount: 2,
      insertedRowCount: 1,
      updatedRowCount: 1,
      skippedRowCount: 1,
    });
    expect(result.segmentId).toEqual(expect.any(String));
    expect(await database.readTable("items")).toEqual([
      { id: 1, value: "local", _synced: false },
      { id: 2, value: "remote", _synced: true },
      { id: 3, value: "new", _synced: true },
    ]);

    const skipped = await database.upsert(
      "items",
      { id: 1, value: "still-must-not-win", _synced: true },
      { conflictWhere: { column: "_synced", operator: "=", value: true } },
    );
    expect(skipped).toMatchObject({
      segmentId: null,
      requestedRowCount: 1,
      rowCount: 0,
      skippedRowCount: 1,
      blockCount: 0,
      storedBytes: 0,
    });
    await database.close();
  });

  it("windows guarded upsert classification beyond the SQL parameter limit", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      compression: "raw",
      rowsPerBlock: 2_048,
    });
    await database.createTable({
      name: "many",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "_synced", type: "boolean" },
      ],
    });
    const count = 4_097;
    const ids = Array.from({ length: count }, (_, index) => index + 1);
    await database.insertBatch("many", {
      columns: { id: ids, _synced: ids.map(() => false) },
    });
    const result = await database.upsertBatch(
      "many",
      { columns: { id: ids, _synced: ids.map(() => true) } },
      { conflictWhere: { column: "_synced", operator: "=", value: true } },
    );
    expect(result).toMatchObject({
      requestedRowCount: count,
      rowCount: 0,
      skippedRowCount: count,
      segmentId: null,
    });
    await database.close();
  }, 20_000);

  it("preserves guarded-upsert semantics across concurrent commits", async () => {
    const factory = new IDBFactory();
    const name = crypto.randomUUID();
    const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
    const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
    const left = new MinnowDatabase(leftStore);
    const right = new MinnowDatabase(rightStore);
    await left.createTable({
      name: "guard_race",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number", integer: true },
        { name: "value", type: "string" },
        { name: "synced", type: "boolean" },
      ],
    });
    await left.insert("guard_race", { id: 1, value: "before", synced: true });

    const [, guarded] = await Promise.all([
      left.update("guard_race", 1, { synced: false }),
      right.upsert(
        "guard_race",
        { id: 1, value: "remote", synced: true },
        { conflictWhere: { column: "synced", operator: "=", value: true } },
      ),
    ]);

    expect(await left.readTable("guard_race")).toEqual([
      {
        id: 1,
        value: guarded.rowCount === 1 ? "remote" : "before",
        synced: false,
      },
    ]);
    leftStore.close();
    rightStore.close();
  });

  it("keeps informational foreign keys in the catalog without enforcing rows or deletes", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const customers = table("customers", { id: column.integer().unique() });
    const orders = table("orders", { id: column.integer().unique() });
    const tickets = table("tickets", {
      id: column.integer().unique(),
      customer_id: column.integer().references("customers", "id", { enforced: false }),
    });
    await database.migrate(schema([customers, orders, tickets]));
    await database.insert("tickets", { id: 1, customer_id: 404 });
    await database.insert("customers", { id: 1 });
    await database.deleteBatch("customers", { keys: [1] });
    expect(await database.readTable("tickets")).toEqual([{ id: 1, customer_id: 404 }]);
    expect(
      (await database.introspect()).tables.find(({ name }) => name === "tickets")?.foreignKeys,
    ).toEqual([
      expect.objectContaining({
        columns: ["customer_id"],
        parentTable: "customers",
        parentColumns: ["id"],
        enforced: false,
      }),
    ]);

    const remapped = table("tickets", {
      id: column.integer().unique(),
      customer_id: column.integer().references("orders", "id", { enforced: false }),
    });
    const migration = await database.migrate(schema([customers, orders, remapped]));
    expect(migration.steps).toContainEqual(
      expect.objectContaining({ kind: "alter-foreign-keys", tableName: "tickets" }),
    );
    expect(
      (await database.introspect()).tables.find(({ name }) => name === "tickets")?.foreignKeys,
    ).toEqual([expect.objectContaining({ parentTable: "orders", enforced: false })]);

    const plain = table("tickets", {
      id: column.integer().unique(),
      customer_id: column.integer(),
    });
    await database.migrate(schema([customers, orders, plain]));
    expect(
      (await database.introspect()).tables.find(({ name }) => name === "tickets")?.foreignKeys,
    ).toEqual([]);
    expect(() =>
      (
        column.integer() as unknown as {
          references(
            tableName: string,
            columnName: string,
            options: { enforced: false; onDelete: "cascade" },
          ): unknown;
        }
      ).references("customers", "id", { enforced: false, onDelete: "cascade" }),
    ).toThrow("cannot declare ON DELETE");
    await database.close();
  });
});
