/**
 * Rekeying: an UPDATE of a primary key moves the row, every declared reference follows in the
 * same commit, and the retired key forwards to the new one for `keyForwardRetentionMs` — for
 * reads and writes, through SQL and the batch API alike — and stays reserved while it does.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { MissingKeyError, UniqueConstraintError } from "./errors.js";
import { KeyedLiveQuery } from "./keyed-live.js";
import type { QueryResult } from "./query.js";
import { column, schema, table } from "./schema.js";
import { createLiveQueryManager } from "./typed-live.js";

const DAY = 24 * 60 * 60 * 1000;

function clock(start = Date.parse("2026-10-15T12:00:00.000Z")): {
  now: () => Date;
  advance: (ms: number) => void;
} {
  let current = start;
  return {
    now: () => new Date(current),
    advance: (ms) => {
      current += ms;
    },
  };
}

async function orders(
  options: { store?: BlockStore; now?: () => Date; keyForwardRetentionMs?: number } = {},
): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(options.store ?? new MemoryBlockStore(), {
    compression: "raw",
    ...(options.now === undefined ? {} : { now: options.now }),
    ...(options.keyForwardRetentionMs === undefined
      ? {}
      : { keyForwardRetentionMs: options.keyForwardRetentionMs }),
  });
  await database.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT, total INTEGER)");
  await database.execute(
    "CREATE TABLE order_lines (id TEXT PRIMARY KEY, order_id TEXT REFERENCES orders(id), sku TEXT)",
  );
  await database.execute("INSERT INTO orders VALUES ('o:7f3a', 'ada', 30), ('41', 'bo', 10)");
  await database.execute(
    "INSERT INTO order_lines VALUES ('l1', 'o:7f3a', 'pen'), ('l2', 'o:7f3a', 'ink'), ('l3', '41', 'cap')",
  );
  return database;
}

async function rows(database: MinnowDatabase, sql: string, params?: unknown[]) {
  return (await database.query(sql, params === undefined ? {} : { params: params as never[] }))
    .rows;
}

describe("rekeying a row", () => {
  it("moves the row and its references in one statement", async () => {
    const database = await orders();
    const result = await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    expect(result).toMatchObject({ kind: "update", rowCount: 1 });
    expect(await rows(database, "SELECT id, customer, total FROM orders ORDER BY id")).toEqual([
      { id: "41", customer: "bo", total: 10 },
      { id: "48213", customer: "ada", total: 30 },
    ]);
    expect(await rows(database, "SELECT id, order_id FROM order_lines ORDER BY id")).toEqual([
      { id: "l1", order_id: "48213" },
      { id: "l2", order_id: "48213" },
      { id: "l3", order_id: "41" },
    ]);
    // A join on the stored keys needs no forwards at all.
    expect(
      await rows(
        database,
        "SELECT o.customer, l.sku FROM orders o JOIN order_lines l ON l.order_id = o.id ORDER BY l.id",
      ),
    ).toEqual([
      { customer: "ada", sku: "pen" },
      { customer: "ada", sku: "ink" },
      { customer: "bo", sku: "cap" },
    ]);
  });

  it("rekeys through the batch API and other columns change with the key", async () => {
    const database = await orders();
    const result = await database.updateBatch("orders", {
      keys: ["o:7f3a", "41"],
      changes: { id: ["48213", "41"], total: [31, 11] },
    });
    expect(result).toMatchObject({ updatedRowCount: 2 });
    expect(await rows(database, "SELECT id, total FROM orders ORDER BY id")).toEqual([
      { id: "41", total: 11 },
      { id: "48213", total: 31 },
    ]);
    await database.update("orders", "48213", { id: "48214" });
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "41" },
      { id: "48214" },
    ]);
  });

  it("refuses a new key that is taken, null, or repeated, and writes nothing", async () => {
    const database = await orders();
    await expect(
      database.execute("UPDATE orders SET id = '41' WHERE id = 'o:7f3a'"),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await expect(
      database.updateBatch("orders", { keys: ["o:7f3a", "41"], changes: { id: ["x", "x"] } }),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await expect(
      database.updateBatch("orders", { keys: ["o:7f3a"], changes: { id: [null] } }),
    ).rejects.toThrow(/null/i);
    await expect(database.update("orders", "missing", { id: "y" })).rejects.toBeInstanceOf(
      MissingKeyError,
    );
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "41" },
      { id: "o:7f3a" },
    ]);
    expect(await rows(database, "SELECT order_id FROM order_lines ORDER BY id")).toEqual([
      { order_id: "o:7f3a" },
      { order_id: "o:7f3a" },
      { order_id: "41" },
    ]);
  });

  it("treats setting a key to itself as an ordinary update", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = id, total = 99 WHERE id = '41'");
    expect(await rows(database, "SELECT id, total FROM orders WHERE id = '41'")).toEqual([
      { id: "41", total: 99 },
    ]);
    // No forward was needed, so none was written.
    expect((await database.listTables()).map(({ name }) => name).sort()).toEqual([
      "order_lines",
      "orders",
    ]);
  });

  it("swaps keys within one batch without either key forwarding", async () => {
    const database = await orders();
    await database.updateBatch("orders", {
      keys: ["o:7f3a", "41"],
      changes: { id: ["41", "o:7f3a"] },
    });
    expect(await rows(database, "SELECT id, customer FROM orders ORDER BY id")).toEqual([
      { id: "41", customer: "ada" },
      { id: "o:7f3a", customer: "bo" },
    ]);
    expect(await rows(database, "SELECT id, order_id FROM order_lines ORDER BY id")).toEqual([
      { id: "l1", order_id: "41" },
      { id: "l2", order_id: "41" },
      { id: "l3", order_id: "o:7f3a" },
    ]);
    // Both keys are live rows, so neither reads as the other.
    expect(await rows(database, "SELECT customer FROM orders WHERE id = '41'")).toEqual([
      { customer: "ada" },
    ]);
    expect(await rows(database, "SELECT customer FROM orders WHERE id = ?", ["o:7f3a"])).toEqual([
      { customer: "bo" },
    ]);
  });

  it("hides the forwards table from every listing", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    expect((await database.listTables()).map(({ name }) => name).sort()).toEqual([
      "order_lines",
      "orders",
    ]);
    expect((await database.introspect()).tables.map(({ name }) => name)).toEqual([
      "order_lines",
      "orders",
    ]);
  });
});

describe("a retired key", () => {
  it("reads the row through SQL literals, parameters, IN lists, and subqueries", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    expect(await rows(database, "SELECT id, customer FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "48213", customer: "ada" },
    ]);
    expect(await rows(database, "SELECT * FROM orders WHERE id = ?", ["o:7f3a"])).toEqual([
      { id: "48213", customer: "ada", total: 30 },
    ]);
    expect(
      await rows(database, "SELECT id FROM orders WHERE id IN ('o:7f3a', '41') ORDER BY id"),
    ).toEqual([{ id: "41" }, { id: "48213" }]);
    expect(
      await rows(database, "SELECT id FROM order_lines WHERE order_id = ? ORDER BY id", ["o:7f3a"]),
    ).toEqual([{ id: "l1" }, { id: "l2" }]);
    expect(
      await rows(
        database,
        "SELECT sku FROM order_lines WHERE order_id IN (SELECT id FROM orders WHERE id = 'o:7f3a') ORDER BY id",
      ),
    ).toEqual([{ sku: "pen" }, { sku: "ink" }]);
    expect(
      await rows(
        database,
        "SELECT l.sku FROM order_lines l JOIN orders o ON o.id = l.order_id WHERE o.id = 'o:7f3a' AND l.order_id = 'o:7f3a' ORDER BY l.id",
      ),
    ).toEqual([{ sku: "pen" }, { sku: "ink" }]);
    expect(await database.readTable("orders")).toHaveLength(2);
  });

  it("writes the row through SQL and the batch API", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await database.execute("UPDATE orders SET total = 31 WHERE id = 'o:7f3a'");
    await database.update("orders", "o:7f3a", { customer: "ada l." });
    expect(await rows(database, "SELECT * FROM orders WHERE id = '48213'")).toEqual([
      { id: "48213", customer: "ada l.", total: 31 },
    ]);
    // A child written against the retired key stores the current one.
    await database.execute("INSERT INTO order_lines VALUES ('l4', 'o:7f3a', 'nib')");
    await database.insert("order_lines", { id: "l5", order_id: "o:7f3a", sku: "cap" });
    await database.update("order_lines", "l3", { order_id: "o:7f3a" });
    expect(
      await rows(
        database,
        "SELECT id, order_id FROM order_lines WHERE id IN ('l3', 'l4', 'l5') ORDER BY id",
      ),
    ).toEqual([
      { id: "l3", order_id: "48213" },
      { id: "l4", order_id: "48213" },
      { id: "l5", order_id: "48213" },
    ]);
    // An upsert naming the retired key updates the row; it never inserts a second one.
    await database.upsert("orders", { id: "o:7f3a", customer: "ada", total: 32 });
    await database.execute(
      "INSERT INTO orders VALUES ('o:7f3a', 'ada', 33) ON CONFLICT (id) DO UPDATE SET total = excluded.total",
    );
    expect(await rows(database, "SELECT id, total FROM orders ORDER BY id")).toEqual([
      { id: "41", total: 10 },
      { id: "48213", total: 33 },
    ]);
    // A plain insert of the retired key is a duplicate: its alias holds it.
    await expect(
      database.execute("INSERT INTO orders VALUES ('o:7f3a', 'dup', 1)"),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await expect(
      database.insert("orders", { id: "o:7f3a", customer: "dup", total: 1 }),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await database.execute("DELETE FROM order_lines WHERE order_id = 'o:7f3a'");
    expect(await rows(database, "SELECT id FROM order_lines ORDER BY id")).toEqual([]);
    await database.deleteBatch("orders", { keys: ["o:7f3a", "48213"] });
    expect(await rows(database, "SELECT id FROM orders")).toEqual([{ id: "41" }]);
  });

  it("stays reserved: no row may be rekeyed onto it", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await expect(
      database.execute("UPDATE orders SET id = 'o:7f3a' WHERE id = '41'"),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    await expect(
      database.execute("UPDATE orders SET id = 'o:7f3a' WHERE id = '48213'"),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
  });

  it("follows a chain in one hop", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = 'b' WHERE id = 'o:7f3a'");
    await database.execute("UPDATE orders SET id = 'c' WHERE id = 'o:7f3a'");
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "c" },
    ]);
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'b'")).toEqual([{ id: "c" }]);
    expect(await rows(database, "SELECT order_id FROM order_lines WHERE id = 'l1'")).toEqual([
      { order_id: "c" },
    ]);
  });

  it("expires after the retention window and maintenance deletes its forward", async () => {
    const time = clock();
    const database = await orders({ now: time.now, keyForwardRetentionMs: 10 * DAY });
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    time.advance(10 * DAY - 1);
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "48213" },
    ]);
    time.advance(2);
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([]);
    // The key is free again once its forward has expired.
    await database.execute("INSERT INTO orders VALUES ('o:7f3a', 'new', 1)");
    await database.collectGarbage();
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "41" },
      { id: "48213" },
      { id: "o:7f3a" },
    ]);
  });

  it("refuses a retention that is not a positive whole number", () => {
    expect(() => new MinnowDatabase(new MemoryBlockStore(), { keyForwardRetentionMs: 0 })).toThrow(
      /retention/i,
    );
  });
});

describe("references follow a rekey", () => {
  it("rekeys a child whose own key includes the reference", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE tickets (id TEXT PRIMARY KEY, note TEXT)");
    await database.execute(
      "CREATE TABLE adjustments (ticket_key TEXT PRIMARY KEY REFERENCES tickets(id), amount INTEGER)",
    );
    await database.execute(
      "CREATE TABLE adjustment_notes (id INTEGER PRIMARY KEY, ticket_key TEXT REFERENCES adjustments(ticket_key))",
    );
    await database.execute("INSERT INTO tickets VALUES ('t:1', 'a'), ('t:2', 'b')");
    await database.execute("INSERT INTO adjustments VALUES ('t:1', 5), ('t:2', 6)");
    await database.execute("INSERT INTO adjustment_notes VALUES (1, 't:1'), (2, 't:2')");
    await database.execute("UPDATE tickets SET id = '900' WHERE id = 't:1'");
    expect(await rows(database, "SELECT ticket_key, amount FROM adjustments ORDER BY 1")).toEqual([
      { ticket_key: "900", amount: 5 },
      { ticket_key: "t:2", amount: 6 },
    ]);
    expect(await rows(database, "SELECT id, ticket_key FROM adjustment_notes ORDER BY id")).toEqual(
      [
        { id: 1, ticket_key: "900" },
        { id: 2, ticket_key: "t:2" },
      ],
    );
    // The child's own retired key forwards too.
    expect(await rows(database, "SELECT amount FROM adjustments WHERE ticket_key = 't:1'")).toEqual(
      [{ amount: 5 }],
    );
  });

  it("moves rows that refer to their own table, including a row that refers to itself", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE nodes (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES nodes(id), label TEXT)",
    );
    await database.execute(
      "INSERT INTO nodes VALUES ('r', NULL, 'root'), ('a', 'r', 'a'), ('b', 'a', 'b'), ('s', 's', 'self')",
    );
    await database.execute("UPDATE nodes SET id = 'A' WHERE id = 'a'");
    await database.execute("UPDATE nodes SET id = 'S' WHERE id = 's'");
    expect(await rows(database, "SELECT id, parent_id FROM nodes ORDER BY label")).toEqual([
      { id: "A", parent_id: "r" },
      { id: "b", parent_id: "A" },
      { id: "r", parent_id: null },
      { id: "S", parent_id: "S" },
    ]);
    // A whole subtree in one statement.
    await database.execute("UPDATE nodes SET id = lower(id) || '2' WHERE id IN ('r', 'A', 'b')");
    expect(await rows(database, "SELECT id, parent_id FROM nodes ORDER BY label")).toEqual([
      { id: "a2", parent_id: "r2" },
      { id: "b2", parent_id: "a2" },
      { id: "r2", parent_id: null },
      { id: "S", parent_id: "S" },
    ]);
  });

  it("follows a composite key, through predicates that pin every column", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE receipts (shop INTEGER, receipt INTEGER, total INTEGER, PRIMARY KEY (shop, receipt))",
    );
    await database.execute(
      "CREATE TABLE refunds (id INTEGER PRIMARY KEY, shop INTEGER, receipt INTEGER, FOREIGN KEY (shop, receipt) REFERENCES receipts (shop, receipt))",
    );
    await database.execute("INSERT INTO receipts VALUES (1, -1, 10), (1, 2, 20)");
    await database.execute("INSERT INTO refunds VALUES (1, 1, -1), (2, 1, 2)");
    await database.execute("UPDATE receipts SET receipt = 7 WHERE shop = 1 AND receipt = -1");
    expect(
      await rows(database, "SELECT shop, receipt, total FROM receipts ORDER BY receipt"),
    ).toEqual([
      { shop: 1, receipt: 2, total: 20 },
      { shop: 1, receipt: 7, total: 10 },
    ]);
    expect(await rows(database, "SELECT id, shop, receipt FROM refunds ORDER BY id")).toEqual([
      { id: 1, shop: 1, receipt: 7 },
      { id: 2, shop: 1, receipt: 2 },
    ]);
    expect(
      await rows(database, "SELECT total FROM receipts WHERE shop = 1 AND receipt = -1"),
    ).toEqual([{ total: 10 }]);
    expect(
      await rows(database, "SELECT id FROM refunds WHERE shop = ? AND receipt = ?", [1, -1]),
    ).toEqual([{ id: 1 }]);
    await database.execute("INSERT INTO refunds VALUES (3, 1, -1)");
    expect(await rows(database, "SELECT receipt FROM refunds WHERE id = 3")).toEqual([
      { receipt: 7 },
    ]);
  });

  it("moves unenforced references, and allows rows whose parent is not here", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE tickets (id TEXT PRIMARY KEY)");
    await database.execute(
      "CREATE TABLE revisions (id INTEGER PRIMARY KEY, ticket_id TEXT REFERENCES tickets(id) NOT ENFORCED)",
    );
    await database.execute("INSERT INTO tickets VALUES ('t:1')");
    // A revision for a ticket outside the synced window is allowed.
    await database.execute("INSERT INTO revisions VALUES (1, 't:1'), (2, 'elsewhere')");
    await database.execute("UPDATE tickets SET id = '77' WHERE id = 't:1'");
    expect(await rows(database, "SELECT id, ticket_id FROM revisions ORDER BY id")).toEqual([
      { id: 1, ticket_id: "77" },
      { id: 2, ticket_id: "elsewhere" },
    ]);
    // Deleting the parent leaves unenforced children alone.
    await database.execute("DELETE FROM tickets WHERE id = '77'");
    expect(await rows(database, "SELECT count(*) AS n FROM revisions")).toEqual([{ n: 2 }]);
    // An evicted row synced back under its current key: the old key still reaches it.
    await database.execute("INSERT INTO tickets VALUES ('77')");
    expect(await rows(database, "SELECT id FROM tickets WHERE id = 't:1'")).toEqual([{ id: "77" }]);
  });

  it("is declared with enforced: false in the schema API", async () => {
    const declared = schema([
      table("tickets", { id: column.string().unique(), note: column.string().nullable() }),
      table("revisions", {
        id: column.number().unique(),
        ticket_id: column.string().references("tickets", "id", { enforced: false }),
      }),
    ]);
    const database = new MinnowDatabase(new MemoryBlockStore(), { schema: declared });
    await database.migrate();
    await database.insert("tickets", { id: "o:1", note: null });
    await database.insertBatch("revisions", [
      { id: 1, ticket_id: "o:1" },
      { id: 2, ticket_id: "not-synced" },
    ]);
    await database.update("tickets", "o:1", { id: "500" });
    expect(await database.readTable("revisions")).toEqual([
      { id: 1, ticket_id: "500" },
      { id: 2, ticket_id: "not-synced" },
    ]);
  });

  it("refuses when a referencing table has no key to update its rows by", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE tickets (id TEXT PRIMARY KEY)");
    await database.execute("CREATE TABLE log (ticket_id TEXT REFERENCES tickets(id), line TEXT)");
    await database.execute("INSERT INTO tickets VALUES ('t:1')");
    await database.execute("INSERT INTO log VALUES ('t:1', 'x')");
    await expect(database.execute("UPDATE tickets SET id = '1' WHERE id = 't:1'")).rejects.toThrow(
      "needs a unique key on log to follow a rekey",
    );
    expect(await rows(database, "SELECT id FROM tickets")).toEqual([{ id: "t:1" }]);
  });
});

describe("rekeying with the rest of the engine", () => {
  it("fires UPDATE triggers for the move, not INSERT or DELETE ones", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE items (id TEXT PRIMARY KEY, qty INTEGER)");
    await database.execute("CREATE TABLE audit (event TEXT, old_id TEXT, new_id TEXT)");
    await database.execute(
      "CREATE TRIGGER items_update AFTER UPDATE ON items BEGIN INSERT INTO audit VALUES ('update', OLD.id, NEW.id); END",
    );
    await database.execute(
      "CREATE TRIGGER items_insert AFTER INSERT ON items BEGIN INSERT INTO audit VALUES ('insert', NULL, NEW.id); END",
    );
    await database.execute(
      "CREATE TRIGGER items_delete AFTER DELETE ON items BEGIN INSERT INTO audit VALUES ('delete', OLD.id, NULL); END",
    );
    await database.execute("INSERT INTO items VALUES ('tmp', 1)");
    await database.execute("UPDATE items SET id = 'real' WHERE id = 'tmp'");
    expect(await rows(database, "SELECT event, old_id, new_id FROM audit ORDER BY event")).toEqual([
      { event: "insert", old_id: null, new_id: "tmp" },
      { event: "update", old_id: "tmp", new_id: "real" },
    ]);
  });

  it("keeps unique, secondary, full-text, and generated columns right", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE people (id TEXT PRIMARY KEY, email TEXT UNIQUE, bio TEXT, label TEXT GENERATED ALWAYS AS (id || ':' || email) STORED)",
    );
    await database.execute("CREATE INDEX people_bio ON people (bio)");
    await database.execute(
      "INSERT INTO people (id, email, bio) VALUES ('p:1', 'ada@x', 'counts things'), ('p:2', 'bo@x', 'draws')",
    );
    await database.execute("UPDATE people SET id = '10' WHERE id = 'p:1'");
    expect(await rows(database, "SELECT id, label FROM people WHERE email = 'ada@x'")).toEqual([
      { id: "10", label: "10:ada@x" },
    ]);
    expect(await rows(database, "SELECT id FROM people WHERE bio = 'counts things'")).toEqual([
      { id: "10" },
    ]);
    expect(await rows(database, "SELECT id FROM people WHERE MATCH(bio) AGAINST 'counts'")).toEqual(
      [{ id: "10" }],
    );
    // The email still belongs to exactly one row.
    await expect(
      database.execute("INSERT INTO people (id, email, bio) VALUES ('p:3', 'ada@x', 'z')"),
    ).rejects.toThrow();
  });

  it("rekeys through MERGE and a trigger body", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE stock (sku TEXT PRIMARY KEY, qty INTEGER)");
    await database.execute("CREATE TABLE renames (old_sku TEXT, new_sku TEXT)");
    await database.execute("INSERT INTO stock VALUES ('tmp-1', 3), ('tmp-2', 4)");
    await database.execute(
      "MERGE INTO stock s USING (SELECT 'tmp-1' AS sku, 'A-1' AS next) d ON s.sku = d.sku WHEN MATCHED THEN UPDATE SET sku = d.next",
    );
    await database.execute(
      "CREATE TRIGGER apply_rename AFTER INSERT ON renames BEGIN UPDATE stock SET sku = NEW.new_sku WHERE sku = NEW.old_sku; END",
    );
    await database.execute("INSERT INTO renames VALUES ('tmp-2', 'A-2')");
    expect(await rows(database, "SELECT sku, qty FROM stock ORDER BY sku")).toEqual([
      { sku: "A-1", qty: 3 },
      { sku: "A-2", qty: 4 },
    ]);
    expect(await rows(database, "SELECT qty FROM stock WHERE sku = 'tmp-2'")).toEqual([{ qty: 4 }]);
  });

  it("rekeys integer, UUID, and timestamp keys", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE requests (id INTEGER PRIMARY KEY, label TEXT)");
    await database.execute(
      "CREATE TABLE request_items (id INTEGER PRIMARY KEY, request_id INTEGER REFERENCES requests(id))",
    );
    await database.execute("INSERT INTO requests VALUES (-3, 'offline'), (12, 'synced')");
    await database.execute("INSERT INTO request_items VALUES (1, -3)");
    await database.execute("UPDATE requests SET id = 4471 WHERE id = -3");
    expect(await rows(database, "SELECT label FROM requests WHERE id = -3")).toEqual([
      { label: "offline" },
    ]);
    expect(await rows(database, "SELECT request_id FROM request_items")).toEqual([
      { request_id: 4471 },
    ]);
    expect(await rows(database, "SELECT id FROM requests WHERE id = ?", ["-3"])).toEqual([
      { id: 4471 },
    ]);

    await database.execute("CREATE TABLE devices (id UUID PRIMARY KEY, name TEXT)");
    await database.execute(
      "INSERT INTO devices VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'tablet')",
    );
    await database.execute(
      "UPDATE devices SET id = 'b1ffcd00-0d1c-4f09-8c7e-7cc0ce491b22' WHERE id = 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'",
    );
    expect(
      await rows(
        database,
        "SELECT id FROM devices WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
      ),
    ).toEqual([{ id: "b1ffcd00-0d1c-4f09-8c7e-7cc0ce491b22" }]);

    await database.execute("CREATE TABLE shifts (starts TIMESTAMPTZ PRIMARY KEY, crew TEXT)");
    await database.execute("INSERT INTO shifts VALUES ('2026-10-15T08:00:00.000Z', 'a')");
    await database.execute(
      "UPDATE shifts SET starts = '2026-10-15T09:00:00.000Z' WHERE starts = '2026-10-15T08:00:00.000Z'",
    );
    expect(
      await rows(database, "SELECT crew FROM shifts WHERE starts = '2026-10-15T08:00:00.000Z'"),
    ).toEqual([{ crew: "a" }]);
  });
});

describe("rekeying inside a write scope", () => {
  it("reads and writes through the retired key before the scope commits", async () => {
    const database = await orders();
    await database.write(async (session) => {
      await session.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
      expect(
        (await session.query("SELECT id, customer FROM orders WHERE id = 'o:7f3a'")).rows,
      ).toEqual([{ id: "48213", customer: "ada" }]);
      await session.execute("UPDATE orders SET total = 1 WHERE id = 'o:7f3a'");
      await session.updateBatch("orders", { keys: ["o:7f3a"], changes: { customer: ["x"] } });
      await session.insertBatch("order_lines", [{ id: "l9", order_id: "o:7f3a", sku: "s" }]);
    });
    expect(await rows(database, "SELECT * FROM orders WHERE id = '48213'")).toEqual([
      { id: "48213", customer: "x", total: 1 },
    ]);
    expect(await rows(database, "SELECT order_id FROM order_lines WHERE id = 'l9'")).toEqual([
      { order_id: "48213" },
    ]);
  });

  it("leaves no trace when the scope aborts", async () => {
    const database = await orders();
    await expect(
      database.write(async (session) => {
        await session.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
        throw new Error("abandoned");
      }),
    ).rejects.toThrow("abandoned");
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "41" },
      { id: "o:7f3a" },
    ]);
    expect(await rows(database, "SELECT id FROM orders WHERE id = '48213'")).toEqual([]);
    await database.execute("INSERT INTO orders VALUES ('48213', 'other', 0)");
  });

  it("works inside BEGIN and COMMIT", async () => {
    const database = await orders();
    await database.execute("BEGIN");
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "48213" },
    ]);
    await database.execute("COMMIT");
    expect(await rows(database, "SELECT id FROM order_lines WHERE order_id = 'o:7f3a'")).toEqual([
      { id: "l1" },
      { id: "l2" },
    ]);
  });
});

describe("rekeying across connections", () => {
  it("reaches another connection on the same store, which forwards the old key too", async () => {
    const store = new MemoryBlockStore();
    const first = await orders({ store });
    const second = new MinnowDatabase(store);
    expect(await rows(second, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "o:7f3a" },
    ]);
    await first.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    expect(await rows(second, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "48213" },
    ]);
    await second.update("orders", "o:7f3a", { total: 5 });
    await expect(
      second.insert("orders", { id: "o:7f3a", customer: "dup", total: 1 }),
    ).rejects.toBeInstanceOf(UniqueConstraintError);
    expect(await rows(first, "SELECT total FROM orders WHERE id = '48213'")).toEqual([
      { total: 5 },
    ]);
  });

  it("keeps a live query on the retired key showing the row", async () => {
    const database = await orders();
    const live = database.liveQueries();
    const results: QueryResult[] = [];
    await live.subscribe("SELECT id, total FROM orders WHERE id = 'o:7f3a'", {
      onChange: (result) => results.push(result),
    });
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await live.refresh();
    await database.execute("UPDATE orders SET total = 7 WHERE id = '48213'");
    await live.refresh();
    expect(results.map((result) => result.rows)).toEqual([
      [{ id: "o:7f3a", total: 30 }],
      [{ id: "48213", total: 30 }],
      [{ id: "48213", total: 7 }],
    ]);
    live.close();
  });
});

describe("following a rekey", () => {
  it("reports key changes with the deliveries, read from the stored forwards", async () => {
    const store = new MemoryBlockStore();
    const database = await orders({ store });
    const live = database.liveQueries();
    const deliveries: Array<{ rows: unknown[]; keyChanges: unknown }> = [];
    await live.subscribe("SELECT id FROM orders ORDER BY id", {
      onChange: (result, delivery) =>
        deliveries.push({ rows: result.rows, keyChanges: delivery.keyChanges }),
    });
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await live.refresh();
    expect(deliveries.at(-1)).toEqual({
      rows: [{ id: "41" }, { id: "48213" }],
      keyChanges: [{ table: "orders", from: "o:7f3a", to: "48213" }],
    });
    // A later commit with no rekey carries none.
    await database.execute("UPDATE orders SET total = 1 WHERE id = '41'");
    await database.execute("INSERT INTO orders VALUES ('z', 'zed', 1)");
    await live.refresh();
    expect(deliveries.at(-1)?.keyChanges).toBeUndefined();
    live.close();

    // Another connection that heard no hint at all still learns of the rekey on its next sweep.
    const other = new MinnowDatabase(store);
    const quiet = other.liveQueries();
    const notices: unknown[] = [];
    await quiet.subscribe("SELECT order_id FROM order_lines ORDER BY id", {
      onChange: (_result, delivery) => notices.push(delivery.keyChanges),
    });
    await database.execute("UPDATE orders SET id = '48214' WHERE id = '48213'");
    await quiet.refresh();
    expect(notices.at(-1)).toEqual([
      { table: "orders", from: "o:7f3a", to: "48214" },
      { table: "orders", from: "48213", to: "48214" },
    ]);
    quiet.close();
  });

  it("reports a rekeyed row as a rekey in a keyed diff", async () => {
    const database = await orders();
    const manager = createLiveQueryManager(database);
    const sql = "SELECT id, customer FROM orders ORDER BY customer";
    const query = manager.watch<{ id: string; customer: string }>({
      query: sql,
      execute: async () =>
        (await database.query(sql)).rows as unknown as Array<{ id: string; customer: string }>,
      decode: (result) => result.rows as unknown as Array<{ id: string; customer: string }>,
    });
    const keyed = new KeyedLiveQuery(query, { key: "id" });
    const unsubscribe = keyed.subscribe(() => undefined);
    await keyed.refresh();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await keyed.refresh();
    const snapshot = keyed.getSnapshot();
    expect(snapshot.status === "ready" ? snapshot.changes : undefined).toEqual([
      {
        type: "rekey",
        from: "o:7f3a",
        key: "48213",
        row: { id: "48213", customer: "ada" },
        previous: { id: "o:7f3a", customer: "ada" },
        previousIndex: 0,
        index: 0,
      },
    ]);
    unsubscribe();
    await manager.close();
  });

  it("resolves a held key to the current one", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = 'b' WHERE id = 'o:7f3a'");
    await database.execute("UPDATE orders SET id = 'c' WHERE id = 'b'");
    expect(await database.resolveKey("orders", "o:7f3a")).toBe("c");
    expect(await database.resolveKey("orders", "b")).toBe("c");
    expect(await database.resolveKey("orders", "41")).toBe("41");
    expect(await database.resolveKey("orders", "never")).toBe("never");
    await database.execute(
      "CREATE TABLE receipts (shop INTEGER, receipt INTEGER, PRIMARY KEY (shop, receipt))",
    );
    await database.execute("INSERT INTO receipts VALUES (1, -1)");
    await database.execute("UPDATE receipts SET receipt = 9 WHERE shop = 1 AND receipt = -1");
    expect(await database.resolveKey("receipts", [1, -1])).toEqual([1, 9]);
    await expect(database.resolveKey("receipts", 1)).rejects.toThrow("primary-key column");
  });
});

describe("live queries over a key that a rekey redirects", () => {
  it("re-run when only the forwards changed, for a reference written before its parent", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE tickets (id TEXT PRIMARY KEY)");
    await database.execute(
      "CREATE TABLE revisions (id INTEGER PRIMARY KEY, ticket_id TEXT REFERENCES tickets(id) NOT ENFORCED)",
    );
    await database.execute("INSERT INTO tickets VALUES ('o:1')");
    // Synced before its ticket was: it already names the server key.
    await database.execute("INSERT INTO revisions VALUES (1, '77')");
    const live = database.liveQueries();
    const counts: unknown[] = [];
    const ids: unknown[] = [];
    await live.subscribe("SELECT count(*) AS n FROM revisions WHERE ticket_id = 'o:1'", {
      onChange: (result) => counts.push(result.rows[0]?.n),
    });
    await live.subscribe("SELECT id FROM revisions WHERE ticket_id = 'o:1' ORDER BY id", {
      onChange: (result) => ids.push(result.rows.map((row) => row.id)),
    });
    // Warm both, so the next window is one an incremental patch could try to answer.
    await database.execute("INSERT INTO revisions VALUES (2, 'elsewhere')");
    await live.refresh();
    // No revision row changes; only the forward does.
    await database.execute("UPDATE tickets SET id = '77' WHERE id = 'o:1'");
    await live.refresh();
    expect(counts).toEqual([0, 1]);
    expect(ids).toEqual([[], [1]]);
    live.close();
  });
});
