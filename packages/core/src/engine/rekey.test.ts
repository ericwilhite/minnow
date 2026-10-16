/**
 * Rekeying: an UPDATE of a primary key moves the row, every declared reference follows in the
 * same commit, and the retired key forwards to the new one for `keyForwardRetentionMs` — for
 * reads and writes, through SQL and the batch API alike — and stays reserved while it does.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { MissingKeyError, UniqueConstraintError } from "./errors.js";
import { keyForwardTestHooks } from "./key-forwards.js";
import { pointReadTestHooks } from "./point-read.js";
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
    await database.execute("INSERT INTO shifts VALUES ('2026-10-15 08:00:00+00', 'a')");
    await database.execute(
      "UPDATE shifts SET starts = '2026-10-15 09:00:00+00' WHERE starts = '2026-10-15 08:00:00+00'",
    );
    expect(
      await rows(database, "SELECT crew FROM shifts WHERE starts = '2026-10-15 08:00:00+00'"),
    ).toEqual([{ crew: "a" }]);
    // The same instant spelled with another offset names the same retired key.
    expect(
      await rows(database, "SELECT crew FROM shifts WHERE starts = '2026-10-15 10:00:00+02'"),
    ).toEqual([{ crew: "a" }]);
    // Text that is no timestamp at all is a type error naming the column, not a key error.
    await expect(database.execute("INSERT INTO shifts VALUES ('not a time', 'b')")).rejects.toThrow(
      "starts[0] must be datetime",
    );
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
    // Each rekey is reported once, as the step it made: o:7f3a → 48213 was the first commit's.
    expect(notices.at(-1)).toEqual([{ table: "orders", from: "48213", to: "48214" }]);
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
    const keyed = new KeyedLiveQuery(query, { key: "id", table: "orders" });
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

describe("retired keys in every SQL shape (read-path audit)", () => {
  async function negative(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)");
    await database.execute(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(id))",
    );
    await database.execute("INSERT INTO orders VALUES (-1, 'offline'), (5, 'five')");
    await database.execute("INSERT INTO items VALUES (1, -1), (2, -1), (3, 5)");
    await database.execute("UPDATE orders SET id = 900 WHERE id = -1");
    return database;
  }

  it("follows a negative key in ON, derived tables, CTEs, and negated parameters", async () => {
    const database = await negative();
    for (const sql of [
      "SELECT count(*) AS n FROM orders o JOIN items i ON i.order_id = o.id AND o.id = -1",
      "SELECT count(*) AS n FROM orders o JOIN items i ON o.id = i.order_id AND i.order_id IN (-1)",
      "SELECT count(*) AS n FROM (SELECT * FROM items WHERE order_id = -1) d",
      "WITH x AS (SELECT * FROM items WHERE order_id = -1) SELECT count(*) AS n FROM x",
    ]) {
      expect(await rows(database, sql), sql).toEqual([{ n: 2 }]);
    }
    expect(
      await rows(database, "SELECT count(*) AS n FROM items WHERE order_id = -$1", [1]),
    ).toEqual([{ n: 2 }]);
    expect(
      await rows(database, "SELECT coalesce((SELECT note FROM orders WHERE id = -1), 'none') AS n"),
    ).toEqual([{ n: "offline" }]);
  });

  it("follows a key under OR, NOT, and IS in a WHERE and in HAVING", async () => {
    const database = await negative();
    // A WHERE that is not a plain conjunction reaches the plan as one `IS TRUE` predicate
    // wrapping the whole expression; the equality inside it must still follow.
    for (const [sql, n] of [
      ["SELECT count(*) AS n FROM orders WHERE id = -1 OR note = 'none'", 1],
      ["SELECT count(*) AS n FROM orders WHERE note = 'none' OR id IN (-1)", 1],
      // The optimizer turns `NOT (id = -1)` into `id <> -1`; the negations follow as well.
      ["SELECT count(*) AS n FROM orders WHERE NOT (id = -1)", 1],
      ["SELECT count(*) AS n FROM orders WHERE id <> -1", 1],
      ["SELECT count(*) AS n FROM orders WHERE id NOT IN (-1, 4)", 1],
      ["SELECT count(*) AS n FROM orders WHERE id IS DISTINCT FROM -1", 1],
      ["SELECT count(*) AS n FROM items WHERE order_id <> -1", 1],
      ["SELECT count(*) AS n FROM orders WHERE (id = -1 OR note IS NULL) IS TRUE", 1],
      ["SELECT count(*) AS n FROM items WHERE order_id = -1 OR id = 3", 3],
      ["SELECT count(*) AS n FROM items WHERE NOT (order_id = -1 OR id = 3)", 0],
      [
        "SELECT count(*) AS n FROM (SELECT order_id FROM items GROUP BY order_id HAVING order_id = -1 OR count(*) > 5) g",
        1,
      ],
    ] as const) {
      expect(await rows(database, sql), sql).toEqual([{ n }]);
    }
    // Writes resolve their WHERE the same way.
    await database.execute("UPDATE orders SET note = 'synced' WHERE id = -1 OR id = 5");
    expect(await rows(database, "SELECT id, note FROM orders ORDER BY id")).toEqual([
      { id: 5, note: "synced" },
      { id: 900, note: "synced" },
    ]);
    // The three-valued partition holds: p, NOT p, and p IS NULL count every row exactly once.
    await database.execute("INSERT INTO orders VALUES (7, NULL)");
    await database.execute("UPDATE orders SET note = NULL WHERE id = 900");
    const p = "(id = -1 OR note = 'synced')";
    const count = async (where: string): Promise<unknown> =>
      (await rows(database, `SELECT count(*) AS n FROM orders WHERE ${where}`))[0]?.n;
    expect([await count(p), await count(`NOT ${p}`), await count(`${p} IS NULL`)]).toEqual([
      2, 0, 1,
    ]);
  });

  it("pins a composite key per source, not per table", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE r (shop INTEGER, receipt INTEGER, t INTEGER, PRIMARY KEY (shop, receipt))",
    );
    await database.execute("INSERT INTO r VALUES (1, -1, 10), (2, -1, 20), (2, 7, 99)");
    await database.execute("UPDATE r SET receipt = 5 WHERE shop = 1 AND receipt = -1");
    expect(
      await rows(
        database,
        "SELECT a.t AS at, b.t AS bt FROM r a, r b WHERE a.shop = 1 AND b.receipt = -1",
      ),
    ).toEqual([{ at: 10, bt: 20 }]);
    expect(
      await rows(
        database,
        "SELECT a.t AS at FROM r a, r b WHERE a.shop = 1 AND a.receipt = -1 AND b.shop = 2 AND b.receipt = 7",
      ),
    ).toEqual([{ at: 10 }]);
  });

  it("follows a key through a view, and a cached or live answer over the view moves too", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE tickets (id TEXT PRIMARY KEY, note TEXT)");
    await database.execute(
      "CREATE TABLE revisions (id INTEGER PRIMARY KEY, ticket_id TEXT REFERENCES tickets(id) NOT ENFORCED)",
    );
    await database.execute("INSERT INTO tickets VALUES ('o:1', 'n')");
    await database.execute("INSERT INTO revisions VALUES (1, '77')");
    await database.execute("CREATE VIEW open_tickets AS SELECT id, note FROM tickets");
    await database.execute(
      "CREATE VIEW counted AS SELECT count(*) AS n FROM revisions WHERE ticket_id = 'o:1'",
    );
    const live = database.liveQueries();
    const counts: unknown[] = [];
    await live.subscribe("SELECT n FROM counted", {
      onChange: (result) => counts.push(result.rows[0]?.n),
    });
    expect(await rows(database, "SELECT n FROM counted")).toEqual([{ n: 0 }]);
    await database.execute("UPDATE tickets SET id = '77' WHERE id = 'o:1'");
    await live.refresh();
    expect(await rows(database, "SELECT n FROM counted")).toEqual([{ n: 1 }]);
    expect(counts).toEqual([0, 1]);
    expect(await rows(database, "SELECT id FROM open_tickets WHERE id = 'o:1'")).toEqual([
      { id: "77" },
    ]);
    expect(
      await rows(database, "SELECT v.note FROM open_tickets v WHERE v.id = ?", ["o:1"]),
    ).toEqual([{ note: "n" }]);
    live.close();
  });

  it("stops following an expired key even for a cached answer", async () => {
    const time = clock();
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      now: time.now,
      keyForwardRetentionMs: 10 * DAY,
    });
    await database.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)");
    await database.execute(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(id))",
    );
    await database.execute("INSERT INTO orders VALUES (-1)");
    await database.execute("INSERT INTO items VALUES (1, -1)");
    await database.execute("UPDATE orders SET id = 900 WHERE id = -1");
    const sql = "SELECT count(*) AS n FROM items WHERE order_id = -1";
    expect(await rows(database, sql)).toEqual([{ n: 1 }]);
    time.advance(11 * DAY);
    expect(await rows(database, sql)).toEqual([{ n: 0 }]);
  });
});

describe("rekeying under savepoints, constraints, retention, and maintenance (write-path audit)", () => {
  it("rolls a savepoint back with the forwards the rekey staged", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, customer TEXT)");
    await database.execute(
      "CREATE TABLE notes (id TEXT PRIMARY KEY, order_id TEXT REFERENCES orders(id) NOT ENFORCED)",
    );
    await database.execute("INSERT INTO orders VALUES ('a', 'ada'), ('b', 'bo')");
    await database.execute("BEGIN");
    await database.execute("SAVEPOINT s");
    await database.execute("UPDATE orders SET id = 'x' WHERE id = 'a'");
    await database.execute("ROLLBACK TO s");
    expect(await rows(database, "SELECT id, customer FROM orders WHERE id = 'a'")).toEqual([
      { id: "a", customer: "ada" },
    ]);
    await database.execute("INSERT INTO notes VALUES ('n1', 'a')");
    await database.execute("UPDATE orders SET id = 'y' WHERE id = 'a'");
    await database.execute("COMMIT");
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "b" },
      { id: "y" },
    ]);
    expect(await rows(database, "SELECT order_id FROM notes")).toEqual([{ order_id: "y" }]);
  });

  it("forwards a key before generated columns and CHECK constraints read it", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE o (id INTEGER PRIMARY KEY, who TEXT)");
    await database.execute(
      "CREATE TABLE l (id INTEGER PRIMARY KEY, o_id INTEGER REFERENCES o(id), label TEXT GENERATED ALWAYS AS ('line of ' || o_id) STORED)",
    );
    await database.execute(
      "CREATE TABLE neg (id INTEGER PRIMARY KEY, o_id INTEGER REFERENCES o(id) NOT ENFORCED, CHECK (o_id < 0))",
    );
    await database.execute("INSERT INTO o (id, who) VALUES (-1, 'ada')");
    await database.execute("UPDATE o SET id = 900 WHERE id = -1");
    await database.execute("INSERT INTO l (id, o_id) VALUES (1, -1)");
    await database.insert("l", { id: 2, o_id: -1 });
    expect(await rows(database, "SELECT id, o_id, label FROM l ORDER BY id")).toEqual([
      { id: 1, o_id: 900, label: "line of 900" },
      { id: 2, o_id: 900, label: "line of 900" },
    ]);
    await expect(database.execute("INSERT INTO neg VALUES (1, -1)")).rejects.toThrow("CHECK");
  });

  it("keeps an expired forward expired whatever retention a later connection uses", async () => {
    const time = clock();
    const store = new MemoryBlockStore();
    const first = new MinnowDatabase(store, { now: time.now, keyForwardRetentionMs: DAY });
    await first.execute("CREATE TABLE o (id TEXT PRIMARY KEY, who TEXT)");
    await first.execute("INSERT INTO o VALUES ('a', 'old')");
    await first.execute("UPDATE o SET id = 'x' WHERE id = 'a'");
    time.advance(2 * DAY);
    await first.execute("INSERT INTO o VALUES ('a', 'new')");
    const second = new MinnowDatabase(store, { now: time.now, keyForwardRetentionMs: 10 * DAY });
    expect(await rows(second, "SELECT id, who FROM o WHERE id = 'a'")).toEqual([
      { id: "a", who: "new" },
    ]);
    await second.execute("UPDATE o SET who = 'touched' WHERE id = 'a'");
    expect(await rows(second, "SELECT id, who FROM o ORDER BY id")).toEqual([
      { id: "a", who: "touched" },
      { id: "x", who: "old" },
    ]);
  });

  it("never trims a forward written again after the trim looked", async () => {
    const time = clock();
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { now: time.now, keyForwardRetentionMs: DAY });
    const other = new MinnowDatabase(store, { now: time.now, keyForwardRetentionMs: DAY });
    await database.execute("CREATE TABLE o (id TEXT PRIMARY KEY)");
    await database.execute("INSERT INTO o VALUES ('a')");
    await database.execute("UPDATE o SET id = 'x' WHERE id = 'a'");
    time.advance(2 * DAY);
    await database.execute("INSERT INTO o VALUES ('a')");
    let collecting: Promise<unknown> | undefined;
    await database.write(async (session) => {
      await session.execute("UPDATE o SET id = 'z' WHERE id = 'a'");
      collecting = other.collectGarbage();
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
    await collecting;
    expect(await rows(database, "SELECT id FROM o WHERE id = 'a'")).toEqual([{ id: "z" }]);
    expect(await database.resolveKey("o", "a")).toBe("z");
  });

  it("follows a child whose key contains its composite reference, through literals", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE receipt (shop INTEGER, receipt INTEGER, PRIMARY KEY (shop, receipt))",
    );
    await database.execute(
      "CREATE TABLE line (shop INTEGER, receipt INTEGER, n INTEGER, qty INTEGER, PRIMARY KEY (shop, receipt, n), FOREIGN KEY (shop, receipt) REFERENCES receipt (shop, receipt))",
    );
    await database.execute("INSERT INTO receipt VALUES (1, -1)");
    await database.execute("INSERT INTO line VALUES (1, -1, 2, 5)");
    await database.execute("UPDATE receipt SET receipt = 7 WHERE shop = 1 AND receipt = -1");
    expect(
      await rows(
        database,
        "SELECT receipt, qty FROM line WHERE shop = 1 AND receipt = -1 AND n = 2",
      ),
    ).toEqual([{ receipt: 7, qty: 5 }]);
    expect(await rows(database, "SELECT n FROM line WHERE shop = 1 AND receipt = -1")).toEqual([
      { n: 2 },
    ]);
  });
});

describe("chained forwards", () => {
  it("reaches a row rekeyed several times from every key it ever had", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = 'b' WHERE id = 'o:7f3a'");
    await database.execute("UPDATE orders SET id = 'c' WHERE id = 'b'");
    // Rekeying by an older key follows the chain to the row first.
    await database.execute("UPDATE orders SET id = 'd' WHERE id = 'o:7f3a'");
    for (const key of ["o:7f3a", "b", "c", "d"]) {
      expect(await rows(database, "SELECT id, total FROM orders WHERE id = ?", [key])).toEqual([
        { id: "d", total: 30 },
      ]);
      expect(await database.resolveKey("orders", key)).toBe("d");
    }
    // A child written with the oldest key stores the current one.
    await database.execute("INSERT INTO order_lines VALUES ('l9', 'o:7f3a', 'nib')");
    expect(await rows(database, "SELECT order_id FROM order_lines WHERE id = 'l9'")).toEqual([
      { order_id: "d" },
    ]);
    // Every retired key stays reserved.
    for (const key of ["o:7f3a", "b", "c"]) {
      await expect(
        database.execute("INSERT INTO orders VALUES (?, 'x', 1)", [key]),
      ).rejects.toThrow(UniqueConstraintError);
    }
    // One stored forward per rekey; none was rewritten.
    expect(await database.resolveKey("orders", "41")).toBe("41");
  });

  it("follows a chain that runs through a scope's own staged step", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = 'b' WHERE id = 'o:7f3a'");
    await database.write(async (session) => {
      await session.execute("UPDATE orders SET id = 'c' WHERE id = 'b'");
      expect((await session.query("SELECT id FROM orders WHERE id = 'o:7f3a'")).rows).toEqual([
        { id: "c" },
      ]);
      await session.execute("UPDATE orders SET total = 7 WHERE id = 'o:7f3a'");
    });
    expect(await rows(database, "SELECT id, total FROM orders WHERE id = 'o:7f3a'")).toEqual([
      { id: "c", total: 7 },
    ]);
  });

  it("stops at a step that expired, so an old key stops forwarding on schedule", async () => {
    const time = clock();
    const database = await orders({ now: time.now, keyForwardRetentionMs: 10 * DAY });
    await database.execute("UPDATE orders SET id = 'b' WHERE id = 'o:7f3a'");
    time.advance(4 * DAY);
    await database.execute("UPDATE orders SET id = 'c' WHERE id = 'b'");
    time.advance(7 * DAY);
    // The first step expired at day 10; the second lasts until day 14.
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'o:7f3a'")).toEqual([]);
    expect(await rows(database, "SELECT id FROM orders WHERE id = 'b'")).toEqual([{ id: "c" }]);
  });
});

describe("statements that stage nothing (level-zero ceilings)", () => {
  it("commits an update that names a retired key and sets the key the row already has", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    const result = await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    expect(result).toMatchObject({ kind: "update", rowCount: 1 });
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "41" },
      { id: "48213" },
    ]);
  });

  it("commits a scope whose statements changed nothing or rolled back to a savepoint", async () => {
    const database = await orders();
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await database.write(async (session) => {
      await session.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
      await session.execute("UPDATE orders SET id = 'z' WHERE id = 'missing'");
      await session.execute("DELETE FROM orders WHERE id = 'missing'");
    });
    await database.execute("BEGIN");
    await database.execute("SAVEPOINT s");
    await expect(
      database.execute("UPDATE orders SET id = '41' WHERE id = '48213'"),
    ).rejects.toThrow();
    await database.execute("ROLLBACK TO s");
    await database.execute("UPDATE orders SET id = '48213' WHERE id = 'o:7f3a'");
    await database.execute("COMMIT");
    expect(await rows(database, "SELECT id FROM orders ORDER BY id")).toEqual([
      { id: "41" },
      { id: "48213" },
    ]);
  });
});

describe("point reads beside forwards (performance audit)", () => {
  it("keeps a foreign-key point read on its fast path unless its parent has forwards", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { compression: "raw" });
    await database.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)");
    await database.execute("CREATE TABLE tags (id INTEGER PRIMARY KEY)");
    await database.execute(
      "CREATE TABLE lines (order_id INTEGER REFERENCES orders(id), n INTEGER, sku TEXT, PRIMARY KEY (order_id, n))",
    );
    await database.execute("INSERT INTO orders VALUES (-1), (7)");
    await database.execute("INSERT INTO tags VALUES (1)");
    await database.execute("INSERT INTO lines VALUES (-1, 1, 'pen'), (7, 1, 'ink')");
    const served = async (sql: string): Promise<{ rows: unknown[]; fast: boolean }> => {
      const before = pointReadTestHooks.served;
      const result = await rows(database, sql);
      return { rows: result, fast: pointReadTestHooks.served > before };
    };
    // A rekey of an unrelated table leaves the lines read on the fast path.
    await database.execute("UPDATE tags SET id = 2 WHERE id = 1");
    expect(await served("SELECT sku FROM lines WHERE order_id = 7 AND n = 1")).toEqual({
      rows: [{ sku: "ink" }],
      fast: true,
    });
    // Once the parent has forwards, a retired parent key still finds the moved lines.
    await database.execute("UPDATE orders SET id = 900 WHERE id = -1");
    expect(await served("SELECT sku FROM lines WHERE order_id = -1 AND n = 1")).toEqual({
      rows: [{ sku: "pen" }],
      fast: false,
    });
  });
});

describe("the forwards mirror another connection keeps (performance audit)", () => {
  it("layers each commit's forwards and agrees with a whole read throughout", async () => {
    const time = clock();
    const store = new MemoryBlockStore();
    // Maintenance runs only when the test asks, so the clock jumps below never land in the
    // middle of a background job; compaction is then exercised on purpose.
    const options = {
      compression: "raw" as const,
      now: time.now,
      keyForwardRetentionMs: 5 * DAY,
      autoCompact: false,
      autoCollect: false,
    };
    const writer = new MinnowDatabase(store, options);
    const reader = new MinnowDatabase(store, options);
    await writer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)");
    await writer.insertBatch(
      "t",
      Array.from({ length: 200 }, (_, id) => ({ id, label: `row ${String(id)}` })),
    );
    let seed = 0x5eed;
    const random = (limit: number): number => {
      seed = (Math.imul(seed, 1103515245) + 12345) >>> 0;
      return seed % limit;
    };
    const current = Array.from({ length: 200 }, (_, id) => id);
    const retired: number[] = [];
    let next = 1000;
    let readerWholeReads = 0;
    let compactions = 0;
    for (let step = 0; step < 120; step += 1) {
      // Every tenth step folds the forwards table, which leaves the next window unprovable.
      const action = step % 10 === 9 ? 19 : random(19);
      if (action < 14) {
        const slot = random(current.length);
        const from = current[slot] ?? 0;
        const to = next++;
        await writer.execute(`UPDATE t SET id = ${String(to)} WHERE id = ${String(from)}`);
        current[slot] = to;
        retired.push(from);
      } else if (action < 17) {
        time.advance((1 + random(3)) * DAY);
        await writer.collectGarbage();
      } else if (action < 19) {
        // A batch of rekeys in one statement: one window, many forwards.
        await writer.execute(
          `UPDATE t SET id = id + 100000 WHERE id < ${String(next)} AND id % 7 = ${String(random(7))}`,
        );
        current.splice(
          0,
          current.length,
          ...(await rows(writer, "SELECT id FROM t ORDER BY id")).map((row) => row.id as number),
        );
        next += 100000;
      } else {
        if ((await writer.compactTable("\u0000minnow_key_forwards")).compacted) compactions += 1;
      }
      const probes = [...retired.slice(-12), ...current.slice(0, 4)];
      const before = keyForwardTestHooks.wholeReads;
      const incremental = await Promise.all(probes.map((key) => reader.resolveKey("t", key)));
      const viaSql = await Promise.all(
        probes.map(
          async (key) =>
            (await rows(reader, `SELECT id FROM t WHERE id = ${String(key)}`))[0]?.id ?? null,
        ),
      );
      readerWholeReads += keyForwardTestHooks.wholeReads - before;
      const fresh = new MinnowDatabase(store, options);
      const whole = await Promise.all(probes.map((key) => fresh.resolveKey("t", key)));
      const wholeSql = await Promise.all(
        probes.map(
          async (key) =>
            (await rows(fresh, `SELECT id FROM t WHERE id = ${String(key)}`))[0]?.id ?? null,
        ),
      );
      await fresh.close();
      expect(incremental).toEqual(whole);
      expect(viaSql).toEqual(wholeSql);
    }
    // The reader read the table whole once, plus once per window compaction folded away.
    expect(compactions).toBeGreaterThan(0);
    expect(readerWholeReads).toBeLessThanOrEqual(1 + compactions);
    await reader.close();
    await writer.close();
  });
});
