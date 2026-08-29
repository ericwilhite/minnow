import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { QueryMemoryBudgetError } from "./memory.js";

async function seeded(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.createTable({
    name: "people",
    uniqueKey: "name",
    columns: [
      { name: "name", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  await database.execute("INSERT INTO people (name, score) VALUES ('Ada', 10), ('Grace', 25)");
  return database;
}

describe("RETURNING in SQL statements", () => {
  it("returns written, post-update, and deleted rows", async () => {
    const database = await seeded();
    const inserted = await database.execute(
      "INSERT INTO people (name, score) VALUES (?, ?) RETURNING people.*",
      ["Linus", 5],
    );
    expect(inserted).toMatchObject({
      kind: "insert",
      returnedRows: [{ name: "Linus", score: 5 }],
    });
    const updated = await database.execute(
      "UPDATE people SET score = score + 1 WHERE name = 'Ada' " +
        "RETURNING people.name, people.score",
    );
    expect(updated).toMatchObject({ kind: "update", returnedRows: [{ name: "Ada", score: 11 }] });
    const deleted = await database.execute(
      "DELETE FROM people WHERE score > 20 RETURNING people.name",
    );
    expect(deleted).toMatchObject({ kind: "delete", returnedRows: [{ name: "Grace" }] });
  });
});

describe("DELETE selection", () => {
  it("streams unique keys through mutation history and falls back for staged overlays", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      compression: "raw",
      rowsPerBlock: 256,
      executionMemoryBudgetBytes: 256_000,
      autoCompact: false,
      autoCollect: false,
    });
    await database.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT)");
    await database.insertBatch("events", {
      columns: {
        id: Array.from({ length: 4_096 }, (_, index) => index),
        label: Array.from({ length: 4_096 }, (_, index) => `event-${String(index)}`),
      },
    });
    // The key-only scan must replay existing deltas, not read the base blocks in isolation.
    await database.updateBatch("events", {
      keys: [700, 2_000, 3_500],
      changes: { label: ["changed-a", "changed-b", "changed-c"] },
    });
    const deleted = await database.execute(
      "DELETE FROM events WHERE id BETWEEN ? AND ?",
      [512, 3_583],
    );
    expect(deleted).toMatchObject({ kind: "delete", rowCount: 3_072 });
    expect((await database.query("SELECT COUNT(*) AS n FROM events")).rows).toEqual([{ n: 1_024 }]);

    await database.write(async (transaction) => {
      await transaction.insertBatch("events", {
        columns: { id: [5_000], label: ["staged"] },
      });
      // Once the scope has staged data, SQL mutation selection keeps the overlay executor so
      // this statement sees both the new row and committed rows.
      const scoped = await transaction.execute("DELETE FROM events WHERE id >= 4090");
      expect(scoped).toMatchObject({ kind: "delete", rowCount: 7 });
    });
    expect((await database.query("SELECT COUNT(*) AS n FROM events")).rows).toEqual([{ n: 1_018 }]);
  });

  it("routes subquery predicates through resolved mutation selection", async () => {
    const makeDatabase = async (): Promise<MinnowDatabase> => {
      const database = new MinnowDatabase(new MemoryBlockStore());
      await database.execute(
        'CREATE TABLE "widgets" ("widgetKey" INTEGER UNIQUE, "retained" BOOLEAN)',
      );
      await database.execute('CREATE TABLE "keptKeys" ("keptKey" INTEGER UNIQUE)');
      await database.execute(
        'INSERT INTO "widgets" ("widgetKey", "retained") VALUES (1, false), (2, false)',
      );
      await database.execute('INSERT INTO "keptKeys" ("keptKey") VALUES (1)');
      return database;
    };

    const notIn = await makeDatabase();
    await expect(
      notIn.execute(
        'DELETE FROM "widgets" WHERE "widgetKey" NOT IN ' + '(SELECT "keptKey" FROM "keptKeys")',
      ),
    ).resolves.toMatchObject({ kind: "delete", rowCount: 1 });
    expect((await notIn.query('SELECT "widgetKey" FROM "widgets"')).rows).toEqual([
      { widgetKey: 1 },
    ]);

    const notExists = await makeDatabase();
    await expect(
      notExists.execute(
        'DELETE FROM "widgets" WHERE NOT EXISTS (' +
          'SELECT 1 FROM "keptKeys" WHERE "keptKeys"."keptKey" = "widgetKey")',
      ),
    ).resolves.toMatchObject({ kind: "delete", rowCount: 1 });
    expect((await notExists.query('SELECT "widgetKey" FROM "widgets"')).rows).toEqual([
      { widgetKey: 1 },
    ]);

    const returning = await makeDatabase();
    await expect(
      returning.execute(
        'DELETE FROM "widgets" WHERE NOT EXISTS (' +
          'SELECT 1 FROM "keptKeys" WHERE "keptKeys"."keptKey" = "widgetKey") ' +
          'RETURNING "widgetKey"',
      ),
    ).resolves.toMatchObject({
      kind: "delete",
      rowCount: 1,
      returnedRows: [{ widgetKey: 2 }],
    });

    const update = await makeDatabase();
    await expect(
      update.execute(
        'UPDATE "widgets" SET "retained" = true WHERE EXISTS (' +
          'SELECT 1 FROM "keptKeys" WHERE "keptKeys"."keptKey" = "widgetKey") ' +
          'RETURNING "widgetKey", "retained"',
      ),
    ).resolves.toMatchObject({
      kind: "update",
      rowCount: 1,
      returnedRows: [{ widgetKey: 1, retained: true }],
    });
  });
});

describe("nested correlated subqueries through the public API", () => {
  it("runs sibling and nested EXISTS repeatedly through query and execute", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute('CREATE TABLE "aa" ("aaKey" INTEGER UNIQUE, "bbKey" INTEGER)');
    await database.execute('CREATE TABLE "bb" ("bbKey" INTEGER UNIQUE, "ok" BOOLEAN)');
    await database.execute('CREATE TABLE "cc" ("ccKey" INTEGER UNIQUE, "bbKey" INTEGER)');
    await database.execute('INSERT INTO "aa" ("aaKey", "bbKey") VALUES (0, 1), (2, 2)');
    await database.execute('INSERT INTO "bb" ("bbKey", "ok") VALUES (1, true), (2, true)');
    await database.execute('INSERT INTO "cc" ("ccKey", "bbKey") VALUES (10, 1), (20, 2)');

    const sql =
      'SELECT "aaKey" FROM "aa" WHERE "aa"."aaKey" = 0 ' +
      'OR EXISTS (SELECT 1 FROM "bb" ' +
      'WHERE "bb"."bbKey" = "aa"."bbKey" AND "bb"."ok" = true) ' +
      'OR EXISTS (SELECT 1 FROM "cc" WHERE "cc"."bbKey" = "aa"."bbKey" ' +
      'AND EXISTS (SELECT 1 FROM "bb" ' +
      'WHERE "bb"."bbKey" = "cc"."bbKey" AND "bb"."ok" = true)) ' +
      'ORDER BY "aaKey"';
    const expected = [{ aaKey: 0 }, { aaKey: 2 }];
    expect((await database.query(sql)).rows).toEqual(expected);
    await expect(database.execute(sql)).resolves.toMatchObject({
      kind: "rows",
      result: { rows: expected },
    });
    expect((await database.query(sql)).rows).toEqual(expected);
  });

  it("resolves unqualified outer names before nested decorrelation", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute('CREATE TABLE "widgets" ("widgetKey" INTEGER UNIQUE)');
    await database.execute('CREATE TABLE "keptKeys" ("keptKey" INTEGER UNIQUE)');
    await database.execute('INSERT INTO "widgets" ("widgetKey") VALUES (1), (2)');
    await database.execute('INSERT INTO "keptKeys" ("keptKey") VALUES (1)');
    expect(
      (
        await database.query(
          'SELECT "widgetKey" FROM "widgets" WHERE EXISTS (' +
            'SELECT 1 FROM "keptKeys" ' +
            'WHERE "keptKeys"."keptKey" = "widgets"."widgetKey") OR EXISTS (' +
            'SELECT 1 FROM "keptKeys" WHERE "keptKeys"."keptKey" = "widgetKey")',
        )
      ).rows,
    ).toEqual([{ widgetKey: 1 }]);
  });
});

describe("INSERT defaults", () => {
  it("evaluates SQL defaults per omitted row and preserves explicit NULL", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE defaulted (id INTEGER DEFAULT 1, label TEXT DEFAULT lower('MADE') NOT NULL, note TEXT DEFAULT 'note', token TEXT DEFAULT gen_random_uuid())",
    );

    const first = await database.execute("INSERT INTO defaulted DEFAULT VALUES RETURNING *");
    expect(first).toMatchObject({
      kind: "insert",
      returnedRows: [{ id: 1, label: "made", note: "note" }],
    });
    const second = await database.execute(
      "INSERT INTO defaulted (id, label, note, token) VALUES (2, DEFAULT, NULL, DEFAULT) RETURNING *",
    );
    expect(second).toMatchObject({
      kind: "insert",
      returnedRows: [{ id: 2, label: "made", note: null }],
    });
    const firstToken = (first as { returnedRows?: Array<{ token?: string }> }).returnedRows?.[0]
      ?.token;
    const secondToken = (second as { returnedRows?: Array<{ token?: string }> }).returnedRows?.[0]
      ?.token;
    expect(firstToken).toMatch(/^[0-9a-f-]{36}$/);
    expect(secondToken).toMatch(/^[0-9a-f-]{36}$/);
    expect(firstToken).not.toBe(secondToken);
    await expect(
      database.execute("INSERT INTO defaulted (id, label) VALUES (3, NULL)"),
    ).rejects.toThrow("label[0] cannot be null");
  });

  it("evaluates a sequence default once through ON CONFLICT DO NOTHING", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE SEQUENCE generated_ids");
    await database.execute(
      "CREATE TABLE generated (id INTEGER DEFAULT NEXTVAL('generated_ids') PRIMARY KEY, label TEXT NOT NULL)",
    );
    expect(
      await database.execute(
        "INSERT INTO generated (label) VALUES ('one') ON CONFLICT (id) DO NOTHING RETURNING id",
      ),
    ).toMatchObject({ returnedRows: [{ id: 1 }] });
    expect((await database.query("SELECT CURRVAL('generated_ids') AS n")).rows).toEqual([{ n: 1 }]);
  });

  it("uses an omitted defaulted key to find ON CONFLICT DO UPDATE exactly once", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE SEQUENCE conflict_ids");
    await database.execute(
      "CREATE TABLE generated_conflict (id INTEGER DEFAULT NEXTVAL('conflict_ids') PRIMARY KEY, label TEXT NOT NULL)",
    );
    // An explicit key does not consume the independent sequence, so the proposed default is 1
    // and must find this row even though id is absent from the INSERT column list.
    await database.execute("INSERT INTO generated_conflict (id, label) VALUES (1, 'old')");
    expect(
      await database.execute(
        "INSERT INTO generated_conflict (label) VALUES ('new') " +
          "ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label RETURNING *",
      ),
    ).toMatchObject({ returnedRows: [{ id: 1, label: "new" }] });
    expect((await database.query("SELECT CURRVAL('conflict_ids') AS n")).rows).toEqual([{ n: 1 }]);
  });

  it("materializes an omitted auto-increment key in the general DO UPDATE path", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "generated_conflict",
      columns: [
        { name: "id", type: "number", integer: true, defaultValue: { kind: "autoincrement" } },
        { name: "label", type: "string" },
        { name: "note", type: "string", defaultValue: { kind: "literal", value: "kept" } },
      ],
      uniqueKey: "id",
    });
    expect(
      await database.execute(
        "INSERT INTO generated_conflict (label) VALUES ('fresh') " +
          "ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label RETURNING *",
      ),
    ).toMatchObject({ returnedRows: [{ id: 1, label: "fresh", note: "kept" }] });
    expect(
      await database.execute(
        "INSERT INTO generated_conflict (id, label, note) VALUES (1, 'updated', DEFAULT) " +
          "ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label RETURNING *",
      ),
    ).toMatchObject({ returnedRows: [{ id: 1, label: "updated", note: "kept" }] });
  });

  it("rejects defaults that are not variable-free or type-compatible", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await expect(
      database.execute("CREATE TABLE row_default (source TEXT, copy TEXT DEFAULT source)"),
    ).rejects.toThrow("variable-free scalar SQL expression");
    await expect(
      database.execute("CREATE TABLE wrong_default (value TEXT DEFAULT 1)"),
    ).rejects.toThrow("Default literal must be a string");
    await expect(
      database.execute("CREATE TABLE subquery_default (value INTEGER DEFAULT (SELECT 1))"),
    ).rejects.toThrow("variable-free scalar SQL expression");
  });

  it("supports durable sequence and random defaults", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE SEQUENCE default_ids");
    await database.execute(
      "CREATE TABLE generated (id INTEGER DEFAULT nextval('default_ids'), sample REAL DEFAULT random())",
    );
    await database.execute("INSERT INTO generated DEFAULT VALUES");
    await database.execute("INSERT INTO generated DEFAULT VALUES");
    const rows = (await database.query("SELECT id, sample FROM generated ORDER BY id")).rows;
    expect(rows.map(({ id }) => id)).toEqual([1, 2]);
    for (const { sample } of rows) {
      expect(sample).toEqual(expect.any(Number));
      expect(sample as number).toBeGreaterThanOrEqual(0);
      expect(sample as number).toBeLessThan(1);
    }
  });
});

describe("INSERT ... SELECT", () => {
  it("materializes the query at one snapshot and inserts its rows", async () => {
    const database = await seeded();
    const copied = await database.execute(
      "INSERT INTO people (name, score) SELECT name || '-copy' AS name, score * 2 AS score FROM people WHERE score >= ? RETURNING *",
      [10],
    );
    expect(copied).toMatchObject({
      kind: "insert",
      rowCount: 2,
      returnedRows: [
        { name: "Ada-copy", score: 20 },
        { name: "Grace-copy", score: 50 },
      ],
    });
  });

  it("inserts nothing when the query is empty, without a write", async () => {
    const database = await seeded();
    const empty = await database.execute(
      "INSERT INTO people (name, score) SELECT name AS name, score AS score FROM people WHERE score > 999",
    );
    expect(empty).toEqual({ kind: "insert", table: "people", rowCount: 0 });
  });

  it("rejects wildcard and mismatched select lists", async () => {
    const database = await seeded();
    await expect(
      database.execute("INSERT INTO people (name, score) SELECT * FROM people"),
    ).rejects.toThrow("explicit select list");
    await expect(
      database.execute("INSERT INTO people (name, score) SELECT name AS name FROM people"),
    ).rejects.toThrow("exactly the insert column count");
  });
});

describe("CREATE TABLE", () => {
  it("creates a table with mapped types, nullability, and a unique key", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const created = await database.execute(
      "CREATE TABLE made (id INTEGER PRIMARY KEY, label VARCHAR(80) NOT NULL, at TIMESTAMP, ok BOOLEAN)",
    );
    expect(created).toEqual({ kind: "create-table", table: "made" });
    const inserted = await database.execute(
      "INSERT INTO made (id, label, at, ok) VALUES (1, 'x', NULL, TRUE) RETURNING *",
    );
    expect(inserted).toMatchObject({
      kind: "insert",
      returnedRows: [{ id: 1, label: "x", at: null, ok: true }],
    });
    const [table] = await database.listTables();
    expect(table?.columns.map(({ name, type }) => `${name}:${type}`)).toEqual([
      "id:number",
      "label:string",
      "at:datetime",
      "ok:boolean",
    ]);
  });

  it("stores generated columns, recomputes them on every write, and indexes their values", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE offline_rows (" +
        "id INTEGER PRIMARY KEY, field_id TEXT NOT NULL, ticket_number INTEGER NOT NULL, " +
        "version INTEGER NOT NULL, offline_key TEXT " +
        "GENERATED ALWAYS AS (field_id || ':' || CAST(ticket_number AS TEXT) || ':' || CAST(version AS TEXT)) STORED NOT NULL)",
    );
    await database.execute("CREATE INDEX offline_rows_key_idx ON offline_rows (offline_key)");

    const inserted = await database.execute(
      "INSERT INTO offline_rows (id, field_id, ticket_number, version) VALUES (1, 'field', 42, 3) RETURNING *",
    );
    expect(inserted).toMatchObject({
      kind: "insert",
      returnedRows: [
        { id: 1, field_id: "field", ticket_number: 42, version: 3, offline_key: "field:42:3" },
      ],
    });
    expect(
      (await database.query("SELECT id FROM offline_rows WHERE offline_key = 'field:42:3'")).rows,
    ).toEqual([{ id: 1 }]);

    const updated = await database.execute(
      "UPDATE offline_rows SET version = 4 WHERE id = 1 RETURNING offline_key",
    );
    expect(updated).toMatchObject({
      kind: "update",
      returnedRows: [{ offline_key: "field:42:4" }],
    });
    expect(
      (await database.query("SELECT id FROM offline_rows WHERE offline_key = 'field:42:4'")).rows,
    ).toEqual([{ id: 1 }]);
    expect(
      (await database.query("SELECT id FROM offline_rows WHERE offline_key = 'field:42:3'")).rows,
    ).toEqual([]);

    expect(
      await database.execute(
        "INSERT INTO offline_rows (id, field_id, ticket_number, version) VALUES " +
          "(1, 'field', 42, 5), (2, 'other', 7, 1) " +
          "ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version RETURNING id, offline_key",
      ),
    ).toMatchObject({
      returnedRows: [
        { id: 1, offline_key: "field:42:5" },
        { id: 2, offline_key: "other:7:1" },
      ],
    });

    await expect(
      database.execute(
        "INSERT INTO offline_rows (id, field_id, ticket_number, version, offline_key) VALUES (2, 'x', 1, 1, 'wrong')",
      ),
    ).rejects.toThrow("Generated column cannot be assigned: offline_key");
    await expect(
      database.execute("UPDATE offline_rows SET offline_key = 'wrong' WHERE id = 1"),
    ).rejects.toThrow("Generated column cannot be assigned: offline_key");
  });

  it("rejects generated expressions that are volatile, cross-row, or chained", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await expect(
      database.execute(
        "CREATE TABLE volatile_generated (id INTEGER PRIMARY KEY, token TEXT GENERATED ALWAYS AS (gen_random_uuid()) STORED)",
      ),
    ).rejects.toThrow("cannot call volatile function");
    await expect(
      database.execute(
        "CREATE TABLE aggregate_generated (id INTEGER PRIMARY KEY, total INTEGER GENERATED ALWAYS AS (SUM(id)) STORED)",
      ),
    ).rejects.toThrow("immutable row expression");
    await expect(
      database.execute(
        "CREATE TABLE chained_generated (id INTEGER PRIMARY KEY, first INTEGER GENERATED ALWAYS AS (id + 1) STORED, second INTEGER GENERATED ALWAYS AS (first + 1) STORED)",
      ),
    ).rejects.toThrow("cannot reference a generated column");
  });

  it("rejects unknown types and enforces additional UNIQUE constraints", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await expect(database.execute("CREATE TABLE bad (x NO_SUCH_TYPE)")).rejects.toThrow(
      "Unsupported column type: NO_SUCH_TYPE",
    );
    await database.execute(
      "CREATE TABLE good (a INTEGER PRIMARY KEY, b TEXT UNIQUE, c TEXT, CONSTRAINT good_c_key UNIQUE (c))",
    );
    await database.execute("INSERT INTO good VALUES (1, 'b1', 'c1')");
    await expect(database.execute("INSERT INTO good VALUES (2, 'b1', 'c2')")).rejects.toThrow(
      "good.b",
    );
    await expect(database.execute("INSERT INTO good VALUES (2, 'b2', 'c1')")).rejects.toThrow(
      "good.c",
    );
    // PostgreSQL UNIQUE allows multiple NULLs.
    await database.execute("INSERT INTO good VALUES (2, NULL, NULL), (3, NULL, NULL)");
  });

  it("stores PostgreSQL value domains over stable primitive block encodings", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await expect(
      database.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')"),
    ).resolves.toEqual({ kind: "create-type", name: "mood" });
    await expect(database.execute("CREATE TABLE bad_domain (x missing_type)")).rejects.toThrow(
      "Unsupported column type: missing_type",
    );
    await database.execute(
      "CREATE TABLE domain_values (" +
        "id UUID PRIMARY KEY, amount NUMERIC(30, 10), document JSONB, choices INTEGER[], " +
        "at TIME, span INTERVAL, feeling mood)",
    );
    const inserted = await database.execute(
      "INSERT INTO domain_values VALUES (" +
        "'550E8400-E29B-41D4-A716-446655440000', 0.1, '{\"b\":2,\"a\":1}', " +
        "ARRAY[1, 2], TIME '12:34:56', INTERVAL '1 month 2 days', 'happy') RETURNING *",
    );
    expect(inserted).toMatchObject({
      returnedRows: [
        {
          id: "550e8400-e29b-41d4-a716-446655440000",
          amount: "0.1000000000",
          document: '{"a":1,"b":2}',
          choices: "[1,2]",
          at: "12:34:56",
          span: "1 mons 2 days 0 usecs",
          feeling: "happy",
        },
      ],
    });
    expect(
      (await database.query("SELECT feeling FROM domain_values WHERE amount = 0.1")).rows,
    ).toEqual([{ feeling: "happy" }]);
    await database.execute(
      "INSERT INTO domain_values (id, feeling) VALUES ('00000000-0000-0000-0000-000000000001', 'sad')",
    );
    expect(
      (await database.query("SELECT feeling FROM domain_values ORDER BY feeling")).rows,
    ).toEqual([{ feeling: "sad" }, { feeling: "happy" }]);
    await expect(
      database.execute(
        "INSERT INTO domain_values (id, feeling) VALUES ('00000000-0000-0000-0000-000000000002', 'unknown')",
      ),
    ).rejects.toThrow("not a value of enum mood");
    expect((await database.listTables()).map(({ name }) => name)).toEqual(["domain_values"]);
  });

  it("uses a composite PRIMARY KEY as hidden row identity", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE receipts (shop INTEGER, receipt INTEGER, total REAL, PRIMARY KEY (shop, receipt))",
    );
    await database.execute("INSERT INTO receipts VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)");
    await expect(database.execute("INSERT INTO receipts VALUES (1, 2, 99)")).rejects.toThrow(
      "receipts.(shop, receipt)",
    );
    expect((await database.query("SELECT * FROM receipts ORDER BY shop, receipt")).rows).toEqual([
      { shop: 1, receipt: 1, total: 10 },
      { shop: 1, receipt: 2, total: 20 },
      { shop: 2, receipt: 1, total: 30 },
    ]);
    expect(await database.readTable("receipts")).toEqual([
      { shop: 1, receipt: 1, total: 10 },
      { shop: 1, receipt: 2, total: 20 },
      { shop: 2, receipt: 1, total: 30 },
    ]);
    // The physical tuple locator is neither a public projection nor a MATCH(*) document field.
    expect(
      (await database.query("SELECT * FROM receipts WHERE MATCH(*) AGAINST 'bff*'")).rows,
    ).toEqual([]);
    await database.execute("UPDATE receipts SET total = total + 5 WHERE shop = 1 AND receipt = 2");
    await expect(
      database.execute("UPDATE receipts SET receipt = 3 WHERE shop = 1"),
    ).rejects.toThrow("Primary key column cannot be updated");
    await database.execute("DELETE FROM receipts WHERE shop = 2 AND receipt = 1");
    expect(
      (await database.query("SELECT shop, receipt, total FROM receipts ORDER BY shop, receipt"))
        .rows,
    ).toEqual([
      { shop: 1, receipt: 1, total: 10 },
      { shop: 1, receipt: 2, total: 25 },
    ]);
    expect((await database.listTables())[0]?.columns.map(({ name }) => name)).toEqual([
      "shop",
      "receipt",
      "total",
    ]);
  });

  it("enforces composite FOREIGN KEY tuples and cascades with their parent", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE receipts (shop INTEGER, receipt INTEGER, PRIMARY KEY (shop, receipt))",
    );
    await database.execute(
      "CREATE TABLE lines (id INTEGER PRIMARY KEY, shop INTEGER, receipt INTEGER, " +
        "FOREIGN KEY (shop, receipt) REFERENCES receipts (shop, receipt) ON DELETE CASCADE)",
    );
    await database.execute("INSERT INTO receipts VALUES (1, 1), (1, 2)");
    await database.execute("INSERT INTO lines VALUES (10, 1, 1), (11, 1, 2), (12, NULL, 99)");
    await expect(database.execute("INSERT INTO lines VALUES (13, 2, 1)")).rejects.toThrow(
      "FOREIGN KEY lines_shop_receipt_fkey",
    );
    await database.execute("UPDATE lines SET receipt = 2 WHERE id = 10");
    await expect(database.execute("UPDATE lines SET shop = 2 WHERE id = 10")).rejects.toThrow(
      "FOREIGN KEY lines_shop_receipt_fkey",
    );
    await database.execute("DELETE FROM receipts WHERE shop = 1 AND receipt = 1");
    expect((await database.query("SELECT id, shop, receipt FROM lines ORDER BY id")).rows).toEqual([
      { id: 10, shop: 1, receipt: 2 },
      { id: 11, shop: 1, receipt: 2 },
      { id: 12, shop: null, receipt: 99 },
    ]);
  });
});

describe("ON CONFLICT", () => {
  it("addresses composite primary keys for NOTHING, REPLACE, and conditional UPDATE", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE receipts (shop INTEGER, receipt INTEGER, total INTEGER, PRIMARY KEY (shop, receipt))",
    );
    await database.execute("INSERT INTO receipts VALUES (1, 1, 10)");
    await database.execute(
      "INSERT INTO receipts VALUES (1, 1, 20), (1, 2, 30) ON CONFLICT (shop, receipt) DO NOTHING",
    );
    await database.execute(
      "INSERT INTO receipts VALUES (1, 1, 5) ON CONFLICT (shop, receipt) DO UPDATE SET total = EXCLUDED.total WHERE EXCLUDED.total > receipts.total",
    );
    await database.execute(
      "INSERT INTO receipts VALUES (1, 1, 40) ON CONFLICT (shop, receipt) DO REPLACE",
    );
    expect((await database.query("SELECT * FROM receipts ORDER BY shop, receipt")).rows).toEqual([
      { shop: 1, receipt: 1, total: 40 },
      { shop: 1, receipt: 2, total: 30 },
    ]);
  });

  it("DO REPLACE performs a whole-row upsert, including key-only tables", async () => {
    const database = await seeded();
    const replaced = await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 99) ON CONFLICT (name) DO REPLACE RETURNING *",
    );
    expect(replaced).toMatchObject({
      kind: "insert",
      rowCount: 1,
      returnedRows: [{ name: "Ada", score: 99 }],
    });

    await database.execute("CREATE TABLE singleton (id REAL PRIMARY KEY)");
    await database.execute("INSERT INTO singleton (id) VALUES (1)");
    await expect(
      database.execute(
        "INSERT INTO singleton (id) VALUES (1) ON CONFLICT (id) DO REPLACE RETURNING *",
      ),
    ).resolves.toMatchObject({ kind: "insert", rowCount: 1, returnedRows: [{ id: 1 }] });
  });

  it("DO NOTHING skips existing keys and returns only inserted rows", async () => {
    const database = await seeded();
    const result = await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 99), ('Linus', 1), ('Linus', 2) ON CONFLICT (name) DO NOTHING RETURNING *",
    );
    expect(result).toMatchObject({
      kind: "insert",
      rowCount: 1,
      returnedRows: [{ name: "Linus", score: 1 }],
    });
    const ada = await database.query("SELECT score FROM people WHERE name = 'Ada'");
    expect(ada.rows).toEqual([{ score: 10 }]);
  });

  it("DO UPDATE SET EXCLUDED upserts whole rows", async () => {
    const database = await seeded();
    const result = await database.execute(
      "INSERT INTO people (name, score) VALUES ('Grace', 99), ('Marie', 7) ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score",
    );
    expect(result).toMatchObject({ kind: "insert", rowCount: 2 });
    const rows = await database.query("SELECT name, score FROM people ORDER BY name");
    expect(rows.rows).toEqual([
      { name: "Ada", score: 10 },
      { name: "Grace", score: 99 },
      { name: "Marie", score: 7 },
    ]);
  });

  it("DO UPDATE with a column subset merges only those columns", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "p",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
        { name: "city", type: "string", nullable: true },
      ],
    });
    await database.execute(
      "INSERT INTO p (name, score, city) VALUES ('Ada', 10, 'London'), ('Grace', 25, 'DC')",
    );
    const merged = await database.execute(
      "INSERT INTO p (name, score, city) VALUES ('Ada', 99, 'Paris'), ('Linus', 1, 'Helsinki') ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score RETURNING *",
    );
    expect(merged).toMatchObject({
      kind: "insert",
      rowCount: 2,
      returnedRows: [
        // Ada keeps her stored city: only score was assigned from EXCLUDED.
        { name: "Ada", score: 99, city: "London" },
        { name: "Linus", score: 1, city: "Helsinki" },
      ],
    });
    const scoreOnly = await database.execute(
      "INSERT INTO p (name, score, city) VALUES ('Grace', 30, 'ignored') ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score RETURNING score",
    );
    expect(scoreOnly).toMatchObject({ returnedRows: [{ score: 30 }] });
    await database.execute(
      "INSERT INTO p (name, score) VALUES ('Grace', 2) ON CONFLICT (name) DO UPDATE SET score = score + EXCLUDED.score, city = 'Arlington'",
    );
    const rows = await database.query("SELECT name, score, city FROM p ORDER BY name");
    expect(rows.rows).toEqual([
      { name: "Ada", score: 99, city: "London" },
      { name: "Grace", score: 32, city: "Arlington" },
      { name: "Linus", score: 1, city: "Helsinki" },
    ]);
    await expect(
      database.execute(
        "INSERT INTO p (name, score) VALUES ('x', 1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name",
      ),
    ).rejects.toThrow("cannot reassign the conflict key");
  });

  it("rolls back conflicting updates when a fresh row fails validation", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "atomic_upsert",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "required", type: "string" },
        { name: "score", type: "number" },
        { name: "tag", type: "string", nullable: true },
      ],
    });
    await database.execute(
      "INSERT INTO atomic_upsert (id, required, score, tag) VALUES (1, 'kept', 10, NULL)",
    );

    await expect(
      database.execute(
        "INSERT INTO atomic_upsert (id, score) VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET score = score + EXCLUDED.score",
      ),
    ).rejects.toThrow("required[0] cannot be null");

    await expect(
      database.execute(
        "INSERT INTO atomic_upsert (id, score, tag) VALUES (1, 20, 'changed'), (2, 30, 'new') ON CONFLICT (id) DO UPDATE SET score = EXCLUDED.score",
      ),
    ).rejects.toThrow("cannot be null");

    expect(
      (await database.query("SELECT id, required, score, tag FROM atomic_upsert ORDER BY id")).rows,
    ).toEqual([{ id: 1, required: "kept", score: 10, tag: null }]);
  });

  it("pads unlisted insert columns with NULL and returns them", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "p",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
        { name: "city", type: "string", nullable: true },
      ],
    });
    const inserted = await database.execute(
      "INSERT INTO p (name, score) VALUES ('Ada', 1) RETURNING *",
    );
    expect(inserted).toMatchObject({
      returnedRows: [{ name: "Ada", score: 1, city: null }],
    });
    // A non-nullable column without a default still rejects the batch.
    await expect(database.execute("INSERT INTO p (name) VALUES ('Grace')")).rejects.toThrow(
      "cannot be null",
    );
    await expect(database.execute("INSERT INTO p (name, nope) VALUES ('x', 1)")).rejects.toThrow(
      "INSERT column does not exist: nope",
    );
  });

  it("evaluates conflict expressions against the target and EXCLUDED rows", async () => {
    const database = await seeded();
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('x', 1) ON CONFLICT (score) DO NOTHING",
      ),
    ).rejects.toThrow("unique key column");
    await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 3) ON CONFLICT (name) DO UPDATE SET score = score + EXCLUDED.score + ?",
      [2],
    );
    expect((await database.query("SELECT score FROM people WHERE name = 'Ada'")).rows).toEqual([
      { score: 15 },
    ]);
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('x', 1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name",
      ),
    ).rejects.toThrow("cannot reassign the conflict key");
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('x', 1) ON CONFLICT (name) DO UPDATE SET nope = EXCLUDED.nope",
      ),
    ).rejects.toThrow("column does not exist: nope");
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('Ada', 1), ('Ada', 2) ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score",
      ),
    ).rejects.toThrow("cannot affect name Ada twice");
  });

  it("applies DO UPDATE WHERE after finding the conflict and returns only affected rows", async () => {
    const database = await seeded();
    const result = await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 9), ('Grace', 30), ('Linus', 4) " +
        "ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score " +
        "WHERE EXCLUDED.score > people.score RETURNING *",
    );
    expect(result).toMatchObject({
      kind: "insert",
      rowCount: 2,
      returnedRows: [
        { name: "Grace", score: 30 },
        { name: "Linus", score: 4 },
      ],
    });
    expect((await database.query("SELECT name, score FROM people ORDER BY name")).rows).toEqual([
      { name: "Ada", score: 10 },
      { name: "Grace", score: 30 },
      { name: "Linus", score: 4 },
    ]);

    const unknown = await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 100) " +
        "ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score WHERE NULL RETURNING score",
    );
    expect(unknown).toMatchObject({ kind: "insert", rowCount: 0, returnedRows: [] });
    expect((await database.query("SELECT score FROM people WHERE name = 'Ada'")).rows).toEqual([
      { score: 10 },
    ]);

    await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 13) " +
        "ON CONFLICT (name) DO UPDATE SET score = EXCLUDED.score WHERE EXCLUDED.score > ?",
      [12],
    );
    expect((await database.query("SELECT score FROM people WHERE name = 'Ada'")).rows).toEqual([
      { score: 13 },
    ]);
  });

  it("fills EXCLUDED defaults before evaluating conflict expressions", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "defaulted_upsert",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "qty", type: "number", defaultValue: { kind: "literal", value: 5 } },
      ],
    });
    await database.execute("INSERT INTO defaulted_upsert (name, qty) VALUES ('existing', 2)");
    await database.execute(
      "INSERT INTO defaulted_upsert (name) VALUES ('existing'), ('fresh') ON CONFLICT (name) DO UPDATE SET qty = CASE WHEN EXCLUDED.qty > qty THEN EXCLUDED.qty ELSE qty + 1 END",
    );
    expect(
      (await database.query("SELECT name, qty FROM defaulted_upsert ORDER BY name")).rows,
    ).toEqual([
      { name: "existing", qty: 5 },
      { name: "fresh", qty: 5 },
    ]);
  });
});

describe("execute engine controls", () => {
  async function eventsDatabase(
    store: MemoryBlockStore,
    rowCount: number,
  ): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 32 });
    await database.createTable({
      name: "events",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number", integer: true },
        { name: "label", type: "string" },
      ],
    });
    await database.insertBatch("events", {
      columns: {
        id: Array.from({ length: rowCount }, (_, index) => index),
        label: Array.from({ length: rowCount }, (_, index) => `event-${String(index)}`),
      },
    });
    return database;
  }

  it("reports stats for buffered SELECTs and honors memoize", async () => {
    const database = await eventsDatabase(new MemoryBlockStore(), 512);
    const peaks: number[] = [];
    const onStats = ({ peakMemoryBytes }: { peakMemoryBytes: number }): void => {
      peaks.push(peakMemoryBytes);
    };
    const sql = "SELECT id, label FROM events ORDER BY label";
    const first = await database.execute(sql, undefined, { onStats });
    if (first.kind !== "rows") throw new Error("expected a rows result");
    expect(first.result.rows).toHaveLength(512);
    await database.execute(sql, undefined, { onStats });
    await database.execute(sql, undefined, { onStats, memoize: false });
    expect(peaks).toHaveLength(3);
    expect(peaks[0]).toBeGreaterThan(0);
    // A memo hit runs nothing and reports zero; memoize: false computes again.
    expect(peaks[1]).toBe(0);
    expect(peaks[2]).toBeGreaterThan(0);
    await database.close();
  });

  it("applies an execution memory budget to buffered SELECTs", async () => {
    const database = await eventsDatabase(new MemoryBlockStore(), 64);
    await expect(
      database.execute("SELECT id, label FROM events ORDER BY label", undefined, {
        executionMemoryBudgetBytes: 1,
        memoize: false,
      }),
    ).rejects.toBeInstanceOf(QueryMemoryBudgetError);
    await database.close();
  });

  it("aborts a buffered SELECT between storage batches and stops reading", async () => {
    class AbortingStore extends MemoryBlockStore {
      blockReads = 0;
      abortAtRead = Number.POSITIVE_INFINITY;
      readonly controller = new AbortController();

      override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
        this.blockReads += ids.length;
        if (this.blockReads >= this.abortAtRead) {
          this.controller.abort(new Error("stop buffered select"));
        }
        return super.getBlocks(ids);
      }
    }
    const store = new AbortingStore();
    const writer = await eventsDatabase(store, 5_000);

    // A cold database over the same store measures what a complete scan reads.
    store.blockReads = 0;
    const complete = new MinnowDatabase(store);
    const full = await complete.execute("SELECT id, label FROM events", undefined, {
      memoize: false,
    });
    if (full.kind !== "rows") throw new Error("expected a rows result");
    expect(full.result.rows).toHaveLength(5_000);
    const fullReads = store.blockReads;
    await complete.close();

    // Abort from inside the first storage read; the engine stops at the next batch boundary.
    store.blockReads = 0;
    store.abortAtRead = 1;
    const database = new MinnowDatabase(store);
    await expect(
      database.execute("SELECT id, label FROM events", undefined, {
        signal: store.controller.signal,
        memoize: false,
      }),
    ).rejects.toThrow("stop buffered select");
    const readsAtRejection = store.blockReads;
    expect(readsAtRejection).toBeGreaterThan(0);
    expect(readsAtRejection).toBeLessThan(fullReads);
    await new Promise((resolve) => setTimeout(resolve, 10));
    // Nothing kept reading in the background after the rejection.
    expect(store.blockReads).toBe(readsAtRejection);
    await database.close();
    await writer.close();
  });

  it("checks the signal before a mutation runs", async () => {
    const database = await seeded();
    const controller = new AbortController();
    const reason = new Error("stop before insert");
    controller.abort(reason);
    await expect(
      database.execute("INSERT INTO people (name, score) VALUES ('Linus', 5)", undefined, {
        signal: controller.signal,
      }),
    ).rejects.toBe(reason);
    expect((await database.query("SELECT count(*) AS n FROM people")).rows).toEqual([{ n: 2 }]);
  });
});
