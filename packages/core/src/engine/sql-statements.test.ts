import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

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
      "INSERT INTO people (name, score) VALUES (?, ?) RETURNING *",
      ["Linus", 5],
    );
    expect(inserted).toMatchObject({
      kind: "insert",
      returnedRows: [{ name: "Linus", score: 5 }],
    });
    const updated = await database.execute(
      "UPDATE people SET score = score + 1 WHERE name = 'Ada' RETURNING name, score",
    );
    expect(updated).toMatchObject({ kind: "update", returnedRows: [{ name: "Ada", score: 11 }] });
    const deleted = await database.execute("DELETE FROM people WHERE score > 20 RETURNING name");
    expect(deleted).toMatchObject({ kind: "delete", returnedRows: [{ name: "Grace" }] });
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

  it("rejects unknown types and duplicate unique keys", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await expect(database.execute("CREATE TABLE bad (x JSONB)")).rejects.toThrow(
      "Unsupported column type: JSONB",
    );
    await expect(
      database.execute("CREATE TABLE bad (a INTEGER PRIMARY KEY, b TEXT UNIQUE)"),
    ).rejects.toThrow("one unique key column");
  });
});

describe("ON CONFLICT", () => {
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
