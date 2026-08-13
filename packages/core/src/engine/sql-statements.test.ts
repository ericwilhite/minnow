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

describe("ON CONFLICT", () => {
  it("DO NOTHING skips existing keys and returns only inserted rows", async () => {
    const database = await seeded();
    const result = await database.execute(
      "INSERT INTO people (name, score) VALUES ('Ada', 99), ('Linus', 1) ON CONFLICT (name) DO NOTHING RETURNING *",
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

  it("rejects unsupported conflict shapes explicitly", async () => {
    const database = await seeded();
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('x', 1) ON CONFLICT (score) DO NOTHING",
      ),
    ).rejects.toThrow("unique key column");
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('x', 1) ON CONFLICT (name) DO UPDATE SET score = score + 1",
      ),
    ).rejects.toThrow("column = EXCLUDED.column");
    await expect(
      database.execute(
        "INSERT INTO people (name, score) VALUES ('x', 1) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name",
      ),
    ).rejects.toThrow("missing: score");
  });
});
