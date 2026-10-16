/**
 * An OR whose every branch an index serves is narrowed to the union of the branches' candidates.
 * The executor still evaluates the OR on every candidate, so each answer must equal the same
 * statement written so that no index can serve it (`col + 0`, `col || ''`), across equality, IN,
 * and range branches, branches that are conjunctions, unservable branches, NULLs, and a mutation
 * history.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import type { QueryValue } from "./query.js";

async function itemsDatabase(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 64,
    autoCompact: false,
  });
  await database.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, owner INTEGER, label TEXT NOT NULL, price REAL NOT NULL, note TEXT)",
  );
  await database.execute("CREATE INDEX items_owner ON items (owner)");
  await database.execute("CREATE INDEX items_label ON items (label)");
  await database.execute("CREATE INDEX items_price ON items (price)");
  for (let batch = 0; batch < 4; batch += 1) {
    await database.insertBatch(
      "items",
      Array.from({ length: 250 }, (_, offset) => {
        const id = batch * 250 + offset + 1;
        return {
          id,
          owner: id % 31 === 0 ? null : id % 40,
          label: `label-${String(id % 57)}`,
          price: (id % 200) / 4,
          note: id % 3 === 0 ? null : `note-${String(id % 5)}`,
        };
      }),
    );
  }
  return database;
}

async function differential(
  database: MinnowDatabase,
  sql: string,
  unindexed: string,
  params: QueryValue[] = [],
): Promise<void> {
  const served = await database.query(sql, { params, memoize: false });
  const scanned = await database.query(unindexed, { params, memoize: false });
  const sorted = (rows: ReadonlyArray<Record<string, QueryValue>>): string[] =>
    rows.map((row) => JSON.stringify(row)).sort();
  expect(sorted(served.rows)).toEqual(sorted(scanned.rows));
}

describe("index-served disjunctions", () => {
  it("answer exactly as an unindexed scan", async () => {
    const database = await itemsDatabase();
    try {
      for (const [sql, unindexed, params] of [
        [
          "SELECT id FROM items WHERE owner = ? OR label = ?",
          "SELECT id FROM items WHERE owner + 0 = ? OR label || '' = ?",
          [7, "label-3"],
        ],
        [
          "SELECT COUNT(*) AS n FROM items WHERE owner = ? OR label = ? OR owner = ?",
          "SELECT COUNT(*) AS n FROM items WHERE owner + 0 = ? OR label || '' = ? OR owner + 0 = ?",
          [7, "label-3", 12],
        ],
        [
          "SELECT id FROM items WHERE owner IN (1, 2) OR price BETWEEN ? AND ?",
          "SELECT id FROM items WHERE owner + 0 IN (1, 2) OR price + 0 BETWEEN ? AND ?",
          [10, 11],
        ],
        [
          "SELECT id FROM items WHERE (owner = ? AND price > ?) OR (label = ? AND note IS NULL)",
          "SELECT id FROM items WHERE (owner + 0 = ? AND price + 0 > ?) OR (label || '' = ? AND note IS NULL)",
          [5, 20, "label-9"],
        ],
        [
          "SELECT id FROM items WHERE label = ? AND (owner = ? OR owner = ?)",
          "SELECT id FROM items WHERE label || '' = ? AND (owner + 0 = ? OR owner + 0 = ?)",
          ["label-1", 1, 18],
        ],
        // A branch no index serves keeps the whole OR on the scan.
        [
          "SELECT id FROM items WHERE owner = ? OR note = ?",
          "SELECT id FROM items WHERE owner + 0 = ? OR note || '' = ?",
          [7, "note-2"],
        ],
        [
          "SELECT id FROM items WHERE owner = ? OR label = ?",
          "SELECT id FROM items WHERE owner + 0 = ? OR label || '' = ?",
          [null, "label-4"],
        ],
        [
          "SELECT id FROM items WHERE owner = ? OR label = ?",
          "SELECT id FROM items WHERE owner + 0 = ? OR label || '' = ?",
          [999, "nothing"],
        ],
        [
          "SELECT id FROM items WHERE NOT (owner = ? OR label = ?)",
          "SELECT id FROM items WHERE NOT (owner + 0 = ? OR label || '' = ?)",
          [7, "label-3"],
        ],
      ] as const) {
        await differential(database, sql, unindexed, [...params]);
      }
    } finally {
      await database.close();
    }
  });

  it("narrow a LIKE with a literal prefix through the index", async () => {
    const database = await itemsDatabase();
    try {
      await database.insertBatch("items", [
        { id: 5001, owner: 1, label: "label-1%x", price: 1, note: null },
        { id: 5002, owner: 1, label: "label-1_y", price: 1, note: null },
        { id: 5003, owner: 1, label: "label-é1", price: 1, note: null },
        { id: 5004, owner: 1, label: "LABEL-1", price: 1, note: null },
      ]);
      for (const pattern of [
        "label-1%",
        "label-1_",
        "label-%1",
        "label-1",
        "label-1\\%%",
        "label-1\\_%",
        "label-é%",
        "%-1",
        "label-1%x",
        "l%",
      ]) {
        await differential(
          database,
          "SELECT id FROM items WHERE label LIKE ?",
          "SELECT id FROM items WHERE label || '' LIKE ?",
          [pattern],
        );
        await differential(
          database,
          "SELECT id FROM items WHERE label NOT LIKE ? AND owner = 1",
          "SELECT id FROM items WHERE label || '' NOT LIKE ? AND owner + 0 = 1",
          [pattern],
        );
        await differential(
          database,
          "SELECT id FROM items WHERE label ILIKE ?",
          "SELECT id FROM items WHERE label || '' ILIKE ?",
          [pattern],
        );
      }
      await differential(
        database,
        "SELECT id FROM items WHERE label LIKE 'label-1!%%' ESCAPE '!'",
        "SELECT id FROM items WHERE label || '' LIKE 'label-1!%%' ESCAPE '!'",
      );
    } finally {
      await database.close();
    }
  });

  it("stay exact over a mutation history", async () => {
    const database = await itemsDatabase();
    try {
      await database.execute("UPDATE items SET owner = 7 WHERE id IN (1, 2, 3)");
      await database.execute("UPDATE items SET label = 'label-3' WHERE id = 400");
      await database.execute("DELETE FROM items WHERE owner = 7 AND id > 900");
      for (let round = 0; round < 2; round += 1) {
        await differential(
          database,
          "SELECT id, owner, label FROM items WHERE owner = ? OR label = ?",
          "SELECT id, owner, label FROM items WHERE owner + 0 = ? OR label || '' = ?",
          [7, "label-3"],
        );
      }
    } finally {
      await database.close();
    }
  });
});
