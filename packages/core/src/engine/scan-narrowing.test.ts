/**
 * Scans narrow with binary search when a predicate compares a column stored in ascending
 * order. Narrowing must never change an answer, so every case here compares the engine
 * against a plain JavaScript filter over the same rows — including the shapes that must fall
 * back to a full scan: unsorted columns, nulls, NaN, and descending storage.
 *
 * Tables are deliberately larger than one scan batch (2048 rows). Below that size the scan
 * never narrows, so a smaller table would exercise none of this.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type DatabaseRow } from "./database.js";

const ROWS = 5_000;

interface SourceRow {
  id: number;
  sorted_at: Date;
  shuffled: number;
  nullable: number | null;
  amount: number;
}

/**
 * `id` and `sorted_at` are stored ascending, `shuffled` is not, and `nullable` is ascending
 * apart from the nulls sprinkled through it — one table covering both sides of the check.
 */
function sourceRows(): SourceRow[] {
  return Array.from({ length: ROWS }, (_, index) => ({
    id: index + 1,
    sorted_at: new Date(Date.UTC(2026, 0, 1) + index * 86_400_000),
    shuffled: ((index * 2_654_435_761) % ROWS) + 1,
    nullable: index % 97 === 0 ? null : index + 1,
    amount: (index % 250) + 0.5,
  }));
}

async function openDatabase(rows: readonly SourceRow[]): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 4_096 });
  await database.createTable({
    name: "events",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "sorted_at", type: "datetime" },
      { name: "shuffled", type: "number" },
      { name: "nullable", type: "number", nullable: true },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("events", {
    columns: {
      id: rows.map((row) => row.id),
      sorted_at: rows.map((row) => row.sorted_at),
      shuffled: rows.map((row) => row.shuffled),
      nullable: rows.map((row) => row.nullable),
      amount: rows.map((row) => row.amount),
    },
  });
  return database;
}

function ids(result: readonly DatabaseRow[]): number[] {
  return result.map((row) => Number(row.id));
}

describe("ascending-column scan narrowing", () => {
  it("answers comparisons against an ascending key exactly as a full scan would", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    const cases: Array<{ sql: string; keep: (row: SourceRow) => boolean }> = [
      { sql: "WHERE id = 3137", keep: (row) => row.id === 3137 },
      { sql: "WHERE id = 999999", keep: (row) => row.id === 999999 },
      { sql: "WHERE id = 1", keep: (row) => row.id === 1 },
      { sql: `WHERE id = ${String(ROWS)}`, keep: (row) => row.id === ROWS },
      { sql: "WHERE id > 4990", keep: (row) => row.id > 4990 },
      { sql: "WHERE id >= 4990", keep: (row) => row.id >= 4990 },
      { sql: "WHERE id < 7", keep: (row) => row.id < 7 },
      { sql: "WHERE id <= 7", keep: (row) => row.id <= 7 },
      { sql: "WHERE id >= 2000 AND id < 2010", keep: (row) => row.id >= 2000 && row.id < 2010 },
      { sql: "WHERE id > 4000 AND id < 100", keep: () => false },
      { sql: "WHERE id != 42", keep: (row) => row.id !== 42 },
      { sql: "WHERE id IN (1, 2, 3, 5, 8)", keep: (row) => [1, 2, 3, 5, 8].includes(row.id) },
      {
        sql: "WHERE id IN (11, 4999, 2500)",
        keep: (row) => [11, 4999, 2500].includes(row.id),
      },
      { sql: "WHERE id IN (0, 99999)", keep: () => false },
      { sql: "WHERE id NOT IN (1, 2, 3)", keep: (row) => ![1, 2, 3].includes(row.id) },
    ];
    for (const { sql, keep } of cases) {
      const result = await database.query(`SELECT id FROM events ${sql} ORDER BY id`, {
        memoize: false,
      });
      expect(ids(result.rows), sql).toEqual(rows.filter(keep).map((row) => row.id));
    }
  });

  it("narrows a datetime column stored in ascending order", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    const cutoff = rows[3_000]?.sorted_at ?? new Date(0);
    const result = await database.query(
      "SELECT id FROM events WHERE sorted_at >= ? ORDER BY id LIMIT 5",
      { params: [cutoff], memoize: false },
    );
    expect(ids(result.rows)).toEqual(
      rows
        .filter((row) => row.sorted_at.getTime() >= cutoff.getTime())
        .slice(0, 5)
        .map((row) => row.id),
    );
  });

  it("scans in full when the compared column is not stored in order", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    for (const sql of [
      "WHERE shuffled = 1234",
      "WHERE shuffled < 40",
      "WHERE shuffled IN (7, 4321, 88)",
    ]) {
      const result = await database.query(`SELECT id FROM events ${sql} ORDER BY id`, {
        memoize: false,
      });
      const expected = rows
        .filter((row) => {
          if (sql.includes("= 1234")) return row.shuffled === 1234;
          if (sql.includes("< 40")) return row.shuffled < 40;
          return [7, 4321, 88].includes(row.shuffled);
        })
        .map((row) => row.id);
      expect(ids(result.rows), sql).toEqual(expected);
      expect(expected.length).toBeGreaterThan(0);
    }
  });

  it("keeps nulls out of the ordering it searches", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    // `nullable` ascends apart from its nulls; a binary search over it would skip real rows,
    // so this column must fall back to the scan.
    for (const [sql, keep] of [
      ["WHERE nullable >= 4900", (row: SourceRow) => row.nullable !== null && row.nullable >= 4900],
      ["WHERE nullable = 4901", (row: SourceRow) => row.nullable === 4901],
      ["WHERE nullable < 12", (row: SourceRow) => row.nullable !== null && row.nullable < 12],
      [
        "WHERE nullable IN (98, 195, 400)",
        (row: SourceRow) => row.nullable !== null && [98, 195, 400].includes(row.nullable),
      ],
    ] as const) {
      const result = await database.query(`SELECT id FROM events ${sql} ORDER BY id`, {
        memoize: false,
      });
      expect(ids(result.rows), sql).toEqual(rows.filter(keep).map((row) => row.id));
    }
  });

  it("finds every row of a repeated value in a non-decreasing column", async () => {
    // Ties are where a lower/upper bound pair earns its keep: each value covers a run of
    // hundreds of rows, and an off-by-one bound would clip the run's first or last member.
    const rows = sourceRows().map((row, index) => ({ ...row, amount: Math.floor(index / 500) }));
    const database = await openDatabase(rows);
    for (const [sql, keep] of [
      ["WHERE amount = 0", (row: SourceRow) => row.amount === 0],
      ["WHERE amount = 4", (row: SourceRow) => row.amount === 4],
      ["WHERE amount = 9", (row: SourceRow) => row.amount === 9],
      ["WHERE amount > 7", (row: SourceRow) => row.amount > 7],
      ["WHERE amount <= 1", (row: SourceRow) => row.amount <= 1],
      ["WHERE amount IN (2, 6)", (row: SourceRow) => [2, 6].includes(row.amount)],
    ] as const) {
      const result = await database.query(`SELECT id FROM events ${sql} ORDER BY id`, {
        memoize: false,
      });
      expect(ids(result.rows), sql).toEqual(rows.filter(keep).map((row) => row.id));
    }
  });

  it("scans in full when the key is stored in descending order", async () => {
    const rows = sourceRows()
      .map((row, index) => ({ ...row, id: ROWS - index }))
      .map((row) => ({ ...row, nullable: row.id }));
    const database = await openDatabase(rows);
    const result = await database.query("SELECT id FROM events WHERE id <= 4 ORDER BY id", {
      memoize: false,
    });
    expect(ids(result.rows)).toEqual([1, 2, 3, 4]);
  });

  it("narrows underneath aggregates, ordering, and paging", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    const window = rows.filter((row) => row.id >= 1_500 && row.id <= 2_500);

    const counted = await database.query(
      "SELECT COUNT(*) AS row_count, SUM(amount) AS total FROM events WHERE id >= 1500 AND id <= 2500",
      { memoize: false },
    );
    expect(counted.rows[0]?.row_count).toBe(window.length);
    expect(Number(counted.rows[0]?.total)).toBeCloseTo(
      window.reduce((total, row) => total + row.amount, 0),
      6,
    );

    const paged = await database.query(
      "SELECT id FROM events WHERE id >= 1500 AND id <= 2500 ORDER BY id DESC LIMIT 3 OFFSET 2",
      { memoize: false },
    );
    expect(ids(paged.rows)).toEqual(
      window
        .map((row) => row.id)
        .reverse()
        .slice(2, 5),
    );
  });

  it("narrows a join's scan side without dropping matches", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    await database.createTable({
      name: "labels",
      uniqueKey: "label_id",
      columns: [
        { name: "label_id", type: "number" },
        { name: "name", type: "string" },
      ],
    });
    await database.insertBatch("labels", {
      columns: {
        label_id: Array.from({ length: 250 }, (_, index) => index),
        name: Array.from({ length: 250 }, (_, index) => `label-${String(index)}`),
      },
    });
    const result = await database.query(
      "SELECT e.id AS id, l.name AS name FROM events e JOIN labels l ON l.label_id = e.amount - 0.5 WHERE e.id >= 4990 ORDER BY e.id",
      { memoize: false },
    );
    expect(result.rows.map((row) => `${String(row.id)}:${String(row.name)}`)).toEqual(
      rows
        .filter((row) => row.id >= 4990)
        .map((row) => `${String(row.id)}:label-${String(row.amount - 0.5)}`),
    );
  });

  it("narrows against rows added after the first segment", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    const appended = Array.from({ length: 3_000 }, (_, index) => ROWS + index + 1);
    await database.insertBatch("events", {
      columns: {
        id: appended,
        sorted_at: appended.map((id) => new Date(Date.UTC(2026, 0, 1) + id * 86_400_000)),
        shuffled: appended.map((id) => id),
        nullable: appended.map((id) => id),
        amount: appended.map((id) => (id % 250) + 0.5),
      },
    });
    const result = await database.query(
      "SELECT id FROM events WHERE id >= 4998 AND id <= 5003 ORDER BY id",
      { memoize: false },
    );
    expect(ids(result.rows)).toEqual([4998, 4999, 5000, 5001, 5002, 5003]);
  });

  it("reports zone-map pruning for a literal list", async () => {
    const database = await openDatabase(sourceRows());
    const plan = await database.explain("SELECT id FROM events WHERE id IN (1, 2, 3)");
    expect(plan).toContain("zone-map pruning applies");
  });

  it("answers lists that hold a NULL or a column reference", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    // A NULL member matches nothing and drops out of the pruning set; a column member is not
    // a literal at all, so the list stays unprunable and the scan decides every row.
    for (const [sql, keep] of [
      ["WHERE id IN (1, NULL, 3)", (row: SourceRow) => [1, 3].includes(row.id)],
      ["WHERE id IN (7, amount)", (row: SourceRow) => row.id === 7 || row.id === row.amount],
    ] as const) {
      const result = await database.query(`SELECT id FROM events ${sql} ORDER BY id`, {
        memoize: false,
      });
      expect(ids(result.rows), sql).toEqual(rows.filter(keep).map((row) => row.id));
    }
  });

  it("narrows against a key column left ascending by updates and deletes", async () => {
    const rows = sourceRows();
    const database = await openDatabase(rows);
    await database.execute("DELETE FROM events WHERE id >= 2000 AND id < 2010");
    await database.execute("UPDATE events SET amount = 12.5 WHERE id = 2500");
    const result = await database.query(
      "SELECT id, amount FROM events WHERE id >= 1998 AND id <= 2011 ORDER BY id",
      { memoize: false },
    );
    expect(ids(result.rows)).toEqual([1998, 1999, 2010, 2011]);

    const updated = await database.query("SELECT amount FROM events WHERE id = 2500", {
      memoize: false,
    });
    expect(updated.rows[0]?.amount).toBe(12.5);
  });
});
