/**
 * Ordered row selection narrows a top-N, keyset page, or MIN/MAX scan to the rows that can
 * decide its answer. Its one obligation is exactness: every statement it serves must answer
 * exactly as the full scan does — ties at the limit, OFFSET, NULL placement, alias shadowing,
 * datetime bounds — and every statement it cannot prove must fall back. Each statement here runs
 * with the selection enabled and disabled, and the complete results must be identical.
 */
import { afterEach, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import type { QueryValue } from "./query.js";
import { orderedSelectionTestHooks } from "./ordered-selection.js";

afterEach(() => {
  orderedSelectionTestHooks.disabled = false;
});

async function differential(
  database: MinnowDatabase,
  sql: string,
  params: QueryValue[] = [],
): Promise<{ served: boolean; rows: Array<Record<string, QueryValue>> }> {
  orderedSelectionTestHooks.disabled = false;
  const before = orderedSelectionTestHooks.served;
  const narrowed = await database.query(sql, { params, memoize: false });
  const served = orderedSelectionTestHooks.served > before;
  orderedSelectionTestHooks.disabled = true;
  const ordinary = await database.query(sql, { params, memoize: false });
  orderedSelectionTestHooks.disabled = false;
  expect(narrowed.columns).toEqual(ordinary.columns);
  expect(narrowed.rows).toEqual(ordinary.rows);
  return { served, rows: narrowed.rows };
}

const DAY = 86_400_000;

/** Several append segments of small blocks: ascending keys, tied scores, scattered dates. */
async function eventsDatabase(options: { nullScores?: boolean } = {}): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 64,
    autoCompact: false,
  });
  await database.execute(
    `CREATE TABLE events (id INTEGER PRIMARY KEY, score REAL${options.nullScores === true ? "" : " NOT NULL"}, at TIMESTAMP NOT NULL, label TEXT NOT NULL)`,
  );
  for (let batch = 0; batch < 4; batch += 1) {
    await database.insertBatch(
      "events",
      Array.from({ length: 150 }, (_, offset) => {
        const id = batch * 150 + offset + 1;
        return {
          id,
          score: options.nullScores === true && id % 9 === 0 ? null : (id * 37) % 50,
          at: new Date(Date.UTC(2026, 0, 1) + ((id * 7919) % 400) * DAY),
          label: `label-${String(id % 13)}`,
        };
      }),
    );
  }
  return database;
}

describe("ordered row selection", () => {
  it("serves top-N by an ascending key in both directions, with OFFSET and edge limits", async () => {
    const database = await eventsDatabase();
    try {
      for (const sql of [
        "SELECT id, label FROM events ORDER BY id DESC LIMIT 5",
        "SELECT id, label FROM events ORDER BY id LIMIT 5",
        "SELECT id, label FROM events ORDER BY id DESC LIMIT 5 OFFSET 3",
        "SELECT id FROM events ORDER BY id LIMIT 3 OFFSET 598",
        "SELECT id FROM events ORDER BY id DESC LIMIT 1000",
        "SELECT * FROM events ORDER BY id DESC LIMIT 7",
        "SELECT * FROM events ORDER BY events.id DESC LIMIT 2",
        "SELECT e.id FROM events e ORDER BY e.id DESC LIMIT 4",
        "SELECT id AS x, score FROM events ORDER BY x DESC LIMIT 3",
        "SELECT id, label FROM events ORDER BY id DESC, label LIMIT 6",
      ]) {
        const { served } = await differential(database, sql);
        expect(served, sql).toBe(true);
      }
      const { served, rows } = await differential(
        database,
        "SELECT id FROM events ORDER BY id LIMIT 0",
      );
      expect(served).toBe(true);
      expect(rows).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it("serves keyset pages and range-bounded orders", async () => {
    const database = await eventsDatabase();
    try {
      for (const [sql, params] of [
        ["SELECT id, label FROM events WHERE id > ? ORDER BY id LIMIT 10", [137]],
        ["SELECT id, label FROM events WHERE id >= ? ORDER BY id LIMIT 10", [137]],
        ["SELECT id FROM events WHERE id < ? ORDER BY id DESC LIMIT 10", [301]],
        ["SELECT id FROM events WHERE ? < id ORDER BY id LIMIT 4", [590]],
        ["SELECT id FROM events WHERE id BETWEEN ? AND ? ORDER BY id DESC LIMIT 4", [100, 260]],
        ["SELECT id FROM events WHERE id > ? AND id <= ? ORDER BY id LIMIT 50", [10, 20]],
        ["SELECT id FROM events WHERE id = ? ORDER BY id LIMIT 5", [44]],
        ["SELECT id FROM events WHERE id > ? ORDER BY id LIMIT 5", [600]],
        ["SELECT id FROM events WHERE id > ? AND id < ? ORDER BY id LIMIT 5", [50, 40]],
      ] as const) {
        const { served } = await differential(database, sql, [...params]);
        expect(served, sql).toBe(true);
      }
    } finally {
      await database.close();
    }
  });

  it("keeps every row tied with the last one on an unsorted column", async () => {
    const database = await eventsDatabase();
    try {
      for (const sql of [
        "SELECT id, score FROM events ORDER BY score DESC, id LIMIT 6",
        "SELECT id, score FROM events ORDER BY score DESC, id DESC LIMIT 6",
        "SELECT id, score FROM events ORDER BY score, id LIMIT 6 OFFSET 2",
        "SELECT id, score FROM events ORDER BY score DESC LIMIT 40",
        "SELECT id, score FROM events WHERE score >= 20 AND score < 30 ORDER BY score, id LIMIT 9",
        "SELECT id, at FROM events ORDER BY at DESC, id LIMIT 4",
        "SELECT id, at FROM events ORDER BY at, id LIMIT 4",
      ]) {
        const { served } = await differential(database, sql);
        expect(served, sql).toBe(true);
      }
      const { served } = await differential(
        database,
        "SELECT id, at FROM events WHERE at >= ? ORDER BY at, id LIMIT 5",
        [new Date(Date.UTC(2026, 0, 1) + 200 * DAY)],
      );
      expect(served).toBe(true);
    } finally {
      await database.close();
    }
  });

  it("answers MIN and MAX from the rows holding them", async () => {
    const database = await eventsDatabase({ nullScores: true });
    try {
      for (const [sql, params] of [
        ["SELECT MAX(id) AS m FROM events", []],
        ["SELECT MIN(id) AS m FROM events", []],
        ["SELECT MIN(score) AS lo, MAX(score) AS hi FROM events", []],
        ["SELECT MIN(at) AS lo, MAX(id) AS hi FROM events", []],
        ["SELECT MAX(id) AS m FROM events WHERE id < ?", [333]],
        ["SELECT MIN(score) AS m FROM events WHERE score > ?", [17]],
        ["SELECT MAX(id) AS m FROM events WHERE id > ?", [1_000_000]],
        ["SELECT MAX(at) AS m FROM events WHERE at < ?", [new Date(Date.UTC(2026, 3, 1))]],
      ] as const) {
        const { served } = await differential(database, sql, [...params]);
        expect(served, sql).toBe(true);
      }
    } finally {
      await database.close();
    }
  });

  it("falls back for every shape it cannot prove", async () => {
    const database = await eventsDatabase({ nullScores: true });
    try {
      for (const [sql, params] of [
        // NULLs sort into an unconstrained order: first for DESC, and they can fill a short one.
        ["SELECT id, score FROM events ORDER BY score DESC LIMIT 5", []],
        ["SELECT id, score FROM events ORDER BY score NULLS FIRST LIMIT 5", []],
        ["SELECT id, score FROM events ORDER BY score LIMIT 5", []],
        // An alias shadowing the column name orders by the aliased expression.
        ["SELECT label AS id, id AS y FROM events ORDER BY id LIMIT 3", []],
        // Any predicate on another column decides which rows qualify.
        ["SELECT id FROM events WHERE id > ? AND label = 'label-3' ORDER BY id LIMIT 3", [10]],
        ["SELECT id FROM events WHERE id <> ? ORDER BY id LIMIT 3", [1]],
        ["SELECT COUNT(*) AS n, MAX(id) AS m FROM events", []],
        ["SELECT MAX(id) AS m FROM events GROUP BY label", []],
        ["SELECT MAX(id + 1) AS m FROM events", []],
        ["SELECT id FROM events ORDER BY id + 0 DESC LIMIT 3", []],
        ["SELECT DISTINCT label FROM events ORDER BY label LIMIT 3", []],
        ["SELECT id FROM events ORDER BY id DESC", []],
        ["SELECT id FROM events ORDER BY id DESC LIMIT 5000", []],
      ] as const) {
        const { served } = await differential(database, sql, [...params]);
        expect(served, sql).toBe(false);
      }
    } finally {
      await database.close();
    }
  });

  it("counts past the rows ahead of a deep OFFSET page, keeping its tie group whole", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      rowsPerBlock: 512,
      autoCompact: false,
    });
    try {
      await database.execute(
        "CREATE TABLE pages (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, aligned INTEGER NOT NULL, label TEXT NOT NULL)",
      );
      for (let batch = 0; batch < 3; batch += 1) {
        await database.insertBatch(
          "pages",
          Array.from({ length: 2_000 }, (_, offset) => {
            const id = batch * 2_000 + offset + 1;
            // `aligned` ties in runs of 16 that never cross a 512-row block.
            return {
              id,
              bucket: Math.floor(id / 10),
              aligned: Math.floor((id - 1) / 16),
              label: `label-${String(id % 7)}`,
            };
          }),
        );
      }
      for (const [sql, params] of [
        ["SELECT id, label FROM pages ORDER BY id LIMIT 20 OFFSET 4500", []],
        ["SELECT id, label FROM pages ORDER BY id DESC LIMIT 20 OFFSET 4500", []],
        ["SELECT id FROM pages ORDER BY id LIMIT 3 OFFSET 5999", []],
        ["SELECT id FROM pages ORDER BY id LIMIT 3 OFFSET 6000", []],
        ["SELECT id FROM pages ORDER BY id DESC LIMIT 10 OFFSET 100000", []],
        ["SELECT id FROM pages WHERE id > ? ORDER BY id LIMIT 10 OFFSET 4000", [1_234]],
        ["SELECT id FROM pages WHERE id <= ? ORDER BY id DESC LIMIT 10 OFFSET 4100", [5_555]],
        // Ties straddle the page start: the remaining ORDER BY term orders them.
        ["SELECT id, aligned FROM pages ORDER BY aligned, id DESC LIMIT 7 OFFSET 4503", []],
        ["SELECT id, aligned FROM pages ORDER BY aligned DESC, id LIMIT 7 OFFSET 4497", []],
        [
          "SELECT id, aligned FROM pages WHERE aligned > 3 ORDER BY aligned, id DESC LIMIT 9 OFFSET 4100",
          [],
        ],
      ] as const) {
        const { served } = await differential(database, sql, [...params]);
        expect(served, sql).toBe(true);
      }
      // A tie group spanning two blocks puts them out of one strict order; a page deep enough
      // to need counting then falls back, and must still answer exactly.
      for (const sql of [
        "SELECT id, bucket FROM pages ORDER BY bucket, id DESC LIMIT 7 OFFSET 4503",
        "SELECT id, bucket FROM pages ORDER BY bucket DESC, id LIMIT 7 OFFSET 4497",
      ]) {
        await differential(database, sql);
      }
    } finally {
      await database.close();
    }
  });

  it("falls back over a mutation history and stays exact after it folds", async () => {
    const database = await eventsDatabase();
    try {
      await database.execute("DELETE FROM events WHERE id > 590");
      await database.execute("UPDATE events SET id = id + 1000 WHERE id = 5");
      for (const sql of [
        "SELECT id FROM events ORDER BY id DESC LIMIT 5",
        "SELECT MAX(id) AS m, MIN(id) AS n FROM events",
      ]) {
        const { served } = await differential(database, sql);
        expect(served, sql).toBe(false);
      }
      await database.compactTable("events");
      for (const sql of [
        "SELECT id FROM events ORDER BY id DESC LIMIT 5",
        "SELECT id FROM events ORDER BY id LIMIT 5",
        "SELECT MAX(id) AS m, MIN(id) AS n FROM events",
      ]) {
        await differential(database, sql);
      }
    } finally {
      await database.close();
    }
  });
});
