import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type QueryOptions } from "./database.js";
import { QueryMemoryBudgetError } from "./memory.js";

describe("snapshot metadata counts", () => {
  it("matches ordinary execution across writes, snapshots, transactions, and compaction", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 2,
    });
    const reader = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    const check = async (expected: number, options: QueryOptions = {}) => {
      const result = await reader.query("SELECT COUNT(*) AS n FROM items", {
        ...options,
        memoize: false,
      });
      expect(result.rows).toEqual([{ n: expected }]);
      expect(result).toEqual(
        await reader.query("SELECT COUNT(*) + 0 AS n FROM items", {
          ...options,
          memoize: false,
        }),
      );
    };
    try {
      await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)");
      await check(0);
      await database.execute("INSERT INTO items VALUES (1, 'a'), (2, NULL), (3, 'b')");
      const version = await store.getCurrentManifestVersion();
      if (version === null) throw new Error("Expected a committed manifest");
      await check(3);
      await database.execute("DELETE FROM items WHERE id = 2");
      await check(2);
      await database.execute("UPDATE items SET value = 'c' WHERE id = 1");
      await check(2);
      await database.upsertBatch("items", [
        { id: 1, value: "d" },
        { id: 4, value: "e" },
      ]);
      await check(3);
      await database.execute("BEGIN");
      await database.execute("DELETE FROM items WHERE id = 1");
      expect(
        (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
      ).toEqual([{ n: 2 }]);
      await check(3);
      await database.execute("COMMIT");
      await check(2);
      await check(3, { version });
      await database.compactTable("items", { minimumLevel0Segments: 2 });
      await check(2);
      await check(3, { version });
      await database.execute("DELETE FROM items");
      await check(0);
      await database.execute("DROP TABLE items");
      await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
      await check(0);
      await database.execute("INSERT INTO items VALUES (10)");
      await check(1);
    } finally {
      await reader.close();
      await database.close();
    }
  });

  it("preserves result ownership, tails, budgets, cancellation, and non-count semantics", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    try {
      await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)");
      await database.execute("INSERT INTO items VALUES (1, 'a'), (2, NULL), (3, 'a')");
      await database.execute("DELETE FROM items WHERE id = 1");
      for (const tail of ["", "LIMIT 0", "LIMIT 1", "OFFSET 1", "LIMIT 1 OFFSET 1"]) {
        const sql = `SELECT COUNT(*) AS n FROM items ${tail}`;
        expect(await database.query(sql, { memoize: false })).toEqual(
          await database.query(sql.replace("COUNT(*)", "COUNT(*) + 0"), { memoize: false }),
        );
      }
      let peakBytes = 0;
      const first = await database.query("SELECT COUNT(*) AS n FROM items", {
        memoize: false,
        onStats: ({ peakMemoryBytes }) => {
          peakBytes = peakMemoryBytes;
        },
      });
      expect(peakBytes).toBeGreaterThan(0);
      const row = first.rows[0];
      if (row === undefined) throw new Error("Expected a count row");
      row.n = 999;
      first.columns[0] = "changed";
      expect(
        (await database.query("SELECT COUNT(*) AS n FROM items", { memoize: false })).rows,
      ).toEqual([{ n: 2 }]);
      for (const spillToStorage of [undefined, false, true]) {
        await expect(
          database.query("SELECT COUNT(*) AS n FROM items", {
            memoize: false,
            executionMemoryBudgetBytes: 1,
            ...(spillToStorage === undefined ? {} : { spillToStorage }),
          }),
        ).rejects.toBeInstanceOf(QueryMemoryBudgetError);
      }
      const abort = new AbortController();
      abort.abort(new Error("cancel count"));
      await expect(
        database.query("SELECT COUNT(*) AS n FROM items", { signal: abort.signal }),
      ).rejects.toThrow("cancel count");
      for (const [sql, rows] of [
        ["SELECT COUNT(value) AS n FROM items", [{ n: 1 }]],
        ["SELECT COUNT(DISTINCT value) AS n FROM items", [{ n: 1 }]],
        ["SELECT COUNT(*) AS n FROM items WHERE id = 2", [{ n: 1 }]],
        ["SELECT COUNT(*) AS n FROM items HAVING COUNT(*) > 2", []],
        ["SELECT COUNT(*) AS n FROM items GROUP BY value", [{ n: 1 }, { n: 1 }]],
        ["SELECT COUNT(*) AS n FROM items a CROSS JOIN items b", [{ n: 4 }]],
      ] as const) {
        expect((await database.query(sql, { memoize: false })).rows).toEqual(rows);
      }
      await database.execute("CREATE VIEW singles AS SELECT id FROM items WHERE id = 2");
      expect(
        (await database.query("SELECT COUNT(*) AS n FROM singles", { memoize: false })).rows,
      ).toEqual([{ n: 1 }]);
      await expect(database.query("SELECT COUNT(*) AS n FROM missing")).rejects.toThrow();
    } finally {
      await database.close();
    }
  });
});
