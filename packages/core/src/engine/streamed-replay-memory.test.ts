import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

describe("streamed mutation replay memory", () => {
  it("bounds repeated upserts by surviving patches, including after reopening", async () => {
    const store = new MemoryBlockStore();
    const options = { autoCompact: false, autoCollect: false, rowsPerBlock: 32 };
    let database = new MinnowDatabase(store, options);
    try {
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "string" },
          { name: "amount", type: "number" },
          { name: "other", type: "number" },
        ],
      });
      const peaks: number[] = [];
      for (let version = 0; version < 48; version += 1) {
        await database.upsertBatch(
          "items",
          Array.from({ length: 256 }, (_, id) => ({
            id: `item-${String(id).padStart(4, "0")}`,
            amount: version,
            other: id,
          })),
        );
        if (version !== 15 && version !== 47) continue;
        // A cold build, a cached replay, and a new connection must all fit the same budget.
        for (let pass = 0; pass < 3; pass += 1) {
          if (pass === 2) {
            await database.close();
            database = new MinnowDatabase(store, options);
          }
          const result = await database.query(
            "SELECT COUNT(*) AS n, SUM(amount) AS amount, SUM(other) AS other FROM items",
            {
              memoize: false,
              executionMemoryBudgetBytes: 128 * 1024,
              onStats: (stats) => peaks.push(stats.peakMemoryBytes),
            },
          );
          expect(result.rows).toEqual([{ n: 256, amount: version * 256, other: (255 * 256) / 2 }]);
        }
      }
      // Extra history needs a dead-row bitmap, not another map entry and patch per old row.
      expect(peaks).toHaveLength(6);
      expect(peaks[3]).toBeLessThan((peaks[0] ?? 0) + 16 * 1024);
    } finally {
      await database.close();
    }
  });

  it("keeps partial updates and delete/reinsert order while discarding superseded layers", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 16,
    });
    try {
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "amount", type: "number" },
          { name: "other", type: "number", nullable: true },
        ],
      });
      const rows = Array.from({ length: 128 }, (_, id) => ({ id, amount: id, other: id }));
      const keys = rows.map((row) => row.id);
      await database.insertBatch("items", rows);
      for (let version = 0; version < 32; version += 1) {
        await database.updateBatch("items", {
          keys,
          changes: { amount: keys.map(() => version) },
        });
        await database.updateBatch("items", {
          keys,
          changes: { other: keys.map((id) => (id % 2 === 0 ? null : -version)) },
        });
      }
      await database.deleteBatch("items", { keys: [0, 1] });
      await database.upsertBatch("items", [{ id: 0, amount: 100, other: 200 }]);
      await database.updateBatch("items", { keys: [0], changes: { amount: [300] } });
      const expected = [
        ...keys.slice(2).map((id) => ({ id, amount: 31, other: id % 2 === 0 ? null : -31 })),
        { id: 0, amount: 300, other: 200 },
      ];
      for (let pass = 0; pass < 2; pass += 1) {
        const result = await database.query("SELECT id, amount, other FROM items", {
          memoize: false,
          executionMemoryBudgetBytes: 128 * 1024,
        });
        expect(result.rows).toEqual(expected);
      }
    } finally {
      await database.close();
    }
  });

  it("releases resolved patch memory between scan windows", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 32,
    });
    try {
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "amount", type: "number" },
        ],
      });
      const keys = Array.from({ length: 4096 }, (_, id) => id);
      await database.insertBatch(
        "items",
        keys.map((id) => ({ id, amount: 0 })),
      );
      await database.updateBatch("items", { keys, changes: { amount: keys } });
      const sql = "SELECT SUM(amount) AS total FROM items";
      for (const budget of [2 * 1024 * 1024, 800 * 1024]) {
        const result = await database.query(sql, {
          memoize: false,
          executionMemoryBudgetBytes: budget,
        });
        expect(result.rows).toEqual([{ total: (4095 * 4096) / 2 }]);
      }
    } finally {
      await database.close();
    }
  });
});
