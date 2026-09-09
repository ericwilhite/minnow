import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { applyWindowFunctions } from "./windows.js";
import { QueryMemoryContext } from "./memory.js";
import type { QueryResult, WindowSpec } from "./query.js";

describe("window execution optimizations", () => {
  it("keeps derived values exact across numeric coercion, Dates, booleans and protected text", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, amount NUMERIC(10,2), at TIMESTAMP, flag BOOLEAN, note TEXT)",
      );
      const date = new Date("2026-01-01T00:00:00Z");
      const note = "\0minnow-domain:numeric:123";
      await db.execute("INSERT INTO t VALUES (1,NULL,?,TRUE,?),(2,10.25,NULL,NULL,NULL)", [
        date,
        note,
      ]);
      const sql =
        "SELECT id, COALESCE(amount,0) AS amount, at, flag, note, ROW_NUMBER() OVER (ORDER BY id) AS n FROM t ORDER BY id";
      const expected = [
        { id: 1, amount: "0.00", at: date, flag: true, note, n: 1 },
        { id: 2, amount: "10.25", at: null, flag: null, note: null, n: 2 },
      ];
      for (const memoize of [false, true, true]) {
        expect((await db.query(sql, { memoize })).rows).toEqual(expected);
      }
      expect((await db.query(sql.replace("FROM t", "FROM t WHERE FALSE"))).rows).toEqual([]);
    } finally {
      await db.close();
    }
  });

  it("matches SQLite across positional frames, shared peers, nulls and exclusions", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const sqlite = new DatabaseSync(":memory:");
    try {
      const ddl = "CREATE TABLE t (id INTEGER PRIMARY KEY, p INTEGER, k INTEGER, x INTEGER)";
      await db.execute(ddl);
      sqlite.exec(ddl);
      const values = Array.from({ length: 90 }, (_, id) =>
        [
          id,
          id % 7 === 0 ? "NULL" : id % 3,
          Math.floor(id / 5),
          id % 4 === 0 ? "NULL" : id - 40,
        ].join(","),
      );
      const insert = `INSERT INTO t VALUES ${values.map((row) => `(${row})`).join(",")}`;
      await db.execute(insert);
      sqlite.exec(insert);
      for (const frame of [
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING",
        "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW",
        "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP",
        "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE TIES",
        "RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING",
        "GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
      ]) {
        for (const sharedRanks of [false, true]) {
          const order = "PARTITION BY p ORDER BY k DESC NULLS LAST";
          const over = `OVER (${order} ${frame})`;
          const select = [
            "SUM(x)",
            "AVG(x)",
            "COUNT(x)",
            "MIN(x)",
            "MAX(x)",
            "FIRST_VALUE(x)",
            "LAST_VALUE(x)",
            "NTH_VALUE(x, 2)",
          ].map((call, index) => `${call} ${over} AS c${String(index)}`);
          if (sharedRanks) {
            for (const call of [
              "RANK()",
              "DENSE_RANK()",
              "CUME_DIST()",
              "PERCENT_RANK()",
              "ROW_NUMBER()",
              "NTILE(7)",
              "LAG(x)",
              "LEAD(x)",
            ]) {
              select.push(`${call} OVER (${order}) AS c${String(select.length)}`);
            }
          }
          const sql = `SELECT id, ${select.join(",")} FROM t ORDER BY id`;
          expect((await db.query(sql, { memoize: false })).rows, sql).toEqual(
            sqlite.prepare(sql).all(),
          );
        }
      }
    } finally {
      sqlite.close();
      await db.close();
    }
  });

  it("omits peer buffers for a ROWS frame while preserving mixed-window results", () => {
    const result: QueryResult = {
      columns: ["x"],
      columnDomains: [null],
      rows: Array.from({ length: 4096 }, (_, x) => ({ x })),
    };
    const window: WindowSpec = {
      name: "SUM",
      alias: "sum",
      argumentAlias: "x",
      partitionAliases: [],
      orderAliases: [{ alias: "x", direction: "asc" }],
      frame: { unit: "rows", start: { kind: "unbounded-preceding" }, end: { kind: "current-row" } },
    };
    const peaks: number[] = [];
    for (const windows of [
      [window],
      [window, { ...window, name: "RANK" as const, alias: "rank" }],
    ]) {
      const memory = new QueryMemoryContext();
      try {
        const output = applyWindowFunctions(result, windows, { memoryContext: memory });
        expect(output.rows.at(-1)?.sum).toBe((4095 * 4096) / 2);
        peaks.push(memory.usage.peakBytes);
      } finally {
        memory.close();
      }
    }
    // One additional output column costs 16 bytes per row; peer preparation costs 20 more.
    expect((peaks[1] ?? 0) - (peaks[0] ?? 0)).toBe(4096 * 36);
    expect(result.rows[0]).toEqual({ x: 0 });
  });

  it("defines colliding and inherited aliases as own writable values without invoking setters", () => {
    for (const alias of ["__proto__", "toString", "value"]) {
      let writes = 0;
      const prototype = Object.create(Object.prototype) as Record<string, unknown>;
      Object.defineProperty(prototype, alias, {
        set: () => {
          writes += 1;
        },
      });
      const row = Object.create(prototype) as Record<string, number>;
      row.x = 3;
      const output = applyWindowFunctions(
        { columns: ["x"], columnDomains: [null], rows: [row] },
        [{ name: "ROW_NUMBER", alias, partitionAliases: [], orderAliases: [] }],
        { copyRows: false },
      );
      expect(writes).toBe(0);
      expect(Object.getOwnPropertyDescriptor(output.rows[0], alias)).toEqual({
        value: 1,
        writable: true,
        configurable: true,
        enumerable: true,
      });
      expect(Object.getPrototypeOf(row)).toBe(prototype);
    }
  });
});
