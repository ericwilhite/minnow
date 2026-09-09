import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { compileQuery, type QueryResult } from "./query.js";

describe("live maintenance correctness", () => {
  it("keeps a concurrently joining subscription alive when the previous owner closes", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const live = db.liveQueries();
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
      await db.execute("INSERT INTO t VALUES (1,10)");
      const sql = "SELECT id,x FROM t ORDER BY id";
      const first = await live.subscribe(sql, { onChange: () => undefined });
      let latest: QueryResult | undefined;
      const joining = live.subscribe(sql, {
        onChange: (result) => {
          latest = result;
        },
      });
      first.close();
      const second = await joining;
      await db.execute("UPDATE t SET x=20");
      await live.refresh();
      expect(latest?.rows).toEqual([{ id: 1, x: 20 }]);
      expect(live.stats.retainedBytes).toBeGreaterThan(0);
      second.close();
      expect(live.stats.retainedBytes).toBe(0);
    } finally {
      live.close();
      await db.close();
    }
  });
  it("keeps aggregate state atomic across byte rejection and retries", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, x INTEGER)");
      await db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20)");
      const sql = "SELECT g, COUNT(*) AS n, SUM(x) AS s FROM t GROUP BY g ORDER BY g";
      const measuring = db.liveQueries();
      await measuring.subscribe(sql, { onChange: () => undefined });
      const limit = measuring.stats.retainedBytes + 100;
      measuring.close();
      const live = db.liveQueries({ maxRetainedBytes: limit });
      let latest: QueryResult | undefined;
      const errors: unknown[] = [];
      try {
        await live.subscribe(sql, {
          onChange: (result) => {
            latest = result;
          },
          onError: (error) => errors.push(error),
        });
        const before = latest;
        await db.execute(`UPDATE t SET g='${"x".repeat(2000)}', x=100 WHERE id=1`);
        await live.refresh();
        await live.refresh();
        expect(errors.length).toBeGreaterThanOrEqual(2);
        expect(latest).toBe(before);
        expect(live.stats.retainedBytes).toBeLessThanOrEqual(limit);
        await db.execute("UPDATE t SET g='a', x=11 WHERE id=1");
        await live.refresh();
        expect(latest).toEqual(await db.query(sql, { memoize: false }));
        expect(latest?.rows).toEqual([{ g: "a", n: 2, s: 31 }]);
      } finally {
        live.close();
        expect(live.stats.retainedBytes).toBe(0);
      }
    } finally {
      await db.close();
    }
  });

  it("serves adapter re-executions from accepted maintenance, including public domain values", async () => {
    const store = new MemoryBlockStore();
    const db = new MinnowDatabase(store);
    const peer = new MinnowDatabase(store);
    const live = db.liveQueries();
    try {
      await peer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x NUMERIC(12,2), label TEXT)");
      // Public TEXT resembling internal domain tags must survive memo publication unchanged.
      await peer.execute("INSERT INTO t VALUES (1,2,?),(2,10,?)", [
        "\0minnow-domain:numeric:2",
        "\0minnow-domain:interval:not-json",
      ]);
      for (const sql of ["SELECT SUM(x) AS s FROM t", "SELECT id,x,label FROM t ORDER BY x,id"]) {
        const peaks: number[] = [];
        let pending: Promise<QueryResult> | undefined;
        const subscription = await live.observe(sql, {
          suppressUnchanged: true,
          onInvalidate: () => {
            pending = db.query(sql, { onStats: (stats) => peaks.push(stats.peakMemoryBytes) });
          },
        });
        expect(await pending).toEqual(await db.query(sql, { memoize: false }));
        await peer.execute("UPDATE t SET x=x+1 WHERE id=1");
        await live.refresh();
        expect(await pending).toEqual(await db.query(sql, { memoize: false }));
        expect(peaks).toEqual([0, 0]);
        subscription.close();
      }
      expect(live.stats.maintained).toBe(2);
    } finally {
      live.close();
      await peer.close();
      await db.close();
    }
  });

  it("marks a growing bounded result incomplete before later edge refills", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const live = db.liveQueries();
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      await db.execute("INSERT INTO t VALUES (1)");
      const sql = "SELECT id FROM t ORDER BY id LIMIT 2";
      let latest: QueryResult | undefined;
      await live.subscribe(sql, {
        onChange: (result) => {
          latest = result;
        },
      });
      await db.execute(
        `INSERT INTO t VALUES ${Array.from({ length: 80 }, (_, i) => `(${String(i + 2)})`).join(",")}`,
      );
      await live.refresh();
      for (let id = 1; id <= 40; id += 1) {
        await db.execute(`DELETE FROM t WHERE id=${String(id)}`);
        await live.refresh();
        expect(latest).toEqual(await db.query(sql, { memoize: false }));
      }
      expect(live.stats.reruns).toBeGreaterThan(0);
    } finally {
      live.close();
      await db.close();
    }
  });
  it.each(["", " GROUP BY g", " GROUP BY g HAVING COUNT(DISTINCT x) > 1"])(
    "falls back for DISTINCT aggregates%s",
    async (suffix) => {
      const db = new MinnowDatabase(new MemoryBlockStore());
      const live = db.liveQueries();
      try {
        await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, g TEXT, x INTEGER)");
        await db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',10),(3,'a',20),(4,'b',NULL)");
        const sql = `SELECT COUNT(DISTINCT x) AS n, SUM(DISTINCT x) AS s, AVG(DISTINCT x) AS a FROM t${suffix} ORDER BY n,s`;
        let latest: QueryResult | undefined;
        await live.subscribe(sql, {
          onChange: (result) => {
            latest = result;
          },
        });
        expect(latest).toEqual(await db.query(sql, { memoize: false }));
        for (const write of [
          "DELETE FROM t WHERE id=1",
          "UPDATE t SET x=20 WHERE id=2",
          "INSERT INTO t VALUES (5,'b',30)",
          "DELETE FROM t",
        ]) {
          await db.execute(write);
          await live.refresh();
          expect(latest, write).toEqual(await db.query(sql, { memoize: false }));
        }
        expect(live.stats.maintained).toBe(0);
        expect(live.stats.reruns).toBeGreaterThan(0);
      } finally {
        live.close();
        await db.close();
      }
    },
  );

  it("keeps numeric ordering exact through randomized inserts, updates, deletes and window refills", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.tuple(
            fc.integer({ min: 1, max: 12 }),
            fc.integer({ min: -20, max: 120 }),
            fc.boolean(),
          ),
          { minLength: 25, maxLength: 40 },
        ),
        async (writes) => {
          const db = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false });
          const live = db.liveQueries();
          try {
            await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x NUMERIC(12,2))");
            await db.execute("INSERT INTO t VALUES (1,2),(2,10),(3,NULL),(4,-10),(5,100)");
            const statements = [
              "SELECT id,x FROM t ORDER BY x,id LIMIT 2",
              "SELECT id,x FROM t ORDER BY x DESC NULLS LAST,id LIMIT 3 OFFSET 1",
              "SELECT id FROM t ORDER BY x,id",
              "SELECT id,x*2 AS y FROM t ORDER BY y,id LIMIT 4",
              "SELECT id,x FROM t ORDER BY 2 DESC,1 LIMIT 2",
            ];
            const latest = new Map<string, QueryResult>();
            for (const sql of statements)
              await live.subscribe(sql, { onChange: (result) => latest.set(sql, result) });
            for (const [id, value, remove] of writes) {
              await db.execute(
                remove
                  ? `DELETE FROM t WHERE id=${String(id)}`
                  : `INSERT INTO t VALUES (${String(id)},${String(value)}) ON CONFLICT (id) DO UPDATE SET x=EXCLUDED.x`,
              );
              await live.refresh();
              for (const sql of statements)
                expect(latest.get(sql), sql).toEqual(await db.query(sql, { memoize: false }));
            }
            expect(live.stats.maintained).toBeGreaterThan(0);
          } finally {
            live.close();
            await db.close();
          }
        },
      ),
      { seed: 42017, numRuns: 5 },
    );
  });

  it.each([true, false])(
    "captures parameter ownership before asynchronous registration (incremental=%s)",
    async (incremental) => {
      const db = new MinnowDatabase(new MemoryBlockStore());
      const live = db.liveQueries({ incremental });
      try {
        await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)");
        await db.execute("INSERT INTO t VALUES (1,10),(2,20)");
        const input = {
          kind: "sql-query" as const,
          sql: "SELECT id,x FROM t WHERE id>=? ORDER BY id",
          params: [2],
        };
        let latest: QueryResult | undefined;
        const opening = live.subscribe(input, {
          onChange: (result) => {
            latest = result;
          },
        });
        input.params[0] = 1;
        await opening;
        expect(latest?.rows).toEqual([{ id: 2, x: 20 }]);
        await db.execute("UPDATE t SET x=21 WHERE id=2");
        await live.refresh();
        await live.subscribe(
          { ...input, params: [2] },
          {
            onChange: (result) => {
              latest = result;
            },
          },
        );
        expect(latest?.rows).toEqual([{ id: 2, x: 21 }]);
      } finally {
        live.close();
        await db.close();
      }
    },
  );

  it("captures compiled-plan and Date ownership", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const live = db.liveQueries({ incremental: false });
    try {
      const plan = compileQuery("SELECT 2 AS n");
      const opening = live.subscribe(
        { kind: "typed-query", plan },
        { onChange: (result) => expect(result.rows).toEqual([{ n: 2 }]) },
      );
      const selected = plan.select[0];
      if (selected === undefined) throw new Error("Missing projection");
      selected.expression = { kind: "literal", value: 3 };
      await opening;
      const date = new Date("2026-01-01T00:00:00Z");
      const dateOpening = live.subscribe(
        { kind: "sql-query", sql: "SELECT ? AS d", params: [date] },
        {
          onChange: (result) =>
            expect(result.rows).toEqual([{ d: new Date("2026-01-01T00:00:00Z") }]),
        },
      );
      date.setUTCFullYear(2030);
      await dateOpening;
    } finally {
      live.close();
      await db.close();
    }
  });
});

describe("memo eligibility after view expansion", () => {
  it("does not memoize volatile views, nested views, or typed reads and refreshes proofs after replacement", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute("CREATE VIEW v AS SELECT 1 AS r");
      expect((await db.query("SELECT r FROM v")).rows).toEqual([{ r: 1 }]);
      await db.execute("CREATE OR REPLACE VIEW v AS SELECT RANDOM() AS r");
      await db.execute("CREATE VIEW nested AS SELECT r FROM v");
      for (const sql of ["SELECT r FROM v", "SELECT r FROM nested"]) {
        const values = new Set<string>();
        for (let i = 0; i < 6; i += 1) {
          values.add(JSON.stringify((await db.query(sql)).rows));
          values.add(
            JSON.stringify(await db.run({ kind: "typed-query", plan: compileQuery(sql) })),
          );
        }
        expect(values.size).toBe(12);
      }
      await db.execute("DROP VIEW nested");
      await db.execute("CREATE OR REPLACE VIEW v AS SELECT 2 AS r");
      await db.execute("CREATE VIEW nested AS SELECT r FROM v");
      await db.query("SELECT r FROM nested");
      const peaks: number[] = [];
      expect(
        (
          await db.query("SELECT r FROM nested", {
            onStats: (stats) => peaks.push(stats.peakMemoryBytes),
          })
        ).rows,
      ).toEqual([{ r: 2 }]);
      expect(peaks).toEqual([0]);
    } finally {
      await db.close();
    }
  });

  it("includes long SQL identities in the buffer pool budget", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore(), { bufferPoolBytes: 32768 });
    try {
      for (let i = 0; i < 20; i += 1)
        await db.query(`SELECT 1 AS n /* ${String(i)} ${"x".repeat(15000)} */`);
      const stats = db.bufferPoolStats();
      expect(stats.usedBytes).toBeLessThanOrEqual(stats.limitBytes);
      expect(stats.entries).toBeLessThanOrEqual(1);
      expect(stats.evictions).toBeGreaterThan(0);
    } finally {
      await db.close();
    }
  });
});
