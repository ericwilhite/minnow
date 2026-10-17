/**
 * The edges of four PostgreSQL forms the conformance corpus diffs against PGlite: date and
 * timestamp arithmetic, ANY/ALL over arrays (an array bound to one placeholder above all),
 * GENERATE_SERIES, and the ordered-set aggregates. These pin what an oracle cannot show — that
 * an array parameter binds to exactly the plan an IN list does, that the one-loop series agrees
 * with the recursive form that defines it and outruns its iteration cap, that ordered-set
 * aggregates survive a spill and a group larger than any JSON value — and the refusals.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import {
  bindPlanParameters,
  bindStatementParameters,
  compileQuery,
  compileStatement,
  executeRowQuery,
  type CompiledQuery,
} from "./query.js";

async function database(): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 64 });
  await db.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, grp TEXT NOT NULL, score DOUBLE PRECISION, day DATE, at TIMESTAMP)",
  );
  return db;
}

/** The same plan with every GENERATE_SERIES shortcut removed: only the recursive form remains. */
function withoutSeries(plan: CompiledQuery): CompiledQuery {
  const copy = structuredClone(plan);
  const strip = (block: CompiledQuery): void => {
    for (const source of [block.base, ...block.joins]) {
      if (source.recursive !== undefined) {
        delete source.recursive.series;
        strip(source.recursive.base);
        strip(source.recursive.step);
      }
      if (source.derived !== undefined) strip(source.derived);
      for (const member of source.union?.blocks ?? []) strip(member);
    }
  };
  strip(copy);
  return copy;
}

describe("date and timestamp arithmetic", () => {
  it("refuses the combinations PostgreSQL has no operator for", async () => {
    const db = await database();
    for (const [sql, message] of [
      ["SELECT TIMESTAMP '2026-01-01' + 1 AS x", /No \+ operator for timestamp and number/],
      ["SELECT DATE '2026-01-01' + DATE '2026-01-02' AS x", /No \+ operator for date and date/],
      ["SELECT DATE '2026-01-01' + 1.5 AS x", /whole number of days/],
      ["SELECT TIMESTAMP '2026-01-01' * 2 AS x", /No \* operator for timestamp and number/],
    ] as const) {
      await expect(db.query(sql), sql).rejects.toThrow(message);
    }
    await db.close();
  });

  it("returns typed DATE and INTERVAL values from columns", async () => {
    const db = await database();
    await db.execute(
      "INSERT INTO items VALUES (1, 'a', 1, '2024-02-28', '2026-01-31 10:00:00'), (2, 'a', 2, NULL, NULL)",
    );
    const result = await db.query(
      "SELECT id, day + 1 AS next_day, day - DATE '2024-01-01' AS days, at - TIMESTAMP '2026-01-30' AS since, at + (TIMESTAMP '2026-03-01' - TIMESTAMP '2026-02-01') AS later FROM items ORDER BY id",
    );
    expect(result.columnDomains).toEqual([
      null,
      { kind: "date" },
      null,
      { kind: "interval" },
      null,
    ]);
    expect(result.rows).toEqual([
      {
        id: 1,
        next_day: "2024-02-29",
        days: 58,
        since: "1 day 10:00:00",
        later: new Date("2026-02-28T10:00:00.000Z"),
      },
      { id: 2, next_day: null, days: null, since: null, later: null },
    ]);
    await db.close();
  });
});

describe("ANY and ALL over an array parameter", () => {
  it("binds = ANY($1) to exactly the plan an IN list of the same values binds to", () => {
    const any = bindPlanParameters(compileQuery("SELECT id FROM items WHERE id = ANY($1)"), [
      [3, 1, 2],
    ]);
    const list = bindPlanParameters(
      compileQuery("SELECT id FROM items WHERE id IN ($1, $2, $3)"),
      [3, 1, 2],
    );
    expect(any.predicates).toEqual(list.predicates);
    const all = bindPlanParameters(compileQuery("SELECT id FROM items WHERE grp <> ALL(?)"), [
      ["a", "b"],
    ]);
    const notIn = bindPlanParameters(compileQuery("SELECT id FROM items WHERE grp NOT IN (?, ?)"), [
      "a",
      "b",
    ]);
    expect(all.predicates).toEqual(notIn.predicates);
    const update = bindStatementParameters(
      compileStatement("DELETE FROM items WHERE id = ANY($1)"),
      [[7, 8]],
    );
    expect(update.kind === "delete" ? update.predicates : []).toEqual([
      {
        left: { kind: "column", reference: "id" },
        operator: "IN",
        right: {
          kind: "list",
          items: [
            { kind: "literal", value: 7 },
            { kind: "literal", value: 8 },
          ],
        },
      },
    ]);
  });

  it("reads keyed rows, updates, and deletes through the array", async () => {
    const db = await database();
    await db.insertBatch("items", {
      columns: {
        id: [1, 2, 3, 4, 5],
        grp: ["a", "b", "a", "c", "b"],
        score: [1, 2, 3, 4, 5],
        day: [null, null, null, null, null],
        at: [null, null, null, null, null],
      },
    });
    expect(
      (
        await db.query("SELECT id FROM items WHERE id = ANY($1) ORDER BY id", {
          params: [[5, 2, 9]],
        })
      ).rows,
    ).toEqual([{ id: 2 }, { id: 5 }]);
    // Minnow's own array text — what an array column hands back — binds the same way.
    expect(
      (
        await db.query("SELECT id FROM items WHERE grp = ANY($1) ORDER BY id", {
          params: ['["c"]'],
        })
      ).rows,
    ).toEqual([{ id: 4 }]);
    const updated = await db.execute("UPDATE items SET score = 0 WHERE grp <> ALL($1)", [
      ["a", "b"],
    ]);
    expect(updated.kind === "update" ? updated.rowCount : -1).toBe(1);
    const deleted = await db.execute("DELETE FROM items WHERE id = ANY(?::int[])", [[1, 3, 4]]);
    expect(deleted.kind === "delete" ? deleted.rowCount : -1).toBe(3);
    expect((await db.query("SELECT id, score FROM items ORDER BY id")).rows).toEqual([
      { id: 2, score: 2 },
      { id: 5, score: 5 },
    ]);
    await db.close();
  });

  it("refuses an array anywhere but ANY/ALL, and arrays it cannot bind", async () => {
    const db = await database();
    await expect(db.query("SELECT $1 AS a", { params: [[1, 2]] })).rejects.toThrow(
      /binds only as the operand of ANY or ALL/,
    );
    await expect(db.execute("INSERT INTO items (id, grp) VALUES ($1, 'a')", [[1]])).rejects.toThrow(
      /binds only as the operand of ANY or ALL/,
    );
    await expect(
      db.query("SELECT id FROM items WHERE id = ANY($1)", { params: [[[1], [2]]] as never }),
    ).rejects.toThrow(/one-dimensional/);
    await expect(
      db.query("SELECT id FROM items WHERE id = ANY($1)", { params: [[{ id: 1 }]] as never }),
    ).rejects.toThrow(/must be null, boolean, finite number, string, or valid Date/);
    await expect(
      db.query("SELECT id FROM items WHERE id = ANY($1)", { params: [5] }),
    ).rejects.toThrow(/must be an array for ANY or ALL/);
    await expect(
      db.query("SELECT id FROM items WHERE id = ANY($1::int)", { params: [[1]] }),
    ).rejects.toThrow(/names an array type/);
    await db.execute("INSERT INTO items (id, grp) VALUES (1, 'a')");
    await expect(db.query("SELECT id FROM items WHERE id = ANY(grp)")).rejects.toThrow(
      /take a subquery or an array/,
    );
    await db.close();
  });

  it("ranges over an array column's members, NULL arrays and members included", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    await db.execute("CREATE TABLE tagged (id INTEGER PRIMARY KEY, tags TEXT[], sizes INTEGER[])");
    // An empty array column value is written as its JSON text.
    await db.execute(
      "INSERT INTO tagged VALUES (1, ARRAY['red', 'blue'], ARRAY[1, 5]), (2, ARRAY['green', NULL], ARRAY[7]), (3, NULL, NULL), (4, '[]', '[]')",
    );
    const result = await db.query(
      "SELECT id, 'blue' = ANY(tags) AS blue, 'red' <> ALL(tags) AS not_red, 6 > ANY(sizes) AS small, id < ALL(sizes) AS below FROM tagged ORDER BY id",
    );
    expect(result.rows).toEqual([
      { id: 1, blue: true, not_red: false, small: true, below: false },
      { id: 2, blue: null, not_red: null, small: false, below: true },
      { id: 3, blue: null, not_red: null, small: null, below: null },
      { id: 4, blue: false, not_red: true, small: false, below: true },
    ]);
    expect(
      (await db.query("SELECT id FROM tagged WHERE 'green' = ANY(tags) ORDER BY id")).rows,
    ).toEqual([{ id: 2 }]);
    await db.close();
  });
});

describe("GENERATE_SERIES", () => {
  it("produces exactly the rows of the recursive form that defines it", () => {
    for (const sql of [
      "SELECT n FROM generate_series(1, 10) AS n",
      "SELECT n FROM generate_series(10, -10, -3) AS n",
      "SELECT n FROM generate_series(5, 1) AS n",
      "SELECT n FROM generate_series(1, 5, -1) AS n",
      "SELECT n FROM generate_series(NULL, 5) AS n",
      "SELECT t FROM generate_series(TIMESTAMP '2024-01-31', TIMESTAMP '2025-03-01', INTERVAL '1 month') AS t",
      "SELECT t FROM generate_series(DATE '2026-03-01', DATE '2026-02-20', INTERVAL '-2 days 6 hours') AS t",
      "SELECT t FROM generate_series(TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-02', INTERVAL '90 minutes') AS t",
    ]) {
      const plan = compileQuery(sql);
      const fast = executeRowQuery(plan, new Map());
      const recursive = executeRowQuery(withoutSeries(plan), new Map());
      expect(fast, sql).toEqual(recursive);
    }
  });

  it("runs past the recursive CTE iteration cap with constant and bound arguments", async () => {
    const db = await database();
    const result = await db.query(
      "SELECT COUNT(*) AS c, SUM(n) AS total, MIN(n) AS lo, MAX(n) AS hi FROM generate_series(1, CAST($1 AS INTEGER)) AS n",
      { params: [200_000] },
    );
    expect(result.rows).toEqual([{ c: 200_000, total: 20_000_100_000, lo: 1, hi: 200_000 }]);
    // Bounds from the statement clock and a subquery come from the same base row.
    expect(
      (
        await db.query(
          "SELECT COUNT(*) AS c FROM generate_series(CURRENT_DATE - 49999, CURRENT_DATE, INTERVAL '1 day') AS d, generate_series(1, (SELECT 2)) AS k",
        )
      ).rows,
    ).toEqual([{ c: 100_000 }]);
    await db.close();
  });

  it("refuses what it cannot generate", async () => {
    const db = await database();
    for (const [sql, message] of [
      ["SELECT n FROM generate_series(1, 5, 0) AS n", /step size cannot equal zero/],
      ["SELECT n FROM generate_series(0.5, 2) AS n", /integer or timestamp arguments/],
      [
        "SELECT t FROM generate_series(TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-05') AS t",
        /INTERVAL step/,
      ],
      ["SELECT n FROM items, generate_series(1, items.id) AS n", /cannot refer to table rows/],
      ["SELECT n FROM generate_series(1) AS n", /a start, a stop/],
    ] as const) {
      await expect(db.query(sql), sql).rejects.toThrow(message);
    }
    await expect(
      db.query("SELECT n FROM generate_series(1, 5, CAST($1 AS INTEGER)) AS n", { params: [0] }),
    ).rejects.toThrow(/step size cannot equal zero/);
    await db.close();
  });
});

describe("ordered-set aggregates", () => {
  it("handle groups far larger than any JSON value, and agree after a spill", async () => {
    const db = await database();
    const count = 120_000;
    await db.insertBatch("items", {
      columns: {
        id: Array.from({ length: count }, (_, index) => index),
        grp: Array.from({ length: count }, (_, index) => (index % 3 === 0 ? "x" : "y")),
        score: Array.from({ length: count }, (_, index) => (index * 7_919) % count),
        day: Array.from({ length: count }, () => null),
        at: Array.from({ length: count }, () => null),
      },
    });
    const sql =
      "SELECT grp, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS median, PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY score DESC) AS p10, MODE() WITHIN GROUP (ORDER BY (id * id) % 10 DESC) AS common FROM items GROUP BY grp ORDER BY grp";
    // The most frequent value; DESC breaks a tie toward the largest.
    const modeOf = (values: number[]): number => {
      const counts = new Map<number, number>();
      for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1);
      let best = Number.NEGATIVE_INFINITY;
      let bestCount = 0;
      for (const [value, seen] of counts) {
        if (seen > bestCount || (seen === bestCount && value > best)) {
          best = value;
          bestCount = seen;
        }
      }
      return best;
    };
    const reference = await db.query(sql);
    const expected = (group: "x" | "y") => {
      const scores = Array.from({ length: count }, (_, index) => index)
        .filter((index) => (index % 3 === 0) === (group === "x"))
        .map((index) => (index * 7_919) % count)
        .sort((a, b) => a - b);
      const position = 0.5 * (scores.length - 1);
      const low = scores[Math.floor(position)] ?? 0;
      const high = scores[Math.ceil(position)] ?? 0;
      const descending = [...scores].reverse();
      return {
        grp: group,
        median: low + (high - low) * (position - Math.floor(position)),
        p10: descending[Math.ceil(0.9 * descending.length) - 1],
        common: modeOf(
          Array.from({ length: count }, (_, index) => index)
            .filter((index) => (index % 3 === 0) === (group === "x"))
            .map((index) => (index * index) % 10),
        ),
      };
    };
    expect(reference.rows).toEqual([expected("x"), expected("y")]);
    // A spill partitions groups, so it needs many small ones: each group's inputs stay whole.
    const many =
      "SELECT id % 400 AS bucket, PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY score) AS p, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY score DESC) AS d, MODE() WITHIN GROUP (ORDER BY grp) AS m FROM items WHERE id < 6000 GROUP BY id % 400 ORDER BY bucket";
    const resident = await db.query(many);
    expect(resident.rows).toHaveLength(400);
    const spilled = await db.query(many, {
      executionMemoryBudgetBytes: 400_000,
      spillToStorage: true,
      spillPageRows: 256,
    });
    expect(spilled.rows).toEqual(resident.rows);
    await db.close();
  });

  it("validates the fraction and its placement", async () => {
    const db = await database();
    await db.execute("INSERT INTO items (id, grp, score) VALUES (1, 'a', 1), (2, 'a', 3)");
    expect(
      (
        await db.query(
          "SELECT PERCENTILE_CONT(NULL) WITHIN GROUP (ORDER BY score) AS p, PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY score) AS q FROM items",
        )
      ).rows,
    ).toEqual([{ p: null, q: 1.5 }]);
    for (const [sql, message] of [
      [
        "SELECT PERCENTILE_CONT(1.5) WITHIN GROUP (ORDER BY score) AS p FROM items",
        /not between 0 and 1/,
      ],
      ["SELECT PERCENTILE_CONT(0.5) AS p FROM items", /requires WITHIN GROUP/],
      ["SELECT MODE(score) WITHIN GROUP (ORDER BY score) AS m FROM items", /takes no arguments/],
      [
        "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY score, id) AS p FROM items",
        /exactly one ORDER BY/,
      ],
      [
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY grp) AS p FROM items",
        /requires numeric input/,
      ],
      [
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) OVER () AS p FROM items",
        /does not support FILTER or OVER/,
      ],
    ] as const) {
      await expect(db.query(sql), sql).rejects.toThrow(message);
    }
    await db.close();
  });
});
