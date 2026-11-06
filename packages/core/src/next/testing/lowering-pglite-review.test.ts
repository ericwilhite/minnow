/**
 * Review B6-p2b findings against PostgreSQL (PGlite), on the review's own fixture. Each case runs
 * in PGlite and through lowering + the IR reference interpreter (pglite-oracle.ts): rows, result
 * types and errors must match, and every executed plan is type-checked (ir-check.ts).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createReach, RANGE_KEY_STATES, RANGE_NULL_STATES } from "../../testing/reach.js";
import { sqlTypeOf, type SqlType } from "../ir/functions.js";
import { PlanningError } from "../lower/errors.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { check as checkIn, nextOutcome, pgOutcome, type Case } from "./pglite-oracle.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const check = (setup: readonly string[], cases: readonly Case[]) => checkIn(pg, setup, cases);

const REVIEW = [
  "CREATE TABLE a (id integer, v integer)",
  "CREATE TABLE b (id integer, v integer)",
  "CREATE TABLE c (id integer, v integer)",
  "CREATE INDEX bv ON b (v, id)",
  "INSERT INTO a VALUES (1, 10), (2, 20), (3, NULL)",
  "INSERT INTO b VALUES (1, 1), (1, 2), (4, NULL), (NULL, 3)",
  "INSERT INTO c VALUES (1, 7), (4, 8)",
];

describe("init plans run on the first read of their slot (R24.1)", () => {
  it("never runs an init plan whose value no row needs, nor raises its errors", async () => {
    await check(REVIEW, [
      // b has four rows: reading the scalar subquery would raise 21000.
      { sql: "SELECT (SELECT id FROM b) AS x FROM a WHERE id = 999" },
      { sql: "SELECT (SELECT id FROM b) AS x FROM a LIMIT 0" },
      { sql: "SELECT (SELECT id FROM b) AS x FROM a ORDER BY id LIMIT 0" },
      {
        sql: "SELECT CASE WHEN id = 999 THEN (SELECT id FROM b) ELSE 0 END AS x FROM a ORDER BY id",
      },
      // Division by zero inside the init plan (22012), also never reached.
      { sql: "SELECT (SELECT 10 / (v - v) FROM b WHERE v = 1) AS x FROM a WHERE id = 999" },
    ]);
  });

  it("still raises an init plan's error once a row reads it", async () => {
    await check(REVIEW, [
      { sql: "SELECT (SELECT id FROM b) AS x FROM a WHERE id = 1" },
      { sql: "SELECT CASE WHEN id = 2 THEN (SELECT id FROM b) ELSE 0 END AS x FROM a ORDER BY id" },
      { sql: "SELECT (SELECT 10 / (v - v) FROM b WHERE v = 1) AS x FROM a WHERE id = 1" },
      // One evaluation per query: the same value on every row.
      { sql: "SELECT id, (SELECT max(v) FROM b) AS m FROM a ORDER BY id" },
    ]);
  });
});

describe("set operations resolve unknown-typed constants against the other side", () => {
  it("in either arm order, as each pair of a chain, and nested", async () => {
    await check(REVIEW, [
      { sql: "SELECT NULL AS n UNION SELECT 1 ORDER BY n NULLS LAST" },
      { sql: "SELECT 1 AS n UNION SELECT NULL ORDER BY n NULLS LAST" },
      { sql: "SELECT '1' AS n UNION SELECT 2 ORDER BY n" },
      { sql: "SELECT 2 AS n UNION SELECT '1' ORDER BY n" },
      { sql: "SELECT NULL AS n UNION ALL SELECT 1.5 ORDER BY n" },
      { sql: "SELECT NULL AS n UNION SELECT NULL" },
      { sql: "SELECT 'a' AS n UNION SELECT 'b' ORDER BY n" },
      { sql: "SELECT NULL AS a, 'x' AS b UNION SELECT 1, 'y' ORDER BY a" },
      { sql: "SELECT NULL AS n INTERSECT SELECT 1" },
      { sql: "SELECT 1 AS n EXCEPT SELECT NULL ORDER BY n" },
      { sql: "SELECT id AS n FROM a UNION SELECT NULL ORDER BY n NULLS FIRST" },
      { sql: "SELECT '7' AS n UNION SELECT id FROM a ORDER BY n" },
      { sql: "SELECT n FROM (SELECT NULL AS n UNION SELECT 1) s ORDER BY n" },
      // Each pair resolves on its own: two unknowns are text before the third arm is seen.
      { sql: "SELECT NULL AS n UNION SELECT NULL UNION SELECT 1 ORDER BY n" },
      { sql: "SELECT 'x' AS n UNION SELECT 'y' UNION SELECT 1" },
      { sql: "SELECT 1 AS n UNION (SELECT NULL UNION SELECT '3') ORDER BY n" },
      { sql: "(SELECT NULL AS n UNION SELECT NULL) UNION SELECT 1 ORDER BY n" },
      { sql: "SELECT 1 AS n UNION SELECT NULL UNION SELECT '3' ORDER BY n" },
      { sql: "SELECT 'a' AS n UNION SELECT 1" },
      { sql: "SELECT CAST(NULL AS text) AS n UNION SELECT 1" },
    ]);
  });
});

/**
 * R24.2: a FULL JOIN with no hash- or merge-joinable condition is an intended extension.
 * PostgreSQL refuses it (0A000) for an implementation reason; its result is still defined, and is
 * what PostgreSQL returns for the equivalent rewrite: the LEFT JOIN, UNION ALL the right side's
 * rows that match no left row, NULL-extended. Registered as an R21 divergence once the registry
 * (next/testing/divergences.ts, on rebuild/write-p2a) is on main.
 */
describe("FULL JOIN without a joinable condition (R24.2)", () => {
  it("matches PostgreSQL's LEFT JOIN UNION ALL anti-join rewrite", async () => {
    const next = await check(REVIEW, []);
    const shapes: ReadonlyArray<{ on: string; order: string }> = [
      { on: "a.id < b.id", order: "1 NULLS LAST, 2 NULLS LAST" },
      { on: "a.v > b.v", order: "1 NULLS LAST, 2 NULLS LAST" },
      { on: "a.id = b.id OR a.v < b.v", order: "1 NULLS LAST, 2 NULLS LAST" },
      { on: "a.id + 1 < b.id AND a.v IS NOT NULL", order: "1 NULLS LAST, 2 NULLS LAST" },
    ];
    for (const { on, order } of shapes) {
      const sql = `SELECT a.id, b.v FROM a FULL JOIN b ON ${on} ORDER BY ${order}`;
      expect(await pgOutcome(pg, sql), `${sql} in PostgreSQL`).toEqual({ error: "0A000" });
      const rewrite =
        `SELECT a.id, b.v FROM a LEFT JOIN b ON ${on} ` +
        `UNION ALL SELECT NULL::integer, b.v FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE ${on}) ` +
        `ORDER BY ${order}`;
      expect(nextOutcome(next, sql), sql).toEqual(await pgOutcome(pg, rewrite));
    }
  });
});

// A unique order key (id), peer groups with gaps and a NULL (g), and a NULL value (x).
const FRAMES = [
  "CREATE TABLE w (id integer PRIMARY KEY, g integer, x integer)",
  "INSERT INTO w VALUES (1, 1, 4), (2, 1, 7), (3, 2, NULL), (4, 3, 2), (5, 3, 9), (6, 3, 1), (7, 5, 6), (8, 8, 3), (9, 8, 8), (10, NULL, 5)",
];

// AVG over integers is double precision here (ruling C11), numeric in PostgreSQL: the moving
// averages average a double precision column. A constant non-positive ntile / nth_value argument
// is refused by the front end before execution (no 22014 / 22016 to compare).
describe("window frames and functions against PostgreSQL (review B6-p2b finding 6)", () => {
  it("ROWS frames: moving averages and sums, empty frames", async () => {
    await check(FRAMES, [
      {
        sql: "SELECT id, avg(x::double precision) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS c FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, max(x) OVER (ORDER BY id DESC ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) AS m FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, avg(x::double precision) OVER (PARTITION BY g % 2 ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, first_value(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS f, last_value(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS l, nth_value(x, 2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n FROM w ORDER BY id",
      },
    ]);
  });

  it("RANGE and GROUPS frames over peers, with offsets and NULL keys", async () => {
    await check(FRAMES, [
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY g DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS c FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(x) OVER (ORDER BY g NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS c FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY g GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS c FROM w ORDER BY id",
      },
    ]);
  });

  it("EXCLUDE CURRENT ROW, GROUP and TIES", async () => {
    await check(FRAMES, [
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) AS c FROM w ORDER BY id",
      },
    ]);
  });

  it("ntile, percent_rank, cume_dist and nth_value", async () => {
    await check(FRAMES, [
      { sql: "SELECT id, ntile(3) OVER (ORDER BY id) AS t FROM w ORDER BY id" },
      { sql: "SELECT id, ntile(4) OVER (ORDER BY id DESC) AS t FROM w ORDER BY id" },
      { sql: "SELECT id, ntile(20) OVER (ORDER BY id) AS t FROM w ORDER BY id" },
      { sql: "SELECT id, ntile(2) OVER (PARTITION BY g % 2 ORDER BY id) AS t FROM w ORDER BY id" },
      {
        sql: "SELECT id, percent_rank() OVER (ORDER BY g) AS p, cume_dist() OVER (ORDER BY g) AS c FROM w ORDER BY id",
      },
      { sql: "SELECT id, nth_value(x, 3) OVER (ORDER BY id) AS n FROM w ORDER BY id" },
    ]);
    // The review's probes, on its fixture.
    await check(REVIEW, [
      { sql: "SELECT id, ntile(2) OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      { sql: "SELECT id, percent_rank() OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      { sql: "SELECT id, cume_dist() OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      { sql: "SELECT id, nth_value(v, 2) OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      {
        sql: "SELECT id, v, sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM b ORDER BY v NULLS LAST, id",
      },
    ]);
  });

  it("integer to decimal casts", async () => {
    await check(REVIEW, [
      { sql: "SELECT 1 AS n UNION ALL SELECT 1.5 ORDER BY n" },
      { sql: "SELECT id FROM b WHERE v >= 1.5 AND v <= 2.5 ORDER BY id" },
      { sql: "SELECT CAST(v AS numeric(4, 1)) AS d FROM b WHERE v IS NOT NULL ORDER BY v" },
      { sql: "SELECT CAST(id * 1000 AS numeric(3, 0)) AS d FROM a" },
    ]);
  });
});

describe("RANGE offsets at NULL boundaries and past 2^53 (review B6-p2b-2 finding 3)", () => {
  it("matches PostgreSQL over every placement, bound pair and exclusion", async () => {
    const tables = [
      {
        setup: [
          "CREATE TABLE r (id integer PRIMARY KEY, k integer)",
          "INSERT INTO r VALUES (1, 1), (2, 1), (3, 3), (4, NULL), (5, 6), (6, NULL), (7, 4)",
        ],
        name: "r",
      },
      {
        setup: [
          "CREATE TABLE big (id integer PRIMARY KEY, k bigint)",
          // Through text casts, so the keys do not depend on how big literals are typed.
          "INSERT INTO big VALUES (1, CAST('9007199254740992' AS bigint)), (2, CAST('9007199254740993' AS bigint)), (3, CAST('9007199254740994' AS bigint)), (4, NULL), (5, CAST('9007199254740996' AS bigint)), (6, CAST('-9007199254740993' AS bigint))",
        ],
        name: "big",
      },
    ];
    const orders = ["k", "k NULLS FIRST", "k DESC", "k DESC NULLS LAST"];
    const bounds = [
      "1 PRECEDING AND CURRENT ROW",
      "CURRENT ROW AND 1 FOLLOWING",
      "1 FOLLOWING AND UNBOUNDED FOLLOWING",
      "UNBOUNDED PRECEDING AND 1 PRECEDING",
      "0 PRECEDING AND 0 FOLLOWING",
      "2 PRECEDING AND 1 PRECEDING",
    ];
    const exclusions = ["", " EXCLUDE CURRENT ROW", " EXCLUDE GROUP", " EXCLUDE TIES"];
    for (const table of tables) {
      const cases: Case[] = [];
      for (const order of orders)
        for (const bound of bounds)
          for (const exclusion of exclusions)
            cases.push({
              sql: `SELECT id, count(*) OVER (ORDER BY ${order} RANGE BETWEEN ${bound}${exclusion}) AS n, min(id) OVER (ORDER BY ${order} RANGE BETWEEN ${bound}${exclusion}) AS m FROM ${table.name} ORDER BY id`,
            });
      await check(table.setup, cases);
    }
  });
});

describe("RANGE offsets over NUMERIC, float and temporal keys (review B6-p2b-3 finding 1)", () => {
  // Authority: PostgreSQL's window frames (sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS) and its
  // in_range support functions: exact numeric bounds, NaN above every value and peer of NaN,
  // ±Infinity peers of themselves, an infinite key beyond every finite bound.
  const TABLES: ReadonlyArray<{
    readonly name: string;
    readonly setup: readonly string[];
    readonly offsets: readonly string[];
    readonly states: ReadonlyArray<(typeof RANGE_KEY_STATES)[number]>;
  }> = [
    {
      name: "rn",
      setup: [
        "CREATE TABLE rn (id integer PRIMARY KEY, k numeric)",
        // The review's keys: distinct exact decimals one apart past 2^53.
        "INSERT INTO rn VALUES (1, 9007199254740992), (2, 9007199254740993), (3, 9007199254740994), (4, NULL), (5, 1.25), (6, 1.5), (7, -0.5), (8, 1.50)",
      ],
      offsets: ["0", "1", "0.25", "0.5"],
      states: ["numeric exact"],
    },
    {
      name: "rs",
      setup: [
        "CREATE TABLE rs (id integer PRIMARY KEY, k numeric)",
        "INSERT INTO rs VALUES (1, 'Infinity'), (2, 'Infinity'), (3, '-Infinity'), (4, 0), (5, 1), (6, NULL), (7, 'NaN'), (8, 'NaN')",
      ],
      offsets: ["0", "1"],
      states: ["numeric special"],
    },
    {
      name: "rf",
      setup: [
        "CREATE TABLE rf (id integer PRIMARY KEY, k double precision)",
        // The review's keys, and NaN peers.
        "INSERT INTO rf VALUES (1, 'Infinity'), (2, 'Infinity'), (3, '-Infinity'), (4, 0), (5, 1), (6, NULL), (7, 'NaN'), (8, 'NaN'), (9, 1.5)",
      ],
      offsets: ["0", "1", "0.5"],
      states: ["float infinite", "float NaN"],
    },
    {
      name: "rd",
      setup: [
        "CREATE TABLE rd (id integer PRIMARY KEY, k date)",
        "INSERT INTO rd VALUES (1, DATE '2024-01-01'), (2, DATE '2024-01-02'), (3, NULL), (4, DATE '2024-01-04')",
      ],
      offsets: ["INTERVAL '1 day'", "INTERVAL '2 days'"],
      states: ["temporal refused"],
    },
  ];
  const ORDERS = ["k", "k NULLS FIRST", "k DESC", "k DESC NULLS LAST"];
  const BOUNDS = [
    (o: string) => `${o} PRECEDING AND CURRENT ROW`,
    (o: string) => `CURRENT ROW AND ${o} FOLLOWING`,
    (o: string) => `${o} PRECEDING AND ${o} FOLLOWING`,
    (o: string) => `${o} FOLLOWING AND UNBOUNDED FOLLOWING`,
    (o: string) => `UNBOUNDED PRECEDING AND ${o} PRECEDING`,
  ];
  const EXCLUSIONS = ["", " EXCLUDE GROUP", " EXCLUDE TIES"];

  it("matches PostgreSQL for every key, placement, bound, offset and exclusion", async () => {
    const reach = createReach("RANGE keys", RANGE_KEY_STATES);
    const failures: string[] = [];
    for (const table of TABLES) {
      await pg.exec(`DROP TABLE IF EXISTS ${table.name}`);
      const next = new LoweredDatabase();
      for (const sql of table.setup) {
        await pg.exec(sql);
        // The reference has no dates: lowering refuses these frames before any row is read.
        if (!table.states.includes("temporal refused") || sql.startsWith("CREATE"))
          next.execute(sql);
      }
      for (const order of ORDERS)
        for (const bound of BOUNDS)
          for (const offset of table.offsets)
            for (const exclusion of EXCLUSIONS) {
              reach.beginHistory();
              const frame = `ORDER BY ${order} RANGE BETWEEN ${bound(offset)}${exclusion}`;
              const sql = `SELECT id, count(*) OVER (${frame}) AS n, min(id) OVER (${frame}) AS m FROM ${table.name} ORDER BY id`;
              const expected = await pgOutcome(pg, sql);
              const actual = nextOutcome(next, sql);
              const same = JSON.stringify(actual) === JSON.stringify(expected);
              // Interval offsets: PostgreSQL runs them; lowering refuses them (0A000) for now.
              const refused =
                table.states.includes("temporal refused") &&
                "rows" in expected &&
                "error" in actual &&
                actual.error === "0A000";
              if (same || refused) for (const state of table.states) reach.reach(state);
              else
                failures.push(
                  `${sql}\n  pglite: ${JSON.stringify(expected)}\n  next:   ${JSON.stringify(actual)}`,
                );
            }
    }
    expect(failures.slice(0, 5).join("\n")).toBe("");
    expect(failures.length).toBe(0);
    reach.assertReached();
  });
});

describe("aggregate signatures against PostgreSQL (review B6-p2b-3 finding 2)", () => {
  // Authority: PostgreSQL's aggregate overloads (pg_typeof, 42883), except the rulings
  // ir/functions.ts AGGREGATES records: avg of integers and floats is double precision (C11).
  const COLUMNS: ReadonlyArray<readonly [column: string, type: string]> = [
    ["c_bool", "boolean"],
    ["c_int2", "smallint"],
    ["c_int4", "integer"],
    ["c_int8", "bigint"],
    ["c_float4", "real"],
    ["c_float8", "double precision"],
    ["c_numeric", "numeric"],
    ["c_text", "text"],
    ["c_varchar", "varchar(5)"],
    ["c_date", "date"],
    ["c_time", "time"],
    ["c_timestamp", "timestamp"],
    ["c_timestamptz", "timestamptz"],
    ["c_interval", "interval"],
    ["c_json", "json"],
    ["c_jsonb", "jsonb"],
    ["c_uuid", "uuid"],
    // bytea columns are not declared through the front end yet.
  ];
  const PG_NAMES: Readonly<Record<string, SqlType>> = {
    boolean: "bool",
    smallint: "int2",
    integer: "int4",
    bigint: "int8",
    real: "float4",
    "double precision": "float8",
    numeric: "numeric",
    text: "text",
    "character varying": "text",
    date: "date",
    "time without time zone": "time",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    interval: "interval",
    json: "json",
    jsonb: "jsonb",
    uuid: "uuid",
    bytea: "bytea",
  };
  const RULED: Readonly<Record<string, SqlType>> = {
    "avg(smallint)": "float8",
    "avg(integer)": "float8",
    "avg(bigint)": "float8",
    "avg(real)": "float8",
  };

  it("types or refuses (42883) sum, avg, min, max and count over every column type", async () => {
    const create = `CREATE TABLE agg (${COLUMNS.map(([c, t]) => `${c} ${t}`).join(", ")})`;
    await pg.exec("DROP TABLE IF EXISTS agg");
    await pg.exec(create);
    const next = new LoweredDatabase();
    next.execute(create);
    const failures: string[] = [];
    for (const fn of ["sum", "avg", "min", "max", "count"])
      for (const [column, declared] of COLUMNS) {
        let expected: string;
        try {
          const shown = await pg.query<{ t: string }>(
            `SELECT pg_typeof(${fn}(${column}))::text AS t FROM agg`,
          );
          const name = shown.rows[0]?.t ?? "";
          expected = RULED[`${fn}(${declared})`] ?? PG_NAMES[name] ?? `unmapped ${name}`;
        } catch (error) {
          expected = String((error as { code?: string }).code);
        }
        let actual: string;
        try {
          const lowered = next.lower(`SELECT ${fn}(${column}) AS x FROM agg`);
          const type = lowered.kind === "query" ? lowered.plan.columns[0]?.type : undefined;
          actual = (type === undefined ? null : sqlTypeOf(type)) ?? `no type`;
        } catch (error) {
          actual = error instanceof PlanningError ? error.sqlState : String(error);
        }
        if (actual !== expected)
          failures.push(`${fn}(${declared}): pglite ${expected}, next ${actual}`);
      }
    expect(failures).toEqual([]);
  });
});

describe("NULL keys in RANGE offset frames (the B7 report)", () => {
  // Authority: PostgreSQL's nodeWindowAgg: for a non-NULL current row an offset bound compares
  // only non-NULL keys, and NULLs sort below every key (NULLS FIRST) or above (NULLS LAST), so a
  // frame that runs to that end of the partition (UNBOUNDED, or an offset past every key)
  // includes them. A NULL row's frame is its NULL peers.
  const TABLES = [
    {
      name: "ni",
      setup: [
        "CREATE TABLE ni (id integer PRIMARY KEY, k integer)",
        "INSERT INTO ni VALUES (1, NULL), (2, 1), (3, 2), (4, NULL), (5, 5), (6, 2)",
      ],
    },
    {
      name: "nn",
      setup: [
        "CREATE TABLE nn (id integer PRIMARY KEY, k numeric)",
        "INSERT INTO nn VALUES (1, NULL), (2, 1.5), (3, 2), (4, 9007199254740993), (5, NULL)",
      ],
    },
    {
      name: "nf",
      setup: [
        "CREATE TABLE nf (id integer PRIMARY KEY, k double precision)",
        "INSERT INTO nf VALUES (1, NULL), (2, 'Infinity'), (3, 0.5), (4, '-Infinity'), (5, NULL), (6, 'NaN')",
      ],
    },
  ];
  const ORDERS = [
    "k",
    "k NULLS FIRST",
    "k NULLS LAST",
    "k DESC",
    "k DESC NULLS FIRST",
    "k DESC NULLS LAST",
  ];
  const BOUNDS = [
    (o: string) => `${o} PRECEDING AND CURRENT ROW`,
    (o: string) => `CURRENT ROW AND ${o} FOLLOWING`,
    (o: string) => `${o} PRECEDING AND ${o} FOLLOWING`,
    (o: string) => `UNBOUNDED PRECEDING AND ${o} PRECEDING`,
    (o: string) => `UNBOUNDED PRECEDING AND ${o} FOLLOWING`,
    (o: string) => `${o} PRECEDING AND UNBOUNDED FOLLOWING`,
    (o: string) => `${o} FOLLOWING AND UNBOUNDED FOLLOWING`,
    (o: string) => `${o} FOLLOWING AND ${o} FOLLOWING`,
    (o: string) => `${o} PRECEDING AND ${o} PRECEDING`,
  ];
  // 0 and 1 stay inside the keys; 10 runs past every finite one, to the NULLs at that end.
  const OFFSETS = ["0", "1", "10"];

  it("puts NULL keys where NULLS FIRST / LAST sorts them, as PostgreSQL", async () => {
    const reach = createReach("RANGE NULL keys", RANGE_NULL_STATES);
    const failures: string[] = [];
    for (const table of TABLES) {
      await pg.exec(`DROP TABLE IF EXISTS ${table.name}`);
      const next = new LoweredDatabase();
      for (const sql of table.setup) {
        await pg.exec(sql);
        next.execute(sql);
      }
      for (const order of ORDERS)
        for (const bound of BOUNDS)
          for (const offset of OFFSETS) {
            reach.beginHistory();
            const frame = `ORDER BY ${order} RANGE BETWEEN ${bound(offset)}`;
            const sql = `SELECT id, k IS NULL AS null_key, count(*) OVER (${frame}) AS n, count(k) OVER (${frame}) AS c, min(id) OVER (${frame}) AS m FROM ${table.name} ORDER BY id`;
            const expected = await pgOutcome(pg, sql);
            const actual = nextOutcome(next, sql);
            if (JSON.stringify(actual) !== JSON.stringify(expected)) {
              failures.push(
                `${sql}\n  pglite: ${JSON.stringify(expected)}\n  next:   ${JSON.stringify(actual)}`,
              );
              continue;
            }
            if (!("rows" in expected)) continue;
            const nullsFirst = order.includes("NULLS FIRST") || order === "k DESC";
            for (const row of expected.rows as unknown[][]) {
              const [, nullKey, n, c] = row;
              if (nullKey === true) reach.reach("null row");
              else if (Number(n) > Number(c))
                reach.reach(nullsFirst ? "nulls first in a frame" : "nulls last in a frame");
              else reach.reach("nulls outside a frame");
            }
          }
    }
    expect(failures.slice(0, 3).join("\n")).toBe("");
    expect(failures.length).toBe(0);
    reach.assertReached();
  });
});
