import { afterAll, describe, expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../testing/seeds.js";
import {
  createReach,
  DOUBLE_AGGREGATE_STATES,
  INTEGER_ARITHMETIC_EXACTNESS_STATES,
  INTEGER_MUTATION_EXACTNESS_STATES,
} from "../testing/reach.js";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import {
  annotatePlanIntegerDivision,
  bindPlanParameters,
  compileQuery,
  executeQuery,
  executeRowQuery,
} from "./query.js";

const doubleReach = createReach("double-aggregate-exactness", DOUBLE_AGGREGATE_STATES);
const reach = createReach("integer-exactness", INTEGER_ARITHMETIC_EXACTNESS_STATES);
const mutationReach = createReach("integer-mutation-exactness", INTEGER_MUTATION_EXACTNESS_STATES);
afterAll(() => {
  doubleReach.assertReached();
  reach.assertReached();
  mutationReach.assertReached();
});

describe("integer exactness", () => {
  for (const seed of seedsFor("integer-exactness", [11, 47, 101])) {
    it(`checks boundary arithmetic and sums against BigInt, seed ${String(seed)}`, () => {
      fc.assert(
        fc.property(
          fc.integer({ min: 0, max: 32 }),
          fc.integer({ min: 1, max: 40 }),
          fc.boolean(),
          (offset, delta, negative) => {
            reach.beginHistory();
            for (const execute of [executeQuery, executeRowQuery]) {
              const plan = bindPlanParameters(compileQuery("SELECT 1 + $1 AS v"), [delta / 2]);
              expect(execute(plan, new Map()).rows[0]?.v).toBe(1 + delta / 2);
            }
            if (delta % 2 === 1) reach.reach("fractional parameter");

            const a = (negative ? -1 : 1) * (Number.MAX_SAFE_INTEGER - offset);
            const b = (negative ? -1 : 1) * delta;
            const exact = BigInt(a) + BigInt(b);
            const safe =
              exact >= -BigInt(Number.MAX_SAFE_INTEGER) && exact <= BigInt(Number.MAX_SAFE_INTEGER);
            reach.reach(safe ? "safe" : negative ? "negative overflow" : "positive overflow");
            for (const execute of [executeQuery, executeRowQuery]) {
              for (const plan of [
                compileQuery(`SELECT a - '${exact.toString()}' AS v FROM t`),
                bindPlanParameters(compileQuery("SELECT a - $1 AS v FROM t"), [exact.toString()]),
              ]) {
                annotatePlanIntegerDivision(plan, () => true);
                const run = () => execute(plan, new Map([["t", [{ a }]]]));
                if (safe) expect(run().rows[0]?.v).toBe(-b);
                else expect(run).toThrow(RangeError);
              }
              const fractionalText = compileQuery("SELECT a - '1.5' AS v FROM t");
              annotatePlanIntegerDivision(fractionalText, () => true);
              expect(() => execute(fractionalText, new Map([["t", [{ a }]]]))).toThrow(TypeError);
              expect(
                execute(
                  compileQuery("SELECT CAST(a AS DOUBLE PRECISION) - '1.5' AS v FROM t"),
                  new Map([["t", [{ a }]]]),
                ).rows[0]?.v,
              ).toBe(a - 1.5);
            }
            reach.reach(safe ? "integer text safe" : "integer text refusal");
            reach.reach("fractional text refusal");

            for (const execute of [executeQuery, executeRowQuery]) {
              const count = Math.abs(a);
              const run = () =>
                execute(
                  compileQuery("SELECT WIDTH_BUCKET(a, 0, 1, b) AS v FROM t"),
                  new Map([["t", [{ a: 2, b: count }]]]),
                );
              if (count === Number.MAX_SAFE_INTEGER) expect(run).toThrow(RangeError);
              else expect(run().rows[0]?.v).toBe(count + 1);
            }
            for (const sql of [
              "SELECT a + b AS v FROM t",
              "SELECT SUM(a) AS v FROM t",
              "SELECT SUM(DISTINCT a) AS v FROM t",
              "SELECT SUM(CASE WHEN g = 0 THEN a ELSE 0 END) AS v FROM t",
              "SELECT SUM(CASE WHEN label = 'x' THEN a ELSE 0 END) AS v FROM t",
              "SELECT SUM(a) AS v FROM t GROUP BY label",
              "SELECT SUM(a) AS v FROM t GROUP BY g, b",
              "SELECT SUM(a) AS v FROM t GROUP BY g",
              "SELECT AVG(a) AS v FROM t",
              "SELECT SUM(a) OVER (ORDER BY g) AS v FROM t",
              "SELECT AVG(a) OVER (ORDER BY g) AS v FROM t",
              "SELECT SUM(a) OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM t",
            ]) {
              const rows = sql.includes("a + b")
                ? [{ a, b, g: 0, label: "x" }]
                : [
                    { a, b: 0, g: 0, label: "x" },
                    { a: b, b: 0, g: 0, label: "x" },
                  ];
              for (const execute of [executeQuery, executeRowQuery]) {
                const plan = compileQuery(sql);
                annotatePlanIntegerDivision(plan, () => true);
                const run = () => execute(plan, new Map([["t", rows]]));
                if (!safe) expect(run, sql).toThrow(RangeError);
                else {
                  const result = run();
                  expect(result.rows.at(-1)?.v, sql).toBe(
                    sql.includes("AVG") ? Number(exact) / 2 : Number(exact),
                  );
                }
              }
              if (sql.includes("label")) reach.reach("dictionary aggregate");
            }

            for (const [operator, x, y] of [
              ["-", a, -b],
              ["*", (negative ? -1 : 1) * (94906265 + offset), 94906265 + delta],
            ] as const) {
              const result = operator === "-" ? BigInt(x) - BigInt(y) : BigInt(x) * BigInt(y);
              for (const execute of [executeQuery, executeRowQuery]) {
                const plan = compileQuery(`SELECT a ${operator} b AS v FROM t`);
                annotatePlanIntegerDivision(plan, () => true);
                const run = () => execute(plan, new Map([["t", [{ a: x, b: y }]]]));
                if (
                  result < -BigInt(Number.MAX_SAFE_INTEGER) ||
                  result > BigInt(Number.MAX_SAFE_INTEGER)
                )
                  expect(run).toThrow(RangeError);
                else expect(run().rows[0]?.v).toBe(Number(result));
              }
            }
          },
        ),
        { seed, numRuns: 80 },
      );
    });
  }

  for (const seed of seedsFor("double-aggregate-exactness", [11, 47, 101])) {
    it(`preserves typed DOUBLE aggregates at integer boundaries, seed ${String(seed)}`, () => {
      fc.assert(
        fc.property(
          fc.boolean(),
          fc.integer({ min: 0, max: 32 }),
          fc.constantFrom("whole safe inputs", "whole unsafe inputs", "fractional inputs"),
          (negative, offset, kind) => {
            doubleReach.beginHistory();
            const sign = negative ? -1 : 1;
            const d =
              sign *
              (kind === "whole safe inputs"
                ? Number.MAX_SAFE_INTEGER - offset
                : kind === "whole unsafe inputs"
                  ? 2 ** 53 + offset * 2
                  : 2 ** 51 + offset + 0.5);
            const values = [d, d, 0.5];
            doubleReach.reach(
              kind === "fractional inputs" ? kind : `${negative ? "negative" : "positive"} ${kind}`,
            );
            const rows = values.map((d, id) => ({ d, id, g: "a", k: 0 }));
            for (const sql of [
              "SELECT SUM(d) AS v FROM t",
              "SELECT AVG(d) AS v FROM t",
              "SELECT SUM(DISTINCT d) AS v FROM t",
              "SELECT SUM(d) AS v FROM t GROUP BY g",
              "SELECT SUM(d) AS v FROM t GROUP BY k",
              "SELECT SUM(d) AS v FROM t GROUP BY g, k",
              "SELECT SUM(d + 0) AS v FROM t",
              "SELECT SUM(CASE WHEN g = 'a' THEN d ELSE 0 END) AS v FROM t",
              "SELECT SUM(d) OVER (ORDER BY id) AS v FROM t",
              "SELECT AVG(d) OVER (ORDER BY id) AS v FROM t",
              "SELECT SUM(d) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM t",
            ]) {
              const contributions = sql.includes("DISTINCT")
                ? [...new Set(values)]
                : sql.includes("1 PRECEDING")
                  ? values.slice(-2)
                  : values;
              const total = contributions.reduce((a, b) => a + b, 0);
              const expected = sql.includes("AVG") ? total / contributions.length : total;
              for (const execute of [executeQuery, executeRowQuery]) {
                const plan = compileQuery(sql);
                annotatePlanIntegerDivision(plan, () => new Set());
                expect(execute(plan, new Map([["t", rows]])).rows.at(-1)?.v, sql).toBe(expected);
                // No catalog means the column has no integer declaration, even if every value is whole.
                expect(execute(compileQuery(sql), new Map([["t", rows]])).rows.at(-1)?.v, sql).toBe(
                  expected,
                );
              }
              if (sql.includes("GROUP BY g") || sql.includes("CASE"))
                doubleReach.reach("dictionary");
              if (sql.includes("OVER")) doubleReach.reach("window");
            }
            doubleReach.reach("unknown inputs");
          },
        ),
        { seed, numRuns: 80 },
      );
    });
  }

  it("preserves the five catalog DOUBLE aggregate regressions", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, d DOUBLE PRECISION, g TEXT)");
      await db.execute(
        "INSERT INTO t VALUES (1, 5000000000000000, 'a'), (2, 5000000000000000, 'a'), (3, 0.5, 'b')",
      );
      for (const [sql, expected] of [
        ["SELECT SUM(d) AS v FROM t", [{ v: 1e16 }]],
        [
          "SELECT g, SUM(d) AS v FROM t GROUP BY g ORDER BY g",
          [
            { g: "a", v: 1e16 },
            { g: "b", v: 0.5 },
          ],
        ],
        ["SELECT AVG(d) AS v FROM t", [{ v: 1e16 / 3 }]],
        ["SELECT SUM(d) OVER (ORDER BY id) AS v FROM t", [{ v: 5e15 }, { v: 1e16 }, { v: 1e16 }]],
        ["SELECT SUM(CASE WHEN g='a' THEN d ELSE 0 END) AS v FROM t", [{ v: 1e16 }]],
      ] as const) {
        expect((await db.query(sql)).rows, sql).toEqual(expected);
        const rows = [
          { id: 1, d: 5e15, g: "a" },
          { id: 2, d: 5e15, g: "a" },
          { id: 3, d: 0.5, g: "b" },
        ];
        for (const execute of [executeQuery, executeRowQuery]) {
          const plan = compileQuery(sql);
          annotatePlanIntegerDivision(plan, () => new Set(["id"]));
          expect(execute(plan, new Map([["t", rows]])).rows, sql).toEqual(expected);
        }
      }
      const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];
      for (const argument of ["$1", "CASE WHEN id = 1 THEN $1 ELSE 5000000000000000 END"]) {
        for (const suffix of ["", " OVER (ORDER BY id)"]) {
          const sql = `SELECT SUM(${argument})${suffix} AS v FROM t`;
          for (const value of [5e15, 0.5, 1e16]) {
            const contributions = argument === "$1" ? rows.map(() => value) : [value, 5e15, 5e15];
            const expected = contributions.reduce((a, b) => a + b, 0);
            for (const execute of [executeQuery, executeRowQuery]) {
              const plan = bindPlanParameters(compileQuery(sql), [value]);
              annotatePlanIntegerDivision(plan, () => new Set(["id"]));
              const run = () => execute(plan, new Map([["t", rows]]));
              if (value === 5e15) expect(run).toThrow(RangeError);
              else expect(run().rows.at(-1)?.v, sql).toBe(expected);
            }
            const run = db.query(sql, { params: [value] });
            if (value === 5e15) await expect(run).rejects.toThrow(RangeError);
            else expect((await run).rows.at(-1)?.v, sql).toBe(expected);
          }
        }
      }
    } finally {
      await db.close();
    }
  });

  it("refuses oversized integer-valued scalar results", () => {
    for (const execute of [executeQuery, executeRowQuery]) {
      for (const sql of [
        "SELECT WIDTH_BUCKET(a, 0, 1, b) AS v FROM t",
        "SELECT DIV(a, b) AS v FROM t",
      ]) {
        const rows = sql.includes("WIDTH_BUCKET")
          ? [{ a: 2, b: Number.MAX_SAFE_INTEGER }]
          : [{ a: 1e30, b: 1 }];
        expect(() => execute(compileQuery(sql), new Map([["t", rows]]))).toThrow(RangeError);
      }
    }
  });

  it("keeps the last exact live sum after overflow and recovers on the next write", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    const live = db.liveQueries();
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a BIGINT)");
      await db.execute("INSERT INTO t VALUES (1, 9007199254740991)");
      const seen: unknown[] = [];
      const errors: unknown[] = [];
      await live.subscribe("SELECT SUM(a) AS v FROM t", {
        onChange: (result) => {
          seen.push(result.rows);
        },
        onError: (error) => {
          errors.push(error);
        },
      });
      await db.execute("INSERT INTO t VALUES (2, 2)");
      await live.refresh();
      expect(errors.some((error) => error instanceof RangeError)).toBe(true);
      expect(seen.at(-1)).toEqual([{ v: Number.MAX_SAFE_INTEGER }]);
      await db.execute("UPDATE t SET a = -1 WHERE id = 2");
      await live.refresh();
      expect(seen.at(-1)).toEqual([{ v: Number.MAX_SAFE_INTEGER - 1 }]);
    } finally {
      live.close();
      await db.close();
    }
  });

  for (const seed of seedsFor("integer-mutation-exactness", [11, 47, 101])) {
    it(`checks overflowing mutation intermediates, seed ${String(seed)}`, async () => {
      const db = new MinnowDatabase(new MemoryBlockStore());
      try {
        await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a BIGINT, b BIGINT)");
        await db.execute("INSERT INTO t VALUES (1, 0, 0)");
        await db.execute("CREATE TABLE events (v BIGINT, w BIGINT)");
        await db.execute("CREATE TABLE out (id INTEGER PRIMARY KEY, n DOUBLE PRECISION)");
        await db.execute("INSERT INTO out VALUES (1,0)");
        for (const [name, type] of [
          ["event_i", "BIGINT"],
          ["event_f", "DOUBLE PRECISION"],
        ] as const) {
          await db.execute(`CREATE TABLE ${name} (v ${type})`);
          await db.execute(
            `CREATE TRIGGER ${name}_divide AFTER INSERT ON ${name} BEGIN UPDATE out SET n = NEW.v / 2 WHERE id = 1; END`,
          );
        }
        await db.execute(
          "CREATE TRIGGER change_t AFTER INSERT ON events BEGIN UPDATE t SET a = (NEW.v + NEW.w) - NEW.w WHERE id = 1; END",
        );
        await db.execute(
          "CREATE TABLE date_source (id INTEGER PRIMARY KEY, first_day DATE, last_day DATE, v BIGINT, days BIGINT)",
        );
        await db.execute(
          "INSERT INTO date_source VALUES (1, DATE '2020-01-01', DATE '2020-01-02', 0, 1)",
        );
        await db.execute(
          "CREATE TABLE date_events (first_day DATE, last_day DATE, v BIGINT, days BIGINT)",
        );
        await db.execute(
          "CREATE TRIGGER date_change AFTER INSERT ON date_events BEGIN UPDATE t SET a = ((NEW.last_day - NEW.first_day) * NEW.v) / NEW.days WHERE id = 1; END",
        );
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 0, max: 32 }),
            fc.integer({ min: 1, max: 40 }),
            fc.boolean(),
            fc.boolean(),
            async (offset, delta, negative, integerSource) => {
              const a = (negative ? -1 : 1) * (Number.MAX_SAFE_INTEGER - offset);
              const b = (negative ? -1 : 1) * delta;
              const exact = BigInt(a) + BigInt(b);
              const safe =
                exact >= -BigInt(Number.MAX_SAFE_INTEGER) &&
                exact <= BigInt(Number.MAX_SAFE_INTEGER);
              mutationReach.beginHistory();
              mutationReach.reach(
                safe ? "safe" : negative ? "negative overflow" : "positive overflow",
              );
              for (const sql of [
                "INSERT INTO t VALUES (1, 0, 0) ON CONFLICT (id) DO UPDATE SET a = (t.a + t.b) - t.b",
                "MERGE INTO t AS target USING (SELECT 1 AS id) AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET a = (target.a + target.b) - target.b",
                "INSERT INTO events VALUES ($1, $2)",
              ]) {
                await db.execute("UPDATE t SET a = $1, b = $2 WHERE id = 1", [a, b]);
                const run = db.execute(
                  sql,
                  sql.startsWith("INSERT INTO events") ? [a, b] : undefined,
                );
                if (safe) await run;
                else await expect(run, sql).rejects.toThrow(RangeError);
                expect(await db.query("SELECT a FROM t")).toMatchObject({ rows: [{ a }] });
              }
              await db.execute(`INSERT INTO ${integerSource ? "event_i" : "event_f"} VALUES ($1)`, [
                delta,
              ]);
              expect(await db.query("SELECT n FROM out")).toMatchObject({
                rows: [{ n: integerSource ? Math.trunc(delta / 2) : delta / 2 }],
              });
              if (delta % 2 === 1)
                mutationReach.reach(
                  integerSource ? "integer trigger source" : "float trigger source",
                );
              const days = delta % 2 === 0 ? 1 : delta + 1;
              const lastDay = new Date(Date.UTC(2020, 0, 1 + days)).toISOString().slice(0, 10);
              mutationReach.reach(days === 1 ? "date-derived safe" : "date-derived overflow");
              await db.execute(
                "UPDATE date_source SET last_day = $1, v = $2, days = $3 WHERE id = 1",
                [lastDay, a, days],
              );
              for (const [sql, params] of [
                [
                  "MERGE INTO t AS target USING date_source AS s ON target.id = s.id WHEN MATCHED THEN UPDATE SET a = ((CAST(s.last_day AS DATE) - CAST(s.first_day AS DATE)) * s.v) / s.days",
                  undefined,
                ],
                [
                  "INSERT INTO date_events VALUES (DATE '2020-01-01', $1, $2, $3)",
                  [lastDay, a, days],
                ],
              ] as const) {
                const run = db.execute(sql, params);
                if (days === 1) await run;
                else await expect(run, sql).rejects.toThrow(RangeError);
                expect(await db.query("SELECT a FROM t")).toMatchObject({ rows: [{ a }] });
              }
              const sequenceDb = new MinnowDatabase(new MemoryBlockStore());
              try {
                const base = Math.abs(a);
                const sequenceSafe =
                  BigInt(base) + 1n + BigInt(delta) <= BigInt(Number.MAX_SAFE_INTEGER);
                await sequenceDb.execute("CREATE SEQUENCE s");
                for (const name of ["NEXTVAL", "CURRVAL"]) {
                  const run = sequenceDb.query(
                    `SELECT ((${name}('s') + ${String(base)}) + ${String(delta)}) - ${String(delta)} - 1 AS v`,
                  );
                  if (sequenceSafe) expect((await run).rows).toEqual([{ v: base }]);
                  else await expect(run).rejects.toThrow(RangeError);
                }
                await sequenceDb.execute("CREATE SEQUENCE d");
                await sequenceDb.execute(
                  `CREATE TABLE defaulted (a BIGINT DEFAULT (((NEXTVAL('d') + ${String(base)}) + ${String(delta)}) - ${String(delta)} - 1))`,
                );
                const write = sequenceDb.execute("INSERT INTO defaulted DEFAULT VALUES");
                if (sequenceSafe) {
                  await write;
                  expect((await sequenceDb.query("SELECT a FROM defaulted")).rows).toEqual([
                    { a: base },
                  ]);
                } else await expect(write).rejects.toThrow(RangeError);
                mutationReach.reach(sequenceSafe ? "sequence safe" : "sequence overflow");
              } finally {
                await sequenceDb.close();
              }
            },
          ),
          { seed, numRuns: 40 },
        );
      } finally {
        await db.close();
      }
    });
  }

  it("checks catalog arithmetic, writes, and exact constant folds", async () => {
    const db = new MinnowDatabase(new MemoryBlockStore());
    try {
      await db.execute("CREATE TABLE t (a BIGINT, b BIGINT)");
      await db.execute("INSERT INTO t VALUES (9007199254740991, 2)");
      for (const expression of [
        "a + b",
        "a - -b",
        "a * b",
        "ABS(a + b)",
        "-(a + b)",
        "(a + b) % b",
        "DIV(a + b, b)",
      ]) {
        await expect(db.execute(`SELECT ${expression} FROM t`)).rejects.toThrow(RangeError);
      }
      await expect(db.execute("SELECT a - '9007199254740993' AS v FROM t")).rejects.toThrow(
        RangeError,
      );
      await expect(db.execute("SELECT a - $1 AS v FROM t", ["9007199254740993"])).rejects.toThrow(
        RangeError,
      );
      await expect(db.execute("INSERT INTO t VALUES (9007199254740993, 1)")).rejects.toThrow();
      await db.execute(
        "CREATE TABLE generated (a BIGINT, b BIGINT GENERATED ALWAYS AS (a + 2) STORED)",
      );
      await expect(
        db.execute("INSERT INTO generated (a) VALUES (9007199254740991)"),
      ).rejects.toThrow(RangeError);
      await db.execute("CREATE TABLE checked (a BIGINT CHECK (a + 2 > 0))");
      const failure: unknown = await db
        .execute("INSERT INTO checked VALUES (9007199254740991)")
        .catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(TypeError);
      if (failure instanceof Error) expect(failure.cause).toBeInstanceOf(RangeError);
      const result = await db.execute("SELECT 9007199254740991 + 2 AS v");
      expect(result).toMatchObject({ result: { rows: [{ v: "9007199254740993" }] } });
    } finally {
      await db.close();
    }
  });
});
