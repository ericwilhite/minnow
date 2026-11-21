/**
 * Set operations against PostgreSQL (PGlite), the authority for rows, duplicates, NULLs, column
 * type resolution and affected-row counts. Generated arms (filters, literal and NULL arms that
 * coerce, empty arms) are combined and wrapped every way a query uses a set operation: as is,
 * ordered with LIMIT/OFFSET, aggregated, grouped, joined, and as an INSERT ... SELECT source.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SET_OPERATION_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

type State = (typeof SET_OPERATION_REACH)[number];
const normal = (value: unknown): unknown =>
  typeof value === "bigint" ? Number(value) : value instanceof Date ? value.toISOString() : value;
const rowsOf = (rows: ReadonlyArray<Record<string, unknown>>) =>
  rows.map((row) => Object.values(row).map(normal));
const bag = (rows: unknown[][]) =>
  [...rows].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

/** One arm: a projection of (k INT, v TEXT) from a source, possibly filtered. */
const arm = fc.oneof(
  fc.record({
    kind: fc.constant("a" as const),
    filter: fc.constantFrom(
      "",
      " WHERE k > 2",
      " WHERE v IS NULL",
      " WHERE id % 2 = 0",
      " WHERE k > 99",
    ),
  }),
  fc.record({
    kind: fc.constant("b" as const),
    filter: fc.constantFrom("", " WHERE w <> 'b1'", " WHERE id > 3"),
  }),
  fc.record({ kind: fc.constant("literal" as const), k: fc.integer({ min: -2, max: 4 }) }),
  fc.record({ kind: fc.constant("nulls" as const) }),
  fc.record({ kind: fc.constant("big" as const) }),
);
type Arm = typeof arm extends fc.Arbitrary<infer T> ? T : never;
const armSql = (a: Arm): string => {
  switch (a.kind) {
    case "a":
      return `SELECT k AS k, v AS v FROM a${a.filter}`;
    case "b":
      return `SELECT k AS k, w AS v FROM b${a.filter}`;
    case "literal":
      return `SELECT ${String(a.k)} AS k, 'lit' AS v`;
    case "nulls":
      return "SELECT NULL AS k, NULL AS v";
    case "big":
      return "SELECT big AS k, 'big' AS v FROM c";
  }
};
const wrap = fc.constantFrom(
  "bag",
  "ordered",
  "aggregated",
  "grouped",
  "joined",
  "insert",
  "rollup",
  "cube",
  "grouping sets",
) satisfies fc.Arbitrary<State>;

for (const seed of seedsFor("next-set-operations", [0x5e71, 0x5e72]))
  it(`UNION ALL matches PostgreSQL (seed ${String(seed)})`, async () => {
    const reach = createReach("set operations", SET_OPERATION_REACH);
    const core = await new StorageCore(32).open(),
      pg = new PGlite();
    await pg.waitReady;
    const both = async (sql: string) => {
      await pg.query(sql);
      await core.execute(sql);
    };
    try {
      await both("CREATE TABLE a(id INT PRIMARY KEY, k INT, v TEXT)");
      await both("CREATE TABLE b(id INT PRIMARY KEY, k INT, w TEXT)");
      await both("CREATE TABLE c(id INT PRIMARY KEY, big BIGINT)");
      await both("CREATE TABLE dst(k BIGINT, v TEXT)");
      await both(
        `INSERT INTO a VALUES ${Array.from({ length: 9 }, (_, i) => `(${String(i)}, ${i % 4 === 3 ? "NULL" : String(i % 5)}, ${i % 3 === 0 ? "NULL" : `'a${String(i % 4)}'`})`).join(", ")}`,
      );
      await both(
        `INSERT INTO b VALUES ${Array.from({ length: 6 }, (_, i) => `(${String(i)}, ${String(i % 3)}, 'b${String(i % 2)}')`).join(", ")}`,
      );
      await both("INSERT INTO c VALUES (1, 5000000000), (2, -7)");
      await fc.assert(
        fc.asyncProperty(
          fc.array(arm, { minLength: 2, maxLength: 4 }),
          wrap,
          fc.integer({ min: 0, max: 6 }),
          fc.integer({ min: 0, max: 3 }),
          async (arms, how, limit, offset) => {
            reach.beginHistory();
            const union = arms.map(armSql).join(" UNION ALL ");
            reach.reach(arms.length > 2 ? "three or more arms" : "two arms");
            if (arms.some((a) => a.kind === "literal" || a.kind === "nulls" || a.kind === "big"))
              reach.reach("coerced arm");
            if (arms.some((a) => "filter" in a && a.filter.includes("99")))
              reach.reach("empty arm");
            reach.reach(how);
            const sql =
              how === "insert"
                ? `INSERT INTO dst ${union}`
                : how === "bag"
                  ? union
                  : how === "ordered"
                    ? `${union} ORDER BY 1 NULLS LAST, 2 NULLS LAST LIMIT ${String(limit)} OFFSET ${String(offset)}`
                    : how === "aggregated"
                      ? `SELECT count(*) AS n, count(k) AS kn, sum(k) AS s, min(v) AS lo FROM (${union}) u(k, v)`
                      : how === "grouped"
                        ? `SELECT k, count(*) AS n FROM (${union}) u(k, v) GROUP BY k ORDER BY k NULLS LAST`
                        : how === "joined"
                          ? `SELECT u.k, b.w FROM (${union}) u(k, v) JOIN b ON b.k = u.k ORDER BY 1, 2`
                          : // Several grouping sets lower to a UNION ALL of grouped aggregates.
                            `SELECT k, v, count(*) AS n, sum(k) AS s FROM (${union}) u(k, v) GROUP BY ${
                              how === "rollup"
                                ? "ROLLUP (k, v)"
                                : how === "cube"
                                  ? "CUBE (k, v)"
                                  : "GROUPING SETS ((k), (v), ())"
                            } ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3, 4 NULLS LAST`;
            if (how === "insert") {
              await pg.query("DELETE FROM dst");
              await core.execute("DELETE FROM dst");
            }
            let expected: Awaited<ReturnType<typeof pg.query<Record<string, unknown>>>>;
            try {
              expected = await pg.query<Record<string, unknown>>(sql);
            } catch (error) {
              // A statement PostgreSQL refuses (arm types that cannot be matched, a value of the
              // wrong type for its column) is refused by next/ with the same SQLSTATE.
              const code = (error as { code?: string }).code ?? "unknown";
              await expect(core.execute(sql), sql).rejects.toMatchObject({ sqlState: code });
              reach.reach("refused like PostgreSQL");
              return;
            }
            const result = await core.execute(sql);
            if (how === "insert") {
              if ("rowCount" in result) expect(result.rowCount, sql).toBe(expected.affectedRows);
              const check = "SELECT k, v FROM dst ORDER BY k NULLS LAST, v NULLS LAST";
              const now = await core.execute(check);
              if (!("batches" in now)) throw new Error("dst");
              expect(rowsOf(oracleRows(now)), sql).toEqual(
                rowsOf((await pg.query<Record<string, unknown>>(check)).rows),
              );
              return;
            }
            if (!("batches" in result)) throw new Error(`no rows: ${sql}`);
            const actual = rowsOf(oracleRows(result));
            const wanted = rowsOf(expected.rows);
            if (how === "bag") {
              expect(bag(actual), sql).toEqual(bag(wanted));
              if (new Set(actual.map((row) => JSON.stringify(row))).size < actual.length)
                reach.reach("duplicates kept");
            } else expect(actual, sql).toEqual(wanted);
            expect(core.spill.bytesInUse, sql).toBe(0);
          },
        ),
        {
          seed,
          numRuns: 60,
          examples: [
            [
              [
                { kind: "a", filter: "" },
                { kind: "a", filter: "" },
              ],
              "bag",
              0,
              0,
            ],
            [
              [{ kind: "a", filter: " WHERE k > 99" }, { kind: "b", filter: "" }, { kind: "big" }],
              "ordered",
              4,
              1,
            ],
            [
              [
                { kind: "nulls" },
                { kind: "literal", k: 2 },
                { kind: "b", filter: " WHERE id > 3" },
              ],
              "aggregated",
              0,
              0,
            ],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 240000);
