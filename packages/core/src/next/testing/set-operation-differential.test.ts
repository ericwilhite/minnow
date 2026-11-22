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
import { listedGap } from "./next-parity-gaps.js";

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
  // Text concatenation evaluates in producer phases.
  fc.record({ kind: fc.constant("concat" as const) }),
  fc.record({ kind: fc.constant("big" as const) }),
  // Other column names: a compound's output columns are its first arm's (PostgreSQL).
  fc.record({ kind: fc.constant("renamed" as const) }),
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
    case "concat":
      return "SELECT k AS k, v || '!' AS v FROM a";
    case "renamed":
      return "SELECT k AS n, w AS label FROM b";
    case "big":
      return "SELECT big AS k, 'big' AS v FROM c";
  }
};
const OPERATORS = [
  "UNION ALL",
  "UNION",
  "INTERSECT",
  "INTERSECT ALL",
  "EXCEPT",
  "EXCEPT ALL",
] as const;
const operator = fc.constantFrom(...OPERATORS);
const wrap = fc.constantFrom(
  "bag",
  "ordered",
  "ordinal",
  "aggregated",
  "grouped",
  "joined",
  "insert",
  "rollup",
  "cube",
  "grouping sets",
) satisfies fc.Arbitrary<State>;

for (const seed of seedsFor("next-set-operations", [0x5e71, 0x5e72]))
  it(`set operations match PostgreSQL (seed ${String(seed)})`, async () => {
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
          fc.array(operator, { minLength: 3, maxLength: 3 }),
          wrap,
          fc.integer({ min: 0, max: 6 }),
          fc.integer({ min: 0, max: 3 }),
          async (arms, ops, how, limit, offset) => {
            reach.beginHistory();
            // PostgreSQL binds INTERSECT tighter than UNION and EXCEPT; mixed chains check it.
            const used = ops.slice(0, arms.length - 1);
            const union = arms
              .map((a, i) => (i === 0 ? armSql(a) : `${used[i - 1] ?? "UNION ALL"} ${armSql(a)}`))
              .join(" ");
            for (const op of used) reach.reach(op.toLowerCase() as State);
            if (new Set(used).size > 1) reach.reach("mixed chain");
            reach.reach(arms.length > 2 ? "three or more arms" : "two arms");
            if (arms.some((a) => a.kind === "literal" || a.kind === "nulls" || a.kind === "big"))
              reach.reach("coerced arm");
            if (arms.some((a) => "filter" in a && a.filter.includes("99")))
              reach.reach("empty arm");
            reach.reach(how);
            if (arms.some((a) => a.kind === "renamed")) reach.reach("renamed arm");
            // A leading INTERSECT chain followed by UNION or EXCEPT is a nested first member:
            // the compound's output columns are that chain's first arm's (PostgreSQL).
            const nestedFirst =
              used[0]?.startsWith("INTERSECT") === true &&
              used.some((op) => !op.startsWith("INTERSECT"));
            if (how === "ordinal" && nestedFirst) reach.reach("ordinal over a nested first member");
            const sql =
              how === "insert"
                ? `INSERT INTO dst ${union}`
                : how === "bag"
                  ? union
                  : how === "ordered"
                    ? `${union} ORDER BY k NULLS LAST, v NULLS LAST LIMIT ${String(limit)} OFFSET ${String(offset)}`
                    : how === "ordinal"
                      ? // Ordinals name the compound's output columns.
                        `${union} ORDER BY 2 DESC NULLS FIRST, 1 NULLS LAST LIMIT ${String(limit)} OFFSET ${String(offset)}`
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
            let result: Awaited<ReturnType<typeof core.execute>>;
            try {
              result = await core.execute(sql);
            } catch (error) {
              // The one documented refusal PostgreSQL does not share: an expression evaluated in
              // producer phases inside a UNION ALL arm (README "Set operations"). ROLLUP, CUBE and
              // GROUPING SETS run as UNION ALL arms over the whole chain.
              const unionAllPhase =
                (used.includes("UNION ALL") ||
                  how === "rollup" ||
                  how === "cube" ||
                  how === "grouping sets") &&
                arms.some((a) => a.kind === "concat") &&
                listedGap(error)?.id === "union-all-phase-arm";
              if (!unionAllPhase) throw error;
              reach.reach("phase arm refused under UNION ALL");
              return;
            }
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
            if (arms.some((x) => x.kind === "concat"))
              reach.reach("phase arm in a hash-count set operation");
            if (!("batches" in result)) throw new Error(`no rows: ${sql}`);
            const actual = rowsOf(oracleRows(result));
            const wanted = rowsOf(expected.rows);
            // Output column names are the first arm's.
            expect(
              expected.fields.map((field) => field.name),
              sql,
            ).toEqual(result.columns.map((column) => column.name));
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
              [{ kind: "a", filter: "" }, { kind: "renamed" }, { kind: "a", filter: "" }],
              ["INTERSECT", "UNION", "UNION ALL"],
              "ordinal",
              6,
              0,
            ],
            [
              [
                { kind: "a", filter: "" },
                { kind: "a", filter: " WHERE k > 2" },
                { kind: "b", filter: "" },
              ],
              ["INTERSECT", "UNION", "UNION ALL"],
              "ordinal",
              6,
              1,
            ],
            [
              [
                { kind: "a", filter: "" },
                { kind: "a", filter: "" },
              ],
              ["UNION ALL", "UNION", "UNION"],
              "bag",
              0,
              0,
            ],
            [
              [{ kind: "a", filter: " WHERE k > 99" }, { kind: "b", filter: "" }, { kind: "big" }],
              ["UNION ALL", "UNION ALL", "UNION"],
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
              ["UNION ALL", "UNION ALL", "UNION"],
              "aggregated",
              0,
              0,
            ],
            [
              [
                { kind: "a", filter: "" },
                { kind: "b", filter: "" },
                { kind: "a", filter: "" },
              ],
              ["UNION", "INTERSECT", "EXCEPT"],
              "bag",
              0,
              0,
            ],
            [
              [
                { kind: "a", filter: "" },
                { kind: "a", filter: " WHERE k > 2" },
              ],
              ["INTERSECT ALL", "EXCEPT ALL", "UNION"],
              "ordered",
              6,
              0,
            ],
            [
              [
                { kind: "a", filter: "" },
                { kind: "a", filter: " WHERE id % 2 = 0" },
                { kind: "b", filter: "" },
              ],
              ["EXCEPT ALL", "INTERSECT", "UNION"],
              "bag",
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

it("compares blank-padded CHAR and json like PostgreSQL, or refuses", async () => {
  const pg = new PGlite();
  const core = await new StorageCore(32).open();
  try {
    for (const ddl of [
      "CREATE TABLE p(id INT PRIMARY KEY, j JSON)",
      "INSERT INTO p VALUES (1, '{\"x\":1}'), (2, '{\"x\":1}'), (3, '[1]')",
      // next/ cannot write CHAR(n) values yet (bounded text casts are refused), so this table
      // stays empty; the refusal below comes from planning, before any row is read.
      "CREATE TABLE q(id INT PRIMARY KEY, c CHAR(3))",
    ]) {
      await pg.exec(ddl);
      await core.execute(ddl);
    }
    for (const kind of ["UNION", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"]) {
      // CHAR(n) compares without trailing blanks (bpchar equality, so 'a' = 'a  '). The
      // hash-count operator keys the stored text, so next/ refuses rather than split them.
      const chars = `SELECT c FROM q ${kind} SELECT c FROM q`;
      expect((await pg.query(chars)).rows, chars).toEqual([]);
      const refusal: unknown = await core.execute(chars).then(
        () => undefined,
        (error: unknown) => error,
      );
      expect(refusal, chars).toMatchObject({ sqlState: "0A000" });
      expect(String(refusal), chars).toContain("blank-padded CHAR equality");
      // json has no equality operator: PostgreSQL refuses every deduplicating set operation.
      const json = `SELECT j FROM p ${kind} SELECT j FROM p`;
      await expect(pg.query(json), json).rejects.toMatchObject({ code: "42883" });
      await expect(core.execute(json), json).rejects.toMatchObject({ sqlState: "42883" });
    }
    // UNION ALL compares nothing, so both types run.
    for (const all of [
      "SELECT c FROM q UNION ALL SELECT c FROM q",
      "SELECT j::text AS j FROM p UNION ALL SELECT j::text FROM p ORDER BY 1",
    ]) {
      const ran = await core.execute(all);
      if (!("batches" in ran)) throw new Error(`Expected UNION ALL rows: ${all}`);
      expect(rowsOf(oracleRows(ran)), all).toEqual(
        rowsOf((await pg.query<Record<string, unknown>>(all)).rows),
      );
    }
  } finally {
    await core.close();
    await pg.close();
  }
}, 60_000);

// Known divergence, pinned so the planning-time folding fix flips it: PostgreSQL folds immutable
// constant expressions while planning (eval_const_expressions), so a constant 1/0 raises 22012
// before any row is produced, whatever LIMIT says. next/ evaluates it lazily, like 0.20 and like
// next/ without a set operation (SELECT 1/0 LIMIT 0). Data-dependent errors stay lazy in both.
it("known divergence: a constant error in a later UNION ALL arm is skipped by LIMIT", async () => {
  const pg = new PGlite();
  const core = await new StorageCore(32).open();
  try {
    const sql = "SELECT 99 AS x UNION ALL SELECT 1/0 LIMIT 1";
    await expect(pg.query(sql)).rejects.toMatchObject({ code: "22012" });
    const result = await core.execute(sql);
    if (!("batches" in result)) throw new Error("Expected rows");
    expect(oracleRows(result)).toEqual([{ x: 99 }]);
    // A data-dependent error behind the same stop is lazy in PostgreSQL too. (A VALUES source
    // would not do: PostgreSQL pulls it up and folds 1 / (1 - 1) as a constant.)
    for (const ddl of ["CREATE TABLE z(x INT)", "INSERT INTO z VALUES (1)"]) {
      await pg.exec(ddl);
      await core.execute(ddl);
    }
    const lazy = "SELECT 99 AS x UNION ALL SELECT 1 / (x - x) FROM z LIMIT 1";
    expect((await pg.query(lazy)).rows).toEqual([{ x: 99 }]);
    const lazyResult = await core.execute(lazy);
    if (!("batches" in lazyResult)) throw new Error("Expected rows");
    expect(oracleRows(lazyResult)).toEqual([{ x: 99 }]);
  } finally {
    await core.close();
    await pg.close();
  }
});
