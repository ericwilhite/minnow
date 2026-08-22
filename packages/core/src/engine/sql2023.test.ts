/**
 * SQL:2023 (ISO/IEC 9075:2023) surface tests, keyed to the feature IDs of Annex F. The
 * differential harness diffs these forms against SQLite and PGlite where the oracles agree;
 * this file pins the semantics the standard fixes on its own — including the ones no oracle
 * can judge, like a statement's single reading of the clock — and the errors that mark the
 * boundary of what the engine accepts.
 *
 * `run` executes through both executors and asserts they agree, so no feature can be correct in
 * one and wrong in the other.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import { compileQuery, executeQuery, executeRowQuery } from "./query.js";

const tables = new Map<string, DatabaseRow[]>([
  [
    "rows",
    [
      { id: 1, region: "west", amount: 10, label: "  alpha  " },
      { id: 2, region: "west", amount: 6, label: "xxbravoxx" },
      { id: 3, region: "east", amount: 3, label: "charlie" },
      { id: 4, region: null, amount: 8, label: "delta" },
    ],
  ],
  ["dims", [{ region: "west", tag: "W" }]],
]);

function run(sql: string): DatabaseRow[] {
  const plan = compileQuery(sql);
  const vectorized = executeQuery(plan, tables);
  const byRow = executeRowQuery(plan, tables);
  expect(vectorized.rows).toEqual(byRow.rows);
  return vectorized.rows;
}

function value(sql: string): unknown {
  return run(sql)[0]?.v;
}

describe("E021 character strings", () => {
  it("measures length in characters, octets, and the standard spellings", () => {
    expect(value("SELECT CHAR_LENGTH('héllo') AS v")).toBe(5);
    expect(value("SELECT CHARACTER_LENGTH('héllo') AS v")).toBe(5);
    expect(value("SELECT LENGTH('héllo') AS v")).toBe(5);
    // OCTET_LENGTH counts the UTF-8 encoding, where é is two bytes.
    expect(value("SELECT OCTET_LENGTH('héllo') AS v")).toBe(6);
  });

  it("takes substrings by the standard's position window (E021-06)", () => {
    expect(value("SELECT SUBSTRING('abcdef' FROM 2 FOR 3) AS v")).toBe("bcd");
    expect(value("SELECT SUBSTRING('abcdef' FROM 3) AS v")).toBe("cdef");
    expect(value("SELECT SUBSTRING('abcdef', 2, 3) AS v")).toBe("bcd");
    // The window is intersected with the string: a start before 1 shortens the result rather
    // than shifting it, and a window entirely off the string is empty.
    expect(value("SELECT SUBSTRING('abcdef' FROM 0 FOR 3) AS v")).toBe("ab");
    expect(value("SELECT SUBSTRING('abcdef' FROM -2 FOR 3) AS v")).toBe("");
    expect(value("SELECT SUBSTR('abcdef', 10) AS v")).toBe("");
    expect(() => run("SELECT SUBSTR('abcdef', 1, -1) AS v")).toThrow("non-negative integer");
  });

  it("trims with a side, a trim string, and the default space (E021-09, T056)", () => {
    expect(value("SELECT TRIM(label) AS v FROM rows WHERE id = 1")).toBe("alpha");
    expect(value("SELECT TRIM(BOTH FROM label) AS v FROM rows WHERE id = 1")).toBe("alpha");
    expect(value("SELECT TRIM(BOTH 'x' FROM label) AS v FROM rows WHERE id = 2")).toBe("bravo");
    expect(value("SELECT TRIM(LEADING 'x' FROM label) AS v FROM rows WHERE id = 2")).toBe(
      "bravoxx",
    );
    expect(value("SELECT TRIM(TRAILING 'x' FROM label) AS v FROM rows WHERE id = 2")).toBe(
      "xxbravo",
    );
    // T056: a multi-character trim removes the whole string repeatedly, not a set of characters.
    expect(value("SELECT TRIM(LEADING 'ab' FROM 'ababcab') AS v")).toBe("cab");
    expect(value("SELECT TRIM(BOTH 'ab' FROM 'abXab') AS v")).toBe("X");
    expect(value("SELECT LTRIM('xxa', 'x') AS v")).toBe("a");
    expect(() => compileQuery("SELECT TRIM(LEADING 'x') AS v")).toThrow("TRIM LEADING requires");
  });

  it("finds positions and pads to width (E021-11, T055)", () => {
    expect(value("SELECT POSITION('cd' IN 'abcdef') AS v")).toBe(3);
    expect(value("SELECT POSITION('zz' IN 'abcdef') AS v")).toBe(0);
    expect(value("SELECT POSITION('' IN 'abc') AS v")).toBe(1);
    expect(value("SELECT LPAD('ab', 5, '-') AS v")).toBe("---ab");
    expect(value("SELECT RPAD('ab', 5, '-') AS v")).toBe("ab---");
    expect(value("SELECT LPAD('ab', 7, 'xy') AS v")).toBe("xyxyxab");
    // A width below the value's length truncates, and an empty fill cannot pad.
    expect(value("SELECT LPAD('abcdef', 3) AS v")).toBe("abc");
    expect(value("SELECT RPAD('ab', 5, '') AS v")).toBe("ab");
  });

  it("overlays a replacement at a position", () => {
    expect(value("SELECT OVERLAY('abcdef' PLACING 'ZZ' FROM 2) AS v")).toBe("aZZdef");
    expect(value("SELECT OVERLAY('abcdef' PLACING 'ZZ' FROM 2 FOR 4) AS v")).toBe("aZZf");
    expect(value("SELECT OVERLAY('abcdef' PLACING '' FROM 2 FOR 2) AS v")).toBe("adef");
    expect(() => compileQuery("SELECT OVERLAY('abc' PLACING 'z') AS v")).toThrow("Expected FROM");
  });

  it("propagates NULL through every string function", () => {
    for (const call of [
      "CHAR_LENGTH(region)",
      "OCTET_LENGTH(region)",
      "SUBSTRING(region FROM 1 FOR 1)",
      "TRIM(BOTH 'x' FROM region)",
      "LPAD(region, 4, '-')",
      "OVERLAY(region PLACING 'z' FROM 1)",
      "POSITION('a' IN region)",
    ]) {
      expect(value(`SELECT ${call} AS v FROM rows WHERE id = 4`)).toBeNull();
    }
  });
});

describe("E051 basic query specification", () => {
  it("selects one source's columns with a qualified wildcard (E051-07)", () => {
    expect(run("SELECT r.* FROM rows r WHERE id = 3")).toEqual([
      { id: 3, region: "east", amount: 3, label: "charlie" },
    ]);
    // With several sources the outputs are alias-qualified, the same naming a bare * uses.
    expect(
      run("SELECT d.*, r.id FROM rows r JOIN dims d ON d.region = r.region WHERE r.id = 1"),
    ).toEqual([{ "d.region": "west", "d.tag": "W", id: 1 }]);
    expect(run("SELECT s.* FROM (SELECT id, amount FROM rows WHERE id = 1) s")).toEqual([
      { id: 1, amount: 10 },
    ]);
    expect(() => run("SELECT x.* FROM rows r")).toThrow("Unknown table for x.*");
  });

  it("renames a table's columns positionally (E051-09)", () => {
    expect(run("SELECT y.a AS a, y.c AS c FROM rows AS y(a, b, c, d) WHERE a = 3")).toEqual([
      { a: 3, c: 3 },
    ]);
    expect(run("SELECT * FROM rows AS y(a, b, c, d) WHERE a = 3")).toEqual([
      { a: 3, b: "east", c: 3, d: "charlie" },
    ]);
    expect(() => executeRowQuery(compileQuery("SELECT a FROM rows AS y(a, b)"), tables)).toThrow(
      "must match the table's column count",
    );
  });

  it("accepts the ALL set quantifier on aggregates (E091-06)", () => {
    expect(value("SELECT SUM(ALL amount) AS v FROM rows")).toBe(27);
    expect(value("SELECT COUNT(ALL region) AS v FROM rows")).toBe(3);
    expect(value("SELECT COUNT(DISTINCT region) AS v FROM rows")).toBe(2);
  });
});

describe("E071 query expressions", () => {
  it("nests set operations inside derived tables and CTEs (E071-06)", () => {
    expect(
      run("SELECT s.id FROM (SELECT id FROM rows UNION SELECT 99 AS id) s ORDER BY id"),
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 99 }]);
    expect(
      run("WITH s AS (SELECT id FROM rows EXCEPT SELECT 1 AS id) SELECT id FROM s ORDER BY id"),
    ).toEqual([{ id: 2 }, { id: 3 }, { id: 4 }]);
    expect(run("SELECT s.id FROM ((SELECT id FROM rows) INTERSECT (SELECT 3 AS id)) s")).toEqual([
      { id: 3 },
    ]);
  });
});

describe("E161 and T351 comments", () => {
  it("skips simple and bracketed comments without changing the statement", () => {
    const plain = run("SELECT id FROM rows WHERE id = 1");
    expect(run("SELECT id FROM rows WHERE id = 1 -- trailing")).toEqual(plain);
    expect(run("SELECT id FROM rows\n-- a whole line\nWHERE id = 1")).toEqual(plain);
    expect(run("SELECT /* inline */ id FROM rows WHERE id = 1")).toEqual(plain);
    // Markers inside a literal are data, not comments.
    expect(value("SELECT '-- /* not a comment' AS v")).toBe("-- /* not a comment");
    expect(() => compileQuery("SELECT id FROM rows /* open")).toThrow("Unterminated comment");
  });
});

describe("F041-07 comma joins", () => {
  it("reads a comma between table references as a cross join", () => {
    expect(run("SELECT COUNT(*) AS v FROM rows, dims")).toEqual([{ v: 4 }]);
    expect(
      run("SELECT r.id AS id FROM rows r, dims d WHERE d.region = r.region ORDER BY id"),
    ).toEqual([{ id: 1 }, { id: 2 }]);
    expect(run("SELECT COUNT(*) AS v FROM rows a, rows b, dims d")).toEqual([{ v: 16 }]);
  });

  it("keeps multi-way equality graphs correct across a disconnected written order", () => {
    expect(
      run(
        "SELECT a.id AS id FROM rows d, rows b, rows e, rows a, rows c " +
          "WHERE a.id = b.id AND b.id = c.id AND c.id = d.id AND d.id = e.id " +
          "AND e.region = 'east'",
      ),
    ).toEqual([{ id: 3 }]);
  });
});

describe("F401 named-column and natural joins", () => {
  it("joins on the named columns with USING", () => {
    expect(
      run("SELECT r.id AS id, d.tag AS tag FROM rows r JOIN dims d USING (region) ORDER BY id"),
    ).toEqual([
      { id: 1, tag: "W" },
      { id: 2, tag: "W" },
    ]);
    // USING keeps working as an outer join's condition, and names several columns at once.
    expect(
      run("SELECT r.id AS id FROM rows r LEFT JOIN dims d USING (region) ORDER BY id"),
    ).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]);
    expect(() => compileQuery("SELECT id FROM rows r JOIN dims d USING (nope)")).not.toThrow();
  });

  it("joins on every shared column with NATURAL (F401-01)", () => {
    // rows and dims share only `region`.
    expect(run("SELECT r.id AS id FROM rows r NATURAL JOIN dims d ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
    expect(run("SELECT COUNT(*) AS v FROM rows r NATURAL LEFT JOIN dims d")).toEqual([{ v: 4 }]);
    expect(() =>
      compileQuery("SELECT id FROM rows r NATURAL JOIN dims d ON r.id = d.region"),
    ).toThrow("takes no ON or USING");
    expect(() => compileQuery("SELECT id FROM rows r NATURAL RIGHT JOIN dims d")).toThrow(
      "NATURAL RIGHT JOIN is not supported",
    );
  });
});

describe("F866 FETCH FIRST ... WITH TIES", () => {
  it("keeps the rows tying with the last one", () => {
    // amounts are 3, 6, 8, 10 — no ties, so WITH TIES and ONLY agree.
    expect(run("SELECT id, amount FROM rows ORDER BY amount FETCH FIRST 2 ROWS ONLY")).toEqual(
      run("SELECT id, amount FROM rows ORDER BY amount FETCH FIRST 2 ROWS WITH TIES"),
    );
    // Ordering by region ties every western row with the first.
    expect(
      run(
        "SELECT id FROM rows WHERE region IS NOT NULL ORDER BY region DESC FETCH FIRST 1 ROWS WITH TIES",
      ),
    ).toEqual([{ id: 1 }, { id: 2 }]);
    // The tie window starts after OFFSET, like the limit it extends.
    expect(
      run("SELECT id FROM rows ORDER BY id OFFSET 1 ROWS FETCH FIRST 1 ROWS WITH TIES"),
    ).toEqual([{ id: 2 }]);
    expect(() => compileQuery("SELECT id FROM rows FETCH FIRST 1 ROWS WITH TIES")).not.toThrow();
  });
});

describe("T122 WITH in subqueries", () => {
  it("scopes a nested WITH to the query expression that declares it", () => {
    expect(
      run(
        "SELECT s.n AS n FROM (WITH inner_cte AS (SELECT id FROM rows) SELECT id AS n FROM inner_cte) s ORDER BY n",
      ),
    ).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }]);
    // An outer CTE stays visible inside the nested WITH...
    expect(
      run(
        "WITH outer_cte AS (SELECT id FROM rows WHERE id < 3) SELECT s.n AS n FROM (WITH inner_cte AS (SELECT id FROM outer_cte) SELECT id AS n FROM inner_cte) s ORDER BY n",
      ),
    ).toEqual([{ n: 1 }, { n: 2 }]);
    // ...but the inner name does not leak back out.
    expect(() =>
      run("SELECT s.n AS n FROM (WITH c AS (SELECT id FROM rows) SELECT id AS n FROM c) s, c"),
    ).toThrow("Unknown table: c");
  });
});

describe("F051 datetime functions", () => {
  it("reads the clock once per statement (F051-06/07/08)", () => {
    expect(
      value("SELECT CASE WHEN CURRENT_TIMESTAMP = CURRENT_TIMESTAMP THEN 1 ELSE 0 END AS v"),
    ).toBe(1);
    // Every row of one statement sees the same instant, so no row can disagree with another.
    expect(run("SELECT DISTINCT CURRENT_DATE AS v FROM rows").length).toBe(1);
    // Each execution is its own statement and reads the clock again, so these compare one
    // execution's value rather than diffing two.
    const once = (sql: string): unknown => executeQuery(compileQuery(sql), tables).rows[0]?.v;
    const date = once("SELECT CURRENT_DATE AS v");
    expect(date).toBeInstanceOf(Date);
    expect((date as Date).toISOString()).toMatch(/T00:00:00\.000Z$/);
    expect(once("SELECT CURRENT_TIMESTAMP AS v")).toBeInstanceOf(Date);
    expect(once("SELECT LOCALTIMESTAMP AS v")).toBeInstanceOf(Date);
    expect(
      executeRowQuery(compileQuery("SELECT CURRENT_TIMESTAMP AS v"), tables).rows[0]?.v,
    ).toBeInstanceOf(Date);
    // The engine has no TIME type, so LOCALTIME reads as an 'HH:MM:SS' string.
    expect(once("SELECT LOCALTIME AS v")).toMatch(/^\d{2}:\d{2}:\d{2}$/);
    expect(once("SELECT CURRENT_TIME AS v")).toMatch(/^\d{2}:\d{2}:\d{2}$/);
    expect(() => compileQuery("SELECT CURRENT_DATE(3) AS v")).toThrow("Expected )");
  });

  it("never memoizes a result that depends on the clock", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "t",
      uniqueKey: "id",
      columns: [{ name: "id", type: "number" }],
    });
    await database.insertBatch("t", [{ id: 1 }]);
    const first = await database.query("SELECT CURRENT_TIMESTAMP AS v FROM t");
    await new Promise((resolve) => setTimeout(resolve, 2));
    const second = await database.query("SELECT CURRENT_TIMESTAMP AS v FROM t");
    const at = (result: { rows: DatabaseRow[] }): number =>
      (result.rows[0]?.v as Date | undefined)?.getTime() ?? 0;
    expect(at(second)).toBeGreaterThan(at(first));
  });
});

describe("T611/T612 window frames and functions", () => {
  it("reads a value at a position in the frame (T618)", () => {
    expect(run("SELECT id, NTH_VALUE(amount, 2) OVER (ORDER BY id) AS v FROM rows")).toEqual([
      { id: 1, v: null },
      { id: 2, v: 6 },
      { id: 3, v: 6 },
      { id: 4, v: 6 },
    ]);
    // A position past the frame is NULL rather than an error.
    expect(
      run("SELECT id, NTH_VALUE(amount, 9) OVER (ORDER BY id) AS v FROM rows").every(
        (row) => row.v === null,
      ),
    ).toBe(true);
    expect(() =>
      compileQuery("SELECT NTH_VALUE(amount, 0) OVER (ORDER BY id) AS v FROM rows"),
    ).toThrow("positive integer position");
  });

  it("counts peer groups in a GROUPS frame", () => {
    // Ordered by region the peer groups are NULL (id 4), east (id 3), then west (ids 1 and 2),
    // and the frame spans the current group plus the one before it.
    expect(
      run(
        "SELECT id, COUNT(*) OVER (ORDER BY region GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM rows ORDER BY id",
      ),
    ).toEqual([
      { id: 1, v: 3 },
      { id: 2, v: 3 },
      { id: 3, v: 2 },
      { id: 4, v: 1 },
    ]);
    expect(() =>
      compileQuery("SELECT SUM(amount) OVER (GROUPS UNBOUNDED PRECEDING) AS v FROM rows"),
    ).toThrow("GROUPS frames require ORDER BY");
  });

  it("removes the current row, its group, or its ties from a frame", () => {
    const whole = "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING";
    // Two rows share region 'west', so the exclusions differ there.
    expect(
      run(
        `SELECT id, COUNT(*) OVER (ORDER BY region ${whole} EXCLUDE CURRENT ROW) AS v FROM rows ORDER BY id`,
      ),
    ).toEqual([
      { id: 1, v: 3 },
      { id: 2, v: 3 },
      { id: 3, v: 3 },
      { id: 4, v: 3 },
    ]);
    expect(
      run(
        `SELECT id, COUNT(*) OVER (ORDER BY region ${whole} EXCLUDE GROUP) AS v FROM rows ORDER BY id`,
      ),
    ).toEqual([
      { id: 1, v: 2 },
      { id: 2, v: 2 },
      { id: 3, v: 3 },
      { id: 4, v: 3 },
    ]);
    // EXCLUDE TIES drops the peers but keeps the row itself.
    expect(
      run(
        `SELECT id, COUNT(*) OVER (ORDER BY region ${whole} EXCLUDE TIES) AS v FROM rows ORDER BY id`,
      ),
    ).toEqual([
      { id: 1, v: 3 },
      { id: 2, v: 3 },
      { id: 3, v: 4 },
      { id: 4, v: 4 },
    ]);
  });

  it("names a window once and reuses it (T620)", () => {
    expect(
      run(
        "SELECT id, SUM(amount) OVER w AS s, COUNT(*) OVER w AS c FROM rows WINDOW w AS (ORDER BY id)",
      ),
    ).toEqual([
      { id: 1, s: 10, c: 1 },
      { id: 2, s: 16, c: 2 },
      { id: 3, s: 19, c: 3 },
      { id: 4, s: 27, c: 4 },
    ]);
    // A named window carries its partitioning and framing exactly as if written inline.
    expect(
      run(
        "SELECT id, ROW_NUMBER() OVER w AS v FROM rows WINDOW w AS (PARTITION BY region ORDER BY id)",
      ),
    ).toEqual(run("SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS v FROM rows"));
    expect(() =>
      compileQuery("SELECT SUM(amount) OVER q AS v FROM rows WINDOW w AS (ORDER BY id)"),
    ).toThrow("Unknown window name: q");
    expect(() =>
      compileQuery(
        "SELECT SUM(amount) OVER w AS v FROM rows WINDOW w AS (ORDER BY id), w AS (ORDER BY id)",
      ),
    ).toThrow("Duplicate window name: w");
  });
});

describe("set functions beyond the five accumulators", () => {
  it("computes variance and standard deviation from sums", () => {
    // amounts are 10, 6, 3, 8: mean 6.75, squared deviations 10.5625 + 0.5625 + 14.0625 + 1.5625.
    expect(value("SELECT ROUND(VAR_POP(amount), 6) AS v FROM rows")).toBeCloseTo(6.6875, 6);
    expect(value("SELECT ROUND(VAR_SAMP(amount), 6) AS v FROM rows")).toBeCloseTo(8.916667, 5);
    expect(value("SELECT ROUND(STDDEV_POP(amount), 6) AS v FROM rows")).toBeCloseTo(2.58602, 4);
    // Bare VARIANCE and STDDEV are the sample forms, as in PostgreSQL.
    expect(value("SELECT VARIANCE(amount) AS v FROM rows")).toBe(
      value("SELECT VAR_SAMP(amount) AS v FROM rows"),
    );
    expect(value("SELECT STDDEV(amount) AS v FROM rows")).toBe(
      value("SELECT STDDEV_SAMP(amount) AS v FROM rows"),
    );
    // A sample statistic needs two rows; one row divides by zero, which reads as NULL.
    expect(value("SELECT VAR_SAMP(amount) AS v FROM rows WHERE id = 1")).toBeNull();
    expect(value("SELECT VAR_POP(amount) AS v FROM rows WHERE id = 99")).toBeNull();
  });

  it("picks any value of a group (T626) and folds booleans", () => {
    // ANY_VALUE returns one row's value per group; which one is implementation-dependent.
    const picked = run("SELECT region, ANY_VALUE(amount) AS v FROM rows GROUP BY region");
    expect(picked.length).toBe(3);
    for (const row of picked) {
      const members = run(
        row.region === null
          ? "SELECT amount FROM rows WHERE region IS NULL"
          : `SELECT amount FROM rows WHERE region = '${String(row.region)}'`,
      ).map((member) => member.amount);
      expect(members).toContain(row.v);
    }
    expect(value("SELECT EVERY(amount > 1) AS v FROM rows")).toBe(true);
    expect(value("SELECT EVERY(amount > 5) AS v FROM rows")).toBe(false);
    expect(value("SELECT BOOL_OR(amount > 9) AS v FROM rows")).toBe(true);
    expect(value("SELECT BOOL_AND(amount > 9) AS v FROM rows")).toBe(false);
  });
});

describe("T433 GROUPING", () => {
  it("marks the columns a grouping set aggregated away", () => {
    expect(
      run(
        "SELECT region, GROUPING(region) AS g, SUM(amount) AS s FROM rows WHERE region IS NOT NULL GROUP BY ROLLUP(region)",
      ),
    ).toEqual([
      { region: "west", g: 0, s: 16 },
      { region: "east", g: 0, s: 3 },
      { region: null, g: 1, s: 19 },
    ]);
    // Several arguments read as a bitmask, most significant first.
    const sets = run(
      "SELECT GROUPING(region, amount) AS g FROM rows GROUP BY GROUPING SETS ((region, amount), (region), ())",
    );
    expect(new Set(sets.map((row) => row.g))).toEqual(new Set([0, 1, 3]));
    expect(() => compileQuery("SELECT GROUPING(region) AS g FROM rows GROUP BY region")).toThrow(
      "GROUPING requires GROUP BY ROLLUP",
    );
    expect(() =>
      compileQuery("SELECT GROUPING(amount) AS g FROM rows GROUP BY ROLLUP(region)"),
    ).toThrow("must appear in the GROUP BY");
  });
});

describe("F641 row constructors", () => {
  it("compares rows field by field and lexicographically", () => {
    expect(run("SELECT id FROM rows WHERE (id, region) = (1, 'west')")).toEqual([{ id: 1 }]);
    // Inequality is the disjunction of the field inequalities, so a NULL field cannot make the
    // row equal: row 4 differs on id whatever its region is.
    expect(run("SELECT id FROM rows WHERE (region, id) <> ('west', 1) ORDER BY id")).toEqual([
      { id: 2 },
      { id: 3 },
      { id: 4 },
    ]);
    // Lexicographic: the second field only decides when the first ties.
    expect(run("SELECT id FROM rows WHERE (amount, id) > (6, 1) ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
      { id: 4 },
    ]);
    expect(run("SELECT id FROM rows WHERE (amount, id) >= (6, 2) ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
      { id: 4 },
    ]);
    expect(run("SELECT id FROM rows WHERE (amount, id) < (6, 2) ORDER BY id")).toEqual([{ id: 3 }]);
  });

  it("supports IN over rows and the row null predicates", () => {
    expect(
      run("SELECT id FROM rows WHERE (id, region) IN ((1, 'west'), (3, 'east')) ORDER BY id"),
    ).toEqual([{ id: 1 }, { id: 3 }]);
    // Row 4 is kept: its id already differs, so the row is unequal whatever its NULL region is.
    expect(run("SELECT id FROM rows WHERE (id, region) NOT IN ((1, 'west')) ORDER BY id")).toEqual([
      { id: 2 },
      { id: 3 },
      { id: 4 },
    ]);
    // A row IS NULL only when every field is null, and IS NOT NULL only when none is.
    expect(run("SELECT id FROM rows WHERE (region, region) IS NULL")).toEqual([{ id: 4 }]);
    expect(run("SELECT id FROM rows WHERE (id, region) IS NULL")).toEqual([]);
    expect(run("SELECT id FROM rows WHERE (id, region) IS NOT NULL ORDER BY id")).toEqual([
      { id: 1 },
      { id: 2 },
      { id: 3 },
    ]);
  });

  it("rejects rows the standard gives no meaning", () => {
    expect(() => compileQuery("SELECT id FROM rows WHERE (id, region) = (1, 'west', 2)")).toThrow(
      "same number of fields",
    );
    expect(() => compileQuery("SELECT id FROM rows WHERE (id, region) = 1")).toThrow(
      "a row on both sides",
    );
    expect(() => compileQuery("SELECT (1, 2) AS v")).toThrow("only allowed in a comparison");
    expect(() =>
      compileQuery("SELECT id FROM rows WHERE (id, (region, id)) = (1, (2, 3))"),
    ).toThrow("cannot nest");
  });
});

describe("E151 statement transactions", () => {
  async function counted(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute("INSERT INTO t (id, n) VALUES (1, 10)");
    return database;
  }
  const rows = async (database: MinnowDatabase): Promise<DatabaseRow[]> =>
    (await database.query("SELECT id, n FROM t ORDER BY id")).rows;

  it("publishes everything between BEGIN and COMMIT at once", async () => {
    const database = await counted();
    await database.execute("BEGIN");
    await database.execute("INSERT INTO t (id, n) VALUES (2, 20)");
    await database.execute("UPDATE t SET n = 99 WHERE id = 1");
    // Read-your-writes: inside the transaction the statements see each other.
    expect(await rows(database)).toEqual([
      { id: 1, n: 99 },
      { id: 2, n: 20 },
    ]);
    const committed = await database.execute("COMMIT");
    expect(committed).toMatchObject({ kind: "transaction", action: "commit" });
    expect(await rows(database)).toEqual([
      { id: 1, n: 99 },
      { id: 2, n: 20 },
    ]);
  });

  it("discards everything on ROLLBACK", async () => {
    const database = await counted();
    await database.execute("START TRANSACTION");
    await database.execute("INSERT INTO t (id, n) VALUES (3, 30)");
    await database.execute("DELETE FROM t WHERE id = 1");
    expect(await rows(database)).toEqual([{ id: 3, n: 30 }]);
    await database.execute("ROLLBACK");
    expect(await rows(database)).toEqual([{ id: 1, n: 10 }]);
  });

  it("rolls back a transaction left open, rather than holding the scope forever", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { transactionIdleTimeoutMs: 5 });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute("BEGIN");
    await database.execute("INSERT INTO t (id, n) VALUES (1, 10)");
    // Nothing touches it for longer than the idle bound: the scope ends itself.
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect((await database.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 0 }]);
    // And the next BEGIN is allowed, because the abandoned one is no longer open.
    await database.execute("BEGIN");
    await database.execute("ROLLBACK");
  });

  it("rolls back an open transaction and releases resident state when the database closes", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute("INSERT INTO t (id, n) VALUES (0, 5)");
    await database.query("SELECT * FROM t");
    expect(database.bufferPoolStats().usedBytes).toBeGreaterThan(0);
    await database.execute("BEGIN");
    await database.execute("INSERT INTO t (id, n) VALUES (1, 10)");
    const closing = database.close();
    expect(database.close()).toBe(closing);
    await closing;
    expect(database.bufferPoolStats()).toMatchObject({ usedBytes: 0, entries: 0 });

    const reopened = new MinnowDatabase(store);
    expect((await reopened.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 1 }]);
    expect((await store.listTransactions()).every((record) => record.status !== "active")).toBe(
      true,
    );
    await reopened.close();
  });

  it("stays open while its statements are slow, however long they take", async () => {
    /** A store whose reads are slow, so one statement outlives several idle sweeps. */
    class SlowStore extends MemoryBlockStore {
      override async getBlocks(ids: readonly string[]): ReturnType<MemoryBlockStore["getBlocks"]> {
        await new Promise((resolve) => setTimeout(resolve, 12));
        return super.getBlocks(ids);
      }
    }
    const database = new MinnowDatabase(new SlowStore(), { transactionIdleTimeoutMs: 5 });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute("INSERT INTO t (id, n) VALUES (1, 1)");
    await database.execute("BEGIN");
    // Every statement here runs longer than the whole idle bound. Idle means nothing running,
    // not time passing, so the scope has to survive all of them.
    for (let index = 0; index < 5; index += 1) {
      await database.execute(`UPDATE t SET n = ${String(index)} WHERE id = 1`);
    }
    await expect(database.execute("COMMIT")).resolves.toMatchObject({ action: "commit" });
    expect((await database.query("SELECT n FROM t")).rows).toEqual([{ n: 4 }]);
  });

  it("refuses what it cannot take back, and the spellings it has no meaning for", async () => {
    const database = await counted();
    await expect(database.execute("COMMIT")).rejects.toThrow("COMMIT without an open transaction");
    await expect(database.execute("ROLLBACK")).rejects.toThrow(
      "ROLLBACK without an open transaction",
    );
    await database.execute("BEGIN");
    await expect(database.execute("BEGIN")).rejects.toThrow("A transaction is already open");
    // The catalog commits outside the scope, so a rollback could not take a DDL statement back.
    await expect(database.execute("CREATE TABLE other (a INTEGER)")).rejects.toThrow(
      "CREATE TABLE is not allowed inside a transaction",
    );
    await database.execute("ROLLBACK");
    // The standard's optional noise parses; an isolation level does not, since there is one.
    await database.execute("BEGIN WORK");
    await database.execute("COMMIT TRANSACTION");
    await expect(
      database.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
    ).rejects.toThrow();
  });
});

describe("F312 MERGE", () => {
  async function stocked(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE stock (sku INTEGER PRIMARY KEY, qty INTEGER NOT NULL, note TEXT)",
    );
    await database.execute("CREATE TABLE delivery (sku INTEGER PRIMARY KEY, qty INTEGER NOT NULL)");
    await database.execute("INSERT INTO stock (sku, qty, note) VALUES (1, 10, 'a'), (2, 5, 'b')");
    await database.execute("INSERT INTO delivery (sku, qty) VALUES (1, 3), (3, 7)");
    return database;
  }

  it("updates what matched and inserts what did not", async () => {
    const database = await stocked();
    const result = await database.execute(
      `MERGE INTO stock s USING delivery d ON s.sku = d.sku
       WHEN MATCHED THEN UPDATE SET qty = s.qty + d.qty
       WHEN NOT MATCHED THEN INSERT (sku, qty, note) VALUES (d.sku, d.qty, 'new')`,
    );
    expect(result).toMatchObject({ kind: "merge", table: "stock", rowCount: 2 });
    expect((await database.query("SELECT * FROM stock ORDER BY sku")).rows).toEqual([
      { sku: 1, qty: 13, note: "a" },
      { sku: 2, qty: 5, note: "b" },
      { sku: 3, qty: 7, note: "new" },
    ]);
  });

  it("tries the branches in order, with their own conditions", async () => {
    const database = await stocked();
    await database.execute(
      `MERGE INTO stock s USING delivery d ON s.sku = d.sku
       WHEN MATCHED AND s.qty >= 10 THEN DELETE
       WHEN MATCHED THEN UPDATE SET note = 'kept'
       WHEN NOT MATCHED AND d.qty > 5 THEN INSERT (sku, qty, note) VALUES (d.sku, d.qty, 'big')`,
    );
    // sku 1 matched and had qty 10, so the first branch claimed it; sku 3 was not matched and
    // its quantity passed the branch condition; sku 2 was in neither source row.
    expect((await database.query("SELECT * FROM stock ORDER BY sku")).rows).toEqual([
      { sku: 2, qty: 5, note: "b" },
      { sku: 3, qty: 7, note: "big" },
    ]);
  });

  it("takes a query as its source", async () => {
    const database = await stocked();
    await database.execute(
      `MERGE INTO stock s USING (SELECT sku, qty * 2 AS doubled FROM delivery WHERE qty > 5) d
       ON s.sku = d.sku
       WHEN NOT MATCHED THEN INSERT (sku, qty, note) VALUES (d.sku, d.doubled, 'sub')`,
    );
    expect((await database.query("SELECT * FROM stock WHERE sku = 3")).rows).toEqual([
      { sku: 3, qty: 14, note: "sub" },
    ]);
  });

  it("fires the triggers the equivalent statements would", async () => {
    const database = await stocked();
    await database.execute("CREATE TABLE audit (action TEXT, sku INTEGER)");
    await database.execute(
      "CREATE TRIGGER stock_ins AFTER INSERT ON stock BEGIN INSERT INTO audit (action, sku) VALUES ('ins', NEW.sku); END",
    );
    await database.execute(
      "CREATE TRIGGER stock_upd AFTER UPDATE ON stock BEGIN INSERT INTO audit (action, sku) VALUES ('upd', NEW.sku); END",
    );
    await database.execute(
      `MERGE INTO stock s USING delivery d ON s.sku = d.sku
       WHEN MATCHED THEN UPDATE SET qty = 1
       WHEN NOT MATCHED THEN INSERT (sku, qty, note) VALUES (d.sku, d.qty, 'new')`,
    );
    expect(
      (await database.query("SELECT action, sku FROM audit ORDER BY action, sku")).rows,
    ).toEqual([
      { action: "ins", sku: 3 },
      { action: "upd", sku: 1 },
    ]);
  });

  it("refuses two source rows for one target row", async () => {
    const database = await stocked();
    // SQL:2023 15.8 makes this a cardinality violation. Applying the last one silently would
    // make the result depend on the order the source happened to produce.
    await expect(
      database.execute(
        `MERGE INTO stock s USING (SELECT 1 AS sku, 5 AS qty UNION ALL SELECT 1 AS sku, 7 AS qty) d
         ON s.sku = d.sku WHEN MATCHED THEN UPDATE SET qty = d.qty`,
      ),
    ).rejects.toThrow("matched stock row 1 from more than one source row");
    // And nothing was written: the statement is one scope.
    expect((await database.query("SELECT qty FROM stock WHERE sku = 1")).rows).toEqual([
      { qty: 10 },
    ]);
  });

  it("says what it cannot address", async () => {
    const database = await stocked();
    // The unique key is how the keyed write paths address rows, so the match has to name it.
    await expect(
      database.execute(
        "MERGE INTO stock s USING delivery d ON s.qty = d.qty WHEN MATCHED THEN DELETE",
      ),
    ).rejects.toThrow("MERGE ON must equate s.sku with a source value");
    await expect(
      database.execute(
        "MERGE INTO stock s USING delivery d ON s.sku = d.sku WHEN MATCHED THEN UPDATE SET sku = 5",
      ),
    ).rejects.toThrow("MERGE cannot update the unique key: sku");
    await expect(
      database.execute("MERGE INTO stock s USING delivery d ON s.sku = d.sku"),
    ).rejects.toThrow("MERGE needs at least one WHEN clause");
    await expect(
      database.execute(
        "MERGE INTO stock s USING delivery d ON s.sku = d.sku WHEN NOT MATCHED BY SOURCE THEN DELETE",
      ),
    ).rejects.toThrow("MATCHED BY SOURCE and BY TARGET are not supported");
  });
});

describe("F031 views", () => {
  async function seeded(): Promise<MinnowDatabase> {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute(
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, region TEXT NOT NULL, total INTEGER NOT NULL)",
    );
    await database.execute(
      "INSERT INTO orders (id, region, total) VALUES (1, 'west', 10), (2, 'east', 5), (3, 'west', 20)",
    );
    return database;
  }

  it("answers by name, and reads through to the tables behind it (F031-02)", async () => {
    const database = await seeded();
    await database.execute(
      "CREATE VIEW big AS SELECT id, region, total FROM orders WHERE total >= 10",
    );
    expect((await database.query("SELECT * FROM big ORDER BY id")).rows).toEqual([
      { id: 1, region: "west", total: 10 },
      { id: 3, region: "west", total: 20 },
    ]);
    // A view is a source like any other: aliased, grouped, joined, counted.
    expect(
      (await database.query("SELECT b.region AS r, SUM(b.total) AS s FROM big b GROUP BY b.region"))
        .rows,
    ).toEqual([{ r: "west", s: 30 }]);
    expect(
      (
        await database.query(
          "SELECT o.id AS id FROM orders o JOIN big b ON b.id = o.id ORDER BY id",
        )
      ).rows,
    ).toEqual([{ id: 1 }, { id: 3 }]);
    // It reflects the table underneath rather than a copy taken at definition time.
    await database.execute("INSERT INTO orders (id, region, total) VALUES (4, 'east', 40)");
    expect((await database.query("SELECT COUNT(*) AS n FROM big")).rows).toEqual([{ n: 3 }]);
    // And the catalog lists it with the columns a reader can select.
    const listed = (await database.listTables()).find((table) => table.name === "big");
    expect(listed?.columns.map((column) => column.name)).toEqual(["id", "region", "total"]);
  });

  it("stacks on another view and can be replaced", async () => {
    const database = await seeded();
    await database.execute(
      "CREATE VIEW big AS SELECT id, region, total FROM orders WHERE total >= 10",
    );
    await database.execute(
      "CREATE VIEW west_big AS SELECT id, total FROM big WHERE region = 'west'",
    );
    expect((await database.query("SELECT * FROM west_big ORDER BY id")).rows).toEqual([
      { id: 1, total: 10 },
      { id: 3, total: 20 },
    ]);
    await expect(database.execute("CREATE VIEW big AS SELECT id FROM orders")).rejects.toThrow(
      "View already exists: big",
    );
    await database.execute(
      "CREATE OR REPLACE VIEW big AS SELECT id, region, total FROM orders WHERE total >= 20",
    );
    // The stacked view follows the redefinition, because it stores the query rather than a plan.
    expect((await database.query("SELECT * FROM west_big")).rows).toEqual([{ id: 3, total: 20 }]);
  });

  it("is not a table, and says so", async () => {
    const database = await seeded();
    await database.execute(
      "CREATE VIEW big AS SELECT id, region, total FROM orders WHERE total >= 10",
    );
    await expect(database.execute("INSERT INTO big (id) VALUES (9)")).rejects.toThrow(
      "big is a view, not a table",
    );
    await expect(database.execute("DELETE FROM big WHERE id = 1")).rejects.toThrow(
      "big is a view, not a table",
    );
    await expect(database.execute("DROP TABLE big")).rejects.toThrow(
      "big is a view; use DROP VIEW",
    );
    await expect(database.execute("DROP VIEW orders")).rejects.toThrow("Not a view: orders");
    // A body that cannot be typed fails where it is written.
    await expect(database.execute("CREATE VIEW bad AS SELECT nope FROM orders")).rejects.toThrow(
      "Ambiguous or missing column: nope",
    );
  });

  it("refuses to drop a table a view reads", async () => {
    const database = await seeded();
    await database.execute("CREATE VIEW big AS SELECT id, total FROM orders WHERE total >= 10");
    // A view left pointing at a table that is gone would fail on its next read, which is the
    // same reason a trigger writing to the table blocks the drop.
    await expect(database.execute("DROP TABLE orders")).rejects.toThrow(
      "Cannot drop orders: view big reads it",
    );
    await database.execute("DROP VIEW big");
    await expect(database.execute("DROP TABLE orders")).resolves.toMatchObject({ dropped: true });
  });

  it("catches a cycle between views rather than recursing", async () => {
    const database = await seeded();
    await database.execute("CREATE VIEW a AS SELECT id, total FROM orders");
    await database.execute("CREATE VIEW b AS SELECT id, total FROM a");
    // Neither definition is recursive on its own; redefining one over the other closes a loop.
    await database.execute("CREATE OR REPLACE VIEW a AS SELECT id, total FROM b");
    await expect(database.query("SELECT * FROM a")).rejects.toThrow(
      "A view cannot be defined in terms of itself",
    );
    // And the loop is recoverable: redefining either side makes both readable again.
    await database.execute("CREATE OR REPLACE VIEW a AS SELECT id, total FROM orders");
    expect((await database.query("SELECT COUNT(*) AS n FROM b")).rows).toEqual([{ n: 3 }]);
  });

  it("drops, freeing the name", async () => {
    const database = await seeded();
    await database.execute("CREATE VIEW big AS SELECT id, total FROM orders WHERE total >= 10");
    await expect(database.execute("DROP VIEW big")).resolves.toEqual({
      kind: "drop-view",
      view: "big",
      dropped: true,
    });
    await expect(database.query("SELECT * FROM big")).rejects.toThrow("Table not found: big");
    await expect(database.execute("DROP VIEW big")).rejects.toThrow("View not found: big");
    await expect(database.execute("DROP VIEW IF EXISTS big")).resolves.toMatchObject({
      dropped: false,
    });
    // The name is free for a table now.
    await database.execute("CREATE TABLE big (a INTEGER)");
    expect((await database.query("SELECT COUNT(*) AS n FROM big")).rows).toEqual([{ n: 0 }]);
  });
});

describe("catalog-dependent rewrites", () => {
  /** A store that counts how often the read path asks for the whole catalog. */
  class CountingStore extends MemoryBlockStore {
    listings = 0;
    override async listTables(): ReturnType<MemoryBlockStore["listTables"]> {
      this.listings += 1;
      return super.listTables();
    }
  }

  it("does not read the catalog per query", async () => {
    // Resolving a view name is the one thing a read cannot do from the statement alone, and it
    // used to cost a full catalog listing on every query — which made an unrelated point lookup
    // slower as the schema grew. The view set is cached against the catalog epoch instead.
    const store = new CountingStore();
    const database = new MinnowDatabase(store);
    for (let index = 0; index < 8; index += 1) {
      await database.execute(
        `CREATE TABLE t${String(index)} (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)`,
      );
    }
    await database.execute("INSERT INTO t0 (id, n) VALUES (1, 1)");
    await database.query("SELECT n FROM t0 WHERE id = 1", { memoize: false });
    const before = store.listings;
    for (let index = 0; index < 20; index += 1) {
      await database.query("SELECT n FROM t0 WHERE id = 1", { memoize: false });
    }
    expect(store.listings - before).toBe(0);
    // A catalog change invalidates the cache, so a view created now is still found.
    await database.execute("CREATE VIEW v AS SELECT n FROM t0");
    expect((await database.query("SELECT n FROM v")).rows).toEqual([{ n: 1 }]);
  });

  it("resolves views wherever a read runs, including inside a write scope", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute("INSERT INTO t (id, n) VALUES (1, 10), (2, 20)");
    await database.execute("CREATE VIEW v AS SELECT id, n FROM t WHERE n >= 20");
    await database.write(async (session) => {
      expect((await session.query("SELECT id FROM v")).rows).toEqual([{ id: 2 }]);
      // And it sees what the scope has staged, like any other read in one.
      await session.insertBatch("t", { columns: { id: [3], n: [30] } });
      expect((await session.query("SELECT COUNT(*) AS n FROM v")).rows).toEqual([{ n: 2 }]);
    });
    await database.execute("BEGIN");
    expect((await database.query("SELECT COUNT(*) AS n FROM v")).rows).toEqual([{ n: 2 }]);
    await database.execute("ROLLBACK");
  });

  it("renames the columns of a view through an alias list", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute("INSERT INTO t (id, n) VALUES (1, 10)");
    await database.execute("CREATE VIEW v AS SELECT id, n FROM t");
    // The view is a derived source by the time the alias list applies, so the rename has to wrap
    // it rather than look its columns up by name.
    expect((await database.query("SELECT x.a AS a, x.b AS b FROM v AS x(a, b)")).rows).toEqual([
      { a: 1, b: 10 },
    ]);
  });
});

describe("SQL/JSON (T811, T812, T821-T823, T825)", () => {
  const documents = new Map<string, DatabaseRow[]>([
    [
      "docs",
      [
        {
          id: 1,
          doc: JSON.stringify({
            name: "ada",
            tags: ["x", "y"],
            meta: { score: 9 },
            flag: true,
            none: null,
          }),
          plain: "not json",
        },
      ],
    ],
  ]);
  const json = (sql: string): unknown => {
    const plan = compileQuery(sql);
    const vectorized = executeQuery(plan, documents);
    expect(vectorized.rows).toEqual(executeRowQuery(plan, documents).rows);
    return vectorized.rows[0]?.v;
  };

  it("reads scalars along a path (T822)", () => {
    expect(json("SELECT JSON_VALUE(doc, '$.name') AS v FROM docs")).toBe("ada");
    expect(json("SELECT JSON_VALUE(doc, '$.meta.score') AS v FROM docs")).toBe("9");
    expect(json("SELECT JSON_VALUE(doc, '$.tags[1]') AS v FROM docs")).toBe("y");
    expect(json("SELECT JSON_VALUE(doc, '$[\"name\"]') AS v FROM docs")).toBe("ada");
    // A path that selects nothing, a JSON null, or a whole object has no scalar value.
    expect(json("SELECT JSON_VALUE(doc, '$.missing') AS v FROM docs")).toBeNull();
    expect(json("SELECT JSON_VALUE(doc, '$.none') AS v FROM docs")).toBeNull();
    expect(json("SELECT JSON_VALUE(doc, '$.meta') AS v FROM docs")).toBeNull();
    // A document that is not JSON selects nothing rather than failing the statement.
    expect(json("SELECT JSON_VALUE(plain, '$.a') AS v FROM docs")).toBeNull();
    // Values arrive as text, so a numeric use casts, as the standard's RETURNING would.
    expect(json("SELECT CAST(JSON_VALUE(doc, '$.meta.score') AS INTEGER) + 1 AS v FROM docs")).toBe(
      10,
    );
  });

  it("returns JSON text for objects and arrays (T823) and tests existence (T821)", () => {
    expect(json("SELECT JSON_QUERY(doc, '$.meta') AS v FROM docs")).toBe('{"score":9}');
    expect(json("SELECT JSON_QUERY(doc, '$.tags') AS v FROM docs")).toBe('["x","y"]');
    expect(json("SELECT JSON_QUERY(doc, '$.nope') AS v FROM docs")).toBeNull();
    expect(json("SELECT JSON_EXISTS(doc, '$.meta.score') AS v FROM docs")).toBe(true);
    expect(json("SELECT JSON_EXISTS(doc, '$.nope') AS v FROM docs")).toBe(false);
    expect(json("SELECT JSON_EXISTS(doc, '$.tags[5]') AS v FROM docs")).toBe(false);
  });

  it("tests a value's JSON shape (T825)", () => {
    const ids = (sql: string): DatabaseRow[] => executeQuery(compileQuery(sql), documents).rows;
    expect(ids("SELECT id FROM docs WHERE doc IS JSON")).toEqual([{ id: 1 }]);
    expect(ids("SELECT id FROM docs WHERE plain IS JSON")).toEqual([]);
    expect(ids("SELECT id FROM docs WHERE plain IS NOT JSON")).toEqual([{ id: 1 }]);
    expect(ids("SELECT id FROM docs WHERE doc IS JSON OBJECT")).toEqual([{ id: 1 }]);
    expect(ids("SELECT id FROM docs WHERE doc IS JSON ARRAY")).toEqual([]);
    expect(ids("SELECT id FROM docs WHERE JSON_QUERY(doc, '$.tags') IS JSON ARRAY")).toEqual([
      { id: 1 },
    ]);
  });

  it("constructs objects and arrays (T811, T812)", () => {
    expect(json("SELECT JSON_OBJECT('a' VALUE 1, 'b' VALUE 'two') AS v")).toBe('{"a":1,"b":"two"}');
    expect(json("SELECT JSON_OBJECT(KEY 'a' VALUE 1) AS v")).toBe('{"a":1}');
    expect(json("SELECT JSON_OBJECT('a', 1) AS v")).toBe('{"a":1}');
    // With no null-handling clause, SQL's default is NULL ON NULL for both constructors.
    expect(json("SELECT JSON_OBJECT('a', 1, 'b', NULL) AS v")).toBe('{"a":1,"b":null}');
    // WITHOUT UNIQUE KEYS is the default, so the JSON text retains duplicate names. Keys are
    // scalar expressions converted to text; special JavaScript property names stay ordinary.
    expect(json("SELECT JSON_OBJECT('a', 1, 'a', 2) AS v")).toBe('{"a":1,"a":2}');
    expect(json("SELECT JSON_OBJECT(7, 'seven', '__proto__', TRUE) AS v")).toBe(
      '{"7":"seven","__proto__":true}',
    );
    expect(() => run("SELECT JSON_OBJECT(NULL, 1) AS v")).toThrow("keys cannot be NULL");
    expect(json("SELECT JSON_ARRAY(1, 'two', TRUE, NULL) AS v")).toBe('[1,"two",true,null]');
    expect(json("SELECT JSON_ARRAY() AS v")).toBe("[]");
    expect(json("SELECT JSON_OBJECT('n' VALUE JSON_VALUE(doc, '$.name')) AS v FROM docs")).toBe(
      '{"n":"ada"}',
    );
  });

  it("aggregates values into JSON arrays with SQL null semantics (T826)", () => {
    expect(value("SELECT JSON_ARRAYAGG(region) AS v FROM rows")).toBe(
      '["west","west","east",null]',
    );
    expect(value("SELECT JSON_ARRAYAGG(DISTINCT region) AS v FROM rows")).toBe(
      '["west","east",null]',
    );
    expect(value("SELECT JSON_ARRAYAGG(region) AS v FROM rows WHERE amount < 0")).toBeNull();
    expect(
      run(
        "SELECT region, JSON_ARRAYAGG(amount) AS amounts FROM rows GROUP BY region ORDER BY region",
      ),
    ).toEqual([
      { region: null, amounts: "[8]" },
      { region: "east", amounts: "[3]" },
      { region: "west", amounts: "[10,6]" },
    ]);
  });

  it("rejects JSON_ARRAYAGG extensions that are not implemented", () => {
    expect(() => compileQuery("SELECT JSON_ARRAYAGG(*) FROM rows")).toThrow(
      "requires a scalar value expression",
    );
    expect(() =>
      compileQuery("SELECT JSON_ARRAYAGG(region) FILTER (WHERE amount > 5) FROM rows"),
    ).toThrow("filter rows in WHERE");
    expect(() => compileQuery("SELECT JSON_ARRAYAGG(region) OVER () FROM rows")).toThrow(
      "window use is not supported",
    );
  });

  it("rejects paths and arities it cannot answer", () => {
    // A constant path is checked once at compile time rather than per row.
    expect(() => compileQuery("SELECT JSON_VALUE(doc, 'meta') AS v FROM docs")).toThrow(
      "paths start at $",
    );
    expect(() => compileQuery("SELECT JSON_VALUE(doc, '$.a[') AS v FROM docs")).toThrow(
      "unclosed subscript",
    );
    // The multi-value forms need a row-producing operator this engine does not have.
    expect(() => compileQuery("SELECT JSON_QUERY(doc, '$.tags[*]') AS v FROM docs")).toThrow(
      "array indexes and member names",
    );
    expect(() => compileQuery("SELECT JSON_VALUE(doc) AS v FROM docs")).toThrow(
      "requires a JSON document and a path",
    );
    expect(() => compileQuery("SELECT IS_JSON(doc) AS v FROM docs")).toThrow(
      "Use the IS JSON predicate",
    );
  });
});

describe("E141/F031 schema statements", () => {
  async function fresh(): Promise<MinnowDatabase> {
    return new MinnowDatabase(new MemoryBlockStore());
  }

  it("declares keys, defaults, and IF NOT EXISTS", async () => {
    const database = await fresh();
    await database.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier TEXT DEFAULT 'basic', made TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
    );
    await database.execute("INSERT INTO t (id, name) VALUES (1, 'ada')");
    const rows = await database.query("SELECT id, name, tier, made IS NOT NULL AS stamped FROM t");
    expect(rows.rows).toEqual([{ id: 1, name: "ada", tier: "basic", stamped: true }]);
    // E141-08: the table-level spelling of the same single-column key.
    await database.execute("CREATE TABLE u (a INTEGER, b TEXT, PRIMARY KEY (a))");
    await expect(
      database.execute("INSERT INTO u (a, b) VALUES (1, 'x'), (1, 'y')"),
    ).rejects.toThrow();
    // A second CREATE TABLE fails, and IF NOT EXISTS makes it a no-op instead.
    await expect(database.execute("CREATE TABLE t (id INTEGER)")).rejects.toThrow("already exists");
    await expect(database.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER)")).resolves.toEqual({
      kind: "create-table",
      table: "t",
    });
  });

  it("adds a column to an existing table (F031-04)", async () => {
    const database = await fresh();
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    await database.execute("INSERT INTO t (id) VALUES (1)");
    await database.execute("ALTER TABLE t ADD COLUMN score INTEGER");
    // Rows written before the column exists read it as NULL.
    expect((await database.query("SELECT id, score FROM t")).rows).toEqual([
      { id: 1, score: null },
    ]);
    await database.execute("INSERT INTO t (id, score) VALUES (2, 7)");
    expect((await database.query("SELECT score FROM t WHERE id = 2")).rows).toEqual([{ score: 7 }]);
    await expect(database.execute("ALTER TABLE t ADD COLUMN score TEXT")).rejects.toThrow(
      "Column already exists",
    );
    // Existing rows have no value for a new column, so it cannot be NOT NULL.
    await expect(
      database.execute("ALTER TABLE t ADD COLUMN flag BOOLEAN NOT NULL"),
    ).rejects.toThrow("adds a nullable column");
  });

  it("creates a table from a query (T172)", async () => {
    const database = await fresh();
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, region TEXT NOT NULL)");
    await database.execute("INSERT INTO t (id, region) VALUES (1, 'west'), (2, 'east')");
    await database.execute("CREATE TABLE copy AS SELECT id, region FROM t WHERE id = 1");
    expect((await database.query("SELECT * FROM copy")).rows).toEqual([{ id: 1, region: "west" }]);
    await expect(database.execute("CREATE TABLE copy AS SELECT id FROM t")).rejects.toThrow(
      "already exists",
    );
  });

  it("enforces CHECK constraints on every write path (E141-06)", async () => {
    const database = await fresh();
    await database.execute(
      "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL CHECK (qty > 0), note TEXT, CONSTRAINT short_note CHECK (LENGTH(note) < 5))",
    );
    await database.execute("INSERT INTO items (id, qty, note) VALUES (1, 5, 'abc')");
    // A column constraint and a named table constraint both refuse the row.
    await expect(
      database.execute("INSERT INTO items (id, qty, note) VALUES (2, 0, 'abc')"),
    ).rejects.toThrow("CHECK items_qty_check failed for row 0 of items");
    await expect(
      database.execute("INSERT INTO items (id, qty, note) VALUES (3, 5, 'toolong')"),
    ).rejects.toThrow("CHECK short_note failed");
    // Three-valued logic: an unknown passes, which is why NULL satisfies LENGTH(note) < 5.
    await database.execute("INSERT INTO items (id, qty, note) VALUES (4, 5, NULL)");
    expect((await database.query("SELECT id FROM items ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 4 },
    ]);
    // An update is checked against the post-image, so an untouched column still counts.
    await expect(database.execute("UPDATE items SET qty = -1 WHERE id = 1")).rejects.toThrow(
      "CHECK items_qty_check failed",
    );
    await database.execute("UPDATE items SET qty = 9 WHERE id = 1");
    // And so is the update half of an upsert, which runs inside a write scope.
    await expect(
      database.execute(
        "INSERT INTO items (id, qty, note) VALUES (1, -7, 'x') ON CONFLICT (id) DO UPDATE SET qty = EXCLUDED.qty",
      ),
    ).rejects.toThrow("CHECK items_qty_check failed");
    expect((await database.query("SELECT qty FROM items WHERE id = 1")).rows).toEqual([{ qty: 9 }]);
  });

  it("refuses a constraint it could never evaluate", async () => {
    const database = await fresh();
    await expect(database.execute("CREATE TABLE c (a INTEGER CHECK (b > 0))")).rejects.toThrow(
      "refers to a column this table has no: b",
    );
    await expect(database.execute("CREATE TABLE c (a INTEGER CHECK (SUM(a) > 0))")).rejects.toThrow(
      "takes a row condition, not an aggregate",
    );
    // A reference to a table that is not there is refused where it is written.
    await expect(
      database.execute("CREATE TABLE c (a INTEGER, b INTEGER REFERENCES other(id))"),
    ).rejects.toThrow("references a table that does not exist: other");
  });

  it("enforces foreign keys on both sides (E141-04)", async () => {
    const database = await fresh();
    await database.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    await database.execute(
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer INTEGER NOT NULL REFERENCES customers(id), total INTEGER NOT NULL)",
    );
    await database.execute(
      "CREATE TABLE notes (id INTEGER PRIMARY KEY, customer INTEGER REFERENCES customers(id) ON DELETE SET NULL, body TEXT NOT NULL)",
    );
    await database.execute(
      "CREATE TABLE logs (id INTEGER PRIMARY KEY, customer INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE)",
    );
    await database.execute("INSERT INTO customers (id, name) VALUES (1, 'ada'), (2, 'grace')");

    // Child side: a value must name a row that exists, on insert and on update alike.
    await database.execute("INSERT INTO orders (id, customer, total) VALUES (10, 1, 5)");
    await expect(
      database.execute("INSERT INTO orders (id, customer, total) VALUES (11, 99, 5)"),
    ).rejects.toThrow("has no customers row with id 99");
    await expect(database.execute("UPDATE orders SET customer = 98 WHERE id = 10")).rejects.toThrow(
      "has no customers row with id 98",
    );
    // A NULL reference names no parent, which the standard leaves satisfied.
    await database.execute("INSERT INTO notes (id, customer, body) VALUES (20, NULL, 'none')");

    // Parent side: the default refuses while a child still points at the row.
    await expect(database.execute("DELETE FROM customers WHERE id = 1")).rejects.toThrow(
      "still has 1 orders row(s) referencing customers",
    );
    // With the referencing order gone, the other two actions run: SET NULL clears, CASCADE deletes.
    await database.execute("INSERT INTO notes (id, customer, body) VALUES (21, 1, 'about ada')");
    await database.execute("INSERT INTO logs (id, customer) VALUES (30, 1)");
    await database.execute("DELETE FROM orders WHERE id = 10");
    await database.execute("DELETE FROM customers WHERE id = 1");
    expect((await database.query("SELECT id, customer FROM notes ORDER BY id")).rows).toEqual([
      { id: 20, customer: null },
      { id: 21, customer: null },
    ]);
    expect((await database.query("SELECT COUNT(*) AS n FROM logs")).rows).toEqual([{ n: 0 }]);
    // And the table cannot be dropped while something references it.
    // Whichever key the catalog reports first, the drop names it and refuses.
    await expect(database.execute("DROP TABLE customers")).rejects.toThrow(
      /Cannot drop customers: foreign key \w+ on \w+ references it/,
    );
  });

  it("refuses references it could not keep", async () => {
    const database = await fresh();
    await database.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    // The reference has to be to the parent's unique key: that is the only column the engine can
    // probe for existence and the only one its keyed writes address rows by.
    await expect(
      database.execute("CREATE TABLE c (a INTEGER REFERENCES customers(name))"),
    ).rejects.toThrow("must reference customers's unique key id");
    await expect(
      database.execute("CREATE TABLE c (a TEXT REFERENCES customers(id))"),
    ).rejects.toThrow("compares string with number");
    await expect(
      database.execute(
        "CREATE TABLE c (a INTEGER NOT NULL REFERENCES customers(id) ON DELETE SET NULL)",
      ),
    ).rejects.toThrow("cannot SET NULL a NOT NULL column");
    // A unique key never changes here, so an ON UPDATE action would have nothing to act on.
    await expect(
      database.execute("CREATE TABLE c (a INTEGER REFERENCES customers(id) ON UPDATE CASCADE)"),
    ).rejects.toThrow("has nothing to act on");
  });

  it("checks a foreign key against the parent's key index, not its rows", async () => {
    // The child-side probe asks the parent's persistent unique-key lookup, so its cost does not
    // grow with the parent. Counting block reads is what makes that observable: a probe that
    // scans the parent has to read the parent's blocks, and this insert reads none.
    class CountingStore extends MemoryBlockStore {
      blockReads = 0;

      override async getBlock(id: string): Promise<Uint8Array | undefined> {
        this.blockReads += 1;
        return super.getBlock(id);
      }

      override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
        this.blockReads += ids.length;
        return super.getBlocks(ids);
      }
    }
    const store = new CountingStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 8, compression: "raw" });
    await database.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
    await database.execute(
      "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer INTEGER NOT NULL REFERENCES customers(id))",
    );
    await database.insertBatch("customers", {
      columns: {
        id: Array.from({ length: 200 }, (_, index) => index + 1),
        name: Array.from({ length: 200 }, (_, index) => `c${String(index + 1)}`),
      },
    });

    store.blockReads = 0;
    await database.execute("INSERT INTO orders (id, customer) VALUES (1, 137)");
    expect(store.blockReads).toBe(0);

    // The rejection still names the missing parent, and still costs no parent scan to find.
    store.blockReads = 0;
    await expect(
      database.execute("INSERT INTO orders (id, customer) VALUES (2, 999)"),
    ).rejects.toThrow("has no customers row with id 999");

    // Read-your-writes inside a scope is unchanged: a parent staged in the same scope satisfies
    // the reference, and one the scope deleted stops satisfying it.
    await database.write(async (tx) => {
      await tx.insertBatch("customers", { columns: { id: [201], name: ["fresh"] } });
      await tx.insertBatch("orders", { columns: { id: [3], customer: [201] } });
    });
    expect((await database.query("SELECT COUNT(*) AS n FROM orders")).rows).toEqual([{ n: 2 }]);
    await expect(
      database.write(async (tx) => {
        await tx.deleteBatch("customers", { keys: [201] });
        await tx.insertBatch("orders", { columns: { id: [4], customer: [201] } });
      }),
    ).rejects.toThrow(/FOREIGN KEY/);
    expect(
      (await database.query("SELECT COUNT(*) AS n FROM customers WHERE id = 201")).rows,
    ).toEqual([{ n: 1 }]);
  });

  it("keeps a cascade atomic with the delete that caused it", async () => {
    const database = await fresh();
    await database.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    await database.execute(
      "CREATE TABLE child (id INTEGER PRIMARY KEY, p INTEGER NOT NULL REFERENCES parent(id) ON DELETE CASCADE, note TEXT NOT NULL CHECK (LENGTH(note) < 4))",
    );
    await database.execute("INSERT INTO parent (id, n) VALUES (1, 1)");
    await database.execute("INSERT INTO child (id, p, note) VALUES (10, 1, 'ok')");
    await database.execute("DELETE FROM parent WHERE id = 1");
    expect((await database.query("SELECT COUNT(*) AS n FROM child")).rows).toEqual([{ n: 0 }]);
    expect((await database.query("SELECT COUNT(*) AS n FROM parent")).rows).toEqual([{ n: 0 }]);
  });
});

describe("T661 and T662 numeric literals", () => {
  it("reads non-decimal integers and digit separators", () => {
    expect(value("SELECT 0x1F AS v")).toBe(31);
    expect(value("SELECT 0X1f AS v")).toBe(31);
    expect(value("SELECT 0o17 AS v")).toBe(15);
    expect(value("SELECT 0b1011 AS v")).toBe(11);
    expect(value("SELECT 1_000_000 AS v")).toBe(1_000_000);
    expect(value("SELECT 1_0.5_5 AS v")).toBe(10.55);
    expect(value("SELECT 0xFF_FF AS v")).toBe(65_535);
    for (const bad of ["0xZZ", "1__0", "1_", "0b2", "0o8"]) {
      expect(() => compileQuery(`SELECT ${bad} AS v`)).toThrow();
    }
  });

  it("reads unary plus as the identity", () => {
    expect(value("SELECT +5 AS v")).toBe(5);
    expect(value("SELECT 3 - +1 AS v")).toBe(2);
    expect(value("SELECT +amount AS v FROM rows WHERE id = 3")).toBe(3);
  });
});
