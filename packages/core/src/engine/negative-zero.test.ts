import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";

/**
 * PostgreSQL has `%`, `MOD`, and `DIV` only for integers and NUMERIC, neither of which has a
 * negative zero, so `-4 % 4` is 0; its float8 `SIGN(-0)` is 0 too. JavaScript computes -0 for
 * each, and a -0 that reaches a result disagrees with a 0 computed elsewhere: `Object.is` and
 * every structural comparison tell them apart, so a live group keyed by `id % 4` held 0 after a
 * patch and -0 when the query ran fresh (live-differential seed 213708905).
 */
const negativeZeros = (rows: ReadonlyArray<Record<string, unknown>>): string[] =>
  rows.flatMap((row, index) =>
    Object.entries(row)
      .filter(([, value]) => Object.is(value, -0))
      .map(([name]) => `row ${String(index)} ${name}`),
  );

async function open(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  await database.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, n NUMERIC, d DOUBLE PRECISION, g TEXT)",
  );
  await database.execute(
    "INSERT INTO t VALUES (-4, -4, -4, -0.4, 'a'), (0, 0, 0, 0, 'a'), (4, 4, 4, 0.4, 'b')",
  );
  return database;
}

describe("negative zero", () => {
  it.each([
    ["a projection", "SELECT id % 4 AS r, MOD(i, 4) AS m FROM t"],
    ["a filtered projection", "SELECT i % 2 AS r FROM t WHERE i < 0"],
    ["a constant", "SELECT -4 % 4 AS r, MOD(-4, 2) AS m, DIV(-1, 4) AS d, SIGN(-0.0) AS s"],
    ["a parameter", "SELECT i % ? AS r, MOD(i, ?) AS m FROM t", [4, 2]],
    ["a correlated subquery", "SELECT (SELECT u.i % 4 FROM t u WHERE u.id = t.id) AS r FROM t"],
    ["a group key", "SELECT g, id % 4 AS r, COUNT(*) AS n FROM t GROUP BY g, id % 4 ORDER BY g, r"],
    ["an aggregate argument", "SELECT MIN(i % 4) AS lo, MAX(i % 4) AS hi FROM t"],
    ["DIV and SIGN", "SELECT DIV(i, 8) AS q, SIGN(d * 0) AS s, n % 2 AS r FROM t"],
  ] as const)("is never a remainder, quotient, or sign in %s", async (_shape, sql, params?) => {
    const database = await open();
    const result = await database.query(sql, params === undefined ? {} : { params: [...params] });
    expect(result.rows.length).toBeGreaterThan(0);
    expect(negativeZeros(result.rows)).toEqual([]);
    await database.close();
  });

  it("is never stored in an integer column", async () => {
    const database = await open();
    await database.execute("INSERT INTO t VALUES (10, -0, 1, 1, 'c')");
    await database.execute("INSERT INTO t VALUES (11, ?, 1, 1, 'c')", [-0]);
    await database.execute("UPDATE t SET i = ? WHERE id = 4", [-0]);
    await database.insert("t", { id: 12, i: -0, n: 1, d: 1, g: "c" });
    const result = await database.query("SELECT id, i FROM t WHERE i = 0 ORDER BY id");
    expect(result.rows.map((row) => row.id)).toEqual([0, 4, 10, 11, 12]);
    expect(negativeZeros(result.rows)).toEqual([]);
    await database.close();
  });

  it("is kept by double precision, as PostgreSQL's float8 keeps it", async () => {
    const database = await open();
    await database.execute("INSERT INTO t VALUES (13, 1, 1, CAST('-0' AS DOUBLE PRECISION), 'c')");
    const stored = await database.query("SELECT d FROM t WHERE id = 13");
    expect(Object.is(stored.rows[0]?.d, -0)).toBe(true);
    // float8 negation of zero is -0, which TO_CHAR shows as a sign.
    const negated = await database.query("SELECT -d AS m FROM t WHERE id = 0");
    expect(Object.is(negated.rows[0]?.m, -0)).toBe(true);
    await database.close();
  });
});
