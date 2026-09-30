import { afterAll, beforeAll, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";

let postgres: PGlite;
let database: MinnowDatabase;
beforeAll(async () => {
  postgres = new PGlite();
  await postgres.waitReady;
  await postgres.exec("SET TIME ZONE 'UTC'");
  database = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false, autoCollect: false });
}, 30_000);
afterAll(async () => {
  await database.close();
  await postgres.close();
});

it.each([
  "SELECT DIV(9007199254740993::NUMERIC, 1) AS v",
  "SELECT DIV(9007199254740993.0001::NUMERIC, 1::NUMERIC) AS v",
  "SELECT DIV(-123.99::NUMERIC, 0.1::NUMERIC) AS v",
  "SELECT DIV(9007199254740993::NUMERIC, 0.0000000000000001::NUMERIC) AS v",
  "SELECT TO_CHAR(9007199254740993.0001::NUMERIC, 'FM9999999999999999.0000') AS v",
  "SELECT TO_CHAR(-1.25::NUMERIC, 'FM9.9') AS v",
  "SELECT QUOTE_LITERAL(DATE '2026-01-01') AS v",
  "SELECT QUOTE_LITERAL(9007199254740993::NUMERIC) AS v",
  "SELECT QUOTE_LITERAL(TRUE) AS v",
  "SELECT QUOTE_LITERAL('1.20'::NUMERIC(10,2)) AS v",
  "SELECT TO_CHAR(TO_DATE('0001-01-01','YYYY-MM-DD'),'YYYY-MM-DD') AS v",
  "SELECT TO_CHAR(TO_TIMESTAMP('0099-02-28 12:00:00','YYYY-MM-DD HH24:MI:SS'),'YYYY-MM-DD HH24:MI:SS') AS v",
  "SELECT QUOTE_IDENT('select') AS v",
  "SELECT QUOTE_IDENT('between') AS v",
  "SELECT QUOTE_IDENT('customer_id') AS v",
  "SELECT FORMAT('%2$s%s', 'a', 'b', 'c') AS v",
  "SELECT FORMAT('%3$s %1$s %s', 'a', 'b', 'c') AS v",
  "SELECT FORMAT('%I %L %% %s', 'select', DATE '2026-01-01', NULL) AS v",
  "SELECT TO_CHAR(MAKE_TIMESTAMP(1,1,1,0,0,0), 'YYYY-MM-DD HH24:MI:SS') AS v",
  "SELECT TO_CHAR(MAKE_TIMESTAMP(99,2,28,23,59,59.125), 'YYYY-MM-DD HH24:MI:SS.MS') AS v",
  "SELECT TO_CHAR('2000-02-29 12:00:00+05:30'::TIMESTAMPTZ, 'YYYY-MM-DD HH24:MI:SS') AS v",
])("matches PostgreSQL for %s", async (sql) => {
  expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
});

it.each([
  "SELECT FORMAT('%q', 'a')",
  "SELECT FORMAT('%0$s', 'a')",
  "SELECT FORMAT('%', 'a')",
  "SELECT FORMAT('%2$s%s', 'a', 'b')",
  "SELECT DIV(1::NUMERIC, 0::NUMERIC)",
  "SELECT MAKE_TIMESTAMP(2026,1,1,0,61,0)",
  "SELECT MAKE_TIMESTAMP(2026,1,1,0,0,61)",
  "SELECT MAKE_TIMESTAMP(2026,2,30,0,0,0)",
  "SELECT TIMESTAMP '2026-02-30 12:00:00'",
  "SELECT '2026-02-30'::TIMESTAMP",
  "SELECT TIMESTAMP '2026-01-01 24:30:00'",
  "SELECT '1'::TEXT = 1",
  "SELECT 1 = CAST('1' AS TEXT)",
  "SELECT 't'::TEXT = TRUE",
])("refuses invalid or incompatible inputs like PostgreSQL: %s", async (sql) => {
  await expect(postgres.query(sql)).rejects.toBeInstanceOf(Error);
  await expect(database.query(sql)).rejects.toBeInstanceOf(Error);
  // A failed scalar cannot poison the following statement.
  expect((await database.query("SELECT 1 AS usable")).rows).toEqual([{ usable: 1 }]);
});

it("preserves exact scalar semantics after catalog binding and vector execution", async () => {
  const setup = [
    "CREATE TABLE exact_values (id INTEGER PRIMARY KEY, n NUMERIC(35,16))",
    "INSERT INTO exact_values VALUES (1,9007199254740993.0001),(2,-1.25)",
  ];
  for (const sql of setup) {
    await database.execute(sql);
    await postgres.exec(sql);
  }
  const sql = `SELECT id, DIV(n, 1) AS q, TO_CHAR(n, 'FM9999999999999999.0000') AS formatted,
    QUOTE_LITERAL(n) AS quoted FROM exact_values ORDER BY id`;
  // QUOTE_LITERAL reflects the declared scale. Keep it under the same type inference as DIV.
  const expected = (await postgres.query(sql)).rows;
  expect((await database.query(sql, { memoize: false })).rows).toEqual(expected);
});

it("preserves TEXT typing through catalog binding, predicates, and expressions", async () => {
  for (const sql of [
    "CREATE TABLE typed_text (id INTEGER PRIMARY KEY, s TEXT)",
    "INSERT INTO typed_text VALUES (1,'1'),(2,'2')",
  ]) {
    await database.execute(sql);
    await postgres.exec(sql);
  }
  for (const sql of [
    "SELECT s = 1 FROM typed_text",
    "SELECT id FROM typed_text WHERE s = 1",
    "SELECT COALESCE(s,'0') = 1 FROM typed_text",
    "SELECT UPPER(s) = 1 FROM typed_text",
  ]) {
    await expect(postgres.query(sql)).rejects.toBeInstanceOf(Error);
    await expect(database.query(sql, { memoize: false })).rejects.toBeInstanceOf(Error);
  }
  for (const sql of [
    "SELECT id FROM typed_text WHERE s = '1'",
    "SELECT s::INTEGER AS n FROM typed_text ORDER BY id",
  ]) {
    expect((await database.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
  }
});

it("keeps identifier quoting aligned with the PostgreSQL keyword catalog", async () => {
  const { rows } = await postgres.query<{ word: string; quoted: string }>(
    "SELECT word, quote_ident(word) AS quoted FROM pg_get_keywords()",
  );
  expect(rows.length).toBeGreaterThan(400);
  for (const { word, quoted } of rows) {
    expect((await database.query(`SELECT QUOTE_IDENT('${word}') AS quoted`)).rows).toEqual([
      { quoted },
    ]);
  }
});
