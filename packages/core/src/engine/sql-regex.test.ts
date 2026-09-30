import { beforeAll, afterAll, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { compileRegexPattern } from "./sql-semantics.js";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";

let postgres: PGlite;
let db: MinnowDatabase;
beforeAll(async () => {
  postgres = new PGlite();
  await postgres.waitReady;
  db = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false, autoCollect: false });
}, 30_000);
afterAll(async () => {
  await db.close();
  await postgres.close();
});

it.each([
  "SELECT SUBSTRING('abc' FROM 'a|ab') AS v",
  "SELECT REGEXP_REPLACE('abc', 'a|ab', '_') AS v",
  "SELECT SUBSTRING('ab' FROM '(a|ab)(b?)') AS v",
  "SELECT SUBSTRING('abcabc' FROM '(ab)+') AS v",
  "SELECT 'A19!' ~ '^[[:alpha:]][[:digit:]]+[[:punct:]]$' AS v",
  "SELECT 'abc' ~ '^a{1,3}b.c$' AS v",
  "SELECT 'aab' ~ '(a+)+$' AS v",
  "SELECT REGEXP_REPLACE('a1b2', '[[:digit:]]', '#', 'g') AS v",
  "SELECT REGEXP_REPLACE('ab', '(a)(b)', '\\2\\1') AS v",
  "SELECT REGEXP_REPLACE('ab', '(a)', '\\&$') AS v",
  "SELECT REGEXP_REPLACE('aaa', 'a*', '_', 'g') AS v",
  "SELECT REGEXP_REPLACE('ab', '', '_', 'g') AS v",
  "SELECT SUBSTRING('abc' FROM '(a*)*') AS v",
  "SELECT 'ABC' ~* '^abc$' AS v",
])("matches PostgreSQL for %s", async (sql) => {
  expect((await db.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
});

it("bounds pathological failing matches, including nested repetition and ambiguous alternation", () => {
  for (const pattern of ["(a+)+$", "(a|aa)+$", "a*a*a*a*a*b", "(a*)*$"]) {
    const expression = compileRegexPattern(pattern);
    try {
      expect(expression.test("a".repeat(1000) + "!")).toBe(false);
    } catch (error) {
      expect(error).toBeInstanceOf(RangeError);
    }
  }
});

it.each(["(a)\\1", "(?=a)a", "(?<=a)b", "a+?", "a{1001}", "(", "[[:bogus:]]"])(
  "explicitly refuses unsupported or invalid syntax %s",
  (pattern) => {
    expect(() => compileRegexPattern(pattern)).toThrow();
  },
);

it("keeps flags and cached matchers independent across successive calls", () => {
  expect(compileRegexPattern("a", "ic").test("A")).toBe(false);
  expect(compileRegexPattern("a", "ci").test("A")).toBe(true);
  const expression = compileRegexPattern("a+", "g");
  expect(expression.exec("aab")?.[0]).toBe("aa");
  expect(expression.exec("aab")?.[0]).toBe("aa");
});

it("matches PostgreSQL across generated ambiguous patterns and small inputs", async () => {
  // A Cartesian corpus exercises alternation priority, captures, optional groups, and zero-width
  // cycles independently of the implementation, using the PostgreSQL engine as the oracle.
  const patterns = [
    "a|ab",
    "(a|ab)",
    "(a*)(a*)",
    "(a|b)*",
    "(ab|a)+",
    "(a*)*",
    "(a)?b",
    "(a+)(b?)",
    "(a|aa)(a?)",
    "[ab]{0,3}",
    "^(a|b)+$",
    "[^b]*",
    "(a*)(b*)(a*)",
  ];
  const inputs = ["", "a", "b", "ab", "aa", "aaa", "aba", "aaba", "babb", "!ab!"];
  for (const pattern of patterns)
    for (const input of inputs) {
      const sql = `SELECT SUBSTRING('${input}' FROM '${pattern}') AS v`;
      expect((await db.query(sql)).rows, `${pattern} on ${input}`).toEqual(
        (await postgres.query(sql)).rows,
      );
    }
});
it.each([
  "SELECT REGEXP_REPLACE('a' || CHR(10) || 'b', '[^b]+', '_', 'n') AS v",
  "SELECT REGEXP_REPLACE('a' || CHR(10) || 'b', '^b$', '_', 'n') AS v",
  "SELECT REGEXP_REPLACE('a' || CHR(10) || 'b', '.', '_', 'g') AS v",
  "SELECT REGEXP_REPLACE('a1_!', '[[:alnum:]_]+', '_') AS v",
  "SELECT SUBSTRING('ab' FROM '(z)?(ab)') AS v",
])("matches PostgreSQL newline/class/capture semantics for %s", async (sql) => {
  expect((await db.query(sql)).rows).toEqual((await postgres.query(sql)).rows);
});
