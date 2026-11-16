import { beforeAll, afterAll, expect, it, vi } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import { compileRegexPattern } from "./sql-semantics.js";
import { MinnowDatabase } from "./database.js";
import { SqlRegex } from "./sql-regex.js";
import { MAX_SQL_PATTERN_MATCH_STEPS } from "./cache-limits.js";
import {
  createReach,
  SQL_REGEX_MATCH_MODES_REACH,
  SQL_REGEX_WORK_REACH,
  SQL_REGEX_FLAGS_REACH,
  SQL_REGEX_ACCEPTANCE_REACH,
} from "../testing/reach.js";
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

/** Fail a regressed interpreter by counted work before the unchanged runner timeout. */
function boundedTest(expression: SqlRegex, input: string, bound: number, label: string): boolean {
  const spend = expression.spend.bind(expression);
  let steps = 0;
  const observer = vi.spyOn(SqlRegex.prototype, "spend").mockImplementation((work) => {
    if (++steps > bound) expect(steps, label).toBeLessThanOrEqual(bound);
    spend(work);
  });
  try {
    const result = expression.test(input);
    expect(steps, label).toBeGreaterThan(0);
    expect(steps, label).toBeLessThanOrEqual(bound);
    return result;
  } finally {
    observer.mockRestore();
  }
}

it("bounds pathological failing matches, including nested repetition and ambiguous alternation", async () => {
  const input = "a".repeat(1000) + "!";
  // Pattern-cache accounting retains 96 bytes per compiled instruction.
  // The fixture's complete state-position space fits the existing memo ceiling: no eviction.
  // Each state has at most two outgoing edges plus one new search start per input position.
  const reach = createReach("SQL regex bounded matches", SQL_REGEX_WORK_REACH);
  const cases = [
    ["(a+)+$", "nested repetition"],
    ["(a|aa)+$", "ambiguous alternation"],
    ["a*a*a*a*a*b", "repeated wildcards"],
    ["(a*)*$", "nullable end match"],
    ["^(a*)*$", "anchored empty cycles"],
  ] as const;
  for (const [pattern, state] of cases) {
    reach.beginHistory();
    const expression = compileRegexPattern(pattern);
    // The original nullable pattern matches empty at the end. PostgreSQL is the authority
    // for every result; the anchored additional case must reject the trailing exclamation.
    const expected = (
      await postgres.query<{ matched: boolean }>("SELECT $1::text ~ $2::text AS matched", [
        input,
        pattern,
      ])
    ).rows[0]?.matched;
    const bound = (2 * (expression.retainedSize / 96) + 1) * (input.length + 1);
    expect((expression.retainedSize / 96) * (input.length + 1)).toBeLessThanOrEqual(65_536);
    expect(boundedTest(expression, input, bound, pattern)).toBe(expected);
    reach.reach(state);
  }
  reach.assertReached();
});

it("keeps long literal successes and failed-start cache rollover within linear work", async () => {
  const literal = "a".repeat(4096);
  const cases = [
    [literal, literal, "long literal match"],
    [literal + "|b", literal, "long literal alternative"],
    ["b", "a".repeat(65_536) + "b", "failed-start cache rollover"],
  ] as const;
  const reach = createReach("SQL regex accepted searches", SQL_REGEX_ACCEPTANCE_REACH);
  for (const [pattern, input, state] of cases) {
    reach.beginHistory();
    const expression = compileRegexPattern(pattern);
    const expected = (
      await postgres.query<{ matched: boolean }>("SELECT $1::text ~ $2::text AS matched", [
        input,
        pattern,
      ])
    ).rows[0]?.matched;
    expect(expected).toBe(true);
    const bound = 2 * (input.length + expression.retainedSize / 96 + 1);
    expect(boundedTest(expression, input, bound, state)).toBe(expected);
    reach.reach(state);
  }
  reach.assertReached();
});

it("retains the complete fixed pattern work ceiling", () => {
  const work = { remaining: MAX_SQL_PATTERN_MATCH_STEPS };
  const expression = compileRegexPattern("");
  for (let step = 0; step < MAX_SQL_PATTERN_MATCH_STEPS; step += 1) expression.spend(work);
  expect(() => expression.spend(work)).toThrow("SQL pattern match exceeds its work limit");
});

it.each(["(a)\\1", "(?=a)a", "(?<=a)b", "a+?", "a{1001}", "(", "[[:bogus:]]"])(
  "explicitly refuses unsupported or invalid syntax %s",
  (pattern) => {
    expect(() => compileRegexPattern(pattern)).toThrow();
  },
);

it("keeps flags and cached matchers independent across successive calls", async () => {
  expect(compileRegexPattern("a", "ic").test("A")).toBe(false);
  expect(compileRegexPattern("a", "ci").test("A")).toBe(true);
  const expression = compileRegexPattern("a+", "g");
  expect(expression.exec("aab")?.[0]).toBe("aa");
  expect(expression.exec("aab")?.[0]).toBe("aa");
  const reach = createReach("SQL regex flags", SQL_REGEX_FLAGS_REACH);
  const cases = [
    ["n", "^b$", "a\nb", "case-sensitive mode"],
    ["n", "^a$", "a\nb", "case-sensitive mode"],
    ["n", ".", "\n", "case-sensitive mode"],
    ["", "^b$", "a\nb", "case-sensitive mode"],
    ["i", "^[ab]+$", "AB", "case-insensitive mode"],
    ["ic", "a", "A", "case-sensitive mode"],
    ["ci", "a", "A", "case-insensitive mode"],
    ["in", "^a$", "\nA\n", "case-insensitive mode"],
  ] as const;
  for (const [flags, pattern, input, mode] of cases) {
    reach.beginHistory();
    const expected = (
      await postgres.query<{ matched: boolean }>(
        "SELECT regexp_match($1, $2, $3) IS NOT NULL AS matched",
        [input, pattern, flags],
      )
    ).rows[0]?.matched;
    expect(compileRegexPattern(pattern, flags).test(input), `${pattern}/${flags}`).toBe(expected);
    reach.reach(mode);
    if (flags.includes("n")) reach.reach("newline mode");
  }
  reach.assertReached();
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
    "",
    ".",
    "^$",
    "^a$",
    "a$",
    "^a",
    "[[:alpha:]]",
    "😀",
    "[😀a]+",
    "a.b",
  ];
  const inputs = [
    "",
    "a",
    "b",
    "ab",
    "aa",
    "aaa",
    "aba",
    "aaba",
    "babb",
    "!ab!",
    "\n",
    "a\nb",
    "😀",
    "😀a",
    "a😀b",
    "aa\n",
  ];
  const reach = createReach("SQL regex match modes", SQL_REGEX_MATCH_MODES_REACH);
  for (const pattern of patterns)
    for (const input of inputs) {
      reach.beginHistory();
      if (input.includes("😀")) reach.reach("astral input");
      if (input.includes("\n")) reach.reach("newline input");
      if (pattern.includes("^")) reach.reach("start anchor");
      if (pattern.includes("$")) reach.reach("end anchor");
      const sql = `SELECT SUBSTRING('${input}' FROM '${pattern}') AS v, '${input}' ~ '${pattern}' AS matched`;
      const actual = (await db.query(sql)).rows;
      expect(actual, `${pattern} on ${input}`).toEqual((await postgres.query(sql)).rows);
      reach.reach("captured substring");
      reach.reach(actual[0]?.matched === true ? "boolean match" : "boolean rejection");
      if (actual[0]?.v === "" && actual[0].matched === true) reach.reach("zero-width match");
    }
  reach.assertReached();
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
