/**
 * The client's statement classifier (owner/sequence.ts `classifyStatementText`) against its
 * authority, the front end itself (engine/query.ts `compileStatement`), on generated spellings:
 * every transaction-control form the front end accepts, savepoints, reads and writes whose
 * literals hide `;` and keywords, in random letter case, with quoted keywords, comments (including
 * a bracketed comment that opens another, which the front end does not nest), and trailing or
 * extra statements.
 *
 * Law: when the front end compiles the text, the classifier gives the same kind (its transaction
 * action, or `other`). Text with a statement separator besides one trailing `;` is `unknown`
 * (the reconnect ruling: multi-statement text is never trusted). Text the front end refuses
 * runs nothing, so any class is safe there; the suite still requires the classifier not to throw.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { compileStatement } from "../../engine/query.js";
import { STATEMENT_KIND_STATES, createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { classifyStatementText, type StatementKind } from "../owner/sequence.js";

const SEEDS = seedsFor("next-owner-statement-kind", [0x5e55]);

/** A statement as words (`null` marks where a separator goes) and whether it is a control form. */
const STATEMENTS: ReadonlyArray<readonly string[]> = [
  ["BEGIN"],
  ["BEGIN", "WORK"],
  ["BEGIN", "TRANSACTION"],
  ["BEGIN", "READ", "ONLY"],
  ["BEGIN", "TRANSACTION", "READ", "WRITE"],
  ["BEGIN", "ISOLATION", "LEVEL", "READ", "COMMITTED"],
  ["BEGIN", "NOT", "DEFERRABLE"],
  ["START", "TRANSACTION"],
  ["START", "TRANSACTION", "READ", "ONLY"],
  ["START"],
  ["COMMIT"],
  ["COMMIT", "WORK"],
  ["COMMIT", "TRANSACTION"],
  ["END"],
  ["END", "TRANSACTION"],
  ["ROLLBACK"],
  ["ROLLBACK", "WORK"],
  ["ABORT"],
  ["ABORT", "TRANSACTION"],
  ["SAVEPOINT", "a"],
  ["RELEASE", "a"],
  ["RELEASE", "SAVEPOINT", "a"],
  ["ROLLBACK", "TO", "a"],
  ["ROLLBACK", "TO", "SAVEPOINT", "a"],
  ["SELECT", "1"],
  ["SELECT", "'x; COMMIT'"],
  ["SELECT", "'it''s; BEGIN'"],
  ["SELECT", "E'a\\'; COMMIT'"],
  ["SELECT", "$$; BEGIN $$"],
  ["SELECT", "$q$ ; END $q$"],
  ["SELECT", '"commit"', "FROM", "t"],
  ["UPDATE", "t", "SET", "n", "=", "1"],
  ["DELETE", "FROM", "t"],
  ["SHOW", "timezone"],
];

const SEPARATORS = [
  " ",
  "\n",
  "\t ",
  " /* note */ ",
  "/*;*/",
  " /* COMMIT; BEGIN */ ",
  " -- BEGIN; COMMIT\n",
  " /* a /* b */ ",
] as const;

const KEYWORDS = new Set([
  "BEGIN",
  "START",
  "COMMIT",
  "END",
  "ROLLBACK",
  "ABORT",
  "SAVEPOINT",
  "RELEASE",
  "TO",
  "WORK",
  "TRANSACTION",
]);

/** A word in random case; a keyword sometimes as a quoted identifier, which the front end routes too. */
const spelled = (word: string): fc.Arbitrary<string> =>
  fc
    .tuple(fc.array(fc.boolean(), { minLength: word.length, maxLength: word.length }), fc.boolean())
    .map(([upper, quoted]) => {
      if (!/^[A-Za-z]+$/u.test(word)) return word;
      // ASCII words only (checked above), so code units are characters.
      const cased = word
        .split("")
        .map((ch, i) => (upper[i] ? ch.toUpperCase() : ch.toLowerCase()))
        .join("");
      return quoted && KEYWORDS.has(word) ? `"${cased}"` : cased;
    });

const statement = fc.constantFrom(...STATEMENTS).chain((words) =>
  fc
    .tuple(
      fc.tuple(...words.map(spelled)),
      fc.array(fc.constantFrom(...SEPARATORS), {
        minLength: words.length + 1,
        maxLength: words.length + 1,
      }),
    )
    .map(
      ([parts, separators]) =>
        parts.map((part, i) => `${separators[i] ?? " "}${part}`).join("") +
        (separators.at(-1) ?? ""),
    ),
);

const text = fc
  .tuple(
    statement,
    fc.constantFrom("", ";", " ; ", ";\n"),
    fc.oneof({ weight: 3, arbitrary: fc.constant(null) }, { weight: 1, arbitrary: statement }),
  )
  .map(([first, end, second]) => ({
    text: second === null ? `${first}${end}` : `${first}; ${second}${end}`,
    multi: second !== null,
  }));

/** The front end's answer: the kind it routes the text to, or `error` when it refuses it. */
function authority(sql: string): StatementKind | "error" {
  let compiled: ReturnType<typeof compileStatement>;
  try {
    compiled = compileStatement(sql);
  } catch {
    return "error";
  }
  if (compiled.kind !== "transaction") return "other";
  switch (compiled.action) {
    case "begin":
    case "commit":
    case "rollback":
      return compiled.action;
    case "savepoint":
    case "release":
    case "rollback-to":
      return "savepoint";
  }
}

describe("classifyStatementText against the front end", () => {
  it.each(SEEDS)("agrees with compileStatement on generated spellings (seed %i)", (seed) => {
    const reach = createReach("next-owner-statement-kind", STATEMENT_KIND_STATES, {
      minimum: 50,
    });
    fc.assert(
      fc.property(text, ({ text: sql, multi }) => {
        reach.beginHistory();
        const kind = classifyStatementText(sql);
        if (multi) {
          reach.reach("multi-statement");
          expect(kind, sql).toBe("unknown");
          return;
        }
        const expected = authority(sql);
        if (expected === "error") {
          reach.reach("refused by the front end");
          return;
        }
        reach.reach(`compiled ${expected === "unknown" ? "other" : expected}`);
        expect(kind, sql).toBe(expected);
      }),
      { seed, numRuns: 3_000 },
    );
    reach.assertReached();
  });

  it("pins the spellings the reconnect review named", () => {
    const cases: ReadonlyArray<readonly [string, StatementKind]> = [
      ["BEGIN", "begin"],
      ["  begin work ;", "begin"],
      ["/* x */ Start Transaction Read Only", "begin"],
      ['-- lead\n"BEGIN"', "begin"],
      ["/* a /* b */ COMMIT", "commit"],
      ["end", "commit"],
      ["Abort", "rollback"],
      ["ROLLBACK TO SAVEPOINT a", "savepoint"],
      ["SELECT 'x; COMMIT'", "other"],
      ["UPDATE t SET n = 1; COMMIT", "unknown"],
      ["BEGIN; UPDATE t SET n = 1", "unknown"],
      ["SELECT 'unterminated", "unknown"],
      ["/* unterminated BEGIN", "unknown"],
    ];
    for (const [sql, kind] of cases) expect(classifyStatementText(sql), sql).toBe(kind);
  });
});
