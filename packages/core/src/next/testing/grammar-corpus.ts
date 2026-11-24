/**
 * The SQL corpus of the snapshot differential (snapshot-corpus-pglite.test.ts): the repository's
 * real statements, and seeded mutations of them that land near the grammar's edges.
 *
 * Sources: the committed SQLLogicTest profile, the SQL feature matrix's examples, every `sql` block
 * of the documentation, the console's sample queries, and the interaction simulator's statements
 * (recorded by the test from a run). Mutations: token deletion (one to three tokens), truncation,
 * a duplicated token, a keyword swapped for another of its kind, an emptied argument list, a type
 * modifier on a type name, and a subquery replaced by another query form. A mutation edits the
 * original text in place, so the spacing between the tokens it keeps is the original's: adjacent
 * forms (`~<~`, `a[1:2]`, `:=`, `a$b`, `1_000.`) survive.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import fc from "fast-check";
import { parseSqlLogicTest } from "../../testing/sqllogictest.js";

const CORE = fileURLToPath(new URL("../../../", import.meta.url));
const REPO = fileURLToPath(new URL("../../../../../", import.meta.url));

/** A source's statements, deduplicated in order. */
export interface CorpusSource {
  readonly name: string;
  readonly statements: readonly string[];
}

/**
 * Splits text into statements at top-level semicolons (scanned as PostgreSQL's scanner reads them:
 * quoted strings with their escapes, dollar quotes, nested comments), keeping each statement's
 * text as written.
 */
export function splitStatements(text: string): string[] {
  const out: string[] = [];
  let start = 0;
  for (const token of scan(text)) {
    if (token.text !== ";" || openTriggerBody(text.slice(start, token.start))) continue;
    out.push(text.slice(start, token.start));
    start = token.end;
  }
  out.push(text.slice(start));
  return out.map((statement) => statement.trim()).filter((statement) => statement !== "");
}

/** Inside next/'s `CREATE TRIGGER ... BEGIN ... END` body, whose statements end in semicolons. */
function openTriggerBody(statement: string): boolean {
  return (
    /^\s*CREATE\s+TRIGGER\b[\s\S]*\bBEGIN\b/iu.test(statement) && !/\bEND\s*$/iu.test(statement)
  );
}

function unique(statements: Iterable<string>): string[] {
  return [...new Set(statements)];
}

function files(root: string, suffix: string): string[] {
  return readdirSync(root, { recursive: true, encoding: "utf8" })
    .filter((name) => name.endsWith(suffix))
    .sort()
    .map((name) => join(root, name));
}

/** The SQLLogicTest profile's statements and queries. */
function sqlLogicTest(): CorpusSource {
  const statements: string[] = [];
  for (const path of files(join(CORE, "testdata", "sqllogictest"), ".test"))
    for (const record of parseSqlLogicTest(readFileSync(path, "utf8"), path))
      if (record.kind === "statement" || record.kind === "query") statements.push(record.sql);
  return { name: "SQLLogicTest profile", statements: unique(statements) };
}

/** The SQL feature matrix's examples. */
function featureMatrix(): CorpusSource {
  const matrix = JSON.parse(readFileSync(join(CORE, "sql-feature-matrix.json"), "utf8")) as {
    features: Array<{ example?: string }>;
  };
  return {
    name: "SQL feature matrix",
    statements: unique(matrix.features.flatMap((f) => splitStatements(f.example ?? ""))),
  };
}

/** Every ```sql block of the documentation. */
function documentation(): CorpusSource {
  const statements: string[] = [];
  for (const path of files(join(REPO, "apps", "site", "content", "docs"), ".mdx")) {
    const text = readFileSync(path, "utf8");
    for (const block of text.matchAll(/```sql[^\n]*\n([\s\S]*?)```/gu))
      for (const statement of splitStatements(block[1] ?? "")) statements.push(statement);
  }
  return { name: "documentation", statements: unique(statements) };
}

/** The console's sample queries (apps/site/components/playground/queries.ts). */
function consoleQueries(): CorpusSource {
  const text = readFileSync(
    join(REPO, "apps", "site", "components", "playground", "queries.ts"),
    "utf8",
  );
  const statements = [...text.matchAll(/\bsql:\s*`([\s\S]*?)`/gu)].flatMap((m) =>
    splitStatements(m[1] ?? ""),
  );
  return { name: "console queries", statements: unique(statements) };
}

/**
 * PostgreSQL's own forms of statements next/ reads differently or not at all, as its documentation
 * writes them: without them no mutation reaches the trigger, view and query forms only PostgreSQL
 * has. Which of their mutations PostgreSQL's grammar reads is still decided by PostgreSQL.
 */
const POSTGRES_FORMS = [
  "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
  "CREATE TRIGGER tr BEFORE UPDATE OF v ON t FOR EACH ROW WHEN (OLD.v IS DISTINCT FROM NEW.v) EXECUTE FUNCTION f()",
  "CREATE TRIGGER tr INSTEAD OF INSERT ON w FOR EACH ROW EXECUTE FUNCTION f()",
  "CREATE TRIGGER tr AFTER TRUNCATE ON t FOR EACH STATEMENT EXECUTE PROCEDURE f()",
  "CREATE TRIGGER tr AFTER DELETE ON t REFERENCING OLD TABLE AS gone FOR EACH STATEMENT EXECUTE FUNCTION f()",
  "CREATE VIEW w (a, b) AS SELECT 1, 2",
  "CREATE VIEW public.w AS SELECT 1",
  "CREATE VIEW w WITH (security_barrier) AS SELECT v FROM t",
  "CREATE OR REPLACE VIEW w AS SELECT v FROM t WITH LOCAL CHECK OPTION",
  "SELECT EXISTS (VALUES (1)), EXISTS (TABLE t), EXISTS (WITH w AS (SELECT 1) SELECT * FROM w)",
  "SELECT EXISTS (SELECT 1 FROM t UNION SELECT 2)",
  "SELECT count(DISTINCT v), string_agg(DISTINCT s, ',') FROM t",
  "SELECT CAST(v AS text), CAST(s AS numeric(10, 2)), CAST(s AS varchar(3)), v::int4, s::character varying(4)",
  "CREATE TABLE z (a text, b varchar(10), c numeric(5, 2), d timestamp(3) with time zone, e char(2), f float(20), g interval(3))",
  "SELECT ARRAY(SELECT v FROM t), ARRAY[1, 2][1], (SELECT 1)",
  "SELECT v FROM t WHERE v IN (SELECT v FROM t) ORDER BY v USING <",
  "SELECT DATE '2000-01-01', TIMESTAMP(3) '2000-01-01 00:00:00', INTERVAL '1' DAY",
  "INSERT INTO t (v) VALUES (1), (DEFAULT) ON CONFLICT (v) DO UPDATE SET v = EXCLUDED.v RETURNING *",
  "WITH RECURSIVE r (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
  "SELECT sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t WINDOW x AS (PARTITION BY v)",
];

/**
 * Fixed forms from the reviews of #59 (Neon, 2026-11-23): valid PostgreSQL that #59's token rules
 * claimed PostgreSQL rejects, statements PostgreSQL takes no snapshot for (each first word of
 * `PlannedStmtRequiresSnapshot`'s refusals but transaction control, which the probe cannot run),
 * text before the first word PostgreSQL's scanner skips, and several statements in one text.
 */
const REVIEW_FORMS = [
  "SELECT (ARRAY[1, 2, 3])[1:2]",
  "SELECT v[1:2] FROM t",
  "SELECT substr(string := 'abc', start := 2)",
  "SELECT 1 AS café",
  "SELECT π",
  "SELECT 1 AS a$b",
  "SELECT $é$hello$é$",
  "SELECT 0x7fffffffffffffff, 0o777777777777777777777, 0x_1, 1_000., 1_0.e5",
  "SELECT (1 -- a comment\r)",
  "CREATE TABLE trigger (id integer)",
  "CREATE SCHEMA trigger",
  "SELECT s FROM t ORDER BY s USING ~<~",
  "SELECT v FROM t ORDER BY v USING <<",
  "CREATE TABLE z (a boolean CHECK (NOT pg_is_in_recovery()))",
  "SELECT j OPERATOR(pg_catalog.?) 'a' FROM t",
  "SELECT 1 case",
  "SELECT xmlelement(name window)",
  "CREATE TABLE z (a integer UNIQUE NULLS NOT DISTINCT)",
  "SELECT * FROM (SELECT 1 distinct) x",
  "VALUES (***1)",
  "SELECT * FROM (SELECT 1) int (a) ORDER BY a USING <",
  "WITH int (a) AS (SELECT 1) TABLE int",
  "WITH nullif (a) AS (SELECT 1) TABLE nullif",
  "CREATE TABLE coalesce ()",
  "SELECT * FROM (SELECT 1) AS enum (a) ORDER BY a USING <",
  "WITH c AS (SELECT 1 FROM rows WHERE range BETWEEN following AND preceding) TABLE c",
  "SELECT sum(1) OVER (ORDER BY 1 ROWS BETWEEN unbounded + 1 FOLLOWING AND 2 FOLLOWING) FROM t",
  "LOCK TABLE t IN ACCESS SHARE MODE",
  "LOCK TABLE missing_table",
  "LISTEN channel",
  "UNLISTEN channel",
  "NOTIFY channel, 'payload'",
  "FETCH 1 FROM missing_cursor",
  "MOVE 1 FROM missing_cursor",
  "CHECKPOINT",
  "SET CONSTRAINTS ALL DEFERRED",
  "RESET search_path",
  "SHOW search_path",
  "; SELECT 1",
  ";; SELECT missing_column FROM t",
  "-- a comment\rSELECT 1",
  "/* a /* nested */ comment */; SELECT 1",
  "SELECT 1; SELECT 2",
  "SELECT 1; SELEC 2",
  "SET search_path = public; SELECT 1",
  "INSERT INTO t VALUES (2, 2, 'b'); SELECT 1",
  // Neon's re-check of #59 (f361faf1): statements analysed at Parse, utility statements, the
  // PREPARE forms (TRANSACTION is an unreserved keyword), and a keyword folded beyond ASCII.
  "EXPLAIN SELECT 1",
  "EXPLAIN (COSTS OFF) SELECT v FROM t",
  "DECLARE c CURSOR FOR SELECT 1",
  "CALL no_such()",
  "DO $$ BEGIN END $$",
  "PREPARE p AS SELECT 1",
  "PREPARE transaction AS SELECT 1",
  "PREPARE transaction (integer) AS SELECT $1",
  "/* c */ PREPARE /* c */ transaction AS SELECT 1",
  "TRUNCATE t",
  "TRUNCATE missing_table",
  "CREATE TABLE z AS SELECT 1 WITH NO DATA",
  "CREATE TABLE IF NOT EXISTS t AS SELECT 1",
  "CREATE TABLE z AS SELECT missing_column FROM t",
  "CREATE TEMP TABLE z AS SELECT 1",
  "CREATE UNLOGGED TABLE z AS SELECT 1",
  "CREATE TABLE z (a integer GENERATED ALWAYS AS (1) STORED)",
  "CREATE TABLE z (a integer GENERATED ALWAYS AS (1) STORED, b no_such_type)",
  "CREATE MATERIALIZED VIEW mv AS SELECT 1",
  "CREATE MATERIALIZED VIEW mv AS SELECT 1 WITH NO DATA",
  "SELECT (",
  "ſelect 1",
];

/** The sources read from the repository (the simulator's are recorded by the test). */
export function repositorySources(): CorpusSource[] {
  return [
    sqlLogicTest(),
    featureMatrix(),
    documentation(),
    consoleQueries(),
    { name: "PostgreSQL forms", statements: POSTGRES_FORMS },
    { name: "review forms", statements: REVIEW_FORMS },
  ];
}

// --- Tokens -------------------------------------------------------------------------------------

/** One token of a statement: its text, whether it is a word (a keyword or a name), and where. */
export interface Token {
  readonly text: string;
  readonly word: boolean;
  readonly start: number;
  readonly end: number;
}

const OPERATOR_CHARACTERS = "+-*/<>=~!@#%^&|`?";

/**
 * The tokens of `sql` without whitespace and comments, as PostgreSQL's scanner (scan.l) splits
 * them: `--` comments end at a line feed or a carriage return; block comments nest; strings take
 * `''` (and, after E, backslash escapes); `$tag$` quotes; identifiers take letters, digits, `_`,
 * `$` and non-ASCII characters; numbers take `_` separators and 0x, 0o and 0b prefixes; an
 * operator is the longest run of operator characters. Unterminated text runs to the end.
 */
export function tokens(sql: string): Token[] {
  return scan(sql);
}

function scan(sql: string): Token[] {
  const out: Token[] = [];
  let i = 0;
  const at = (k: number) => sql[k] ?? "";
  const isWordStart = (c: string) => /[A-Za-z_\u0080-\uffff]/u.test(c) && c !== "";
  const isWordPart = (c: string) => /[A-Za-z0-9_$\u0080-\uffff]/u.test(c) && c !== "";
  const quoted = (from: number, quote: string, backslash: boolean): number => {
    let k = from + 1;
    while (k < sql.length) {
      if (backslash && at(k) === "\\") k += 2;
      else if (at(k) === quote && at(k + 1) === quote) k += 2;
      else if (at(k) === quote) return k + 1;
      else k += 1;
    }
    return sql.length;
  };
  while (i < sql.length) {
    const c = at(i);
    const start = i;
    if (/\s/u.test(c)) {
      i += 1;
      continue;
    }
    if (c === "-" && at(i + 1) === "-") {
      while (i < sql.length && at(i) !== "\n" && at(i) !== "\r") i += 1;
      continue;
    }
    if (c === "/" && at(i + 1) === "*") {
      let depth = 0;
      while (i < sql.length) {
        if (at(i) === "/" && at(i + 1) === "*") {
          depth += 1;
          i += 2;
        } else if (at(i) === "*" && at(i + 1) === "/") {
          depth -= 1;
          i += 2;
          if (depth === 0) break;
        } else i += 1;
      }
      continue;
    }
    let word = false;
    if (c === "'") i = quoted(i, "'", false);
    else if ((c === "E" || c === "e") && at(i + 1) === "'") i = quoted(i + 1, "'", true);
    else if (
      (c === "U" || c === "u") &&
      at(i + 1) === "&" &&
      (at(i + 2) === "'" || at(i + 2) === '"')
    )
      i = quoted(i + 2, at(i + 2), false);
    else if (c === '"') i = quoted(i, '"', false);
    else if (c === "$" && /[0-9]/u.test(at(i + 1))) {
      i += 1;
      while (/[0-9]/u.test(at(i))) i += 1;
    } else if (c === "$") {
      const tag = /^\$(?:[A-Za-z_\u0080-\uffff][A-Za-z0-9_\u0080-\uffff]*)?\$/u.exec(
        sql.slice(i),
      )?.[0];
      if (tag === undefined) i += 1;
      else {
        const close = sql.indexOf(tag, i + tag.length);
        i = close < 0 ? sql.length : close + tag.length;
      }
    } else if (/[0-9]/u.test(c) || (c === "." && /[0-9]/u.test(at(i + 1)))) {
      const number =
        /^(?:0[xX][0-9A-Fa-f_]*|0[oO][0-7_]*|0[bB][01_]*|(?:[0-9][0-9_]*)?\.?[0-9_]*(?:[eE][-+]?[0-9_]+)?)/u.exec(
          sql.slice(i),
        )?.[0];
      i += Math.max(1, number?.length ?? 1);
    } else if (isWordStart(c)) {
      word = true;
      i += 1;
      while (isWordPart(at(i))) i += 1;
    } else if (c === ":" && (at(i + 1) === ":" || at(i + 1) === "=")) i += 2;
    else if (OPERATOR_CHARACTERS.includes(c)) {
      while (
        i < sql.length &&
        OPERATOR_CHARACTERS.includes(at(i)) &&
        !(at(i) === "-" && at(i + 1) === "-") &&
        !(at(i) === "/" && at(i + 1) === "*")
      )
        i += 1;
    } else i += 1;
    out.push({ text: sql.slice(start, i), word, start, end: i });
  }
  return out;
}

/**
 * `sql` with tokens `from` to `to` (exclusive) replaced by `text`: the text between the tokens
 * kept is the original's. A space keeps the replacement apart from a word beside it.
 */
function splice(
  sql: string,
  list: readonly Token[],
  from: number,
  to: number,
  text: string,
): string {
  const begin = list[from]?.start ?? sql.length;
  const finish = list[to - 1]?.end ?? begin;
  const before = sql.slice(0, begin);
  const after = sql.slice(to > from ? finish : begin);
  const pad = (left: string, right: string) =>
    left !== "" && right !== "" && /[A-Za-z0-9_$]$/u.test(left) && /^[A-Za-z0-9_$]/u.test(right)
      ? " "
      : "";
  return `${before}${pad(before, text)}${text}${pad(text === "" ? before : text, after)}${after}`;
}

/** Keywords that stand where another of their group could: a swap is a near miss of the grammar. */
const SWAPS: ReadonlyArray<readonly string[]> = [
  ["SELECT", "VALUES", "TABLE", "WITH"],
  ["AFTER", "BEFORE", "INSTEAD OF"],
  ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "MERGE"],
  ["UNION", "INTERSECT", "EXCEPT"],
  ["INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL"],
  ["ASC", "DESC"],
  ["ROWS", "RANGE", "GROUPS"],
  ["PRECEDING", "FOLLOWING"],
  ["AND", "OR"],
  ["EXISTS", "NOT EXISTS"],
  ["ANY", "ALL", "SOME"],
  ["WHERE", "HAVING"],
  ["ROLLUP", "CUBE", "GROUPING SETS"],
  ["FIRST", "LAST"],
  ["NULL", "TRUE", "FALSE", "DEFAULT"],
  ["TABLE", "VIEW", "INDEX", "TRIGGER", "TYPE", "SEQUENCE", "SCHEMA", "FUNCTION"],
  ["ROW", "STATEMENT"],
  ["CASCADE", "RESTRICT"],
  ["LIKE", "ILIKE", "SIMILAR TO"],
  ["DISTINCT", "ALL"],
  ["ON", "USING"],
  ["COUNT", "SUM", "MIN", "MAX", "AVG", "STRING_AGG"],
];

/** Type names, both the special-grammar ones and generic names. */
export const TYPE_NAMES = [
  "integer",
  "int",
  "bigint",
  "smallint",
  "text",
  "varchar",
  "char",
  "character",
  "numeric",
  "decimal",
  "boolean",
  "bool",
  "date",
  "timestamp",
  "timestamptz",
  "time",
  "interval",
  "real",
  "float",
  "json",
  "jsonb",
  "bytea",
  "uuid",
] as const;
const TYPE_SET = new Set<string>(TYPE_NAMES.map((name) => name.toUpperCase()));
const MODIFIERS = ["(1)", "(1, 2)", "(1.5)", "()", "(x)", "(-1)", "('a')", "(1, 2, 3)"];
const QUERY_FORMS = [
  "VALUES (1)",
  "TABLE t",
  "WITH w AS (SELECT 1) SELECT 1",
  "SELECT",
  "(SELECT 1)",
  "SELECT 1 UNION SELECT 2",
];

/** The index of the parenthesis closing the one at `open`, or -1. */
function closing(list: readonly Token[], open: number): number {
  let depth = 0;
  for (let index = open; index < list.length; index++) {
    if (list[index]?.text === "(") depth++;
    else if (list[index]?.text === ")" && --depth === 0) return index;
  }
  return -1;
}

export const MUTATIONS = [
  "deleted tokens",
  "truncated",
  "duplicated token",
  "swapped keyword",
  "emptied list",
  "type modifier",
  "subquery form",
] as const;
export type Mutation = (typeof MUTATIONS)[number];

/**
 * `sql` mutated by `kind` with choices drawn from `pick` (an index below its argument), or null
 * when the statement has nothing that mutation applies to.
 */
export function mutate(sql: string, kind: Mutation, pick: (n: number) => number): string | null {
  const list = tokens(sql);
  if (list.length === 0) return null;
  switch (kind) {
    case "deleted tokens": {
      const at = pick(list.length);
      const span = 1 + pick(3);
      return splice(sql, list, at, Math.min(list.length, at + span), "");
    }
    case "truncated":
      return list.length < 2 ? null : sql.slice(0, list[pick(list.length - 1)]?.end ?? 0);
    case "duplicated token": {
      const at = pick(list.length);
      const token = list[at];
      if (token === undefined) return null;
      // The token again, after itself, with the spacing that preceded it (a space at the start).
      const gap = at === 0 ? " " : sql.slice(list[at - 1]?.end ?? 0, token.start) || " ";
      return `${sql.slice(0, token.end)}${gap}${token.text}${sql.slice(token.end)}`;
    }
    case "swapped keyword": {
      const places: Array<[number, readonly string[]]> = [];
      list.forEach((token, index) => {
        for (const group of SWAPS)
          if (token.word && group.some((word) => word.split(" ")[0] === token.text.toUpperCase()))
            places.push([index, group]);
      });
      if (places.length === 0) return null;
      const [at, group] = places[pick(places.length)] ?? [0, []];
      const word = group[pick(group.length)] ?? "";
      return splice(sql, list, at, at + 1, word);
    }
    case "emptied list": {
      const opens = list.flatMap((token, index) => (token.text === "(" ? [index] : []));
      if (opens.length === 0) return null;
      const open = opens[pick(opens.length)] ?? 0;
      const close = closing(list, open);
      if (close < 0) return null;
      return splice(sql, list, open + 1, close, "");
    }
    case "type modifier": {
      const places = list.flatMap((token, index) =>
        token.word && TYPE_SET.has(token.text.toUpperCase()) ? [index] : [],
      );
      if (places.length === 0) return null;
      const at = places[pick(places.length)] ?? 0;
      const after = list[at + 1]?.text === "(" ? closing(list, at + 1) + 1 : at + 1;
      if (after <= at) return null;
      const modifier = MODIFIERS[pick(MODIFIERS.length)] ?? "";
      const token = list[at];
      if (token === undefined) return null;
      return `${sql.slice(0, token.end)}${modifier}${sql.slice(list[after - 1]?.end ?? token.end)}`;
    }
    case "subquery form": {
      const opens = list.flatMap((token, index) =>
        token.text === "(" && /^(SELECT|WITH|VALUES)$/iu.test(list[index + 1]?.text ?? "")
          ? [index]
          : [],
      );
      if (opens.length === 0) return null;
      const open = opens[pick(opens.length)] ?? 0;
      const close = closing(list, open);
      if (close < 0) return null;
      const form = QUERY_FORMS[pick(QUERY_FORMS.length)] ?? "";
      return splice(sql, list, open + 1, close, form);
    }
  }
}

/** A seeded statement of the corpus: an original, or a mutation of one. */
export interface CorpusStatement {
  readonly sql: string;
  readonly source: string;
  readonly mutation: Mutation | null;
}

/**
 * `count` statements drawn with `seed`: each picks a source, one of its statements, and either
 * keeps it (one in five) or mutates it.
 */
export function drawCorpus(
  sources: readonly CorpusSource[],
  seed: number,
  count: number,
): CorpusStatement[] {
  const live = sources.filter((source) => source.statements.length > 0);
  const draw = fc
    .tuple(
      fc.nat({ max: live.length - 1 }),
      fc.nat(),
      fc.option(fc.constantFrom(...MUTATIONS), { freq: 4 }),
      fc.array(fc.nat(), { minLength: 8, maxLength: 8 }),
    )
    .map(([s, i, mutation, choices]) => {
      const source = live[s] ?? live[0];
      if (source === undefined) throw new Error("no corpus sources");
      const original = source.statements[i % source.statements.length] ?? "";
      if (mutation === null) return { sql: original, source: source.name, mutation };
      let next = 0;
      const pick = (n: number) => (choices[next++ % choices.length] ?? 0) % n;
      const sql = mutate(original, mutation, pick);
      return sql === null
        ? { sql: original, source: source.name, mutation: null }
        : { sql, source: source.name, mutation };
    });
  return fc.sample(draw, { seed, numRuns: count });
}
