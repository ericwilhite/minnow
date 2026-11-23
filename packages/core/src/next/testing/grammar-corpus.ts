/**
 * The SQL corpus of the grammar-verdict differential (grammar-verdict-pglite.test.ts): the
 * repository's real statements, and seeded mutations of them that land near the grammar's edges.
 *
 * Sources: the committed SQLLogicTest profile, the SQL feature matrix's examples, every `sql` block
 * of the documentation, the console's sample queries, and the interaction simulator's statements
 * (recorded by the test from a run). Mutations: token deletion (one to three tokens), truncation,
 * a duplicated token, a keyword swapped for another of its kind, an emptied argument list, a type
 * modifier on a type name, and a subquery replaced by another query form.
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

/** Splits text into statements at top-level semicolons, keeping quotes and comments whole. */
export function splitStatements(text: string): string[] {
  const out: string[] = [];
  let start = 0;
  let index = 0;
  while (index < text.length) {
    const c = text[index];
    const next = text[index + 1];
    if (c === "'" || c === '"') {
      const end = text.indexOf(c, index + 1);
      index = end < 0 ? text.length : end + 1;
    } else if (c === "-" && next === "-") {
      const end = text.indexOf("\n", index);
      index = end < 0 ? text.length : end + 1;
    } else if (c === "/" && next === "*") {
      const end = text.indexOf("*/", index + 2);
      index = end < 0 ? text.length : end + 2;
    } else if (c === ";" && !openTriggerBody(text.slice(start, index))) {
      out.push(text.slice(start, index));
      start = ++index;
    } else index++;
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
      statements.push(...splitStatements(block[1] ?? ""));
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

/** The sources read from the repository (the simulator's are recorded by the test). */
export function repositorySources(): CorpusSource[] {
  return [
    sqlLogicTest(),
    featureMatrix(),
    documentation(),
    consoleQueries(),
    { name: "PostgreSQL forms", statements: POSTGRES_FORMS },
  ];
}

// --- Tokens -------------------------------------------------------------------------------------

/** One token of a statement: its text, and whether it is a word (a keyword or a name). */
export interface Token {
  readonly text: string;
  readonly word: boolean;
}

const TOKEN =
  /\s+|--[^\n]*|\/\*[\s\S]*?\*\/|'(?:[^']|'')*'?|"(?:[^"]|"")*"?|\$\d+|\d+(?:\.\d*)?(?:[eE][-+]?\d+)?|\.\d+(?:[eE][-+]?\d+)?|[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*|->>|#>>|::|<=|>=|<>|!=|\|\||->|#>|@>|<@|@@|[^\s]/gu;

/** The tokens of `sql` without whitespace and comments (an approximation of PostgreSQL's scanner). */
export function tokens(sql: string): Token[] {
  const out: Token[] = [];
  for (const match of sql.matchAll(TOKEN)) {
    const text = match[0];
    if (/^\s/u.test(text) || text.startsWith("--") || text.startsWith("/*")) continue;
    out.push({ text, word: /^[A-Za-z_]/u.test(text) });
  }
  return out;
}

const join_ = (list: readonly Token[]): string => list.map((token) => token.text).join(" ");

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
      return join_([...list.slice(0, at), ...list.slice(at + span)]);
    }
    case "truncated":
      return list.length < 2 ? null : join_(list.slice(0, 1 + pick(list.length - 1)));
    case "duplicated token": {
      const at = pick(list.length);
      return join_([...list.slice(0, at + 1), ...list.slice(at)]);
    }
    case "swapped keyword": {
      const places: Array<[number, readonly string[]]> = [];
      list.forEach((token, index) => {
        for (const group of SWAPS)
          if (group.some((word) => word.split(" ")[0] === token.text.toUpperCase()))
            places.push([index, group]);
      });
      if (places.length === 0) return null;
      const [at, group] = places[pick(places.length)] ?? [0, []];
      const word = group[pick(group.length)] ?? "";
      return join_([...list.slice(0, at), { text: word, word: true }, ...list.slice(at + 1)]);
    }
    case "emptied list": {
      const opens = list.flatMap((token, index) => (token.text === "(" ? [index] : []));
      if (opens.length === 0) return null;
      const open = opens[pick(opens.length)] ?? 0;
      const close = closing(list, open);
      if (close < 0) return null;
      return join_([...list.slice(0, open + 1), ...list.slice(close)]);
    }
    case "type modifier": {
      const places = list.flatMap((token, index) =>
        TYPE_SET.has(token.text.toUpperCase()) ? [index] : [],
      );
      if (places.length === 0) return null;
      const at = places[pick(places.length)] ?? 0;
      const after = list[at + 1]?.text === "(" ? closing(list, at + 1) + 1 : at + 1;
      if (after <= at) return null;
      const modifier = MODIFIERS[pick(MODIFIERS.length)] ?? "";
      return `${join_(list.slice(0, at + 1))} ${modifier} ${join_(list.slice(after))}`.trim();
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
      return `${join_(list.slice(0, open + 1))} ${form} ${join_(list.slice(close))}`;
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
