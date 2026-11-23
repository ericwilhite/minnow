/**
 * Every failure the SQL front end can raise while it reads a statement is classified: a grammar
 * failure, marked where it is raised (packages/core/src/engine/grammar-failure.ts), or a refusal
 * of a statement it read, listed below with its class.
 *
 * Why: the 1.0 owner decides whether a failed statement took its transaction's snapshot from that
 * mark (next/ `Lowerer.takesSnapshot`). PostgreSQL takes the snapshot for every statement its raw
 * parser (gram.y, scan.l) accepts, before analysis; a statement the raw parser rejects takes none.
 * next/'s front end reads and checks in one pass, so a grammar failure left unmarked would take a
 * snapshot PostgreSQL never takes (Astra's re-check of #59: `SELECT 1 WINDOW w`, `SELECT CASE
 * END`, `CREATE VIEW v AS`). This test makes the classification mandatory: a new throw in the
 * parse phase that is neither marked nor listed fails it.
 *
 * The parse phase is computed, not listed: the `Parser` class, the tokenizer and the statement
 * parsers, and every function of engine/query.ts they call, directly or not (the parser folds
 * constants and resolves some names as it reads, so evaluation and resolution helpers are in it).
 * A throw is classified when it is marked (`syntaxError`, `syntaxCompileError`, `grammarFailure`),
 * states its grammar condition (`refusal(message, grammar)`: one message whose text is a grammar
 * failure in some cases, a bare `*`, and read in others), rethrows a caught error, or matches an
 * entry below (its owner and the start of its message).
 * Each class names why PostgreSQL's raw parser accepts that text; the doubtful ones were probed in
 * PGlite (after `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SAVEPOINT s; <statement>;
 * ROLLBACK TO s; RELEASE s`, a change of isolation level is refused with 25001 exactly when the
 * raw parse succeeded).
 */
import ts from "typescript";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const SOURCE = fileURLToPath(new URL("../packages/core/src/engine/query.ts", import.meta.url));

/** Why a listed throw is not a grammar failure: what PostgreSQL's raw parser does with the text. */
const CLASSES = {
  analysis:
    "PostgreSQL's grammar reads the statement; parse analysis or planning refuses it, after the snapshot",
  unsupported:
    "PostgreSQL's grammar reads (and runs) the statement; next/ refuses it after reading",
  evaluation:
    "constant folding while parsing; PostgreSQL evaluates at planning or execution, after the snapshot",
  limit: "a size limit of next/'s; PostgreSQL's grammar reads the text",
  "operator-character":
    "PostgreSQL reads most such characters (` ~ ! @ # % ^ & | ?) as operator characters; only \\ { } fail its scanner",
  "drop-kind":
    "DROP of a kind next/ does not keep: PostgreSQL reads DROP SCHEMA, FUNCTION and the like; an unknown kind word fails its grammar",
  relocation: "rethrows an error already raised, at its position; the mark travels with it",
  api: "not SQL text: an argument that is not a string",
} as const;
type Class = keyof typeof CLASSES;

/** Owner (class or function), the start of the message as written (80 characters), class. */
const EARLY_SEMANTIC: ReadonlyArray<readonly [string, string, Class]> = [
  [
    "scalarFunctionValueGeneric",
    '"GROUPING requires GROUP BY ROLLUP, CUBE, or GROUPING SETS"',
    "evaluation",
  ],
  ["scalarFunctionValueGeneric", "`${name} must be resolved before execution`", "evaluation"],
  [
    "scalarFunctionValueGeneric",
    '"Array subscripts require an array and an integer position"',
    "evaluation",
  ],
  ["scalarFunctionValueGeneric", '"Array subscripts require an array"', "evaluation"],
  [
    "scalarFunctionValueGeneric",
    '"Array subscripts currently require one-dimensional scalar arrays"',
    "evaluation",
  ],
  [
    "scalarFunctionValueGeneric",
    "`${name} must be resolved by the database catalog`",
    "evaluation",
  ],
  ["scalarFunctionValueGeneric", '"POWER produced a non-finite number"', "evaluation"],
  ["scalarFunctionValueGeneric", '"SQRT requires a non-negative number"', "evaluation"],
  ["scalarFunctionValueGeneric", "`${name} length must be a non-negative integer`", "evaluation"],
  [
    "scalarFunctionValueGeneric",
    "`${name} result exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`",
    "evaluation",
  ],
  ["scalarFunctionValueGeneric", '"OVERLAY start must be a positive integer"', "evaluation"],
  ["scalarFunctionValueGeneric", '"OVERLAY length must be a non-negative integer"', "evaluation"],
  ["scalarFunctionValueGeneric", '"SUBSTR start must be an integer"', "evaluation"],
  ["scalarFunctionValueGeneric", '"SUBSTR length must be a non-negative integer"', "evaluation"],
  [
    "assertScalarInputLength",
    "`${label} exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`",
    "evaluation",
  ],
  [
    "boundedScalarResult",
    "`${label} exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`",
    "evaluation",
  ],
  [
    "codePointSlice",
    "`SUBSTR result exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`",
    "evaluation",
  ],
  [
    "assertReplacementResultLength",
    "`REPLACE result exceeds ${String(MAX_SQL_SCALAR_RESULT_CHARACTERS)} characters`",
    "evaluation",
  ],
  ["dateAddValue", '"Date arithmetic requires a date or datetime value"', "evaluation"],
  [
    "dateTruncValue",
    '"DATE_TRUNC requires a unit of year, quarter, month, week, day, hour, minute, or',
    "evaluation",
  ],
  ["dateTruncValue", '"DATE_TRUNC requires a date or datetime value"', "evaluation"],
  ["declaredType", '"TIMESTAMP precision other than 6 is not supported"', "unsupported"],
  ["rejectSemicolons", '"Run one SELECT statement at a time"', "unsupported"],
  [
    "throwLocated",
    "new SqlCompileError(error.message, Math.max(error.offset + offset, 0), error.len",
    "relocation",
  ],
  [
    "throwLocated",
    "new SqlCompileError( error.message, Math.max(offset + span.start, 0), Math.max(s",
    "relocation",
  ],
  [
    "parseSettingStatement",
    '`SET TIME ZONE ${zone?.text ?? ""} is not supported: every datetime is an instan',
    "unsupported",
  ],
  [
    "parseSettingStatement",
    '"SET SESSION CHARACTERISTICS AS TRANSACTION is not supported: set each transacti',
    "unsupported",
  ],
  [
    "parseSettingStatement",
    '`SET ${settingName} = ${zone?.text ?? ""} is not supported: every datetime is an',
    "unsupported",
  ],
  ["parseTruncateStatement", '"TRUNCATE takes one table at a time"', "unsupported"],
  [
    "parseTruncateStatement",
    '"TRUNCATE CASCADE is not supported; truncate dependents explicitly"',
    "unsupported",
  ],
  [
    "parseTransactionStatement",
    "`${keyword} takes no ${extra.text}: this engine has one isolation level`",
    "unsupported",
  ],
  [
    "parseTransactionModes",
    "`${statement} ISOLATION LEVEL SERIALIZABLE is not supported: the engine has one ",
    "unsupported",
  ],
  [
    "compileCheckExpression",
    "`CHECK ${name} takes a row condition over this table's own columns`",
    "analysis",
  ],
  [
    "parseCreateView",
    '"CREATE VIEW takes a name and AS <query>; column lists are not supported"',
    "unsupported",
  ],
  ["compileStatement", '"CREATE TYPE supports AS ENUM (...)"', "unsupported"],
  ["compileStatement", '"CREATE SEQUENCE options are not supported yet"', "unsupported"],
  [
    "compileStatement",
    '"DROP supports: DROP TABLE, DROP VIEW, DROP INDEX, DROP TRIGGER"',
    "drop-kind",
  ],
  [
    "evaluate",
    "`Placeholder $${String(expression.index + 1)} is unbound; pass parameters when e",
    "evaluation",
  ],
  ["evaluate", '"Subqueries are only supported in WHERE, HAVING, SELECT, and IN"', "evaluation"],
  ["evaluate", '"Value lists are only supported with IN"', "evaluation"],
  ["evaluate", '"Window functions are only allowed in the select list"', "evaluation"],
  ["evaluate", '"EXISTS subqueries must be resolved before evaluation"', "evaluation"],
  ["evaluate", '"Full-text search columns must be expanded before evaluation"', "evaluation"],
  ["evaluate", '"BM25 corpus statistics are missing from the plan"', "evaluation"],
  ["evaluate", "`${expression.name} requires grouped execution`", "evaluation"],
  ["evaluate", "`A scalar subquery returned ${String(group.length)} rows`", "evaluation"],
  ["evaluate", '"STRING_AGG requires a delimiter"', "evaluation"],
  ["evaluate", '"STRING_AGG value must be a string"', "evaluation"],
  ["evaluate", '"STRING_AGG delimiter must be a string"', "evaluation"],
  ["extractDatePart", "`Unsupported EXTRACT field: ${field}`", "evaluation"],
  ["extractDatePart", '"EXTRACT requires a datetime value"', "evaluation"],
  ["quantifiedArrayCondition", '"ANY/ALL take a subquery or an array"', "evaluation"],
  [
    "generateSeriesSource",
    '"GENERATE_SERIES takes a start, a stop, and an optional step"',
    "evaluation",
  ],
  ["generateSeriesSource", '"GENERATE_SERIES arguments cannot refer to table rows"', "evaluation"],
  [
    "generateSeriesSource",
    '"GENERATE_SERIES over timestamps takes an INTERVAL step"',
    "evaluation",
  ],
  ["generateSeriesSource", '"GENERATE_SERIES takes integer or timestamp arguments"', "evaluation"],
  ["generateSeriesSource", '"GENERATE_SERIES step size cannot equal zero"', "evaluation"],
  ["orderedSetAggregateValue", "`${name} takes a numeric fraction`", "evaluation"],
  [
    "orderedSetAggregateValue",
    "`percentile value ${String(read)} is not between 0 and 1`",
    "evaluation",
  ],
  ["orderedSetAggregateValue", '"PERCENTILE_CONT requires numeric input"', "evaluation"],
  ["quantifiedMembers", '"ANY/ALL subqueries must be resolved before evaluation"', "evaluation"],
  ["quantifiedMembers", '"ANY/ALL take a subquery or an array"', "evaluation"],
  ["quantifiedMembers", '"ANY/ALL take a one-dimensional array of scalar values"', "evaluation"],
  [
    "evaluateBooleanExpression",
    '"EXISTS subqueries must be resolved before evaluation"',
    "evaluation",
  ],
  ["evaluateBooleanExpression", '"IN requires a value list"', "evaluation"],
  ["evaluateBooleanExpression", '"LIKE requires string operands"', "evaluation"],
  ["evaluateBooleanExpression", '"Boolean conditions require boolean operands"', "evaluation"],
  ["comparisonHolds", '"IN is only supported in WHERE predicates"', "evaluation"],
  ["comparisonHolds", '"LIKE requires string operands"', "evaluation"],
  ["resolveColumn", "`Unknown table alias: ${alias}`", "analysis"],
  ["resolveColumn", "`Ambiguous or missing column: ${reference}`", "analysis"],
  ["numeric", '"Arithmetic and numeric aggregates require numbers"', "evaluation"],
  ["asQueryValue", '"Query produced an unsupported value"', "evaluation"],
  [
    "rewriteUpsertExpression",
    '"ON CONFLICT DO UPDATE expressions cannot contain aggregates"',
    "analysis",
  ],
  [
    "rewriteUpsertExpression",
    '"ON CONFLICT DO UPDATE expressions use target and EXCLUDED scalar values only"',
    "analysis",
  ],
  ["Parser", "`Duplicate CTE name: ${name}`", "analysis"],
  ["Parser", '"Constraint columns must be unique"', "analysis"],
  ["Parser", '"CREATE TABLE has two PRIMARY KEYs"', "analysis"],
  ["Parser", "`Conflicting identity declarations for column ${name}`", "analysis"],
  ["Parser", "`Identity column ${name} must be an integer type`", "analysis"],
  ["Parser", "`AUTOINCREMENT column ${name} must be an integer type`", "analysis"],
  ["Parser", '"CREATE TABLE column names must be unique"', "analysis"],
  ["Parser", "`Constraint column is not declared: ${name}`", "analysis"],
  ["Parser", "`CHECK ${check.name} refers to a column this table has no: ${column}`", "analysis"],
  ["Parser", "`Generated column ${column.name} cannot also have a DEFAULT`", "analysis"],
  [
    "Parser",
    "`Generated column ${column.name} refers to a column this table has no: ${referen",
    "analysis",
  ],
  [
    "Parser",
    "`Generated column ${column.name} cannot reference a generated column: ${referenc",
    "analysis",
  ],
  ["Parser", '"FOREIGN KEY and REFERENCES must name the same number of columns"', "analysis"],
  [
    "Parser",
    '`ON UPDATE ${action === "restrict" || action === "no action" ? "RESTRICT / NO AC',
    "analysis",
  ],
  ["Parser", '"A NOT ENFORCED foreign key cannot declare an ON DELETE action"', "analysis"],
  ["Parser", '"SET DEFAULT is not supported; use SET NULL or CASCADE"', "unsupported"],
  ["Parser", "`CHECK ${fallbackName} takes a row condition, not an aggregate`", "analysis"],
  ["Parser", "`CHECK ${fallbackName} cannot take a parameter`", "analysis"],
  ["Parser", '"IDENTITY sequence options are not supported"', "unsupported"],
  ["Parser", '"Generated columns take an immutable row expression"', "analysis"],
  ["Parser", '"DROP TABLE CASCADE is not supported; drop dependents explicitly"', "unsupported"],
  [
    "Parser",
    '"ALTER TABLE RENAME TO is not supported; create the new table and copy the rows ',
    "unsupported",
  ],
  ["Parser", '"ALTER TABLE RENAME CONSTRAINT is not supported"', "unsupported"],
  [
    "Parser",
    '"ALTER TABLE DROP COLUMN CASCADE is not supported; drop dependents explicitly"',
    "unsupported",
  ],
  ["Parser", "`Unsupported CAST target: ${word}`", "unsupported"],
  ["Parser", '"INSERT columns must be unique"', "analysis"],
  ["Parser", '"INSERT ... SELECT must produce exactly the insert column count"', "analysis"],
  ["Parser", '"Each INSERT row must match the column list length"', "analysis"],
  [
    "Parser",
    "\"ON CONFLICT ON CONSTRAINT is not supported; name the constraint's columns: ON C",
    "unsupported",
  ],
  ["Parser", "`ON CONFLICT DO UPDATE sets a column twice: ${target}`", "analysis"],
  ["Parser", '"ON CONFLICT DO UPDATE cannot reassign the conflict key"', "analysis"],
  ["Parser", "`RETURNING qualifier must name the target table: ${alias ?? table}`", "analysis"],
  ["Parser", '"RETURNING target.* must be the only returned item"', "analysis"],
  [
    "Parser",
    "`RETURNING qualifier must name the target table: ${alias ?? table} ` + `(RETURNI",
    "analysis",
  ],
  ["Parser", '"RETURNING cannot use aggregate functions"', "analysis"],
  ["Parser", "`Duplicate output column: ${item.alias}`", "analysis"],
  [
    "Parser",
    '"A MERGE source query cannot take parameters; bind them in the branches"',
    "analysis",
  ],
  ["Parser", '"A MERGE source query requires an alias"', "analysis"],
  ["Parser", '"MERGE source and target need distinct names"', "analysis"],
  ["Parser", '"MERGE ... MATCHED BY SOURCE and BY TARGET are not supported"', "unsupported"],
  ["Parser", '"Aggregate functions are not allowed in MERGE assignments"', "analysis"],
  ["Parser", '"MERGE assignments must set each column once"', "analysis"],
  ["Parser", '"MERGE INSERT values must match its column list"', "analysis"],
  ["Parser", '"MERGE does not support RETURNING; read the rows back with a SELECT"', "unsupported"],
  [
    "Parser",
    '"UPDATE SET (columns) = (SELECT …) is not supported; assign each column its own ',
    "unsupported",
  ],
  [
    "Parser",
    "`UPDATE SET assigns ${String(columns.length)} columns from ${String(values.lengt",
    "analysis",
  ],
  ["Parser", '"UPDATE assignments must set each column once"', "analysis"],
  ["Parser", '"Aggregate functions are not allowed in UPDATE assignments"', "analysis"],
  ["Parser", '"Window functions are not allowed in UPDATE assignments"', "analysis"],
  ["Parser", "`SET qualifier must name the target table: ${alias ?? table}`", "analysis"],
  ["Parser", '"Aggregate functions are not allowed in mutation predicates"', "analysis"],
  ["Parser", "`${label} take a bare ? placeholder or a constant expression`", "analysis"],
  ["Parser", "`${label} must be constant expressions`", "analysis"],
  ["Parser", "`Duplicate window name: ${name.text}`", "analysis"],
  ["Parser", '"SELECT * requires a FROM clause"', "analysis"],
  ["Parser", "`${clause} positions cannot be used with SELECT *; name the columns`", "analysis"],
  ["Parser", "`${clause} position ${String(expression.value)} is not in select list`", "analysis"],
  ["Parser", '"CUBE supports at most 5 expressions (32 grouping sets)"', "limit"],
  [
    "Parser",
    '"A parenthesized join can only follow a comma, CROSS JOIN, or INNER JOIN"',
    "analysis",
  ],
  ["Parser", '"A parenthesized join takes an ON condition, not USING"', "analysis"],
  ["Parser", '"NATURAL RIGHT JOIN is not supported; write it as a LEFT join"', "unsupported"],
  ["Parser", '"RIGHT JOIN is only supported as the sole join"', "unsupported"],
  ["Parser", '"RIGHT JOIN cannot be combined with SELECT *"', "analysis"],
  ["Parser", '"A parenthesized join cannot take an alias"', "analysis"],
  ["Parser", '"A row constructor is only allowed in a comparison or IN list"', "analysis"],
  ["Parser", '"A recursive CTE may only reference itself in its step member"', "analysis"],
  ["Parser", '"A recursive CTE takes exactly one UNION between base and step"', "analysis"],
  ["Parser", '"VALUES rows must share one width"', "analysis"],
  ["Parser", '"A derived table requires an alias"', "analysis"],
  ["Parser", '"A LATERAL derived table requires an alias"', "analysis"],
  ["Parser", '"UNNEST currently requires an ARRAY constructor"', "unsupported"],
  ["Parser", '"UNNEST ARRAY members cannot refer to table rows"', "analysis"],
  ["Parser", '"UNNEST requires an ARRAY constructor"', "analysis"],
  ["Parser", '"JSON_TABLE currently requires a constant document"', "unsupported"],
  ["Parser", '"JSON_TABLE requires an alias"', "analysis"],
  ["Parser", '"JSON_TABLE document must be JSON text"', "analysis"],
  ["Parser", '"JSON_TABLE document is invalid JSON"', "analysis"],
  ["Parser", '"JSON_TABLE row path supports $ and $[*]"', "unsupported"],
  ["Parser", '"JSON_TABLE requires a row source"', "analysis"],
  ["Parser", "`Recursive CTE references require WITH RECURSIVE: ${table}`", "analysis"],
  ["Parser", '"Compared rows must have the same number of fields"', "analysis"],
  ["Parser", '"Row constructors cannot nest"', "analysis"],
  ["Parser", '"A row IN list takes row constructors"', "analysis"],
  ["Parser", '"A row constructor takes a comparison, IN, or IS NULL"', "analysis"],
  ["Parser", '"A row comparison takes a row on both sides"', "analysis"],
  ["Parser", '"LIKE ESCAPE takes at most one character"', "analysis"],
  ["Parser", '"SIMILAR TO ESCAPE takes a single character"', "analysis"],
  ["Parser", '"An ANY/ALL placeholder cast names an array type, as in $1::int[]"', "analysis"],
  ["Parser", "`${operator} array positions are integers`", "analysis"],
  ["Parser", "`${operator} keys are member names or array positions`", "analysis"],
  ["Parser", "`A SQL statement cannot exceed ${String(MAX_SQL_PARAMETERS)} parameters`", "limit"],
  ["Parser", "`Parameter numbers start at $1: $${token.text}`", "analysis"],
  [
    "Parser",
    "error instanceof Error ? error.message : `Invalid number: ${token.text}`",
    "unsupported",
  ],
  ["Parser", '"NOW takes no arguments"', "analysis"],
  [
    "Parser",
    "`${upper}(precision) is not supported: the engine keeps milliseconds`",
    "unsupported",
  ],
  ["Parser", '"SUBSTRING requires a string argument"', "analysis"],
  ["Parser", "`TRIM ${side} requires FROM`", "analysis"],
  [
    "Parser",
    'upper === "ROW_NUMBER" ? "ROW_NUMBER takes no arguments" : `WITHIN GROUP is requ',
    "analysis",
  ],
  ["Parser", "`${upper} does not take a window frame`", "analysis"],
  ["Parser", '"NTILE requires a positive integer bucket count"', "analysis"],
  ["Parser", '"NTILE does not take a window frame"', "analysis"],
  ["Parser", '"NTILE requires ORDER BY inside OVER (...)"', "analysis"],
  ["Parser", '"NTH_VALUE requires a positive integer position"', "analysis"],
  ["Parser", "`${upper} requires a value argument`", "analysis"],
  ["Parser", "`${upper} requires ORDER BY inside OVER (...)`", "analysis"],
  ["Parser", "`${upper} takes a value, an optional offset, and an optional default`", "analysis"],
  ["Parser", "`${upper} ${label} must be a constant`", "analysis"],
  ["Parser", "`${upper} offset must be a non-negative integer`", "analysis"],
  ["Parser", "`Unsupported function: ${identifier}`", "unsupported"],
  ["Parser", '"DISTINCT is only supported inside aggregate functions"', "unsupported"],
  ["Parser", '"STRING_AGG requires a value and delimiter"', "analysis"],
  ["Parser", "`${name} requires exactly one argument`", "analysis"],
  ["Parser", '`${name} takes ${range} argument${simple.maxArgs === 1 ? "" : "s"}`', "analysis"],
  ["Parser", "`${name} takes scalar arguments`", "analysis"],
  ["Parser", '"ROUND requires one or two arguments"', "analysis"],
  ["Parser", "`${name} takes a string and optional trim characters`", "analysis"],
  ["Parser", "`${name} requires a string, a length, and an optional fill`", "analysis"],
  ["Parser", '"OVERLAY requires a string, a replacement, a start, and a length"', "analysis"],
  ["Parser", "`${name} takes no arguments`", "analysis"],
  ["Parser", "`${name} requires a JSON document and a path`", "analysis"],
  ["Parser", '"JSON_OBJECT takes key and value pairs"', "analysis"],
  ["Parser", '"Use the IS JSON predicate rather than calling IS_JSON"', "analysis"],
  ["Parser", '"REPLACE requires a string, a search, and a replacement"', "analysis"],
  ["Parser", '"SUBSTR requires a string, a start, and an optional length"', "analysis"],
  ["Parser", '"DATE_TRUNC requires a unit and a datetime argument"', "analysis"],
  ["Parser", "`Unsupported DATE_TRUNC unit: ${unit.value}`", "unsupported"],
  ["Parser", '"JSON_ARRAYAGG FILTER is not supported; filter rows in WHERE"', "unsupported"],
  ["Parser", '"FILTER conditions cannot contain aggregates"', "analysis"],
  ["Parser", '"JSON_ARRAYAGG window use is not supported"', "unsupported"],
  ["Parser", '"STRING_AGG window use is not supported"', "unsupported"],
  ["Parser", '"DISTINCT window aggregates are not supported"', "unsupported"],
  [
    "Parser",
    'name === "MODE" ? "MODE() takes no arguments" : `${name} takes one fraction argu',
    "analysis",
  ],
  ["Parser", "`${name} takes a constant fraction`", "analysis"],
  ["Parser", "`${name} requires WITHIN GROUP (ORDER BY ...)`", "analysis"],
  ["Parser", "`${name} takes exactly one ORDER BY expression`", "analysis"],
  ["Parser", "`${name} cannot order by another aggregate`", "analysis"],
  ["Parser", "`${name} does not support FILTER or OVER`", "unsupported"],
  ["Parser", "`${name} cannot take another aggregate as its argument`", "analysis"],
  ["Parser", "`Unknown window name: ${name}`", "analysis"],
  ["Parser", '"GROUPS frames require ORDER BY inside OVER (...)"', "analysis"],
  ["Parser", '"Offset RANGE frames require exactly one ORDER BY expression"', "analysis"],
  ["Parser", '"frame starting offset must not be null"', "analysis"],
  ["Parser", '"frame ending offset must not be null"', "analysis"],
  ["Parser", '"argument of ROWS must be type bigint, not type interval"', "analysis"],
  ["Parser", '"argument of GROUPS must be type bigint, not type interval"', "analysis"],
  ["Parser", '"Window frame offsets other than numeric literals are not supported"', "unsupported"],
  [
    "Parser",
    '"Window frame offsets must be non-negative integers, or numeric RANGE offsets"',
    "analysis",
  ],
  ["Parser", '"frame starting offset must not be negative"', "analysis"],
  ["Parser", '"frame ending offset must not be negative"', "analysis"],
  ["Parser", '"RANGE frame offsets outside double precision are not supported"', "unsupported"],
  ["Parser", '"bigint out of range"', "analysis"],
  ["desugarGroupingSets", '"GROUPING SETS cannot be combined with SELECT DISTINCT"', "analysis"],
  [
    "desugarGroupingSets",
    '"HAVING with GROUPING SETS supports aggregate and literal conditions only"',
    "unsupported",
  ],
  ["desugarGroupingSets", '"GROUPING requires at least one grouping column"', "analysis"],
  ["desugarGroupingSets", '"GROUPING arguments must appear in the GROUP BY clause"', "analysis"],
  ["resolveOrderByOrdinals", '"ORDER BY ordinals cannot be used with SELECT *"', "analysis"],
  ["resolveOrderByOrdinals", "`ORDER BY ordinal is out of range: ${String(ordinal)}`", "analysis"],
  [
    "resolveGroupByReferences",
    "`GROUP BY ordinal is out of range: ${String(ordinal)}`",
    "analysis",
  ],
  [
    "resolveGroupByReferences",
    '"GROUP BY ordinals cannot name an aggregate or window column"',
    "analysis",
  ],
  [
    "assertDistinctOnLeadsOrder",
    '"SELECT DISTINCT ON expressions must match initial ORDER BY expressions"',
    "analysis",
  ],
  [
    "desugarFullJoin",
    '"NATURAL FULL JOIN is not supported; name the shared columns: FULL JOIN … USING ',
    "unsupported",
  ],
  ["desugarFullJoin", '"FULL JOIN is only supported as the sole join"', "unsupported"],
  ["desugarFullJoin", '"FULL JOIN cannot be combined with SELECT *"', "analysis"],
  [
    "assertDistinctSourceNames",
    '`table name "${source.alias}" specified more than once`',
    "analysis",
  ],
  ["lowerSelectBlock", '"GROUPING requires GROUP BY ROLLUP, CUBE, or GROUPING SETS"', "analysis"],
  ["lowerSelectBlock", '"HAVING requires GROUP BY or aggregate functions"', "analysis"],
  ["lowerSelectBlock", '"Window functions are only allowed in the select list"', "analysis"],
  ["lowerSelectBlock", '"DISTINCT aggregates are not allowed in WHERE"', "analysis"],
  ["lowerSelectBlock", '"DISTINCT aggregates are not allowed in GROUP BY"', "analysis"],
  ["expandViewSources", '"A view cannot be defined in terms of itself"', "analysis"],
  ["expandRowLockCteViews", '"not a known planning constant"', "unsupported"],
  [
    "assembleOrderByExpressionBlock",
    '"ORDER BY expressions require a named select list; SELECT * orders by column nam',
    "analysis",
  ],
  [
    "assembleOrderByExpressionBlock",
    '"SELECT DISTINCT orders by selected columns or output aliases only"',
    "analysis",
  ],
  ["assembleOrderByExpressionBlock", '"ORDER BY position is outside the select list"', "analysis"],
  ["validateLimit", '"LIMIT must be between 0 and 100,000"', "limit"],
  [
    "renameBlockOutputs",
    "`A column list needs named columns in the query body: ${name}`",
    "analysis",
  ],
  [
    "renameBlockOutputs",
    '"Column alias list must match the derived table\'s column count"',
    "analysis",
  ],
  [
    "renameBlockOutputs",
    "`CTE ${name} declares ${String(columns.length)} columns but selects ${String(blo",
    "analysis",
  ],
  ["jsonTableColumnValue", '"JSON_TABLE boolean column needs a boolean"', "evaluation"],
  ["jsonTableColumnValue", '"JSON_TABLE numeric column has an invalid value"', "evaluation"],
  ["jsonTableColumnValue", '"JSON_TABLE datetime is invalid"', "evaluation"],
  ["timestampLiteral", "`Invalid TIMESTAMP literal: ${text}`", "analysis"],
  ["validateOffset", '"OFFSET must be between 0 and 100,000,000"', "limit"],
  [
    "desugarWindows",
    '"Full-text MATCH and BM25 are not supported inside OVER(...)"',
    "unsupported",
  ],
  [
    "desugarWindows",
    "`OVER(...) must use aggregates or GROUP BY expressions in a grouped query: ${ali",
    "analysis",
  ],
  ["desugarWindows", '"Window functions cannot be nested inside OVER(...)"', "analysis"],
  ["tokenize", "`A SQL statement cannot exceed ${String(MAX_SQL_TOKENS)} tokens`", "limit"],
  ["tokenize", "`SQL nesting cannot exceed ${String(MAX_SQL_NESTING_DEPTH)} levels`", "limit"],
  ["tokenize", "`Invalid number: ${sql.slice(start, index)}`", "unsupported"],
  ["tokenize", "`Unsupported SQL character: ${character}`", "operator-character"],
  ["validateSqlSource", '"SQL must be a string"', "api"],
  [
    "validateSqlSource",
    "`SQL text cannot exceed ${String(MAX_SQL_TEXT_CHARACTERS)} characters`",
    "limit",
  ],
];

/** Where reading a statement starts: every function they reach is in the parse phase. */
const ROOTS = [
  "tokenize",
  "parseSettingStatement",
  "parseTruncateStatement",
  "parseTransactionStatement",
  "parseTransactionModes",
  "parseCreateView",
  "parseCreateTrigger",
  "rejectSemicolons",
  "validateSqlSource",
];

interface Throw {
  readonly owner: string;
  readonly line: number;
  readonly message: string;
  readonly kind: "marked" | "rethrow" | "other";
}

function audit(): { throws: Throw[]; missingRoots: string[] } {
  const text = readFileSync(SOURCE, "utf8");
  const file = ts.createSourceFile(SOURCE, text, ts.ScriptTarget.Latest, true);
  const functions = new Map<string, ts.FunctionDeclaration>();
  let parser: ts.ClassDeclaration | undefined;
  for (const statement of file.statements) {
    if (ts.isFunctionDeclaration(statement) && statement.name)
      functions.set(statement.name.text, statement);
    if (ts.isClassDeclaration(statement) && statement.name?.text === "Parser") parser = statement;
  }
  const calls = (node: ts.Node): string[] => {
    const found: string[] = [];
    const visit = (child: ts.Node) => {
      if (ts.isCallExpression(child) && ts.isIdentifier(child.expression))
        found.push(child.expression.text);
      ts.forEachChild(child, visit);
    };
    visit(node);
    return found;
  };
  const phase = new Set<string>();
  const queue: ts.Node[] = [];
  const reach = (name: string) => {
    const declaration = functions.get(name);
    if (declaration === undefined || phase.has(name)) return;
    phase.add(name);
    queue.push(declaration);
  };
  for (const root of ROOTS) reach(root);
  if (parser) for (const name of calls(parser)) reach(name);
  for (let node = queue.pop(); node !== undefined; node = queue.pop())
    for (const name of calls(node)) reach(name);
  const ownerOf = (node: ts.Node): string => {
    for (let parent = node.parent; parent !== undefined; parent = parent.parent)
      if (ts.isClassDeclaration(parent)) return parent.name?.text ?? "?";
    for (let parent = node.parent; parent !== undefined; parent = parent.parent)
      if (ts.isFunctionDeclaration(parent)) return parent.name?.text ?? "?";
    return "?";
  };
  const throws: Throw[] = [];
  const visit = (node: ts.Node) => {
    if (ts.isThrowStatement(node)) {
      const owner = ownerOf(node);
      if (owner === "Parser" || phase.has(owner)) {
        const expression = node.expression;
        let kind: Throw["kind"] = "other";
        if (ts.isIdentifier(expression)) kind = "rethrow";
        else if (
          ts.isCallExpression(expression) &&
          ts.isIdentifier(expression.expression) &&
          ["syntaxError", "syntaxCompileError", "grammarFailure", "refusal"].includes(
            expression.expression.text,
          )
        )
          kind = "marked";
        const first =
          ts.isNewExpression(expression) || ts.isCallExpression(expression)
            ? expression.arguments?.[0]
            : undefined;
        throws.push({
          owner,
          line: file.getLineAndCharacterOfPosition(node.getStart()).line + 1,
          message: (first?.getText() ?? "").replace(/\s+/gu, " ").slice(0, 80),
          kind,
        });
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  const missingRoots = [
    ...ROOTS.filter((root) => !functions.has(root)),
    ...(parser ? [] : ["Parser"]),
  ];
  return { throws, missingRoots };
}

const listed = (owner: string, message: string) =>
  EARLY_SEMANTIC.find(([o, m]) => o === owner && m === message);

describe("the SQL front end's failures are classified", () => {
  const { throws, missingRoots } = audit();

  it("finds the parser and every root of the parse phase", () => {
    expect(missingRoots).toEqual([]);
    expect(throws.length, "the parse phase has throws").toBeGreaterThan(300);
  });

  it("marks or lists every throw of the parse phase", () => {
    const unclassified = throws
      .filter((entry) => entry.kind === "other" && listed(entry.owner, entry.message) === undefined)
      .map((entry) => `engine/query.ts:${String(entry.line)} ${entry.owner}: ${entry.message}`);
    expect(
      unclassified,
      "classify each: mark a grammar failure (syntaxError / syntaxCompileError: PostgreSQL's raw parser rejects the text), or list it in EARLY_SEMANTIC with its class",
    ).toEqual([]);
  });

  it("lists nothing that is marked, gone, or out of the parse phase", () => {
    const live = new Set(
      throws
        .filter((entry) => entry.kind === "other")
        .map((entry) => `${entry.owner}\u0000${entry.message}`),
    );
    const stale = EARLY_SEMANTIC.filter(
      ([owner, message]) => !live.has(`${owner}\u0000${message}`),
    );
    expect(stale, "entries that match no unmarked throw of the parse phase").toEqual([]);
    for (const [, , kind] of EARLY_SEMANTIC) expect(Object.keys(CLASSES)).toContain(kind);
  });
});
