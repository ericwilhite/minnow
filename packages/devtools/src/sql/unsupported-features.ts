// Generated from packages/core/sql-feature-matrix.json by scripts/devtools-unsupported-features.mts.
// Do not edit: run `npm run devtools:matrix` after the matrix changes.
import type { UnsupportedFeatureRecord } from "./feature-matrix.js";

/** Every feature the engine records as unsupported, with the error fragment that names it. */
export const unsupportedFeatures: readonly UnsupportedFeatureRecord[] = [
  {
    id: "aggregate.ordered-set",
    error: "Unsupported function: percentile_cont",
    notes:
      "Ordered-set aggregates (percentile_cont, percentile_disc, mode) and corr are not supported; a window over ROW_NUMBER() and COUNT() OVER () computes a median.",
  },
  {
    id: "array.concat",
    error: "PostgreSQL array concatenation (||) is not supported",
    notes:
      "PostgreSQL's array || array and array || element are structural concatenation. Minnow refuses || whenever either operand is an array rather than inventing a text concatenation PostgreSQL does not have.",
  },
  {
    id: "catalog.information-schema",
    error: "Expected eof, found .",
    notes:
      "information_schema, pg_catalog, and sqlite_master are not supported; the catalog is read through the API (listTables, describe).",
  },
  {
    id: "ddl.add-column-if-not-exists",
    error: "Expected eof, found EXISTS",
    notes:
      "ADD COLUMN IF NOT EXISTS is not supported; check the catalog first or let the duplicate-column error stand.",
  },
  {
    id: "ddl.add-constraint",
    error: "Expected eof, found CHECK",
    notes:
      "ADD CONSTRAINT and DROP CONSTRAINT are not supported; constraints are declared in CREATE TABLE.",
  },
  {
    id: "ddl.alter-column",
    error: "Expected ADD, found ALTER",
    notes: "ALTER COLUMN forms are not supported; see ddl.alter-table-rename.",
  },
  {
    id: "ddl.alter-table-rename",
    error: "Expected ADD, found RENAME",
    notes:
      "ALTER TABLE RENAME COLUMN, RENAME TO, ALTER COLUMN TYPE / SET DEFAULT / SET NOT NULL / DROP NOT NULL, ADD CONSTRAINT, and DROP CONSTRAINT are not supported as SQL. ALTER TABLE ADD COLUMN and DROP COLUMN are. The schema DSL's migrate() renames columns and widens nullability through the catalog.",
  },
  {
    id: "ddl.alter-type",
    error: "Expected TABLE, found TYPE",
    notes:
      "ALTER TYPE and DROP TYPE are not supported; CREATE TYPE … AS ENUM is, and the schema DSL widens enum values through migrate().",
  },
  {
    id: "ddl.bytea",
    error: "Unsupported column type: BYTEA",
    notes:
      "BYTEA columns and bytea literals, casts, and functions (encode, decode, sha256) are not supported; store binary data as base64 or hex TEXT.",
  },
  {
    id: "ddl.create-table-like",
    error: "Unsupported column type: keyed",
    notes:
      "CREATE TABLE … (LIKE other) is not supported; spell the columns out, or CREATE TABLE AS SELECT for a data copy.",
  },
  {
    id: "ddl.deferrable-constraints",
    error: "Expected )",
    notes:
      "DEFERRABLE constraints are not supported; every constraint is checked at the statement.",
  },
  {
    id: "ddl.domain",
    error: "Expected TABLE, found DOMAIN",
    notes: "CREATE DOMAIN is not supported; put the CHECK on the column.",
  },
  {
    id: "ddl.drop-table-multiple",
    error: "Expected eof, found ,",
    notes: "DROP TABLE takes one table per statement.",
  },
  {
    id: "ddl.exclude-constraint",
    error: "Expected )",
    notes: "EXCLUDE constraints are not supported.",
  },
  {
    id: "ddl.expression-index",
    error: "Expected )",
    notes: "Expression indexes are not supported; index a stored generated column instead.",
  },
  {
    id: "ddl.foreign-key-on-update",
    error: "ON UPDATE CASCADE has nothing to act on: a unique key cannot change",
    notes:
      "ON UPDATE actions are not supported because primary-key values cannot be updated; ON DELETE CASCADE, SET NULL, RESTRICT, and NO ACTION are.",
  },
  {
    id: "ddl.foreign-key-set-default",
    error: "SET DEFAULT is not supported; use SET NULL or CASCADE",
    notes:
      "SET DEFAULT would rewrite orphaned references to the column's default value at delete time; Minnow implements RESTRICT, CASCADE, and SET NULL. The statement is rejected at parse, before touching the catalog.",
  },
  {
    id: "ddl.function",
    error: "Expected TABLE, found FUNCTION",
    notes:
      "CREATE FUNCTION and CREATE TRIGGER … EXECUTE FUNCTION are not supported; triggers take an inline BEGIN … END body of INSERT, UPDATE, and DELETE statements.",
  },
  {
    id: "ddl.materialized-view",
    error: "Expected TABLE, found MATERIALIZED",
    notes: "Materialized views are not supported; CREATE TABLE AS SELECT stores a snapshot.",
  },
  {
    id: "ddl.partial-index",
    error: "Expected eof, found WHERE",
    notes:
      "Partial indexes (WHERE), expression indexes, INCLUDE columns, USING method, and CONCURRENTLY are not supported; an index is over one or more plain columns.",
  },
  {
    id: "ddl.schema",
    error: "Expected TABLE, found SCHEMA",
    notes:
      "Schemas are not supported: there is one namespace, and a schema-qualified name (public.users) is refused.",
  },
  {
    id: "ddl.schema-qualified-name",
    error: "Expected eof, found .",
    notes: "Schema-qualified names are not supported; there is one namespace.",
  },
  {
    id: "ddl.sequence-functions",
    error: "Unsupported function: setval",
    notes:
      "setval, currval, and ALTER SEQUENCE are not supported; nextval and CREATE SEQUENCE are.",
  },
  {
    id: "ddl.serial-non-key",
    error: "Auto-increment requires the unique key column: seq",
    notes:
      "SERIAL and GENERATED … AS IDENTITY are supported on the primary key only; a sequence-fed non-key column is refused.",
  },
  {
    id: "ddl.view-column-list",
    error: "CREATE VIEW takes a name and AS <query>; column lists are not supported",
    notes:
      "A column list on CREATE VIEW and WITH CHECK OPTION are not supported; alias the columns in the view's SELECT.",
  },
  {
    id: "expression.at-time-zone",
    error: "Expected eof, found AT",
    notes:
      "AT TIME ZONE and timezone() are not supported: every datetime is an instant in UTC, and rendering in another zone belongs to the application.",
  },
  {
    id: "expression.bitwise-operators",
    error: "Unsupported SQL character: &",
    notes:
      "The bitwise operators &, |, #, ~, <<, and >> and the prefix operators |/ and @ are not supported.",
  },
  {
    id: "expression.date-minus-date",
    error: "Arithmetic and numeric aggregates require numbers",
    notes:
      "date - date and timestamp - timestamp are not supported; see expression.interval-values.",
  },
  {
    id: "expression.interval-values",
    error: "Date arithmetic requires a date or datetime value",
    notes:
      "Interval-valued arithmetic — interval + interval, timestamp - timestamp, date - date, date + integer, justify_days — is not supported. A date or datetime plus or minus an INTERVAL is; subtract two EXTRACT(EPOCH …) readings for a duration in seconds.",
  },
  {
    id: "expression.row-constructor-value",
    error: "Unsupported function: ROW",
    notes:
      "A row constructor as a value is not supported; row comparisons ((a, b) = (1, 2), (a, b) IN (…)) are.",
  },
  {
    id: "function.generate-series",
    error: "Expected identifier, found 1",
    notes: "generate_series is not supported; a recursive CTE produces a series.",
  },
  {
    id: "function.regexp-arrays",
    error: "Unsupported function: regexp_match",
    notes:
      "regexp_match, regexp_matches, regexp_split_to_array, and regexp_split_to_table return arrays or sets, which are not supported; SUBSTRING(text FROM 'pattern'), REGEXP_REPLACE, and the ~ operators cover single matches.",
  },
  {
    id: "function.server-introspection",
    error: "Unsupported function: version",
    notes:
      "version(), current_user, session_user, current_schema, pg_typeof, and setseed describe a server this engine is not; SHOW server_version answers the version question.",
  },
  {
    id: "join.right-multi",
    error: "RIGHT JOIN is only supported as the sole join",
    notes:
      "RIGHT JOIN desugars by swapping the two sides of a LEFT JOIN, which needs the block to hold exactly one join. Rewrite the block so the preserved side is on the left of a LEFT JOIN.",
  },
  {
    id: "join.right-wildcard",
    error: "RIGHT JOIN cannot be combined with SELECT *",
    notes:
      "The desugaring swaps the two sources, which would reorder a wildcard's output columns. Name the output columns explicitly, or write the mirrored LEFT JOIN.",
  },
  {
    id: "json.concat",
    error: "PostgreSQL JSONB concatenation (||) is not supported",
    notes:
      "PostgreSQL's jsonb || jsonb merges documents structurally. Minnow refuses || whenever either operand is JSONB rather than inventing a text concatenation PostgreSQL does not have.",
  },
  {
    id: "json.containment-operators",
    error: "Unsupported SQL character: @",
    notes:
      "The @>, <@, ?, ?|, and ?& operators are not supported; compare members with ->> or test presence with JSON_EXISTS.",
  },
  {
    id: "json.inspection-functions",
    error: "Unsupported function: jsonb_typeof",
    notes:
      "jsonb_typeof and jsonb_array_length are not supported; JSON_EXISTS, JSON_VALUE, and JSON_QUERY answer most of the same questions.",
  },
  {
    id: "json.mutation-functions",
    error: "Unsupported function: jsonb_set",
    notes:
      "jsonb_set, jsonb_insert, jsonb_strip_nulls, and json_extract_path_text are not supported; rebuild the document with JSON_OBJECT / json_build_object and read members with -> and ->>.",
  },
  {
    id: "json.object-agg",
    error: "Unsupported function: json_object_agg",
    notes:
      "json_object_agg / jsonb_object_agg are not supported; json_agg of json_build_object pairs is the usual substitute.",
  },
  {
    id: "json.path-operators",
    error: "Unsupported SQL character: #",
    notes:
      "The #> and #>> path operators are not supported; chain -> and ->> or use JSON_VALUE with a path.",
  },
  {
    id: "json.table-correlated",
    error: "JSON_TABLE currently requires a constant document",
    notes:
      "PostgreSQL evaluates JSON_TABLE laterally against each source row's document. Minnow expands only constant documents, so a column-valued document is refused at compile time.",
  },
  {
    id: "literal.bit-string",
    error: "Expected eof, found 101",
    notes: "Bit-string literals and the BIT types are not supported.",
  },
  {
    id: "mutation.data-modifying-cte",
    error: "Expected SELECT, found INSERT",
    notes: "A data-modifying statement inside WITH is not supported; run the write, then the read.",
  },
  {
    id: "mutation.update-keyless",
    error: "UPDATE requires a table with a unique key",
    notes:
      "Deliberate: mutation segments address rows by unique key, so tables without one cannot be updated or deleted through any API.",
  },
  {
    id: "mutation.update-row-value",
    error: "Expected identifier, found (",
    notes: "The row-value assignment SET (a, b) = (…) is not supported; assign each column.",
  },
  {
    id: "mutation.update-set-default",
    error: "Ambiguous or missing column: DEFAULT",
    notes:
      "SET column = DEFAULT and the row-value form SET (a, b) = (…) are not supported; assign the value explicitly.",
  },
  {
    id: "mutation.upsert-non-key-unique",
    error: "ON CONFLICT targets the table's primary or unique key columns: name",
    notes:
      "ON CONFLICT targets the table's primary or row-addressing unique key; a secondary UNIQUE column, a constraint name (ON CONSTRAINT), or a partial-index predicate cannot be the target.",
  },
  {
    id: "privileges.grant",
    error: "Expected SELECT, found GRANT",
    notes: "An embedded, single-user database in the page has no principals to grant to.",
  },
  {
    id: "select.tablesample",
    error: "Expected eof, found SYSTEM",
    notes: "TABLESAMPLE is not supported; ORDER BY RANDOM() LIMIT n samples rows.",
  },
  {
    id: "statement.explain",
    error: "Expected SELECT, found EXPLAIN",
    notes: "EXPLAIN is not supported as SQL; the explain() API renders the plan for a query.",
  },
  {
    id: "statement.multiple",
    error: "Run one SELECT statement at a time",
    notes: "One statement per execute() call; a script is split at its semicolons by the caller.",
  },
  {
    id: "statement.server-commands",
    error: "Expected SELECT, found VACUUM",
    notes:
      "VACUUM, ANALYZE, COMMENT ON, LISTEN, NOTIFY, and DISCARD are server commands with no meaning here; maintenance runs automatically and is driven from the API.",
  },
  {
    id: "subquery.array-constructor",
    error: "Expected )",
    notes:
      "ARRAY(subquery) is not supported; a scalar subquery with json_agg builds the same list as a JSON document.",
  },
  {
    id: "transaction.ddl-inside",
    error: "CREATE TABLE is not allowed inside a transaction",
    notes:
      "Schema statements are refused inside BEGIN … COMMIT: the catalog commits outside the scope, so a rollback could not take them back. Run DDL outside a transaction; migration tools that wrap migrations in one need that step split out.",
  },
  {
    id: "transaction.isolation-level",
    error:
      "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE is not supported: the engine has one isolation level",
    notes:
      "Every transaction reads one snapshot and commits atomically, which satisfies READ UNCOMMITTED, READ COMMITTED, and REPEATABLE READ, so those levels are accepted in SET TRANSACTION and BEGIN. SERIALIZABLE promises more than that and is refused rather than silently downgraded.",
  },
  {
    id: "transaction.lock-table",
    error: "Expected SELECT, found LOCK",
    notes:
      "LOCK TABLE is not supported; a single-session engine has no other session to lock against.",
  },
  {
    id: "type.array-any",
    error: "ANY/ALL take a subquery",
    notes:
      "PostgreSQL's ANY, ALL, and SOME accept an array operand as well as a subquery. Minnow implements only the subquery form, so the array variant is refused at compile time.",
  },
  {
    id: "type.array-any-parameter",
    error: "ANY/ALL take a subquery",
    notes: "= ANY(array) is not supported; use IN (…) with a list or a subquery.",
  },
  {
    id: "type.interval-arithmetic",
    error: "Date arithmetic requires a date or datetime value",
    notes:
      "PostgreSQL adds intervals into a combined interval and subtracts timestamps into an interval. Minnow's interval arithmetic only shifts a date or datetime by + or - INTERVAL, so an interval-valued result is refused.",
  },
  {
    id: "window.distinct-aggregate",
    error: "DISTINCT window aggregates are not supported",
    notes:
      "An aggregate used as a window function cannot take DISTINCT; PostgreSQL rejects this form too. DISTINCT aggregates work in grouped aggregation, so aggregate in a grouped block and window over that.",
  },
];
