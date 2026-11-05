# Lowering inventory (track B6, Phase 1)

What each construct of the existing front end becomes in the IR (`ir/ir.ts`), what is refused at
first, what the front end must change, and what lowering needs from other contracts.

Sources: `plan/model.ts` (`CompiledQuery`, `Expression`), `engine/query.ts` (`CompiledStatement`,
parser, shape passes), `engine/optimizer.ts` (rewrites, decorrelator), `engine/database.ts`
(`#applyCatalogRewrites`, statement execution), `engine/sql-semantics.ts`, `engine/sql-domains.ts`.

Status:

- **L** — lowered in Phase 2 as described.
- **U** — refused at first with `UnsupportedPlanError(construct)` (`lower/errors.ts`); the key is
  in parentheses.
- **R** — needs a contract change first (see "Contract requests"); lowered once it lands, refused
  with the key shown until then.

## Query blocks (`CompiledQuery`)

| Front-end construct                                                    | IR                                                                                                                         | Status               |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| `base` / join source naming a table                                    | `scan` (columns pruned to those referenced; `emitLocator` + `fetch` for late materialization when the cost model picks it) | L                    |
| equality on every primary-key or unique-index column                   | `keyLookup` (keys cast to the index column types; see "Keys")                                                              | L                    |
| range on a leading ordered-index column                                | index range scan                                                                                                           | R (IR 4)             |
| `derived` (derived table, inlined CTE, view body)                      | the subtree, its output renamed                                                                                            | L                    |
| `union` (`UNION`/`INTERSECT`/`EXCEPT` [`ALL`])                         | `setOp` (left-associative chain as the front end orders it)                                                                | L                    |
| `recursive` (WITH RECURSIVE)                                           | `recursive` + `workTableScan`                                                                                              | L                    |
| `recursive.series` (GENERATE_SERIES)                                   | `recursive` (a dedicated series source is a Phase 2 optimization)                                                          | L                    |
| `windowed` source + `WindowSpec`s                                      | `window` (partition/order keys projected first; frame offsets are constants)                                               | L                    |
| `joins[]` kind `inner`                                                 | `join` `inner`, `hash` on `left`/`right` keys, rest of `on` as `residual`; order chosen by the cost model                  | L                    |
| `crossJoinPlan` (comma join, CROSS JOIN)                               | `join` `cross` (or `inner` once WHERE supplies keys)                                                                       | L                    |
| `joins[]` kind `left`                                                  | `join` `left` (probe = preserved side)                                                                                     | L                    |
| RIGHT JOIN                                                             | already a mirrored `left` (front end)                                                                                      | L                    |
| `full: true`, or its UNION ALL desugar                                 | `join` `full`, or the `setOp` the front end built — never re-fused                                                         | L                    |
| `joins[]` kind `semi` / `anti` (decorrelated IN/EXISTS)                | `join` `semi` / `anti`                                                                                                     | L                    |
| decorrelated nullable `IN` needing three-valued marks                  | `join` `mark` (output: left columns + one bool, TRUE/FALSE/NULL)                                                           | L                    |
| `natural` / `using`                                                    | already expanded (`expandNaturalJoins`, `mergeJoinColumns`)                                                                | L                    |
| `inputHints` on a LEFT join                                            | conjuncts in the joined side's `scan.filter` (never evaluated as a predicate)                                              | L                    |
| `lateral` left correlated after `decorrelateLateralSources`            | relation-valued `apply`                                                                                                    | U (`lateral`)        |
| `predicates` (WHERE conjuncts)                                         | pushed into `scan.filter` where they read one scan, else `filter`; join keys become `join` keys                            | L                    |
| `groupBy` + aggregates                                                 | `project` (keys and arguments), `aggregate`                                                                                | L                    |
| `pendingSelectShape.groupingSets` / ROLLUP / CUBE                      | the front end's UNION ALL of grouped blocks with GROUPING literals → `setOp`                                               | L                    |
| SELECT DISTINCT                                                        | already a `groupBy` over the selected expressions → `aggregate` with no aggregates                                         | L                    |
| DISTINCT ON                                                            | the front end's window + filter → `window`, `filter`                                                                       | L                    |
| `having`                                                               | `filter` over the aggregate                                                                                                | L                    |
| `orderBy`                                                              | `sort` (keys projected first; `MINNOW_COLLATE` → `SortKey.collation`)                                                      | L                    |
| `limit` / `offset` (constants or `limitParameter` / `offsetParameter`) | `sort.limit` (top-N) or `limit`, `Bound` const or param                                                                    | L                    |
| `limitWithTies`                                                        | `sort.withTies`                                                                                                            | L                    |
| `usesStatementDatetime`                                                | `PhysicalPlan.clockSlot`                                                                                                   | L                    |
| `usesVolatileFunctions`, `usesSequenceCalls`                           | `PhysicalPlan.volatile`; every `apply` over them `memoize: false`                                                          | L                    |
| FROM-less SELECT (`DUAL_TABLE`)                                        | `values` with one empty row                                                                                                | L                    |
| JSON_TABLE (constant document)                                         | already rows at parse time → `values`                                                                                      | L                    |
| `fts` (`@@`, BM25)                                                     | none                                                                                                                       | U (`fullTextSearch`) |

## Expressions (`Expression`)

| Front-end construct                                                                                             | IR                                                                                                                                                                                                                                | Status                   |
| --------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ |
| `literal`                                                                                                       | `literal` (integer: int4, else int8, else numeric, by magnitude; `exactText`/`decimal` → numeric with its scale; `timestamp`/`zoned` → timestamp(tz); `sqlDomain` → that type; string → PostgreSQL's `unknown`, typed by context) | L                        |
| `parameter`                                                                                                     | `param` slot `index`; type from its position (or `parameterTypes`), `text` when nothing fixes it                                                                                                                                  | L                        |
| `column`                                                                                                        | `column` by position                                                                                                                                                                                                              | L                        |
| `binary` (`+ - * / % ‖`)                                                                                        | `call` (`add`, `sub`, `mul`, `div`, `mod`, `concat`) resolved by operand types; `integer` flag ignored: integer `/` falls out of int types                                                                                        | L                        |
| `call` scalar function                                                                                          | `call` with the PostgreSQL function name, overload resolved                                                                                                                                                                       | L                        |
| `CAST`                                                                                                          | `cast` to the target type (`declaredTypeName` for numeric targets, C5)                                                                                                                                                            | L                        |
| `MINNOW_COLLATE`                                                                                                | the collation of the enclosing `compare` / `inList` / `SortKey`                                                                                                                                                                   | L                        |
| `MINNOW_TIMESTAMP_TEXT`, `MINNOW_TO_CHAR_LOCAL`                                                                 | dropped: `timestamp(zoned: false)` already renders without a zone                                                                                                                                                                 | L                        |
| `MINNOW_JSON_GET(_TEXT)`, `MINNOW_ARRAY_*`, `MINNOW_REGEX_MATCH`, `ARRAY`                                       | `call` (`json_get`, `json_get_text`, `array_at`…, `regex_match`)                                                                                                                                                                  | L                        |
| `MINNOW_TUPLE_KEY` (decorrelation)                                                                              | several `join` keys (one per tuple member, `nullSafe` per the decorrelator)                                                                                                                                                       | L                        |
| `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIME`                                                                | casts of the clock `param`                                                                                                                                                                                                        | L                        |
| `NEXTVAL`, `CURRVAL` (sequence name resolved from the catalog)                                                  | `call` `nextval` / `currval` with the sequence id                                                                                                                                                                                 | L                        |
| `RANDOM`, `GEN_RANDOM_UUID`                                                                                     | `call`; sets `volatile`                                                                                                                                                                                                           | L                        |
| aggregate `call` (COUNT, SUM, AVG, MIN, MAX, STRING_AGG, JSON_ARRAYAGG, percentiles, MODE, MINNOW_SINGLE_VALUE) | `AggregateCall` (`distinct`, `filter`, `orderBy`); SUM(int2/int4) → int64, SUM(int8) → decimal(null, 0), AVG(int/float) → float64 (C11)                                                                                           | L                        |
| `window`                                                                                                        | `WindowCall` in a `window` operator (the front end already moved them into `windowed` sources)                                                                                                                                    | L                        |
| `condition` comparisons, `IS [NOT] DISTINCT FROM`, `IS [NOT] NULL`, `IS TRUE`                                   | `compare`, `distinctFrom`, `isNull`, `call` `is_true`                                                                                                                                                                             | L                        |
| `condition` `IN` / `NOT IN` list                                                                                | `inList`                                                                                                                                                                                                                          | L                        |
| `condition` `= ANY(array)` / `<> ALL(array)`                                                                    | `call` `any_array` / `all_array` (three-valued)                                                                                                                                                                                   | L                        |
| `condition` LIKE / ILIKE / SIMILAR TO                                                                           | `like` (`escape`)                                                                                                                                                                                                                 | L                        |
| `logical`, `not`, `case`, `list`                                                                                | `and`/`or` (flattened), `not`, `case`, `inList` items                                                                                                                                                                             | L                        |
| uncorrelated `subquery` (scalar)                                                                                | `initPlans` slot + `param`                                                                                                                                                                                                        | L                        |
| uncorrelated `IN (subquery)` / `exists`                                                                         | `join` `semi` / `anti` / `mark`, or an init plan when the decorrelator kept it in SELECT                                                                                                                                          | L                        |
| correlated `subquery` / `exists` / quantified, left by the decorrelator                                         | `apply` (`scalar` / `exists` / `quantified`), bindings into slots                                                                                                                                                                 | L                        |
| correlated subquery elsewhere (inside an aggregate argument, ORDER BY of a window)                              | none                                                                                                                                                                                                                              | U (`correlatedSubquery`) |
| a function the kernels do not provide yet                                                                       | none                                                                                                                                                                                                                              | U (`function`)           |

## Statements (`CompiledStatement`)

| Statement                                                     | Lowered to                                                                                                                                                                                                              | Status                                            |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| `select`                                                      | `LoweredQuery`; `pointRead` when the only data access is `keyLookup`                                                                                                                                                    | L                                                 |
| `insert` VALUES / DEFAULT VALUES                              | `values` (each `DEFAULT` and omitted column from `lowerTableRules().defaults`, `nextval` included) → `project` to catalog order, generated columns computed, casts to column types → `modify` `insert`                  | L                                                 |
| `insert` … SELECT                                             | the query subtree → same `project` → `modify` `insert`                                                                                                                                                                  | L                                                 |
| `insert` ON CONFLICT DO NOTHING                               | `modify` `upsert`, action `nothing`, arbiters = the unique index whose columns equal the target (PostgreSQL's inference; none: 42P10), or every unique index for `anyTarget`                                            | R (IR 1)                                          |
| `insert` ON CONFLICT DO UPDATE (`replace`, `update`, `where`) | `modify` `upsert`, assignments over (existing row ++ EXCLUDED row), `onRepeat: "error"`                                                                                                                                 | R (IR 1, 5)                                       |
| `insert` RETURNING (`*`, names, `returningItems`)             | `modify.returning` over the written row                                                                                                                                                                                 | L                                                 |
| `update` (assignments, predicates)                            | `scan` (target, `emitLocator`) → `filter` → `project` (locator, assigned values, generated columns depending on them) → `modify` `update`, `assigned` = assigned columns + those generated columns, `onRepeat: "error"` | L                                                 |
| `update` SET col = DEFAULT                                    | the column's default expression                                                                                                                                                                                         | L                                                 |
| `update` … FROM                                               | target ⋈ sources (inner, cost-ordered) → same → `onRepeat: "first"` (first match wins, as today)                                                                                                                        | L                                                 |
| `update` / `delete` RETURNING reading FROM columns            | `modify.returning` over the table row ++ passthrough input columns                                                                                                                                                      | R (IR 5)                                          |
| `delete` (predicates), TRUNCATE                               | `scan` (`emitLocator`) → `filter` → `project` (locator) → `modify` `delete`                                                                                                                                             | L                                                 |
| `delete` … USING                                              | target ⋈ sources → `onRepeat: "first"`                                                                                                                                                                                  | L                                                 |
| `merge`                                                       | `modify` `merge`                                                                                                                                                                                                        | U (`merge`), R (IR 2)                             |
| `create-table`                                                | `CatalogChange` `createTable`: types via `declaredColumnType` (lower/columns.ts), defaults/CHECK/generated as normalized SQL text, PK/UNIQUE/FK by column position                                                      | L (R for SERIAL and self-references: access 1, 2) |
| `create-table-as`                                             | `createTable` (types of the lowered query) + `followUp` INSERT … SELECT                                                                                                                                                 | L                                                 |
| `add-column`, `rename-column`, `drop-column`                  | `alterTable` with the table's next definition                                                                                                                                                                           | L                                                 |
| `create-index`, `drop-index`                                  | `createIndex` (role `secondary`; `unique` → role `unique`) / `dropIndex` (looked up by name)                                                                                                                            | L                                                 |
| `drop-table`                                                  | `dropTable`                                                                                                                                                                                                             | L                                                 |
| `create-sequence`                                             | `createSequence` (start 1, increment 1, int8 bounds, no cycle: what the front end parses)                                                                                                                               | L                                                 |
| `create-view`, `drop-view`                                    | `createView` (body SQL) / `dropView`; OR REPLACE → drop + create in one statement                                                                                                                                       | L                                                 |
| `create-enum`                                                 | none (the catalog has no types)                                                                                                                                                                                         | U (`enumType`), R (access 3)                      |
| `create-trigger`, `drop-trigger`                              | none (the catalog has no triggers)                                                                                                                                                                                      | U (`trigger`)                                     |
| `transaction`                                                 | `LoweredTransactionControl`                                                                                                                                                                                             | L                                                 |
| `set`, `show`                                                 | `LoweredUtility`                                                                                                                                                                                                        | L                                                 |

IF [NOT] EXISTS is decided at lowering against the catalog version, so a no-op DDL lowers to
`change: null`. The plan is valid only at that version, so this is exact.

## Rules that cross tracks

- **Keys.** `keyLookup` keys are cast to the index column types. A constant that does not fit
  the column type (`int4_col = 5000000000`) makes the conjunct false (an empty lookup), as
  PostgreSQL's cross-type comparison does; a placeholder takes the column type by position, so
  an out-of-range value is a bind error, as in PostgreSQL.
- **Ranges** use an index only when it is `ordered` (never an enum's `equality` index) and the
  comparison's collation is the default byte order (null or `"C"`, C8). Anything else is a scan
  with a filter, pruned by page bounds.
- **Defaults and generated columns** are filled into every insert plan by lowering (ir/rules.ts);
  the write path receives complete rows (access/modify.ts). For UPDATE, generated columns that
  read an assigned column are recomputed in the plan and added to `assigned`.
- **Result descriptors** (lower/result.ts): `sqlType` as PostgreSQL prints it; int64 renders
  `bigint` except COUNT, SUM(int2/int4), ROW_NUMBER, RANK, DENSE_RANK (`number`); SUM(int8) is
  `decimal(null, 0)` rendered `bigint` (D2, R16).
- **Costs.** Join order from `ExecutorCosts` (ir/costs.ts) × statistics (lower/statistics.ts):
  every left-deep connected order up to 8 relations, greedy above; outer, semi, anti, lateral and
  volatile joins keep the front end's relative order.
- **Re-planning.** Owner cache key (text, catalog version); `statisticsDrifted(basis)` on each hit.

## Front-end changes

### Done in this commit: declared numeric widths (C5)

- `compileWithDeclaredTypes(compile)` (engine/query.ts): inside it, numeric column definitions
  (CREATE TABLE, ADD COLUMN, JSON_TABLE COLUMNS) and numeric CAST / `::` targets (including
  `$1::int2[]`) carry `declaredTypeName`, PostgreSQL's canonical name: `smallint`, `integer`,
  `bigint`, `real`, `double precision`. SERIAL / BIGSERIAL / SMALLSERIAL → integer / bigint /
  smallint; `FLOAT(p)` → `real` for p ≤ 24, else `double precision`.
- Outside the scope, which is every compile the current engine does, the parser output is
  unchanged: no field is added. A field the engine merely ignored would not have been enough:
  its query identity and the vector binder's expression signatures are structural, so
  `SELECT CAST(a AS SMALLINT) … GROUP BY CAST(a AS BIGINT)` (accepted today) would have failed
  with "must appear in GROUP BY". Under the scope that difference is intended: PostgreSQL treats
  those as different expressions.
- Proof: `engine/declared-types.test.ts` (names under the scope; no field outside it; no stored
  field; identical results, including that GROUP BY case, which also passes on the parent
  commit), and the front end's suites, unchanged and green: `query`, `optimizer`, `decorrelate`,
  `postgres-sql-surface`, `postgres-sql-expressions`, `sql-conformance`, `sql-statements`,
  `sql-semantics`, `sql-mutation-conformance`, `query-cache`, `schema` (12 files, 460 tests).
- Cost: the shipped parser grows by the name table and the scope (main entry about +0.6 KiB raw,
  +0.2 KiB gzip; tarball +588 bytes). The bundle and tarball ratchets in `tree-shaking.test.ts`
  and `scripts/package-shape.test.ts` record the new measurements.

### Still needed (Phase 2, each behind the same scope or behaviour-neutral)

1. **Access mode.** `BEGIN READ ONLY` / `START TRANSACTION READ ONLY` parse and are discarded;
   the transaction statement must keep `readOnly` (R11.2).
2. **Exports for the passes lowering re-hosts.** `expandQualifiedWildcards`,
   `expandDistinctWildcard` and `validateGrouping` are private to engine/query.ts, and
   `normalizePlanDomainLiterals` to engine/database.ts. Export them as one
   `prepareColumnarShape(plan, columnsOf)` beside `createPreparedColumnarQuery`, which then calls
   it (no behaviour change), and move the domain-literal pass into engine/sql-domains.ts.
3. **Sequence names.** `#resolveSequenceCalls` is private to the old database; lowering resolves
   `nextval('name')` from `CatalogView.sequences` itself. No front-end change.
4. **Array element names.** `physicalTypeOfName` (types.ts) lacks spellings the parser accepts as
   array elements: `FLOAT`, `CHAR`, `CHARACTER`, `STRING`, `DATETIME`. Until added, such columns
   are `UnsupportedPlanError("columnType")`.

## Contract requests

### IR (`ir/ir.ts`; A1 and B4)

1. **Arbiters.** `modify.onConflict.indexId: number` → `arbiters: readonly number[]` (empty = every
   unique index, bare DO NOTHING), matching access/ `TableWriter.conflicts(batch, arbiters, out)`.
2. **MERGE.** A `merge` modify kind: input = source LEFT JOIN target (target locator nullable);
   `actions`, tried in order per row: `{ when: "matched" | "notMatched"; condition; action:
update(assignments) | delete | insert(values) | nothing }`; `onRepeat: "error"` (SQLSTATE 21000).
   The executor holds one writer per action kind (access/ allows several).
3. **Client rows.** An `input` relation (`{ op: "input"; output }`): the rows of a batch write,
   supplied with the execution. Lets `lowerWrite` reuse the insert/upsert/update/delete plans
   (defaults, generated columns, casts, RETURNING) instead of a second write path.
4. **Index range scan.** `{ op: "indexRange"; tableId; indexId; lower, upper: { keys:
ScalarExpr[]; inclusive } | null; columns; emitLocator }` over an `ordered` index.
5. **Positions in `modify`.** State what `onConflict.assignments`/`where` and `returning` read:
   assignments and WHERE over (existing row ++ proposed EXCLUDED row), each in catalog order;
   RETURNING over the affected row (new for insert/update/upsert, old for delete) ++ any trailing
   passthrough input columns, which UPDATE … FROM / DELETE … USING need to return source columns.
   The update input becomes: locator, assigned columns, then passthrough columns.
6. **Mark join output.** State that `join` `mark` outputs the left columns plus one bool column.

### Access (`access/catalog.ts`; B3)

1. **Owned sequences.** `TableInput` column `identity: { start, increment } | null`: the write
   path creates the owned sequence with the table and sets `identitySequence` and `ownedBy`.
   Lowering cannot: the sequence must exist before the table and own a column id assigned later.
2. **Self-referencing foreign keys.** `TableInput.foreignKeys[].parentTableId` accepts `"self"`
   (CREATE TABLE t (…, parent REFERENCES t)), since the table id is assigned by the change.
3. **Enum types.** CREATE TYPE … AS ENUM needs a catalog object (`TypeDef { id, name, labels }`,
   with `createType`/`dropType` changes) so a later CREATE TABLE can name it.
4. **Planning statistics.** A synchronous per-table summary at the current root (live row count;
   per column null count, bounds, distinct estimate, average width), maintained at flush and
   compaction, for the owner's `PlanningStatistics` (lower/statistics.ts).

### Executor (`exec/`; B4)

1. Import `TableRuleExpressions` from ir/rules.ts (now with `defaults`) and `ExecutorCosts` from
   ir/costs.ts, deleting the copies in exec/rules.ts and exec/executor.ts: lower/ may not import
   exec/.
2. Kernel signatures: lowering resolves overloads against the executor's kernel table. Needed as
   data lowering can import (proposed: `ir/functions.ts`, a table of `fn` × argument kinds →
   result kind, owned by B4).

### Owner (`owner/`; B5)

1. `WireColumnMeta` is `ResultColumn` (lower/result.ts); `render` may be `"bigint"` on a
   `decimal(null, 0)` column (SUM(int8)).
2. `PlannedStatement` comes from `LoweredStatement` (kind, columns, parameters,
   `controlsTransaction`, `catalogVersion`); `readSet` is `reads`.

### Contracts (`next/errors.ts`; A1)

1. One `UnsupportedPlanError` for lowering and the executor (`code` MINNOW_UNSUPPORTED_PLAN,
   0A000, `stage`, `construct`).

## Open questions

1. **Arithmetic over counts.** `COUNT(*) + 1` and `COUNT(*) * 100 / total` are int64 and render
   `bigint` under the literal D2 rule. Keep that, or let `number` propagate through integer
   arithmetic whose operands are all `number`-rendered (still exact-or-RangeError)?
2. **SUM(int8)** as `decimal(null, 0)` rendered `bigint` (exact past 2^63, PostgreSQL's NUMERIC),
   or `int64` with an overflow error (22003)? Lowering assumes the first.
3. **Schema DSL.** `column.integer()` and `column.number()` declare no width, so
   `requireDeclaredWidths` refuses them. Map them to INTEGER and DOUBLE PRECISION (and add
   `column.bigint()` / `smallint()` / `real()`), or keep the int64 fallback for `integer()`?
4. **Text widths.** VARCHAR(n) and CHAR(n) print `text` (`sqlType`); keeping `character
varying(n)` needs the width carried like numeric names. Worth it?
