# Lowering inventory (track B6, Phase 1)

What each construct of the existing front end becomes in the IR (`ir/ir.ts`), what is refused at
first, what the front end must change, and what lowering needs from other contracts.

Sources: `plan/model.ts` (`CompiledQuery`, `Expression`), `engine/query.ts` (`CompiledStatement`,
parser, shape passes), `engine/optimizer.ts` (rewrites, decorrelator), `engine/database.ts`
(`#applyCatalogRewrites`, statement execution), `engine/sql-semantics.ts`, `engine/sql-domains.ts`.

Status:

- **M1** — lowered now (Phase 2 increment 1), covered by `lowerer.test.ts`, the differential
  test against the current engine (`testing/lowering-differential.test.ts`) and the PostgreSQL
  oracle (`testing/lowering-pglite.test.ts`, one case per finding of review B6-p2a); every plan
  those run is type-checked (`testing/ir-check.ts`).
- **M3** — lowered now (Phase 2 increment 2), covered by `from.test.ts` (join order, build
  sides, access paths), the differential test, the PostgreSQL oracle
  (`testing/lowering-pglite-m3.test.ts`) and the plan type checker.
- **L** — lowered in a later increment as described; refused until then with
  `UnsupportedPlanError` (stage "lower") and the construct key in parentheses.
- **U** — refused at first with `UnsupportedPlanError(construct)` (`lower/errors.ts`).
- **R** — needs a contract change first (see "Contract requests"); refused with the key shown
  until it lands.

## Query blocks (`CompiledQuery`)

| Front-end construct                                                    | IR                                                                                                                                                                                                                                                                                               | Status               |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------- |
| `base` / join source naming a table                                    | `scan` (columns pruned to those referenced; `emitLocator` + `fetch` for late materialization when the cost model picks it)                                                                                                                                                                       | M1                   |
| equality on every primary-key or unique-index column                   | `keyLookup` (keys cast to the index column types; see "Keys")                                                                                                                                                                                                                                    | M1                   |
| range on a leading ordered-index column                                | `indexRange`: equality on a prefix plus one range on the next column, under byte-order collation; chosen when the estimated fraction read is at most 5% (`INDEX_RANGE_MAX_FRACTION`), else a scan with a filter                                                                                  | M3                   |
| `derived` (derived table, inlined CTE, view body)                      | the subtree, its output renamed                                                                                                                                                                                                                                                                  | M1                   |
| `union` (`UNION`/`INTERSECT`/`EXCEPT` [`ALL`])                         | `setOp` (left-associative chain as the front end orders it)                                                                                                                                                                                                                                      | M3                   |
| `recursive` (WITH RECURSIVE)                                           | `recursive` + `workTableScan`                                                                                                                                                                                                                                                                    | M3                   |
| `recursive.series` (GENERATE_SERIES)                                   | `series` (original argument arity and typed start/stop/step)                                                                                                                                                                                                                                     | M3                   |
| `windowed` source + `WindowSpec`s                                      | `window`, one per distinct PARTITION BY / ORDER BY (partition, order and argument expressions are columns of the inner block); frame offsets are constants                                                                                                                                       | M3                   |
| `joins[]` kind `inner`                                                 | `join` `inner`, `hash` on `left`/`right` keys, rest of `on` as `residual`; order chosen by the cost model                                                                                                                                                                                        | M3                   |
| `crossJoinPlan` (comma join, CROSS JOIN)                               | `join` `cross` (or `inner` once WHERE supplies keys)                                                                                                                                                                                                                                             | M3                   |
| `joins[]` kind `left`                                                  | `join` `left` (probe = preserved side)                                                                                                                                                                                                                                                           | M3                   |
| RIGHT JOIN                                                             | already a mirrored `left` (front end)                                                                                                                                                                                                                                                            | M3                   |
| `full: true`, or its UNION ALL desugar                                 | `join` `full`, or the `setOp` the front end built — never re-fused                                                                                                                                                                                                                               | M3                   |
| `joins[]` kind `semi` / `anti` (decorrelated IN/EXISTS)                | `join` `semi` / `anti`                                                                                                                                                                                                                                                                           | M3                   |
| decorrelated nullable `IN` needing three-valued marks                  | `join` `mark` (output: left columns + one bool); the subquery's WHERE, correlated conditions included, goes in `join.filter`, only the comparison in `residual` (R20.5). The decorrelator rewrites correlated `IN` into joins first, so in practice only uncorrelated `IN` / `NOT IN` reaches it | M3                   |
| `natural` / `using`                                                    | already expanded (`expandNaturalJoins`, `mergeJoinColumns`)                                                                                                                                                                                                                                      | M3                   |
| `inputHints` on a LEFT join                                            | not used: the hint only narrows what the joined side reads, so that side is read with its own conditions alone                                                                                                                                                                                   | M3 (ignored)         |
| `lateral` left correlated after `decorrelateLateralSources`            | relation-valued `apply`                                                                                                                                                                                                                                                                          | U (`lateral`)        |
| `predicates` (WHERE conjuncts)                                         | pushed into `scan.filter` where they read one scan, else `filter`; join keys become `join` keys                                                                                                                                                                                                  | M1                   |
| `groupBy` + aggregates                                                 | `project` (keys and arguments), `aggregate`                                                                                                                                                                                                                                                      | M1                   |
| `pendingSelectShape.groupingSets` / ROLLUP / CUBE                      | the front end's UNION ALL of grouped blocks with GROUPING literals → `setOp`                                                                                                                                                                                                                     | M3                   |
| SELECT DISTINCT                                                        | already a `groupBy` over the selected expressions → `aggregate` with no aggregates                                                                                                                                                                                                               | M1                   |
| DISTINCT ON                                                            | the front end's window + filter → `window`, `filter`                                                                                                                                                                                                                                             | M3                   |
| `having`                                                               | `filter` over the aggregate                                                                                                                                                                                                                                                                      | M1                   |
| `orderBy`                                                              | `sort` (keys projected first; `MINNOW_COLLATE` → `SortKey.collation`)                                                                                                                                                                                                                            | M1                   |
| `limit` / `offset` (constants or `limitParameter` / `offsetParameter`) | `sort.limit` (top-N) or `limit`, `Bound` const or param                                                                                                                                                                                                                                          | M1                   |
| `limitWithTies`                                                        | `sort.withTies`                                                                                                                                                                                                                                                                                  | M1                   |
| `usesStatementDatetime`                                                | `PhysicalPlan.clockSlot`                                                                                                                                                                                                                                                                         | M1                   |
| `usesVolatileFunctions`, `usesSequenceCalls`                           | `PhysicalPlan.volatile`; every `apply` over them `memoize: false`                                                                                                                                                                                                                                | M1                   |
| FROM-less SELECT (`DUAL_TABLE`)                                        | `values` with one empty row                                                                                                                                                                                                                                                                      | M1                   |
| JSON_TABLE (constant document)                                         | already rows at parse time → `values`                                                                                                                                                                                                                                                            | M1                   |
| `fts` (`@@`, BM25)                                                     | none                                                                                                                                                                                                                                                                                             | U (`fullTextSearch`) |

## Expressions (`Expression`)

| Front-end construct                                                                                             | IR                                                                                                                                                                                                                                                                     | Status                                                                                                          |
| --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `literal`                                                                                                       | `literal` (integer: int4, else int8, else numeric, by magnitude; `exactText`/`decimal` → numeric with its scale; `timestamp`/`zoned` → timestamp(tz); `sqlDomain` → that type; string → PostgreSQL's `unknown`, typed by context)                                      | M1                                                                                                              |
| `parameter`                                                                                                     | `param` slot `index`; type from its position (or `parameterTypes`), `text` when nothing fixes it                                                                                                                                                                       | M1                                                                                                              |
| `column`                                                                                                        | `column` by position                                                                                                                                                                                                                                                   | M1                                                                                                              |
| `binary` (`+ - * / % ‖`)                                                                                        | `call` (`add`, `sub`, `mul`, `div`, `mod`, `concat`) resolved by operand types; `integer` flag ignored: integer `/` falls out of int types                                                                                                                             | M1                                                                                                              |
| `call` scalar function                                                                                          | `call` with the PostgreSQL function name, overload resolved                                                                                                                                                                                                            | M1                                                                                                              |
| `CAST`                                                                                                          | `cast` to the target type (`declaredTypeName` for numeric targets, C5)                                                                                                                                                                                                 | M1                                                                                                              |
| `MINNOW_COLLATE`                                                                                                | the collation of the enclosing `compare` / `inList` / `SortKey`                                                                                                                                                                                                        | M1                                                                                                              |
| `MINNOW_TIMESTAMP_TEXT`, `MINNOW_TO_CHAR_LOCAL`                                                                 | dropped: `timestamp(zoned: false)` already renders without a zone                                                                                                                                                                                                      | M1                                                                                                              |
| `MINNOW_JSON_GET(_TEXT)`                                                                                        | `call` (`json_get`, `json_get_text`), typed JSON/JSONB extraction; source-bounded raw JSON and checkpointed JSONB project/filter execution                                                                                                                             | M1                                                                                                              |
| `MINNOW_ARRAY_*`, `MINNOW_REGEX_MATCH`, `ARRAY`                                                                 | `call` (`array_at`…, `regex_match`)                                                                                                                                                                                                                                    | L                                                                                                               |
| `MINNOW_TUPLE_KEY` (decorrelation)                                                                              | several `join` keys (one per tuple member, `nullSafe` per the decorrelator)                                                                                                                                                                                            | L                                                                                                               |
| `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIME`                                                                | casts of the clock `param`                                                                                                                                                                                                                                             | M1                                                                                                              |
| `NEXTVAL`, `CURRVAL` (sequence name resolved from the catalog)                                                  | `call` `nextval` / `currval` with the sequence id                                                                                                                                                                                                                      | M1                                                                                                              |
| `RANDOM`, `GEN_RANDOM_UUID`                                                                                     | `call`; sets `volatile`                                                                                                                                                                                                                                                | M1                                                                                                              |
| aggregate `call` (COUNT, SUM, AVG, MIN, MAX, STRING_AGG, JSON_ARRAYAGG, percentiles, MODE, MINNOW_SINGLE_VALUE) | `AggregateCall` (`distinct`, `filter`, `orderBy`); SUM(int2/int4) → int64, SUM(int8) → decimal(null, 0), AVG(int/float) → float64 (C11)                                                                                                                                | M1 (scalar-fraction numeric ordered sets supported; interval interpolation and JSON aggregates: L, `aggregate`) |
| `window`                                                                                                        | `WindowCall` in a `window` operator (the front end already moved them into `windowed` sources)                                                                                                                                                                         | M3                                                                                                              |
| `condition` comparisons, `IS [NOT] DISTINCT FROM`, `IS [NOT] NULL`, `IS TRUE`                                   | `compare`, `distinctFrom`, `isNull`; `IS TRUE` is `distinctFrom` (negated, literal true)                                                                                                                                                                               | M1                                                                                                              |
| `condition` `IN` / `NOT IN` list                                                                                | `inList`                                                                                                                                                                                                                                                               | M1                                                                                                              |
| `condition` `= ANY(array)` / `<> ALL(array)`                                                                    | `inList` for an `ARRAY[…]` of constants; `call` `any_array` / `all_array` (three-valued) for an array value                                                                                                                                                            | M3 for `ARRAY[…]`; L (`function`) for array values                                                              |
| `condition` LIKE / ILIKE / SIMILAR TO                                                                           | `like` (`escape`)                                                                                                                                                                                                                                                      | M1                                                                                                              |
| `logical`, `not`, `case`, `list`                                                                                | `and`/`or` (flattened), `not`, `case`, `inList` items                                                                                                                                                                                                                  | M1                                                                                                              |
| uncorrelated `subquery` (scalar)                                                                                | `initPlans` slot + `param` (a `single_value` aggregate raises 21000 on a second row)                                                                                                                                                                                   | M3                                                                                                              |
| uncorrelated `IN (subquery)` / `exists`                                                                         | `join` `semi` / `anti` / `mark`; an init plan where no join can attach (HAVING, grouped select lists, DML expressions)                                                                                                                                                 | M3                                                                                                              |
| correlated `subquery` / `exists` / quantified, left by the decorrelator                                         | `apply` (`scalar` / `exists` / `quantified`), bindings into slots, `memoize` unless volatile. The decorrelator turns nearly all of them into joins first, and the front end refuses the shapes it cannot (see "Still needed" 4), so `apply` is rarely reached from SQL | M3                                                                                                              |
| correlated subquery elsewhere (inside an aggregate argument, ORDER BY of a window)                              | none                                                                                                                                                                                                                                                                   | U (`correlatedSubquery`)                                                                                        |
| a function the kernels do not provide yet                                                                       | none                                                                                                                                                                                                                                                                   | U (`function`)                                                                                                  |

## Statements (`CompiledStatement`)

| Statement                                                     | Lowered to                                                                                                                                                                                                              | Status                                      |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| `select`                                                      | `LoweredQuery`; `pointRead` when the only data access is `keyLookup`                                                                                                                                                    | M1                                          |
| `insert` VALUES / DEFAULT VALUES                              | `values` (each `DEFAULT` and omitted column from `lowerTableRules().defaults`, `nextval` included) → `project` to catalog order, generated columns computed, casts to column types → `modify` `insert`                  | M1                                          |
| `insert` … SELECT                                             | the query subtree → same `project` → `modify` `insert`                                                                                                                                                                  | M1                                          |
| `insert` ON CONFLICT DO NOTHING                               | `modify` `upsert`, action `nothing`, arbiters = the unique index whose columns equal the target (PostgreSQL's inference; none: 42P10), or every unique index for `anyTarget`                                            | M1 (IR additions landed)                    |
| `insert` ON CONFLICT DO UPDATE (`replace`, `update`, `where`) | `modify` `upsert`, assignments over (existing row ++ EXCLUDED row), `onRepeat: "error"`; an unqualified name is ambiguous (42702); generated columns read the materialized assignments                                  | M1 (IR additions landed)                    |
| `insert` RETURNING (`*`, names, `returningItems`)             | `modify.returning` over the written row                                                                                                                                                                                 | M1                                          |
| `update` (assignments, predicates)                            | `scan` (target, `emitLocator`) → `filter` → `project` (locator, assigned values, generated columns depending on them) → `modify` `update`, `assigned` = assigned columns + those generated columns, `onRepeat: "error"` | M1                                          |
| `update` SET col = DEFAULT                                    | the column's default expression                                                                                                                                                                                         | M1                                          |
| `update` … FROM                                               | target ⋈ sources (inner, cost-ordered) → same → `onRepeat: "first"` (first match wins, as today)                                                                                                                        | M3                                          |
| `update` / `delete` RETURNING reading FROM columns            | `modify.returning` over the table row ++ passthrough input columns (lowering builds it)                                                                                                                                 | front end refuses it (see "Still needed")   |
| `delete` (predicates)                                         | `scan` (`emitLocator`) → `filter` → `project` (locator) → `modify` `delete`                                                                                                                                             | M1                                          |
| TRUNCATE                                                      | `modify` `truncate` (command tag TRUNCATE); refused (0A000) when another table's foreign key references it                                                                                                              | M1                                          |
| `delete` … USING                                              | target ⋈ sources → `onRepeat: "first"`                                                                                                                                                                                  | M3                                          |
| `merge`                                                       | `modify` `merge`: target LEFT JOIN source (source table, view or query), actions in order, each reading (source ++ locator ++ target row)                                                                               | M3                                          |
| `create-table`                                                | `CatalogChange` `createTable`: types via `declaredColumnType` (lower/columns.ts), defaults/CHECK/generated as normalized SQL text, PK/UNIQUE/FK by column position                                                      | M1 (SERIAL, self-references: R, access 1–2) |
| `create-table-as`                                             | `createTable` (types of the lowered query) + `followUp` INSERT … SELECT; WITH NO DATA has no follow-up                                                                                                                  | M1                                          |
| `add-column`, `rename-column`, `drop-column`                  | `alterTable` with the table's next definition                                                                                                                                                                           | M1                                          |
| `create-index`, `drop-index`                                  | `createIndex` (role `secondary`; `unique` → role `unique`) / `dropIndex` (looked up by name)                                                                                                                            | M1                                          |
| `drop-table`                                                  | `dropTable`                                                                                                                                                                                                             | M1                                          |
| `create-sequence`                                             | `createSequence` (start 1, increment 1, int8 bounds, no cycle: what the front end parses)                                                                                                                               | M1                                          |
| `create-view`, `drop-view`                                    | `createView` (body SQL) / `dropView`; OR REPLACE → drop + create in one statement                                                                                                                                       | M1                                          |
| `create-enum`                                                 | none (the catalog has no types)                                                                                                                                                                                         | U (`enumType`), R (access 3)                |
| `create-trigger`, `drop-trigger`                              | none (the catalog has no triggers)                                                                                                                                                                                      | U (`trigger`)                               |
| `transaction`                                                 | `LoweredTransactionControl`                                                                                                                                                                                             | M1                                          |
| `set`, `show`                                                 | `LoweredUtility`                                                                                                                                                                                                        | M1                                          |

IF [NOT] EXISTS is decided at lowering against the catalog version, so a no-op DDL lowers to
`change: null`. The plan is valid only at that version, so this is exact.

## Rules that cross tracks

- **Keys.** `keyLookup` keys are cast to the index column types. A constant that does not fit
  the column type (`int4_col = 5000000000`) makes the conjunct false (an empty lookup), as
  PostgreSQL's cross-type comparison does; a placeholder takes the column type by position, so
  an out-of-range value is a bind error, as in PostgreSQL.
- **Ranges** use an index only when it is `ordered` (never an enum's `equality` index) and the
  comparison's collation is the default byte order (null or `"C"`, C8). Anything else is a scan
  with a filter, pruned by page bounds. The fraction read comes from the column's bounds
  (linear interpolation) when statistics have them, else PostgreSQL's defaults: 0.005 for `=`,
  1/3 for one inequality, 0.005 for a closed range.
- **Defaults and generated columns** are filled into every insert plan by lowering (ir/rules.ts);
  the write path receives complete rows (access/modify.ts). For UPDATE, generated columns that
  read an assigned column are recomputed in the plan and added to `assigned`.
- **Result descriptors** (lower/result.ts): `sqlType` as PostgreSQL prints it; int64 renders
  `bigint` except COUNT, SUM(int2/int4), ROW_NUMBER, RANK, DENSE_RANK (`number`); SUM(int8) is
  `decimal(null, 0)` rendered `bigint` (D2, R16).
- **Costs.** Join order from `ExecutorCosts` (ir/costs.ts) × statistics (lower/statistics.ts):
  every left-deep connected order up to 8 relations, greedy above; outer, semi and anti joins
  keep the front end's relative order. An equality join keeps `L × R / max(NDV)` rows, where a
  unique key's NDV is its table's rows and a column without statistics has 200 (PostgreSQL's
  default), which never outweighs a known count. The smaller side is the hash build side.
- **Re-planning.** Owner cache key (text, catalog version); `statisticsDrifted(basis)` on each hit.

## Front-end changes

### Done: declared numeric widths (C5) and character widths (R17.4)

- `compileWithDeclaredTypes(compile)` (engine/query.ts): inside it, numeric column definitions
  (CREATE TABLE, ADD COLUMN, JSON_TABLE COLUMNS) and numeric CAST / `::` targets (including
  `$1::int2[]`) carry `declaredTypeName`, PostgreSQL's canonical name: `smallint`, `integer`,
  `bigint`, `real`, `double precision`. SERIAL / BIGSERIAL / SMALLSERIAL → integer / bigint /
  smallint; `FLOAT(p)` → `real` for p ≤ 24, else `double precision`. Character columns and
  targets carry `declaredLength`: VARCHAR(n) / CHARACTER VARYING(n) as `{ length: n, fixed:
false }`, CHAR(n) / CHARACTER(n) as fixed, CHAR alone as CHAR(1).
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

### Also done in Phase 2 increment 1

- **Shape passes.** `prepareColumnarShape(plan, columnsOf)` (engine/query.ts) exports the passes
  `createPreparedColumnarQuery` runs, in its order; the current engine still calls its own copy
  (unchanged). The domain-literal pass is not needed: lowering types constants by context.
- **Sequence names** resolve from `CatalogView.sequences` in lowering; no front-end change.
- **Array element names.** `physicalTypeOfName` accepts `FLOAT`, `CHAR`, `CHARACTER`, `STRING`,
  `DATETIME` (contracts amendment).

### Still needed

1. **Done in lowering: access mode.** The front end still drops `READ ONLY`; lowering reads it
   from the statement's words (a parsed transaction statement holds only keywords), so the
   shipped parser is unchanged (R11.2).
2. **RETURNING from FROM / USING sources.** `UPDATE c … FROM o … RETURNING o.qty` and
   `DELETE FROM c USING o … RETURNING o.qty` are refused by the parser ("RETURNING qualifier
   must name the target table"); PostgreSQL returns them. Lowering already carries the source
   columns (`modify.passthrough`), so only the parser check must go.
3. **Fixed in the shipped engine, pending merge** (branch `fix/full-join-order-by`, R24.3):
   ORDER BY a qualified unselected column (`ORDER BY c.id` sorted by a selected `o.id`, FULL JOIN
   and windows); a correlated aggregate subquery's LIMIT / OFFSET applied before aggregating
   (review B6-p2b 1); EXISTS over a correlated aggregate; DISTINCT ON with an ORDER BY that does
   not lead with its expressions (42P10, review B6-p2b 4) and DISTINCT ON ordinals. Lowering
   receives the corrected front end once that branch is on main.
4. **Correlated shapes the front end refuses** before lowering sees them, so they are not
   `apply` yet: a correlated subquery in HAVING, in a grouped select list or nested in another
   subquery's select list ("Correlated subqueries are supported only as top-level WHERE
   conditions"), with GROUP BY or HAVING inside it, and DISTINCT inside a correlated scalar
   subquery. PostgreSQL accepts these correlated forms.

The typed preparation path supports FULL JOIN after another join, FULL JOIN under windows, and
NATURAL FULL JOIN. These are covered by `testing/lower-m3-joins.test.ts` through the real executor
and PostgreSQL. The shipped executor still uses its sole-FULL-join rewrite and refuses NATURAL
FULL JOIN; its restrictions are not restrictions of typed lowering. NATURAL RIGHT JOIN remains
refused by typed preparation.

## Contract requests

### Landed (contracts amendment, R17.4–R17.6)

- IR (`ir/ir.ts`), all additive: ON CONFLICT `arbiters` (`indexId` kept as `arbiters[0]` or −1);
  modify kind `merge` with `MergeSpec`; the `input` relation for batch writes; `indexRange`;
  `keyLookup.emitLocator`; `modify.passthrough` and the read rules for ON CONFLICT and
  RETURNING; the mark-join output; `cast.assignment` (22001 instead of truncation).
- `ir/functions.ts`: every `call.fn` with its overloads, strictness, volatility, implicit casts
  and preferred types. The executor implements exactly these overloads.
- `ir/rules.ts` and `ir/costs.ts` are the single definitions of `TableRuleExpressions` (with
  `defaults`) and `ExecutorCosts`; exec/ re-exports them.
- `next/errors.ts`: the one `UnsupportedPlanError` (stage, construct); exec/ re-exports it.
- `types.ts`: `TextType.maxLength` / `fixed`, `SqlTypeDescriptor.declaredLength`.

### Write path (access/; B3 Phase 2, R17.7)

1. **Owned sequences.** `TableInput` column `identity: { start, increment } | null`: the write
   path creates the owned sequence with the table and sets `identitySequence` and `ownedBy`.
   Until then SERIAL / IDENTITY is `UnsupportedPlanError("identityColumn")`.
2. **Self-referencing foreign keys.** `TableInput.foreignKeys[].parentTableId` accepts `"self"`.
   Until then `UnsupportedPlanError("selfReference")`.
3. **Enum types** in the catalog (`TypeDef { id, name, labels }`, `createType` / `dropType`).
   Until then `UnsupportedPlanError("enumType")`.
4. **Planning statistics**: a synchronous per-table summary at the current root (live rows; per
   column null count, bounds, distinct estimate, average width) for `PlanningStatistics`.
5. **VARCHAR(n) on write** (R17.4): lowering puts an assignment cast on every written value, so
   SQL writes raise 22001; the write path may also check `maxLength` for writes it makes itself.

### Executor (exec/; B4 Phase 2)

1. Kernels for `ir/functions.ts`, `cast.assignment`, the `input` relation (batches supplied with
   `ExecuteRequest`), `indexRange`, `keyLookup.emitLocator`, `modify.passthrough`, and ON CONFLICT
   over `arbiters`.
2. Defaults in `TableRuleExpressions` read the statement clock as `param` slot 0 (ir/rules.ts).

### Owner (owner/; B5)

1. `WireColumnMeta` is `ResultColumn` (lower/result.ts); `render` may be `"bigint"` on a
   `decimal(null, 0)` column (SUM(int8)).
2. `PlannedStatement` comes from `LoweredStatement` (kind, columns, parameters,
   `controlsTransaction`, `catalogVersion`); `readSet` is `reads`.
3. Schema-DSL tables lower through `Lowerer.lowerSchemaTable` (R17.3 widths).

## Decided (R17)

1. int8 renders `bigint` only when an operand is bigint-rendered (BIGINT column, SUM(bigint),
   CAST AS BIGINT, bigint parameter); otherwise `number`. `COUNT(*) + 1` is `number`.
2. SUM(int8) is exact, `decimal(null, 0)` rendered `bigint`.
3. Schema DSL: `column.integer()` → INTEGER, `column.number()` → DOUBLE PRECISION.
4. VARCHAR(n) / CHAR(n) widths are kept, printed (`character varying(n)`, `character(n)`) and
   enforced on write (22001). CHAR(n) is not blank-padded: a documented divergence.

## Real executor integration

B6's reviewed lowering is the authority for scalar, set, recursive and locking plans as well as
joins and windows. B7 adds exact real-executor campaigns, byte-collation join keys, completed
outer-join WHERE placement, and uncorrelated EXISTS/IN marks that avoid unsupported apply.
The catalogue executes every cell it can and records each remaining refusal by class; its data
stays frozen. The authorized S_q7/guide-4/guide-5 refreeze only casts ROUND inputs to NUMERIC;
the harness reverses those corrections to verify the original whole-catalogue fingerprint.
The old FLOAT8/int4 overload remains a legacy extension planned for removal at cutover under R19.

Frame offsets follow PostgreSQL execution timing: ROWS/GROUPS and NULL offsets validate when
WindowAgg starts; negative/NaN RANGE values validate at non-NULL comparisons. Ranking frames
are discarded after typing when PostgreSQL optimizes them away. LIMIT 0 never starts validation.
Generated real-executor and resident/streamed campaigns cover empty and NULL-only inputs.

## Ordered sets (M3)

Numeric `percentile_cont`, `percentile_disc` and `mode` lower to `AggregateCall` using the R19
signatures: direct percentile fractions are in `args`, evaluated once per group; the sorted
input is the single `orderBy` key. `mode` has no direct arguments. Continuous inputs coerce to
float8 and produce float8; discrete/mode results preserve the ordered input type. Fractions
outside [0, 1], including NaN, give PostgreSQL's 22003 at execution, even for an empty scalar
aggregate. LIMIT 0 does not evaluate the aggregate. NULL fractions and no non-NULL values
produce NULL. NULL ordered inputs do not contribute to positions or mode counts.

Execution is isolated in `exec/agg/ordered-set.ts`. A shared external sort orders groups, then
one shared sort per ordered call holds or spills the current group's values. Values and result
copies are charged before allocation. Each sort keeps its own fixed minimum and an independent
loan owner, reserved before the plan holds fixed buffers. The operator retains borrowed spill charges through
cancellation/drain and uses the shared task clock. No unbounded group-to-array map is built.

Current explicit capability limits: numeric ordered inputs, constant/parameter direct fractions,
ordered-set-only aggregate lists. Array fractions, group-key direct fractions, interval
interpolation and mixed ordinary/ordered-set lists are refused with typed UnsupportedPlanError.
PG comparisons use real storage/executor tests; REAL's and wide BIGINT's differences from the
shipped number model have narrow independent R21 witnesses in `testing/divergences.ts`.

## Numeric ROUND execution

The R19 `round(numeric,int4)` overload executes through `exec/kernels/round.ts`.
It matches PostgreSQL's half-away-from-zero rounding, negative scales, strict NULLs,
NaN/infinity propagation, display-scale clamp and numeric-overflow error. The scalar
kernel has fixed charged scratch and charges wide results before construction; reused
batches release previous wide results. One-argument numeric and float8 ROUND remain
explicit execution refusals. Corrected S_q7 and guide-4/5 execute against original PG
and shipped-engine rows; no new-core tolerance or changed data is used.

Unary numeric spellings are classified by signed magnitude before overload resolution.
The parser's unary-prefix form is kept distinct from explicit arithmetic: `-2147483648`
is int4, but `(-1) * 2147483648` is int8. Decimal spellings and explicit casts keep their
PostgreSQL types; integer arithmetic folding still preserves the resolved call type.

The literal plan-cache scanner uses these signed integer ranges too, including both
minima. Lifted slots and the client's own parameter types agree with the original
PG-typed expression; decimal spellings and nonliteral arithmetic stay unlifted.

## Remaining frozen-catalogue refusals

The registry in `testing/divergences.ts` names every remaining refusal and its reason. The
catalogue suite checks its exact class, stage, construct and SQLSTATE, and executes each refused
SQL statement against PGlite to verify the authority outcome. These are capability limits of
this unexported core; they do not describe the shipped engine's SQL support.

| Cell    | Stage / construct | Reason                                                                                                        |
| ------- | ----------------- | ------------------------------------------------------------------------------------------------------------- |
| guide-7 | lower / function  | JSON_OBJECT and JSON_ARRAYAGG need heterogeneous typed construction and charged, spillable JSON accumulation. |
| guide-8 | lower / function  | Whole-row json_agg needs record typing and bounded JSON accumulation; its expanded JSON_OBJECT is refused.    |

Canonical DATE literals now lower directly to epoch-day vectors using the binder's shared
canonical parser. The shipped SQL front end still limits DATE literal spelling to AD years
0001..9999; moving the parser does not expand that syntax. The physical parser/binder continues
to accept its full date range, BC and infinities.

Constant JSON_TABLE rows retain their declared types, including NULL-only prefixes and empty
sources. A literal-only UNION ALL (including this front-end desugar) folds to one `values` bag,
keeping order and duplicates. This rewrite neither evaluates scalar calls nor implements
UNION DISTINCT, INTERSECT or arbitrary set execution. EXCEPT uses its own hash-count executor.

Timestamp and timestamptz DATE_TRUNC execute in the fixed UTC session (C4), with PostgreSQL's
units/aliases, integer microseconds, calendar boundaries, BC dates, infinity propagation and
unit/range errors. The interval overload stays a typed execution refusal. This does not add
non-UTC sessions or the three-argument timestamptz overload to R19.

The monthly guide-13/17 authority queries pin PostgreSQL's floating SUM input to order_id ASC,
matching the verified stored scan and stable per-group updates. Guide-17's LAG stage emits
month ASC before its global SUM, so the PG global SUM uses that full-frame order too. The
frozen SQL remains unchanged. Unordered PostgreSQL float sums may choose a different input
order; these comparisons use explicit orders rather than a numerical tolerance.

## Temporal scalar execution

DATE subtraction returns int4 days. Timestamp/timestamptz subtraction returns a finite INTERVAL
with zero months and PostgreSQL's `interval_justify_hours` day/remainder fields; negative
remainders truncate toward zero. Wide inputs subtract as exact BigInts and int64 difference
overflow gives 22008. Infinite DATE subtraction and equal infinite timestamps give PG's 22008.
Other timestamp differences that would produce an infinite INTERVAL stay typed execution
refusals; no interval-infinity writer encoding or stored-format meaning is introduced.

DATE, TIMESTAMP, TIMESTAMPTZ and finite INTERVAL cast to unbounded TEXT in PostgreSQL's default
ISO date / postgres interval styles, with the fixed UTC zone. SQL timestamp text trims fraction
zeros and uses historical BC years; D1's ISO timestamp row-value format remains unchanged.
Mixed-sign intervals keep their months, days and microseconds as separate fields. Bounded TEXT
casts and infinite INTERVAL output remain explicit typed refusals.

The text kernels reserve bounded ASCII buffers before allocation (48 bytes per date/timestamp
slot, 128 per interval slot), reuse them and skip NULL/unselected garbage. Sparse and
nonascending selections keep monotone output offsets. Differential tests construct PG input
from independent ordinal date and interval fields and compare interval subtraction fields
directly, rather than judging arithmetic through its text renderer. Guide-3 now executes
unchanged against both PGlite and the shipped engine; guide-9 and guide-14/15 also execute unchanged.

SQL TIMESTAMP literals now retain their original spelling in a parser-only weak side table,
carried through plan cloning and structural identity. Lowering builds native microseconds from
calendar and fractional fields, rather than reconstructing them from the shipped parser's Date.
SQL TIMESTAMP ignores a written zone as PostgreSQL does; casts to the fixed-UTC zoned type keep
these microseconds. No published model field is added. Normal shipped parsing keeps its existing
Date values and precision. Short date/time spellings and BC fractions retain the shared front
end's validation; the native timestamp boundary is checked with 22008. Existing front-end syntax
and JS Date range limits remain; native vector coverage does not imply every native value is
expressible as a SQL literal. Generated SQL/API tests cover 0–6 fraction digits, microsecond
comparisons, differences, text, direct/derived/CTE/view/grouped/aggregate scopes, serialized timestamp defaults and wide years.

The native timestamp contract has fixed microsecond precision. Explicit TIMESTAMP/TIMESTAMPTZ
precision 6 is admitted; other precision modifiers on CAST targets or columns give a typed
lowering refusal (`columnType`, 0A000) until rounding and column precision constraints exist.
They must not inherit the shipped front end's silently ignored modifiers. Normal shipped
compilation retains its existing millisecond/ignored-precision behavior. The generated modifier
matrix checks every target spelling and both contexts against PG admission and the typed limit.

EXCEPT and EXCEPT ALL now execute using canonical SQL equality keys (NULLs compare as one
class), exact int64 left/right counts and the first left representative. Radix partition spill
uses the shared Pool/SpillIo and a reverse-linked on-disk page directory, so the page index does
not grow in RAM. A one-key collision pass guarantees progress when partition hashes collide.
Optional file write caches have one bounded shared allowance; exhaustion falls back to direct
page writes. Typed execution limits: 1–31 hash-key columns, no CHAR blank-padding equality,
JSONB/array semantic keys or locators. Plain JSON lacks equality and is rejected with PG 42883.
UNION DISTINCT and INTERSECT remain explicit execution refusals. Stored formats and shipped SQL
behavior are unchanged. Generated SQL and resident/spilled real-scratch campaigns compare
multiplicities, NULLs and representative float/NUMERIC/INTERVAL output exactly with PGlite.
Wide EXCEPT rows preflight key encoding, the copied key and representative together. Spill
pages borrow the input row rather than retaining a second staging arena. Recursive joins
preflight page/key work and pin the complete single-row table/replay transition until its
buffers close; dead key encodings retire before staging/output. Competing-grant generators
compare native and lowered SQL with PGlite, including collision fallback and every join kind.
Repeated key indexes, asymmetric duplicate predicates and encoded MARK correlation prefixes
carry their complete key-position bounds through chunk load, table build, replay and restore.
These bounds follow the selected key columns, not unrelated payloads. Spill-page and saved
key statistics cover distinct-key partition forwarding as well as collision/chunk fallback.
The independent result-sink width bound for two large parameter-derived join outputs remains
an admission-owner limitation (B6-join-result-admission handoff); it is not masked by a new
set/join refusal. The operator/tape resident-state proof excludes backend implementation costs; shared scratch
control credit is admitted separately.

## JSON extraction

The private rebuild executes JSON `->` and `->>` with text keys and int4 array indexes,
including negative indexes. int2 selectors widen to int4; boolean, NUMERIC and int8 selectors
give PostgreSQL's 42883. Raw JSON keeps its original number spelling, duplicate keys and
whitespace; text extraction decodes strings and maps JSON null to SQL NULL. Object lookups use
the last matching key. Missing paths and mismatched container types return SQL NULL, as in PG.
The guide-9 SQL and datasets are unchanged: it now executes against PGlite and the shipped
engine. The remaining frozen-catalogue refusals are guide-7/8 construction and aggregation.

Projection/filter extraction runs one borrowed row at a time with a prepaid input-sized
workspace, checks the shared task clock, and retains its source until downstream readiness.
Text-to-JSON validation preserves source bytes. Source provenance is followed through VALUES,
views, CTEs and projections; document parameters, lazy init values and expanding scalar sources
without a stable input envelope stay typed execution refusals. Non-NULL JSON materialized by
the shipped front end (JSON_TABLE JSON columns) stays a
typed lowering refusal because that normalizer discards decimal scale and original spelling.
Use an explicit `'…'::JSON` literal for exact extraction. JSON expressions mixed with volatile
evaluation also stay refused.

The private rebuild also executes `text::JSONB`, JSONB `->`/`->>` and `JSONB::text`. It preserves
exact decimal digits and display scale, normalizes object keys in PostgreSQL's UTF8-length/byte
order, keeps the last duplicate, and handles scalar integer selectors 0/-1 as PostgreSQL does.
Invalid input retains PG's JSON, Unicode and numeric SQLSTATEs. A tiny numeric exponent may
expand, so this path counts before allocation and checkpoints completed output plus unread input
as bounded native spill fragments. It retires producer credit before FIFO, cancellable re-admission
through B4's shared idle pool. Owned parameter bindings survive that handoff. Delivery uses a
separately admitted single-row buffer; it neither reopens consumed sources nor replays completed rows.
Filter/LIMIT demand and the stream position of row data errors survive the checkpoint; cancellation
and storage faults still unwind immediately.
Unary, EXCEPT, supported join and window parents share the same JSON/hybrid-aggregate
checkpoint controller. Only retaining ancestors participate; consumed join builds are not reopened.
Checkpointable windows use their complete native initial minimum and separately admitted restore
workspace before copying. They retain ordinary fast routes through empty-prefix handoffs
and until a nonempty cut or grouped pressure fallback; only then is peer-stable input converted to ordinal continuation. Both producer kinds retain one shared idle account and cleanup ownership
through cancellation, I/O faults and concurrent continuations.
Document parameters, lazy init values, foreign expanding document producers, front-end materialized
JSON/JSONB literals and JSON ↔ JSONB casts remain explicit refusals. JSON paths, construction and
aggregation remain separate work; guide-7/8 remain the only two catalogue refusals (64/66 execute).
Canonicalization uses bounded scratch and iterative traversal, with quadratic CPU for wide objects;
there is no claimed wide-object speedup. No published engine behavior or stored format changes here.

The R21 registry records the shipped engine's canonicalized number-text difference for literal
JSON decimals with a zero fractional part. Generated checks require the native result to match
PG's original JSON extraction and the shipped result to match an independent PG int4::text
witness. Its matcher rejects unrelated wrong values; no tolerance or native-result waiver is used.

## Calendar and recursive execution

Date/timestamp/timestamptz plus or minus a finite interval now executes in fixed UTC. The
kernel clamps the month, adds calendar days, then adds exact integer microseconds. Each
calendar stage is range-checked before a later field can cancel it, as in PostgreSQL. Finite
interval EXTRACT returns exact NUMERIC coefficients and PG display scales; interval comparisons
use PG's 30-day-month duration, including NULL and infinity ordering. Infinite interval
arithmetic/extraction, date/time EXTRACT overloads and dynamic TEXT-to-INTERVAL casts remain
typed execution refusals.

Only the native parser records the original INTERVAL arithmetic text and GENERATE_SERIES arity
in private weak side tables, copied through query clones and included in expression identity.
User DATE_ADD calls remain refused; normal shipped parsing and execution are unchanged. The
shared SQL literal parser's existing safe-integer interval field limit still applies.

Integer and timestamp GENERATE_SERIES in FROM uses a dedicated streaming operator with prepaid fixed
batches, shared task-clock yields, cancellation and downstream demand. Original arity resolves
against the R19 table before default-step synthesis. NULL arguments produce no rows; a zero
step gives 22023 when demanded. Numeric series and timezone-argument series remain refused.
There is no hidden row cap. PG materializes a FROM FunctionScan before LIMIT; the huge-range
prefix check therefore uses a finite PG prefix witness, while >100,000-row counts use identical
SQL against PG.

UNION ALL recursion supports fixed-width work tables, simple VALUES/scan/series seeds and a
linear filter/project step over its work-table scan. Two bounded staging buffers alternate
between levels and spill wider frontiers through the shared pool. A fixed-size binary carry
directory preserves FIFO page and row order without a resident page-reference list. The step
frame is compiled once and reused; task yields, cancellation and demand continue across levels.
All I/O/consumer borrows settle before cleanup. There is no recursion-depth cap.

UNION DISTINCT recursion, variable-width/decimal work tables and nonlinear/join/aggregate steps
remain explicit typed execution refusals. These are capability limits, not SQL row-count limits.
The scratch backend's existing extent-directory bookkeeping remains outside this operator's
fixed metadata proof. Real browser I/O and process RSS are not proved by the memory-service and
ScratchSpillService/SimBackend properties. Under completely full ordinary memory, main's shared
aggregate may refuse its 12-byte output; the recursive pressure property leaves 64 KiB for that
existing output allocation. It still prevents optional spill-buffer growth. No recursive-specific
refusal masks this separate shared aggregate issue.

The unchanged guide-15 native result is compared to PostgreSQL's NUMERIC output. Only the
independent shipped-engine witness casts busy_hours to float8, matching the current engine's
EXTRACT type (R21). No tolerance or frozen-SQL correction is used.

### DATE_PART and EXTRACT

`DATE_PART` has its own R19 overloads returning `float8`; PostgreSQL 14+ `EXTRACT` returns
`NUMERIC`. Native-only private call provenance survives child rewrites, clones and expression
identity without a public model field. Finite interval DATE_PART uses PostgreSQL's float8
operation order, including second/millisecond fractions and epoch cancellation; it does not
convert the exact EXTRACT result after evaluation. Date/time DATE_PART kernels remain typed
execution refusals. Its float8 type still rejects two-argument ROUND at lowering, as PostgreSQL
does, including over date/timestamp arguments whose execution is not implemented yet.
