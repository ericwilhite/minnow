import type { AnyTable, SchemaDefinition } from "@minnowdb/core";
import {
  ColumnNode,
  DefaultInsertValueNode,
  InsertQueryNode,
  ListNode,
  MergeQueryNode,
  PostgresQueryCompiler,
  PrimitiveValueListNode,
  RawNode,
  ValueListNode,
  ValuesNode,
  type AggregateFunctionNode,
  type AlterTableNode,
  type ColumnDefinitionNode,
  type CompiledQuery,
  type CreateIndexNode,
  type CreateTableNode,
  type CreateViewNode,
  type DeleteQueryNode,
  type DropIndexNode,
  type DropTableNode,
  type DropViewNode,
  type ForeignKeyConstraintNode,
  type FunctionNode,
  type JoinNode,
  type JSONPathNode,
  type MatchedNode,
  type OperationNode,
  type PrimaryKeyConstraintNode,
  type QueryId,
  type OnConflictNode,
  type RootOperationNode,
  type SchemableIdentifierNode,
  type SelectQueryNode,
  type UniqueConstraintNode,
  type UpdateQueryNode,
  type ValuesItemNode,
} from "kysely";

interface TableMetadata {
  readonly fallbackColumn?: string;
}

interface RuntimeColumn {
  readonly hasDefault: boolean;
  readonly isNullable: boolean;
}

function metadataByTable(
  schema: SchemaDefinition<readonly AnyTable[]> | undefined,
): ReadonlyMap<string, TableMetadata> {
  if (schema === undefined) return new Map();
  return new Map(
    schema.tables.map((table) => {
      const fallbackColumn = Object.entries(table.columns).find(([, definition]) => {
        const column = definition as RuntimeColumn;
        return column.hasDefault || column.isNullable;
      })?.[0];
      return [table.name, fallbackColumn === undefined ? {} : { fallbackColumn }] as const;
    }),
  );
}

function isEmptyValuesRow(row: ValuesItemNode): boolean {
  return (PrimitiveValueListNode.is(row) || ValueListNode.is(row)) && row.values.length === 0;
}

function normalizeEmptyValues(
  node: InsertQueryNode,
  tableName: string,
  metadata: TableMetadata | undefined,
): InsertQueryNode {
  if (
    (node.columns?.length ?? 0) !== 0 ||
    node.values === undefined ||
    !ValuesNode.is(node.values) ||
    node.values.values.length === 0 ||
    !node.values.values.every(isEmptyValuesRow)
  ) {
    return node;
  }

  if (node.values.values.length === 1) {
    const { columns, values, ...withoutEmptyValues } = node;
    void columns;
    void values;
    return Object.freeze({ ...withoutEmptyValues, defaultValues: true });
  }

  if (metadata?.fallbackColumn === undefined) {
    throw new TypeError(
      `Kysely INSERT INTO ${tableName} contains multiple empty rows. Pass Minnow's schema to ` +
        "createKysely/MinnowDialect so the compiler can name a defaulted or nullable column, " +
        "or insert the rows separately.",
    );
  }

  return InsertQueryNode.cloneWith(node, {
    columns: [ColumnNode.create(metadata.fallbackColumn)],
    values: ValuesNode.create(
      node.values.values.map(() => ValueListNode.create([DefaultInsertValueNode.create()])),
    ),
  });
}

function normalizeInsert(
  node: RootOperationNode,
  tables: ReadonlyMap<string, TableMetadata>,
): RootOperationNode {
  if (!InsertQueryNode.is(node) || node.into === undefined) return node;
  const tableName = node.into.table.identifier.name;
  return normalizeEmptyValues(node, tableName, tables.get(tableName));
}

/** Whole statement kinds Minnow's engine has no counterpart for. */
const REFUSED_STATEMENTS: ReadonlyMap<string, string> = new Map([
  [
    "CreateSchemaNode",
    "Minnow has no schemas or catalogs, so CREATE SCHEMA has nothing to create; every table " +
      "lives in the single default namespace.",
  ],
  [
    "DropSchemaNode",
    "Minnow has no schemas or catalogs, so DROP SCHEMA has nothing to drop; every table lives " +
      "in the single default namespace.",
  ],
  [
    "AlterTypeNode",
    "Minnow does not support ALTER TYPE. Create a new enum type with the values you need, " +
      "migrate the columns to it, and stop using the old one.",
  ],
  [
    "DropTypeNode",
    "Minnow does not support DROP TYPE; an enum type stays registered in the catalog once " +
      "created.",
  ],
  [
    "RefreshMaterializedViewNode",
    "Minnow does not support materialized views, so there is nothing to refresh. Use an " +
      "ordinary view, or store the rows with CREATE TABLE ... AS SELECT.",
  ],
]);

const MUTATION_NODE_KINDS: ReadonlySet<string> = new Set([
  "InsertQueryNode",
  "UpdateQueryNode",
  "DeleteQueryNode",
  "MergeQueryNode",
]);

/** Detects the RawNode Kysely builds for MERGE's `thenDoNothing()`. */
function isDoNothingResult(result: OperationNode | undefined): boolean {
  return (
    result !== undefined &&
    RawNode.is(result) &&
    result.sqlFragments.join(" ").toLowerCase().includes("do nothing")
  );
}

/**
 * PostgreSQL SQL compilation with Kysely's empty-object inserts normalized to valid SQL.
 *
 * Kysely can build PostgreSQL forms Minnow's engine refuses, and the engine's refusal is a bare
 * parse error ("Expected eof, found on") with no hint at the unsupported feature. This
 * compiler knows which builder produced the node, so it refuses those forms before execution
 * with the feature named and an alternative offered — the same treatment MERGE ... RETURNING
 * already gets. Forms the engine reads — DISTINCT ON, UPDATE ... FROM, DELETE ... USING, the
 * row-locking clauses it ignores, CREATE TEMPORARY TABLE — pass through unchanged.
 */
export class MinnowQueryCompiler extends PostgresQueryCompiler {
  readonly #tables: ReadonlyMap<string, TableMetadata>;

  constructor(schema?: SchemaDefinition<readonly AnyTable[]>) {
    super();
    this.#tables = metadataByTable(schema);
  }

  override compileQuery(node: RootOperationNode, queryId: QueryId): CompiledQuery {
    const refusedStatement = REFUSED_STATEMENTS.get(node.kind);
    if (refusedStatement !== undefined) throw new TypeError(refusedStatement);
    // These clauses live on several root node kinds; a structural read avoids repeating the
    // check per kind. TS narrows `in` checks on optional properties to "defined", which the
    // nodes do not guarantee, so the read stays explicit.
    const clauses = node as {
      readonly with?: {
        readonly expressions: ReadonlyArray<{ readonly expression: OperationNode }>;
      };
      readonly explain?: OperationNode;
      readonly top?: OperationNode;
      readonly output?: OperationNode;
    };
    if (
      clauses.with?.expressions.some((cte) => MUTATION_NODE_KINDS.has(cte.expression.kind)) === true
    ) {
      throw new TypeError(
        "Minnow supports only SELECT queries inside WITH; a data-modifying CTE " +
          "(INSERT/UPDATE/DELETE ... RETURNING) does not run. Execute the mutation first and " +
          "use its RETURNING rows, or put the SELECT in the CTE and the mutation around it.",
      );
    }
    if (clauses.explain !== undefined) {
      throw new TypeError(
        "Minnow does not run SQL EXPLAIN statements. Call the Minnow database's own explain() " +
          "method with the query's SQL to see the optimized plan.",
      );
    }
    if (clauses.top !== undefined) {
      throw new TypeError(
        "Minnow does not support the T-SQL TOP clause. Use limit() on a select, or a keyed " +
          "subquery for mutations.",
      );
    }
    if (clauses.output !== undefined) {
      throw new TypeError(
        "Minnow does not support the T-SQL OUTPUT clause. Use returning()/returningAll() " +
          "instead.",
      );
    }
    if (MergeQueryNode.is(node)) {
      // Minnow's MERGE reports an affected-row count but carries no returned rows, so a
      // `mergeInto(...).returning(...)` would type-check and then silently yield []. Refuse it
      // here, before execution, instead of returning nothing.
      if (node.returning !== undefined) {
        throw new TypeError(
          "Minnow does not support RETURNING on MERGE statements. Remove .returning()/.returningAll() " +
            "and read the affected-row count, or query the rows separately after the merge.",
        );
      }
      if (node.whens?.some((when) => isDoNothingResult(when.result)) === true) {
        throw new TypeError(
          "Minnow does not support MERGE ... THEN DO NOTHING. Narrow the WHEN clause with " +
            "whenMatchedAnd/whenNotMatchedAnd so the merge skips those rows instead.",
        );
      }
    }
    return super.compileQuery(normalizeInsert(node, this.#tables), queryId);
  }

  protected override visitSelectQuery(node: SelectQueryNode): void {
    if (node.top !== undefined) {
      throw new TypeError(
        "Minnow does not support the T-SQL TOP clause. Use limit() on a select, or a keyed " +
          "subquery for mutations.",
      );
    }
    super.visitSelectQuery(node);
  }

  // Kysely's PostgreSQL-flavored JSON functions are the most common porting trap: the engine
  // names the unsupported function, but only this adapter can name the Minnow replacement.
  static readonly #JSON_FUNCTION_ALTERNATIVES: ReadonlyMap<string, string> = new Map([
    [
      "json_agg",
      "Minnow does not provide PostgreSQL's json_agg. Import jsonArrayFrom from " +
        "@minnowdb/kysely/helpers, or use JSON_ARRAYAGG through fn.agg.",
    ],
    [
      "jsonb_agg",
      "Minnow does not provide PostgreSQL's jsonb_agg. Import jsonArrayFrom from " +
        "@minnowdb/kysely/helpers, or use JSON_ARRAYAGG through fn.agg.",
    ],
    [
      "to_json",
      "Minnow does not provide PostgreSQL's to_json. Import jsonBuildObject or jsonObjectFrom " +
        "from @minnowdb/kysely/helpers.",
    ],
    [
      "to_jsonb",
      "Minnow does not provide PostgreSQL's to_jsonb. Import jsonBuildObject or jsonObjectFrom " +
        "from @minnowdb/kysely/helpers.",
    ],
    [
      "json_build_object",
      "Minnow does not provide PostgreSQL's json_build_object. Import jsonBuildObject from " +
        "@minnowdb/kysely/helpers; it emits Minnow's JSON_OBJECT.",
    ],
    [
      "jsonb_build_object",
      "Minnow does not provide PostgreSQL's jsonb_build_object. Import jsonBuildObject from " +
        "@minnowdb/kysely/helpers; it emits Minnow's JSON_OBJECT.",
    ],
  ]);

  #refuseJsonPortabilityTrap(functionName: string): void {
    const alternative = MinnowQueryCompiler.#JSON_FUNCTION_ALTERNATIVES.get(
      functionName.toLowerCase(),
    );
    if (alternative !== undefined) throw new TypeError(alternative);
  }

  protected override visitFunction(node: FunctionNode): void {
    this.#refuseJsonPortabilityTrap(node.func);
    super.visitFunction(node);
  }

  protected override visitAggregateFunction(node: AggregateFunctionNode): void {
    this.#refuseJsonPortabilityTrap(node.func);
    super.visitAggregateFunction(node);
  }

  protected override visitJoin(node: JoinNode): void {
    if (node.joinType === "CrossApply" || node.joinType === "OuterApply") {
      throw new TypeError(
        "Minnow does not support the T-SQL CROSS APPLY and OUTER APPLY joins. Use " +
          "innerJoinLateral() or leftJoinLateral() instead.",
      );
    }
    super.visitJoin(node);
  }

  protected override visitJSONPath(node: JSONPathNode): void {
    // `->$` and `->>$` compile to a JSON path string such as '$."name"'. Minnow's -> and ->>
    // read that string as one member name, so the traversal quietly yields NULL.
    void node;
    throw new TypeError(
      "Minnow's -> and ->> operators take one key or array index per step, not a JSON path. " +
        "Use eb.ref(column, '->') or eb.ref(column, '->>') with .key() and .at(), or " +
        "JSON_VALUE through sql for a path.",
    );
  }

  protected override visitMatched(node: MatchedNode): void {
    if (node.bySource) {
      throw new TypeError(
        "Minnow does not support MERGE ... WHEN NOT MATCHED BY SOURCE. Update or delete the " +
          "unmatched target rows with a separate statement whose WHERE excludes the source keys.",
      );
    }
    super.visitMatched(node);
  }

  protected override visitInsertQuery(node: InsertQueryNode): void {
    if (node.replace === true) {
      throw new TypeError(
        "Minnow does not support MySQL's REPLACE INTO. Use " +
          "onConflict((oc) => oc.columns([...]).doUpdateSet(...)) instead.",
      );
    }
    if (node.orAction !== undefined) {
      throw new TypeError(
        "Minnow does not support SQLite's INSERT OR IGNORE/ABORT/FAIL/ROLLBACK actions. Use " +
          "onConflict((oc) => oc.columns([...]).doNothing()) for ignore semantics.",
      );
    }
    if (node.onDuplicateKey !== undefined) {
      throw new TypeError(
        "Minnow does not support MySQL's ON DUPLICATE KEY UPDATE. Use " +
          "onConflict((oc) => oc.columns([...]).doUpdateSet(...)) instead.",
      );
    }
    super.visitInsertQuery(node);
  }

  protected override visitUpdateQuery(node: UpdateQueryNode): void {
    if (node.table !== undefined && ListNode.is(node.table)) {
      throw new TypeError(
        "Minnow does not support MySQL's multi-table UPDATE. Update one table per statement, " +
          "with WHERE ... IN (SELECT ...) for values that come from another table.",
      );
    }
    if (node.orderBy !== undefined || node.limit !== undefined) {
      throw new TypeError(
        "Minnow does not support MySQL's ORDER BY/LIMIT on UPDATE. Select the target keys " +
          "with WHERE ... IN (SELECT ... ORDER BY ... LIMIT ...) instead.",
      );
    }
    super.visitUpdateQuery(node);
  }

  protected override visitDeleteQuery(node: DeleteQueryNode): void {
    if (node.from.froms.length > 1) {
      throw new TypeError(
        "Minnow does not support MySQL's multi-table DELETE. Delete from one table per " +
          "statement, with WHERE ... IN (SELECT ...) for keys that come from another table.",
      );
    }
    if (node.orderBy !== undefined || node.limit !== undefined) {
      throw new TypeError(
        "Minnow does not support MySQL's ORDER BY/LIMIT on DELETE. Select the target keys " +
          "with WHERE ... IN (SELECT ... ORDER BY ... LIMIT ...) instead.",
      );
    }
    super.visitDeleteQuery(node);
  }

  protected override visitOnConflict(node: OnConflictNode): void {
    if (node.constraint !== undefined || node.indexExpression !== undefined) {
      throw new TypeError(
        "Minnow supports ON CONFLICT only with an explicit column target. Name the unique " +
          "key's columns instead of a constraint or index expression.",
      );
    }
    super.visitOnConflict(node);
  }

  protected override visitValueList(node: ValueListNode): void {
    this.#refuseEmptyInList(node.values.length);
    super.visitValueList(node);
  }

  protected override visitPrimitiveValueList(node: PrimitiveValueListNode): void {
    this.#refuseEmptyInList(node.values.length);
    super.visitPrimitiveValueList(node);
  }

  #refuseEmptyInList(length: number): void {
    // Insert rows never reach this empty: normalizeInsert rewrites empty value objects first.
    if (length === 0) {
      throw new TypeError(
        "An empty list compiles to IN (), which is not valid SQL. Add Kysely's " +
          "HandleEmptyInListsPlugin to resolve empty lists before they compile.",
      );
    }
  }

  protected override visitSchemableIdentifier(node: SchemableIdentifierNode): void {
    if (node.schema !== undefined) {
      throw new TypeError(
        "Minnow has no schemas or catalogs. Remove WithSchemaPlugin and schema-qualified " +
          "names; every table lives in the single default namespace.",
      );
    }
    super.visitSchemableIdentifier(node);
  }

  protected override visitCreateTable(node: CreateTableNode): void {
    // The engine reads CREATE TEMPORARY TABLE as an ordinary table: every table lives in one
    // database with one durability. ON COMMIT would need the table to go away by itself.
    if (node.onCommit !== undefined) {
      throw new TypeError(
        "Minnow does not support ON COMMIT on temporary tables: a temporary table is an " +
          "ordinary table here. Remove .onCommit() and drop the table when done.",
      );
    }
    if ((node.indexes?.length ?? 0) > 0) {
      throw new TypeError(
        "Minnow does not support MySQL's inline INDEX in CREATE TABLE. Create the table, then " +
          "add the index with createIndex().",
      );
    }
    super.visitCreateTable(node);
  }

  protected override visitDropTable(node: DropTableNode): void {
    if (node.temporary === true) {
      throw new TypeError(
        "Minnow does not support MySQL's DROP TEMPORARY TABLE: a temporary table is an " +
          "ordinary table here, so drop it with dropTable() and remove .temporary().",
      );
    }
    super.visitDropTable(node);
  }

  protected override visitDropView(node: DropViewNode): void {
    if (node.materialized === true) {
      throw new TypeError(
        "Minnow does not support materialized views, so DROP MATERIALIZED VIEW has nothing to " +
          "drop. Remove .materialized().",
      );
    }
    if (node.cascade === true) {
      throw new TypeError(
        "Minnow does not support DROP VIEW ... CASCADE. Drop the views that depend on this one " +
          "first, then remove .cascade().",
      );
    }
    super.visitDropView(node);
  }

  protected override visitDropIndex(node: DropIndexNode): void {
    if (node.table !== undefined) {
      throw new TypeError(
        "Minnow does not support MySQL's DROP INDEX ... ON table. Remove .on(); index names " +
          "are unique across the database.",
      );
    }
    if (node.cascade === true) {
      throw new TypeError(
        "Minnow does not support DROP INDEX ... CASCADE; nothing depends on an index. Remove " +
          ".cascade().",
      );
    }
    super.visitDropIndex(node);
  }

  static readonly #DEFERRABLE_CONSTRAINT =
    "Minnow does not support deferrable constraints; every constraint is checked by the " +
    "statement that writes. Remove .deferrable()/.notDeferrable() and " +
    ".initiallyDeferred()/.initiallyImmediate().";

  protected override visitPrimaryKeyConstraint(node: PrimaryKeyConstraintNode): void {
    if (node.deferrable !== undefined || node.initiallyDeferred !== undefined) {
      throw new TypeError(MinnowQueryCompiler.#DEFERRABLE_CONSTRAINT);
    }
    super.visitPrimaryKeyConstraint(node);
  }

  protected override visitUniqueConstraint(node: UniqueConstraintNode): void {
    if (node.deferrable !== undefined || node.initiallyDeferred !== undefined) {
      throw new TypeError(MinnowQueryCompiler.#DEFERRABLE_CONSTRAINT);
    }
    if (node.nullsNotDistinct === true) {
      throw new TypeError(
        "Minnow does not support NULLS NOT DISTINCT on unique constraints; unique keys treat " +
          "NULLs as distinct.",
      );
    }
    if (node.columns.some((column) => !ColumnNode.is(column))) {
      throw new TypeError(
        "Minnow does not support expression unique constraints. Store the expression in a " +
          "generated column (generatedAlwaysAs(...).stored()) and make that column unique.",
      );
    }
    super.visitUniqueConstraint(node);
  }

  protected override visitForeignKeyConstraint(node: ForeignKeyConstraintNode): void {
    if (node.deferrable !== undefined || node.initiallyDeferred !== undefined) {
      throw new TypeError(MinnowQueryCompiler.#DEFERRABLE_CONSTRAINT);
    }
    super.visitForeignKeyConstraint(node);
  }

  /**
   * `.autoIncrement()` spells MySQL's `auto_increment` in Kysely's PostgreSQL compiler, which
   * no PostgreSQL-style parser reads. Minnow accepts SQLite's `INTEGER PRIMARY KEY AUTOINCREMENT`
   * as its auto-increment key, so the modifier compiles the way Kysely's own SQLite dialect
   * compiles it and the builder's portable spelling keeps working.
   */
  protected override getAutoIncrement(): string {
    return "autoincrement";
  }

  protected override visitColumnDefinition(node: ColumnDefinitionNode): void {
    if (node.identity === true) {
      throw new TypeError(
        "Minnow does not support the T-SQL IDENTITY column modifier. Give the column the " +
          "'serial' data type, or call generatedAlwaysAsIdentity()/" +
          "generatedByDefaultAsIdentity().",
      );
    }
    if (node.ifNotExists === true) {
      throw new TypeError(
        "Minnow does not support ADD COLUMN IF NOT EXISTS. Check db.introspection.getTables() " +
          "for the column first, or declare the schema and let database.migrate() add it.",
      );
    }
    if (node.unsigned === true) {
      throw new TypeError(
        "Minnow does not support MySQL's UNSIGNED integers. Use a plain integer column and a " +
          "CHECK constraint if the column must stay non-negative.",
      );
    }
    if (node.nullsNotDistinct === true) {
      throw new TypeError(
        "Minnow does not support NULLS NOT DISTINCT on unique columns; unique keys treat " +
          "NULLs as distinct.",
      );
    }
    if (node.generated?.expression !== undefined && node.generated.stored !== true) {
      throw new TypeError(
        "Minnow supports only stored generated columns. Add .stored() after " +
          "generatedAlwaysAs(...).",
      );
    }
    super.visitColumnDefinition(node);
  }

  protected override visitCreateIndex(node: CreateIndexNode): void {
    if (node.using !== undefined) {
      throw new TypeError(
        "Minnow does not support index access methods (USING). Remove .using(); every Minnow " +
          "index is the same sorted secondary index.",
      );
    }
    if (node.where !== undefined) {
      throw new TypeError(
        "Minnow does not support partial indexes. Remove .where(); the index covers every row.",
      );
    }
    if (node.nullsNotDistinct === true) {
      throw new TypeError(
        "Minnow does not support NULLS NOT DISTINCT on unique indexes; unique keys treat " +
          "NULLs as distinct.",
      );
    }
    if (node.columns?.some((column) => RawNode.is(column)) === true) {
      throw new TypeError(
        "Minnow does not support expression indexes. Store the expression in a generated " +
          "column (generatedAlwaysAs(...).stored()) and index that column.",
      );
    }
    super.visitCreateIndex(node);
  }

  protected override visitCreateView(node: CreateViewNode): void {
    if (node.materialized === true) {
      throw new TypeError(
        "Minnow does not support materialized views. Use an ordinary view, or store the rows " +
          "with CREATE TABLE ... AS SELECT.",
      );
    }
    if (node.temporary === true) {
      throw new TypeError("Minnow does not support temporary views. Create an ordinary view.");
    }
    if (node.ifNotExists === true) {
      throw new TypeError(
        "Minnow does not support CREATE VIEW IF NOT EXISTS. Use orReplace(), or check the " +
          "catalog first with introspection.",
      );
    }
    if (node.columns !== undefined) {
      throw new TypeError(
        "Minnow does not support view column lists. Alias the columns inside the view's " +
          "SELECT instead.",
      );
    }
    super.visitCreateView(node);
  }

  protected override visitAlterTable(node: AlterTableNode): void {
    if (node.renameTo !== undefined) {
      throw new TypeError(
        "Minnow does not support ALTER TABLE ... RENAME TO. Create the new table, copy the " +
          "rows with INSERT ... SELECT, and drop the old one.",
      );
    }
    if (node.setSchema !== undefined) {
      throw new TypeError(
        "Minnow has no schemas or catalogs, so ALTER TABLE ... SET SCHEMA has nowhere to " +
          "move the table.",
      );
    }
    if (
      node.addConstraint !== undefined ||
      node.dropConstraint !== undefined ||
      node.renameConstraint !== undefined
    ) {
      throw new TypeError(
        "Minnow does not support altering table constraints after creation. Create a new " +
          "table with the constraints you need, copy the rows with INSERT ... SELECT, and " +
          "drop the old one.",
      );
    }
    if (node.addIndex !== undefined || node.dropIndex !== undefined) {
      throw new TypeError(
        "Minnow does not support MySQL's ALTER TABLE ... ADD/DROP INDEX. Use " +
          "createIndex()/dropIndex() on the schema module instead.",
      );
    }
    for (const alteration of node.columnAlterations ?? []) {
      if (alteration.kind === "RenameColumnNode") {
        throw new TypeError(
          "Minnow does not support ALTER TABLE ... RENAME COLUMN. Add the new column, copy the " +
            "values with UPDATE, and drop the old column.",
        );
      }
      if (alteration.kind === "AlterColumnNode" || alteration.kind === "ModifyColumnNode") {
        throw new TypeError(
          "Minnow does not support altering a column's type, default, or nullability in " +
            "place. Add a new column with the wanted definition, copy the values with UPDATE, " +
            "and drop the old column.",
        );
      }
    }
    super.visitAlterTable(node);
  }
}
