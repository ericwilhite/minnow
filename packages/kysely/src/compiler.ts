import type { AnyTable, SchemaDefinition } from "@minnowdb/core";
import {
  ColumnNode,
  DefaultInsertValueNode,
  InsertQueryNode,
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
  type FunctionNode,
  type JoinNode,
  type OperationNode,
  type QueryId,
  type OnConflictNode,
  type RootOperationNode,
  type SchemableIdentifierNode,
  type SelectModifier,
  type SelectModifierNode,
  type SelectQueryNode,
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

const LOCKING_MODIFIERS: ReadonlySet<SelectModifier> = new Set([
  "ForUpdate",
  "ForNoKeyUpdate",
  "ForShare",
  "ForKeyShare",
  "NoWait",
  "SkipLocked",
]);

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
 * parse error ("Expected eof, found from") with no hint at the unsupported feature. This
 * compiler knows which builder produced the node, so it refuses those forms before execution
 * with the feature named and an alternative offered — the same treatment MERGE ... RETURNING
 * already gets.
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
    if (node.distinctOn !== undefined) {
      throw new TypeError(
        "Minnow does not support DISTINCT ON. Group by the key, or rank rows with a window " +
          "function and keep the first per key.",
      );
    }
    super.visitSelectQuery(node);
  }

  protected override visitSelectModifier(node: SelectModifierNode): void {
    if (node.modifier !== undefined && LOCKING_MODIFIERS.has(node.modifier)) {
      throw new TypeError(
        "Minnow does not support row-locking clauses such as FOR UPDATE: the engine has a " +
          "single writer and snapshot reads, so remove the locking modifier.",
      );
    }
    super.visitSelectModifier(node);
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
    if (node.from !== undefined) {
      throw new TypeError(
        "Minnow does not support UPDATE ... FROM. Rewrite the update with a correlated " +
          "subquery or WHERE ... IN (SELECT ...).",
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
    if (node.using !== undefined) {
      throw new TypeError(
        "Minnow does not support DELETE ... USING. Rewrite the delete with a correlated " +
          "subquery or WHERE ... IN (SELECT ...).",
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
    if (node.temporary === true || node.onCommit !== undefined) {
      throw new TypeError(
        "Minnow does not support temporary tables. Create an ordinary table and drop it when " +
          "done, or query over VALUES/a CTE for transient rows.",
      );
    }
    super.visitCreateTable(node);
  }

  protected override visitColumnDefinition(node: ColumnDefinitionNode): void {
    if (
      node.autoIncrement === true ||
      node.identity === true ||
      node.generated?.identity === true
    ) {
      throw new TypeError(
        "Minnow does not support serial/identity/auto-increment DDL in SQL. Declare the " +
          "column with Minnow's schema DSL (column.integer().autoIncrement()) and migrate, or " +
          "default the column to nextval() of a CREATE SEQUENCE sequence.",
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
