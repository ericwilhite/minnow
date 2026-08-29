import type { AnyTable, SchemaDefinition } from "@minnowdb/core";
import {
  ColumnNode,
  DefaultInsertValueNode,
  InsertQueryNode,
  MergeQueryNode,
  PostgresQueryCompiler,
  PrimitiveValueListNode,
  ValueListNode,
  ValuesNode,
  type AlterTableNode,
  type CompiledQuery,
  type DeleteQueryNode,
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
    // Minnow's MERGE reports an affected-row count but carries no returned rows, so a
    // `mergeInto(...).returning(...)` would type-check and then silently yield []. Refuse it
    // here, before execution, instead of returning nothing.
    if (MergeQueryNode.is(node) && node.returning !== undefined) {
      throw new TypeError(
        "Minnow does not support RETURNING on MERGE statements. Remove .returning()/.returningAll() " +
          "and read the affected-row count, or query the rows separately after the merge.",
      );
    }
    return super.compileQuery(normalizeInsert(node, this.#tables), queryId);
  }

  protected override visitSelectQuery(node: SelectQueryNode): void {
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

  protected override visitUpdateQuery(node: UpdateQueryNode): void {
    if (node.from !== undefined) {
      throw new TypeError(
        "Minnow does not support UPDATE ... FROM. Rewrite the update with a correlated " +
          "subquery or WHERE ... IN (SELECT ...).",
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

  protected override visitAlterTable(node: AlterTableNode): void {
    if (node.renameTo !== undefined) {
      throw new TypeError(
        "Minnow does not support ALTER TABLE ... RENAME TO. Create the new table, copy the " +
          "rows with INSERT ... SELECT, and drop the old one.",
      );
    }
    if (node.columnAlterations?.some((alteration) => alteration.kind === "RenameColumnNode")) {
      throw new TypeError(
        "Minnow does not support ALTER TABLE ... RENAME COLUMN. Add the new column, copy the " +
          "values with UPDATE, and drop the old column.",
      );
    }
    super.visitAlterTable(node);
  }
}
