import type { AnyTable, SchemaDefinition } from "@minnowdb/core";
import {
  ColumnNode,
  DefaultInsertValueNode,
  InsertQueryNode,
  PostgresQueryCompiler,
  PrimitiveValueListNode,
  ValueListNode,
  ValuesNode,
  type CompiledQuery,
  type QueryId,
  type RootOperationNode,
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

/** PostgreSQL SQL compilation with Kysely's empty-object inserts normalized to valid SQL. */
export class MinnowQueryCompiler extends PostgresQueryCompiler {
  readonly #tables: ReadonlyMap<string, TableMetadata>;

  constructor(schema?: SchemaDefinition<readonly AnyTable[]>) {
    super();
    this.#tables = metadataByTable(schema);
  }

  override compileQuery(node: RootOperationNode, queryId: QueryId): CompiledQuery {
    return super.compileQuery(normalizeInsert(node, this.#tables), queryId);
  }
}
