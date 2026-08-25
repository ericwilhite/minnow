import {
  sql,
  type ExpressionBuilder,
  type RawBuilder,
  type SqlBool,
  type StringReference,
} from "kysely";

/** A visible Kysely column. Minnow searches every SQL type through its canonical text form. */
export type KyselySearchColumn<DB, TB extends keyof DB> = StringReference<DB, TB>;

/** MATCH and BM25 require at least one column. */
export type KyselySearchColumns<DB, TB extends keyof DB> = readonly [
  KyselySearchColumn<DB, TB>,
  ...Array<KyselySearchColumn<DB, TB>>,
];

function searchColumns<DB, TB extends keyof DB>(
  builder: ExpressionBuilder<DB, TB>,
  columns: KyselySearchColumns<DB, TB>,
): RawBuilder<unknown> {
  if (columns.length === 0) throw new RangeError("Full-text search needs at least one column");
  return sql.join(
    columns.map((column) => builder.ref(column)),
    sql`, `,
  );
}

/** A type-safe `MATCH(columns) AGAINST query` predicate. The query is always parameterized. */
function match<DB, TB extends keyof DB>(
  builder: ExpressionBuilder<DB, TB>,
  columns: KyselySearchColumns<DB, TB>,
  query: string,
): RawBuilder<SqlBool> {
  return sql<SqlBool>`MATCH(${searchColumns(builder, columns)}) AGAINST ${query}`;
}

/** A type-safe `BM25(columns) AGAINST query` ranking expression. */
function rank<DB, TB extends keyof DB>(
  builder: ExpressionBuilder<DB, TB>,
  columns: KyselySearchColumns<DB, TB>,
  query: string,
): RawBuilder<number> {
  return sql<number>`BM25(${searchColumns(builder, columns)}) AGAINST ${query}`;
}

/** Full-text predicates and ranking expressions for Kysely queries. */
export const search = { match, rank } as const;
