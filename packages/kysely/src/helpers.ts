import {
  AliasNode,
  ColumnNode,
  ExpressionWrapper,
  IdentifierNode,
  ReferenceNode,
  SelectQueryNode,
  TableNode,
  ValueNode,
  sql,
  type Expression,
  type RawBuilder,
  type ShallowDehydrateObject,
  type ShallowDehydrateValue,
  type Simplify,
} from "kysely";

interface JsonObjectPair {
  readonly key: Expression<unknown>;
  readonly value: Expression<unknown>;
}

function literal(value: string): Expression<unknown> {
  return new ExpressionWrapper(ValueNode.createImmediate(value));
}

function column(table: string, name: string): Expression<unknown> {
  return new ExpressionWrapper(
    ReferenceNode.create(ColumnNode.create(name), TableNode.create(table)),
  );
}

/** Explicit output names and their references in one derived query. */
function jsonObjectPairs(expression: Expression<unknown>, table: string): JsonObjectPair[] {
  const node = expression.toOperationNode();
  if (!SelectQueryNode.is(node)) {
    throw new TypeError("jsonArrayFrom and jsonObjectFrom require a select query");
  }
  try {
    return (node.selections ?? []).map(({ selection }) => {
      if (ReferenceNode.is(selection) && ColumnNode.is(selection.column)) {
        const name = selection.column.column.name;
        return { key: literal(name), value: column(table, name) };
      }
      if (ColumnNode.is(selection)) {
        const name = selection.column.name;
        return { key: literal(name), value: column(table, name) };
      }
      if (AliasNode.is(selection) && IdentifierNode.is(selection.alias)) {
        const name = selection.alias.name;
        return { key: literal(name), value: column(table, name) };
      }
      throw new TypeError("selection has no explicit output name");
    });
  } catch {
    throw new TypeError(
      "jsonArrayFrom and jsonObjectFrom require explicit selections; selectAll() is not supported",
    );
  }
}

function jsonObjectExpression(pairs: readonly JsonObjectPair[]): RawBuilder<unknown> {
  const members = pairs.map(({ key, value }) => sql`${key} VALUE ${value}`);
  return sql`JSON_OBJECT(${sql.join(members)})`;
}

/**
 * Aggregates an explicitly selected query into a JSON array of its result objects.
 *
 * Enable `resultDecoding: { json: "parse" }` so the runtime value matches the inferred object
 * array. The subquery's ORDER BY, LIMIT, and OFFSET apply independently for every correlated
 * outer row. Empty inputs return `[]`.
 */
export function jsonArrayFrom<O>(
  expression: Expression<O>,
): RawBuilder<Array<Simplify<ShallowDehydrateObject<O>>>> {
  const object = jsonObjectExpression(jsonObjectPairs(expression, "__minnow_json_array"));
  return sql<Array<Simplify<ShallowDehydrateObject<O>>>>`COALESCE(
    (SELECT JSON_ARRAYAGG(${object}) FROM ${expression} AS "__minnow_json_array"),
    JSON_ARRAY()
  )`;
}

/**
 * Turns the single row returned by an explicitly selected query into a JSON object.
 *
 * Enable `resultDecoding: { json: "parse" }` so the runtime value matches the inferred object.
 * Zero rows return NULL; more than one row raises the normal scalar-subquery cardinality error.
 */
export function jsonObjectFrom<O>(
  expression: Expression<O>,
): RawBuilder<Simplify<ShallowDehydrateObject<O>> | null> {
  const object = jsonObjectExpression(jsonObjectPairs(expression, "__minnow_json_object"));
  return sql<Simplify<ShallowDehydrateObject<O>> | null>`(
    SELECT ${object} FROM ${expression} AS "__minnow_json_object"
  )`;
}

/** Minnow's SQL-standard JSON_OBJECT constructor with Kysely's exact keyed result inference. */
export function jsonBuildObject<O extends Record<string, Expression<unknown>>>(
  object: O,
): RawBuilder<
  Simplify<{
    [K in keyof O]: O[K] extends Expression<infer V> ? ShallowDehydrateValue<V> : never;
  }>
> {
  const pairs = Object.keys(object).map((key) => {
    const value = object[key];
    if (value === undefined) throw new TypeError(`Missing JSON object expression: ${key}`);
    return { key: literal(key), value };
  });
  return jsonObjectExpression(pairs) as RawBuilder<
    Simplify<{
      [K in keyof O]: O[K] extends Expression<infer V> ? ShallowDehydrateValue<V> : never;
    }>
  >;
}
