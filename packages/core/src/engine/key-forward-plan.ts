import type { TableSource } from "../plan/model.js";
import type { TableColumnRecord, TableRecord } from "../storage/types.js";
import type { BatchValue } from "./batch.js";
import {
  addressStorageKey,
  storageKeyToken,
  tableAddress,
  type KeyForward,
  type KeyForwardLookup,
  type TableAddress,
} from "./key-forwards.js";
import { clonePlanTree, type CompiledQuery, type Expression } from "./query.js";
import { externalSqlTextValue } from "./sql-domains.js";

/**
 * SQL's side of rekeying: a literal or bound parameter compared by equality to a primary-key or
 * foreign-key column, naming a retired key, is replaced with the current key of the row it
 * names — before planning, so the keyed point read, index probes, and zone pruning all see the
 * current key. Stored rows only ever hold current keys, so joins and ranges need nothing here.
 *
 * Recognized: `column = value`, `value = column`, `column IN (values)`, `IS NOT DISTINCT FROM`,
 * and their negations (`<>`, `NOT IN`, `IS DISTINCT FROM`), wherever they appear in a block's
 * WHERE, HAVING, or ON, nested under AND, OR, NOT, and `IS` tests, in every block of the statement — subqueries, derived tables, CTEs, and view
 * bodies included. A value is a constant or a bound parameter, negated or not (`-1`, `-$1`). A
 * column of a derived table or view that passes a key column through counts as that key column.
 * A composite key is followed when one block's top-level conjunction pins every one of its
 * columns, on one source, with `=`.
 */

type LiteralExpression = Extract<Expression, { kind: "literal" }>;

/**
 * A key constant in the plan: a literal, or a negated one, which the parser spells
 * `-1 * literal` and which constant folding only collapses later, at execution.
 */
interface KeyConstant {
  readonly node: Expression;
  readonly literal: LiteralExpression;
}

/** One equality the rewrite can follow: the constants of a key, in key-column order. */
interface KeySite {
  readonly tableId: string;
  readonly address: TableAddress;
  readonly columns: readonly TableColumnRecord[];
  /** Each entry is one alternative key: one constant per key column. */
  readonly tuples: ReadonlyArray<readonly KeyConstant[]>;
}

/** A column reference resolved to the catalog column it reads, and the source it reads from. */
interface ResolvedColumn {
  readonly table: TableRecord;
  readonly column: TableColumnRecord;
  readonly source: TableSource;
}

/**
 * Equality and its negations. The optimizer turns `NOT (id = -1)` into `id <> -1` before this
 * pass runs, so a negation must follow too, or `p` and `NOT p` would disagree about a row.
 */
const EQUALITY_OPERATORS = new Set([
  "=",
  "IS NOT DISTINCT FROM",
  "IN",
  "<>",
  "!=",
  "IS DISTINCT FROM",
  "NOT IN",
]);

/** The operators whose value side is a list of alternatives. */
const LIST_OPERATORS = new Set(["IN", "NOT IN"]);

/** Whether a source is a catalog table rather than a derived, set, windowed, or recursive one. */
function isTableSource(source: TableSource): boolean {
  return (
    source.derived === undefined &&
    source.union === undefined &&
    source.windowed === undefined &&
    source.recursive === undefined
  );
}

/**
 * The key constant an expression is, if it is one. With `placeholders`, an unbound parameter
 * counts too: a prepared template asks where a bound value could name a key.
 */
function keyConstant(expression: Expression, placeholders = false): KeyConstant | undefined {
  if (expression.kind === "literal") return { node: expression, literal: expression };
  if (
    placeholders &&
    (expression.kind === "parameter" ||
      (expression.kind === "binary" &&
        expression.operator === "*" &&
        expression.left.kind === "literal" &&
        expression.left.value === -1 &&
        expression.right.kind === "parameter"))
  ) {
    return { node: expression, literal: { kind: "literal", value: null } };
  }
  if (
    expression.kind === "binary" &&
    expression.operator === "*" &&
    expression.left.kind === "literal" &&
    expression.left.value === -1 &&
    expression.right.kind === "literal" &&
    typeof expression.right.value === "number" &&
    expression.right.internalSqlValue !== true
  ) {
    return { node: expression, literal: { kind: "literal", value: -expression.right.value } };
  }
  return undefined;
}

/** Turns a key constant's node, in place, into a literal holding `value`. */
function setKeyConstant(constant: KeyConstant, value: LiteralExpression["value"]): void {
  if (constant.node.kind === "literal") {
    constant.node.value = value;
    return;
  }
  const node = constant.node as Record<string, unknown>;
  for (const key of Object.keys(node)) Reflect.deleteProperty(node, key);
  Object.assign(node, { kind: "literal", value } satisfies LiteralExpression);
}

/** What a key column's equality resolves through: its own table's key, or a parent's. */
interface KeyTarget {
  readonly tableId: string;
  readonly address: TableAddress;
  /** The key columns, in the order the target's address lists them. */
  readonly columns: readonly TableColumnRecord[];
}

/** Every key a table's columns take part in: its own primary key and each foreign key. */
function keyTargets(table: TableRecord, tables: ReadonlyMap<string, TableRecord>): KeyTarget[] {
  const targets: KeyTarget[] = [];
  const own = tableAddress(table);
  if (own !== undefined) targets.push({ tableId: table.id, address: own, columns: own.columns });
  for (const key of table.foreignKeys ?? []) {
    const parent = tables.get(key.parentTable);
    const address = parent === undefined ? undefined : tableAddress(parent);
    if (parent === undefined || address === undefined) continue;
    const columns = key.columns.map((name) =>
      table.columns.find((column) => column.name === name && column.hidden !== true),
    );
    if (columns.some((column) => column === undefined)) continue;
    targets.push({
      tableId: parent.id,
      address,
      columns: columns as TableColumnRecord[],
    });
  }
  return targets;
}

/** The stored form of a literal compared to a key column, when the literal can be one. */
function storedKeyLiteral(
  column: TableColumnRecord,
  literal: LiteralExpression,
): Exclude<BatchValue, null> | undefined {
  const value = literal.value;
  if (value === null) return undefined;
  if (column.sqlDomain !== undefined) {
    // Only a literal the domain pass already normalized carries the stored value.
    return literal.internalSqlValue === true && typeof value !== "object" ? value : undefined;
  }
  switch (column.type) {
    case "string":
      // A text value the engine stores protected never round-trips through a plain literal.
      return typeof value === "string" && externalSqlTextValue(value) === value ? value : undefined;
    case "number":
      return typeof value === "number" && Number.isFinite(value) ? value : undefined;
    case "boolean":
      return typeof value === "boolean" ? value : undefined;
    case "datetime":
      return value instanceof Date ? value : undefined;
  }
}

/** The literal form of a current key value for the column the literal is compared to. */
function keyLiteralValue(column: TableColumnRecord, value: BatchValue): BatchValue | undefined {
  if (column.sqlDomain !== undefined) return value;
  if (column.type === "string" && typeof value === "string" && value.startsWith("\u0000")) {
    return undefined;
  }
  return value;
}

/** Calls `visit` for every subquery block an expression contains, at any depth. */
function forEachSubquery(value: unknown, visit: (block: CompiledQuery) => void): void {
  if (typeof value !== "object" || value === null || value instanceof Date) return;
  if (Array.isArray(value)) {
    for (const item of value) forEachSubquery(item, visit);
    return;
  }
  const node = value as { kind?: unknown; block?: CompiledQuery };
  if ((node.kind === "subquery" || node.kind === "exists") && node.block !== undefined) {
    visit(node.block);
    return;
  }
  for (const child of Object.values(node)) forEachSubquery(child, visit);
}

/**
 * Calls `visit` for every key equality in the plan whose constants could name a retired key.
 * The nodes it passes belong to the plan walked, so a visit may rewrite them in place.
 */
function visitKeySites(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, TableRecord>,
  visit: (site: KeySite) => void,
  placeholders = false,
): void {
  const targetsByTable = new Map<string, KeyTarget[]>();
  const targetsOf = (table: TableRecord): KeyTarget[] => {
    let targets = targetsByTable.get(table.id);
    if (targets === undefined) {
      targets = keyTargets(table, tables);
      targetsByTable.set(table.id, targets);
    }
    return targets;
  };

  /** Resolves a reference among one block's sources, then its enclosing blocks'. */
  const resolveIn = (
    sources: readonly TableSource[],
    outer: readonly TableSource[],
    reference: string,
  ): ResolvedColumn | undefined => {
    const columnOf = (source: TableSource, name: string): ResolvedColumn | undefined => {
      if (isTableSource(source)) {
        const table = tables.get(source.table);
        const column = table?.columns.find(
          (candidate) => candidate.name === name && candidate.hidden !== true,
        );
        return table === undefined || column === undefined ? undefined : { table, column, source };
      }
      // A derived table or view: its column counts when it passes a catalog column through.
      const block = source.derived;
      if (block === undefined || source.union !== undefined) return undefined;
      const index = source.columnAliases?.indexOf(name) ?? -1;
      const items = block.select.filter((item, position) =>
        index === -1 ? item.alias === name : position === index,
      );
      const item = items.length === 1 ? items[0] : undefined;
      if (item?.expression.kind !== "column") return undefined;
      const inner = resolveIn([block.base, ...block.joins], [], item.expression.reference);
      return inner === undefined ? undefined : { ...inner, source };
    };
    /** Whether a source may expose the name: a derived source may expose any name. */
    const exposes = (source: TableSource, name: string): boolean =>
      !isTableSource(source) || columnOf(source, name) !== undefined;
    const separator = reference.indexOf(".");
    if (separator !== -1) {
      const alias = reference.slice(0, separator);
      const source = [...sources, ...outer].find((candidate) => candidate.alias === alias);
      return source === undefined ? undefined : columnOf(source, reference.slice(separator + 1));
    }
    // The innermost block that has the name decides it, as SQL resolves names; a name two of
    // its sources could expose is ambiguous here and left alone.
    for (const level of [sources, outer]) {
      const candidates = level.filter((source) => exposes(source, reference));
      if (candidates.length === 0) continue;
      const only = candidates.length === 1 ? candidates[0] : undefined;
      return only === undefined ? undefined : columnOf(only, reference);
    }
    return undefined;
  };

  const walkBlock = (block: CompiledQuery, outer: readonly TableSource[]): void => {
    const sources = [block.base, ...block.joins];
    for (const source of sources) {
      const inner = source.lateral === true ? [...sources, ...outer] : outer;
      if (source.derived !== undefined) walkBlock(source.derived, inner);
      for (const member of source.union?.blocks ?? []) walkBlock(member, inner);
      if (source.windowed !== undefined) walkBlock(source.windowed.block, inner);
      if (source.recursive !== undefined) {
        walkBlock(source.recursive.base, inner);
        walkBlock(source.recursive.step, inner);
      }
    }
    const scope = [...sources, ...outer];
    const resolve = (reference: string): ResolvedColumn | undefined =>
      resolveIn(sources, outer, reference);
    const scalarSite = (columnSide: Expression, valueSide: Expression, operator: string): void => {
      if (columnSide.kind !== "column") return;
      const constants: KeyConstant[] = [];
      if (LIST_OPERATORS.has(operator)) {
        if (valueSide.kind !== "list") return;
        for (const item of valueSide.items) {
          const constant = keyConstant(item, placeholders);
          if (constant !== undefined) constants.push(constant);
        }
      } else {
        const constant = keyConstant(valueSide, placeholders);
        if (constant !== undefined) constants.push(constant);
      }
      if (constants.length === 0) return;
      const resolved = resolve(columnSide.reference);
      if (resolved === undefined) return;
      for (const target of targetsOf(resolved.table)) {
        if (target.columns.length !== 1 || target.columns[0]?.id !== resolved.column.id) continue;
        visit({
          tableId: target.tableId,
          address: target.address,
          columns: target.columns,
          tuples: constants.map((constant) => [constant]),
        });
      }
    };
    const walkPredicate = (expression: Expression | undefined): void => {
      if (expression === undefined) return;
      switch (expression.kind) {
        case "condition":
          if (EQUALITY_OPERATORS.has(expression.operator)) {
            scalarSite(expression.left, expression.right, expression.operator);
            if (!LIST_OPERATORS.has(expression.operator)) {
              scalarSite(expression.right, expression.left, expression.operator);
            }
          }
          // A boolean operand of a condition is a predicate too. A WHERE or HAVING that is not
          // a plain conjunction reaches the plan as one `(expression) IS TRUE` predicate, and
          // `(p) IS NULL` wraps `p` the same way: the equalities inside must follow alike, or
          // `p`, `NOT p`, and `p IS NULL` would stop partitioning the rows.
          walkPredicate(expression.left);
          walkPredicate(expression.right);
          return;
        case "logical":
          walkPredicate(expression.left);
          walkPredicate(expression.right);
          return;
        case "not":
          walkPredicate(expression.operand);
          return;
        default:
          return;
      }
    };
    for (const predicate of [...block.predicates, ...block.having]) {
      walkPredicate({ kind: "condition", ...predicate });
    }
    for (const join of block.joins) walkPredicate(join.on);
    // Every subquery in the block, wherever it sits, is a block of its own.
    forEachSubquery(
      [
        block.select,
        block.predicates,
        block.having,
        block.groupBy,
        block.orderBy,
        block.joins.map(({ left, right, on }) => [left, right, on]),
      ],
      (subquery) => {
        walkBlock(subquery, scope);
      },
    );
    // Composite keys: every column pinned by `=` on one source in the block's own conjunction.
    const pinned = new Map<
      TableSource,
      { table: TableRecord; columns: Map<string, KeyConstant> }
    >();
    for (const predicate of block.predicates) {
      if (predicate.operator !== "=") continue;
      for (const [columnSide, valueSide] of [
        [predicate.left, predicate.right],
        [predicate.right, predicate.left],
      ] as const) {
        if (columnSide.kind !== "column") continue;
        const constant = keyConstant(valueSide, placeholders);
        if (constant === undefined) continue;
        const resolved = resolve(columnSide.reference);
        if (resolved === undefined) continue;
        let entry = pinned.get(resolved.source);
        if (entry === undefined) {
          entry = { table: resolved.table, columns: new Map() };
          pinned.set(resolved.source, entry);
        }
        // A view may pass through columns of several joined tables; pin each table's own.
        if (entry.table.id === resolved.table.id) entry.columns.set(resolved.column.id, constant);
      }
    }
    for (const { table, columns } of pinned.values()) {
      for (const target of targetsOf(table)) {
        if (target.columns.length < 2) continue;
        const constants = target.columns.map((column) => columns.get(column.id));
        if (constants.some((constant) => constant === undefined)) continue;
        visit({
          tableId: target.tableId,
          address: target.address,
          columns: target.columns,
          tuples: [constants as KeyConstant[]],
        });
      }
    }
  };
  walkBlock(plan, []);
}

/** The forward a site's tuple names, if it names a retired key. */
function siteForward(
  lookup: KeyForwardLookup,
  site: Pick<KeySite, "tableId" | "address" | "columns">,
  tuple: readonly KeyConstant[],
): KeyForward | undefined {
  const values: Array<Exclude<BatchValue, null>> = [];
  for (const [position, constant] of tuple.entries()) {
    const column = site.columns[position];
    const value = column === undefined ? undefined : storedKeyLiteral(column, constant.literal);
    if (value === undefined) return undefined;
    values.push(value);
  }
  let token: string;
  try {
    token = storageKeyToken(addressStorageKey(site.address, values));
  } catch {
    return undefined;
  }
  return lookup.get(site.tableId, token);
}

/**
 * Whether the plan compares any primary-key or foreign-key column to a constant — or, with
 * `placeholders`, to an unbound parameter that will become one.
 */
export function planHasKeyLiterals(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, TableRecord>,
  placeholders = false,
): boolean {
  let found = false;
  visitKeySites(
    plan,
    tables,
    () => {
      found = true;
    },
    placeholders,
  );
  return found;
}

/**
 * The plan with every key constant naming a retired key replaced by the current key. A plan
 * with nothing to follow is returned as is, so the shared compiled plan is only cloned when
 * needed.
 */
export function forwardPlanKeyLiterals(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, TableRecord>,
  lookup: KeyForwardLookup,
): CompiledQuery {
  const probe = { needed: false };
  visitKeySites(plan, tables, (site) => {
    if (!probe.needed && lookup.hasTable(site.tableId)) {
      probe.needed = site.tuples.some((tuple) => siteForward(lookup, site, tuple) !== undefined);
    }
  });
  if (!probe.needed) return plan;
  const clone = clonePlanTree(plan);
  visitKeySites(clone, tables, (site) => {
    if (!lookup.hasTable(site.tableId)) return;
    for (const tuple of site.tuples) {
      const forward = siteForward(lookup, site, tuple);
      if (forward === undefined) continue;
      const replacements = tuple.map((_, position) => {
        const column = site.columns[position];
        return column === undefined
          ? undefined
          : keyLiteralValue(column, forward.toValues[position] ?? null);
      });
      if (replacements.some((value) => value === undefined)) continue;
      for (const [position, constant] of tuple.entries()) {
        setKeyConstant(constant, replacements[position] as LiteralExpression["value"]);
      }
    }
  });
  return clone;
}

/**
 * The point read's side of the same rule: equalities that pin a primary key or a foreign key
 * naming a retired key are rewritten to the current key. Returns the input when nothing
 * forwards, so a table with forwards keeps the fast path for every live key.
 */
export function forwardKeyEqualities<
  T extends { readonly column: string; readonly value: unknown },
>(
  table: TableRecord,
  equalities: readonly T[],
  tables: ReadonlyMap<string, TableRecord>,
  lookup: KeyForwardLookup,
): readonly T[] {
  let forwarded: T[] | undefined;
  for (const target of keyTargets(table, tables)) {
    if (!lookup.hasTable(target.tableId)) continue;
    const positions = target.columns.map((column) =>
      (forwarded ?? equalities).findIndex((equality) => equality.column === column.name),
    );
    if (positions.some((position) => position === -1)) continue;
    const constants = positions.map((position): KeyConstant => {
      const literal: LiteralExpression = {
        kind: "literal",
        value: (forwarded ?? equalities)[position]?.value as LiteralExpression["value"],
      };
      return { node: literal, literal };
    });
    const forward = siteForward(lookup, target, constants);
    if (forward === undefined) continue;
    const replacements = target.columns.map((column, index) =>
      keyLiteralValue(column, forward.toValues[index] ?? null),
    );
    if (replacements.some((value) => value === undefined)) continue;
    forwarded ??= [...equalities];
    for (const [index, position] of positions.entries()) {
      const equality = forwarded[position];
      if (equality !== undefined) forwarded[position] = { ...equality, value: replacements[index] };
    }
  }
  return forwarded ?? equalities;
}
