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
 * Recognized: `column = value`, `value = column`, `column IN (values)`, `column = ANY (values)`
 * and `IS NOT DISTINCT FROM`, wherever they appear in a block's WHERE, HAVING, or ON, nested
 * under AND, OR, and NOT. A composite key is followed when one block's top-level conjunction
 * pins every one of its columns with `=`.
 */

type LiteralExpression = Extract<Expression, { kind: "literal" }>;

/** One equality the rewrite can follow: the literals of a key, in key-column order. */
interface KeySite {
  readonly tableId: string;
  readonly address: TableAddress;
  readonly columns: readonly TableColumnRecord[];
  /** Each entry is one alternative key: one literal per key column. */
  readonly tuples: ReadonlyArray<readonly LiteralExpression[]>;
}

const EQUALITY_OPERATORS = new Set(["=", "IS NOT DISTINCT FROM", "IN", "= ANY"]);

/** Whether a source is a catalog table rather than a derived, set, windowed, or recursive one. */
function isTableSource(source: TableSource): boolean {
  return (
    source.derived === undefined &&
    source.union === undefined &&
    source.windowed === undefined &&
    source.recursive === undefined
  );
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

/**
 * Calls `visit` for every key equality in the plan whose literals could name a retired key. The
 * literal nodes it passes belong to the plan walked, so a visit may rewrite them in place.
 */
function visitKeySites(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, TableRecord>,
  visit: (site: KeySite) => void,
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
    const resolve = (
      reference: string,
    ): { table: TableRecord; column: TableColumnRecord } | undefined => {
      const columnOf = (source: TableSource, name: string) => {
        if (!isTableSource(source)) return undefined;
        const table = tables.get(source.table);
        const column = table?.columns.find(
          (candidate) => candidate.name === name && candidate.hidden !== true,
        );
        return table === undefined || column === undefined ? undefined : { table, column };
      };
      const separator = reference.indexOf(".");
      if (separator !== -1) {
        const alias = reference.slice(0, separator);
        const source = scope.find((candidate) => candidate.alias === alias);
        return source === undefined ? undefined : columnOf(source, reference.slice(separator + 1));
      }
      // The innermost block that has the name decides it, as SQL resolves names.
      for (const level of [sources, outer]) {
        const matches = level.flatMap((source) => {
          const found = columnOf(source, reference);
          // A derived source may also expose the name; then it is not a catalog column at all.
          if (found === undefined && !isTableSource(source)) return [undefined];
          return found === undefined ? [] : [found];
        });
        if (matches.length === 1) return matches[0];
        if (matches.length > 1) return undefined;
      }
      return undefined;
    };
    const scalarSite = (columnSide: Expression, valueSide: Expression, operator: string): void => {
      if (columnSide.kind !== "column") return;
      const literals: LiteralExpression[] = [];
      if (operator === "IN" || operator === "= ANY") {
        if (valueSide.kind !== "list") return;
        for (const item of valueSide.items) if (item.kind === "literal") literals.push(item);
      } else if (valueSide.kind === "literal") {
        literals.push(valueSide);
      }
      if (literals.length === 0) return;
      const resolved = resolve(columnSide.reference);
      if (resolved === undefined) return;
      for (const target of targetsOf(resolved.table)) {
        if (target.columns.length !== 1 || target.columns[0]?.id !== resolved.column.id) continue;
        visit({
          tableId: target.tableId,
          address: target.address,
          columns: target.columns,
          tuples: literals.map((literal) => [literal]),
        });
      }
    };
    const walkExpression = (expression: Expression | undefined): void => {
      if (expression === undefined) return;
      switch (expression.kind) {
        case "condition":
          if (EQUALITY_OPERATORS.has(expression.operator)) {
            scalarSite(expression.left, expression.right, expression.operator);
            if (expression.operator === "=" || expression.operator === "IS NOT DISTINCT FROM") {
              scalarSite(expression.right, expression.left, expression.operator);
            }
          }
          walkExpression(expression.left);
          walkExpression(expression.right);
          return;
        case "logical":
          walkExpression(expression.left);
          walkExpression(expression.right);
          return;
        case "not":
          walkExpression(expression.operand);
          return;
        case "subquery":
        case "exists":
          walkBlock(expression.block, scope);
          return;
        case "list":
          for (const item of expression.items) walkExpression(item);
          return;
        default:
          return;
      }
    };
    for (const predicate of [...block.predicates, ...block.having]) {
      walkExpression({ kind: "condition", ...predicate });
    }
    for (const join of block.joins) walkExpression(join.on);
    for (const item of block.select) {
      if (item.expression.kind === "subquery" || item.expression.kind === "exists") {
        walkExpression(item.expression);
      }
    }
    // Composite keys: every column pinned by `=` in the block's own conjunction.
    const pinned = new Map<string, Map<string, LiteralExpression>>();
    for (const predicate of block.predicates) {
      if (predicate.operator !== "=") continue;
      for (const [columnSide, valueSide] of [
        [predicate.left, predicate.right],
        [predicate.right, predicate.left],
      ] as const) {
        if (columnSide.kind !== "column" || valueSide.kind !== "literal") continue;
        const resolved = resolve(columnSide.reference);
        if (resolved === undefined) continue;
        let columns = pinned.get(resolved.table.id);
        if (columns === undefined) {
          columns = new Map();
          pinned.set(resolved.table.id, columns);
        }
        columns.set(resolved.column.id, valueSide);
      }
    }
    for (const [tableId, columns] of pinned) {
      const table = [...tables.values()].find((candidate) => candidate.id === tableId);
      if (table === undefined) continue;
      for (const target of targetsOf(table)) {
        if (target.columns.length < 2) continue;
        const literals = target.columns.map((column) => columns.get(column.id));
        if (literals.some((literal) => literal === undefined)) continue;
        visit({
          tableId: target.tableId,
          address: target.address,
          columns: target.columns,
          tuples: [literals as LiteralExpression[]],
        });
      }
    }
  };
  walkBlock(plan, []);
}

/** The forward a site's tuple names, if it names a retired key. */
function siteForward(
  lookup: KeyForwardLookup,
  site: KeySite,
  tuple: readonly LiteralExpression[],
): KeyForward | undefined {
  const values: Array<Exclude<BatchValue, null>> = [];
  for (const [position, literal] of tuple.entries()) {
    const column = site.columns[position];
    const value = column === undefined ? undefined : storedKeyLiteral(column, literal);
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

/** Whether the plan compares any primary-key or foreign-key column to a constant. */
export function planHasKeyLiterals(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, TableRecord>,
): boolean {
  let found = false;
  visitKeySites(plan, tables, () => {
    found = true;
  });
  return found;
}

/**
 * The plan with every key literal naming a retired key replaced by the current key. A plan with
 * nothing to follow is returned as is, so the shared compiled plan is only cloned when needed.
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
      for (const [position, literal] of tuple.entries()) {
        literal.value = replacements[position] as LiteralExpression["value"];
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
    const literals = positions.map((position): LiteralExpression => ({
      kind: "literal",
      value: (forwarded ?? equalities)[position]?.value as LiteralExpression["value"],
    }));
    const forward = siteForward(
      lookup,
      { tableId: target.tableId, address: target.address, columns: target.columns, tuples: [] },
      literals,
    );
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
