/**
 * The front end's passes, run over the new catalog (lower/lowerer.ts steps 1–3): what
 * `MinnowDatabase.#applyCatalogRewrites` and `createPreparedColumnarQuery` run today, with each
 * catalog fact read from access/'s `CatalogView`.
 *
 * Passes that only shape the current engine's execution are not run: key forwarding,
 * domain-literal normalization (lowering types constants by context), the timestamp-text and
 * integer-division annotations (types decide both), statement-clock substitution and WITH TIES
 * expansion (the IR has `clockSlot` and `sort.withTies`).
 */
import { expandedRowLock } from "../../engine/row-locking.js";
import type { CatalogView } from "../access/catalog.js";
import {
  bindPendingSelectShapes,
  compileQuery,
  compileStatement,
  compileWithDeclaredTypes,
  expandNaturalJoins,
  expandRowReferences,
  expandSourceColumnAliases,
  expandViewSources,
  expandRowLockCteViews,
  expressionColumns,
  extendGroupByWithKeyDependents,
  foldIdentifierCase,
  mergeJoinColumns,
  blockHasSubqueries,
  prepareColumnarShape,
  type CompiledStatement,
} from "../../engine/query.js";
import { optimizePlan, qualifyCorrelatedReferences } from "../../engine/optimizer.js";
import type { CompiledQuery } from "../../plan/model.js";

/** The catalog facts the passes read, derived once per (catalog version) and cached. */
export interface FrontEndCatalog {
  readonly columns: ReadonlyMap<string, readonly string[]>;
  readonly keys: ReadonlyMap<string, readonly string[]>;
  readonly views: ReadonlyMap<string, string>;
}

const facts = new WeakMap<CatalogView, FrontEndCatalog>();

export function frontEndCatalog(catalog: CatalogView): FrontEndCatalog {
  let cached = facts.get(catalog);
  if (cached !== undefined) return cached;
  const columns = new Map<string, readonly string[]>();
  const keys = new Map<string, readonly string[]>();
  for (const table of catalog.tables) {
    columns.set(
      table.name,
      table.columns.map((column) => column.name),
    );
    const primary = table.indexes.find((index) => index.id === table.primaryKey);
    if (primary !== undefined)
      keys.set(
        table.name,
        primary.columnIds.map((id) => table.columns.find((column) => column.id === id)?.name ?? ""),
      );
  }
  const views = new Map(catalog.views.map((view) => [view.name, view.bodySql]));
  cached = { columns, keys, views };
  facts.set(catalog, cached);
  return cached;
}

/** Parses one statement with declared widths (C5, R17.4). */
export function parseStatement(text: string): CompiledStatement {
  return compileWithDeclaredTypes(() => compileStatement(text));
}

/** Text that plainly starts a query: `compileStatement` would compile it with `compileQuery`. */
const QUERY_START = /^\s*(?:\(|(?:SELECT|WITH|VALUES|TABLE)\b)/iu;

/**
 * Parses one statement; a query is compiled once (`compileStatement` compiles a SELECT and
 * keeps only its text, which would double the parse on the plan-cache miss path).
 */
export function parseAny(
  text: string,
): { readonly query: CompiledQuery } | { readonly statement: CompiledStatement } {
  return QUERY_START.test(text) ? { query: parseQuery(text) } : { statement: parseStatement(text) };
}

/** Parses a SELECT with declared widths. */
export function parseQuery(text: string): CompiledQuery {
  return compileWithDeclaredTypes(() => compileQuery(text));
}

/**
 * Runs the catalog and shape passes over a parsed query, in the current engine's order; the
 * shape passes repeat the wildcard binding as `createPreparedColumnarQuery` does (a no-op then).
 */
export function prepareQuery(parsed: CompiledQuery, catalog: CatalogView): CompiledQuery {
  return compileWithDeclaredTypes(() => prepareTypedQuery(parsed, catalog));
}

function prepareTypedQuery(parsed: CompiledQuery, catalog: CatalogView): CompiledQuery {
  const front = frontEndCatalog(catalog);
  const columnsOf = (name: string): readonly string[] | undefined => front.columns.get(name);
  let plan = foldIdentifierCase(parsed, front.columns);
  plan = expandRowReferences(plan, columnsOf);
  const bodies = new Map<string, CompiledQuery>();
  const viewBody = (name: string): CompiledQuery | undefined => {
    const sql = front.views.get(name);
    if (sql === undefined) return undefined;
    let body = bodies.get(name);
    if (body === undefined) {
      body = parseQuery(sql);
      bodies.set(name, body);
    }
    return body;
  };
  if (front.views.size > 0) plan = expandViewSources(plan, viewBody);
  const rowLocking = expandedRowLock(
    parsed,
    bodies.values(),
    front.views.size === 0 ? undefined : (body) => expandRowLockCteViews(body, viewBody),
  );
  if (rowLocking !== undefined) plan.rowLocking = rowLocking;
  if (front.keys.size > 0)
    plan = extendGroupByWithKeyDependents(plan, (name) => front.keys.get(name), columnsOf);
  plan = bindPendingSelectShapes(plan, columnsOf);
  plan = mergeJoinColumns(plan, columnsOf);
  plan = expandSourceColumnAliases(plan, columnsOf);
  plan = expandNaturalJoins(plan, columnsOf);
  if (blockHasSubqueries(plan))
    plan = optimizePlan(qualifyCorrelatedReferences(plan, front.columns), { resolvedNames: true });
  checkHavingNames(plan, columnsOf);
  return prepareColumnarShape(plan, columnsOf);
}

/**
 * A HAVING condition's names resolve as PostgreSQL resolves them, before the grouping check
 * would call them ungrouped (42803): a qualifier no source has is 42P01 (missing FROM-clause
 * entry), a column no source has 42703, an unqualified name two sources have 42702 (unless a
 * USING join merged it). Output aliases are not in scope there, as in PostgreSQL. Checked only where every source is a catalog table, so its columns are
 * known; nested blocks are not checked here (their names may be correlated).
 */
function checkHavingNames(
  plan: CompiledQuery,
  columnsOf: (name: string) => readonly string[] | undefined,
): void {
  if (plan.having.length === 0) return;
  const sources = [plan.base, ...plan.joins];
  const known = sources.map((source) =>
    source.derived === undefined &&
    source.union === undefined &&
    source.recursive === undefined &&
    source.windowed === undefined &&
    source.columnAliases === undefined
      ? columnsOf(source.table)
      : undefined,
  );
  if (known.some((columns) => columns === undefined)) return;
  const merged = new Set(plan.joins.flatMap((join) => join.using ?? []));
  for (const predicate of plan.having)
    for (const reference of [predicate.left, predicate.right].flatMap(expressionColumns)) {
      const dot = reference.indexOf(".");
      if (dot === -1) {
        const owners = known.filter((columns) => columns?.includes(reference) === true).length;
        if (owners === 0) throw new TypeError(`column "${reference}" does not exist`);
        if (owners > 1 && !merged.has(reference))
          throw new TypeError(`column reference "${reference}" is ambiguous`);
        continue;
      }
      const qualifier = reference.slice(0, dot);
      const index = sources.findIndex((source) => (source.alias || source.table) === qualifier);
      if (index === -1) throw new TypeError(`missing FROM-clause entry for table "${qualifier}"`);
      if (known[index]?.includes(reference.slice(dot + 1)) !== true)
        throw new TypeError(`column ${reference} does not exist`);
    }
}
