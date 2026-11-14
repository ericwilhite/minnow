/** Definition-time relation schemas for stored views, independent of later catalog growth. */
import type { CatalogView, TableDef, ViewDef, ViewSourceSchema } from "../access/catalog.js";
import type { CompiledQuery } from "../../plan/model.js";
import { catalogDeclarations } from "../../engine/catalog-ddl-sources.js";
import { PlanningError } from "./errors.js";
export interface BoundView {
  readonly view: ViewDef;
  readonly count: number;
}
interface Bindings {
  readonly tables: Map<string, TableDef>;
  readonly views: Map<string, BoundView>;
  readonly names: Map<ViewSourceSchema, string>;
}
const cache = new WeakMap<CatalogView, Bindings>();
export function viewSources(catalog: CatalogView): Bindings {
  const existing = cache.get(catalog);
  if (existing) return existing;
  const tables = new Map<string, TableDef>(),
    views = new Map<string, BoundView>(),
    names = new Map<ViewSourceSchema, string>();
  for (const owner of catalog.views)
    for (const schema of owner.sourceSchemas ?? []) {
      let name = `\u0000view:${String(owner.id)}:${schema.kind}:${String(schema.objectId)}`;
      while (
        catalog.tableByName(name) ||
        catalog.views.some((v) => v.name === name) ||
        tables.has(name) ||
        views.has(name)
      )
        name += "\u0000";
      if (schema.kind === "table") {
        const table = catalog.table(schema.objectId);
        if (!table) throw new PlanningError("Missing stored view source", "42P01");
        const columns = schema.columnIds.flatMap((id) => {
          const c = table.columns.find((c) => c.id === id);
          return c ? [c] : [];
        });
        tables.set(name, { ...table, name, columns });
      } else {
        const view = catalog.views.find((v) => v.id === schema.objectId);
        if (!view?.outputs) throw new PlanningError("Missing stored view output schema", "0A000");
        views.set(name, { view, count: schema.outputCount });
      }
      names.set(schema, name);
    }
  const result = { tables, views, names };
  cache.set(catalog, result);
  return result;
}
/** Live column names/types follow stable IDs; the definition-time order and visibility do not. */
export function bindViewSources(body: CompiledQuery, view: ViewDef, catalog: CatalogView): void {
  const bindings = viewSources(catalog),
    seen = new Set<object>();
  const sources = new Map(
    (view.sourceSchemas ?? []).map((schema) => {
      const relation =
        schema.kind === "table"
          ? catalog.table(schema.objectId)
          : catalog.views.find((v) => v.id === schema.objectId);
      if (!relation) throw new PlanningError("Missing stored view source", "42P01");
      return [relation.name, schema] as const;
    }),
  );
  const visit = (value: unknown): void => {
    if (value === null || typeof value !== "object" || seen.has(value)) return;
    seen.add(value);
    const node = value as Record<string, unknown>;
    if (node.kind === "literal") return;
    if (
      typeof node.table === "string" &&
      typeof node.alias === "string" &&
      !node.derived &&
      !node.union &&
      !node.recursive &&
      !node.windowed
    ) {
      const schema = sources.get(node.table),
        name = schema && bindings.names.get(schema);
      if (schema && name) {
        if (schema.kind === "table" && Array.isArray(node.columnAliases)) {
          const aliases = node.columnAliases as string[],
            table = catalog.table(schema.objectId);
          node.columnAliases = schema.columnIds.flatMap((id, ordinal) => {
            const col = table?.columns.find((c) => c.id === id);
            return col ? [aliases[ordinal] ?? col.name] : [];
          });
        }
        node.table = name;
      }
    }
    for (const child of Object.values(node)) visit(child);
    for (const child of catalogDeclarations(node) ?? []) visit(child);
  };
  visit(body);
}
