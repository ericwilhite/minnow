/** Native catalog descriptions stay logical and typed; no 0.x Catalog/TableDefinition cast. */
import type {
  CatalogDescription,
  ExplainDescription,
} from "../../../core/dist/next/owner/inspection.js";
import type { EditorSchema } from "../../dist/editor/editor.js";
import { el } from "../../dist/dom.js";

export function editorSchema(catalog: CatalogDescription): EditorSchema {
  const schema: EditorSchema = Object.create(null) as EditorSchema;
  for (const relation of [...catalog.tables, ...catalog.views])
    Object.defineProperty(schema, relation.name, {
      enumerable: true,
      configurable: true,
      value: relation.columns.map((column) => column.name),
    });
  return schema;
}
export function catalogRelations(catalog: CatalogDescription) {
  return [
    ...catalog.tables.map((table) => ({
      key: `table:${String(table.id)}`,
      kind: "table" as const,
      name: table.name,
      columns: table.columns,
      managed: table.managed,
    })),
    ...catalog.views.map((view) => ({
      key: `view:${String(view.id)}`,
      kind: "view" as const,
      name: view.name,
      columns: view.columns,
      managed: view.managed,
    })),
  ];
}
export function renderCatalog(catalog: CatalogDescription): HTMLElement {
  const node = el("div", { class: "native-catalog" });
  for (const relation of catalogRelations(catalog)) {
    const title = el("summary", {
      text: `${relation.name} (${relation.kind}${relation.managed ? ", managed" : ""})`,
    });
    const columns = el(
      "ul",
      {},
      relation.columns.map((column) =>
        el("li", {
          text: `${column.name}: ${column.sqlType}${column.nullable ? "" : " NOT NULL"}`,
        }),
      ),
    );
    const section = el("details", { attrs: { "data-relation": relation.key } }, [title, columns]);
    if (relation.kind === "table") {
      const table = catalog.tables.find((table) => `table:${String(table.id)}` === relation.key);
      if (table) {
        for (const column of table.columns) {
          const annotations = [
            column.defaultSql === null ? null : `DEFAULT ${column.defaultSql}`,
            column.generatedSql === null ? null : `GENERATED ${column.generatedSql}`,
            column.identity === null
              ? null
              : `IDENTITY ${column.identity.always ? "ALWAYS" : "BY DEFAULT"}`,
          ];
          for (const annotation of annotations)
            if (annotation !== null)
              section.append(el("p", { text: `${column.name}: ${annotation}` }));
        }
        for (const index of table.indexes)
          section.append(
            el("p", {
              text: `${index.name}: ${index.role}${index.unique ? " UNIQUE" : ""} (${index.columnIds.map((id, i) => `${table.columns.find((column) => column.id === id)?.name ?? String(id)} ${index.directions[i] ?? "asc"}`).join(", ")}) — ${index.state}`,
            }),
          );
        for (const check of table.checks)
          section.append(el("p", { text: `${check.name}: CHECK (${check.sql})` }));
        for (const fk of table.foreignKeys)
          section.append(
            el("p", {
              text: `${fk.name}: REFERENCES ${fk.parentName ?? `table ${String(fk.parentTableId)}`} ON DELETE ${fk.onDelete} ON UPDATE ${fk.onUpdate}${fk.enforced ? "" : " NOT ENFORCED"}`,
            }),
          );
        for (const trigger of catalog.triggers.filter((trigger) => trigger.tableId === table.id))
          section.append(
            el("p", { text: `${trigger.name}: ${trigger.timing} ${trigger.event} (definition)` }),
          );
        for (const fts of catalog.fts.filter((fts) => fts.tableId === table.id))
          section.append(
            el("p", {
              text: `FTS ${String(fts.id)}: column ${table.columns.find((column) => column.id === fts.columnId)?.name ?? String(fts.columnId)} (definition only)`,
            }),
          );
      }
    } else {
      const view = catalog.views.find((view) => `view:${String(view.id)}` === relation.key);
      if (view) section.append(el("pre", { text: view.sql }));
    }
    node.append(section);
  }
  for (const type of catalog.types)
    node.append(el("p", { text: `${type.name}: ENUM (${type.labels.join(", ")})` }));
  for (const sequence of catalog.sequences)
    node.append(
      el("p", {
        text: `${sequence.name}: SEQUENCE START ${sequence.start} INCREMENT ${sequence.increment} MINVALUE ${sequence.min} MAXVALUE ${sequence.max}${sequence.cycle ? " CYCLE" : " NO CYCLE"} (definition only)`,
      }),
    );
  return node;
}
export function renderExplain(explain: ExplainDescription): HTMLElement {
  return el("pre", { class: "native-explain", text: explain.text });
}
