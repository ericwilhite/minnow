import type { QueryRow, QueryValue } from "@minnowdb/core";
import { button, el, iconButton, icons } from "../dom.js";
import { dateIsoString } from "../date-value.js";
import { rowToJson, toInsertSql } from "../results/serialize.js";
import { foreignKeyFor, type ForeignKeyInfo, type TableInfo } from "./catalog.js";

export interface RowDetail {
  node: HTMLElement;
  /** Shows one row, or clears when there is none. */
  show(table: TableInfo | undefined, row: QueryRow | undefined): void;
}

export interface RowDetailDeps {
  /** Every table, so the child side of each relationship can be found. */
  catalog(): readonly TableInfo[];
  /** Opens a table filtered to the rows the relationship points at. */
  onFollow(table: string, column: string, value: QueryValue): void;
  onCopied(what: string): void;
  onClose(): void;
}

/** The value written out in full — JSON re-indented where it parses, dates as instants. */
export function detailText(value: QueryValue): string {
  if (value === null) return "NULL";
  if (value instanceof Date) return dateIsoString(value);
  if (typeof value === "string" && /^\s*[[{]/.test(value)) {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return String(value);
}

/** Relationships whose parent side is this table: the child rows that point at a row of it. */
export function childRelations(
  catalog: readonly TableInfo[],
  table: TableInfo,
): Array<{ child: TableInfo; key: ForeignKeyInfo }> {
  return catalog.flatMap((child) =>
    (child.foreignKeys ?? [])
      .filter((key) => key.parentTable === table.name && key.columns.length === 1)
      .map((key) => ({ child, key })),
  );
}

/**
 * One row, top to bottom: every column with its full value, and where the catalog knows a
 * relationship, the way across it — the parent row a foreign key points at, and the child rows
 * that point back. It is a sidebar beside the grid rather than a dialog, so a row can be read
 * while the next one is picked.
 */
export function createRowDetail(deps: RowDetailDeps): RowDetail {
  const title = el("span", { class: "side-title", text: "Row" });
  const close = iconButton("side-toggle", "Hide the row", icons.chevronRight);
  const body = el("div", { class: "detail-body" });
  const node = el("div", { class: "detail side" }, [
    el("div", { class: "side-head" }, [title, el("span", { class: "spacer" }), close]),
    body,
  ]);
  close.addEventListener("click", () => {
    deps.onClose();
  });

  async function copy(text: string, what: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
      deps.onCopied(what);
    } catch {
      // Clipboard access can be refused; the detail view has nothing to add.
    }
  }

  return {
    node,
    show: (table, row) => {
      if (table === undefined || row === undefined) {
        body.replaceChildren(
          el("div", { class: "rail-empty", text: "Select a row to read it here." }),
        );
        return;
      }
      const columns = table.columns.map((column) => column.name);
      const fields = table.columns.map((column) => {
        const value = row[column.name] ?? null;
        const key = foreignKeyFor(table, column.name);
        const parent = key?.parentColumns[0];
        const link =
          key === undefined || parent === undefined || value === null
            ? []
            : [
                (() => {
                  const follow = button("btn mini", `${key.parentTable} →`, {
                    title: `Open ${key.parentTable} where ${parent} = ${String(value)}`,
                  });
                  follow.addEventListener("click", () => {
                    deps.onFollow(key.parentTable, parent, value);
                  });
                  return follow;
                })(),
              ];
        return el("div", { class: "detail-field" }, [
          el("div", { class: "detail-label" }, [
            el("span", { class: "detail-name", text: column.name }),
            el("span", { class: "detail-type", text: column.typeLabel ?? column.type }),
            el("span", { class: "spacer" }),
            ...link,
          ]),
          el("pre", {
            class: `detail-value${value === null ? " null" : ""}`,
            text: detailText(value),
          }),
        ]);
      });

      const related = childRelations(deps.catalog(), table).flatMap(({ child, key }) => {
        const parent = key.parentColumns[0];
        const column = key.columns[0];
        if (parent === undefined || column === undefined) return [];
        const value = row[parent] ?? null;
        if (value === null) return [];
        const open = button("btn mini", `${child.name} →`, {
          title: `Open ${child.name} where ${column} = ${String(value)}`,
        });
        open.addEventListener("click", () => {
          deps.onFollow(child.name, column, value);
        });
        return [
          el("div", { class: "detail-relation" }, [
            el("span", { text: `${child.name}.${column}` }),
            el("span", { class: "spacer" }),
            open,
          ]),
        ];
      });

      const copyJson = button("btn mini", "Copy JSON");
      copyJson.addEventListener("click", () => {
        void copy(rowToJson(columns, row), "row as JSON");
      });
      const copyInsert = button("btn mini", "Copy INSERT");
      copyInsert.addEventListener("click", () => {
        void copy(toInsertSql(table.name, columns, [row]), "row as INSERT");
      });

      body.replaceChildren(
        el("div", { class: "detail-actions" }, [copyJson, copyInsert]),
        ...fields,
        ...(related.length === 0
          ? []
          : [el("div", { class: "index-group", text: "Related rows" }), ...related]),
      );
    },
  };
}
