import { el, icon, iconButton, icons } from "../dom.js";
import { readFlag, writeFlag } from "../storage.js";
import { isEditable, type ColumnInfo, type TableInfo } from "./catalog.js";

export interface SchemaRail {
  node: HTMLElement;
  setCatalog(catalog: readonly TableInfo[]): void;
  setSelected(table: string | undefined): void;
  setError(message: string): void;
}

export interface SchemaRailDeps {
  /** Namespaces the remembered collapsed state. */
  storageKey: string;
  /** Rereads the catalog — tables created after the panel opened are not there otherwise. */
  onRefresh(): void;
  onPickTable(table: TableInfo): void;
  /** A column was clicked — used to insert its name while writing a query. */
  onPickColumn(table: TableInfo, column: ColumnInfo): void;
  /** Expanding is independent of picking, so a table can be inspected without opening it. */
  onToggle?(table: TableInfo, expanded: boolean): void;
}

/**
 * Tables down the left, expanding to their columns. It sits beside every view rather than inside
 * the explorer, because knowing what the columns are called is as useful for writing a query as it
 * is for browsing a table.
 *
 * Row counts are deliberately absent: each one is a full scan, and fifty of them on open would be
 * the slowest thing the panel does.
 */
export function createSchemaRail(deps: SchemaRailDeps): SchemaRail {
  const search = el("input", {
    class: "rail-search",
    type: "text",
    attrs: { placeholder: "Filter tables…", "aria-label": "Filter tables" },
  });
  const refresh = iconButton("side-toggle", "Reload the tables", icons.refresh);
  const collapse = iconButton("side-toggle", "Hide the tables", icons.chevronLeft);
  const expand = iconButton("side-toggle", "Show the tables", icons.chevronRight);
  const list = el("div", { class: "rail-list" });
  const head = el("div", { class: "side-head" }, [search, refresh, collapse]);
  const node = el("div", { class: "rail side" }, [
    head,
    el("div", { class: "side-stub" }, [expand]),
    list,
  ]);

  refresh.addEventListener("click", () => {
    deps.onRefresh();
  });

  const collapsedKey = `${deps.storageKey}:rail-collapsed`;
  let collapsed = readFlag(collapsedKey, false);

  function applyCollapsed(): void {
    node.classList.toggle("collapsed", collapsed);
  }

  for (const [button, next] of [
    [collapse, true],
    [expand, false],
  ] as const) {
    button.addEventListener("click", () => {
      collapsed = next;
      writeFlag(collapsedKey, collapsed);
      applyCollapsed();
    });
  }

  let catalog: TableInfo[] = [];
  let selected: string | undefined;
  let expanded: string | undefined;
  let filter = "";

  function columnRow(table: TableInfo, column: ColumnInfo): HTMLElement {
    const row = el("button", { class: "col", type: "button" }, [
      el("span", { class: "col-name", text: column.name }),
      el("span", { class: "col-type", text: column.nullable ? `${column.type}?` : column.type }),
      ...(column.isUniqueKey
        ? [el("span", { class: "col-key", text: "key", title: "unique key" })]
        : []),
    ]);
    row.title = `Insert ${table.name}.${column.name}`;
    row.addEventListener("click", () => {
      deps.onPickColumn(table, column);
    });
    return row;
  }

  function tableRow(table: TableInfo): HTMLElement[] {
    const open = table.name === expanded;
    const chevron = el("button", {
      class: "tnode-toggle",
      type: "button",
      text: open ? "▾" : "▸",
      attrs: {
        "aria-label": open ? `Collapse ${table.name}` : `Expand ${table.name}`,
        "aria-expanded": String(open),
      },
    });
    chevron.addEventListener("click", () => {
      expanded = open ? undefined : table.name;
      deps.onToggle?.(table, !open);
      render();
    });

    const name = el("button", { class: "tnode-open", type: "button" }, [
      el("span", { class: "tnode-icon" }, [icon(icons.table)]),
      el("span", { class: "tnode-name", text: table.name }),
      el("span", {
        class: "tnode-meta",
        text: String(table.columns.length),
        title: `${String(table.columns.length)} columns`,
      }),
      ...(isEditable(table)
        ? []
        : [
            el("span", {
              class: "tnode-badge",
              text: "no key",
              title: "Rows cannot be edited without a unique key",
            }),
          ]),
    ]);
    name.addEventListener("click", () => {
      expanded = table.name;
      deps.onPickTable(table);
      render();
    });

    const row = el("div", { class: `tnode${table.name === selected ? " on" : ""}` }, [
      chevron,
      name,
    ]);
    return open
      ? [
          row,
          el(
            "div",
            { class: "cols" },
            table.columns.map((column) => columnRow(table, column)),
          ),
        ]
      : [row];
  }

  function render(): void {
    const needle = filter.trim().toLowerCase();
    const shown = catalog.filter((table) => table.name.toLowerCase().includes(needle));
    if (shown.length === 0) {
      list.replaceChildren(
        el("div", {
          class: "rail-empty",
          text: catalog.length === 0 ? "No tables yet." : "No table matches.",
        }),
      );
      return;
    }
    list.replaceChildren(
      el("div", { class: "rail-group", text: `Tables · ${String(catalog.length)}` }),
      ...shown.flatMap(tableRow),
    );
  }

  search.addEventListener("input", () => {
    filter = search.value;
    render();
  });

  render();
  applyCollapsed();

  return {
    node,
    setCatalog: (next) => {
      catalog = [...next];
      render();
    },
    setSelected: (table) => {
      selected = table;
      if (table !== undefined) expanded = table;
      render();
    },
    setError: (message) => {
      list.replaceChildren(el("div", { class: "rail-empty", text: message }));
    },
  };
}
