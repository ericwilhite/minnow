import { el, icon, iconButton, icons } from "../dom.js";
import { readFlag, writeFlag } from "../storage.js";
import {
  isEditable,
  type ColumnInfo,
  type ForeignKeyInfo,
  type IndexInfo,
  type TableInfo,
} from "./catalog.js";

export interface SchemaRail {
  node: HTMLElement;
  setCatalog(catalog: readonly TableInfo[]): void;
  setSelected(table: string | undefined): void;
  setError(message: string): void;
  /** Moves focus to the search box — the keyboard route to a table. */
  focusSearch(): void;
}

export interface SchemaRailDeps {
  /** Namespaces the remembered collapsed state. */
  storageKey: string;
  /** Rereads the catalog — tables created after the panel opened are not there otherwise. */
  onRefresh(): void;
  onPickTable(table: TableInfo): void;
  /** A column was clicked — used to insert its name while writing a query. */
  onPickColumn(table: TableInfo, column: ColumnInfo): void;
  /** An index was clicked — useful when composing DROP INDEX or inspecting its table. */
  onPickIndex(table: TableInfo, index: IndexInfo): void;
  /** A foreign key was clicked: a JOIN to write, or the parent table to open. */
  onPickForeignKey(table: TableInfo, key: ForeignKeyInfo): void;
  /** A view's SQL was clicked: the query to put in the editor, or the view to open. */
  onPickViewSql(table: TableInfo): void;
}

/** The line a column reads as: `NUMERIC(10,2)?`, the declared type with a mark for nullable. */
export function describeColumnType(column: ColumnInfo): string {
  return `${column.typeLabel ?? column.type}${column.nullable ? "?" : ""}`;
}

/** `customer_id → customers.customer_id`, and `on delete cascade` where it is not the default. */
export function describeForeignKey(key: ForeignKeyInfo): string {
  const from = key.columns.join(", ");
  const to = `${key.parentTable}.${key.parentColumns.join(", ")}`;
  const rule = key.onDelete === "restrict" ? "" : ` · on delete ${key.onDelete}`;
  return `${from} → ${to}${rule}${key.enforced ? "" : " · not enforced"}`;
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

  function badge(text: string, title: string, className = "col-badge"): HTMLElement {
    return el("span", { class: className, text, title });
  }

  function columnRow(table: TableInfo, column: ColumnInfo): HTMLElement {
    const row = el("button", { class: "col", type: "button" }, [
      el("span", { class: "col-name", text: column.name }),
      el("span", { class: "col-type", text: describeColumnType(column) }),
      ...(column.isUniqueKey ? [badge("key", "unique key", "col-key")] : []),
      ...(column.enumValues === undefined
        ? []
        : [badge("enum", `one of ${column.enumValues.join(", ")}`)]),
      ...(column.generated === undefined ? [] : [badge("gen", `generated: ${column.generated}`)]),
      ...(column.hasDefault === true ? [badge("auto", "filled by the engine when omitted")] : []),
    ]);
    row.title = `Insert ${table.name}.${column.name}`;
    row.addEventListener("click", () => {
      deps.onPickColumn(table, column);
    });
    return row;
  }

  function indexRow(table: TableInfo, index: IndexInfo): HTMLElement {
    const key = index.columns
      .map((column) => `${column.name}${column.direction === "desc" ? " ↓" : ""}`)
      .join(", ");
    const row = el("button", { class: "index", type: "button" }, [
      el("span", { class: "index-name", text: index.name }),
      el("span", { class: "index-key", text: key }),
      ...(index.unique ? [el("span", { class: "index-badge", text: "unique" })] : []),
      ...(index.state === "ready"
        ? []
        : [el("span", { class: `index-badge ${index.state}`, text: index.state })]),
    ]);
    row.title = `${index.unique ? "UNIQUE " : ""}${index.name} (${key}) · ${index.state}`;
    row.addEventListener("click", () => {
      deps.onPickIndex(table, index);
    });
    return row;
  }

  function foreignKeyRow(table: TableInfo, key: ForeignKeyInfo): HTMLElement {
    const row = el("button", { class: "index", type: "button" }, [
      el("span", { class: "index-name", text: key.name }),
      el("span", { class: "index-key", text: describeForeignKey(key) }),
    ]);
    row.title = `${key.name}: ${describeForeignKey(key)}`;
    row.addEventListener("click", () => {
      deps.onPickForeignKey(table, key);
    });
    return row;
  }

  function group(text: string): HTMLElement {
    return el("div", { class: "index-group", text });
  }

  /** What a table expands to: columns, then every constraint the catalog knows about it. */
  function details(table: TableInfo): HTMLElement {
    const indexes = table.indexes ?? [];
    const keys = table.foreignKeys ?? [];
    const checks = table.checks ?? [];
    const triggers = table.triggers ?? [];
    const children: HTMLElement[] = table.columns.map((column) => columnRow(table, column));
    if (table.view !== undefined) {
      const sql = el("button", { class: "index", type: "button" }, [
        el("span", { class: "index-name", text: "SQL" }),
        el("span", { class: "index-key", text: table.view.sql }),
      ]);
      sql.title = table.view.sql;
      sql.addEventListener("click", () => {
        deps.onPickViewSql(table);
      });
      children.push(group("Definition"), sql);
    }
    if (table.primaryKey !== undefined && table.primaryKey.length > 1) {
      children.push(
        group("Primary key"),
        el("div", { class: "index static" }, [
          el("span", { class: "index-key", text: table.primaryKey.join(", ") }),
        ]),
      );
    }
    if (indexes.length > 0) {
      children.push(
        group(`Indexes · ${String(indexes.length)}`),
        ...indexes.map((index) => indexRow(table, index)),
      );
    }
    if (keys.length > 0) {
      children.push(
        group(`Foreign keys · ${String(keys.length)}`),
        ...keys.map((key) => foreignKeyRow(table, key)),
      );
    }
    if (checks.length > 0) {
      children.push(
        group(`Checks · ${String(checks.length)}`),
        ...checks.map((check) =>
          el("div", { class: "index static", title: `${check.name}: ${check.sql}` }, [
            el("span", { class: "index-name", text: check.name }),
            el("span", { class: "index-key", text: check.sql }),
          ]),
        ),
      );
    }
    if (triggers.length > 0) {
      children.push(
        group(`Triggers · ${String(triggers.length)}`),
        ...triggers.map((trigger) =>
          el("div", { class: "index static" }, [
            el("span", { class: "index-name", text: trigger.name }),
            el("span", {
              class: "index-key",
              text: `${trigger.timing.toUpperCase()} ${trigger.event.toUpperCase()}`,
            }),
          ]),
        ),
      );
    }
    return el("div", { class: "cols" }, children);
  }

  function tableRow(table: TableInfo): HTMLElement[] {
    const open = table.name === expanded;
    const isView = table.view !== undefined;
    const extras = [
      table.indexes?.length ?? 0,
      table.foreignKeys?.length ?? 0,
      table.checks?.length ?? 0,
      table.triggers?.length ?? 0,
    ].reduce((sum, count) => sum + count, 0);
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
      render();
    });

    const name = el("button", { class: "tnode-open", type: "button" }, [
      el("span", { class: "tnode-icon" }, [icon(isView ? icons.view : icons.table)]),
      el("span", { class: "tnode-name", text: table.name }),
      el("span", {
        class: "tnode-meta",
        text:
          extras === 0
            ? String(table.columns.length)
            : `${String(table.columns.length)} · ${String(extras)}`,
        title: `${String(table.columns.length)} columns${
          extras === 0 ? "" : `, ${String(extras)} indexes and constraints`
        }`,
      }),
      ...(isView
        ? [el("span", { class: "tnode-badge view", text: "view", title: "Rows come from a query" })]
        : isEditable(table)
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
    return open ? [row, details(table)] : [row];
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
    const tables = shown.filter((table) => table.view === undefined);
    const views = shown.filter((table) => table.view !== undefined);
    list.replaceChildren(
      ...(tables.length === 0
        ? []
        : [
            el("div", {
              class: "rail-group",
              text: `Tables · ${String(catalog.filter((t) => t.view === undefined).length)}`,
            }),
            ...tables.flatMap(tableRow),
          ]),
      ...(views.length === 0
        ? []
        : [
            el("div", {
              class: "rail-group",
              text: `Views · ${String(catalog.filter((t) => t.view !== undefined).length)}`,
            }),
            ...views.flatMap(tableRow),
          ]),
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
    focusSearch: () => {
      if (collapsed) {
        collapsed = false;
        writeFlag(collapsedKey, false);
        applyCollapsed();
      }
      search.focus();
      search.select();
    },
  };
}
