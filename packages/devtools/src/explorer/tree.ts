import { el } from "../dom.js";
import { isEditable, type TableInfo } from "./catalog.js";

export interface SchemaRail {
  node: HTMLElement;
  setCatalog(catalog: readonly TableInfo[]): void;
  setSelected(table: string | undefined): void;
  setError(message: string): void;
}

/**
 * Tables down the left, expanding to their columns. Row counts are deliberately absent: each one
 * is a full scan, and fifty of them on open would be the slowest thing the panel does.
 */
export function createSchemaRail(onSelect: (table: string) => void): SchemaRail {
  const search = el("input", {
    class: "rail-search",
    type: "text",
    attrs: { placeholder: "Filter tables…", "aria-label": "Filter tables" },
  });
  const list = el("div", { class: "rail-list" });
  const node = el("div", { class: "rail" }, [el("div", { class: "rail-top" }, [search]), list]);

  let catalog: TableInfo[] = [];
  let selected: string | undefined;
  let filter = "";

  function columnRow(table: TableInfo, column: TableInfo["columns"][number]): HTMLElement {
    const parts: Array<Node | string> = [
      el("span", { class: "col-name", text: column.name }),
      el("span", {
        class: "col-type",
        text: column.nullable ? `${column.type}?` : column.type,
      }),
    ];
    if (column.isUniqueKey) {
      parts.push(el("span", { class: "col-key", text: "key", title: "unique key" }));
    }
    return el("div", { class: "col" }, parts);
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
    const nodes: HTMLElement[] = [
      el("div", { class: "rail-group", text: `Tables · ${String(catalog.length)}` }),
    ];
    for (const table of shown) {
      const open = table.name === selected;
      const label = el(
        "button",
        {
          class: `tnode${open ? " on" : ""}`,
          type: "button",
          attrs: { "aria-expanded": String(open) },
        },
        [
          el("span", { class: "tnode-chev", text: open ? "▾" : "▸" }),
          el("span", { class: "tnode-name", text: table.name }),
          el("span", {
            class: "tnode-meta",
            text: String(table.columns.length),
            title: `${String(table.columns.length)} columns`,
          }),
        ],
      );
      if (!isEditable(table)) {
        label.append(
          el("span", {
            class: "tnode-badge",
            text: "no key",
            title: "Rows cannot be edited without a unique key",
          }),
        );
      }
      label.addEventListener("click", () => {
        onSelect(table.name);
      });
      nodes.push(label);
      if (open) {
        nodes.push(
          el(
            "div",
            { class: "cols" },
            table.columns.map((column) => columnRow(table, column)),
          ),
        );
      }
    }
    list.replaceChildren(...nodes);
  }

  search.addEventListener("input", () => {
    filter = search.value;
    render();
  });

  render();

  return {
    node,
    setCatalog: (next) => {
      catalog = [...next];
      render();
    },
    setSelected: (table) => {
      selected = table;
      render();
    },
    setError: (message) => {
      list.replaceChildren(el("div", { class: "rail-empty", text: message }));
    },
  };
}
