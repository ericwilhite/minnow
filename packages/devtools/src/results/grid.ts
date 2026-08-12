import type { QueryRow, QueryValue } from "@minnowdb/core";
import { el } from "../dom.js";

export interface GridColumn {
  name: string;
  /** Right-aligns numbers and drives the sort affordance; absent for a bare query result. */
  type?: string;
  sortable?: boolean;
  sorted?: "asc" | "desc";
  /** Marks the unique key, which is what row actions are keyed by. */
  isKey?: boolean;
}

export interface GridDeps {
  /** Fired when a sortable header is clicked. */
  onSort?(column: string): void;
  /** Fired when the viewport nears the end of the loaded rows. */
  onNearEnd?(): void;
}

export interface Grid {
  node: HTMLElement;
  setColumns(columns: readonly GridColumn[]): void;
  /** Replaces the rows and scrolls back to the top. */
  setRows(rows: readonly QueryRow[]): void;
  /** Adds rows to the end, keeping the scroll position — the paging path. */
  appendRows(rows: readonly QueryRow[]): void;
  /** Replaces the body with a message, for the empty and error states. */
  setMessage(message: string): void;
  rowCount(): number;
}

/** Row height is fixed so the scroll position maps to an index by division, not measurement. */
const rowHeight = 26;
/** Rows rendered beyond the viewport at each end, to cover fast scrolling between frames. */
const overscan = 8;
/** How close to the end the viewport gets before the next page is asked for. */
const loadThresholdRows = 20;

function formatValue(value: QueryValue): { text: string; className: string } {
  if (value === null) return { text: "NULL", className: "cell null" };
  if (typeof value === "number") return { text: String(value), className: "cell number" };
  if (value instanceof Date) return { text: value.toISOString(), className: "cell" };
  if (typeof value === "boolean") return { text: String(value), className: "cell boolean" };
  return { text: value, className: "cell" };
}

/**
 * A windowed table. Only the rows crossing the viewport exist in the DOM, and their nodes are
 * reused as the window moves, so scrolling a million rows costs the same as scrolling fifty: on a
 * typical frame two rows change hands and nothing else is touched.
 *
 * Columns are laid out once per column set into a CSS grid template, which is what lets a row be a
 * flat run of cells with no per-row layout work.
 */
export function createGrid(deps: GridDeps = {}): Grid {
  const head = el("div", { class: "grid-head" });
  const sizer = el("div", { class: "grid-sizer" });
  // The header lives inside the scrolling surface, sticky to its top. Outside it, a table wider
  // than the panel would scroll its rows out from under a header that stayed put.
  const surface = el("div", { class: "grid-surface" }, [head, sizer]);
  const viewport = el("div", { class: "grid-viewport" }, [surface]);
  const node = el("div", { class: "grid" }, [viewport]);

  let columns: GridColumn[] = [];
  let rows: QueryRow[] = [];
  /** Recycled row nodes, addressed as a ring: row `n` always lives in slot `n % pool.length`. */
  let pool: HTMLElement[] = [];
  /** The row index each slot currently shows, or -1 when the slot holds nothing. */
  let slotRow: number[] = [];
  let frame = 0;
  let template = "";
  let notifiedForCount = -1;

  function columnWidth(column: GridColumn): string {
    // Numbers and keys are narrow and predictable; text gets the room, bounded so one long value
    // cannot push every other column off screen.
    if (column.type === "number" || column.isKey === true) return "minmax(90px, 140px)";
    if (column.type === "datetime") return "minmax(160px, 200px)";
    if (column.type === "boolean") return "minmax(70px, 90px)";
    return "minmax(120px, 260px)";
  }

  function renderHead(): void {
    template = columns.map(columnWidth).join(" ");
    head.style.gridTemplateColumns = template;
    sizer.style.gridTemplateColumns = template;
    head.replaceChildren(
      ...columns.map((column) => {
        const label = el("span", { class: "grid-name", text: column.name });
        const children: Array<Node | string> = [label];
        if (column.type !== undefined) {
          children.push(el("span", { class: "grid-type", text: column.type }));
        }
        if (column.sorted !== undefined) {
          children.push(
            el("span", { class: "grid-sort", text: column.sorted === "asc" ? "↑" : "↓" }),
          );
        }
        const cell = el(
          "div",
          {
            class: `grid-th${column.sortable === true ? " sortable" : ""}${column.sorted === undefined ? "" : " sorted"}`,
            ...(column.sortable === true ? {} : { title: `${column.name} cannot be sorted` }),
          },
          children,
        );
        if (column.sortable === true && deps.onSort !== undefined) {
          cell.addEventListener("click", () => {
            deps.onSort?.(column.name);
          });
        }
        return cell;
      }),
    );
  }

  /**
   * Grows the pool to cover the window. It only ever grows: a pool that shrank would renumber
   * every slot in the ring and force a full repaint on the next scroll.
   */
  function growPool(needed: number): void {
    if (pool.length >= needed) return;
    while (pool.length < needed) {
      const row = el("div", { class: "grid-row" });
      row.style.gridTemplateColumns = template;
      row.append(...columns.map(() => el("div", { class: "cell" })));
      pool.push(row);
      slotRow.push(-1);
      sizer.append(row);
    }
    // Ring positions changed, so every slot has to be re-derived.
    slotRow = slotRow.map(() => -1);
  }

  /** Writes one row's values into a pooled node. Text only — no markup is ever parsed. */
  function fill(node: HTMLElement, index: number): void {
    const row = rows[index];
    if (row === undefined) return;
    node.style.transform = `translateY(${String(index * rowHeight)}px)`;
    const cells = node.children;
    for (let column = 0; column < columns.length; column += 1) {
      const cell = cells[column];
      const name = columns[column]?.name;
      if (cell === undefined || name === undefined) continue;
      const { text, className } = formatValue(row[name] ?? null);
      cell.className = className;
      cell.textContent = text;
      if (text.length > 24) (cell as HTMLElement).title = text;
      else (cell as HTMLElement).removeAttribute("title");
    }
  }

  function render(): void {
    if (columns.length === 0) return;
    const visible = Math.ceil(viewport.clientHeight / rowHeight);
    const first = Math.max(0, Math.floor(viewport.scrollTop / rowHeight) - overscan);
    const last = Math.min(rows.length, first + visible + overscan * 2);

    growPool(Math.min(rows.length, visible + overscan * 2 + 1));

    const inWindow = new Set<number>();
    for (let index = first; index < last; index += 1) {
      const slot = index % pool.length;
      inWindow.add(slot);
      // The whole point of the ring: scrolling one row changes one slot's identity, so this
      // guard skips every node that is already showing the right values.
      if (slotRow[slot] === index) continue;
      const node = pool[slot];
      if (node === undefined) continue;
      node.hidden = false;
      fill(node, index);
      slotRow[slot] = index;
    }
    // Near the ends the window is shorter than the ring, leaving slots with stale rows behind.
    for (let slot = 0; slot < pool.length; slot += 1) {
      if (inWindow.has(slot)) continue;
      const node = pool[slot];
      if (node !== undefined && !node.hidden) node.hidden = true;
    }

    if (
      deps.onNearEnd !== undefined &&
      rows.length > 0 &&
      notifiedForCount !== rows.length &&
      last >= rows.length - loadThresholdRows
    ) {
      notifiedForCount = rows.length;
      deps.onNearEnd();
    }
  }

  function schedule(): void {
    if (frame !== 0) return;
    frame = requestAnimationFrame(() => {
      frame = 0;
      render();
    });
  }

  viewport.addEventListener("scroll", schedule, { passive: true });

  /** Drops every pooled node, for when the columns or the whole result change. */
  function reset(): void {
    pool = [];
    slotRow = [];
    sizer.replaceChildren();
  }

  function setHeight(): void {
    sizer.style.height = `${String(rows.length * rowHeight)}px`;
  }

  return {
    node,
    rowCount: () => rows.length,
    setColumns: (next) => {
      columns = [...next];
      reset();
      renderHead();
      render();
    },
    setRows: (next) => {
      rows = [...next];
      notifiedForCount = -1;
      reset();
      setHeight();
      viewport.scrollTop = 0;
      render();
    },
    appendRows: (next) => {
      if (next.length === 0) return;
      rows = rows.concat(next);
      setHeight();
      render();
    },
    setMessage: (message) => {
      rows = [];
      reset();
      sizer.style.height = "auto";
      sizer.append(el("div", { class: "grid-message", text: message }));
    },
  };
}
