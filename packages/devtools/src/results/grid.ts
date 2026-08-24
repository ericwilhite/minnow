import type { QueryRow, QueryValue } from "@minnowdb/core";
import { el, iconButton, icons } from "../dom.js";

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
  /** Fired when a row is clicked, with undefined when the selection is cleared. */
  onSelect?(row: QueryRow | undefined, index: number | undefined): void;
  /** Fired on a double-click in a cell, which is the request to edit it. */
  onEditCell?(row: QueryRow, column: string, index: number): void;
}

export interface CellEditor {
  /** Text the editor opens with. */
  initial: string;
  placeholder?: string;
  onCommit(text: string): void;
  onCancel(): void;
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
  rowAt(index: number): QueryRow | undefined;
  setSelected(index: number | undefined): void;
  selectedIndex(): number | undefined;
  /** Replaces one row's values in place, after a write changed it. */
  replaceRow(index: number, row: QueryRow): void;
  /** Drops one row, after it was deleted. */
  removeRow(index: number): void;
  /** Opens an input over a cell, aligned to the column by the grid template itself. */
  editCell(index: number, column: string, editor: CellEditor): void;
  closeEdit(): void;
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
  const head = el("div", { class: "grid-head", attrs: { role: "row" } });
  const sizer = el("div", { class: "grid-sizer", attrs: { role: "rowgroup" } });
  // The header lives inside the scrolling surface, sticky to its top. Outside it, a table wider
  // than the panel would scroll its rows out from under a header that stayed put.
  const surface = el("div", { class: "grid-surface" }, [head, sizer]);
  const viewport = el("div", { class: "grid-viewport" }, [surface]);
  const node = el("div", { class: "grid", attrs: { role: "grid", "aria-rowcount": "0" } }, [
    viewport,
  ]);

  let columns: GridColumn[] = [];
  let rows: QueryRow[] = [];
  /** Recycled row nodes, addressed as a ring: row `n` always lives in slot `n % pool.length`. */
  let pool: HTMLElement[] = [];
  /** The row index each slot currently shows, or -1 when the slot holds nothing. */
  let slotRow: number[] = [];
  let frame = 0;
  let template = "";
  let notifiedForCount = -1;
  let selected: number | undefined;
  /** The open cell editor, positioned like a row so the grid template aligns it for free. */
  let editing: { index: number; node: HTMLElement; input: HTMLInputElement } | undefined;

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
    node.setAttribute("aria-colcount", String(columns.length));
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
        const sortable = column.sortable === true && deps.onSort !== undefined;
        const cell = el(
          sortable ? "button" : "div",
          {
            class: `grid-th${column.sortable === true ? " sortable" : ""}${column.sorted === undefined ? "" : " sorted"}`,
            ...(sortable ? { type: "button" } : { title: `${column.name} cannot be sorted` }),
            attrs: {
              role: "columnheader",
              ...(column.sorted === undefined
                ? {}
                : { "aria-sort": column.sorted === "asc" ? "ascending" : "descending" }),
              ...(sortable ? { "aria-label": `Sort by ${column.name}` } : {}),
            },
          },
          children,
        );
        // A real button rather than a div with a click handler, so sorting is reachable by
        // keyboard and announced as something that can be pressed.
        if (sortable) {
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
      const row = el("div", { class: "grid-row", attrs: { role: "row" } });
      row.style.gridTemplateColumns = template;
      row.append(
        ...columns.map((_column, index) =>
          el("div", {
            class: "cell",
            attrs: { role: "gridcell", tabindex: index === 0 ? "0" : "-1" },
          }),
        ),
      );
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
    node.dataset.index = String(index);
    node.classList.toggle("sel", index === selected);
    node.setAttribute("aria-rowindex", String(index + 2));
    node.setAttribute("aria-selected", String(index === selected));
    const cells = node.children;
    for (let column = 0; column < columns.length; column += 1) {
      const cell = cells[column];
      const name = columns[column]?.name;
      if (cell === undefined || name === undefined) continue;
      const value = row[name] ?? null;
      const { text, className } = formatValue(value);
      cell.className = className;
      cell.setAttribute("role", "gridcell");
      cell.setAttribute("aria-colindex", String(column + 1));
      cell.setAttribute("tabindex", column === 0 ? "0" : "-1");
      // NULL is rendered as its own marker rather than as the word, so a string reading "NULL"
      // cannot be mistaken for the absence of a value.
      if (value === null) cell.replaceChildren(el("span", { text: "NULL" }));
      else cell.textContent = text;
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

  /** Reads the row and column a pointer event landed on, using the pooled node's own bookkeeping. */
  function locate(event: Event): { index: number; row: QueryRow; column?: string } | undefined {
    const cell = (event.target as Element | null)?.closest(".cell");
    const rowNode = (event.target as Element | null)?.closest(".grid-row");
    if (!(rowNode instanceof HTMLElement)) return undefined;
    const index = Number(rowNode.dataset.index ?? "-1");
    const row = rows[index];
    if (row === undefined) return undefined;
    const position =
      cell === null || cell === undefined
        ? -1
        : Array.prototype.indexOf.call(rowNode.children, cell);
    const column = columns[position]?.name;
    return column === undefined ? { index, row } : { index, row, column };
  }

  sizer.addEventListener("click", (event) => {
    const hit = locate(event);
    if (hit === undefined) return;
    // Clicking the selected row again clears it, so there is always a way back to no selection.
    const next = selected === hit.index ? undefined : hit.index;
    setSelected(next);
    deps.onSelect?.(next === undefined ? undefined : hit.row, next);
  });

  sizer.addEventListener("keydown", (event) => {
    const cell = (event.target as Element | null)?.closest<HTMLElement>(".cell");
    const hit = locate(event);
    if (cell === null || cell === undefined || hit === undefined) return;
    const rowNode = cell.closest<HTMLElement>(".grid-row");
    if (rowNode === null) return;
    const column = Array.prototype.indexOf.call(rowNode.children, cell);
    if (event.key === " " || event.key === "Spacebar") {
      event.preventDefault();
      const next = selected === hit.index ? undefined : hit.index;
      setSelected(next);
      deps.onSelect?.(next === undefined ? undefined : hit.row, next);
      return;
    }
    if (event.key === "Enter" && hit.column !== undefined && deps.onEditCell !== undefined) {
      event.preventDefault();
      deps.onEditCell(hit.row, hit.column, hit.index);
      return;
    }
    const rowOffset = event.key === "ArrowUp" ? -1 : event.key === "ArrowDown" ? 1 : 0;
    const columnOffset = event.key === "ArrowLeft" ? -1 : event.key === "ArrowRight" ? 1 : 0;
    if (rowOffset === 0 && columnOffset === 0) return;
    event.preventDefault();
    const targetRow = sizer.querySelector<HTMLElement>(
      `.grid-row[data-index="${String(hit.index + rowOffset)}"]`,
    );
    const targetCell = (targetRow ?? rowNode).children[
      Math.max(0, Math.min(columns.length - 1, column + columnOffset))
    ];
    if (targetCell instanceof HTMLElement) targetCell.focus();
  });

  sizer.addEventListener("dblclick", (event) => {
    const hit = locate(event);
    if (hit?.column === undefined) return;
    deps.onEditCell?.(hit.row, hit.column, hit.index);
  });

  /** Repaints just the slots holding these rows, rather than the whole window. */
  function invalidate(...indexes: Array<number | undefined>): void {
    for (const index of indexes) {
      if (index === undefined || pool.length === 0) continue;
      const slot = index % pool.length;
      if (slotRow[slot] === index) slotRow[slot] = -1;
    }
    render();
  }

  function setSelected(index: number | undefined): void {
    const previous = selected;
    selected = index;
    invalidate(previous, index);
  }

  function closeEdit(): void {
    editing?.node.remove();
    editing = undefined;
  }

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
    rowAt: (index) => rows[index],
    selectedIndex: () => selected,
    setSelected,
    closeEdit,
    setColumns: (next) => {
      columns = [...next];
      closeEdit();
      reset();
      renderHead();
      render();
    },
    setRows: (next) => {
      rows = [...next];
      node.setAttribute("aria-rowcount", String(rows.length + 1));
      notifiedForCount = -1;
      selected = undefined;
      closeEdit();
      reset();
      setHeight();
      viewport.scrollTop = 0;
      render();
    },
    appendRows: (next) => {
      if (next.length === 0) return;
      rows = rows.concat(next);
      node.setAttribute("aria-rowcount", String(rows.length + 1));
      setHeight();
      render();
    },
    replaceRow: (index, row) => {
      if (rows[index] === undefined) return;
      rows[index] = row;
      invalidate(index);
    },
    removeRow: (index) => {
      if (rows[index] === undefined) return;
      rows.splice(index, 1);
      node.setAttribute("aria-rowcount", String(rows.length + 1));
      if (selected === index) selected = undefined;
      closeEdit();
      setHeight();
      // Every row after the removed one shifted up, so the whole ring is stale.
      slotRow = slotRow.map(() => -1);
      render();
    },
    setMessage: (message) => {
      rows = [];
      node.setAttribute("aria-rowcount", columns.length === 0 ? "0" : "1");
      selected = undefined;
      closeEdit();
      reset();
      sizer.style.height = "auto";
      sizer.append(el("div", { class: "grid-message", text: message, attrs: { role: "status" } }));
    },
    editCell: (index, column, editor) => {
      closeEdit();
      const position = columns.findIndex(({ name }) => name === column);
      if (position < 0 || rows[index] === undefined) return;

      const input = el("input", {
        class: "cell-input",
        type: "text",
        attrs: { "aria-label": `${column} value`, spellcheck: "false" },
      });
      input.value = editor.initial;
      if (editor.placeholder !== undefined) input.placeholder = editor.placeholder;

      const holder = el("div", { class: "cell-edit" }, [input]);
      holder.style.gridColumn = String(position + 1);

      const commit = (): void => {
        const text = input.value;
        closeEdit();
        editor.onCommit(text);
      };
      const cancel = (): void => {
        closeEdit();
        editor.onCancel();
      };

      // The keys alone are not an interface: someone who types a value and reaches for the mouse
      // needs somewhere to click, and an edit that vanished because focus moved reads as the
      // editor being broken. These are anchored to the row's end so a narrow column still has
      // room for them.
      const save = iconButton("cell-action save", "Save this change (Enter)", icons.check);
      const discard = iconButton("cell-action", "Discard this change (Esc)", icons.close);
      save.addEventListener("click", commit);
      discard.addEventListener("click", cancel);

      // Anchored to the cell being edited rather than to the row: the row runs the full width of
      // the table, which on a wide one is off the side of the viewport entirely.
      holder.append(el("div", { class: "cell-actions" }, [save, discard]));
      const node = el("div", { class: "grid-row editing" }, [holder]);
      node.style.gridTemplateColumns = template;
      node.style.transform = `translateY(${String(index * rowHeight)}px)`;

      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") {
          event.preventDefault();
          commit();
        } else if (event.key === "Escape") {
          event.preventDefault();
          cancel();
        }
      });

      sizer.append(node);
      editing = { index, node, input };
      input.focus();
      input.select();
    },
  };
}
