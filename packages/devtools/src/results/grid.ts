import type { QueryRow, QueryValue } from "@minnowdb/core";
import { dateIsoString } from "../date-value.js";
import { el, iconButton, icons } from "../dom.js";
import { draggable } from "../panel/drag.js";
import { toCsv } from "./serialize.js";

export interface GridColumn {
  name: string;
  /** Right-aligns numbers and sizes the column; inferred from the values for a bare query result. */
  type?: string;
  /** What the header shows beside the name — the declared SQL type — when it is more than `type`. */
  label?: string;
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
  /** Fired when the selection changes, with the anchor row, or undefined when it was cleared. */
  onSelect?(row: QueryRow | undefined, index: number | undefined): void;
  /** Fired on a double-click in a cell, which is the request to edit it. */
  onEditCell?(row: QueryRow, column: string, index: number): void;
  /** Fired after Ctrl/Cmd+C put something on the clipboard, with what it was. */
  onCopied?(what: string): void;
  /** Fired on a right-click in a cell, with where the menu should open. */
  onContextMenu?(hit: CellHit, at: { x: number; y: number }): void;
}

export interface CellHit {
  row: QueryRow;
  column: string;
  index: number;
}

export interface CellEditor {
  /** Text the editor opens with. */
  initial: string;
  placeholder?: string;
  /** A closed set of values — an enum column — offered as a menu instead of a box. */
  choices?: readonly string[];
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
  columnNames(): string[];
  setSelected(index: number | undefined): void;
  /** The anchor of the selection: the row clicked last. */
  selectedIndex(): number | undefined;
  /** Every selected row, ascending. Shift-click extends the selection; Ctrl/Cmd-click toggles. */
  selectedIndexes(): number[];
  /** Replaces one row's values in place, after a write changed it. */
  replaceRow(index: number, row: QueryRow): void;
  /** Drops one row, after it was deleted. */
  removeRow(index: number): void;
  /** Opens an input over a cell, aligned to the column by the grid template itself. */
  editCell(index: number, column: string, editor: CellEditor): void;
  closeEdit(): void;
  /**
   * Re-derives the window from the viewport's current size. The grid watches its own viewport
   * where the browser can report a resize; this is for a host that showed a hidden grid and
   * wants the rows there before the next frame reports it.
   */
  layout(): void;
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
  if (value instanceof Date) return { text: dateIsoString(value), className: "cell" };
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
  /** The row clicked last, which is what row actions act on and where a shift-click extends from. */
  let selected: number | undefined;
  const selection = new Set<number>();
  /** Widths dragged on a header, by column name, so a reload of the same table keeps them. */
  const widths = new Map<string, number>();
  /** The open cell editor, positioned like a row so the grid template aligns it for free. */
  let editing:
    { index: number; node: HTMLElement; input: HTMLInputElement | HTMLSelectElement } | undefined;

  function columnWidth(column: GridColumn): string {
    const dragged = widths.get(column.name);
    if (dragged !== undefined) return `${String(dragged)}px`;
    // Numbers and keys are narrow and predictable; text gets the room, bounded so one long value
    // cannot push every other column off screen.
    if (column.type === "number" || column.isKey === true) return "minmax(90px, 140px)";
    if (column.type === "datetime") return "minmax(160px, 200px)";
    if (column.type === "boolean") return "minmax(70px, 90px)";
    return "minmax(120px, 260px)";
  }

  function applyTemplate(): void {
    template = columns.map(columnWidth).join(" ");
    head.style.gridTemplateColumns = template;
    sizer.style.gridTemplateColumns = template;
    for (const row of pool) row.style.gridTemplateColumns = template;
    if (editing !== undefined) editing.node.style.gridTemplateColumns = template;
  }

  /**
   * A drag handle on the header's trailing edge. The width is written into the grid template,
   * which every row shares, so one drag resizes the column for every row at once and no row is
   * laid out on its own.
   */
  function resizeHandle(column: GridColumn, cell: HTMLElement): HTMLElement {
    const handle = el("span", { class: "grid-resize", attrs: { "aria-hidden": "true" } });
    let startWidth = 0;
    draggable(handle, {
      onStart: () => {
        startWidth = cell.getBoundingClientRect().width;
        handle.classList.add("dragging");
        return true;
      },
      onMove: (dx) => {
        widths.set(column.name, Math.max(48, Math.round(startWidth + dx)));
        applyTemplate();
      },
      onEnd: () => {
        handle.classList.remove("dragging");
      },
    });
    // The handle sits inside a button on a sortable column; a press on it must not sort.
    handle.addEventListener("click", (event) => {
      event.stopPropagation();
    });
    return handle;
  }

  function renderHead(): void {
    applyTemplate();
    node.setAttribute("aria-colcount", String(columns.length));
    head.replaceChildren(
      ...columns.map((column) => {
        const label = el("span", { class: "grid-name", text: column.name });
        const children: Array<Node | string> = [label];
        const typeText = column.label ?? column.type;
        if (typeText !== undefined) {
          children.push(el("span", { class: "grid-type", text: typeText }));
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
        cell.append(resizeHandle(column, cell));
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
    node.classList.toggle("sel", selection.has(index));
    node.setAttribute("aria-rowindex", String(index + 2));
    node.setAttribute("aria-selected", String(selection.has(index)));
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
  // The window is sized from the viewport, so a viewport that grew — the panel maximized or
  // dragged taller, or the grid's tab shown after its first page arrived while it was hidden
  // and measured 0px — needs more rows pooled than the last render laid down. Without this the
  // space below the old window stays blank until a scroll happens to ask for a render. Where
  // there is no ResizeObserver the scroll path still completes the window, just late.
  if (typeof ResizeObserver === "function") {
    new ResizeObserver(() => {
      schedule();
    }).observe(viewport);
  }

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

  /**
   * Selection follows the file-manager conventions: a click selects one row (or clears it when
   * it was the one selected, so there is always a way back to nothing), Ctrl/Cmd toggles a row
   * in and out, Shift selects the run from the anchor to the row.
   */
  function select(index: number, modifiers: { toggle: boolean; extend: boolean }): void {
    const previous = [...selection];
    if (modifiers.extend && selected !== undefined) {
      const [low, high] = [Math.min(selected, index), Math.max(selected, index)];
      for (let at = low; at <= high; at += 1) selection.add(at);
    } else if (modifiers.toggle) {
      if (selection.has(index)) selection.delete(index);
      else selection.add(index);
      selected = selection.has(index) ? index : [...selection].at(-1);
    } else if (selection.size === 1 && selection.has(index)) {
      selection.clear();
      selected = undefined;
    } else {
      selection.clear();
      selection.add(index);
      selected = index;
    }
    if (modifiers.extend && selected === undefined) selected = index;
    invalidate(...previous, ...selection);
    deps.onSelect?.(selected === undefined ? undefined : rows[selected], selected);
  }

  sizer.addEventListener("click", (event) => {
    const hit = locate(event);
    if (hit === undefined) return;
    select(hit.index, { toggle: event.metaKey || event.ctrlKey, extend: event.shiftKey });
  });

  /** Ctrl/Cmd+C on a focused cell copies it; with rows selected, copies them as tab-separated text. */
  async function copyFocused(cell: HTMLElement): Promise<void> {
    const names = columns.map((column) => column.name);
    const picked = [...selection].sort((a, b) => a - b).map((index) => rows[index]);
    const text =
      picked.length > 0
        ? toCsv(
            names,
            picked.filter((row): row is QueryRow => row !== undefined),
            "\t",
          )
        : cell.textContent;
    try {
      await navigator.clipboard.writeText(text);
      deps.onCopied?.(picked.length > 0 ? `${String(picked.length)} rows` : "cell");
    } catch {
      // Clipboard access can be refused; there is nothing to show for it in the grid itself.
    }
  }

  sizer.addEventListener("keydown", (event) => {
    const cell = (event.target as Element | null)?.closest<HTMLElement>(".cell");
    const hit = locate(event);
    if (cell === null || cell === undefined || hit === undefined) return;
    const rowNode = cell.closest<HTMLElement>(".grid-row");
    if (rowNode === null) return;
    const column = Array.prototype.indexOf.call(rowNode.children, cell);
    if ((event.key === "c" || event.key === "C") && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      void copyFocused(cell);
      return;
    }
    if (event.key === " " || event.key === "Spacebar") {
      event.preventDefault();
      select(hit.index, { toggle: event.metaKey || event.ctrlKey, extend: event.shiftKey });
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

  sizer.addEventListener("contextmenu", (event) => {
    const hit = locate(event);
    if (hit?.column === undefined || deps.onContextMenu === undefined) return;
    event.preventDefault();
    // A right-click on an unselected row selects it first, the way a file manager does, so the
    // menu's row actions act on the row under the pointer.
    if (!selection.has(hit.index)) select(hit.index, { toggle: false, extend: false });
    deps.onContextMenu(
      { row: hit.row, column: hit.column, index: hit.index },
      { x: event.clientX, y: event.clientY },
    );
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
    const previous = [...selection];
    selection.clear();
    if (index !== undefined) selection.add(index);
    selected = index;
    invalidate(...previous, index);
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
    columnNames: () => columns.map((column) => column.name),
    selectedIndex: () => selected,
    selectedIndexes: () => [...selection].sort((a, b) => a - b),
    setSelected,
    closeEdit,
    layout: schedule,
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
      selection.clear();
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
      // Every selected row past the removed one moved up by one.
      const shifted = [...selection]
        .filter((at) => at !== index)
        .map((at) => (at > index ? at - 1 : at));
      selection.clear();
      for (const at of shifted) selection.add(at);
      if (selected === index) selected = undefined;
      else if (selected !== undefined && selected > index) selected -= 1;
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
      selection.clear();
      closeEdit();
      reset();
      sizer.style.height = "auto";
      sizer.append(el("div", { class: "grid-message", text: message, attrs: { role: "status" } }));
    },
    editCell: (index, column, editor) => {
      closeEdit();
      const position = columns.findIndex(({ name }) => name === column);
      if (position < 0 || rows[index] === undefined) return;

      const input =
        editor.choices === undefined
          ? el("input", {
              class: "cell-input",
              type: "text",
              attrs: { "aria-label": `${column} value`, spellcheck: "false" },
            })
          : el("select", { class: "cell-input", attrs: { "aria-label": `${column} value` } }, [
              el("option", { text: editor.placeholder ?? "", attrs: { value: "" } }),
              ...editor.choices.map((choice) => el("option", { text: choice })),
            ]);
      input.value = editor.initial;
      if (input instanceof HTMLInputElement && editor.placeholder !== undefined) {
        input.placeholder = editor.placeholder;
      }

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

      (input as HTMLElement).addEventListener("keydown", (event) => {
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
      if (input instanceof HTMLInputElement) input.select();
    },
  };
}
