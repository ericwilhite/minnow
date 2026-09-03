import type { QueryRow, QueryValue } from "@minnowdb/core";
import type { ConfirmLayer } from "../confirm.js";
import { button, el } from "../dom.js";
import { createGrid, type GridColumn } from "../results/grid.js";
import { isSortable } from "../sql/literal.js";
import {
  buildCountQuery,
  buildPageQuery,
  cursorFrom,
  pagingMode,
  type Cursor,
  type Sort,
} from "../sql/select.js";
import { isEditableTarget, type DevtoolsTarget, type EditableTarget } from "../target.js";
import { formatForInput, inputHint, parseInput } from "../values.js";
import { findTable, type TableInfo } from "./catalog.js";
import { createFilterBar } from "./filter-editor.js";
import { createInsertForm } from "./insert-form.js";
import {
  applyCellEdit,
  applyDelete,
  applyInsert,
  confirmCellEdit,
  confirmDelete,
  confirmInsert,
  editingBlockedReason,
  rowKey,
} from "./writes.js";

export interface ExplorerView {
  node: HTMLElement;
  /** Replaces the tables this view knows about, reopening whatever was open. */
  setCatalog(catalog: readonly TableInfo[]): Promise<void>;
  /** Opens a table by name, for the rail. */
  open(table: string): Promise<void>;
  /** The view was just shown; the grid lays out for the size it now has. */
  layout(): void;
}

export interface ExplorerDeps {
  target: DevtoolsTarget;
  confirm: ConfirmLayer;
  /** Whether the panel is allowed to change data at all. */
  write: boolean;
}

/** Rows per request. Large enough that scrolling rarely waits, small enough to stay instant. */
const pageSize = 200;

export function createExplorer(deps: ExplorerDeps): ExplorerView {
  const { target, confirm } = deps;
  // Present only when the panel may write and the target can: everything downstream then treats
  // its absence as "editing is off" without re-deriving why.
  const editor: EditableTarget | undefined =
    deps.write && isEditableTarget(target) ? target : undefined;

  const status = el("div", {
    class: "statusbar",
    attrs: { role: "status", "aria-live": "polite" },
  });
  /**
   * The rail drops out of the layout on a narrow panel, so the toolbar carries its own way to
   * choose a table. It is the only route to one when the rail is away, and a shortcut when it is
   * not.
   */
  const tablePicker = el("select", {
    class: "mini table-picker",
    attrs: { "aria-label": "Table" },
  });
  const crumb = el("span", { class: "crumb-meta", text: "" });
  const addRow = button("btn mini", "Add row");
  const deleteRow = button("btn mini danger", "Delete row");
  const banner = el("div", { class: "banner", attrs: { role: "status" } });
  banner.hidden = true;

  const grid = createGrid({
    onSort: (column) => {
      toggleSort(column);
    },
    onNearEnd: () => {
      void loadMore();
    },
    onSelect: () => {
      updateRowActions();
    },
    onEditCell: (row, column, index) => {
      beginCellEdit(row, column, index);
    },
  });

  const filterBar = createFilterBar(() => {
    void reload();
  });
  const insertForm = createInsertForm({
    onSubmit: (values) => {
      void commitInsert(values);
    },
  });

  const main = el("div", { class: "explorer-main" }, [
    el("div", { class: "toolbar" }, [
      tablePicker,
      crumb,
      el("span", { class: "spacer" }),
      addRow,
      deleteRow,
    ]),
    banner,
    filterBar.node,
    grid.node,
    insertForm.node,
    status,
  ]);
  const node = main;

  let catalog: TableInfo[] = [];
  let table: TableInfo | undefined;
  let sort: Sort | undefined;
  let cursor: Cursor | undefined;
  let exhausted = false;
  /**
   * The generation whose request is in flight, if any. A reload must never be refused because an
   * older one is still running — it supersedes it — so this gates appending rather than loading,
   * and only the request that set it may clear it.
   */
  let inFlight: number | undefined;
  /** Rising token: a response from a superseded view is dropped rather than rendered. */
  let generation = 0;
  let total: number | undefined;

  function setStatus(...parts: string[]): void {
    status.replaceChildren(...parts.map((part) => el("span", { text: part })));
  }

  function describeCount(): string {
    const loaded = grid.rowCount();
    if (total === undefined) return `${String(loaded)} rows loaded · counting…`;
    return loaded >= total ? `${String(total)} rows` : `${String(loaded)} of ${String(total)} rows`;
  }

  function columnsFor(current: TableInfo): GridColumn[] {
    return current.columns.map((column) => ({
      name: column.name,
      type: column.nullable ? `${column.type}?` : column.type,
      sortable: isSortable(column.name),
      isKey: column.isUniqueKey,
      ...(sort?.column === column.name ? { sorted: sort.direction } : {}),
    }));
  }

  /** Counting is a full scan, so it runs beside the first page and never blocks it. */
  async function count(current: TableInfo, token: number): Promise<void> {
    total = undefined;
    try {
      const result = await target.query(buildCountQuery(current, filterBar.filters()));
      if (token !== generation) return;
      const value = result.rows[0]?.row_count;
      total = typeof value === "number" ? value : undefined;
    } catch {
      // A count that fails costs a label, not the page; leave it unknown.
      total = undefined;
    }
    if (token === generation) setStatus(describeCount(), pagingLabel(current));
  }

  function pagingLabel(current: TableInfo): string {
    return pagingMode(current, sort) === "keyset"
      ? `cursor: ${sort?.column ?? current.uniqueKey ?? "key"}`
      : "counting from the start";
  }

  async function fetchPage(current: TableInfo, token: number, append: boolean): Promise<void> {
    inFlight = token;
    const started = performance.now();
    try {
      const sql = buildPageQuery({
        table: current,
        filters: filterBar.filters(),
        ...(sort === undefined ? {} : { sort }),
        ...(cursor === undefined ? {} : { cursor }),
        offset: append ? grid.rowCount() : 0,
        limit: pageSize,
      });
      const page = await target.query(sql);
      if (token !== generation) return;

      const rows: QueryRow[] = page.rows;
      if (append) grid.appendRows(rows);
      else if (rows.length === 0) {
        // An empty grid with only a small "0 rows" in the status bar reads as something having
        // gone wrong. Say which of the two empties it is, since one of them has an obvious fix.
        grid.setRows([]);
        grid.setMessage(
          filterBar.filters().length > 0
            ? "No rows match these filters. Remove one to widen the search."
            : `${current.name} has no rows yet.`,
        );
      } else grid.setRows(rows);
      // Replacing the rows drops the selection, so the row actions have to be re-derived or
      // Delete row stays enabled with nothing to delete.
      updateRowActions();

      exhausted = rows.length < pageSize;
      const last = rows[rows.length - 1];
      cursor = last === undefined ? cursor : cursorFrom(last, current, sort);

      const elapsed = Math.round(performance.now() - started);
      setStatus(describeCount(), pagingLabel(current), `${String(elapsed)}ms`);
    } catch (error) {
      if (token !== generation) return;
      grid.setMessage(error instanceof Error ? error.message : String(error));
      setStatus("failed");
      exhausted = true;
    } finally {
      if (inFlight === token) inFlight = undefined;
    }
  }

  async function reload(): Promise<void> {
    const current = table;
    if (current === undefined) return;
    generation += 1;
    const token = generation;
    cursor = undefined;
    exhausted = false;
    grid.setColumns(columnsFor(current));
    setStatus("loading…");
    await Promise.all([fetchPage(current, token, false), count(current, token)]);
  }

  async function loadMore(): Promise<void> {
    const current = table;
    if (current === undefined || exhausted || inFlight !== undefined) return;
    await fetchPage(current, generation, true);
  }

  /**
   * Row editing is offered only where it can actually work: writes allowed, a target that has the
   * write API, and a table with a unique key to address a row by. The banner says which of those
   * is missing rather than leaving a dead button.
   */
  function updateRowActions(): void {
    const current = table;
    if (current === undefined) return;
    const blocked = editingBlockedReason(current, deps.write, isEditableTarget(target));
    const keyed = current.uniqueKey !== undefined;

    // Inserts need no key, so they survive the one blocker that only affects keyed writes.
    addRow.hidden = editor === undefined;
    deleteRow.hidden = editor === undefined || !keyed;
    deleteRow.disabled = grid.selectedIndex() === undefined;

    banner.hidden = blocked === undefined;
    if (blocked !== undefined) banner.textContent = blocked;
  }

  async function withWrite(
    request: ReturnType<typeof confirmCellEdit>,
    run: () => Promise<string>,
  ): Promise<void> {
    if (!(await confirm.ask(request))) {
      setStatus("cancelled");
      return;
    }
    try {
      setStatus(await run());
    } catch (error) {
      setStatus(error instanceof Error ? error.message : String(error));
    }
  }

  function beginCellEdit(row: QueryRow, columnName: string, index: number): void {
    const current = table;
    if (current === undefined || editor === undefined) return;
    if (editingBlockedReason(current, deps.write, true) !== undefined) return;
    const column = current.columns.find(({ name }) => name === columnName);
    const key = rowKey(current, row);
    if (column === undefined || key === undefined) return;

    const from = row[columnName] ?? null;
    grid.editCell(index, columnName, {
      initial: formatForInput(from),
      placeholder: inputHint(column.type, column.nullable),
      onCancel: () => undefined,
      onCommit: (text) => {
        const parsed = parseInput(text, column.type, column.nullable);
        if (!parsed.ok) {
          setStatus(parsed.message);
          return;
        }
        if (parsed.value === from) return;
        void withWrite(
          confirmCellEdit({ table: current, column, key, from, to: parsed.value }),
          async () => {
            const changed = await applyCellEdit(editor, {
              table: current,
              column,
              key,
              from,
              to: parsed.value,
            });
            // Keyed updates read then mutate, so the row is refetched rather than patched — the
            // grid shows what actually landed, not what was asked for.
            await refreshRow(current, key, index);
            return `updated ${String(changed)} row`;
          },
        );
      },
    });
  }

  /** Rereads one row after a write, so the grid never shows an optimistic guess. */
  async function refreshRow(current: TableInfo, key: QueryValue, index: number): Promise<void> {
    const keyColumn = current.columns.find((column) => column.isUniqueKey);
    if (keyColumn === undefined) return;
    const result = await target.query(
      buildPageQuery({
        table: current,
        // The active filters are part of the question: an edit that moves a row out of the
        // current view should take it out of the grid rather than leave a row that no longer
        // matches what is being looked at.
        filters: [
          ...filterBar.filters(),
          { column: keyColumn.name, type: keyColumn.type, operator: "=", values: [key] },
        ],
        limit: 1,
      }),
    );
    const row = result.rows[0];
    if (row === undefined) grid.removeRow(index);
    else grid.replaceRow(index, row);
  }

  function requestDelete(): void {
    const current = table;
    const index = grid.selectedIndex();
    if (current === undefined || editor === undefined || index === undefined) return;
    const row = grid.rowAt(index);
    if (row === undefined) return;
    const key = rowKey(current, row);
    if (key === undefined) return;

    void withWrite(confirmDelete(current, key, row), async () => {
      const removed = await applyDelete(editor, current, key);
      grid.removeRow(index);
      if (total !== undefined) total -= removed;
      updateRowActions();
      return `deleted ${String(removed)} row`;
    });
  }

  async function commitInsert(values: Record<string, QueryValue>): Promise<void> {
    const current = table;
    if (current === undefined || editor === undefined) return;
    await withWrite(confirmInsert(current, values), async () => {
      const inserted = await applyInsert(editor, current, values);
      insertForm.close();
      await reload();
      return `inserted ${String(inserted)} row`;
    });
  }

  /** Clicking a header cycles ascending, descending, then back to the table's own order. */
  function toggleSort(column: string): void {
    if (sort?.column !== column) sort = { column, direction: "asc" };
    else if (sort.direction === "asc") sort = { column, direction: "desc" };
    else sort = undefined;
    void reload();
  }

  async function open(name: string): Promise<void> {
    const current = findTable(catalog, name);
    if (current === undefined) return;
    table = current;
    sort = undefined;
    filterBar.setTable(current);
    insertForm.close();
    updateRowActions();
    tablePicker.value = name;
    const indexCount = current.indexes?.length ?? 0;
    crumb.textContent = [
      current.uniqueKey === undefined ? "no unique key" : `key: ${current.uniqueKey}`,
      indexCount === 0 ? "" : `${String(indexCount)} ${indexCount === 1 ? "index" : "indexes"}`,
    ]
      .filter((part) => part.length > 0)
      .join(" · ");
    await reload();
  }

  /**
   * The catalog is loaded once for the whole panel and handed down, so the rail, the completion,
   * and this view can never disagree about what the tables are.
   */
  async function setCatalog(next: readonly TableInfo[]): Promise<void> {
    catalog = [...next];
    tablePicker.replaceChildren(
      ...catalog.map((entry) => {
        const option = el("option", { text: entry.name });
        option.value = entry.name;
        return option;
      }),
    );
    tablePicker.hidden = catalog.length === 0;
    if (catalog.length === 0) {
      table = undefined;
      grid.setMessage("This database has no tables yet.");
      setStatus("no tables");
      return;
    }
    const reopened = table === undefined ? undefined : findTable(catalog, table.name);
    await open((reopened ?? catalog[0])?.name ?? "");
  }

  addRow.addEventListener("click", () => {
    if (table !== undefined) insertForm.open(table);
  });
  deleteRow.addEventListener("click", requestDelete);
  tablePicker.addEventListener("change", () => {
    void open(tablePicker.value);
  });

  grid.setMessage("Pick a table to browse it.");
  addRow.hidden = true;
  deleteRow.hidden = true;

  return {
    node,
    setCatalog,
    open,
    layout: () => {
      grid.layout();
    },
  };
}
