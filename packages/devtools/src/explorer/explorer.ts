import type { QueryResult, QueryRow, QueryValue } from "@minnowdb/core";
import type { ConfirmLayer } from "../confirm.js";
import { button, el } from "../dom.js";
import { messageOf } from "../errors.js";
import { formatCount } from "../format.js";
import { createFollower, isLiveTarget, type Follower } from "../live.js";
import { createMenu, type MenuItem } from "../menu.js";
import { createGrid, type CellHit, type GridColumn } from "../results/grid.js";
import { rowToJson, toInsertSql } from "../results/serialize.js";
import {
  buildBrowseQuery,
  buildCountQuery,
  buildPageQuery,
  cursorFrom,
  pagingMode,
  type Cursor,
  type Sort,
} from "../sql/select.js";
import { readFlag, writeFlag } from "../storage.js";
import { isEditableTarget, type DevtoolsTarget, type EditableTarget } from "../target.js";
import { formatForInput, inputHint, parseInput } from "../values.js";
import {
  findTable,
  foreignKeyFor,
  isEditable,
  isInsertable,
  tableToReopen,
  type TableInfo,
} from "./catalog.js";
import { childRelations, createRowDetail } from "./detail.js";
import { createFilterBar } from "./filter-editor.js";
import type { Filter } from "./filters.js";
import { createInsertForm } from "./insert-form.js";
import {
  applyCellEdit,
  applyDelete,
  applyDeleteMany,
  applyInsert,
  confirmCellEdit,
  confirmDelete,
  confirmDeleteMany,
  confirmInsert,
  editingBlockedReason,
  rowKey,
} from "./writes.js";

export interface ExplorerView {
  node: HTMLElement;
  /** Replaces the tables this view knows about, reopening whatever was open. */
  setCatalog(catalog: readonly TableInfo[]): Promise<void>;
  /** Opens a table by name, for the rail — already filtered, when following a relationship. */
  open(table: string, filters?: readonly Filter[]): Promise<void>;
  /** The view was just shown; the grid lays out for the size it now has. */
  layout(): void;
  /** Stops any live subscription. */
  destroy(): void;
}

export interface ExplorerDeps {
  target: DevtoolsTarget;
  confirm: ConfirmLayer;
  /** Whether the panel is allowed to change data at all. */
  write: boolean;
  /** Namespaces the remembered layout — whether the row detail is open. */
  storageKey: string;
  /**
   * The table now showing, or `undefined` once there is none. Fired for every route to a table —
   * the rail, the picker, and a catalog reload — so the rail can keep its highlight in step.
   */
  onOpen?(table: string | undefined): void;
  /** Hands a statement to the console: the rows being browsed, as SQL to build on. */
  onRunQuery?(sql: string): void;
}

/** Rows per request. Large enough that scrolling rarely waits, small enough to stay instant. */
const pageSize = 200;

export function createExplorer(deps: ExplorerDeps): ExplorerView {
  const { target, confirm } = deps;
  // Present only when the panel may write and the target can: everything downstream then treats
  // its absence as "editing is off" without re-deriving why.
  const editor: EditableTarget | undefined =
    deps.write && isEditableTarget(target) ? target : undefined;
  const follower: Follower | undefined = isLiveTarget(target) ? createFollower(target) : undefined;

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
  const live = button("btn mini", "Live", {
    title: "Reload the rows whenever the database changes",
    attrs: { "aria-pressed": "false" },
  });
  live.hidden = follower === undefined;
  let liveOn = false;
  const detailToggle = button("btn mini", "Details", {
    title: "Read the selected row in full beside the grid",
    attrs: { "aria-pressed": "false" },
  });
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
      showDetail();
    },
    onEditCell: (row, column, index) => {
      beginCellEdit(row, column, index);
    },
    onCopied: (what) => {
      setStatus(`copied ${what}`);
    },
    onContextMenu: (hit, at) => {
      menu.open(menuFor(hit), at);
    },
  });
  const menu = createMenu();

  const filterBar = createFilterBar(() => {
    void reload({ recount: true });
  });
  const insertForm = createInsertForm({
    onSubmit: (values) => {
      void commitInsert(values);
    },
  });
  const detail = createRowDetail({
    catalog: () => catalog,
    onFollow: (name, column, value) => {
      const parent = findTable(catalog, name);
      const type = parent?.columns.find((entry) => entry.name === column)?.type ?? "string";
      void open(name, [{ column, type, operator: "=", values: [value] }]);
    },
    onCopied: (what) => {
      setStatus(`copied ${what}`);
    },
    onClose: () => {
      setDetailOpen(false);
    },
  });

  const main = el("div", { class: "explorer-main" }, [
    el("div", { class: "toolbar" }, [
      tablePicker,
      crumb,
      el("span", { class: "spacer" }),
      live,
      detailToggle,
      addRow,
      deleteRow,
    ]),
    banner,
    filterBar.node,
    el("div", { class: "explorer-body" }, [grid.node, detail.node]),
    insertForm.node,
    menu.node,
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

  const detailKey = `${deps.storageKey}:detail`;
  let detailOpen = readFlag(detailKey, false);

  function setDetailOpen(next: boolean): void {
    detailOpen = next;
    writeFlag(detailKey, next);
    detail.node.hidden = !next;
    detailToggle.setAttribute("aria-pressed", String(next));
    detailToggle.classList.toggle("on", next);
    showDetail();
  }

  function showDetail(): void {
    if (!detailOpen) return;
    const index = grid.selectedIndex();
    detail.show(table, index === undefined ? undefined : grid.rowAt(index));
  }

  function setStatus(...parts: string[]): void {
    status.replaceChildren(...parts.map((part) => el("span", { text: part })));
  }

  function describeCount(): string {
    const loaded = grid.rowCount();
    if (total === undefined) return `${formatCount(loaded)} rows loaded · counting…`;
    if (loaded >= total) return total === 1 ? "1 row" : `${formatCount(total)} rows`;
    return `${formatCount(loaded)} of ${formatCount(total)} rows`;
  }

  function columnsFor(current: TableInfo): GridColumn[] {
    return current.columns.map((column) => ({
      name: column.name,
      type: column.type,
      label: `${column.typeLabel ?? column.type}${column.nullable ? "?" : ""}`,
      sortable: true,
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

  /** Puts a page's rows in the grid and remembers where the next page starts. */
  function accept(current: TableInfo, rows: QueryRow[], append: boolean): void {
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
    showDetail();
    exhausted = rows.length < pageSize;
    const last = rows[rows.length - 1];
    cursor = last === undefined ? cursor : cursorFrom(last, current, sort);
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
      accept(current, page.rows, append);
      const elapsed = Math.round(performance.now() - started);
      setStatus(describeCount(), pagingLabel(current), `${String(elapsed)}ms`);
    } catch (error) {
      if (token !== generation) return;
      grid.setMessage(messageOf(error));
      setStatus("failed");
      exhausted = true;
    } finally {
      if (inFlight === token) inFlight = undefined;
    }
  }

  /**
   * Reads the first page again. The count is a full scan, so it is repeated only when the answer
   * can have changed — the table or the filters — and not for a sort, which reorders the same rows.
   */
  async function reload(options: { recount: boolean }): Promise<void> {
    const current = table;
    if (current === undefined) return;
    generation += 1;
    const token = generation;
    cursor = undefined;
    exhausted = false;
    grid.setColumns(columnsFor(current));
    setStatus("loading…");
    follow(current);
    await Promise.all([
      fetchPage(current, token, false),
      options.recount ? count(current, token) : Promise.resolve(),
    ]);
  }

  async function loadMore(): Promise<void> {
    const current = table;
    if (current === undefined || exhausted || inFlight !== undefined) return;
    await fetchPage(current, generation, true);
  }

  /** Follows the first page, so a commit elsewhere in the app shows up here without a click. */
  function follow(current: TableInfo): void {
    if (follower === undefined) return;
    if (!liveOn) {
      follower.stop();
      return;
    }
    const token = generation;
    follower.follow(
      buildPageQuery({
        table: current,
        filters: filterBar.filters(),
        ...(sort === undefined ? {} : { sort }),
        limit: pageSize,
      }),
      (result: QueryResult) => {
        if (token !== generation) return;
        cursor = undefined;
        accept(current, result.rows, false);
        void count(current, token);
        setStatus(describeCount(), `live · ${new Date().toLocaleTimeString()}`);
      },
      (error) => {
        setStatus(`live update failed: ${messageOf(error)}`);
      },
    );
  }

  live.addEventListener("click", () => {
    liveOn = !liveOn;
    live.setAttribute("aria-pressed", String(liveOn));
    live.classList.toggle("on", liveOn);
    if (table !== undefined) follow(table);
  });
  detailToggle.addEventListener("click", () => {
    setDetailOpen(!detailOpen);
  });

  /**
   * Row editing is offered only where it can actually work: writes allowed, a target that has the
   * write API, and a table with a unique key to address a row by. The banner says which of those
   * is missing rather than leaving a dead button.
   */
  function updateRowActions(): void {
    const current = table;
    if (current === undefined) return;
    const blocked = editingBlockedReason(current, deps.write, isEditableTarget(target));
    const selected = grid.selectedIndexes().length;

    // Inserts need no key, so they survive the one blocker that only affects keyed writes.
    addRow.hidden = editor === undefined || !isInsertable(current);
    deleteRow.hidden = editor === undefined || !isEditable(current);
    deleteRow.disabled = selected === 0;
    deleteRow.textContent = selected > 1 ? `Delete ${String(selected)} rows` : "Delete row";

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
      setStatus(messageOf(error));
    }
  }

  function beginCellEdit(row: QueryRow, columnName: string, index: number): void {
    const current = table;
    if (current === undefined || editor === undefined) return;
    if (editingBlockedReason(current, deps.write, true) !== undefined) return;
    const column = current.columns.find(({ name }) => name === columnName);
    const key = rowKey(current, row);
    if (column === undefined || key === undefined || column.generated !== undefined) return;

    const from = row[columnName] ?? null;
    grid.editCell(index, columnName, {
      initial: formatForInput(from),
      placeholder: inputHint(column.type, column.nullable),
      ...(column.enumValues === undefined ? {} : { choices: column.enumValues }),
      onCancel: () => undefined,
      onCommit: (text) => {
        const parsed = parseInput(text, column.type, column.nullable, column.enumValues);
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
    showDetail();
  }

  function requestDelete(): void {
    const current = table;
    const indexes = grid.selectedIndexes();
    if (current === undefined || editor === undefined || indexes.length === 0) return;
    const picked = indexes.flatMap((index) => {
      const row = grid.rowAt(index);
      const key = row === undefined ? undefined : rowKey(current, row);
      return row === undefined || key === undefined ? [] : [{ index, row, key }];
    });
    const first = picked[0];
    if (first === undefined) return;

    const request =
      picked.length === 1
        ? confirmDelete(current, first.key, first.row)
        : confirmDeleteMany(
            current,
            picked.map((entry) => entry.key),
          );
    void withWrite(request, async () => {
      const removed =
        picked.length === 1
          ? await applyDelete(editor, current, first.key)
          : await applyDeleteMany(
              editor,
              current,
              picked.map((entry) => entry.key),
            );
      // Highest first, so each removal leaves the lower indexes where they were.
      for (const { index } of [...picked].sort((a, b) => b.index - a.index)) grid.removeRow(index);
      if (total !== undefined) total -= removed;
      updateRowActions();
      showDetail();
      return removed === 1 ? "deleted 1 row" : `deleted ${String(removed)} rows`;
    });
  }

  async function commitInsert(values: Record<string, QueryValue>): Promise<void> {
    const current = table;
    if (current === undefined || editor === undefined) return;
    await withWrite(confirmInsert(current, values), async () => {
      const inserted = await applyInsert(editor, current, values);
      insertForm.close();
      await reload({ recount: true });
      return `inserted ${String(inserted)} row`;
    });
  }

  /** Clicking a header cycles ascending, descending, then back to the table's own order. */
  function toggleSort(column: string): void {
    if (sort?.column !== column) sort = { column, direction: "asc" };
    else if (sort.direction === "asc") sort = { column, direction: "desc" };
    else sort = undefined;
    void reload({ recount: false });
  }

  async function copy(text: string, what: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
      setStatus(`copied ${what}`);
    } catch (error) {
      setStatus(`could not copy: ${messageOf(error)}`);
    }
  }

  /** What a right-click on a cell offers: the value, the row, the relationships, the edits. */
  function menuFor(hit: CellHit): MenuItem[] {
    const current = table;
    if (current === undefined) return [];
    const value = hit.row[hit.column] ?? null;
    const column = current.columns.find(({ name }) => name === hit.column);
    const names = current.columns.map(({ name }) => name);
    const valueText = formatForInput(value);
    const items: MenuItem[] = [
      {
        label: "Copy value",
        hint: value === null ? "NULL" : valueText,
        run: () => void copy(valueText, "value"),
      },
      { label: "Copy row as JSON", run: () => void copy(rowToJson(names, hit.row), "row as JSON") },
      {
        label: "Copy row as INSERT",
        run: () => void copy(toInsertSql(current.name, names, [hit.row]), "row as INSERT"),
      },
    ];
    if (column !== undefined) {
      items.push({
        label: value === null ? `Filter: ${hit.column} is null` : `Filter: ${hit.column} =`,
        ...(value === null ? {} : { hint: valueText }),
        run: () => {
          filterBar.add(
            value === null
              ? { column: hit.column, type: column.type, operator: "is null", values: [] }
              : { column: hit.column, type: column.type, operator: "=", values: [value] },
          );
        },
      });
    }
    const key = foreignKeyFor(current, hit.column);
    const parentColumn = key?.parentColumns[0];
    if (key !== undefined && parentColumn !== undefined && value !== null) {
      items.push({
        label: `Open ${key.parentTable} where ${parentColumn} =`,
        hint: valueText,
        run: () => {
          const parent = findTable(catalog, key.parentTable);
          const type =
            parent?.columns.find((entry) => entry.name === parentColumn)?.type ?? "string";
          void open(key.parentTable, [
            { column: parentColumn, type, operator: "=", values: [value] },
          ]);
        },
      });
    }
    for (const { child, key: relation } of childRelations(catalog, current)) {
      const parent = relation.parentColumns[0];
      const childColumn = relation.columns[0];
      const parentValue = parent === undefined ? null : (hit.row[parent] ?? null);
      if (parent === undefined || childColumn === undefined || parentValue === null) continue;
      const type = child.columns.find((entry) => entry.name === childColumn)?.type ?? "string";
      items.push({
        label: `Related ${child.name} where ${childColumn} =`,
        hint: formatForInput(parentValue),
        run: () => {
          void open(child.name, [
            { column: childColumn, type, operator: "=", values: [parentValue] },
          ]);
        },
      });
    }
    if (deps.onRunQuery !== undefined) {
      items.push({
        label: "Query these rows in the console",
        run: () => {
          deps.onRunQuery?.(buildBrowseQuery(current, filterBar.filters(), sort));
        },
      });
    }
    if (editor !== undefined && isEditable(current) && column?.generated === undefined) {
      items.push({
        label: `Edit ${hit.column}`,
        run: () => {
          beginCellEdit(hit.row, hit.column, hit.index);
        },
      });
    }
    if (editor !== undefined && isInsertable(current)) {
      items.push({
        label: "Duplicate row…",
        run: () => {
          const seed = Object.fromEntries(
            current.columns
              .filter((entry) => !entry.isUniqueKey && entry.hasDefault !== true)
              .map((entry) => [entry.name, hit.row[entry.name] ?? null]),
          );
          insertForm.open(current, seed);
        },
      });
    }
    if (editor !== undefined && isEditable(current)) {
      const selected = grid.selectedIndexes().length;
      items.push({
        label: selected > 1 ? `Delete ${String(selected)} rows…` : "Delete row…",
        danger: true,
        run: requestDelete,
      });
    }
    return items;
  }

  async function open(name: string, filters?: readonly Filter[]): Promise<void> {
    const current = findTable(catalog, name);
    if (current === undefined) return;
    table = current;
    deps.onOpen?.(current.name);
    sort = undefined;
    filterBar.setTable(current);
    if (filters !== undefined) filterBar.setFilters(filters);
    insertForm.close();
    menu.close();
    updateRowActions();
    tablePicker.value = name;
    const indexCount = current.indexes?.length ?? 0;
    crumb.textContent = [
      current.view !== undefined
        ? "view"
        : current.uniqueKey === undefined
          ? "no unique key"
          : `key: ${current.uniqueKey}`,
      indexCount === 0 ? "" : `${String(indexCount)} ${indexCount === 1 ? "index" : "indexes"}`,
    ]
      .filter((part) => part.length > 0)
      .join(" · ");
    await reload({ recount: true });
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
    const reopen = tableToReopen(catalog, table?.name);
    if (reopen === undefined) {
      table = undefined;
      deps.onOpen?.(undefined);
      grid.setMessage("This database has no tables yet.");
      setStatus("no tables");
      return;
    }
    await open(reopen);
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
  detail.node.hidden = !detailOpen;
  detailToggle.setAttribute("aria-pressed", String(detailOpen));
  detailToggle.classList.toggle("on", detailOpen);
  detail.show(undefined, undefined);

  return {
    node,
    setCatalog,
    open,
    layout: () => {
      grid.layout();
    },
    destroy: () => {
      follower?.destroy();
    },
  };
}
