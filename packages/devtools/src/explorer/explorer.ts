import type { QueryRow } from "@minnowdb/core";
import { el } from "../dom.js";
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
import type { DevtoolsTarget } from "../target.js";
import { findTable, toCatalog, type TableInfo } from "./catalog.js";
import { createFilterBar } from "./filter-editor.js";
import { createSchemaRail } from "./tree.js";

export interface ExplorerView {
  node: HTMLElement;
  /** Loads the catalog; safe to call again to pick up new tables. */
  refresh(): Promise<void>;
}

/** Rows per request. Large enough that scrolling rarely waits, small enough to stay instant. */
const pageSize = 200;

export function createExplorer(target: DevtoolsTarget): ExplorerView {
  const status = el("div", { class: "statusbar" });
  const crumb = el("span", { class: "crumb", text: "Pick a table" });

  const grid = createGrid({
    onSort: (column) => {
      toggleSort(column);
    },
    onNearEnd: () => {
      void loadMore();
    },
  });

  const filterBar = createFilterBar(() => {
    void reload();
  });
  const rail = createSchemaRail((name) => {
    void open(name);
  });

  const main = el("div", { class: "explorer-main" }, [
    el("div", { class: "toolbar" }, [crumb, el("span", { class: "spacer" })]),
    filterBar.node,
    grid.node,
    status,
  ]);
  const node = el("div", { class: "explorer" }, [rail.node, main]);

  let catalog: TableInfo[] = [];
  let table: TableInfo | undefined;
  let sort: Sort | undefined;
  let cursor: Cursor | undefined;
  let exhausted = false;
  let loading = false;
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
    if (loading) return;
    loading = true;
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
      else grid.setRows(rows);

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
      loading = false;
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
    if (current === undefined || exhausted || loading) return;
    await fetchPage(current, generation, true);
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
    rail.setSelected(name);
    filterBar.setTable(current);
    crumb.replaceChildren(
      el("span", { text: name }),
      el("span", {
        class: "crumb-meta",
        text: current.uniqueKey === undefined ? "no unique key" : `key: ${current.uniqueKey}`,
      }),
    );
    await reload();
  }

  async function refresh(): Promise<void> {
    try {
      catalog = toCatalog(await target.listTables());
      rail.setCatalog(catalog);
      if (catalog.length === 0) {
        grid.setMessage("This database has no tables yet.");
        setStatus("no tables");
        return;
      }
      const reopened = table === undefined ? undefined : findTable(catalog, table.name);
      await open((reopened ?? catalog[0])?.name ?? "");
    } catch (error) {
      rail.setError(error instanceof Error ? error.message : String(error));
    }
  }

  grid.setMessage("Pick a table to browse it.");

  return { node, refresh };
}
