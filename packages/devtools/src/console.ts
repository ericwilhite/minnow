import { SqlCompileError, type ExecuteResult, type QueryResult } from "@minnowdb/core";
import type { ConfirmLayer } from "./confirm.js";
import { button, el, icon, icons } from "./dom.js";
import { createTextareaEditor, type EditorSchema, type SqlEditor } from "./editor/editor.js";
import { createHistoryRail } from "./history/rail.js";
import { createHistoryStore, type HistoryEntry } from "./history/store.js";
import { createGrid } from "./results/grid.js";
import { changesData, classifyStatement, summarize, writeBlockedMessage } from "./statements.js";
import type { DevtoolsTarget } from "./target.js";

export interface ConsoleDeps {
  target: DevtoolsTarget;
  confirm: ConfirmLayer;
  /** Whether statements that change data are allowed to run at all. */
  write: boolean;
  initialQuery: string;
  /** Namespaces the remembered history alongside the panel geometry. */
  storageKey: string;
  /** The shadow root, which CodeMirror needs to place itself correctly. */
  root: ShadowRoot;
}

export interface ConsoleView {
  node: HTMLElement;
  focus(): void;
  /** Loads the real editor and the catalog. Called the first time the tab is shown. */
  upgrade(): Promise<void>;
}

function describeExecuteResult(result: ExecuteResult): string {
  if (result.kind === "rows") return `${String(result.result.rows.length)} rows`;
  const rows = result.rowCount === 1 ? "1 row" : `${String(result.rowCount)} rows`;
  return `${result.kind}: ${rows} in ${result.table}`;
}

function affectedRows(result: ExecuteResult): number {
  return result.kind === "rows" ? result.result.rows.length : result.rowCount;
}

/** The SQL console: type a statement, run it, read the rows, and find it again afterwards. */
export function createConsole(deps: ConsoleDeps): ConsoleView {
  const history = createHistoryStore(globalThis.localStorage, deps.storageKey);
  let editor: SqlEditor = createTextareaEditor({
    initial: deps.initialQuery,
    onRun: () => {
      void execute();
    },
  });
  let editorSlot = el("div", { class: "editor-slot" }, [editor.node]);
  let selectedEntry: string | undefined;

  const run = button("btn primary", "Run");
  run.prepend(icon(icons.play));
  const notice = el("div", { class: "notice" });
  notice.hidden = true;
  const grid = createGrid();
  const status = el("div", { class: "statusbar" }, [el("span", { text: "ready" })]);

  const historyRail = createHistoryRail({
    onPick: (entry) => {
      selectedEntry = entry.id;
      editor.setValue(entry.sql);
      editor.focus();
      const cached = history.resultFor(entry.id);
      if (cached === undefined) {
        grid.setMessage("Rows for this run are no longer cached. Run it again to see them.");
        setStatus(entry.sql.length === 0 ? "ready" : "recalled");
      } else {
        renderRows(cached);
        setStatus(`recalled · ${String(cached.rows.length)} rows`);
      }
      renderHistory();
    },
    onClear: () => {
      history.clear();
      selectedEntry = undefined;
      renderHistory();
    },
  });

  const main = el("div", { class: "console-main" }, [
    el("div", { class: "toolbar" }, [
      run,
      el("span", { class: "hint", text: "Ctrl/Cmd + Enter" }),
      el("span", { class: "spacer" }),
    ]),
    editorSlot,
    notice,
    grid.node,
    status,
  ]);
  const node = el("div", { class: "console" }, [main, historyRail.node]);

  grid.setMessage("Run a query to see rows here.");
  renderHistory();

  function renderHistory(): void {
    historyRail.render(history.entries(), selectedEntry);
  }

  function setNotice(kind: "error" | "blocked" | "done", message: string): void {
    notice.className = `notice ${kind}`;
    notice.replaceChildren(icon(icons.warning), el("span", { text: message }));
    notice.hidden = false;
  }

  function setStatus(text: string): void {
    status.replaceChildren(el("span", { text }));
  }

  function renderRows(result: QueryResult): void {
    if (result.rows.length === 0) {
      grid.setMessage("No rows.");
      return;
    }
    grid.setColumns(result.columns.map((name) => ({ name })));
    grid.setRows(result.rows);
  }

  /** Points the caret at the failure, so a located compile error reads as one in the editor. */
  function showFailure(error: unknown): void {
    setNotice("error", error instanceof Error ? error.message : String(error));
    if (error instanceof SqlCompileError) {
      editor.selectRange(error.offset, error.offset + Math.max(error.length, 1));
    }
  }

  function record(sql: string, entry: Omit<HistoryEntry, "id" | "at" | "sql">): HistoryEntry {
    const recorded = history.add({ ...entry, sql }, crypto.randomUUID(), Date.now());
    selectedEntry = recorded.id;
    renderHistory();
    return recorded;
  }

  async function execute(): Promise<void> {
    const sql = editor.value().trim();
    if (sql.length === 0) {
      setNotice("error", "Enter a statement to run.");
      return;
    }

    let intent;
    try {
      intent = classifyStatement(sql);
    } catch (error) {
      showFailure(error);
      return;
    }

    if (changesData(intent)) {
      if (!deps.write) {
        setNotice("blocked", writeBlockedMessage(intent));
        return;
      }
      const summary = summarize(intent);
      const confirmed = await deps.confirm.ask({
        title: summary.title,
        facts: summary.facts,
        sql,
        ...(summary.warning === undefined ? {} : { warning: summary.warning }),
        confirmLabel: intent.kind === "delete" ? "Delete rows" : `Run ${intent.kind}`,
        destructive: intent.kind === "delete",
      });
      if (!confirmed) {
        setStatus("cancelled");
        return;
      }
    }

    notice.hidden = true;
    run.disabled = true;
    setStatus("running…");
    const started = performance.now();
    try {
      if (intent.kind === "select") {
        const result = await deps.target.query(sql);
        const ms = Math.round(performance.now() - started);
        renderRows(result);
        const entry = record(sql, { ms, rowCount: result.rows.length });
        history.rememberResult(entry.id, result);
        setStatus(`${String(result.rows.length)} rows · ${String(ms)}ms`);
      } else {
        const result = await deps.target.execute(sql);
        const ms = Math.round(performance.now() - started);
        grid.setMessage("Statement ran.");
        setNotice("done", describeExecuteResult(result));
        record(sql, { ms, rowCount: affectedRows(result) });
        setStatus(`${describeExecuteResult(result)} · ${String(ms)}ms`);
      }
    } catch (error) {
      const ms = Math.round(performance.now() - started);
      showFailure(error);
      record(sql, { ms, error: error instanceof Error ? error.message : String(error) });
      setStatus("failed");
    } finally {
      run.disabled = false;
    }
  }

  /** Table name to columns, for completion. A catalog that fails to load costs completion only. */
  async function readSchema(): Promise<EditorSchema> {
    try {
      const tables = await deps.target.listTables();
      return Object.fromEntries(
        tables.map((table) => [table.name, table.columns.map((column) => column.name)]),
      );
    } catch {
      return {};
    }
  }

  run.addEventListener("click", () => {
    void execute();
  });

  return {
    node,
    focus: () => {
      editor.focus();
    },
    upgrade: async () => {
      const schema = await readSchema();
      editor.setSchema(schema);
      try {
        const { loadCodeMirrorEditor } = await import("./editor/codemirror.js");
        const upgraded = await loadCodeMirrorEditor({
          root: deps.root,
          schema,
          initial: editor.value(),
          onRun: () => {
            void execute();
          },
        });
        const slot = el("div", { class: "editor-slot" }, [upgraded.node]);
        editorSlot.replaceWith(slot);
        editor.destroy();
        editor = upgraded;
        editorSlot = slot;
      } catch {
        // The chunk can be blocked or absent. The textarea it replaces is still there and still
        // runs queries, so this costs highlighting and completion, never the console.
      }
    },
  };
}
