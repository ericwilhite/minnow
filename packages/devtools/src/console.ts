import { SqlCompileError, type ExecuteResult, type QueryResult } from "@minnowdb/core";
import type { ConfirmLayer } from "./confirm.js";
import { button, el, icon, icons } from "./dom.js";
import { createTextareaEditor, type EditorSchema, type SqlEditor } from "./editor/editor.js";
import { createHistoryRail } from "./history/rail.js";
import { createHistoryStore, type HistoryEntry } from "./history/store.js";
import { draggable } from "./panel/drag.js";
import { resizeSplit } from "./panel/window.js";
import { createGrid } from "./results/grid.js";
import { readStored, writeStored } from "./storage.js";
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
  /** Loads the real editor. Called the first time the tab is shown. */
  upgrade(): Promise<void>;
  /** Feeds completion from the panel's shared catalog. */
  setSchema(schema: EditorSchema): void;
  /** Drops text in at the caret — how the rail contributes to a query. */
  insert(text: string): void;
  /** Replaces the whole statement. Embedders use this to offer a query to start from. */
  setQuery(sql: string): void;
  /** Releases the editor. CodeMirror registers observers that outlive its DOM node. */
  destroy(): void;
}

function describeExecuteResult(result: ExecuteResult): string {
  if (result.kind === "rows") return `${String(result.result.rows.length)} rows`;
  if (result.kind === "create-table") return `created table ${result.table}`;
  if (result.kind === "create-trigger") return `created trigger ${result.name} on ${result.table}`;
  if (result.kind === "drop-trigger") return `dropped trigger ${result.name}`;
  if (result.kind === "add-column") return `added column ${result.table}.${result.column}`;
  if (result.kind === "create-view") return `created view ${result.view}`;
  if (result.kind === "drop-view") {
    return result.dropped ? `dropped view ${result.view}` : `no view ${result.view}`;
  }
  if (result.kind === "drop-table") {
    return result.dropped ? `dropped table ${result.table}` : `no table ${result.table}`;
  }
  if (result.kind === "transaction") return `${result.action} transaction`;
  const rows = result.rowCount === 1 ? "1 row" : `${String(result.rowCount)} rows`;
  return `${result.kind}: ${rows} in ${result.table}`;
}

function affectedRows(result: ExecuteResult): number {
  if (result.kind !== "rows") {
    return "rowCount" in result ? result.rowCount : 0;
  }
  return result.result.rows.length;
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
    storageKey: deps.storageKey,
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

  const planText = el("pre", { class: "plan" });
  const planView = el("div", { class: "plan-view" }, [planText]);
  planView.hidden = true;

  const resultTabs = el("div", { class: "subtabs" });
  const rowsTab = button("subtab on", "Rows");
  const planTab = button("subtab", "Plan");
  resultTabs.append(rowsTab, planTab);

  const splitter = el("div", {
    class: "splitter",
    attrs: {
      role: "separator",
      "aria-orientation": "horizontal",
      "aria-label": "Resize the editor",
    },
  });
  const main = el("div", { class: "console-main" }, [
    el("div", { class: "toolbar" }, [
      run,
      el("span", { class: "hint", text: "Ctrl/Cmd + Enter" }),
      el("span", { class: "spacer" }),
    ]),
    editorSlot,
    splitter,
    notice,
    resultTabs,
    grid.node,
    planView,
    status,
  ]);
  const node = el("div", { class: "console" }, [main, historyRail.node]);

  /**
   * The editor's share of the console, dragged by the splitter. Stored in pixels rather than a
   * fraction: the editor wants a number of lines, and that should not change because the panel
   * got taller.
   */
  const splitKey = `${deps.storageKey}:editor-height`;
  const storedHeight = Number(readStored(splitKey) ?? "");
  let editorHeight = Number.isFinite(storedHeight) && storedHeight > 0 ? storedHeight : 170;

  function applySplit(): void {
    editorSlot.style.height = `${String(Math.round(editorHeight))}px`;
  }

  let splitStart = 0;
  let splitBounds = 0;
  draggable(splitter, {
    onStart: () => {
      // Both measurements are taken once. Reading layout inside the move handler would force a
      // reflow on every pointer event, against a value that cannot change mid-drag anyway.
      splitStart = editorSlot.getBoundingClientRect().height;
      splitBounds = main.getBoundingClientRect().height;
      splitter.classList.add("dragging");
      return true;
    },
    onMove: (_dx, dy) => {
      editorHeight = resizeSplit(splitStart, dy, splitBounds, { top: 60, bottom: 120 });
      applySplit();
    },
    onEnd: () => {
      splitter.classList.remove("dragging");
      writeStored(splitKey, String(Math.round(editorHeight)));
    },
  });
  applySplit();

  grid.setMessage("Run a query to see rows here.");
  renderHistory();

  /**
   * Rows and the plan are two readings of the same statement, so they are tabs over one result
   * area rather than a second panel. The plan is asked for only when it is looked at — it compiles
   * and optimizes the query, which is work nobody wants on every run.
   */
  function showResultTab(which: "rows" | "plan"): void {
    rowsTab.className = which === "rows" ? "subtab on" : "subtab";
    planTab.className = which === "plan" ? "subtab on" : "subtab";
    grid.node.hidden = which !== "rows";
    planView.hidden = which !== "plan";
    if (which === "plan") void loadPlan();
  }

  async function loadPlan(): Promise<void> {
    const sql = editor.value().trim();
    if (sql.length === 0) {
      planText.textContent = "Write a query to see its plan.";
      return;
    }
    planText.textContent = "Explaining…";
    try {
      planText.textContent = await deps.target.explain(sql);
    } catch (error) {
      planText.textContent = error instanceof Error ? error.message : String(error);
    }
  }

  rowsTab.addEventListener("click", () => {
    showResultTab("rows");
  });
  planTab.addEventListener("click", () => {
    showResultTab("plan");
  });

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

  /**
   * The matrix lookup, available once the editor chunk that carries the matrix has loaded. Until
   * then a failure reports the compiler's message and nothing more.
   */
  let explainUnsupported: ((message: string) => string | undefined) | undefined;

  /** Points the caret at the failure, so a located compile error reads as one in the editor. */
  function showFailure(error: unknown): void {
    const message = error instanceof Error ? error.message : String(error);
    const extra = explainUnsupported?.(message);
    setNotice("error", extra === undefined ? message : `${message}\n${extra}`);
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
    showResultTab("rows");
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

  run.addEventListener("click", () => {
    void execute();
  });

  let schema: EditorSchema = {};

  return {
    node,
    focus: () => {
      editor.focus();
    },
    setSchema: (next) => {
      schema = next;
      editor.setSchema(next);
    },
    insert: (text) => {
      editor.insert(text);
    },
    setQuery: (sql) => {
      editor.setValue(sql);
      editor.focus();
    },
    destroy: () => {
      editor.destroy();
    },
    upgrade: async () => {
      try {
        const [{ loadCodeMirrorEditor }, diagnostics] = await Promise.all([
          import("./editor/codemirror.js"),
          import("./editor/diagnostics.js"),
        ]);
        explainUnsupported = diagnostics.explainUnsupported;
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
        // The new slot carries none of the old one's inline height, so the split has to be
        // reapplied or the editor drops to its floor the moment CodeMirror arrives.
        applySplit();
      } catch {
        // The chunk can be blocked or absent. The textarea it replaces is still there and still
        // runs queries, so this costs highlighting and completion, never the console.
      }
    },
  };
}
