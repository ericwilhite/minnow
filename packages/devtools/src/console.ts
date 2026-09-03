import type { ExecuteResult, QueryResult } from "@minnowdb/core";
import type { ConfirmLayer, ConfirmRequest } from "./confirm.js";
import { button, el, icon, icons } from "./dom.js";
import { downloadText } from "./download.js";
import { createTextareaEditor, type EditorSchema, type SqlEditor } from "./editor/editor.js";
import { isLocatedError, messageOf } from "./errors.js";
import { formatBytes, formatCount } from "./format.js";
import { createHistoryRail } from "./history/rail.js";
import { createHistoryStore, type HistoryEntry } from "./history/store.js";
import { createFollower, isLiveTarget, type Follower } from "./live.js";
import { draggable } from "./panel/drag.js";
import { resizeSplit } from "./panel/window.js";
import { columnsOf } from "./results/columns.js";
import { createGrid } from "./results/grid.js";
import { toCsv, toJson } from "./results/serialize.js";
import { splitStatements, withAppendedClause, type ScriptStatement } from "./sql/split.js";
import type { StatementIntent } from "./statements.js";
import { readStored, writeStored } from "./storage.js";
import { isCursorTarget, type DevtoolsTarget } from "./target.js";

interface ConsoleDeps {
  target: DevtoolsTarget;
  confirm: ConfirmLayer;
  /** Whether statements that change data are allowed to run at all. */
  write: boolean;
  initialQuery: string;
  /** Namespaces the remembered history alongside the panel geometry. */
  storageKey: string;
  /** The shadow root, which CodeMirror needs to place itself correctly. */
  root: ShadowRoot;
  /** Refreshes shared schema metadata after a successful DDL statement. */
  onCatalogChange(): Promise<void>;
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
  /** Replaces the statement and runs it — how another view hands a query over. */
  runQuery(sql: string): Promise<void>;
  /** Repaints the editor for a palette switch. */
  setDark(dark: boolean): void;
  /** Releases the editor and any live subscription. */
  destroy(): void;
}

/**
 * How many rows an unbounded SELECT shows before the console stops it. The rows come over in one
 * message and are held in memory with the history, so a `SELECT *` over a million-row table is
 * capped here, said so in the status bar, and offered whole on request.
 */
export const rowCap = 1000;

function describeExecuteResult(result: ExecuteResult): string {
  if (result.kind === "rows") return `${formatCount(result.result.rows.length)} rows`;
  if (result.kind === "create-table") return `created table ${result.table}`;
  if (result.kind === "create-type") return `created type ${result.name}`;
  if (result.kind === "create-sequence") return `created sequence ${result.name}`;
  if (result.kind === "create-index") {
    return `created${result.unique ? " unique" : ""} index ${result.index} on ${result.table}(${result.columns.join(", ")})`;
  }
  if (result.kind === "drop-index") {
    return result.dropped ? `dropped index ${result.index}` : `no index ${result.index}`;
  }
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
  if (result.kind === "drop-column") {
    return result.dropped
      ? `dropped column ${result.table}.${result.column}`
      : `no column ${result.table}.${result.column}`;
  }
  if (result.kind === "transaction") return `${result.action} transaction`;
  if (result.kind === "set") return `${result.action} ${result.name}`;
  const rows = result.rowCount === 1 ? "1 row" : `${formatCount(result.rowCount)} rows`;
  return `${result.kind}: ${rows} in ${result.table}`;
}

function affectedRows(result: ExecuteResult): number {
  if (result.kind !== "rows") {
    return "rowCount" in result ? result.rowCount : 0;
  }
  return result.result.rows.length;
}

/** The rows a statement handed back: a RETURNING clause, or a SHOW's own row. */
function returnedResult(result: ExecuteResult): QueryResult | undefined {
  if (result.kind === "rows") return result.result;
  if (!("returnedRows" in result)) return undefined;
  const { returnedRows } = result;
  const columns = result.returnedColumns ?? Object.keys(returnedRows[0] ?? {});
  return {
    columns,
    columnDomains: result.returnedColumnDomains ?? columns.map(() => null),
    rows: returnedRows,
  };
}

/** What one statement of a run produced, for the status line, the notice, and the history. */
interface Outcome {
  summary: string;
  rows?: QueryResult;
  rowCount: number;
  /** True when the rows shown are the first `rowCap` of more. */
  capped?: boolean;
}

/** An abort surfaces as a DOMException named AbortError from the engine and the client alike. */
function isAbort(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
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

  /**
   * The compiler is a large chunk that the launcher, the explorer, and a page that only browses
   * never need; it is fetched the first time something is run or explained. Reading a statement's
   * intent is the one thing the console cannot do without it.
   */
  let statementsModule: Promise<typeof import("./statements.js")> | undefined;
  const statements = (): Promise<typeof import("./statements.js")> =>
    (statementsModule ??= import("./statements.js"));

  const run = button("btn primary", "Run");
  run.prepend(icon(icons.play));
  const cancel = button("btn", "Cancel", { title: "Stop the running query" });
  cancel.hidden = true;
  const live = button("btn mini", "Live", {
    title: "Re-run the query whenever the database changes",
    attrs: { "aria-pressed": "false" },
  });
  const follower: Follower | undefined = isLiveTarget(deps.target)
    ? createFollower(deps.target)
    : undefined;
  live.hidden = follower === undefined;
  let liveOn = false;
  /** The capped statement the last successful SELECT ran, which is what Live follows. */
  let lastSelect: string | undefined;

  const notice = el("div", { class: "notice", attrs: { "aria-live": "polite" } });
  notice.hidden = true;
  const grid = createGrid();
  const status = el(
    "div",
    { class: "statusbar", attrs: { role: "status", "aria-live": "polite" } },
    [el("span", { text: "ready" })],
  );

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
        setStatus(`recalled · ${formatCount(cached.rows.length)} rows`);
      }
      renderHistory();
    },
    onToggleSaved: (entry) => {
      history.toggleSaved(entry.id);
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

  const resultTabs = el("div", { class: "subtabs", attrs: { role: "tablist" } });
  const rowsTab = button("subtab on", "Rows", {
    attrs: {
      role: "tab",
      "aria-selected": "true",
      "aria-controls": "mdt-result-rows",
    },
  });
  const planTab = button("subtab", "Plan", {
    attrs: {
      role: "tab",
      "aria-selected": "false",
      "aria-controls": "mdt-result-plan",
      tabindex: "-1",
    },
  });
  const copyCsv = button("btn mini", "Copy CSV", { title: "Copy the rows as CSV" });
  const copyJson = button("btn mini", "Copy JSON", { title: "Copy the rows as JSON" });
  const download = button("btn mini", "Download CSV", {
    title: "Save every row of the last query as a CSV file",
  });
  const resultActions = el("span", { class: "result-actions" }, [copyCsv, copyJson, download]);
  resultActions.hidden = true;
  resultTabs.append(rowsTab, planTab, el("span", { class: "spacer" }), resultActions);
  grid.node.id = "mdt-result-rows";
  grid.node.setAttribute("aria-label", "Rows");
  planView.id = "mdt-result-plan";
  planView.setAttribute("role", "tabpanel");
  planView.setAttribute("aria-label", "Plan");

  const splitter = el("div", {
    class: "splitter",
    attrs: {
      role: "separator",
      "aria-orientation": "horizontal",
      "aria-label": "Resize the editor",
      tabindex: "0",
      "aria-valuemin": "60",
      "aria-valuenow": "170",
    },
  });
  const main = el("div", { class: "console-main" }, [
    el("div", { class: "toolbar" }, [
      run,
      cancel,
      el("span", { class: "hint", text: "Ctrl/Cmd + Enter · a selection runs alone" }),
      el("span", { class: "spacer" }),
      live,
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
    splitter.setAttribute("aria-valuenow", String(Math.round(editorHeight)));
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

  splitter.addEventListener("keydown", (event) => {
    if (event.key !== "ArrowUp" && event.key !== "ArrowDown") return;
    event.preventDefault();
    const bounds = main.getBoundingClientRect().height;
    editorHeight = resizeSplit(editorHeight, event.key === "ArrowUp" ? -10 : 10, bounds, {
      top: 60,
      bottom: 120,
    });
    applySplit();
    writeStored(splitKey, String(Math.round(editorHeight)));
  });

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
    rowsTab.setAttribute("aria-selected", String(which === "rows"));
    planTab.setAttribute("aria-selected", String(which === "plan"));
    rowsTab.tabIndex = which === "rows" ? 0 : -1;
    planTab.tabIndex = which === "plan" ? 0 : -1;
    grid.node.hidden = which !== "rows";
    planView.hidden = which !== "plan";
    if (which === "plan") void loadPlan();
  }

  async function loadPlan(): Promise<void> {
    const sql = textToRun().text;
    if (sql.length === 0) {
      planText.textContent = "Write a query to see its plan.";
      return;
    }
    planText.textContent = "Explaining…";
    try {
      const { classifyStatement } = await statements();
      const parts = splitStatements(sql);
      const only = parts.length === 1 ? parts[0] : undefined;
      if (only === undefined) {
        planText.textContent =
          "Plans are shown for one SELECT at a time; select one to explain it.";
        return;
      }
      if (classifyStatement(only.sql).kind !== "select") {
        planText.textContent = "Plans are available for SELECT statements.";
        return;
      }
      planText.textContent = await deps.target.explain(only.sql);
    } catch (error) {
      planText.textContent = messageOf(error);
    }
  }

  rowsTab.addEventListener("click", () => {
    showResultTab("rows");
  });
  planTab.addEventListener("click", () => {
    showResultTab("plan");
  });
  resultTabs.addEventListener("keydown", (event) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    if (event.target !== rowsTab && event.target !== planTab) return;
    event.preventDefault();
    const next = event.target === rowsTab ? planTab : rowsTab;
    showResultTab(next === rowsTab ? "rows" : "plan");
    next.focus();
  });

  function renderHistory(): void {
    historyRail.render(history.entries(), selectedEntry);
  }

  function setNotice(kind: "error" | "blocked" | "done", message: string): void {
    notice.className = `notice ${kind}`;
    notice.setAttribute("role", kind === "done" ? "status" : "alert");
    notice.setAttribute("aria-live", kind === "done" ? "polite" : "assertive");
    notice.replaceChildren(icon(icons.warning), el("span", { text: message }));
    notice.hidden = false;
  }

  function setStatus(text: string, ...extras: HTMLElement[]): void {
    status.replaceChildren(el("span", { text }), ...extras);
  }

  /** The last rows shown, for the copy and download actions. */
  let shown: QueryResult | undefined;
  /** The statement the shown rows came from, uncapped, for a whole-result download. */
  let shownSql: string | undefined;
  let shownCapped = false;

  function renderRows(result: QueryResult, sql?: string, capped = false): void {
    shown = result;
    shownSql = sql;
    shownCapped = capped;
    resultActions.hidden = false;
    download.hidden = sql === undefined;
    if (result.rows.length === 0) {
      grid.setColumns(columnsOf(result));
      grid.setMessage("No rows.");
      return;
    }
    grid.setColumns(columnsOf(result));
    grid.setRows(result.rows);
  }

  /**
   * The matrix lookup, available once the editor chunk that carries the matrix has loaded. Until
   * then a failure reports the compiler's message and nothing more.
   */
  let explainUnsupported: ((message: string) => string | undefined) | undefined;

  /** Points the caret at the failure, so a located compile error reads as one in the editor. */
  function showFailure(error: unknown, base = 0): void {
    const message = messageOf(error);
    const extra = explainUnsupported?.(message);
    setNotice("error", extra === undefined ? message : `${message}\n${extra}`);
    if (isLocatedError(error)) {
      const from = base + error.offset;
      editor.selectRange(from, from + Math.max(error.length, 1));
    }
  }

  function record(sql: string, entry: Omit<HistoryEntry, "id" | "at" | "sql">): HistoryEntry {
    const recorded = history.add({ ...entry, sql }, crypto.randomUUID(), Date.now());
    selectedEntry = recorded.id;
    renderHistory();
    return recorded;
  }

  /** What Run runs: the selection when there is one, else the whole editor. */
  function textToRun(): { text: string; base: number } {
    const selected = editor.selectedText();
    if (selected.trim().length > 0) return { text: selected.trim(), base: editor.selectionStart() };
    return { text: editor.value().trim(), base: 0 };
  }

  function stopFollowing(): void {
    follower?.stop();
  }

  /** Follows the last SELECT, replacing the grid whenever the database changes what it returns. */
  function follow(): void {
    if (follower === undefined || !liveOn || lastSelect === undefined) return;
    const sql = lastSelect;
    follower.follow(
      sql,
      (result) => {
        const capped = result.rows.length > rowCap;
        const rows = capped ? { ...result, rows: result.rows.slice(0, rowCap) } : result;
        renderRows(rows, sql, capped);
        setStatus(
          `live · ${describeRows(rows.rows.length, capped)} · ${new Date().toLocaleTimeString()}`,
        );
      },
      (error) => {
        setNotice("error", `Live update failed: ${messageOf(error)}`);
      },
    );
  }

  function describeRows(count: number, capped: boolean): string {
    return capped ? `first ${formatCount(count)} rows` : `${formatCount(count)} rows`;
  }

  live.addEventListener("click", () => {
    liveOn = !liveOn;
    live.setAttribute("aria-pressed", String(liveOn));
    live.classList.toggle("on", liveOn);
    if (liveOn) follow();
    else stopFollowing();
  });

  let controller: AbortController | undefined;
  cancel.addEventListener("click", () => {
    controller?.abort();
    setStatus("cancelling…");
  });

  function setRunning(running: boolean): void {
    run.hidden = running;
    cancel.hidden = !running;
  }

  /** The confirmation for one statement, or for a whole script at once. */
  function confirmationFor(
    parts: readonly ScriptStatement[],
    intents: readonly StatementIntent[],
    text: string,
    tools: Awaited<ReturnType<typeof statements>>,
  ): ConfirmRequest | undefined {
    const confirmed = intents
      .map((intent, index) => ({ intent, part: parts[index] }))
      .filter(({ intent }) => tools.needsConfirmation(intent));
    if (confirmed.length === 0) return undefined;
    const first = confirmed[0];
    if (parts.length === 1 && first !== undefined) {
      const summary = tools.summarize(first.intent);
      const preview = tools.previewQuery(first.part?.sql ?? text, first.intent);
      return {
        title: summary.title,
        facts: summary.facts,
        sql: text,
        ...(summary.warning === undefined ? {} : { warning: summary.warning }),
        confirmLabel: summary.confirmLabel,
        ...(summary.destructive === undefined ? {} : { destructive: summary.destructive }),
        ...(preview === undefined
          ? {}
          : {
              preview: async () => {
                const counted = await deps.target.query(preview);
                const value = counted.rows[0]?.row_count;
                const count = typeof value === "number" ? value : 0;
                return count === 1 ? "1 row" : `${formatCount(count)} rows`;
              },
            }),
      };
    }
    const summaries = confirmed.map(({ intent }) => tools.summarize(intent));
    const warnings = summaries.map((summary) => summary.warning).filter((w) => w !== undefined);
    return {
      title: `Run ${String(parts.length)} statements`,
      facts: [
        ["statements", String(parts.length)],
        ["changes", summaries.map((summary) => summary.title).join("; ")],
      ],
      sql: text,
      ...(warnings.length === 0 ? {} : { warning: warnings.join(" ") }),
      confirmLabel: "Run script",
      destructive: summaries.some((summary) => summary.destructive === true),
    };
  }

  /** Runs one statement, returning what it produced. */
  async function runOne(
    part: ScriptStatement,
    intent: StatementIntent,
    signal: AbortSignal,
    all: boolean,
    onPeak: (bytes: number) => void,
  ): Promise<Outcome> {
    if (intent.kind === "select") {
      const capped = !intent.limited && !all;
      const sql = capped ? withAppendedClause(part.sql, `LIMIT ${String(rowCap + 1)}`) : part.sql;
      const result = await deps.target.query(sql, {
        signal,
        onStats: (stats) => {
          onPeak(stats.peakMemoryBytes);
        },
      });
      const overflow = capped && result.rows.length > rowCap;
      const rows = overflow ? { ...result, rows: result.rows.slice(0, rowCap) } : result;
      return {
        summary: describeRows(rows.rows.length, overflow),
        rows,
        rowCount: rows.rows.length,
        capped: overflow,
      };
    }
    const result = await deps.target.execute(part.sql);
    const returned = returnedResult(result);
    return {
      summary: describeExecuteResult(result),
      ...(returned === undefined ? {} : { rows: returned }),
      rowCount: affectedRows(result),
    };
  }

  async function execute(options: { all?: boolean } = {}): Promise<void> {
    const { text, base } = textToRun();
    if (text.length === 0) {
      setNotice("error", "Enter a statement to run.");
      return;
    }
    if (controller !== undefined) return;

    const tools = await statements();
    const parts = splitStatements(text);
    const intents: StatementIntent[] = [];
    for (const part of parts) {
      try {
        intents.push(tools.classifyStatement(part.sql));
      } catch (error) {
        showFailure(error, base + part.from);
        return;
      }
    }
    if (parts.length === 0) {
      setNotice("error", "Enter a statement to run.");
      return;
    }

    const blocked = intents.find((intent) => tools.changesData(intent));
    if (blocked !== undefined && !deps.write) {
      setNotice("blocked", tools.writeBlockedMessage(blocked));
      return;
    }
    const request = confirmationFor(parts, intents, text, tools);
    if (request !== undefined && !(await deps.confirm.ask(request))) {
      setStatus("cancelled");
      return;
    }

    notice.hidden = true;
    stopFollowing();
    setRunning(true);
    showResultTab("rows");
    setStatus("running…");
    const started = performance.now();
    const aborter = new AbortController();
    controller = aborter;
    let peak: number | undefined;
    const outcomes: Outcome[] = [];
    let failed: unknown;
    let catalogChanged = false;
    try {
      for (const [index, part] of parts.entries()) {
        const intent = intents[index];
        if (intent === undefined) break;
        if (aborter.signal.aborted) throw new DOMException("Cancelled", "AbortError");
        try {
          outcomes.push(
            await runOne(part, intent, aborter.signal, options.all === true, (bytes) => {
              peak = Math.max(peak ?? 0, bytes);
            }),
          );
        } catch (error) {
          failed = error;
          if (!isAbort(error)) showFailure(error, base + part.from);
          break;
        }
        if (tools.changesCatalog(intent)) catalogChanged = true;
        if (intent.kind === "select" && index === parts.length - 1) {
          lastSelect = withAppendedClause(part.sql, `LIMIT ${String(rowCap + 1)}`);
        }
      }
    } finally {
      controller = undefined;
      setRunning(false);
    }
    const ms = Math.round(performance.now() - started);

    // The last rows of the run are what the grid shows: a script's final SELECT, a RETURNING
    // clause, a SHOW's own row. Statements before it are listed in the notice.
    const last = [...outcomes].reverse().find((outcome) => outcome.rows !== undefined);
    const lastPart = parts[outcomes.length - 1];
    if (last?.rows !== undefined) {
      renderRows(last.rows, lastPart?.sql, last.capped === true);
    } else if (outcomes.length > 0) {
      grid.setMessage("Statement ran.");
      resultActions.hidden = true;
    }
    if (parts.length > 1 && outcomes.length > 0) {
      setNotice(
        failed === undefined ? "done" : "error",
        outcomes.map((outcome, index) => `${String(index + 1)}. ${outcome.summary}`).join("\n") +
          (failed === undefined
            ? ""
            : `\n${String(outcomes.length + 1)}. ${isAbort(failed) ? "cancelled" : messageOf(failed)}`),
      );
    } else if (outcomes.length === 1 && intents[0]?.kind !== "select" && failed === undefined) {
      setNotice("done", outcomes[0]?.summary ?? "");
    }

    const summary =
      parts.length > 1
        ? `${String(outcomes.length)} of ${String(parts.length)} statements`
        : (outcomes[0]?.summary ?? "");
    const extras: HTMLElement[] = [];
    if (peak !== undefined) extras.push(el("span", { text: `peak ${formatBytes(peak)}` }));
    if (last?.capped === true) {
      const more = button("btn mini", "Load all rows", {
        title: "Run again without the row cap",
      });
      more.addEventListener("click", () => {
        void execute({ all: true });
      });
      extras.push(more);
    }
    if (failed !== undefined) {
      const entryError = isAbort(failed) ? "cancelled" : messageOf(failed);
      record(text, { ms, error: entryError });
      setStatus(isAbort(failed) ? "cancelled" : "failed");
      lastSelect = undefined;
    } else {
      const entry = record(text, {
        ms,
        rowCount: last?.rowCount ?? outcomes.at(-1)?.rowCount ?? 0,
        ...(intents.length === 1 && intents[0]?.kind === "select" ? {} : { outcome: summary }),
      });
      if (last?.rows !== undefined) history.rememberResult(entry.id, last.rows);
      setStatus(`${summary} · ${String(ms)}ms`, ...extras);
      if (intents.at(-1)?.kind === "select") follow();
      else lastSelect = undefined;
    }
    if (catalogChanged) await deps.onCatalogChange();
  }

  run.addEventListener("click", () => {
    void execute();
  });

  async function copy(text: string, what: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
      setStatus(`copied ${what}`);
    } catch (error) {
      setNotice("error", `Could not copy: ${messageOf(error)}`);
    }
  }
  copyCsv.addEventListener("click", () => {
    if (shown === undefined) return;
    void copy(toCsv(shown.columns, shown.rows), `${formatCount(shown.rows.length)} rows as CSV`);
  });
  copyJson.addEventListener("click", () => {
    if (shown === undefined) return;
    void copy(toJson(shown.columns, shown.rows), `${formatCount(shown.rows.length)} rows as JSON`);
  });
  download.addEventListener("click", () => {
    void downloadShown();
  });

  /**
   * Every row of the last query as a file. The grid holds at most the cap, so a capped result is
   * read again through the target's cursor, a batch at a time; a target without one, or an
   * uncapped result, is written from the rows in hand.
   */
  async function downloadShown(): Promise<void> {
    if (shown === undefined) return;
    const fileName = `minnow-query-${new Date().toISOString().slice(0, 19).replaceAll(":", "-")}.csv`;
    if (!shownCapped || shownSql === undefined || !isCursorTarget(deps.target)) {
      downloadText(toCsv(shown.columns, shown.rows), fileName, "text/csv");
      setStatus(`saved ${formatCount(shown.rows.length)} rows`);
      return;
    }
    setStatus("reading every row…");
    try {
      const chunks: string[] = [];
      let count = 0;
      let first = true;
      for await (const batch of deps.target.queryCursor(shownSql)) {
        const text = toCsv(batch.columns, batch.rows);
        chunks.push(first ? text : text.slice(text.indexOf("\n") + 1));
        first = false;
        count += batch.rows.length;
      }
      downloadText(chunks.join(""), fileName, "text/csv");
      setStatus(`saved ${formatCount(count)} rows`);
    } catch (error) {
      setNotice("error", `Could not read the rows: ${messageOf(error)}`);
    }
  }

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
    runQuery: async (sql) => {
      editor.setValue(sql);
      await execute();
    },
    setDark: (dark) => {
      editor.setDark(dark);
    },
    destroy: () => {
      controller?.abort();
      follower?.destroy();
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
