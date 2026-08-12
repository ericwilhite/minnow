import {
  SqlCompileError,
  type ExecuteResult,
  type QueryResult,
  type QueryValue,
} from "@minnowdb/core";
import type { ConfirmLayer } from "./confirm.js";
import { button, clear, el, icon, icons } from "./dom.js";
import { changesData, classifyStatement, summarize, writeBlockedMessage } from "./statements.js";
import type { DevtoolsTarget } from "./target.js";

export interface ConsoleDeps {
  target: DevtoolsTarget;
  confirm: ConfirmLayer;
  /** Whether statements that change data are allowed to run at all. */
  write: boolean;
  initialQuery: string;
}

export interface ConsoleView {
  node: HTMLElement;
  focus(): void;
}

function formatValue(value: QueryValue): { text: string; className: string } {
  if (value === null) return { text: "NULL", className: "null" };
  if (typeof value === "number") return { text: String(value), className: "number" };
  if (value instanceof Date) return { text: value.toISOString(), className: "" };
  return { text: String(value), className: "" };
}

function describeExecuteResult(result: ExecuteResult): string {
  if (result.kind === "rows") return `${String(result.result.rows.length)} rows`;
  const rows = result.rowCount === 1 ? "1 row" : `${String(result.rowCount)} rows`;
  return `${result.kind}: ${rows} in ${result.table}`;
}

/** The SQL console: type a statement, run it, read the rows. */
export function createConsole(deps: ConsoleDeps): ConsoleView {
  const editor = el("textarea", {
    class: "editor",
    attrs: { spellcheck: "false", "aria-label": "SQL", placeholder: "SELECT * FROM …" },
  });
  editor.value = deps.initialQuery;

  const run = button("btn primary", "Run");
  run.prepend(icon(icons.play));
  const notice = el("div", { class: "notice" });
  notice.hidden = true;
  const results = el("div", { class: "results" }, [
    el("div", { class: "empty", text: "Run a query to see rows here." }),
  ]);
  const status = el("div", { class: "statusbar" }, [el("span", { text: "ready" })]);

  const node = el("div", { class: "body" }, [
    el("div", { class: "toolbar" }, [
      run,
      el("span", { class: "hint", text: "Ctrl/Cmd + Enter" }),
      el("span", { class: "spacer" }),
    ]),
    editor,
    notice,
    results,
    status,
  ]);

  function setNotice(kind: "error" | "blocked" | "done", message: string): void {
    notice.className = `notice ${kind}`;
    notice.replaceChildren(icon(icons.warning), el("span", { text: message }));
    notice.hidden = false;
  }

  function clearNotice(): void {
    notice.hidden = true;
  }

  function setStatus(text: string): void {
    status.replaceChildren(el("span", { text }));
  }

  function renderRows(result: QueryResult): void {
    if (result.rows.length === 0) {
      results.replaceChildren(el("div", { class: "empty", text: "No rows." }));
      return;
    }
    const head = el(
      "tr",
      {},
      result.columns.map((column) => el("th", { text: column })),
    );
    const body = result.rows.map((row) =>
      el(
        "tr",
        {},
        result.columns.map((column) => {
          const { text, className } = formatValue(row[column] ?? null);
          return el("td", { class: className, text, title: text });
        }),
      ),
    );
    clear(results);
    results.append(el("table", {}, [el("thead", {}, [head]), el("tbody", {}, body)]));
  }

  /** Points the caret at the failure, so a located compile error reads as one in the editor. */
  function selectFailure(error: SqlCompileError): void {
    editor.focus();
    editor.setSelectionRange(error.offset, error.offset + Math.max(error.length, 1));
  }

  async function execute(): Promise<void> {
    const sql = editor.value.trim();
    if (sql.length === 0) {
      setNotice("error", "Enter a statement to run.");
      return;
    }

    let intent;
    try {
      intent = classifyStatement(sql);
    } catch (error) {
      setNotice("error", error instanceof Error ? error.message : String(error));
      if (error instanceof SqlCompileError) selectFailure(error);
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

    clearNotice();
    run.disabled = true;
    setStatus("running…");
    const started = performance.now();
    try {
      if (intent.kind === "select") {
        const result = await deps.target.query(sql);
        renderRows(result);
        const elapsed = Math.round(performance.now() - started);
        setStatus(`${String(result.rows.length)} rows · ${String(elapsed)}ms`);
      } else {
        const result = await deps.target.execute(sql);
        const elapsed = Math.round(performance.now() - started);
        results.replaceChildren(el("div", { class: "empty", text: "Statement ran." }));
        setNotice("done", describeExecuteResult(result));
        setStatus(`${describeExecuteResult(result)} · ${String(elapsed)}ms`);
      }
    } catch (error) {
      setNotice("error", error instanceof Error ? error.message : String(error));
      if (error instanceof SqlCompileError) selectFailure(error);
      setStatus("failed");
    } finally {
      run.disabled = false;
    }
  }

  run.addEventListener("click", () => {
    void execute();
  });
  editor.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      void execute();
    }
  });

  return {
    node,
    focus: () => {
      editor.focus();
    },
  };
}
