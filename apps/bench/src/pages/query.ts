import "../style.css";
import type {
  DatasetListResult,
  DatasetRecord,
  EngineId,
  EngineQueryRun,
  RunQueryResult,
} from "../protocol.js";
import { engineNames } from "../protocol.js";
import {
  escapeHtml,
  formatDecimal,
  formatDuration,
  formatInteger,
  formatQueryValue,
  required,
} from "../ui/format.js";
import { highlightAll } from "../ui/highlight.js";
import { initShell } from "../ui/shell.js";
import { BenchWorker } from "../ui/worker-client.js";

initShell();

const examples: Record<string, string> = {
  revenue: `SELECT c.segment, COUNT(*) AS orders, SUM(o.total) AS revenue
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE o.status = 'paid'
GROUP BY c.segment
ORDER BY revenue DESC
LIMIT 20`,
  orders: `SELECT o.status, COUNT(*) AS orders, SUM(o.total) AS gross_total
FROM orders o
GROUP BY o.status
ORDER BY gross_total DESC
LIMIT 10`,
  tax: `SELECT j.name, COUNT(*) AS tax_lines, SUM(t.tax_amount) AS tax_collected
FROM order_taxes t
JOIN tax_rates r ON r.tax_rate_id = t.tax_rate_id
JOIN tax_jurisdictions j ON j.jurisdiction_id = r.jurisdiction_id
GROUP BY j.name
ORDER BY tax_collected DESC
LIMIT 25`,
  inventory: `SELECT s.name, COUNT(*) AS movements, SUM(m.quantity_delta) AS net_units
FROM suppliers s
JOIN inventory_movements m ON m.supplier_id = s.supplier_id
GROUP BY s.name
ORDER BY movements DESC
LIMIT 25`,
  returns: `SELECT c.segment, COUNT(*) AS returns
FROM returns r
JOIN orders o ON o.order_id = r.order_id
JOIN customers c ON c.customer_id = o.customer_id
GROUP BY c.segment
ORDER BY returns DESC
LIMIT 10`,
  events: `SELECT e.event_type, COUNT(*) AS events
FROM order_events e
GROUP BY e.event_type
ORDER BY events DESC
LIMIT 10`,
};

const worker = new BenchWorker();
const datasetSelect = required<HTMLSelectElement>("#dataset-select");
const engineChecks = required<HTMLElement>("#engine-checks");
const examplesBox = required<HTMLElement>("#examples");
const sqlInput = required<HTMLTextAreaElement>("#sql");
const runButton = required<HTMLButtonElement>("#run");
const status = required<HTMLElement>("#status");
const results = required<HTMLElement>("#results");
const agreement = required<HTMLElement>("#agreement");
const engineMetrics = required<HTMLElement>("#engine-metrics");
const planDetails = required<HTMLElement>("#plan-details");
const planText = required<HTMLElement>("#plan");
const resultNote = required<HTMLElement>("#result-note");
const resultHead = required<HTMLElement>("#result-head");
const resultRows = required<HTMLElement>("#result-rows");

let datasets: DatasetRecord[] = [];
let running = false;

sqlInput.value = examples.revenue ?? "";
examplesBox.innerHTML = Object.keys(examples)
  .map((key) => `<button data-example="${key}">${key}</button>`)
  .join("");
for (const button of Array.from(
  examplesBox.querySelectorAll<HTMLButtonElement>("[data-example]"),
)) {
  button.addEventListener("click", () => {
    sqlInput.value = examples[button.dataset.example ?? ""] ?? sqlInput.value;
  });
}

datasetSelect.addEventListener("change", renderEngineChecks);

void worker
  .request<DatasetListResult>("datasetList", {})
  .then((list) => {
    datasets = list.datasets;
    datasetSelect.innerHTML =
      datasets.length === 0
        ? `<option value="">No datasets — generate one first</option>`
        : datasets
            .map(
              (record) =>
                `<option value="${record.id}">${formatDecimal(record.scale)}× · ${formatInteger(record.totalRows)} rows · ${new Date(record.createdAt).toLocaleString()}</option>`,
            )
            .join("");
    renderEngineChecks();
    if (datasets.length === 0) {
      setStatus(`No persisted datasets. Create one on the Datasets page first.`, "");
      runButton.disabled = true;
    }
  })
  .catch((error: unknown) => {
    setStatus(error instanceof Error ? error.message : String(error), "fail");
  });

function selectedDataset(): DatasetRecord | undefined {
  return datasets.find((record) => record.id === datasetSelect.value);
}

function renderEngineChecks(): void {
  const record = selectedDataset();
  engineChecks.innerHTML = (Object.keys(engineNames) as EngineId[])
    .map((engine) => {
      const ready = record?.engines[engine]?.status === "ready";
      return `<label><input type="checkbox" value="${engine}" ${ready ? "checked" : "disabled"} /> ${engineNames[engine]}${ready ? "" : " (not materialized)"}</label>`;
    })
    .join("");
}

runButton.addEventListener("click", () => {
  if (running) return;
  const record = selectedDataset();
  if (record === undefined) {
    setStatus("Select a dataset first.", "fail");
    return;
  }
  const engines = Array.from(engineChecks.querySelectorAll<HTMLInputElement>("input:checked")).map(
    (input) => input.value as EngineId,
  );
  if (engines.length === 0) {
    setStatus("Select at least one engine.", "fail");
    return;
  }
  running = true;
  runButton.disabled = true;
  setStatus("Running — one warm-up and seven measured iterations per engine…", "");
  worker
    .request<RunQueryResult>("runQuery", { datasetId: record.id, engines, sql: sqlInput.value })
    .then(renderResult)
    .catch((error: unknown) => {
      setStatus(error instanceof Error ? error.message : String(error), "fail");
    })
    .finally(() => {
      running = false;
      runButton.disabled = false;
    });
});

function renderResult(result: RunQueryResult): void {
  results.hidden = false;
  const succeeded = result.runs.filter((run) => run.ok);
  setStatus(
    succeeded.length === 0
      ? "Every engine failed — see the table below."
      : `${String(succeeded.length)} of ${String(result.runs.length)} engines returned rows.`,
    succeeded.length === 0 ? "fail" : "ok",
  );
  agreement.innerHTML =
    result.checksumAgreement === null
      ? `<span class="badge muted">checksum comparison needs two successful engines</span>`
      : result.checksumAgreement === "match"
        ? `<span class="badge ok">result checksums match across engines</span>`
        : `<span class="badge fail">result checksums differ across engines</span>`;
  engineMetrics.innerHTML = result.runs
    .map(
      (run) => `<tr>
        <td>${escapeHtml(engineNames[run.engine])}</td>
        <td class="num">${run.ok ? formatDuration(run.prepareMs) : "—"}</td>
        <td class="num">${run.ok ? formatDuration(run.medianMs) : "—"}</td>
        <td class="num">${run.ok ? formatDuration(run.p95Ms) : "—"}</td>
        <td class="num">${run.ok ? formatInteger(run.rowCount) : "—"}</td>
        <td class="num">${run.ok ? String(run.checksum) : "—"}</td>
        <td>${
          run.ok
            ? `<span class="badge ok">ok</span>`
            : `<span class="badge fail" title="${escapeHtml(run.error ?? "")}">failed</span> <small class="note">${escapeHtml(run.error ?? "")}</small>`
        }</td>
      </tr>`,
    )
    .join("");
  const withPlan = result.runs.find((run) => run.plan !== undefined);
  planDetails.hidden = withPlan === undefined;
  planText.textContent = withPlan?.plan ?? "";
  const preview =
    result.runs.find((run) => run.engine === "browserdatabase" && run.ok) ??
    result.runs.find((run) => run.ok);
  renderPreview(preview);
  void highlightAll(results);
}

function renderPreview(run: EngineQueryRun | undefined): void {
  if (run === undefined) {
    resultNote.textContent = "No rows to show.";
    resultHead.innerHTML = "";
    resultRows.innerHTML = "";
    return;
  }
  resultNote.textContent = run.truncated
    ? `${engineNames[run.engine]} result — showing the first ${String(run.previewRows.length)} of ${formatInteger(run.rowCount)} rows. Metrics cover the full result.`
    : `${engineNames[run.engine]} result — all ${formatInteger(run.rowCount)} rows.`;
  resultHead.innerHTML = `<tr>${run.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>`;
  resultRows.innerHTML = run.previewRows
    .map(
      (row) =>
        `<tr>${run.columns.map((column) => `<td>${escapeHtml(formatQueryValue(row[column]))}</td>`).join("")}</tr>`,
    )
    .join("");
}

function setStatus(message: string, kind: "ok" | "fail" | ""): void {
  status.textContent = message;
  status.className = `status ${kind}`;
}
