import { protocolVersion, type WorkerResponse } from "@browserdatabase/worker-protocol";
import {
  getScenario,
  relationalRowCount,
  relationalTotalRows,
  scenarios,
  type AdHocQueryResult,
  type BenchmarkConfig,
  type BenchmarkFocus,
  type BenchmarkProgress,
  type BenchmarkResult,
  type DurabilityMode,
} from "./benchmark.js";
import type { EngineComparisonResult } from "./engine-comparison.js";
import "./style.css";

const form = required<HTMLFormElement>("#controls");
const scaleSelect = required<HTMLSelectElement>("#scale");
const scaleDescription = required<HTMLElement>("#scale-description");
const datasetPlanSummary = required<HTMLElement>("#dataset-plan-summary");
const datasetPlanTables = required<HTMLElement>("#dataset-plan-tables");
const focusSelect = required<HTMLSelectElement>("#focus");
const compressionSelect = required<HTMLSelectElement>("#compression");
const blockSizeSelect = required<HTMLSelectElement>("#block-size");
const durabilitySelect = required<HTMLSelectElement>("#durability");
const repetitionsSelect = required<HTMLSelectElement>("#repetitions");
const runButton = required<HTMLButtonElement>("#run");
const compareButton = required<HTMLButtonElement>("#compare");
const compareEnginesButton = required<HTMLButtonElement>("#compare-engines");
const cancelButton = required<HTMLButtonElement>("#cancel");
const exportButton = required<HTMLButtonElement>("#export");
const progressLabel = required<HTMLElement>("#progress-label");
const progressValue = required<HTMLElement>("#progress-value");
const progressBar = required<HTMLElement>("#progress-bar");
const runSummaryCopy = required<HTMLElement>("#run-summary-copy");
const resultContent = required<HTMLElement>("#result-content");
const emptyState = required<HTMLElement>("#empty-state");
const verificationBadge = required<HTMLElement>("#verification-badge");
const metricCards = required<HTMLElement>("#metric-cards");
const timingChart = required<HTMLElement>("#timing-chart");
const primaryTiming = required<HTMLElement>("#primary-timing");
const storageMetrics = required<HTMLElement>("#storage-metrics");
const transactionTotal = required<HTMLElement>("#transaction-total");
const transactionSummary = required<HTMLElement>("#transaction-summary");
const transactionChecks = required<HTMLElement>("#transaction-checks");
const libraryTotal = required<HTMLElement>("#library-total");
const librarySummary = required<HTMLElement>("#library-summary");
const libraryMetrics = required<HTMLElement>("#library-metrics");
const libraryTimings = required<HTMLElement>("#library-timings");
const libraryChecks = required<HTMLElement>("#library-checks");
const libraryTableRows = required<HTMLElement>("#library-table-rows");
const queryTotal = required<HTMLElement>("#query-total");
const queryNote = required<HTMLElement>("#query-note");
const querySummary = required<HTMLElement>("#query-summary");
const queryDataset = required<HTMLElement>("#query-dataset");
const queryChecks = required<HTMLElement>("#query-checks");
const queryResults = required<HTMLElement>("#query-results");
const entityRows = required<HTMLElement>("#entity-rows");
const blockRows = required<HTMLElement>("#block-rows");
const blockCount = required<HTMLElement>("#block-count");
const historyEmpty = required<HTMLElement>("#history-empty");
const historyTable = required<HTMLElement>("#history-table");
const historyRows = required<HTMLElement>("#history-rows");
const comparisonSummary = required<HTMLElement>("#comparison-summary");
const adHocDatasetState = required<HTMLElement>("#ad-hoc-dataset-state");
const adHocSql = required<HTMLTextAreaElement>("#ad-hoc-sql");
const runAdHocButton = required<HTMLButtonElement>("#run-ad-hoc");
const adHocStatus = required<HTMLElement>("#ad-hoc-status");
const adHocResult = required<HTMLElement>("#ad-hoc-result");
const adHocMetrics = required<HTMLElement>("#ad-hoc-metrics");
const adHocResultHead = required<HTMLElement>("#ad-hoc-result-head");
const adHocResultRows = required<HTMLElement>("#ad-hoc-result-rows");
const adHocResultNote = required<HTMLElement>("#ad-hoc-result-note");
const adHocHistoryEmpty = required<HTMLElement>("#ad-hoc-history-empty");
const adHocHistoryTable = required<HTMLElement>("#ad-hoc-history");
const adHocHistoryRows = required<HTMLElement>("#ad-hoc-history-rows");
const engineComparisonState = required<HTMLElement>("#engine-comparison-state");
const engineComparisonNote = required<HTMLElement>("#engine-comparison-note");
const engineComparisonResults = required<HTMLElement>("#engine-comparison-results");

const worker = new Worker(new URL("./worker.ts", import.meta.url), { type: "module" });
const history: BenchmarkResult[] = [];
const adHocHistory: AdHocQueryResult[] = [];
let activeRequestId: string | undefined;
let activeQueryRequestId: string | undefined;
let activeEngineComparisonRequestId: string | undefined;
let pendingConfigs: BenchmarkConfig[] = [];
let batchTotal = 0;
let batchCompleted = 0;

const adHocExamples: Record<string, string> = {
  revenue: `SELECT c.segment, COUNT(*) AS orders, SUM(o.total) AS revenue
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
WHERE o.status = 'paid'
GROUP BY c.segment
ORDER BY revenue DESC
LIMIT 20`,
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
};

updateConfigurationSummary();

worker.addEventListener("message", (event: MessageEvent<WorkerResponse>) => {
  const message = event.data;
  if (message.kind === "progress") {
    if (
      message.requestId === activeRequestId ||
      message.requestId === activeEngineComparisonRequestId
    )
      updateProgress(message.progress as BenchmarkProgress);
    return;
  }
  if (message.kind === "failure") {
    if (message.requestId === activeQueryRequestId) {
      activeQueryRequestId = undefined;
      runAdHocButton.disabled = false;
      adHocStatus.textContent = `${message.error.name}: ${message.error.message}`;
      adHocStatus.className = "query-error";
      return;
    }
    if (message.requestId === activeEngineComparisonRequestId) {
      activeEngineComparisonRequestId = undefined;
      engineComparisonState.textContent = "Failed";
      engineComparisonNote.textContent = `${message.error.name}: ${message.error.message}`;
      setRunning(false);
      return;
    }
    if (message.requestId === activeRequestId) finishWithError(message.error.message);
    return;
  }
  if (message.requestId === activeQueryRequestId && isAdHocQueryResult(message.result)) {
    activeQueryRequestId = undefined;
    runAdHocButton.disabled = false;
    adHocHistory.unshift(message.result);
    renderAdHocResult(message.result);
    return;
  }
  if (
    message.requestId === activeEngineComparisonRequestId &&
    isEngineComparisonResult(message.result)
  ) {
    activeEngineComparisonRequestId = undefined;
    renderEngineComparison(message.result);
    setRunning(false);
    progressLabel.textContent = "Database engine comparison complete";
    progressValue.textContent = "100%";
    progressBar.style.width = "100%";
    return;
  }
  if (message.requestId !== activeRequestId || !isBenchmarkResult(message.result)) return;
  history.unshift(message.result);
  renderResult(message.result);
  renderHistory();
  batchCompleted += 1;
  if (pendingConfigs.length > 0) {
    startNextRun();
  } else {
    finishRun();
  }
});

worker.addEventListener("error", (event) => {
  finishWithError(`Worker error: ${event.message}`);
});

form.addEventListener("input", updateConfigurationSummary);
form.addEventListener("change", updateConfigurationSummary);

for (const button of Array.from(
  document.querySelectorAll<HTMLButtonElement>("[data-query-example]"),
)) {
  button.addEventListener("click", () => {
    adHocSql.value = adHocExamples[button.dataset.queryExample ?? ""] ?? adHocSql.value;
  });
}

runAdHocButton.addEventListener("click", () => {
  if (activeQueryRequestId !== undefined) return;
  activeQueryRequestId = crypto.randomUUID();
  runAdHocButton.disabled = true;
  adHocStatus.className = "";
  adHocStatus.textContent = "Running seven measured iterations…";
  worker.postMessage({
    version: protocolVersion,
    requestId: activeQueryRequestId,
    operation: "adHocQuery",
    payload: { sql: adHocSql.value },
  });
});

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const config = readConfig();
  const repetitions = Number(repetitionsSelect.value);
  beginBatch(Array.from({ length: repetitions }, () => ({ ...config })));
});

compareButton.addEventListener("click", () => {
  const base = readConfig();
  const blockSizes = [262_144, 524_288, 1_048_576, 2_097_152, 4_194_304];
  const compressions: Array<BenchmarkConfig["compression"]> = ["raw", "rle", "gzip"];
  beginBatch(
    compressions.flatMap((compression) =>
      blockSizes.map((targetBlockBytes) => ({ ...base, compression, targetBlockBytes })),
    ),
  );
});

compareEnginesButton.addEventListener("click", () => {
  if (activeEngineComparisonRequestId !== undefined || activeRequestId !== undefined) return;
  const config = readConfig();
  activeEngineComparisonRequestId = crypto.randomUUID();
  batchTotal = 1;
  batchCompleted = 0;
  setRunning(true);
  cancelButton.hidden = true;
  engineComparisonState.textContent = "Running…";
  engineComparisonNote.textContent = `Loading ${formatInteger(relationalTotalRows(config.scale))} rows into four engines. Large PGlite and SQLite runs can take several minutes.`;
  updateProgress({
    phase: "preparing",
    completed: 0,
    total: 1,
    message: "Starting engine comparison",
  });
  worker.postMessage({
    version: protocolVersion,
    requestId: activeEngineComparisonRequestId,
    operation: "compareEngines",
    payload: {
      scale: config.scale,
      compression: config.compression,
      targetBlockBytes: config.targetBlockBytes,
      durability: config.durability,
      batchRows: 50_000,
    },
  });
});

cancelButton.addEventListener("click", () => {
  if (activeRequestId === undefined) return;
  pendingConfigs = [];
  progressLabel.textContent = "Stopping after the current save or load step…";
  worker.postMessage({
    version: protocolVersion,
    requestId: crypto.randomUUID(),
    operation: "cancelBenchmark",
    payload: { requestId: activeRequestId },
  });
});

exportButton.addEventListener("click", () => {
  const blob = new Blob([JSON.stringify(history, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `browserdatabase-benchmark-${new Date().toISOString().replaceAll(":", "-")}.json`;
  link.click();
  URL.revokeObjectURL(url);
});

function beginBatch(configs: BenchmarkConfig[]): void {
  pendingConfigs = [...configs];
  batchTotal = configs.length;
  batchCompleted = 0;
  setRunning(true);
  runAdHocButton.disabled = true;
  adHocDatasetState.textContent = "Generating a new dataset…";
  adHocStatus.textContent = "Available when the current run completes.";
  startNextRun();
}

function startNextRun(): void {
  const config = pendingConfigs.shift();
  if (config === undefined) {
    finishRun();
    return;
  }
  activeRequestId = crypto.randomUUID();
  updateProgress({
    phase: "preparing",
    completed: 0,
    total: 1,
    message: "Starting",
  });
  worker.postMessage({
    version: protocolVersion,
    requestId: activeRequestId,
    operation: "benchmark",
    payload: config,
  });
}

function readConfig(): BenchmarkConfig {
  const scale = Number(scaleSelect.value);
  return {
    scenario: "commerce",
    focus: focusSelect.value as BenchmarkFocus,
    scale,
    rows: relationalRowCount("orders", scale),
    compression: compressionSelect.value as BenchmarkConfig["compression"],
    targetBlockBytes: Number(blockSizeSelect.value),
    durability: durabilitySelect.value as DurabilityMode,
  };
}

function updateProgress(progress: BenchmarkProgress): void {
  const runProgress = progress.completed / Math.max(1, progress.total);
  const percentage = Math.round(((batchCompleted + runProgress) / Math.max(1, batchTotal)) * 100);
  const prefix =
    batchTotal > 1 ? `Test ${String(batchCompleted + 1)} of ${String(batchTotal)} · ` : "";
  progressLabel.textContent = `${prefix}${progress.message}`;
  progressValue.textContent = `${String(percentage)}%`;
  progressBar.style.width = `${String(percentage)}%`;
}

function renderResult(result: BenchmarkResult): void {
  emptyState.hidden = true;
  resultContent.hidden = false;
  verificationBadge.className = `verification-badge ${result.verified ? "verified" : "failed"}`;
  verificationBadge.textContent = result.verified
    ? "✓ Storage, transactions, writes, and reference queries pass"
    : "× A storage, transaction, write, or reference-query check failed";
  runAdHocButton.disabled = false;
  adHocDatasetState.textContent = `${formatDecimal(result.config.scale)}× · ${formatInteger(result.referenceQueries.totalRows)} rows loaded`;
  adHocStatus.textContent = "Ready. Seven timed iterations per query.";

  const primary = primaryMetric(result);
  const primarySeconds = Math.max(primary.value / 1_000, 0.000_001);
  metricCards.innerHTML = [
    metric("Main time", formatDuration(primary.value), primary.label),
    metric(
      "Speed",
      `${formatDecimal(result.totals.encodedBytes / 1_048_576 / primarySeconds)} MB/s`,
      `${formatInteger(result.totals.rows / primarySeconds)} rows/s`,
    ),
    metric(
      "Saved",
      formatBytes(result.totals.storedBytes),
      `${formatBytes(result.totals.encodedBytes)} before compression`,
    ),
    metric(
      "Size change",
      sizeChange(result.totals.encodedBytes, result.totals.storedBytes),
      compressionLabel(result.config.compression),
    ),
    metric(
      "Blocks",
      formatInteger(result.totals.blocks),
      `${String(result.totals.entities)} data groups · ${String(result.totals.columns)} columns`,
    ),
    metric("Rows", formatInteger(result.totals.rows), "across all data groups"),
  ].join("");

  const timingEntries = Object.entries(result.timingsMs).filter(
    ([name]) => name !== "total",
  ) as Array<[keyof BenchmarkResult["timingsMs"], number]>;
  const maximum = Math.max(...timingEntries.map(([, value]) => value), 0.01);
  timingChart.innerHTML = timingEntries
    .map(
      ([name, value]) => `
        <div class="timing-row">
          <span>${escapeHtml(timingLabel(name))}</span>
          <div class="timing-track"><i style="width:${String(Math.max(1, (value / maximum) * 100))}%"></i></div>
          <strong>${formatDuration(value)}</strong>
        </div>`,
    )
    .join("");
  primaryTiming.textContent = `${primary.label} · ${formatDuration(primary.value)}`;

  storageMetrics.innerHTML = [
    storageStat("Before test", formatOptionalBytes(result.storage.usageBefore)),
    storageStat("During test", formatOptionalBytes(result.storage.usageAfter)),
    storageStat("Change", formatSignedBytes(result.storage.measuredDelta)),
    storageStat("Site limit", formatOptionalBytes(result.storage.quota)),
    storageStat("Numbers checked", formatInteger(result.kernel.numericValues)),
    storageStat("Sum check", formatDecimal(result.kernel.sum)),
  ].join("");

  transactionTotal.textContent = `${formatDuration(result.transactions.probeDurationMs)} probe`;
  transactionSummary.textContent = `The transaction journal persisted ${formatInteger(result.transactions.journaledBlocks)} block IDs and atomically published manifest version ${String(result.transactions.committedVersion)}. The committed record status is “${result.transactions.persistedStatus}”.`;
  transactionChecks.innerHTML = result.transactions.checks.map(checkCard).join("");

  libraryTotal.textContent = `${formatDuration(result.library.timingsMs.insertBatches)} insert · ${formatDuration(result.library.timingsMs.upsertBatch)} upsert · ${formatDuration(result.library.timingsMs.updateBatch)} update · ${formatDuration(result.library.timingsMs.deleteBatch)} delete`;
  librarySummary.textContent = `${formatInteger(result.library.totals.rows)} rows were inserted through BrowserDatabase across ${String(result.library.totals.tables)} persistent tables. The workload used ${formatInteger(result.library.rowsPerBlock)} rows per block and batches capped at ${formatInteger(result.library.maxBatchRows)} rows. A separate ${formatInteger(result.library.upsert.sampleRows)}-key probe uses the persistent key lookup, then measures immutable upserts, narrow partial updates, projected reads, and key deletes at versions ${String(result.library.upsert.baseVersion)} → ${String(result.library.mutations.deleteVersion)}. An isolated append-only table is compacted from two visible segments to one while its old snapshot and source blocks are verified.`;
  libraryMetrics.innerHTML = [
    metric("Tables", String(result.library.totals.tables), "created persistently"),
    metric("Rows", formatInteger(result.library.totals.rows), "accepted by insertBatch"),
    metric(
      "Upsert",
      `${formatInteger(result.library.upsert.insertedRows)} / ${formatInteger(result.library.upsert.updatedRows)}`,
      "inserted / updated rows",
    ),
    metric(
      "Partial update",
      formatInteger(result.library.mutations.updatedRows),
      `${formatBytes(result.library.mutations.updateStoredBytes)} key + changed columns`,
    ),
    metric(
      "Delete",
      formatInteger(result.library.mutations.deletedRows),
      `${formatBytes(result.library.mutations.deleteStoredBytes)} key markers`,
    ),
    metric(
      "Projection",
      formatInteger(result.library.mutations.projectedRows),
      "rows · 2 of 5 columns",
    ),
  ].join("");
  const libraryTimingEntries = Object.entries(result.library.timingsMs).filter(
    ([name]) => name !== "total",
  ) as Array<[keyof BenchmarkResult["library"]["timingsMs"], number]>;
  const libraryTimingMaximum = Math.max(...libraryTimingEntries.map(([, value]) => value), 0.01);
  libraryTimings.innerHTML = libraryTimingEntries
    .map(
      ([name, value]) => `<div class="timing-row">
        <span>${escapeHtml(libraryTimingLabel(name))}</span>
        <div class="timing-track"><i style="width:${String(Math.max(1, (value / libraryTimingMaximum) * 100))}%"></i></div>
        <strong>${formatDuration(value)}</strong>
      </div>`,
    )
    .join("");
  libraryChecks.innerHTML = result.library.checks.map(checkCard).join("");
  libraryTableRows.innerHTML = result.library.tables
    .map(
      (table) => `<tr>
        <td><strong>${escapeHtml(table.name)}</strong></td>
        <td>${formatInteger(table.rows)}</td>
        <td>${formatInteger(table.segments)}</td>
        <td>${formatInteger(table.blocks)}</td>
        <td>${formatBytes(table.storedBytes)}</td>
      </tr>`,
    )
    .join("");

  queryTotal.textContent = `${formatDuration(result.referenceQueries.totalMs)} load + timed measurement wall time`;
  queryNote.textContent = result.referenceQueries.note;
  querySummary.textContent = `${formatInteger(result.referenceQueries.totalRows)} rows across ${String(result.referenceQueries.tables.length)} related tables were created in ${formatDuration(result.referenceQueries.setupMs)}, loaded once in ${formatDuration(result.referenceQueries.loadMs)}, and indexed in ${formatDuration(result.referenceQueries.indexMs)}. Each of the ${String(result.referenceQueries.queries.length)} queries reports a ${String(result.referenceQueries.sampleCount)}-sample total, median, and p95 per execution; fast operations are batched to exceed browser timer resolution. The ${formatDecimal(result.config.scale)}× scale produces ${formatInteger(result.referenceQueries.orderRows)} orders and grows every supporting table from the same multiplier.`;
  queryDataset.innerHTML = result.referenceQueries.tables
    .map(
      (table) => `<article class="dataset-card">
        <div><strong>${escapeHtml(table.name)}</strong><span>${formatInteger(table.rows)} rows · ${formatDuration(table.loadMs)}</span></div>
        <small>${escapeHtml(table.relationship)}</small>
      </article>`,
    )
    .join("");
  queryChecks.innerHTML = result.referenceQueries.integrityChecks.map(checkCard).join("");
  queryResults.innerHTML = result.referenceQueries.queries.map(queryCard).join("");

  entityRows.innerHTML = result.entities
    .map(
      (entity) => `<tr>
        <td><strong>${escapeHtml(entity.name)}</strong></td>
        <td>${formatInteger(entity.rows)}</td>
        <td>${String(entity.columns)}</td>
        <td>${String(entity.blocks)}</td>
        <td>${formatBytes(entity.encodedBytes)}</td>
        <td>${formatBytes(entity.storedBytes)}</td>
        <td>${sizeChange(entity.encodedBytes, entity.storedBytes)}</td>
      </tr>`,
    )
    .join("");

  blockCount.textContent = `${String(result.blocks.length)} blocks`;
  blockRows.innerHTML = result.blocks
    .map(
      (block) => `<tr>
        <td><strong>${escapeHtml(`${block.entity}.${block.column}`)}</strong><small>${escapeHtml(block.id.split("/").at(-1) ?? "")}</small></td>
        <td><span class="type-pill">${escapeHtml(block.type)}</span></td>
        <td>${formatInteger(block.rows)}</td>
        <td>${formatBytes(block.encodedBytes)}</td>
        <td>${formatBytes(block.storedBytes)}</td>
        <td>${sizeChange(block.encodedBytes, block.storedBytes)}</td>
        <td><span class="check ${block.verified ? "pass" : "fail"}">${block.verified ? "match" : "failed"}</span></td>
      </tr>`,
    )
    .join("");
}

function renderAdHocResult(result: AdHocQueryResult): void {
  adHocResult.hidden = false;
  adHocStatus.className = "query-success";
  adHocStatus.textContent = `${formatInteger(result.rowCount)} rows · ${result.tables.join(" → ")}`;
  adHocMetrics.innerHTML = [
    metric("Median", formatDuration(result.metrics.executeMedianMs), "execution · 7 iterations"),
    metric("p95", formatDuration(result.metrics.executeP95Ms), "execution tail"),
    metric("Parse", formatDuration(result.metrics.parseMs), "validation + plan"),
    metric("Source rows", formatInteger(result.metrics.sourceRows), "rows read by table sources"),
    metric("Joined rows", formatInteger(result.metrics.joinedRows), "before WHERE filtering"),
    metric("Result rows", formatInteger(result.rowCount), "before preview limit"),
  ].join("");
  adHocResultHead.innerHTML = `<tr>${result.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>`;
  adHocResultRows.innerHTML = result.previewRows
    .map(
      (row) =>
        `<tr>${result.columns.map((column) => `<td>${escapeHtml(formatQueryValue(row[column]))}</td>`).join("")}</tr>`,
    )
    .join("");
  adHocResultNote.textContent = result.truncated
    ? `Showing the first ${String(result.previewRows.length)} of ${formatInteger(result.rowCount)} rows. Metrics include the full result.`
    : `Showing all ${formatInteger(result.rowCount)} rows.`;
  adHocHistoryEmpty.hidden = false;
  adHocHistoryTable.hidden = adHocHistory.length === 0;
  adHocHistoryEmpty.hidden = adHocHistory.length > 0;
  adHocHistoryRows.innerHTML = adHocHistory
    .map(
      (query, index) => `<tr>
        <td><strong>#${String(adHocHistory.length - index)}</strong><small>${escapeHtml(compactSql(query.sql))}</small></td>
        <td>${escapeHtml(query.tables.join(", "))}</td>
        <td>${formatInteger(query.rowCount)}</td>
        <td>${formatDuration(query.metrics.executeMedianMs)}</td>
        <td>${formatDuration(query.metrics.executeP95Ms)}</td>
        <td>${formatDuration(query.metrics.parseMs)}</td>
      </tr>`,
    )
    .join("");
}

function formatQueryValue(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number")
    return Number.isInteger(value) ? formatInteger(value) : formatDecimal(value);
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
}

function compactSql(sql: string): string {
  const compact = sql.replace(/\s+/g, " ").trim();
  return compact.length > 96 ? `${compact.slice(0, 93)}…` : compact;
}

function renderHistory(): void {
  historyEmpty.hidden = history.length > 0;
  historyTable.hidden = history.length === 0;
  exportButton.disabled = history.length === 0;
  comparisonSummary.hidden = history.length < 2;
  if (history.length >= 2) {
    const fastest = history.reduce((best, result) =>
      primaryMetric(result).value < primaryMetric(best).value ? result : best,
    );
    const smallest = history.reduce((best, result) =>
      result.totals.storedBytes < best.totals.storedBytes ? result : best,
    );
    const fastestLibrary = history.reduce((best, result) =>
      result.library.timingsMs.insertBatches < best.library.timingsMs.insertBatches ? result : best,
    );
    const fastestUpsert = history.reduce((best, result) =>
      result.library.timingsMs.upsertBatch < best.library.timingsMs.upsertBatch ? result : best,
    );
    const fastestUpdate = history.reduce((best, result) =>
      result.library.timingsMs.updateBatch < best.library.timingsMs.updateBatch ? result : best,
    );
    const fastestDelete = history.reduce((best, result) =>
      result.library.timingsMs.deleteBatch < best.library.timingsMs.deleteBatch ? result : best,
    );
    const fastestQueries = history.reduce((best, result) =>
      result.referenceQueries.totalMs < best.referenceQueries.totalMs ? result : best,
    );
    comparisonSummary.innerHTML = [
      comparisonCard(
        "Fastest",
        `${compressionLabel(fastest.config.compression)} · ${formatBytes(fastest.config.targetBlockBytes)}`,
        formatDuration(primaryMetric(fastest).value),
      ),
      comparisonCard(
        "Smallest saved data",
        `${compressionLabel(smallest.config.compression)} · ${formatBytes(smallest.config.targetBlockBytes)}`,
        formatBytes(smallest.totals.storedBytes),
      ),
      comparisonCard(
        "Fastest insertBatch",
        `${compressionLabel(fastestLibrary.config.compression)} · ${formatBytes(fastestLibrary.config.targetBlockBytes)}`,
        formatDuration(fastestLibrary.library.timingsMs.insertBatches),
      ),
      comparisonCard(
        "Fastest bounded upsert",
        `${compressionLabel(fastestUpsert.config.compression)} · ${formatBytes(fastestUpsert.config.targetBlockBytes)}`,
        formatDuration(fastestUpsert.library.timingsMs.upsertBatch),
      ),
      comparisonCard(
        "Fastest partial update",
        `${compressionLabel(fastestUpdate.config.compression)} · ${formatBytes(fastestUpdate.config.targetBlockBytes)}`,
        formatDuration(fastestUpdate.library.timingsMs.updateBatch),
      ),
      comparisonCard(
        "Fastest key delete",
        `${compressionLabel(fastestDelete.config.compression)} · ${formatBytes(fastestDelete.config.targetBlockBytes)}`,
        formatDuration(fastestDelete.library.timingsMs.deleteBatch),
      ),
      comparisonCard(
        "Fastest reference suite",
        `${compressionLabel(fastestQueries.config.compression)} · ${formatBytes(fastestQueries.config.targetBlockBytes)}`,
        formatDuration(fastestQueries.referenceQueries.totalMs),
      ),
    ].join("");
  }
  historyRows.innerHTML = history
    .map((result, index) => {
      const primary = primaryMetric(result);
      return `<tr>
        <td><strong>#${String(history.length - index)}</strong><small>${escapeHtml(new Date(result.recordedAt).toLocaleTimeString())}</small></td>
        <td>${escapeHtml(scenarios.find((scenario) => scenario.id === result.config.scenario)?.name ?? result.config.scenario)}</td>
        <td>${formatDecimal(result.config.scale)}×</td>
        <td>${escapeHtml(compressionLabel(result.config.compression))}</td>
        <td>${formatBytes(result.config.targetBlockBytes)}</td>
        <td>${String(result.totals.blocks)}</td>
        <td>${formatBytes(result.totals.storedBytes)}</td>
        <td>${sizeChange(result.totals.encodedBytes, result.totals.storedBytes)}</td>
        <td>${formatDuration(primary.value)}</td>
        <td>${formatDuration(result.library.timingsMs.insertBatches)}</td>
        <td>${formatDuration(result.library.timingsMs.upsertBatch)}</td>
        <td>${formatDuration(result.library.timingsMs.updateBatch)}</td>
        <td>${formatDuration(result.library.timingsMs.deleteBatch)}</td>
        <td>${formatDuration(result.referenceQueries.totalMs)}</td>
        <td><span class="check ${result.verified ? "pass" : "fail"}">${result.verified ? "match" : "failed"}</span></td>
      </tr>`;
    })
    .join("");
}

function updateConfigurationSummary(): void {
  const scenario = getScenario("commerce");
  const scale = Number(scaleSelect.value);
  const totalRows = relationalTotalRows(scale);
  const orderRows = relationalRowCount("orders", scale);
  const repetitions = Number(repetitionsSelect.value);
  const repetitionCopy = repetitions === 1 ? "one run" : `${String(repetitions)} sequential runs`;
  const blockSize = blockSizeSelect.selectedOptions[0]?.textContent ?? blockSizeSelect.value;

  scaleDescription.textContent = `${formatInteger(totalRows)} total rows · ${formatInteger(orderRows)} orders.`;
  datasetPlanSummary.textContent = `${formatDecimal(scale)}× grows every dimension, bridge, transaction, and ledger table from one deterministic baseline. ${formatInteger(totalRows)} rows will be generated across ${String(scenario.entities.length)} tables.`;
  datasetPlanTables.innerHTML = scenario.entities
    .map(
      (entity) =>
        `<span><strong>${escapeHtml(entity.name)}</strong>${formatInteger(entity.rows(scale))}</span>`,
    )
    .join("");
  runSummaryCopy.textContent = `For ${repetitionCopy}, generate the ${formatDecimal(scale)}× relational commerce dataset (${formatInteger(totalRows)} rows across ${String(scenario.entities.length)} connected tables); encode it with ${compressionSelect.value}; split columns into blocks targeting ${blockSize}; journal and atomically commit with IndexedDB durability “${durabilitySelect.value}”; then read and verify every value. The public API persists the same tables, while fixed probes cover inserts, upserts, partial updates, projections, deletes, conflicts, compaction, and recovery. The relational suite validates primary keys, ${String(41)} foreign-key paths, value domains, and transaction ledgers before running 15 oracle-checked queries seven times each. The ad-hoc console then remains attached to the loaded snapshot. Highlighted time: “${focusSelect.value}”.`;
}

function finishRun(): void {
  activeRequestId = undefined;
  setRunning(false);
  progressLabel.textContent =
    batchTotal > 1
      ? `${String(batchTotal)} tests complete · storage, library, and relational checks pass`
      : "Test complete · storage, library, and reference-query checks pass";
  progressValue.textContent = "100%";
  progressBar.style.width = "100%";
}

function finishWithError(message: string): void {
  activeRequestId = undefined;
  pendingConfigs = [];
  setRunning(false);
  progressLabel.textContent = message;
  progressValue.textContent = "!";
  progressBar.style.width = "0%";
  verificationBadge.className = "verification-badge failed";
  verificationBadge.textContent = "Test failed";
}

function setRunning(running: boolean): void {
  for (const element of Array.from(form.elements)) {
    if (element instanceof HTMLInputElement || element instanceof HTMLSelectElement) {
      element.disabled = running;
    }
  }
  runButton.disabled = running;
  compareButton.disabled = running;
  compareEnginesButton.disabled = running;
  cancelButton.hidden = !running;
}

function renderEngineComparison(result: EngineComparisonResult): void {
  engineComparisonResults.hidden = false;
  const enginesVerified = result.engines.every((engine) => engine.verified);
  engineComparisonState.textContent = !enginesVerified
    ? "Completed with failures"
    : result.sqlChecksumsMatch
      ? "All verified"
      : "SQL checksum mismatch";
  engineComparisonNote.textContent = `${formatInteger(result.dataset.rows)} rows across ${String(result.dataset.tables)} tables in batches of at most ${formatInteger(result.dataset.batchRows)}. ${result.note}`;
  const engineHeaders = result.engines
    .map(
      (engine) =>
        `<th scope="col"><strong>${escapeHtml(engine.name)}</strong><small>${escapeHtml(engine.version)}</small></th>`,
    )
    .join("");
  const comparisonRows: Array<
    [string, (engine: EngineComparisonResult["engines"][number]) => string]
  > = [
    [
      "Persistence",
      (engine) =>
        engine.persistenceVerified === null
          ? "Memory only"
          : engine.persistenceVerified
            ? `Passed · ${formatDuration(engine.persistenceReopenMs ?? 0)}`
            : "Failed",
    ],
    ["Insert", (engine) => formatDuration(engine.insertMs)],
    ["Throughput", (engine) => `${formatInteger(engine.rowsPerSecond)} rows/s`],
    ["Schema + indexes", (engine) => formatDuration(engine.schemaMs + engine.indexMs)],
    ["Total", (engine) => formatDuration(engine.totalMs)],
    ["Total DB size", (engine) => formatDatabaseMb(engine.storedBytes)],
    [
      "Public query prep",
      (engine) =>
        engine.publicReadMs === null ? "included below" : formatDuration(engine.publicReadMs),
    ],
  ];
  const summaryMatrix = `<div class="table-scroll engine-matrix-scroll"><table class="engine-comparison-matrix">
    <thead><tr><th scope="col">Metric</th>${engineHeaders}</tr></thead>
    <tbody>${comparisonRows
      .map(
        ([label, render]) =>
          `<tr><th scope="row">${escapeHtml(label)}</th>${result.engines.map((engine) => `<td>${escapeHtml(render(engine))}</td>`).join("")}</tr>`,
      )
      .join("")}</tbody>
  </table></div>`;
  const queryMatrix = `<div class="table-scroll engine-matrix-scroll"><table class="engine-sql-matrix">
    <thead><tr><th scope="col">Query median / p95</th>${engineHeaders}</tr></thead>
    <tbody>${result.queries
      .map(
        (query) =>
          `<tr><th scope="row">${escapeHtml(query.id.toUpperCase())} · ${escapeHtml(query.name)}</th>${result.engines
            .map((engine) => {
              const measurement = engine.queries.find((entry) => entry.id === query.id);
              return `<td>${measurement === undefined ? "No public SQL API" : `${formatDuration(measurement.medianMs)} / ${formatDuration(measurement.p95Ms)}`}</td>`;
            })
            .join("")}</tr>`,
      )
      .join("")}</tbody>
  </table></div>`;
  const engineCards = result.engines
    .map((engine) => {
      const status = engine.verified ? "pass" : "fail";
      const queryRows =
        engine.queries.length === 0
          ? `<p class="engine-error">${escapeHtml(engine.disclosure)}</p>`
          : `<div class="table-scroll"><table class="engine-query-table">
              <thead><tr><th>Query</th><th>Complexity</th><th>Median</th><th>p95</th><th>Rows</th><th>Checksum</th></tr></thead>
              <tbody>${engine.queries
                .map(
                  (query) =>
                    `<tr><td><strong>${escapeHtml(query.id.toUpperCase())}</strong> ${escapeHtml(query.name)}</td><td>${escapeHtml(query.complexity)}</td><td>${formatDuration(query.medianMs)}</td><td>${formatDuration(query.p95Ms)}</td><td>${formatInteger(query.resultRows)}</td><td>${formatInteger(query.checksum)}</td></tr>`,
                )
                .join("")}</tbody>
            </table></div>`;
      return `<article class="engine-result ${status}">
        <div class="engine-result-heading">
          <div><h4>${escapeHtml(engine.name)}</h4><small>${escapeHtml(engine.version)} · ${escapeHtml(engine.persistence)}</small></div>
          <span class="check ${status}">${engine.verified ? "verified" : "failed"}</span>
        </div>
        ${engine.error === undefined ? "" : `<p class="engine-error">${escapeHtml(engine.error)}</p>`}
        <div class="engine-metrics">
          ${metric("Insert", formatDuration(engine.insertMs), `${formatInteger(engine.rowsPerSecond)} rows/s`)}
          ${metric("Schema + indexes", formatDuration(engine.schemaMs + engine.indexMs), `indexes ${formatDuration(engine.indexMs)}`)}
          ${metric("Total", formatDuration(engine.totalMs), `cold start ${formatDuration(engine.coldStartMs)}`)}
          ${metric("Persistence", engine.persistenceVerified === null ? "Memory only" : engine.persistenceVerified ? "Reopen passed" : "Reopen failed", engine.persistenceVerified === null ? "DuckDB OPFS reopen failed verification" : engine.persistenceReopenMs === null ? "verification did not complete" : formatDuration(engine.persistenceReopenMs))}
          ${metric("Total DB size", formatDatabaseMb(engine.storedBytes), engine.storageSizeKind)}
          ${metric("Peak heap delta", formatOptionalBytes(engine.peakHeapDeltaBytes ?? undefined), engine.peakHeapBytes === null ? "browser API unavailable" : `peak ${formatBytes(engine.peakHeapBytes)}`)}
          ${metric("Largest batch", formatBytes(engine.maxGeneratedBatchBytes), "logical generated values")}
          ${metric("Loaded assets", formatOptionalBytes(engine.loadedResourceBytes ?? undefined), "Resource Timing; cache can hide bytes")}
        </div>
        <details><summary>Configuration and disclosure</summary><p>${escapeHtml(engine.recommendedSettings.join(" · "))}</p><p>${escapeHtml(engine.disclosure)}</p></details>
        ${queryRows}
      </article>`;
    })
    .join("");
  engineComparisonResults.innerHTML = `${summaryMatrix}<h4 class="subsection-title">SQL latency · all four engines</h4>${queryMatrix}<div class="engine-detail-grid">${engineCards}</div>`;
}

function primaryMetric(result: BenchmarkResult): { label: string; value: number } {
  switch (result.config.focus) {
    case "write":
      return {
        label: "saving",
        value:
          result.timingsMs.transactionBegin +
          result.timingsMs.write +
          result.timingsMs.manifestCommit,
      };
    case "read":
      return { label: "loading", value: result.timingsMs.read + result.timingsMs.decode };
    case "roundtrip":
      return { label: "whole test", value: result.timingsMs.total };
  }
}

function metric(label: string, value: string, detail: string): string {
  return `<article class="metric-card"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(detail)}</small></article>`;
}

function storageStat(label: string, value: string): string {
  return `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function comparisonCard(label: string, value: string, detail: string): string {
  return `<article><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(detail)}</small></article>`;
}

function checkCard(check: {
  label: string;
  detail: string;
  durationMs: number;
  passed: boolean;
}): string {
  return `<article class="transaction-check ${check.passed ? "passed" : "failed"}">
    <span class="transaction-check-icon" aria-hidden="true">${check.passed ? "✓" : "×"}</span>
    <div><strong>${escapeHtml(check.label)}</strong><small>${escapeHtml(check.detail)}</small></div>
    <time>${formatDuration(check.durationMs)}</time>
  </article>`;
}

function queryCard(query: BenchmarkResult["referenceQueries"]["queries"][number]): string {
  return `<article class="query-card ${query.verified ? "passed" : "failed"}">
    <div class="query-card-heading">
      <div>
        <span class="complexity-pill ${query.complexity}">${escapeHtml(query.complexity)}</span>
        <span class="query-id">${escapeHtml(query.id.toUpperCase())}</span>
        <h5>${escapeHtml(query.name)}</h5>
      </div>
      <span class="check ${query.verified ? "pass" : "fail"}">${query.verified ? "oracle match" : "oracle mismatch"}</span>
    </div>
    <p class="query-tables">${escapeHtml(query.tables.join(" · "))}</p>
    <dl class="query-timings">
      <div><dt>${String(query.sampleCount)}-sample total</dt><dd>${formatDuration(query.totalMs)}</dd></div>
      <div><dt>Median</dt><dd>${formatDuration(query.medianMs)}</dd></div>
      <div><dt>p95</dt><dd>${formatDuration(query.p95Ms)}</dd></div>
      <div><dt>Measured</dt><dd>${formatDuration(query.measurementMs)} / ${formatInteger(query.iterations)} exec</dd></div>
      <div><dt>Oracle check</dt><dd>${formatDuration(query.oracleMs)}</dd></div>
      <div><dt>Result</dt><dd>${formatInteger(query.resultRows)} / ${formatInteger(query.expectedRows)} rows</dd></div>
    </dl>
    <details>
      <summary>Reference SQL and checksum</summary>
      <pre><code>${escapeHtml(query.sql)}</code></pre>
      <small>Optimized checksum ${formatInteger(query.checksum)} · oracle checksum ${formatInteger(query.oracleChecksum)}</small>
    </details>
  </article>`;
}

function timingLabel(name: keyof BenchmarkResult["timingsMs"]): string {
  const labels: Record<keyof BenchmarkResult["timingsMs"], string> = {
    transactionBegin: "Begin transaction",
    generate: "Create sample data",
    encode: "Prepare + compress",
    write: "Save blocks",
    manifestCommit: "Commit transaction",
    read: "Load blocks",
    decode: "Open + check blocks",
    verify: "Check every value",
    total: "Total",
  };
  return labels[name];
}

function libraryTimingLabel(name: keyof BenchmarkResult["library"]["timingsMs"]): string {
  const labels: Record<keyof BenchmarkResult["library"]["timingsMs"], string> = {
    createTables: "Create tables",
    generateBatches: "Generate batches",
    insertBatches: "insertBatch",
    upsertBatch: "upsertBatch (≤1k keys)",
    updateBatch: "updateBatch (partial)",
    deleteBatch: "deleteBatch (keys)",
    projectedRead: "readTable (2 of 5 columns)",
    verifyInventory: "Verify inventory",
    total: "Total",
  };
  return labels[name];
}

function formatDuration(value: number): string {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1_000) return `${value.toFixed(1)} ms`;
  return `${(value / 1_000).toFixed(2)} s`;
}

function compressionLabel(value: BenchmarkConfig["compression"]): string {
  switch (value) {
    case "raw":
      return "None";
    case "rle":
      return "Simple repeats";
    case "gzip":
      return "Gzip";
  }
}

function formatBytes(value: number): string {
  if (value < 1_024) return `${formatInteger(value)} B`;
  if (value < 1_048_576) return `${(value / 1_024).toFixed(1)} KB`;
  if (value < 1_073_741_824) return `${(value / 1_048_576).toFixed(2)} MB`;
  return `${(value / 1_073_741_824).toFixed(2)} GB`;
}

function formatOptionalBytes(value: number | undefined): string {
  return value === undefined ? "Unavailable" : formatBytes(value);
}

function formatDatabaseMb(value: number | null): string {
  return value === null ? "Unavailable" : `${(value / 1_048_576).toFixed(2)} MB`;
}

function formatSignedBytes(value: number | undefined): string {
  if (value === undefined) return "Unavailable";
  return `${value >= 0 ? "+" : "−"}${formatBytes(Math.abs(value))}`;
}

function formatDecimal(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}

function sizeChange(before: number, after: number): string {
  if (before === after) return "same size";
  const percentage = Math.abs((1 - after / before) * 100);
  return `${formatDecimal(percentage)}% ${after < before ? "smaller" : "larger"}`;
}

function formatInteger(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function isBenchmarkResult(value: unknown): value is BenchmarkResult {
  return (
    typeof value === "object" &&
    value !== null &&
    "totals" in value &&
    "blocks" in value &&
    "transactions" in value &&
    "library" in value &&
    "referenceQueries" in value &&
    "verified" in value
  );
}

function isAdHocQueryResult(value: unknown): value is AdHocQueryResult {
  return (
    typeof value === "object" &&
    value !== null &&
    "sql" in value &&
    "previewRows" in value &&
    "metrics" in value &&
    "rowCount" in value
  );
}

function isEngineComparisonResult(value: unknown): value is EngineComparisonResult {
  return (
    typeof value === "object" &&
    value !== null &&
    "dataset" in value &&
    "engines" in value &&
    "sqlChecksumsMatch" in value
  );
}

// The type parameter gives each statically known selector its concrete DOM element API.
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (element === null) throw new Error(`Missing benchmark element: ${selector}`);
  return element;
}
