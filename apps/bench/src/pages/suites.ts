import "../style.css";
import {
  relationalRowCount,
  type BenchmarkConfig,
  type BenchmarkFocus,
  type BenchmarkResult,
} from "../benchmark.js";
import type {
  DatasetListResult,
  DatasetRecord,
  EngineId,
  FeatureSuiteResult,
  ReferenceSuiteResult,
  WorkProgress,
  WriteSuiteResult,
} from "../protocol.js";
import { engineNames } from "../protocol.js";
import {
  escapeHtml,
  formatBytes,
  formatDecimal,
  formatDuration,
  formatInteger,
  required,
  stat,
} from "../ui/format.js";
import { highlightAll } from "../ui/highlight.js";
import { initShell } from "../ui/shell.js";
import { BenchWorker } from "../ui/worker-client.js";

initShell();

const worker = new BenchWorker();

interface ProgressWidget {
  box: HTMLElement;
  label: HTMLElement;
  bar: HTMLElement;
}

function progressWidget(prefix: string): ProgressWidget {
  return {
    box: required(`#${prefix}-progress`),
    label: required(`#${prefix}-progress-label`),
    bar: required(`#${prefix}-progress-bar`),
  };
}

function onProgress(widget: ProgressWidget): (progress: WorkProgress) => void {
  return (progress) => {
    widget.label.textContent = progress.message;
    const fraction = progress.completed / Math.max(1, progress.total);
    widget.bar.style.width = `${String(Math.round(fraction * 100))}%`;
  };
}

function setStatus(element: HTMLElement, message: string, kind: "ok" | "fail" | ""): void {
  element.textContent = message;
  element.className = `status ${kind}`;
}

function checkedEngines(container: HTMLElement): EngineId[] {
  return Array.from(container.querySelectorAll<HTMLInputElement>("input:checked")).map(
    (input) => input.value as EngineId,
  );
}

/* ---------------------------------------------------------------- reference suite */

const refDataset = required<HTMLSelectElement>("#ref-dataset");
const refEngines = required<HTMLElement>("#ref-engines");
const refRun = required<HTMLButtonElement>("#ref-run");
const refProgress = progressWidget("ref");
const refStatus = required<HTMLElement>("#ref-status");
const refResults = required<HTMLElement>("#ref-results");

const writeDataset = required<HTMLSelectElement>("#write-dataset");
const writeEngines = required<HTMLElement>("#write-engines");
const writeRun = required<HTMLButtonElement>("#write-run");
const writeProgress = progressWidget("write");
const writeStatus = required<HTMLElement>("#write-status");
const writeResults = required<HTMLElement>("#write-results");

let datasets: DatasetRecord[] = [];

/** Both dataset-backed suites offer the same datasets and the same materialized engines. */
function datasetOptions(): string {
  return datasets.length === 0
    ? `<option value="">No datasets — generate one first</option>`
    : datasets
        .map(
          (record) =>
            `<option value="${record.id}">${formatDecimal(record.scale)}× · ${formatInteger(record.totalRows)} rows · ${new Date(record.createdAt).toLocaleString()}</option>`,
        )
        .join("");
}

function renderEngineChecks(container: HTMLElement, datasetId: string): void {
  const record = datasets.find((candidate) => candidate.id === datasetId);
  container.innerHTML = (Object.keys(engineNames) as EngineId[])
    .map((engine) => {
      const ready = record?.engines[engine]?.status === "ready";
      return `<label><input type="checkbox" value="${engine}" ${ready ? "checked" : "disabled"} /> ${engineNames[engine]}${ready ? "" : " (not materialized)"}</label>`;
    })
    .join("");
}

void worker
  .request<DatasetListResult>("datasetList", {})
  .then((list) => {
    datasets = list.datasets;
    refDataset.innerHTML = datasetOptions();
    writeDataset.innerHTML = datasetOptions();
    renderEngineChecks(refEngines, refDataset.value);
    renderEngineChecks(writeEngines, writeDataset.value);
    refRun.disabled = datasets.length === 0;
    writeRun.disabled = datasets.length === 0;
  })
  .catch((error: unknown) => {
    setStatus(refStatus, error instanceof Error ? error.message : String(error), "fail");
  });

refDataset.addEventListener("change", () => {
  renderEngineChecks(refEngines, refDataset.value);
});

refRun.addEventListener("click", () => {
  const engines = checkedEngines(refEngines);
  if (refDataset.value === "" || engines.length === 0) {
    setStatus(refStatus, "Select a dataset and at least one engine.", "fail");
    return;
  }
  refRun.disabled = true;
  refProgress.box.hidden = false;
  setStatus(refStatus, "", "");
  worker
    .request<ReferenceSuiteResult>(
      "suiteReference",
      { datasetId: refDataset.value, engines },
      onProgress(refProgress),
    )
    .then(renderReferenceResult)
    .catch((error: unknown) => {
      setStatus(refStatus, error instanceof Error ? error.message : String(error), "fail");
    })
    .finally(() => {
      refRun.disabled = false;
      refProgress.box.hidden = true;
      refProgress.bar.style.width = "0%";
    });
});

function renderReferenceResult(result: ReferenceSuiteResult): void {
  setStatus(
    refStatus,
    result.passed
      ? "Every query an engine could run agreed with its oracle."
      : "At least one engine result disagreed with its oracle.",
    result.passed ? "ok" : "fail",
  );
  const engineSummary = result.engines
    .map(
      (engine) =>
        `${engineNames[engine]}: ${String(result.supportedByEngine[engine] ?? 0)}/${String(result.queries.length)} supported · ${formatDuration(result.totalMsByEngine[engine] ?? 0)} summed median`,
    )
    .join(" — ");
  const engineHeader = result.engines
    .map((engine) => `<th colspan="3">${escapeHtml(engineNames[engine])}</th>`)
    .join("");
  const subHeader = result.engines
    .map(() => `<th class="num">Median</th><th class="num">p95</th><th>Result</th>`)
    .join("");
  const rows = result.queries
    .map((query) => {
      const cells = result.engines
        .map((engine) => {
          const measurement = query.engines.find((candidate) => candidate.engine === engine);
          if (measurement === undefined) return "<td>—</td><td>—</td><td>—</td>";
          if (!measurement.supported) {
            return `<td class="num">—</td><td class="num">—</td><td><span class="badge gap" title="${escapeHtml(measurement.error ?? "")}">not supported</span><br /><small class="note">${escapeHtml(measurement.error ?? "")}</small></td>`;
          }
          const badge = measurement.verified
            ? `<span class="badge ok">matches oracle</span>`
            : `<span class="badge fail">differs from oracle</span>`;
          return `<td class="num">${formatDuration(measurement.medianMs)}</td><td class="num">${formatDuration(measurement.p95Ms)}</td><td>${badge}</td>`;
        })
        .join("");
      const gap =
        query.surfaceGap === undefined
          ? ""
          : `<p class="note">Not yet expressible: ${escapeHtml(query.surfaceGap)}</p>`;
      return `<tr>
        <td>
          <strong>${escapeHtml(query.id.toUpperCase())}</strong> ${escapeHtml(query.name)}
          <small class="note"> · ${escapeHtml(query.workload.toUpperCase())} read · ${escapeHtml(query.complexity)} · ${escapeHtml(query.tables.join(", "))}</small>
          <details><summary>SQL</summary><pre><code data-lang="sql">${escapeHtml(query.sql)}</code></pre>${gap}</details>
        </td>
        <td class="num">${formatInteger(query.oracleRows)}</td>
        ${cells}
      </tr>`;
    })
    .join("");
  refResults.innerHTML = `
    <p class="note">${escapeHtml(engineSummary)}.</p>
    <p class="note">
      How to read this: <strong>Median</strong> and <strong>p95</strong> summarize
      ${String(result.sampleCount)} measured executions of the prepared query on this machine,
      after one untimed warm-up — median is the typical execution, p95 the slow tail.
      <strong>Result</strong> compares the engine's rows tuple-for-tuple against an independent
      JavaScript implementation of the same query (the “oracle”), computed from the exact rows
      every engine loaded; numbers compare within a relative tolerance because aggregates
      accumulate in different orders. <strong>Rows</strong> is the row count the oracle expects.
      “Not supported” records the engine's own error for a query outside its SQL dialect.
    </p>
    <div class="table-scroll"><table>
      <thead>
        <tr><th rowspan="2">Query</th><th rowspan="2" class="num">Rows</th>${engineHeader}</tr>
        <tr>${subHeader}</tr>
      </thead>
      <tbody>${rows}</tbody>
    </table></div>`;
  void highlightAll(refResults);
}

/* ---------------------------------------------------------------- write suite */

writeDataset.addEventListener("change", () => {
  renderEngineChecks(writeEngines, writeDataset.value);
});

writeRun.addEventListener("click", () => {
  const engines = checkedEngines(writeEngines);
  if (writeDataset.value === "" || engines.length === 0) {
    setStatus(writeStatus, "Select a dataset and at least one engine.", "fail");
    return;
  }
  writeRun.disabled = true;
  writeProgress.box.hidden = false;
  setStatus(writeStatus, "", "");
  worker
    .request<WriteSuiteResult>(
      "suiteWrite",
      { datasetId: writeDataset.value, engines },
      onProgress(writeProgress),
    )
    .then(renderWriteResult)
    .catch((error: unknown) => {
      setStatus(writeStatus, error instanceof Error ? error.message : String(error), "fail");
    })
    .finally(() => {
      writeRun.disabled = false;
      writeProgress.box.hidden = true;
      writeProgress.bar.style.width = "0%";
    });
});

function renderWriteResult(result: WriteSuiteResult): void {
  setStatus(
    writeStatus,
    result.passed
      ? "Every write an engine could run left the table exactly as the oracle predicts."
      : "At least one engine's table disagreed with the oracle.",
    result.passed ? "ok" : "fail",
  );
  const engineSummary = result.engines
    .map(
      (engine) =>
        `${engineNames[engine]}: ${String(result.supportedByEngine[engine] ?? 0)}/${String(result.cases.length)} cases · ${formatDuration(result.totalMsByEngine[engine] ?? 0)} summed median`,
    )
    .join(" — ");
  const engineHeader = result.engines
    .map((engine) => `<th colspan="3">${escapeHtml(engineNames[engine])}</th>`)
    .join("");
  const subHeader = result.engines
    .map(() => `<th class="num">Median</th><th class="num">Rows/s</th><th>Result</th>`)
    .join("");
  const rows = result.cases
    .map((report) => {
      const cells = result.engines
        .map((engine) => {
          const measurement = report.engines.find((candidate) => candidate.engine === engine);
          if (measurement === undefined) return "<td>—</td><td>—</td><td>—</td>";
          if (!measurement.supported) {
            return `<td class="num">—</td><td class="num">—</td><td><span class="badge gap" title="${escapeHtml(measurement.error ?? "")}">not supported</span><br /><small class="note">${escapeHtml(measurement.error ?? "")}</small></td>`;
          }
          const badge = measurement.verified
            ? `<span class="badge ok">matches oracle</span>`
            : `<span class="badge fail">differs from oracle</span>`;
          return `<td class="num">${formatDuration(measurement.medianMs)}</td><td class="num">${formatInteger(measurement.rowsPerSecond)}</td><td>${badge}</td>`;
        })
        .join("");
      const seeded =
        report.seedRows === 0
          ? "empty table"
          : `${formatInteger(report.seedRows)} rows already present`;
      return `<tr>
        <td>
          <strong>${escapeHtml(report.workload.toUpperCase())} · ${escapeHtml(report.operation)}</strong> ${formatInteger(report.rows)} rows
          <small class="note"> · ${escapeHtml(seeded)} · ${formatInteger(report.expectedTableRows)} rows after</small>
        </td>
        <td>${report.checksumAgreement === null ? "—" : report.checksumAgreement === "match" ? `<span class="badge ok">agree</span>` : `<span class="badge fail">disagree</span>`}</td>
        ${cells}
      </tr>`;
    })
    .join("");
  writeResults.innerHTML = `
    <p class="note">${escapeHtml(engineSummary)}.</p>
    <p class="note">
      How to read this: <strong>Median</strong> is the middle of ${String(result.sampleCount)}
      measured runs of the operation, each into a table created empty for that run, so no run
      inherits rows or fragmentation from the one before. <strong>Rows/s</strong> divides the batch
      by that median. <strong>Result</strong> reads the finished table back — with MinnowDatabase's
      result memo off — and compares every row against a JavaScript oracle built from the same
      inputs. <strong>Engines</strong> says whether the engines that ran a case produced identical
      tables. Rows are ${escapeHtml(result.tableColumns.join(", "))}; an upsert's keys start halfway
      through the seeded range, so about half update in place and half insert.
    </p>
    <div class="table-scroll"><table>
      <thead>
        <tr><th rowspan="2">Operation</th><th rowspan="2">Engines</th>${engineHeader}</tr>
        <tr>${subHeader}</tr>
      </thead>
      <tbody>${rows}</tbody>
    </table></div>`;
}

/* ---------------------------------------------------------------- storage suite */

const storageScale = required<HTMLSelectElement>("#storage-scale");
const storageCompression = required<HTMLSelectElement>("#storage-compression");
const storageBlockSize = required<HTMLSelectElement>("#storage-block-size");
const storageDurability = required<HTMLSelectElement>("#storage-durability");
const storageFocus = required<HTMLSelectElement>("#storage-focus");
const storageRun = required<HTMLButtonElement>("#storage-run");
const storageMatrix = required<HTMLButtonElement>("#storage-matrix");
const storageCancel = required<HTMLButtonElement>("#storage-cancel");
const storageProgress = progressWidget("storage");
const storageStatus = required<HTMLElement>("#storage-status");
const storageResults = required<HTMLElement>("#storage-results");
const storageHistory = required<HTMLElement>("#storage-history");

const history: BenchmarkResult[] = [];
let pendingConfigs: BenchmarkConfig[] = [];
let batchTotal = 0;
let batchCompleted = 0;
let activeStorageRequestId: string | undefined;

function readStorageConfig(): BenchmarkConfig {
  const scale = Number(storageScale.value);
  return {
    scenario: "commerce",
    focus: storageFocus.value as BenchmarkFocus,
    scale,
    rows: relationalRowCount("orders", scale),
    compression: storageCompression.value as BenchmarkConfig["compression"],
    targetBlockBytes: Number(storageBlockSize.value),
    durability: storageDurability.value as BenchmarkConfig["durability"],
  };
}

storageRun.addEventListener("click", () => {
  beginBatch([readStorageConfig()]);
});

storageMatrix.addEventListener("click", () => {
  const base = readStorageConfig();
  const blockSizes = [262_144, 524_288, 1_048_576, 2_097_152, 4_194_304];
  const compressions: Array<BenchmarkConfig["compression"]> = ["raw", "rle", "gzip"];
  beginBatch(
    compressions.flatMap((compression) =>
      blockSizes.map((targetBlockBytes) => ({ ...base, compression, targetBlockBytes })),
    ),
  );
});

storageCancel.addEventListener("click", () => {
  pendingConfigs = [];
  if (activeStorageRequestId !== undefined) {
    storageProgress.label.textContent = "Stopping after the current step…";
    worker.cancel(activeStorageRequestId);
  }
});

function beginBatch(configs: BenchmarkConfig[]): void {
  if (activeStorageRequestId !== undefined) return;
  pendingConfigs = [...configs];
  batchTotal = configs.length;
  batchCompleted = 0;
  storageRun.disabled = true;
  storageMatrix.disabled = true;
  storageCancel.hidden = false;
  storageProgress.box.hidden = false;
  setStatus(storageStatus, "", "");
  startNextRun();
}

function startNextRun(): void {
  const config = pendingConfigs.shift();
  if (config === undefined) {
    finishBatch();
    return;
  }
  const { requestId, result } = worker.start<BenchmarkResult>("benchmark", config, (progress) => {
    const runFraction = progress.completed / Math.max(1, progress.total);
    const overall = (batchCompleted + runFraction) / Math.max(1, batchTotal);
    const prefix =
      batchTotal > 1 ? `Run ${String(batchCompleted + 1)} of ${String(batchTotal)} · ` : "";
    storageProgress.label.textContent = `${prefix}${progress.message}`;
    storageProgress.bar.style.width = `${String(Math.round(overall * 100))}%`;
  });
  activeStorageRequestId = requestId;
  result
    .then((benchmark) => {
      history.unshift(benchmark);
      batchCompleted += 1;
      renderStorageResult(benchmark);
      renderStorageHistory();
      startNextRun();
    })
    .catch((error: unknown) => {
      setStatus(storageStatus, error instanceof Error ? error.message : String(error), "fail");
      pendingConfigs = [];
      finishBatch();
    });
}

function finishBatch(): void {
  activeStorageRequestId = undefined;
  storageRun.disabled = false;
  storageMatrix.disabled = false;
  storageCancel.hidden = true;
  storageProgress.box.hidden = true;
  storageProgress.bar.style.width = "0%";
}

function primaryMetric(result: BenchmarkResult): { label: string; value: number } {
  switch (result.config.focus) {
    case "write":
      return {
        label: "write path",
        value:
          result.timingsMs.transactionBegin +
          result.timingsMs.write +
          result.timingsMs.manifestCommit,
      };
    case "read":
      return { label: "read path", value: result.timingsMs.read + result.timingsMs.decode };
    case "roundtrip":
      return { label: "whole roundtrip", value: result.timingsMs.total };
  }
}

function checkList(
  title: string,
  checks: Array<{ label: string; detail: string; durationMs: number; passed: boolean }>,
): string {
  const rows = checks
    .map(
      (check) => `<tr>
        <td>${check.passed ? `<span class="badge ok">✓</span>` : `<span class="badge fail">✗</span>`}</td>
        <td>${escapeHtml(check.label)}<br /><small class="note">${escapeHtml(check.detail)}</small></td>
        <td class="num">${formatDuration(check.durationMs)}</td>
      </tr>`,
    )
    .join("");
  return `<details ${checks.every((check) => check.passed) ? "" : "open"}>
    <summary>${escapeHtml(title)} · ${String(checks.filter((check) => check.passed).length)}/${String(checks.length)} passed</summary>
    <table><tbody>${rows}</tbody></table>
  </details>`;
}

function renderStorageResult(result: BenchmarkResult): void {
  setStatus(
    storageStatus,
    result.verified
      ? "Storage, transactions, writes, and reference queries pass."
      : "A storage, transaction, write, or reference-query check failed.",
    result.verified ? "ok" : "fail",
  );
  const primary = primaryMetric(result);
  const primarySeconds = Math.max(primary.value / 1_000, 0.000_001);
  const referenceQueryRows = result.referenceQueries.queries
    .map((query) => {
      const engineCell = query.engine.supported
        ? `${formatDuration(query.engine.medianMs)} ${
            query.engine.verified
              ? `<span class="badge ok">matches oracle</span>`
              : `<span class="badge fail">differs from oracle</span>`
          }`
        : `<span class="badge gap">not supported</span>`;
      return `<tr>
        <td><strong>${escapeHtml(query.id.toUpperCase())}</strong> ${escapeHtml(query.name)}</td>
        <td class="num">${engineCell}</td>
        <td class="num">${formatDuration(query.javascript.medianMs)}</td>
      </tr>`;
    })
    .join("");
  storageResults.innerHTML = `
    <div class="stats">
      ${stat("Main time", formatDuration(primary.value), primary.label)}
      ${stat("Speed", `${formatDecimal(result.totals.encodedBytes / 1_048_576 / primarySeconds)} MB/s`, `${formatInteger(result.totals.rows / primarySeconds)} rows/s`)}
      ${stat("Stored", formatBytes(result.totals.storedBytes), `${formatBytes(result.totals.encodedBytes)} before compression`)}
      ${stat("Blocks", formatInteger(result.totals.blocks), `${String(result.totals.entities)} tables · ${String(result.totals.columns)} columns`)}
      ${stat("Rows", formatInteger(result.totals.rows), "generated, written, read back, verified")}
      ${stat("insertBatch", formatDuration(result.library.timingsMs.insertBatches), `${formatInteger(result.library.totals.rows)} rows via the write API`)}
    </div>
    ${checkList("Transaction checks", result.transactions.checks)}
    ${checkList("Write API checks", result.library.checks)}
    ${checkList("Dataset integrity checks", result.referenceQueries.integrityChecks)}
    <details>
      <summary>Reference queries · ${String(result.referenceQueries.engineSupportedQueries)}/${String(result.referenceQueries.queries.length)} on SQL · engine median vs JS baseline</summary>
      <table>
        <thead><tr><th>Query</th><th class="num">Engine</th><th class="num">JS baseline</th></tr></thead>
        <tbody>${referenceQueryRows}</tbody>
      </table>
      <p class="note">${escapeHtml(result.referenceQueries.note)}</p>
    </details>`;
}

function renderStorageHistory(): void {
  if (history.length < 2) {
    storageHistory.innerHTML = "";
    return;
  }
  const rows = history
    .map((result) => {
      const primary = primaryMetric(result);
      return `<tr>
        <td>${escapeHtml(result.config.compression)}</td>
        <td class="num">${formatBytes(result.config.targetBlockBytes)}</td>
        <td class="num">${formatDecimal(result.config.scale)}×</td>
        <td class="num">${formatBytes(result.totals.storedBytes)}</td>
        <td class="num">${formatDuration(primary.value)}</td>
        <td class="num">${formatDuration(result.library.timingsMs.insertBatches)}</td>
        <td class="num">${formatDuration(result.referenceQueries.engineTotalMs)}</td>
        <td>${result.verified ? `<span class="badge ok">✓</span>` : `<span class="badge fail">✗</span>`}</td>
      </tr>`;
    })
    .join("");
  storageHistory.innerHTML = `
    <h3>Runs this session</h3>
    <div class="table-scroll"><table>
      <thead><tr><th>Compression</th><th class="num">Block size</th><th class="num">Scale</th><th class="num">Stored</th><th class="num">Main time</th><th class="num">insertBatch</th><th class="num">Reference SQL</th><th></th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`;
}

/* ---------------------------------------------------------------- feature matrix */

const fmEngines = required<HTMLElement>("#fm-engines");
const fmRun = required<HTMLButtonElement>("#fm-run");
const fmProgress = progressWidget("fm");
const fmStatus = required<HTMLElement>("#fm-status");
const fmResults = required<HTMLElement>("#fm-results");

fmRun.addEventListener("click", () => {
  const engines = checkedEngines(fmEngines);
  if (engines.length === 0) {
    setStatus(fmStatus, "Select at least one engine.", "fail");
    return;
  }
  fmRun.disabled = true;
  fmProgress.box.hidden = false;
  setStatus(fmStatus, "", "");
  worker
    .request<FeatureSuiteResult>("suiteFeatureMatrix", { engines }, onProgress(fmProgress))
    .then(renderFeatureResult)
    .catch((error: unknown) => {
      setStatus(fmStatus, error instanceof Error ? error.message : String(error), "fail");
    })
    .finally(() => {
      fmRun.disabled = false;
      fmProgress.box.hidden = true;
      fmProgress.bar.style.width = "0%";
    });
});

function outcomeBadge(outcome: "pass" | "fail" | "accepts" | "rejects", detail?: string): string {
  const title = detail === undefined ? "" : ` title="${escapeHtml(detail)}"`;
  switch (outcome) {
    case "pass":
      return `<span class="badge ok"${title}>pass</span>`;
    case "fail":
      return `<span class="badge fail"${title}>drift</span>`;
    case "accepts":
      return `<span class="badge muted"${title}>accepts</span>`;
    case "rejects":
      return `<span class="badge gap"${title}>rejects</span>`;
  }
}

function renderFeatureResult(result: FeatureSuiteResult): void {
  setStatus(
    fmStatus,
    result.driftFailures === 0
      ? `No drift: the live engine matches the shipped matrix (${String(result.supportedCount)} supported, ${String(result.unsupportedCount)} unsupported).`
      : `${String(result.driftFailures)} features drifted from the shipped matrix.`,
    result.driftFailures === 0 ? "ok" : "fail",
  );
  const header = `<tr><th>Feature</th><th>Cataloged</th>${result.engines
    .map((engine) => `<th>${escapeHtml(engineNames[engine])}</th>`)
    .join("")}</tr>`;
  const rows = result.features
    .map((feature) => {
      const cells = result.engines
        .map((engine) => {
          const outcome = feature.engines.find((candidate) => candidate.engine === engine);
          return `<td>${outcome === undefined ? "—" : outcomeBadge(outcome.outcome, outcome.detail)}</td>`;
        })
        .join("");
      return `<tr>
        <td>
          <code>${escapeHtml(feature.id)}</code>
          <details><summary>example</summary><pre><code data-lang="sql">${escapeHtml(feature.example)}</code></pre>${
            feature.expectedError === undefined
              ? ""
              : `<p class="note">Recorded error: <code>${escapeHtml(feature.expectedError)}</code></p>`
          }</details>
        </td>
        <td>${
          feature.status === "supported"
            ? `<span class="badge ok">supported</span>`
            : `<span class="badge gap">unsupported</span>`
        }</td>
        ${cells}
      </tr>`;
    })
    .join("");
  fmResults.innerHTML = `
    <p class="note">For MinnowDatabase, “pass” on an unsupported feature means the engine still refuses it with the recorded error. “accepts” / “rejects” on other engines is informational — their SQL surfaces simply differ.</p>
    <div class="table-scroll"><table><thead>${header}</thead><tbody>${rows}</tbody></table></div>`;
  void highlightAll(fmResults);
}
