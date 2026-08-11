import "../style.css";
import { relationalTotalRows } from "../benchmark.js";
import type {
  DatasetCreatePayload,
  DatasetListResult,
  DatasetRecord,
  EngineId,
  EngineMaterialization,
} from "../protocol.js";
import {
  escapeHtml,
  formatBytes,
  formatDecimal,
  formatDuration,
  formatInteger,
  formatOptionalBytes,
  required,
} from "../ui/format.js";
import { initShell } from "../ui/shell.js";
import { BenchWorker } from "../ui/worker-client.js";

initShell();

const worker = new BenchWorker();
const form = required<HTMLFormElement>("#generator");
const scaleSelect = required<HTMLSelectElement>("#scale");
const compressionSelect = required<HTMLSelectElement>("#compression");
const blockSizeSelect = required<HTMLSelectElement>("#block-size");
const durabilitySelect = required<HTMLSelectElement>("#durability");
const engineChecks = required<HTMLElement>("#engine-checks");
const plan = required<HTMLElement>("#plan");
const generateButton = required<HTMLButtonElement>("#generate");
const cancelButton = required<HTMLButtonElement>("#cancel-generate");
const progressBox = required<HTMLElement>("#gen-progress");
const progressLabel = required<HTMLElement>("#gen-progress-label");
const progressBar = required<HTMLElement>("#gen-progress-bar");
const status = required<HTMLElement>("#gen-status");
const datasetEmpty = required<HTMLElement>("#dataset-empty");
const datasetTable = required<HTMLElement>("#dataset-table");
const datasetRows = required<HTMLElement>("#dataset-rows");

let activeRequestId: string | undefined;

updatePlan();
scaleSelect.addEventListener("change", updatePlan);
engineChecks.addEventListener("change", updatePlan);
void refreshList();

form.addEventListener("submit", (event) => {
  event.preventDefault();
  if (activeRequestId !== undefined) return;
  const engines = selectedEngines();
  if (engines.length === 0) {
    setStatus("Select at least one engine.", "fail");
    return;
  }
  const payload: DatasetCreatePayload = {
    scale: Number(scaleSelect.value),
    compression: compressionSelect.value as DatasetCreatePayload["compression"],
    targetBlockBytes: Number(blockSizeSelect.value),
    durability: durabilitySelect.value as DatasetCreatePayload["durability"],
    engines,
  };
  const { requestId, result } = worker.start<DatasetRecord>(
    "datasetCreate",
    payload,
    (progress) => {
      progressLabel.textContent = progress.message;
      const fraction = progress.completed / Math.max(1, progress.total);
      progressBar.style.width = `${String(Math.round(fraction * 100))}%`;
    },
  );
  activeRequestId = requestId;
  setRunning(true);
  setStatus("", "");
  result
    .then((record) => {
      const failed = Object.values(record.engines).filter(
        (materialization) => materialization.status === "failed",
      );
      setStatus(
        failed.length === 0
          ? `Dataset ready: ${formatInteger(record.totalRows)} rows per engine.`
          : `Dataset created with failures: ${failed.map((materialization) => materialization.engine).join(", ")}.`,
        failed.length === 0 ? "ok" : "fail",
      );
    })
    .catch((error: unknown) => {
      setStatus(error instanceof Error ? error.message : String(error), "fail");
    })
    .finally(() => {
      activeRequestId = undefined;
      setRunning(false);
      void refreshList();
    });
});

cancelButton.addEventListener("click", () => {
  if (activeRequestId === undefined) return;
  progressLabel.textContent = "Cancelling after the current batch…";
  worker.cancel(activeRequestId);
});

function selectedEngines(): EngineId[] {
  return Array.from(engineChecks.querySelectorAll<HTMLInputElement>("input:checked")).map(
    (input) => input.value as EngineId,
  );
}

function updatePlan(): void {
  const scale = Number(scaleSelect.value);
  const engines = selectedEngines().length;
  plan.textContent = `${formatDecimal(scale)}× generates ${formatInteger(relationalTotalRows(scale))} rows across 50 tables${
    engines > 1 ? `, loaded into ${String(engines)} engines sequentially` : ""
  }. Large scales take minutes per engine.`;
}

function setRunning(running: boolean): void {
  generateButton.disabled = running;
  cancelButton.hidden = !running;
  progressBox.hidden = !running;
  if (!running) progressBar.style.width = "0%";
}

function setStatus(message: string, kind: "ok" | "fail" | ""): void {
  status.textContent = message;
  status.className = `status ${kind}`;
}

async function refreshList(): Promise<void> {
  try {
    const { datasets } = await worker.request<DatasetListResult>("datasetList", {});
    renderList(datasets);
  } catch (error) {
    setStatus(error instanceof Error ? error.message : String(error), "fail");
  }
}

function renderList(datasets: DatasetRecord[]): void {
  datasetEmpty.hidden = datasets.length > 0;
  datasetTable.hidden = datasets.length === 0;
  datasetRows.innerHTML = datasets
    .map(
      (record) => `<tr data-id="${record.id}">
        <td>${escapeHtml(new Date(record.createdAt).toLocaleString())}</td>
        <td class="num">${formatDecimal(record.scale)}×</td>
        <td class="num">${formatInteger(record.totalRows)}</td>
        <td>${engineCell(record.engines.browserdatabase)}</td>
        <td>${engineCell(record.engines.sqlite)}</td>
        <td>${engineCell(record.engines.pglite)}</td>
        <td><button class="danger" data-delete="${record.id}">Delete</button></td>
      </tr>`,
    )
    .join("");
  for (const button of Array.from(
    datasetRows.querySelectorAll<HTMLButtonElement>("[data-delete]"),
  )) {
    button.addEventListener("click", () => {
      const id = button.dataset.delete ?? "";
      if (!window.confirm("Delete this dataset from every engine's storage?")) return;
      button.disabled = true;
      worker
        .request<DatasetListResult>("datasetDelete", { id })
        .then(({ datasets: remaining }) => {
          renderList(remaining);
        })
        .catch((error: unknown) => {
          setStatus(error instanceof Error ? error.message : String(error), "fail");
          button.disabled = false;
        });
    });
  }
}

function engineCell(materialization: EngineMaterialization | undefined): string {
  if (materialization === undefined) return `<span class="badge muted">—</span>`;
  if (materialization.status === "failed") {
    return `<span class="badge fail" title="${escapeHtml(materialization.error ?? "failed")}">failed</span>`;
  }
  const detail = `${formatOptionalBytes(materialization.storedBytes)} · built in ${formatDuration(materialization.buildMs)} · ${escapeHtml(materialization.persistence)}`;
  return `<span class="badge ok" title="${escapeHtml(detail)}">ready</span> <small class="note">${formatBytes(materialization.storedBytes ?? 0)}</small>`;
}
