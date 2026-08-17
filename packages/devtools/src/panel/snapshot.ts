import { readSnapshotSummary, type SnapshotSummary } from "@minnowdb/core/storage";
import type { SnapshotExportProgress, SnapshotLoadProgress } from "@minnowdb/core/storage";
import type { ConfirmLayer, ConfirmRequest } from "../confirm.js";
import { el, iconButton, icons } from "../dom.js";
import type { SnapshotTarget } from "../target.js";

/**
 * Downloading the database and loading one back, as two title-bar controls.
 *
 * A snapshot is one committed version copied out as a single file, so this is the panel's way to
 * take a copy of what you are looking at — to keep it, to hand it to someone, or to put it back
 * into a fresh database later. The file can be hundreds of megabytes, so nothing here waits in
 * silence: the export reports its phases, the load reports its bytes, and both land in a small
 * status chip beside the badges.
 */

/** Snapshot files carry their own magic number; the extension is only for the file picker. */
const SNAPSHOT_EXTENSION = ".minnow";

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${String(bytes)} B`;
  const units = ["KB", "MB", "GB"] as const;
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const rounded = value < 10 ? value.toFixed(1) : String(Math.round(value));
  return `${rounded} ${units[unit] ?? "GB"}`;
}

function percent(done: number, total: number): string {
  if (total <= 0) return "0%";
  return `${String(Math.min(100, Math.round((done / total) * 100)))}%`;
}

/**
 * `minnow-v42-2026-08-17.minnow`. The version says which copy of the database this is — two
 * downloads on one day are otherwise indistinguishable — and the date is what a downloads folder
 * sorts by. The date is the snapshot's own UTC day, so the name matches what is inside the file
 * rather than the time zone of whoever clicked.
 */
export function snapshotFileName(summary: SnapshotSummary, at: Date): string {
  const day = (Number.isFinite(at.getTime()) ? at : new Date(0)).toISOString().slice(0, 10);
  return `minnow-v${String(summary.version)}-${day}${SNAPSHOT_EXTENSION}`;
}

/** What the status chip says while a download runs. */
export function describeExport(progress: SnapshotExportProgress): string {
  if (progress.phase === "reading") return "reading the database…";
  if (progress.phase === "transfer") {
    return `copying ${percent(progress.transferredBytes, progress.totalBytes)}`;
  }
  return `read ${formatBytes(progress.totalBytes)}`;
}

/** And while a restore runs. */
export function describeLoad(progress: SnapshotLoadProgress): string {
  if (progress.phase === "blocks") {
    return `writing ${percent(progress.writtenBytes, progress.totalBytes)}`;
  }
  if (progress.phase === "catalog") return "writing the catalog…";
  return `wrote ${formatBytes(progress.totalBytes)}`;
}

/**
 * The confirmation copy. Everything shown here comes from the file's own header, which is read
 * without decoding a single block, so a person is told exactly what they picked before any of it
 * is loaded.
 */
export function confirmRestore(fileName: string, summary: SnapshotSummary): ConfirmRequest {
  return {
    title: "Restore this database from a snapshot",
    facts: [
      ["file", fileName],
      ["size", formatBytes(summary.byteLength)],
      ["version", String(summary.version)],
      ["tables", String(summary.tableCount)],
      ["taken", summary.createdAt],
      ["call", "importSnapshot(bytes)"],
    ],
    confirmLabel: "Restore database",
    warning:
      "The database has to be empty. One that already holds data refuses the load rather than merging two histories.",
  };
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Hands the bytes to the browser's own download. The anchor goes in the host document rather than
 * the panel's shadow root, which is where a download has to be started from, and the object URL is
 * released on a later task — revoking it in the same turn as the click cancels the download.
 */
function save(bytes: Uint8Array, fileName: string): void {
  // TypeScript allows for a Uint8Array over shared memory, which Blob will not take; a snapshot
  // the engine encoded never is, so the cast narrows rather than lies.
  const part = bytes as Uint8Array<ArrayBuffer>;
  const url = URL.createObjectURL(new Blob([part], { type: "application/octet-stream" }));
  const link = el("a", { attrs: { href: url, download: fileName } });
  link.hidden = true;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => {
    URL.revokeObjectURL(url);
  }, 0);
}

export interface SnapshotActionsDeps {
  target: SnapshotTarget;
  confirm: ConfirmLayer;
  /** Restoring writes a whole database, so it follows `permissions.write` like every other write. */
  write: boolean;
  /** A restore replaces the catalog the panel is showing, so the panel has to read it again. */
  onRestored(): void;
}

export interface SnapshotActions {
  /** Title-bar nodes, in the order they should appear. */
  nodes: HTMLElement[];
}

export function createSnapshotActions(deps: SnapshotActionsDeps): SnapshotActions {
  const status = el("span", { class: "badge snap-status" });
  status.hidden = true;
  const download = iconButton("winbtn", "Download the database", icons.download);
  const restore = iconButton("winbtn", "Restore from a snapshot file", icons.upload);
  const picker = el("input", {
    class: "snap-file",
    type: "file",
    attrs: { accept: SNAPSHOT_EXTENSION, tabindex: "-1", "aria-hidden": "true" },
  });
  picker.hidden = true;

  /** One at a time: both actions move the whole database, and neither is quick. */
  let busy = false;

  function setStatus(text: string, kind = ""): void {
    status.className = `badge snap-status${kind}`;
    status.textContent = text;
    // The chip is narrow, so the full text — an error message especially — lives in the tooltip.
    status.title = text;
    status.hidden = false;
  }

  function setBusy(next: boolean): void {
    busy = next;
    download.disabled = next;
    restore.disabled = next;
  }

  async function downloadSnapshot(): Promise<void> {
    if (busy) return;
    setBusy(true);
    try {
      const bytes = await deps.target.exportSnapshot({
        onProgress: (progress) => {
          setStatus(describeExport(progress));
        },
      });
      // Header only, and it is already in memory: the file names itself after the version it holds.
      const summary = await readSnapshotSummary(bytes);
      save(bytes, snapshotFileName(summary, new Date()));
      setStatus(`saved ${formatBytes(bytes.byteLength)}`, " ok");
    } catch (error) {
      setStatus(messageOf(error), " warn");
    } finally {
      setBusy(false);
    }
  }

  async function restoreFrom(chosen: File): Promise<void> {
    if (busy) return;
    setBusy(true);
    try {
      setStatus(`reading ${chosen.name}`);
      const bytes = new Uint8Array(await chosen.arrayBuffer());
      const summary = await readSnapshotSummary(bytes);
      setStatus("waiting for confirmation");
      if (!(await deps.confirm.ask(confirmRestore(chosen.name, summary)))) {
        setStatus("cancelled");
        return;
      }
      await deps.target.importSnapshot(bytes, {
        onProgress: (progress) => {
          setStatus(describeLoad(progress));
        },
      });
      setStatus(`restored ${formatBytes(bytes.byteLength)}`, " ok");
      deps.onRestored();
    } catch (error) {
      setStatus(messageOf(error), " warn");
    } finally {
      setBusy(false);
    }
  }

  download.addEventListener("click", () => {
    void downloadSnapshot();
  });
  restore.addEventListener("click", () => {
    picker.click();
  });
  picker.addEventListener("change", () => {
    const chosen = picker.files?.[0];
    // Cleared first, so picking the same file twice still counts as a change.
    picker.value = "";
    if (chosen !== undefined) void restoreFrom(chosen);
  });

  // Restoring is a write, and a read-only panel offers no control that would be refused.
  return { nodes: deps.write ? [status, download, restore, picker] : [status, download] };
}
