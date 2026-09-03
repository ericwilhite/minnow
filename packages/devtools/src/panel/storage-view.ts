import { button, el } from "../dom.js";
import { messageOf } from "../errors.js";
import { formatBytes, formatCount } from "../format.js";
import type { DevtoolsTarget } from "../target.js";

/**
 * The two reports a database gives about its own storage. Both are on `MinnowDatabase` and the
 * worker client; a target with neither gets no Storage tab at all.
 */
export interface StorageTarget extends DevtoolsTarget {
  storageStats?(): Promise<Record<string, unknown>>;
  maintenanceStatus?(): Promise<Record<string, unknown>> | Record<string, unknown>;
}

export function isStorageTarget(target: DevtoolsTarget): target is StorageTarget {
  const candidate = target as { storageStats?: unknown; maintenanceStatus?: unknown };
  return (
    typeof candidate.storageStats === "function" ||
    typeof candidate.maintenanceStatus === "function"
  );
}

export interface StorageView {
  node: HTMLElement;
  /** Reads both reports again. Called when the tab is shown and by its own button. */
  refresh(): Promise<void>;
}

/** `logicalBytes` → "logical bytes"; the report's own names, read as words. */
function labelOf(key: string): string {
  return key.replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase();
}

/** A value as it reads: bytes by size, counts with separators, flags as yes or no. */
export function describeStat(key: string, value: unknown): string {
  if (value === null || value === undefined) return "n/a";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number") {
    return /bytes$/i.test(key) ? formatBytes(value) : formatCount(value);
  }
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

/**
 * Flattens one level of nesting — `maintenance.degraded` — so a report reads as one list. The
 * reports are rendered by their own keys rather than a fixed list, so a field the engine adds
 * shows up here without a change.
 */
export function flattenStats(
  report: Record<string, unknown>,
  prefix = "",
): Array<[string, string]> {
  return Object.entries(report).flatMap(([key, value]): Array<[string, string]> => {
    const name = `${prefix}${labelOf(key)}`;
    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      return flattenStats(value as Record<string, unknown>, `${name} · `);
    }
    return [[name, describeStat(key, value)]];
  });
}

/**
 * Bytes on disk, blocks live and obsolete, and what the collector is doing: the facts behind a
 * database that feels slow or large. Read on demand, never in the background — each report walks
 * the store.
 */
export function createStorageView(target: StorageTarget): StorageView {
  const refresh = button("btn mini", "Refresh");
  const status = el("div", {
    class: "statusbar",
    attrs: { role: "status", "aria-live": "polite" },
  });
  const body = el("div", { class: "storage-body" });
  const node = el("div", { class: "storage" }, [
    el("div", { class: "toolbar" }, [
      el("span", { class: "crumb-meta", text: "storage and maintenance" }),
      el("span", { class: "spacer" }),
      refresh,
    ]),
    body,
    status,
  ]);

  function section(title: string, rows: Array<[string, string]>): HTMLElement {
    return el("div", { class: "storage-section" }, [
      el("div", { class: "index-group", text: title }),
      el(
        "div",
        { class: "plate" },
        rows.map(([key, value]) =>
          el("div", { class: "plate-row" }, [
            el("span", { class: "plate-key wide", text: key }),
            el("span", { class: "plate-value", text: value }),
          ]),
        ),
      ),
    ]);
  }

  let loading = false;

  async function load(): Promise<void> {
    if (loading) return;
    loading = true;
    refresh.disabled = true;
    status.textContent = "reading…";
    const started = performance.now();
    const sections: HTMLElement[] = [];
    try {
      if (target.storageStats !== undefined) {
        sections.push(section("Storage", flattenStats(await target.storageStats())));
      }
      if (target.maintenanceStatus !== undefined) {
        sections.push(section("Maintenance", flattenStats(await target.maintenanceStatus())));
      }
      body.replaceChildren(...sections);
      status.textContent = `read at ${new Date().toLocaleTimeString()} · ${String(Math.round(performance.now() - started))}ms`;
    } catch (error) {
      body.replaceChildren(el("div", { class: "grid-message", text: messageOf(error) }));
      status.textContent = "failed";
    } finally {
      loading = false;
      refresh.disabled = false;
    }
  }

  refresh.addEventListener("click", () => {
    void load();
  });
  status.textContent = "not read yet";

  return { node, refresh: load };
}
