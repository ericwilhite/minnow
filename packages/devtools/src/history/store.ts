import type { QueryResult } from "@minnowdb/core";

export interface HistoryEntry {
  id: string;
  sql: string;
  /** Epoch milliseconds, so the list can be ordered and aged without a Date in storage. */
  at: number;
  ms: number;
  /** Rows returned, or rows affected for a statement. Absent when the run failed. */
  rowCount?: number;
  /** Operation-specific result for a non-query statement. */
  outcome?: string;
  error?: string;
}

/** How many runs are remembered. Past this the oldest falls off the end. */
export const historyLimit = 50;

/**
 * How many result sets are held in memory. Text and timings persist; result sets do not — fifty
 * of them would blow the storage quota on the first wide query, so they live in a small in-memory
 * cache and simply stop being available once it turns over.
 */
const resultCacheLimit = 10;

/** The storage slice this needs, so tests can hand it a map instead of a browser. */
export interface HistoryStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export interface HistoryStore {
  entries(): HistoryEntry[];
  /** Records a run and returns the entry, evicting the oldest past the limit. */
  add(entry: Omit<HistoryEntry, "id" | "at">, id: string, at: number): HistoryEntry;
  /** The result of a past run, when it is still cached. */
  resultFor(id: string): QueryResult | undefined;
  rememberResult(id: string, result: QueryResult): void;
  clear(): void;
}

function parseEntries(raw: string | null): HistoryEntry[] {
  if (raw === null) return [];
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return [];
  }
  if (!Array.isArray(value)) return [];
  // Anything malformed is dropped rather than rendered: a corrupt entry must not break the list.
  return value.filter((entry): entry is HistoryEntry => {
    if (typeof entry !== "object" || entry === null) return false;
    const candidate = entry as Partial<HistoryEntry>;
    return (
      typeof candidate.id === "string" &&
      typeof candidate.sql === "string" &&
      typeof candidate.at === "number" &&
      typeof candidate.ms === "number"
    );
  });
}

export function createHistoryStore(storage: HistoryStorage, storageKey: string): HistoryStore {
  const key = `${storageKey}:history`;
  let entries: HistoryEntry[] = [];
  try {
    entries = parseEntries(storage.getItem(key));
  } catch {
    // Storage can be unavailable; history then lives for the session only.
  }
  // Insertion order doubles as eviction order.
  const results = new Map<string, QueryResult>();

  function persist(): void {
    try {
      storage.setItem(key, JSON.stringify(entries));
    } catch {
      // A full or blocked quota costs the history, never the query that was just run.
    }
  }

  return {
    entries: () => [...entries],
    add: (entry, id, at) => {
      const recorded: HistoryEntry = { ...entry, id, at };
      // Newest first, so the list reads top-down without reversing on every render.
      entries = [recorded, ...entries].slice(0, historyLimit);
      const live = new Set(entries.map((item) => item.id));
      for (const cached of [...results.keys()]) {
        if (!live.has(cached)) results.delete(cached);
      }
      persist();
      return recorded;
    },
    resultFor: (id) => results.get(id),
    rememberResult: (id, result) => {
      results.set(id, result);
      while (results.size > resultCacheLimit) {
        const oldest = results.keys().next();
        if (oldest.done === true) break;
        results.delete(oldest.value);
      }
    },
    clear: () => {
      entries = [];
      results.clear();
      persist();
    },
  };
}

/** "3s ago", "4m ago" — enough to place a run without showing a clock. */
export function describeAge(at: number, now: number): string {
  const seconds = Math.max(0, Math.round((now - at) / 1000));
  if (seconds < 60) return `${String(seconds)}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${String(minutes)}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${String(hours)}h ago`;
  return `${String(Math.round(hours / 24))}d ago`;
}

/** The one-line summary under each entry. */
export function describeOutcome(entry: HistoryEntry): string {
  if (entry.error !== undefined) return entry.error;
  if (entry.outcome !== undefined) return `${entry.outcome} · ${String(entry.ms)}ms`;
  const rows = entry.rowCount ?? 0;
  return `${String(rows)} row${rows === 1 ? "" : "s"} · ${String(entry.ms)}ms`;
}
