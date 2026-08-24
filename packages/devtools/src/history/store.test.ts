import { describe, expect, it } from "vitest";
import {
  createHistoryStore,
  describeAge,
  describeOutcome,
  historyLimit,
  type HistoryStorage,
} from "./store.js";

function memoryStorage(initial?: string): HistoryStorage & { readonly written: () => string } {
  const map = new Map<string, string>();
  if (initial !== undefined) map.set("panel:history", initial);
  return {
    getItem: (key) => map.get(key) ?? null,
    setItem: (key, value) => {
      map.set(key, value);
    },
    written: () => map.get("panel:history") ?? "",
  };
}

const result = { columns: ["n"], rows: [{ n: 1 }] };

describe("createHistoryStore", () => {
  it("keeps the newest run first", () => {
    const store = createHistoryStore(memoryStorage(), "panel");
    store.add({ sql: "SELECT 1", ms: 3, rowCount: 1 }, "a", 1000);
    store.add({ sql: "SELECT 2", ms: 4, rowCount: 1 }, "b", 2000);
    expect(store.entries().map((entry) => entry.sql)).toEqual(["SELECT 2", "SELECT 1"]);
  });

  it("drops the oldest past the limit", () => {
    const store = createHistoryStore(memoryStorage(), "panel");
    for (let index = 0; index < historyLimit + 10; index += 1) {
      store.add({ sql: `SELECT ${String(index)}`, ms: 1 }, `id-${String(index)}`, index);
    }
    const entries = store.entries();
    expect(entries).toHaveLength(historyLimit);
    expect(entries[0]?.sql).toBe(`SELECT ${String(historyLimit + 9)}`);
    expect(entries.at(-1)?.sql).toBe("SELECT 10");
  });

  it("persists text and timings, and reloads them", () => {
    const storage = memoryStorage();
    const store = createHistoryStore(storage, "panel");
    store.add({ sql: "SELECT 1", ms: 7, rowCount: 2 }, "a", 1000);
    const reopened = createHistoryStore(storage, "panel");
    expect(reopened.entries()).toEqual([
      { id: "a", sql: "SELECT 1", ms: 7, rowCount: 2, at: 1000 },
    ]);
  });

  it("never persists result sets, which would blow the quota", () => {
    const storage = memoryStorage();
    const store = createHistoryStore(storage, "panel");
    const entry = store.add({ sql: "SELECT 1", ms: 1, rowCount: 1 }, "a", 1000);
    store.rememberResult(entry.id, result);
    expect(storage.written()).not.toContain("columns");
    // The rows are still there for this session.
    expect(store.resultFor("a")).toEqual(result);
    expect(createHistoryStore(storage, "panel").resultFor("a")).toBeUndefined();
  });

  it("forgets cached rows once their entry falls off the list", () => {
    const store = createHistoryStore(memoryStorage(), "panel");
    store.add({ sql: "SELECT 1", ms: 1 }, "old", 0);
    store.rememberResult("old", result);
    for (let index = 0; index < historyLimit; index += 1) {
      store.add({ sql: "SELECT x", ms: 1 }, `id-${String(index)}`, index + 1);
    }
    expect(store.resultFor("old")).toBeUndefined();
  });

  it("caps the cache so a session of wide queries cannot grow without bound", () => {
    const store = createHistoryStore(memoryStorage(), "panel");
    for (let index = 0; index < 15; index += 1) {
      const entry = store.add({ sql: "SELECT 1", ms: 1 }, `id-${String(index)}`, index);
      store.rememberResult(entry.id, result);
    }
    const cached = store.entries().filter((entry) => store.resultFor(entry.id) !== undefined);
    expect(cached.length).toBeLessThanOrEqual(10);
    // The most recent runs are the ones kept.
    expect(store.resultFor("id-14")).toEqual(result);
  });

  it("survives storage that is corrupt, empty, or absent", () => {
    expect(createHistoryStore(memoryStorage("not json"), "panel").entries()).toEqual([]);
    expect(createHistoryStore(memoryStorage("{}"), "panel").entries()).toEqual([]);
    // A malformed entry is dropped; the good ones beside it are kept.
    const mixed = JSON.stringify([{ id: "a", sql: "SELECT 1", at: 1, ms: 2 }, { sql: "no id" }, 7]);
    expect(createHistoryStore(memoryStorage(mixed), "panel").entries()).toHaveLength(1);
  });

  it("does not fail a query because storage refused to save", () => {
    const failing: HistoryStorage = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("full");
      },
    };
    const store = createHistoryStore(failing, "panel");
    expect(() => store.add({ sql: "SELECT 1", ms: 1 }, "a", 1)).not.toThrow();
    expect(store.entries()).toHaveLength(1);
  });

  it("clears both the list and the cached rows", () => {
    const store = createHistoryStore(memoryStorage(), "panel");
    const entry = store.add({ sql: "SELECT 1", ms: 1 }, "a", 1);
    store.rememberResult(entry.id, result);
    store.clear();
    expect(store.entries()).toEqual([]);
    expect(store.resultFor("a")).toBeUndefined();
  });
});

describe("describeAge", () => {
  it("reads at the coarsest useful unit", () => {
    const now = 10_000_000_000;
    expect(describeAge(now - 3_000, now)).toBe("3s ago");
    expect(describeAge(now - 240_000, now)).toBe("4m ago");
    expect(describeAge(now - 7_200_000, now)).toBe("2h ago");
    expect(describeAge(now - 172_800_000, now)).toBe("2d ago");
  });

  it("never reads as the future when clocks disagree", () => {
    expect(describeAge(2000, 1000)).toBe("0s ago");
  });
});

describe("describeOutcome", () => {
  it("shows rows and timing, or the failure instead", () => {
    expect(describeOutcome({ id: "a", sql: "x", at: 0, ms: 12, rowCount: 3 })).toBe(
      "3 rows · 12ms",
    );
    expect(describeOutcome({ id: "a", sql: "x", at: 0, ms: 12, rowCount: 1 })).toBe("1 row · 12ms");
    expect(describeOutcome({ id: "a", sql: "x", at: 0, ms: 12, error: "boom" })).toBe("boom");
    expect(
      describeOutcome({
        id: "a",
        sql: "x",
        at: 0,
        ms: 12,
        outcome: "created index by_name on people(name)",
      }),
    ).toBe("created index by_name on people(name) · 12ms");
  });
});
