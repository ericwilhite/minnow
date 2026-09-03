// @vitest-environment happy-dom
import { describe, expect, it, vi } from "vitest";
import { createHistoryRail } from "./rail.js";
import type { HistoryEntry } from "./store.js";

const entries: HistoryEntry[] = [
  { id: "a", sql: "SELECT 1", at: 1000, ms: 2, rowCount: 1 },
  { id: "b", sql: "SELECT 'kept'", at: 500, ms: 3, rowCount: 1, saved: true },
  { id: "c", sql: "DELETE FROM t", at: 100, ms: 1, error: "nope" },
];

describe("createHistoryRail", () => {
  it("groups saved entries first and marks them", () => {
    const deps = {
      storageKey: "t",
      onPick: vi.fn(),
      onToggleSaved: vi.fn(),
      onClear: vi.fn(),
      now: () => 2000,
    };
    const rail = createHistoryRail(deps);
    rail.render(entries, "a");
    expect([...rail.node.querySelectorAll(".hgroup")].map((n) => n.textContent)).toEqual([
      "Saved",
      "Recent",
    ]);
    expect([...rail.node.querySelectorAll(".hsql")].map((n) => n.textContent)).toEqual([
      "SELECT 'kept'",
      "SELECT 1",
      "DELETE FROM t",
    ]);
    expect(
      [...rail.node.querySelectorAll(".hstar")].map((n) => n.getAttribute("aria-pressed")),
    ).toEqual(["true", "false", "false"]);
    expect(rail.node.querySelector(".hitem.on .hsql")?.textContent).toBe("SELECT 1");
    expect(rail.node.querySelector(".hitem.failed .houtcome")?.textContent).toBe("nope");
  });

  it("picks on the row and saves on the star, without one triggering the other", () => {
    const deps = { storageKey: "t", onPick: vi.fn(), onToggleSaved: vi.fn(), onClear: vi.fn() };
    const rail = createHistoryRail(deps);
    rail.render(entries, undefined);
    rail.node.querySelectorAll<HTMLButtonElement>(".hstar")[1]?.click();
    expect(deps.onToggleSaved).toHaveBeenCalledWith(entries[0]);
    expect(deps.onPick).not.toHaveBeenCalled();
    rail.node.querySelectorAll<HTMLButtonElement>(".hitem")[1]?.click();
    expect(deps.onPick).toHaveBeenCalledWith(entries[0]);
  });

  it("says what is remembered when nothing is yet", () => {
    const rail = createHistoryRail({
      storageKey: "t",
      onPick: vi.fn(),
      onToggleSaved: vi.fn(),
      onClear: vi.fn(),
    });
    rail.render([], undefined);
    expect(rail.node.querySelector(".hempty")?.textContent).toContain("the last 50");
  });
});
