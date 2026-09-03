import { button, el, iconButton, icons } from "../dom.js";
import { readFlag, writeFlag } from "../storage.js";
import { describeAge, describeOutcome, historyLimit, type HistoryEntry } from "./store.js";

export interface HistoryRail {
  node: HTMLElement;
  render(entries: readonly HistoryEntry[], selected: string | undefined): void;
}

export interface HistoryRailDeps {
  /** Namespaces the remembered collapsed state. */
  storageKey: string;
  /** Loads the entry back into the editor, with its rows if they are still cached. */
  onPick(entry: HistoryEntry): void;
  /** Saves or forgets one entry. */
  onToggleSaved(entry: HistoryEntry): void;
  onClear(): void;
  /** Injected so the list can be rendered deterministically in a test. */
  now?(): number;
}

/** The last fifty runs, newest first. */
export function createHistoryRail(deps: HistoryRailDeps): HistoryRail {
  const list = el("div", { class: "hlist" });
  const clear = button("hclear", "Clear", { title: "Forget every query that is not saved" });
  const collapse = iconButton("side-toggle", "Hide the history", icons.chevronRight);
  const expand = iconButton("side-toggle", "Show the history", icons.chevronLeft);
  const node = el("div", { class: "history side" }, [
    el("div", { class: "side-head" }, [
      el("span", { class: "side-title", text: "History" }),
      el("span", { class: "spacer" }),
      clear,
      collapse,
    ]),
    el("div", { class: "side-stub" }, [expand]),
    list,
  ]);

  const collapsedKey = `${deps.storageKey}:history-collapsed`;
  let collapsed = readFlag(collapsedKey, false);
  node.classList.toggle("collapsed", collapsed);

  for (const [control, next] of [
    [collapse, true],
    [expand, false],
  ] as const) {
    control.addEventListener("click", () => {
      collapsed = next;
      writeFlag(collapsedKey, collapsed);
      node.classList.toggle("collapsed", collapsed);
    });
  }

  clear.addEventListener("click", () => {
    deps.onClear();
  });

  return {
    node,
    render: (entries, selected) => {
      if (entries.length === 0) {
        list.replaceChildren(
          el("div", {
            class: "hempty",
            text: `Queries you run are remembered here — the last ${String(historyLimit)}.`,
          }),
        );
        return;
      }
      const now = deps.now?.() ?? Date.now();
      const item = (entry: HistoryEntry): HTMLElement => {
        const pick = el(
          "button",
          {
            class: `hitem${entry.id === selected ? " on" : ""}${entry.error === undefined ? "" : " failed"}`,
            type: "button",
          },
          [
            el("span", { class: "hsql", text: entry.sql, title: entry.sql }),
            el("span", { class: "hmeta" }, [
              el("span", { class: "houtcome", text: describeOutcome(entry) }),
              el("span", { class: "spacer" }),
              el("span", { text: describeAge(entry.at, now) }),
            ]),
          ],
        );
        pick.addEventListener("click", () => {
          deps.onPick(entry);
        });
        const saved = entry.saved === true;
        const star = button(`hstar${saved ? " on" : ""}`, saved ? "★" : "☆", {
          title: saved ? "Forget this query" : "Save this query",
          attrs: { "aria-pressed": String(saved), "aria-label": "Save this query" },
        });
        star.addEventListener("click", () => {
          deps.onToggleSaved(entry);
        });
        return el("div", { class: "hrow" }, [pick, star]);
      };
      const saved = entries.filter((entry) => entry.saved === true);
      const recent = entries.filter((entry) => entry.saved !== true);
      list.replaceChildren(
        ...(saved.length === 0
          ? []
          : [el("div", { class: "hgroup", text: "Saved" }), ...saved.map(item)]),
        ...(recent.length === 0
          ? []
          : [
              ...(saved.length === 0 ? [] : [el("div", { class: "hgroup", text: "Recent" })]),
              ...recent.map(item),
            ]),
      );
    },
  };
}
