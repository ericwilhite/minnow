import { button, el } from "../dom.js";
import { describeAge, describeOutcome, historyLimit, type HistoryEntry } from "./store.js";

export interface HistoryRail {
  node: HTMLElement;
  render(entries: readonly HistoryEntry[], selected: string | undefined): void;
}

export interface HistoryRailDeps {
  /** Loads the entry back into the editor, with its rows if they are still cached. */
  onPick(entry: HistoryEntry): void;
  onClear(): void;
  /** Injected so the list can be rendered deterministically in a test. */
  now?(): number;
}

/** The last fifty runs, newest first. */
export function createHistoryRail(deps: HistoryRailDeps): HistoryRail {
  const list = el("div", { class: "hlist" });
  const clear = button("hclear", "Clear", { title: "Forget every remembered query" });
  const node = el("div", { class: "history" }, [
    el("div", { class: "history-head" }, [
      el("span", { text: "History" }),
      el("span", { class: "spacer" }),
      clear,
    ]),
    list,
  ]);

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
      list.replaceChildren(
        ...entries.map((entry) => {
          const item = el(
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
          item.addEventListener("click", () => {
            deps.onPick(entry);
          });
          return item;
        }),
      );
    },
  };
}
