// @vitest-environment happy-dom
import { act, createElement, type ReactElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { describe, expect, it } from "vitest";
import { useLiveQuery, useLiveSelector, type LiveExternalStore } from "./index.js";

interface Row {
  readonly id: number;
  readonly label: string;
}
interface Snapshot {
  readonly status: "ready";
  readonly rows: readonly Row[];
  readonly version: number;
}

/** A hand-driven external store with the identity behaviour of a typed live query. */
function createStore(rows: readonly Row[]): LiveExternalStore<Snapshot> & {
  publish(rows: readonly Row[], version: number): void;
} {
  let snapshot: Snapshot = { status: "ready", rows, version: 1 };
  const listeners = new Set<() => void>();
  return {
    getSnapshot: () => snapshot,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    publish: (next, version) => {
      snapshot = { status: "ready", rows: next, version };
      for (const listener of [...listeners]) listener();
    },
  };
}

function mount(element: ReactElement): { root: Root; container: HTMLElement } {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  const container = document.createElement("div");
  document.body.append(container);
  const root = createRoot(container);
  act(() => {
    root.render(element);
  });
  return { root, container };
}

describe("useLiveSelector", () => {
  it("re-renders only when the selection changes", () => {
    const first = { id: 1, label: "a" };
    const second = { id: 2, label: "b" };
    const store = createStore([first, second]);
    let wholeRenders = 0;
    let countRenders = 0;
    let rowRenders = 0;

    function Whole(): ReactElement {
      wholeRenders += 1;
      useLiveQuery(store);
      return createElement("span", null, "whole");
    }
    function Count(): ReactElement {
      countRenders += 1;
      const count = useLiveSelector(store, { select: (snapshot) => snapshot.rows.length });
      return createElement("span", { id: "count" }, String(count));
    }
    function FirstRow(): ReactElement {
      rowRenders += 1;
      const row = useLiveSelector(store, {
        select: (snapshot) => snapshot.rows.find((candidate) => candidate.id === 1),
      });
      return createElement("span", { id: "row" }, row?.label ?? "-");
    }
    const { root, container } = mount(
      createElement(
        "div",
        null,
        createElement(Whole),
        createElement(Count),
        createElement(FirstRow),
      ),
    );
    expect(container.querySelector("#count")?.textContent).toBe("2");
    expect([wholeRenders, countRenders, rowRenders]).toEqual([1, 1, 1]);

    // A version bump with the same rows: the whole-snapshot reader renders, selectors do not.
    act(() => {
      store.publish([first, second], 2);
    });
    expect([wholeRenders, countRenders, rowRenders]).toEqual([2, 1, 1]);

    // Row 2 changes: the count and the first row are unaffected.
    act(() => {
      store.publish([first, { id: 2, label: "B" }], 3);
    });
    expect([wholeRenders, countRenders, rowRenders]).toEqual([3, 1, 1]);

    // Row 1 changes: only its selector re-renders.
    const firstRenamed = { id: 1, label: "A" };
    act(() => {
      store.publish([firstRenamed, second], 4);
    });
    expect(container.querySelector("#row")?.textContent).toBe("A");
    expect([wholeRenders, countRenders, rowRenders]).toEqual([4, 1, 2]);

    // A third row: the count re-renders; row 1 keeps its object and its selector stays put.
    act(() => {
      store.publish([firstRenamed, second, { id: 3, label: "c" }], 5);
    });
    expect(container.querySelector("#count")?.textContent).toBe("3");
    expect([wholeRenders, countRenders, rowRenders]).toEqual([5, 2, 2]);
    act(() => {
      root.unmount();
    });
  });

  it("applies a custom equality to derived values", () => {
    const store = createStore([
      { id: 1, label: "a" },
      { id: 2, label: "b" },
    ]);
    let renders = 0;
    const sameIds = (previous: readonly number[], next: readonly number[]): boolean =>
      previous.length === next.length && previous.every((id, index) => id === next[index]);
    function Ids(): ReactElement {
      renders += 1;
      const ids = useLiveSelector(store, {
        select: (snapshot) => snapshot.rows.map((row) => row.id),
        isEqual: sameIds,
      });
      return createElement("span", { id: "ids" }, ids.join(","));
    }
    const { root, container } = mount(createElement(Ids));
    expect(container.querySelector("#ids")?.textContent).toBe("1,2");
    act(() => {
      store.publish(
        [
          { id: 1, label: "A" },
          { id: 2, label: "B" },
        ],
        2,
      );
    });
    expect(renders).toBe(1);
    act(() => {
      store.publish([{ id: 2, label: "B" }], 3);
    });
    expect(container.querySelector("#ids")?.textContent).toBe("2");
    expect(renders).toBe(2);
    act(() => {
      root.unmount();
    });
  });
});
