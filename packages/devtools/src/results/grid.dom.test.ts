// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createGrid } from "./grid.js";

const rows = Array.from({ length: 6 }, (_, index) => ({ id: index, name: `row ${String(index)}` }));

function grid(deps: Parameters<typeof createGrid>[0] = {}) {
  const view = createGrid(deps);
  document.body.append(view.node);
  Object.defineProperty(view.node.querySelector(".grid-viewport"), "clientHeight", { value: 400 });
  view.setColumns([
    { name: "id", type: "number", isKey: true },
    { name: "name", type: "string", label: "TEXT" },
  ]);
  view.setRows(rows);
  return view;
}

const rowNode = (index: number): HTMLElement => {
  const node = document.querySelector<HTMLElement>(`.grid-row[data-index="${String(index)}"]`);
  if (node === null) throw new Error(`row ${String(index)} not rendered`);
  return node;
};

const click = (index: number, init: MouseEventInit = {}): void => {
  rowNode(index).children[0]?.dispatchEvent(new MouseEvent("click", { bubbles: true, ...init }));
};

afterEach(() => {
  document.body.replaceChildren();
});

describe("selection", () => {
  it("selects one row on click, extends with shift, toggles with ctrl or cmd", () => {
    const onSelect = vi.fn();
    const view = grid({ onSelect });
    click(1);
    expect(view.selectedIndexes()).toEqual([1]);
    expect(onSelect).toHaveBeenLastCalledWith(rows[1], 1);
    click(3, { shiftKey: true });
    expect(view.selectedIndexes()).toEqual([1, 2, 3]);
    expect(view.selectedIndex()).toBe(1);
    click(2, { metaKey: true });
    expect(view.selectedIndexes()).toEqual([1, 3]);
    click(5, { ctrlKey: true });
    expect(view.selectedIndexes()).toEqual([1, 3, 5]);
    expect(rowNode(5).classList.contains("sel")).toBe(true);
    expect(rowNode(4).classList.contains("sel")).toBe(false);
    // A plain click starts over; clicking the only selected row again clears the selection.
    click(0);
    expect(view.selectedIndexes()).toEqual([0]);
    click(0);
    expect(view.selectedIndexes()).toEqual([]);
    expect(onSelect).toHaveBeenLastCalledWith(undefined, undefined);
  });

  it("shifts the selection up when a row above it is removed", () => {
    const view = grid();
    click(2);
    click(4, { metaKey: true });
    view.removeRow(1);
    expect(view.selectedIndexes()).toEqual([1, 3]);
    expect(view.rowAt(1)?.id).toBe(2);
  });
});

describe("header", () => {
  it("shows the declared label over the inferred type", () => {
    grid();
    expect([...document.querySelectorAll(".grid-type")].map((n) => n.textContent)).toEqual([
      "number",
      "TEXT",
    ]);
  });

  it("carries a resize handle that writes the dragged width into the shared template", () => {
    const view = grid();
    const handle = document.querySelector<HTMLElement>(".grid-th .grid-resize");
    const header = document.querySelector<HTMLElement>(".grid-th");
    if (handle === null || header === null) throw new Error("no handle");
    header.getBoundingClientRect = () => ({ width: 100 }) as DOMRect;
    handle.dispatchEvent(
      new PointerEvent("pointerdown", { button: 0, clientX: 10, clientY: 0, bubbles: true }),
    );
    window.dispatchEvent(new PointerEvent("pointermove", { clientX: 60, clientY: 0 }));
    window.dispatchEvent(new PointerEvent("pointerup", { clientX: 60, clientY: 0 }));
    expect(document.querySelector<HTMLElement>(".grid-head")?.style.gridTemplateColumns).toBe(
      "150px minmax(120px, 260px)",
    );
    expect(rowNode(0).style.gridTemplateColumns).toBe("150px minmax(120px, 260px)");
    // The width survives a reload of the same columns.
    view.setColumns([{ name: "id", type: "number" }, { name: "name" }]);
    expect(document.querySelector<HTMLElement>(".grid-head")?.style.gridTemplateColumns).toBe(
      "150px minmax(120px, 260px)",
    );
  });

  it("resizes a sortable column too, whose header is a button, without sorting it", () => {
    const onSort = vi.fn();
    const view = createGrid({ onSort });
    document.body.append(view.node);
    Object.defineProperty(view.node.querySelector(".grid-viewport"), "clientHeight", {
      value: 400,
    });
    view.setColumns([{ name: "id", type: "number", sortable: true }, { name: "name" }]);
    view.setRows(rows);
    const header = document.querySelector<HTMLElement>(".grid-th.sortable");
    const handle = header?.querySelector<HTMLElement>(".grid-resize");
    const press = header?.querySelector<HTMLElement>("button.grid-th-label");
    if (handle === null || handle === undefined || header === null || press == null) {
      throw new Error("no handle");
    }
    header.getBoundingClientRect = () => ({ width: 100 }) as DOMRect;
    handle.dispatchEvent(
      new PointerEvent("pointerdown", { button: 0, clientX: 10, clientY: 0, bubbles: true }),
    );
    window.dispatchEvent(new PointerEvent("pointermove", { clientX: 40, clientY: 0 }));
    window.dispatchEvent(new PointerEvent("pointerup", { clientX: 40, clientY: 0 }));
    // The click a press-and-release raises lands on the handle and must not reach the button.
    handle.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(document.querySelector<HTMLElement>(".grid-head")?.style.gridTemplateColumns).toBe(
      "130px minmax(120px, 260px)",
    );
    expect(rowNode(0).style.gridTemplateColumns).toBe("130px minmax(120px, 260px)");
    expect(onSort).not.toHaveBeenCalled();
    // A press on the header itself still sorts.
    press.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(onSort).toHaveBeenCalledWith("id");
  });

  it("is a columnheader holding a button, so sorting is announced as something to press", () => {
    const view = createGrid({ onSort: vi.fn() });
    document.body.append(view.node);
    view.setColumns([
      { name: "id", type: "number", sortable: true, sorted: "desc", isKey: true },
      { name: "name" },
    ]);
    const [id, name] = [...document.querySelectorAll<HTMLElement>(".grid-th")];
    expect(id?.getAttribute("role")).toBe("columnheader");
    expect(id?.getAttribute("aria-sort")).toBe("descending");
    expect(id?.querySelector("button")?.getAttribute("aria-label")).toBe(
      "Sort by id, sorted descending",
    );
    expect(name?.querySelector("button")).toBeNull();
    // A key in the first column is pinned for sideways scrolling, header and cells alike.
    expect(id?.classList.contains("pinned")).toBe(true);
  });
});

/** A grid over `count` rows in a viewport `height` rows tall, scrollable by setting `scrollTop`. */
function tallGrid(count: number, height: number, deps: Parameters<typeof createGrid>[0] = {}) {
  const view = createGrid(deps);
  document.body.append(view.node);
  const viewport = view.node.querySelector<HTMLElement>(".grid-viewport");
  if (viewport === null) throw new Error("no viewport");
  let clientHeight = height * 26;
  let scrollTop = 0;
  Object.defineProperty(viewport, "clientHeight", { get: () => clientHeight });
  Object.defineProperty(viewport, "scrollTop", {
    get: () => scrollTop,
    set: (value: number) => {
      scrollTop = Math.max(0, Math.min(value, count * 26 - clientHeight));
    },
  });
  view.setColumns([
    { name: "id", type: "number", isKey: true },
    { name: "name", type: "string" },
  ]);
  const data = Array.from({ length: count }, (_, id) => ({ id, name: `row ${String(id)}` }));
  view.setRows(data);
  return {
    view,
    data,
    viewport,
    scrollTo: (top: number) => {
      viewport.scrollTop = top;
      view.layout();
      vi.runAllTimers();
    },
    resize: (rows: number) => {
      clientHeight = rows * 26;
      view.layout();
      vi.runAllTimers();
    },
  };
}

/** Rows in view that have no visible node at the right place: a blank gap in the grid. */
function gaps(viewport: HTMLElement): number[] {
  const first = Math.floor(viewport.scrollTop / 26);
  const last = first + Math.ceil(viewport.clientHeight / 26);
  const rendered = new Map<number, HTMLElement>();
  for (const node of document.querySelectorAll<HTMLElement>(".grid-row")) {
    if (!node.hidden) rendered.set(Number(node.dataset.index), node);
  }
  const missing: number[] = [];
  for (let index = first; index < last; index += 1) {
    const node = rendered.get(index);
    if (node?.style.transform !== `translateY(${String(index * 26)}px)`) missing.push(index);
  }
  return missing;
}

describe("windowing", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["requestAnimationFrame"] });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("leaves no gap when rows that scrolled past the end of the window scroll back in", () => {
    // At the end the window is shorter than the ring of pooled nodes, so some are hidden; they
    // must not still claim the rows they showed, or those rows stay blank when scrolled back to.
    // Scrolled a little at a time, the way a wheel does, so the rows just above the window are
    // the ones the hidden nodes last showed.
    const { viewport, scrollTo } = tallGrid(200, 19);
    const end = 200 * 26 - 19 * 26;
    for (let top = 0; top <= end; top += 130) scrollTo(top);
    scrollTo(end);
    expect(gaps(viewport)).toEqual([]);
    for (let top = end; top >= end - 1300; top -= 130) {
      scrollTo(top);
      expect(gaps(viewport)).toEqual([]);
    }
  });

  it("leaves no gap after the grid was hidden while scrolled and is shown again", () => {
    // A hidden tab measures 0px tall and reports no scroll; showing it restores both.
    const { viewport, scrollTo, resize } = tallGrid(200, 19);
    scrollTo(3000);
    resize(0);
    resize(19);
    expect(gaps(viewport)).toEqual([]);
  });

  it("replaces rows where they stand, keeping the scroll and the selection by key", () => {
    const { view, data, viewport, scrollTo } = tallGrid(400, 19);
    scrollTo(2600);
    view.setSelected(120);
    // A live re-read: one row before the selection is gone, so the selected row moved up one.
    const next = data.filter((row) => row.id !== 10).map((row) => ({ ...row, name: "changed" }));
    view.replaceRows(next, (row) => String(row.id));
    expect(viewport.scrollTop).toBe(2600);
    expect(view.selectedIndexes()).toEqual([119]);
    expect(view.rowAt(view.selectedIndex() ?? -1)?.id).toBe(120);
    expect(gaps(viewport)).toEqual([]);
    expect(document.querySelector('.grid-row[data-index="100"]')?.textContent).toContain("changed");
  });

  it("is one tab stop, and moves it with the keys, scrolling the row into view", () => {
    const { viewport } = tallGrid(200, 10);
    const stops = () => [...document.querySelectorAll<HTMLElement>('.cell[tabindex="0"]')];
    expect(stops()).toHaveLength(1);
    const start = stops()[0];
    start?.focus();
    const press = (key: string, init: KeyboardEventInit = {}) => {
      (document.activeElement ?? start)?.dispatchEvent(
        new KeyboardEvent("keydown", { key, bubbles: true, ...init }),
      );
    };
    press("ArrowRight");
    press("PageDown");
    press("PageDown");
    press("PageDown");
    const focused = document.activeElement as HTMLElement | null;
    const row = focused?.closest<HTMLElement>(".grid-row");
    // A page is the rows in view below the header (which has no height here), less one kept for
    // context: three pages of nine from row 0.
    expect(row?.dataset.index).toBe("27");
    expect(focused?.getAttribute("aria-colindex")).toBe("2");
    expect(stops()).toEqual([focused]);
    expect(viewport.scrollTop).toBeGreaterThan(0);
    press("End", { ctrlKey: true });
    expect(
      (document.activeElement as HTMLElement | null)?.closest<HTMLElement>(".grid-row")?.dataset
        .index,
    ).toBe("199");
    expect(gaps(viewport)).toEqual([]);
  });
});

describe("copy", () => {
  it("copies the focused cell, or the selected rows as tab-separated text", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    const onCopied = vi.fn();
    const view = grid({ onCopied });
    const cell = rowNode(1).children[1] as HTMLElement;
    cell.dispatchEvent(new KeyboardEvent("keydown", { key: "c", metaKey: true, bubbles: true }));
    await Promise.resolve();
    expect(writeText).toHaveBeenLastCalledWith("row 1");
    expect(onCopied).toHaveBeenLastCalledWith("cell");
    view.setSelected(0);
    click(2, { shiftKey: true });
    cell.dispatchEvent(new KeyboardEvent("keydown", { key: "c", ctrlKey: true, bubbles: true }));
    await Promise.resolve();
    expect(writeText).toHaveBeenLastCalledWith("id\tname\n0\trow 0\n1\trow 1\n2\trow 2\n");
    expect(onCopied).toHaveBeenLastCalledWith("3 rows");
  });
});
