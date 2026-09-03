// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";
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
