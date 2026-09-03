import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createGrid, type Grid } from "./grid.js";

/**
 * Vitest runs in Node here, with no DOM library installed, so the grid gets the smallest fake
 * that covers what it touches: element creation, attributes, children, classes, inline style,
 * the `hidden` flag, and a `clientHeight` the test can set. Nothing lays out, which is the point
 * — the height is whatever the test says it is.
 */
class FakeElement {
  children: FakeElement[] = [];
  className = "";
  textContent = "";
  title = "";
  hidden = false;
  clientHeight = 0;
  scrollTop = 0;
  readonly style: Record<string, string> = {};
  readonly dataset: Record<string, string> = {};
  readonly attributes = new Map<string, string>();
  readonly classList = {
    toggle: (name: string, force: boolean): void => {
      const names = new Set(this.className.split(" ").filter((part) => part.length > 0));
      if (force) names.add(name);
      else names.delete(name);
      this.className = [...names].join(" ");
    },
  };

  constructor(readonly tagName: string) {}

  setAttribute(name: string, value: string): void {
    this.attributes.set(name, value);
  }
  getAttribute(name: string): string | null {
    return this.attributes.get(name) ?? null;
  }
  removeAttribute(name: string): void {
    this.attributes.delete(name);
  }
  append(...nodes: Array<FakeElement | string>): void {
    for (const node of nodes) {
      if (typeof node === "string") this.textContent += node;
      else this.children.push(node);
    }
  }
  replaceChildren(...nodes: Array<FakeElement | string>): void {
    this.children = [];
    this.textContent = "";
    this.append(...nodes);
  }
  remove(): void {
    // Nothing owns a fake node, so there is nothing to detach it from.
  }
  addEventListener(): void {
    // No events are dispatched here; the test drives the grid through its own callbacks.
  }
  querySelectorAll(): FakeElement[] {
    return [];
  }
}

class FakeResizeObserver {
  static instances: FakeResizeObserver[] = [];
  observed: FakeElement[] = [];
  constructor(readonly callback: () => void) {
    FakeResizeObserver.instances.push(this);
  }
  observe(target: FakeElement): void {
    this.observed.push(target);
  }
  disconnect(): void {
    // The grid never disconnects; it is here so the fake satisfies the type.
  }
}

let frames: Array<() => void> = [];

function flushFrames(): void {
  const pending = frames;
  frames = [];
  for (const callback of pending) callback();
}

function rowsOf(grid: Grid): FakeElement[] {
  const viewport = (grid.node as unknown as FakeElement).children[0];
  const sizer = viewport?.children[0]?.children[1];
  return (sizer?.children ?? []).filter((child) => child.className.startsWith("grid-row"));
}

function pooled(grid: Grid): number {
  return rowsOf(grid).length;
}

function shown(grid: Grid): number {
  return rowsOf(grid).filter((row) => !row.hidden).length;
}

function viewportOf(grid: Grid): FakeElement {
  const viewport = (grid.node as unknown as FakeElement).children[0];
  if (viewport === undefined) throw new Error("the grid has no viewport");
  return viewport;
}

const rowHeight = 26;
const overscan = 8;

function fixture(): Grid {
  const grid = createGrid();
  grid.setColumns([{ name: "id", type: "number", isKey: true }, { name: "kind" }]);
  grid.setRows(Array.from({ length: 500 }, (_, id) => ({ id, kind: "created" })));
  return grid;
}

describe("grid windowing", () => {
  beforeEach(() => {
    FakeResizeObserver.instances = [];
    frames = [];
    vi.stubGlobal("document", { createElement: (tag: string) => new FakeElement(tag) });
    vi.stubGlobal("requestAnimationFrame", (callback: () => void): number => {
      frames.push(callback);
      return frames.length;
    });
    vi.stubGlobal("ResizeObserver", FakeResizeObserver);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("pools only the overscan while the viewport is hidden", () => {
    // A hidden tab measures 0px tall, which is how the explorer's first page usually arrives.
    const grid = fixture();
    expect(shown(grid)).toBe(overscan * 2);
    expect(pooled(grid)).toBe(overscan * 2 + 1);
  });

  it("grows the pool when its viewport reports a new height", () => {
    const grid = fixture();
    const viewport = viewportOf(grid);
    const observer = FakeResizeObserver.instances[0];
    expect(observer?.observed).toEqual([viewport]);

    viewport.clientHeight = 20 * rowHeight;
    observer?.callback();
    // The render is coalesced onto a frame, like a scroll.
    expect(shown(grid)).toBe(overscan * 2);
    flushFrames();

    expect(shown(grid)).toBe(20 + overscan * 2);
    expect(pooled(grid)).toBe(20 + overscan * 2 + 1);
    // Every pooled row shows a real, distinct row.
    const indexes = rowsOf(grid)
      .filter((row) => !row.hidden)
      .map((row) => Number(row.dataset.index));
    expect(new Set(indexes).size).toBe(indexes.length);
    expect(Math.max(...indexes)).toBe(20 + overscan * 2 - 1);
  });

  it("lays out on request when the environment has no ResizeObserver", () => {
    vi.stubGlobal("ResizeObserver", undefined);
    const grid = fixture();
    expect(FakeResizeObserver.instances).toHaveLength(0);

    viewportOf(grid).clientHeight = 30 * rowHeight;
    grid.layout();
    flushFrames();

    expect(shown(grid)).toBe(30 + overscan * 2);
  });

  it("never shows more rows than there are", () => {
    const grid = fixture();
    viewportOf(grid).clientHeight = 1000 * rowHeight;
    grid.layout();
    flushFrames();
    expect(shown(grid)).toBe(500);
    expect(pooled(grid)).toBe(500);
  });
});
