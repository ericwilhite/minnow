import { describe, expect, it } from "vitest";
import {
  clampToViewport,
  cornerRect,
  maximizedRect,
  minimumSize,
  moveBy,
  parseRect,
  preferredSize,
  resizeBy,
  resizeSplit,
} from "./window.js";

const desktop = { width: 1440, height: 900 };

describe("cornerRect", () => {
  it("places the panel against the named corner", () => {
    const size = { width: 800, height: 500 };
    expect(cornerRect("bottom-right", desktop, size)).toEqual({
      x: 1440 - 800 - 24,
      y: 900 - 500 - 24,
      ...size,
    });
    expect(cornerRect("top-left", desktop, size)).toEqual({ x: 24, y: 24, ...size });
  });

  it("anchors to the origin on a viewport too small to hold the minimum", () => {
    // At exactly the desktop floor it still opens at that floor.
    const rect = cornerRect("bottom-right", { width: 500, height: 340 }, desktop);
    expect(rect.x).toBe(0);
    expect(rect.y).toBe(0);
    expect(rect.width).toBe(minimumSize.width);

    // Below the floor, the viewport wins: close and window controls must never hang off-screen.
    const phone = cornerRect("bottom-right", { width: 390, height: 700 }, desktop);
    expect(phone.x).toBe(0);
    expect(phone.width).toBe(390);
  });

  it("fits a panel that is merely larger than asked for", () => {
    const rect = cornerRect(
      "bottom-right",
      { width: 1200, height: 800 },
      { width: 2000, height: 900 },
    );
    expect(rect.x).toBeGreaterThanOrEqual(0);
    expect(rect.y).toBeGreaterThanOrEqual(0);
    expect(rect.x + rect.width).toBeLessThanOrEqual(1200);
  });
});

describe("preferredSize", () => {
  it("keeps the desktop minimum when it fits and the viewport margins when it does not", () => {
    expect(preferredSize(desktop).width).toBeGreaterThanOrEqual(minimumSize.width);
    const size = preferredSize({ width: 320, height: 200 });
    expect(size).toEqual({ width: 304, height: 184 });
  });
});

describe("clampToViewport", () => {
  it("pulls a panel back on screen after the window shrinks", () => {
    const rect = { x: 1200, y: 700, width: 800, height: 500 };
    expect(clampToViewport(rect, { width: 900, height: 640 })).toEqual({
      x: 100,
      y: 140,
      width: 800,
      height: 500,
    });
  });

  it("shrinks a panel restored from a larger screen", () => {
    const restored = { x: 0, y: 0, width: 2400, height: 1400 };
    const clamped = clampToViewport(restored, desktop);
    expect(clamped.width).toBe(desktop.width);
    expect(clamped.height).toBe(desktop.height);
  });

  it("shrinks below the desktop floor on a phone so every control stays reachable", () => {
    const clamped = clampToViewport(
      { x: 200, y: 100, width: minimumSize.width, height: 500 },
      { width: 390, height: 640 },
    );
    expect(clamped).toEqual({ x: 0, y: 100, width: 390, height: 500 });
  });
});

describe("moveBy", () => {
  it("moves without resizing and stops at the edges", () => {
    const rect = { x: 100, y: 100, width: 800, height: 400 };
    expect(moveBy(rect, 50, -30, desktop)).toEqual({ x: 150, y: 70, width: 800, height: 400 });
    const pinned = moveBy(rect, -1000, -1000, desktop);
    expect(pinned).toEqual({ x: 0, y: 0, width: 800, height: 400 });
    expect(moveBy(rect, 5000, 5000, desktop)).toEqual({
      x: desktop.width - 800,
      y: desktop.height - 400,
      width: 800,
      height: 400,
    });
  });
});

describe("resizeBy", () => {
  const rect = { x: 100, y: 100, width: 800, height: 400 };

  it("grows down and right from the bottom-right corner, origin fixed", () => {
    const resized = resizeBy(rect, "se", 120, 60, desktop);
    expect(resized).toEqual({ x: 100, y: 100, width: 920, height: 460 });
  });

  it("moves the origin when a leading edge is dragged, keeping the far edge still", () => {
    // Dragging the left edge right by 50 must shrink the panel, not move it.
    const west = resizeBy(rect, "w", 50, 0, desktop);
    expect(west).toEqual({ x: 150, y: 100, width: 750, height: 400 });
    expect(west.x + west.width).toBe(rect.x + rect.width);

    const north = resizeBy(rect, "n", 40, 40, desktop);
    expect(north).toEqual({ x: 100, y: 140, width: 800, height: 360 });
    expect(north.y + north.height).toBe(rect.y + rect.height);
  });

  it("changes only the axis its edge belongs to", () => {
    expect(resizeBy(rect, "e", 60, 999, desktop)).toEqual({ ...rect, width: 860 });
    expect(resizeBy(rect, "s", 999, 60, desktop)).toEqual({ ...rect, height: 460 });
  });

  it("resizes both axes from a corner", () => {
    expect(resizeBy(rect, "nw", -50, -40, desktop)).toEqual({
      x: 50,
      y: 60,
      width: 850,
      height: 440,
    });
  });

  it("refuses to shrink below the usable minimum, from either side", () => {
    expect(resizeBy(rect, "se", -5000, -5000, desktop)).toMatchObject(minimumSize);
    const west = resizeBy(rect, "w", 5000, 0, desktop);
    expect(west.width).toBe(minimumSize.width);
    // The far edge held, so the origin is what moved.
    expect(west.x + west.width).toBe(rect.x + rect.width);
  });

  it("stops at the viewport edge rather than growing off screen", () => {
    expect(resizeBy(rect, "se", 5000, 5000, desktop).width).toBe(desktop.width - rect.x);
    expect(resizeBy(rect, "w", -5000, 0, desktop).x).toBe(0);
    expect(resizeBy(rect, "n", 0, -5000, desktop).y).toBe(0);
  });
});

describe("maximizedRect", () => {
  it("fills the viewport from the origin", () => {
    expect(maximizedRect(desktop)).toEqual({ x: 0, y: 0, width: 1440, height: 900 });
  });
});

describe("resizeSplit", () => {
  const minimums = { top: 60, bottom: 120 };

  it("moves the divider by the drag", () => {
    expect(resizeSplit(200, 50, 600, minimums)).toBe(250);
    expect(resizeSplit(200, -50, 600, minimums)).toBe(150);
  });

  it("keeps a minimum for each pane rather than collapsing either", () => {
    expect(resizeSplit(200, -5000, 600, minimums)).toBe(minimums.top);
    expect(resizeSplit(200, 5000, 600, minimums)).toBe(600 - minimums.bottom);
  });

  it("gives the top pane away first when the container cannot hold both", () => {
    // 100px of room, and the results alone want 120: the editor keeps its floor and the
    // container scrolls rather than the divider landing at a negative height.
    expect(resizeSplit(80, 0, 100, minimums)).toBe(minimums.top);
  });
});

describe("parseRect", () => {
  it("accepts a well-formed rect", () => {
    expect(parseRect('{"x":1,"y":2,"width":3,"height":4}')).toEqual({
      x: 1,
      y: 2,
      width: 3,
      height: 4,
    });
  });

  it("treats anything unusable as absent, so a corrupt entry cannot break opening", () => {
    expect(parseRect(null)).toBeUndefined();
    expect(parseRect("not json")).toBeUndefined();
    expect(parseRect("[]")).toBeUndefined();
    expect(parseRect('{"x":1,"y":2,"width":3}')).toBeUndefined();
    expect(parseRect('{"x":null,"y":2,"width":3,"height":4}')).toBeUndefined();
    expect(parseRect('{"x":1,"y":2,"width":3,"height":"tall"}')).toBeUndefined();
  });
});
