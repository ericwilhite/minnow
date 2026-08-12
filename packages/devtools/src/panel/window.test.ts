import { describe, expect, it } from "vitest";
import {
  clampToViewport,
  cornerRect,
  minimumSize,
  moveBy,
  parseRect,
  preferredSize,
  resizeBy,
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

  it("still fits a panel larger than the viewport", () => {
    const rect = cornerRect("bottom-right", { width: 500, height: 340 }, desktop);
    expect(rect.x).toBeGreaterThanOrEqual(0);
    expect(rect.y).toBeGreaterThanOrEqual(0);
    expect(rect.width).toBeLessThanOrEqual(500);
  });
});

describe("preferredSize", () => {
  it("never opens smaller than the usable minimum", () => {
    const size = preferredSize({ width: 320, height: 200 });
    expect(size.width).toBe(minimumSize.width);
    expect(size.height).toBe(minimumSize.height);
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
});

describe("moveBy", () => {
  it("moves without resizing and stops at the edges", () => {
    const rect = { x: 100, y: 100, width: 600, height: 400 };
    expect(moveBy(rect, 50, -30, desktop)).toEqual({ x: 150, y: 70, width: 600, height: 400 });
    const pinned = moveBy(rect, -1000, -1000, desktop);
    expect(pinned).toEqual({ x: 0, y: 0, width: 600, height: 400 });
    expect(moveBy(rect, 5000, 5000, desktop)).toEqual({
      x: desktop.width - 600,
      y: desktop.height - 400,
      width: 600,
      height: 400,
    });
  });
});

describe("resizeBy", () => {
  const rect = { x: 100, y: 100, width: 600, height: 400 };

  it("keeps the origin fixed", () => {
    const resized = resizeBy(rect, 120, 60, desktop);
    expect(resized.x).toBe(100);
    expect(resized.y).toBe(100);
    expect(resized).toMatchObject({ width: 720, height: 460 });
  });

  it("refuses to shrink below the usable minimum", () => {
    expect(resizeBy(rect, -5000, -5000, desktop)).toMatchObject(minimumSize);
  });

  it("stops at the viewport edge rather than growing off screen", () => {
    expect(resizeBy(rect, 5000, 5000, desktop).width).toBe(desktop.width - rect.x);
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
