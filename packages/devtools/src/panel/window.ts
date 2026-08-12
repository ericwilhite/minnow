import type { DevtoolsCorner } from "../options.js";

export interface Size {
  width: number;
  height: number;
}

export interface Rect extends Size {
  x: number;
  y: number;
}

/** Below this the panel stops being usable: the toolbar wraps and the results lose their columns. */
export const minimumSize: Size = { width: 460, height: 300 };

/** Gap between the panel and the viewport edge when it first opens. */
const openMargin = 24;

function clamp(value: number, low: number, high: number): number {
  return Math.min(Math.max(value, low), Math.max(low, high));
}

/** The size a freshly opened panel takes: roomy, but never larger than the window it floats in. */
export function preferredSize(viewport: Size): Size {
  return {
    width: clamp(Math.round(viewport.width * 0.62), minimumSize.width, viewport.width - 2 * 8),
    height: clamp(Math.round(viewport.height * 0.7), minimumSize.height, viewport.height - 2 * 8),
  };
}

/** Places a panel of the given size against one corner of the viewport. */
export function cornerRect(corner: DevtoolsCorner, viewport: Size, size: Size): Rect {
  const width = clamp(size.width, minimumSize.width, viewport.width);
  const height = clamp(size.height, minimumSize.height, viewport.height);
  const right = corner.endsWith("right");
  const bottom = corner.startsWith("bottom");
  return clampToViewport(
    {
      x: right ? viewport.width - width - openMargin : openMargin,
      y: bottom ? viewport.height - height - openMargin : openMargin,
      width,
      height,
    },
    viewport,
  );
}

/**
 * Keeps the whole panel on screen. Size is clamped first so a panel restored from a larger
 * monitor shrinks to fit rather than hanging off the edge with its controls unreachable.
 */
export function clampToViewport(rect: Rect, viewport: Size): Rect {
  const width = clamp(rect.width, minimumSize.width, Math.max(viewport.width, minimumSize.width));
  const height = clamp(
    rect.height,
    minimumSize.height,
    Math.max(viewport.height, minimumSize.height),
  );
  return {
    width,
    height,
    x: clamp(rect.x, 0, viewport.width - width),
    y: clamp(rect.y, 0, viewport.height - height),
  };
}

/** Drag: the panel moves, its size never changes, and it stops at the viewport edges. */
export function moveBy(rect: Rect, dx: number, dy: number, viewport: Size): Rect {
  return clampToViewport({ ...rect, x: rect.x + dx, y: rect.y + dy }, viewport);
}

/** Resize from the bottom-right grip: the origin is fixed, so only the size changes. */
export function resizeBy(rect: Rect, dx: number, dy: number, viewport: Size): Rect {
  return {
    x: rect.x,
    y: rect.y,
    width: clamp(
      rect.width + dx,
      minimumSize.width,
      Math.max(viewport.width - rect.x, minimumSize.width),
    ),
    height: clamp(
      rect.height + dy,
      minimumSize.height,
      Math.max(viewport.height - rect.y, minimumSize.height),
    ),
  };
}

/** A stored rect, or undefined when nothing usable is saved. Bad JSON is treated as absent. */
export function parseRect(raw: string | null): Rect | undefined {
  if (raw === null) return undefined;
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (typeof value !== "object" || value === null) return undefined;
  const candidate = value as Record<string, unknown>;
  const numbers = (["x", "y", "width", "height"] as const).map((key) => candidate[key]);
  if (!numbers.every((entry) => typeof entry === "number" && Number.isFinite(entry))) {
    return undefined;
  }
  const [x, y, width, height] = numbers as [number, number, number, number];
  return { x, y, width, height };
}
