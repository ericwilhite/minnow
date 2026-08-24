import type { DevtoolsCorner } from "../options.js";

export interface Size {
  width: number;
  height: number;
}

export interface Rect extends Size {
  x: number;
  y: number;
}

/** Preferred drag floor when the viewport has room; a smaller viewport always wins. */
export const minimumSize: Size = { width: 500, height: 320 };

/** Gap between the panel and the viewport edge when it first opens. */
const openMargin = 24;

function clamp(value: number, low: number, high: number): number {
  return Math.min(Math.max(value, low), Math.max(low, high));
}

/** The size a freshly opened panel takes: roomy, but never larger than the window it floats in. */
export function preferredSize(viewport: Size): Size {
  const available = {
    width: Math.max(1, viewport.width - 2 * 8),
    height: Math.max(1, viewport.height - 2 * 8),
  };
  return {
    width: clamp(
      Math.round(viewport.width * 0.72),
      Math.min(minimumSize.width, available.width),
      available.width,
    ),
    height: clamp(
      Math.round(viewport.height * 0.7),
      Math.min(minimumSize.height, available.height),
      available.height,
    ),
  };
}

/** Places a panel of the given size against one corner of the viewport. */
export function cornerRect(corner: DevtoolsCorner, viewport: Size, size: Size): Rect {
  const width = clamp(size.width, Math.min(minimumSize.width, viewport.width), viewport.width);
  const height = clamp(size.height, Math.min(minimumSize.height, viewport.height), viewport.height);
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
  const width = clamp(rect.width, Math.min(minimumSize.width, viewport.width), viewport.width);
  const height = clamp(rect.height, Math.min(minimumSize.height, viewport.height), viewport.height);
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

/** Which boundary is being dragged. The compass letters are the edges the handle sits on. */
export type ResizeEdge = "n" | "s" | "e" | "w" | "ne" | "nw" | "se" | "sw";

/**
 * Resize from any edge or corner.
 *
 * The two directions are independent, and each has two cases: dragging the right or bottom moves
 * only the size, while dragging the left or top moves the origin and the size together so the
 * opposite edge stays put. The minimum is what stops a leading edge from crossing its partner.
 */
export function resizeBy(
  rect: Rect,
  edge: ResizeEdge,
  dx: number,
  dy: number,
  viewport: Size,
): Rect {
  let { x, y, width, height } = rect;

  if (edge.includes("e")) {
    const available = viewport.width - x;
    width = clamp(width + dx, Math.min(minimumSize.width, available), available);
  } else if (edge.includes("w")) {
    const right = x + width;
    const minimum = Math.min(minimumSize.width, right);
    x = clamp(x + dx, 0, right - minimum);
    width = right - x;
  }

  if (edge.includes("s")) {
    const available = viewport.height - y;
    height = clamp(height + dy, Math.min(minimumSize.height, available), available);
  } else if (edge.includes("n")) {
    const bottom = y + height;
    const minimum = Math.min(minimumSize.height, bottom);
    y = clamp(y + dy, 0, bottom - minimum);
    height = bottom - y;
  }

  return { x, y, width, height };
}

/** The whole viewport, which is what maximizing fills. */
export function maximizedRect(viewport: Size): Rect {
  return { x: 0, y: 0, width: viewport.width, height: viewport.height };
}

/**
 * A horizontal split: how tall the top pane should be after dragging the divider. Both panes keep
 * a minimum, so the divider stops rather than collapsing either one to nothing — and if the
 * container is too short to honour both, the top pane gives way first.
 */
export function resizeSplit(
  topHeight: number,
  delta: number,
  containerHeight: number,
  minimums: { top: number; bottom: number },
): number {
  const highest = containerHeight - minimums.bottom;
  return clamp(topHeight + delta, minimums.top, Math.max(highest, minimums.top));
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
