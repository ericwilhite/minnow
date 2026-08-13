import type { DevtoolsCorner } from "../options.js";

export interface Size {
  width: number;
  height: number;
}

export interface Rect extends Size {
  x: number;
  y: number;
}

/**
 * Below this the panel stops being usable. The width has to carry three columns — the schema rail,
 * the view, and the history — so it is the sum of their minimums rather than a round number.
 */
export const minimumSize: Size = { width: 720, height: 320 };

/** Gap between the panel and the viewport edge when it first opens. */
const openMargin = 24;

function clamp(value: number, low: number, high: number): number {
  return Math.min(Math.max(value, low), Math.max(low, high));
}

/** The size a freshly opened panel takes: roomy, but never larger than the window it floats in. */
export function preferredSize(viewport: Size): Size {
  return {
    width: clamp(Math.round(viewport.width * 0.72), minimumSize.width, viewport.width - 2 * 8),
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
    width = clamp(width + dx, minimumSize.width, Math.max(viewport.width - x, minimumSize.width));
  } else if (edge.includes("w")) {
    const right = x + width;
    x = clamp(x + dx, 0, right - minimumSize.width);
    width = right - x;
  }

  if (edge.includes("s")) {
    height = clamp(
      height + dy,
      minimumSize.height,
      Math.max(viewport.height - y, minimumSize.height),
    );
  } else if (edge.includes("n")) {
    const bottom = y + height;
    y = clamp(y + dy, 0, bottom - minimumSize.height);
    height = bottom - y;
  }

  return { x, y, width, height };
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
