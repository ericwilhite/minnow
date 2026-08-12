import { el, iconButton, icon, icons } from "../dom.js";
import type { ResolvedDevtoolsOptions } from "../options.js";
import {
  clampToViewport,
  cornerRect,
  moveBy,
  parseRect,
  preferredSize,
  resizeBy,
  type Rect,
  type Size,
} from "./window.js";

export interface PanelView {
  id: string;
  label: string;
  node: HTMLElement;
  /** Called the first time the view is shown, for work worth deferring until then. */
  onFirstShow?(): void;
}

export interface PanelDeps {
  options: ResolvedDevtoolsOptions;
  /** Shown as a badge, so a query that freezes the page explains itself. */
  offMainThread: boolean;
  /** The views the panel switches between, in tab order. The first one starts selected. */
  views: readonly PanelView[];
  /** The confirmation layer, stacked above the content inside the panel. */
  overlay: HTMLElement;
  onClose(): void;
}

export interface Panel {
  node: HTMLElement;
  layout(): void;
  destroy(): void;
}

function viewport(): Size {
  return { width: window.innerWidth, height: window.innerHeight };
}

function readStoredRect(storageKey: string): Rect | undefined {
  try {
    return parseRect(localStorage.getItem(`${storageKey}:rect`));
  } catch {
    // Storage can be unavailable (private mode, sandboxed frame); geometry just isn't remembered.
    return undefined;
  }
}

function writeStoredRect(storageKey: string, rect: Rect): void {
  try {
    localStorage.setItem(`${storageKey}:rect`, JSON.stringify(rect));
  } catch {
    // Same: losing the saved position is not worth failing a drag over.
  }
}

/**
 * The floating window: a title bar you drag by, a corner grip you resize from, and geometry that
 * survives a reload. It is deliberately not modal — nothing here covers or blocks the host page,
 * so the app under the panel keeps working while the panel is open.
 */
export function createPanel(deps: PanelDeps): Panel {
  const { options } = deps;
  const floating = options.mode === "launcher";

  const title = el("span", { class: "title", text: "Minnow devtools" });
  const threadBadge = el("span", { class: deps.offMainThread ? "badge ok" : "badge warn" }, [
    el("span", { class: "dot" }),
    el("span", { text: deps.offMainThread ? "worker" : "main thread" }),
  ]);
  threadBadge.title = deps.offMainThread
    ? "Queries run in a worker; the page stays responsive."
    : "Queries run on the main thread, so a slow one will freeze the page.";
  const writeBadge = el("span", {
    class: options.write ? "badge" : "badge warn",
    text: options.write ? "write on" : "read-only",
  });
  writeBadge.title = options.write
    ? "Statements that change data run after you confirm them."
    : "permissions.write is off; statements that change data are refused.";

  const shown = new Set<string>();
  const tabs = el("div", { class: "tabs", attrs: { role: "tablist" } });
  const buttons = deps.views.map((view) => {
    const tab = el("button", {
      class: "tab",
      type: "button",
      text: view.label,
      attrs: { role: "tab", "aria-selected": "false" },
    });
    tab.addEventListener("click", () => {
      select(view.id);
    });
    return tab;
  });
  tabs.append(...buttons);

  /** Only the selected view is in the layout; the others keep their state but take no space. */
  function select(id: string): void {
    deps.views.forEach((view, index) => {
      const active = view.id === id;
      view.node.hidden = !active;
      buttons[index]?.classList.toggle("on", active);
      buttons[index]?.setAttribute("aria-selected", String(active));
      if (active && !shown.has(view.id)) {
        shown.add(view.id);
        view.onFirstShow?.();
      }
    });
  }

  const close = iconButton("winbtn", "Close devtools", icons.close);
  const titlebar = el("div", { class: "titlebar" }, [
    el("span", { class: "mark" }, [icon(icons.fish)]),
    title,
    tabs,
    el("span", { class: "spacer" }),
    threadBadge,
    writeBadge,
  ]);
  if (floating) titlebar.append(close);

  const grip = el("span", { class: "grip", attrs: { "aria-hidden": "true" } }, [icon(icons.grip)]);
  const node = el("div", { class: `panel ${floating ? "floating" : "inline"}` }, [
    titlebar,
    ...deps.views.map((view) => view.node),
    deps.overlay,
  ]);
  if (floating) node.append(grip);
  node.style.zIndex = String(options.zIndex);

  close.addEventListener("click", () => {
    deps.onClose();
  });

  let rect: Rect | undefined;
  let frame = 0;

  function paint(): void {
    if (rect === undefined || !floating) return;
    node.style.left = `${String(rect.x)}px`;
    node.style.top = `${String(rect.y)}px`;
    node.style.width = `${String(rect.width)}px`;
    node.style.height = `${String(rect.height)}px`;
  }

  function schedulePaint(): void {
    if (frame !== 0) return;
    frame = requestAnimationFrame(() => {
      frame = 0;
      paint();
    });
  }

  /** Positions the panel: the geometry it was left in, else against the configured corner. */
  function layout(): void {
    if (!floating) return;
    const bounds = viewport();
    rect ??=
      readStoredRect(options.storageKey) ??
      cornerRect(options.corner, bounds, preferredSize(bounds));
    rect = clampToViewport(rect, bounds);
    paint();
  }

  /**
   * One gesture handler for both the drag and the resize: they differ only in which transform
   * they apply to the rect.
   *
   * The move and end listeners go on the window rather than the handle, so the gesture survives
   * the pointer leaving the title bar — which it does immediately on any real drag. Pointer
   * capture is a best-effort addition on top: it keeps the host page's elements from reacting to
   * a pointer that is mid-drag, but it throws for a pointer the browser does not consider active,
   * and losing it must not cost the drag.
   */
  function gesture(
    handle: HTMLElement,
    apply: (start: Rect, dx: number, dy: number, bounds: Size) => Rect,
    activeClass?: string,
  ): void {
    handle.addEventListener("pointerdown", (event: PointerEvent) => {
      if (event.button !== 0 || rect === undefined) return;
      const start = rect;
      const originX = event.clientX;
      const originY = event.clientY;
      const bounds = viewport();
      const { pointerId } = event;
      let captured = false;
      try {
        handle.setPointerCapture(pointerId);
        captured = true;
      } catch {
        // Not an active pointer; the window listeners below carry the gesture regardless.
      }
      if (activeClass !== undefined) handle.classList.add(activeClass);
      event.preventDefault();

      const move = (moved: PointerEvent): void => {
        rect = apply(start, moved.clientX - originX, moved.clientY - originY, bounds);
        schedulePaint();
      };
      const end = (): void => {
        window.removeEventListener("pointermove", move);
        window.removeEventListener("pointerup", end);
        window.removeEventListener("pointercancel", end);
        if (captured) handle.releasePointerCapture(pointerId);
        if (activeClass !== undefined) handle.classList.remove(activeClass);
        if (rect !== undefined) writeStoredRect(options.storageKey, rect);
      };
      window.addEventListener("pointermove", move);
      window.addEventListener("pointerup", end);
      window.addEventListener("pointercancel", end);
    });
  }

  if (floating) {
    gesture(titlebar, (start, dx, dy, bounds) => moveBy(start, dx, dy, bounds), "dragging");
    gesture(grip, (start, dx, dy, bounds) => resizeBy(start, dx, dy, bounds));
  }

  const onWindowResize = (): void => {
    if (rect === undefined) return;
    rect = clampToViewport(rect, viewport());
    schedulePaint();
  };
  window.addEventListener("resize", onWindowResize);
  select(deps.views[0]?.id ?? "");

  return {
    node,
    layout,
    destroy: () => {
      window.removeEventListener("resize", onWindowResize);
      if (frame !== 0) cancelAnimationFrame(frame);
    },
  };
}
