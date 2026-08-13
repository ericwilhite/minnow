import { el, iconButton, icon, icons } from "../dom.js";
import type { ResolvedDevtoolsOptions } from "../options.js";
import { readFlag, writeFlag } from "../storage.js";
import { draggable } from "./drag.js";
import {
  clampToViewport,
  cornerRect,
  maximizedRect,
  moveBy,
  parseRect,
  preferredSize,
  resizeBy,
  type Rect,
  type ResizeEdge,
  type Size,
} from "./window.js";

const resizeEdges: readonly ResizeEdge[] = ["n", "s", "e", "w", "ne", "nw", "sw"];

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
  /** Always-visible column to the left of the views. */
  rail?: HTMLElement;
  /** The confirmation layer, stacked above the content inside the panel. */
  overlay: HTMLElement;
  onClose(): void;
}

export interface Panel {
  node: HTMLElement;
  layout(): void;
  /** Which view is showing, so shared chrome can act on the right one. */
  activeView(): string;
  show(id: string): void;
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

  let active = deps.views[0]?.id ?? "";

  /** Only the selected view is in the layout; the others keep their state but take no space. */
  function select(id: string): void {
    active = id;
    deps.views.forEach((view, index) => {
      const selected = view.id === id;
      view.node.hidden = !selected;
      buttons[index]?.classList.toggle("on", selected);
      buttons[index]?.setAttribute("aria-selected", String(selected));
      if (selected && !shown.has(view.id)) {
        shown.add(view.id);
        view.onFirstShow?.();
      }
    });
  }

  const maximize = iconButton("winbtn", "Maximize", icons.maximize);
  const close = iconButton("winbtn", "Close devtools", icons.close);
  const titlebar = el("div", { class: "titlebar" }, [
    el("span", { class: "mark" }, [icon(icons.fish)]),
    title,
    tabs,
    el("span", { class: "spacer" }),
    threadBadge,
    writeBadge,
  ]);
  if (floating) titlebar.append(maximize, close);

  const grip = el("span", { class: "grip", attrs: { "aria-hidden": "true" } }, [icon(icons.grip)]);
  // The rail sits beside every view rather than inside one, because the schema is as useful for
  // writing a query as it is for browsing a table.
  const body = el("div", { class: "panel-body" }, [
    ...(deps.rail === undefined ? [] : [deps.rail]),
    el(
      "div",
      { class: "views" },
      deps.views.map((view) => view.node),
    ),
  ]);
  const node = el("div", { class: `panel ${floating ? "floating" : "inline"}` }, [
    titlebar,
    body,
    deps.overlay,
  ]);
  if (floating) node.append(grip);
  node.style.zIndex = String(options.zIndex);

  close.addEventListener("click", () => {
    deps.onClose();
  });
  maximize.addEventListener("click", () => {
    setMaximized(!maximized);
  });
  // Double-clicking the title bar toggles too, the way a window manager does.
  titlebar.addEventListener("dblclick", (event) => {
    if (event.target instanceof Element && event.target.closest("button, input, select, a")) return;
    setMaximized(!maximized);
  });

  let rect: Rect | undefined;
  let frame = 0;
  const maximizedKey = `${options.storageKey}:maximized`;
  let maximized = floating && readFlag(maximizedKey, false);
  /** Where to go back to. The stored rect keeps the real geometry, never the maximized one. */
  let restoreRect: Rect | undefined;

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
    restoreRect ??=
      readStoredRect(options.storageKey) ??
      cornerRect(options.corner, bounds, preferredSize(bounds));
    restoreRect = clampToViewport(restoreRect, bounds);
    rect = maximized ? maximizedRect(bounds) : restoreRect;
    applyMaximizedChrome();
    paint();
  }

  function applyMaximizedChrome(): void {
    node.classList.toggle("maximized", maximized);
    maximize.title = maximized ? "Restore" : "Maximize";
    maximize.setAttribute("aria-label", maximize.title);
    maximize.replaceChildren(icon(maximized ? icons.restore : icons.maximize));
  }

  /**
   * Maximizing keeps the window's own geometry aside rather than overwriting it, so restoring
   * returns to the size it was actually left at — including across a reload.
   */
  function setMaximized(next: boolean): void {
    if (!floating || maximized === next) return;
    if (next && rect !== undefined) restoreRect = rect;
    maximized = next;
    writeFlag(maximizedKey, maximized);
    const bounds = viewport();
    rect = maximized
      ? maximizedRect(bounds)
      : clampToViewport(restoreRect ?? maximizedRect(bounds), bounds);
    applyMaximizedChrome();
    paint();
  }

  /**
   * Moving and resizing differ only in which transform they apply to the rect. Neither is offered
   * while maximized: the window fills the screen, so there is nowhere to drag it to, and the
   * restore control is right there in the title bar.
   */
  function gesture(
    handle: HTMLElement,
    apply: (start: Rect, dx: number, dy: number, bounds: Size) => Rect,
    activeClass?: string,
  ): void {
    let start: Rect | undefined;
    let bounds: Size = { width: 0, height: 0 };
    draggable(handle, {
      onStart: () => {
        if (rect === undefined || maximized) return false;
        start = rect;
        bounds = viewport();
        if (activeClass !== undefined) handle.classList.add(activeClass);
        return true;
      },
      onMove: (dx, dy) => {
        if (start === undefined) return;
        rect = apply(start, dx, dy, bounds);
        schedulePaint();
      },
      onEnd: () => {
        if (activeClass !== undefined) handle.classList.remove(activeClass);
        if (rect !== undefined) writeStoredRect(options.storageKey, rect);
      },
    });
  }

  if (floating) {
    gesture(titlebar, (start, dx, dy, bounds) => moveBy(start, dx, dy, bounds), "dragging");
    // Every boundary resizes, and the corner grip stays as the obvious one.
    gesture(grip, (start, dx, dy, bounds) => resizeBy(start, "se", dx, dy, bounds));
    for (const edge of resizeEdges) {
      const handle = el("span", {
        class: `resize-edge resize-${edge}`,
        attrs: { "aria-hidden": "true" },
      });
      node.append(handle);
      gesture(handle, (start, dx, dy, bounds) => resizeBy(start, edge, dx, dy, bounds));
    }
  }

  const onWindowResize = (): void => {
    if (rect === undefined) return;
    const bounds = viewport();
    rect = maximized ? maximizedRect(bounds) : clampToViewport(rect, bounds);
    if (!maximized) restoreRect = rect;
    schedulePaint();
  };
  window.addEventListener("resize", onWindowResize);
  select(deps.views[0]?.id ?? "");

  return {
    node,
    layout,
    activeView: () => active,
    show: select,
    destroy: () => {
      window.removeEventListener("resize", onWindowResize);
      if (frame !== 0) cancelAnimationFrame(frame);
    },
  };
}
