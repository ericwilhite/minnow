import { el, icon, icons } from "../dom.js";
import type { DevtoolsCorner } from "../options.js";

export interface Launcher {
  node: HTMLElement;
  setOpen(open: boolean): void;
}

const margin = "18px";

/** The floating button. It sits in one corner and toggles the panel; that is all it does. */
export function createLauncher(
  corner: DevtoolsCorner,
  zIndex: number,
  onToggle: () => void,
): Launcher {
  const node = el("button", {
    class: "launcher",
    type: "button",
    attrs: { "aria-label": "Open Minnow devtools", "aria-expanded": "false" },
  });
  node.title = "Minnow devtools";
  node.append(icon(icons.fish));

  node.style.zIndex = String(zIndex);
  node.style[corner.startsWith("bottom") ? "bottom" : "top"] = margin;
  node.style[corner.endsWith("right") ? "right" : "left"] = margin;

  node.addEventListener("click", onToggle);

  return {
    node,
    setOpen: (open) => {
      node.setAttribute("aria-expanded", String(open));
      node.setAttribute("aria-label", open ? "Close Minnow devtools" : "Open Minnow devtools");
      // Hidden while open: the panel has its own close button, and two controls for one state
      // is one too many.
      node.hidden = open;
    },
  };
}
