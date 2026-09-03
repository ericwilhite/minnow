import { el } from "./dom.js";

export interface MenuItem {
  label: string;
  /** Shown after the label, faint: the value the action uses. */
  hint?: string;
  danger?: boolean;
  run(): void;
}

export interface Menu {
  node: HTMLElement;
  /** Opens at a point in viewport coordinates, kept inside the container it lives in. */
  open(items: readonly MenuItem[], at: { x: number; y: number }): void;
  close(): void;
}

/**
 * A context menu inside the panel. It is one element that is repositioned and refilled on every
 * open, and it closes on a click anywhere, on Escape, and on losing focus, so it can never be
 * left hanging over the grid.
 */
export function createMenu(): Menu {
  const node = el("div", { class: "menu", attrs: { role: "menu", tabindex: "-1" } });
  node.hidden = true;

  function close(): void {
    if (node.hidden) return;
    node.hidden = true;
    node.replaceChildren();
  }

  node.addEventListener("keydown", (event) => {
    const items = [...node.querySelectorAll<HTMLButtonElement>(".menu-item")];
    const at = items.indexOf(document.activeElement as HTMLButtonElement);
    if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      close();
    } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const step = event.key === "ArrowDown" ? 1 : -1;
      items[(at + step + items.length) % items.length]?.focus();
    }
  });
  node.addEventListener("focusout", (event) => {
    if (!(event.relatedTarget instanceof Node) || !node.contains(event.relatedTarget)) close();
  });

  return {
    node,
    close,
    open: (items, at) => {
      node.replaceChildren(
        ...items.map((item) => {
          const button = el(
            "button",
            {
              class: `menu-item${item.danger === true ? " danger" : ""}`,
              type: "button",
              attrs: { role: "menuitem" },
            },
            [
              el("span", { text: item.label }),
              ...(item.hint === undefined
                ? []
                : [el("span", { class: "menu-hint", text: item.hint })]),
            ],
          );
          button.addEventListener("click", () => {
            close();
            item.run();
          });
          return button;
        }),
      );
      node.hidden = false;
      // Placed relative to the positioned ancestor, then pulled back inside it if it would
      // overflow; the menu has to be in the layout to know its own size.
      const parent = node.offsetParent ?? node.parentElement;
      const bounds = parent?.getBoundingClientRect() ?? { left: 0, top: 0, width: 0, height: 0 };
      const width = node.offsetWidth;
      const height = node.offsetHeight;
      const x = Math.max(0, Math.min(at.x - bounds.left, bounds.width - width - 4));
      const y = Math.max(0, Math.min(at.y - bounds.top, bounds.height - height - 4));
      node.style.left = `${String(Math.round(x))}px`;
      node.style.top = `${String(Math.round(y))}px`;
      node.querySelector<HTMLButtonElement>(".menu-item")?.focus();
    },
  };
}
