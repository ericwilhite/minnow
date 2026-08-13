/**
 * Small element helpers. The panel builds its DOM node by node rather than from HTML strings:
 * nothing the database returns is ever parsed as markup, and every listener survives a rerender
 * because nodes are updated in place instead of replaced wholesale.
 */

export interface ElementSpec {
  class?: string;
  text?: string;
  title?: string;
  type?: string;
  attrs?: Record<string, string>;
}

export function el<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  spec: ElementSpec = {},
  children: ReadonlyArray<Node | string> = [],
): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  if (spec.class !== undefined) node.className = spec.class;
  if (spec.text !== undefined) node.textContent = spec.text;
  if (spec.title !== undefined) node.title = spec.title;
  if (spec.type !== undefined) node.setAttribute("type", spec.type);
  for (const [name, value] of Object.entries(spec.attrs ?? {})) node.setAttribute(name, value);
  for (const child of children) node.append(child);
  return node;
}

/** A button that never submits a surrounding form and never steals focus styling from the host. */
export function button(
  className: string,
  label: string,
  spec: ElementSpec = {},
): HTMLButtonElement {
  return el("button", { ...spec, class: className, type: "button", text: label });
}

export function iconButton(className: string, label: string, path: string): HTMLButtonElement {
  const node = el("button", { class: className, type: "button", attrs: { "aria-label": label } });
  node.title = label;
  node.append(icon(path));
  return node;
}

/** Icons are single-path outlines; `path` is the `d` attribute. */
export function icon(path: string): SVGSVGElement {
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("fill", "none");
  svg.setAttribute("stroke", "currentColor");
  svg.setAttribute("stroke-width", "2");
  svg.setAttribute("stroke-linecap", "round");
  svg.setAttribute("stroke-linejoin", "round");
  svg.setAttribute("aria-hidden", "true");
  const node = document.createElementNS("http://www.w3.org/2000/svg", "path");
  node.setAttribute("d", path);
  svg.append(node);
  return svg;
}

export const icons = {
  table: "M4 5h16v14H4zM4 10h16M10 10v9",
  chevronLeft: "M15 6l-6 6 6 6",
  chevronRight: "M9 6l6 6-6 6",
  chevronDown: "M6 9l6 6 6-6",
  close: "M6 6l12 12M18 6L6 18",
  maximize: "M5 5h14v14H5z",
  restore: "M9 9h10v10H9zM5 15V5h10",
  split: "M4 12h16",
  refresh: "M20 11a8 8 0 10-2 6M20 5v6h-6",
  check: "M5 13l4 4L19 7",
  minimize: "M6 12h12",
  play: "M6 4l14 8-14 8V4z",
  grip: "M21 9L9 21M21 15l-6 6",
  fish: "M17 12l4-3v6l-4-3M3 12c0-2.8 3.1-5 7-5s7 2.2 7 5-3.1 5-7 5-7-2.2-7-5z",
  warning: "M12 3l9 17H3l9-17zM12 10v4M12 17.2v.1",
} as const;

export function clear(node: Element): void {
  node.replaceChildren();
}
