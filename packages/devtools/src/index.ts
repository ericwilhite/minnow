import { createDevtools, type DevtoolsHandle } from "./devtools.js";
import type { DevtoolsOptions, DevtoolsTheme } from "./options.js";
import type { DevtoolsAttachable } from "./target.js";

export { createDevtools, type DevtoolsHandle } from "./devtools.js";
export { defineMinnowDevtools, elementName, MinnowDevtoolsElement } from "./element.js";
export type {
  DevtoolsCorner,
  DevtoolsMode,
  DevtoolsOptions,
  DevtoolsPermissions,
  DevtoolsTheme,
} from "./options.js";
export type { DevtoolsAttachable, DevtoolsTarget } from "./target.js";

export interface MountOptions extends DevtoolsOptions {
  /**
   * Where the panel lives in the page. Defaults to a fresh element appended to `<body>`, which
   * is what a floating launcher wants; pass a container for `mode: "inline"`.
   */
  container?: Element;
}

export interface MountedDevtools extends DevtoolsHandle {
  /** The host element. `destroy()` removes it when the mount created it. */
  readonly element: Element;
}

/**
 * Mounts the devtools without touching the custom element registry — the plain-JavaScript entry
 * point, and the one to use when a page needs two panels or a name other than
 * `<minnow-devtools>`.
 *
 * ```ts
 * const devtools = mountMinnowDevtools(db, { corner: "bottom-left" });
 * ```
 */
export function mountMinnowDevtools(
  target: DevtoolsAttachable,
  options: MountOptions = {},
): MountedDevtools {
  const { container, ...rest } = options;
  const owned = container === undefined;
  const host = container ?? document.createElement("div");
  if (owned) document.body.append(host);

  const root = host.shadowRoot ?? host.attachShadow({ mode: "open" });
  const handle = createDevtools(root, target, rest);

  return {
    open: () => {
      handle.open();
    },
    setQuery: (sql: string) => {
      handle.setQuery(sql);
    },
    setTheme: (theme: DevtoolsTheme) => {
      handle.setTheme(theme);
    },
    close: () => {
      handle.close();
    },
    toggle: () => {
      handle.toggle();
    },
    get isOpen() {
      return handle.isOpen;
    },
    element: host,
    destroy: () => {
      handle.destroy();
      if (owned) host.remove();
    },
  };
}
