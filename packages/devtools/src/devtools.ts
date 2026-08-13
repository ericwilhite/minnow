import { createConfirmLayer } from "./confirm.js";
import { createConsole } from "./console.js";
import { createExplorer } from "./explorer/explorer.js";
import { matchesHotkey } from "./hotkey.js";
import { resolveOptions, type DevtoolsOptions } from "./options.js";
import { createLauncher } from "./panel/launcher.js";
import { createPanel } from "./panel/panel.js";
import { styles } from "./styles.js";
import { resolveTarget, runsOffMainThread, type DevtoolsAttachable } from "./target.js";

export interface DevtoolsHandle {
  open(): void;
  close(): void;
  toggle(): void;
  readonly isOpen: boolean;
  /** Removes every listener and empties the root. Safe to call twice. */
  destroy(): void;
}

/** Constructable stylesheets where available, a `<style>` node where not. */
function adoptStyles(root: ShadowRoot): void {
  if (typeof CSSStyleSheet === "function" && "adoptedStyleSheets" in root) {
    try {
      const sheet = new CSSStyleSheet();
      sheet.replaceSync(styles);
      root.adoptedStyleSheets = [...root.adoptedStyleSheets, sheet];
      return;
    } catch {
      // Fall through to the style element below.
    }
  }
  const node = document.createElement("style");
  node.textContent = styles;
  root.append(node);
}

/**
 * Builds the devtools inside a shadow root. Both entry points — the custom element and
 * `mountMinnowDevtools` — land here, so there is one implementation of the panel and one place
 * where the target is resolved and the options are read.
 */
export function createDevtools(
  root: ShadowRoot,
  attachable: DevtoolsAttachable,
  options: DevtoolsOptions = {},
): DevtoolsHandle {
  const resolved = resolveOptions(options);
  const target = resolveTarget(attachable);
  adoptStyles(root);

  const confirm = createConfirmLayer();
  const explorer = createExplorer({ target, confirm, write: resolved.write });
  const view = createConsole({
    target,
    confirm,
    root,
    write: resolved.write,
    initialQuery: resolved.initialQuery,
    storageKey: resolved.storageKey,
  });
  const panel = createPanel({
    options: resolved,
    offMainThread: runsOffMainThread(target),
    views: [
      {
        id: "data",
        label: "Data",
        node: explorer.node,
        // The catalog is only read once the tab is actually looked at.
        onFirstShow: () => {
          void explorer.refresh();
        },
      },
      {
        id: "query",
        label: "Query",
        node: view.node,
        // CodeMirror and the catalog are fetched here, so the panel opens without them.
        onFirstShow: () => {
          void view.upgrade();
        },
      },
    ],
    overlay: confirm.node,
    onClose: () => {
      close();
    },
  });

  const launcher =
    resolved.mode === "launcher"
      ? createLauncher(resolved.corner, resolved.zIndex, () => {
          toggle();
        })
      : undefined;

  if (launcher !== undefined) root.append(launcher.node);
  root.append(panel.node);

  let open = false;
  let destroyed = false;

  function setOpen(next: boolean): void {
    if (destroyed || open === next) return;
    open = next;
    panel.node.hidden = !next;
    launcher?.setOpen(next);
    if (next) {
      panel.layout();
      view.focus();
    } else {
      confirm.dismiss();
    }
  }

  function close(): void {
    setOpen(false);
  }

  function toggle(): void {
    setOpen(!open);
  }

  const onKeyDown = (event: KeyboardEvent): void => {
    if (!matchesHotkey(resolved.hotkey, event)) return;
    event.preventDefault();
    toggle();
  };
  if (resolved.mode === "launcher" && resolved.hotkey.length > 0) {
    window.addEventListener("keydown", onKeyDown);
  }

  panel.node.hidden = true;
  launcher?.setOpen(false);
  if (resolved.defaultOpen) setOpen(true);

  return {
    open: () => {
      setOpen(true);
    },
    close,
    toggle,
    get isOpen() {
      return open;
    },
    destroy: () => {
      if (destroyed) return;
      destroyed = true;
      window.removeEventListener("keydown", onKeyDown);
      confirm.dismiss();
      panel.destroy();
      root.replaceChildren();
    },
  };
}
