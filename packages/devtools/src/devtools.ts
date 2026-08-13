import { createConfirmLayer } from "./confirm.js";
import { createConsole } from "./console.js";
import { toCatalog } from "./explorer/catalog.js";
import { createExplorer } from "./explorer/explorer.js";
import { createSchemaRail } from "./explorer/tree.js";
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

  /**
   * The rail is shared, so what it does depends on which view is open: while writing a query it
   * contributes names to the text, and while browsing it opens the table.
   */
  const rail = createSchemaRail({
    storageKey: resolved.storageKey,
    onPickTable: (table) => {
      if (panel.activeView() === "query") view.insert(table.name);
      else void explorer.open(table.name);
    },
    onPickColumn: (table, column) => {
      if (panel.activeView() === "query") view.insert(`${table.name}.${column.name}`);
      else void explorer.open(table.name);
    },
  });

  const panel = createPanel({
    options: resolved,
    offMainThread: runsOffMainThread(target),
    rail: rail.node,
    // The console leads: writing a query is what the panel is opened for most often.
    views: [
      {
        id: "query",
        label: "Query",
        node: view.node,
        // CodeMirror is fetched here, so the panel opens without it.
        onFirstShow: () => {
          void view.upgrade();
        },
      },
      { id: "data", label: "Data", node: explorer.node },
    ],
    overlay: confirm.node,
    onClose: () => {
      close();
    },
  });

  /** Read once for the whole panel: the rail, the explorer, and completion share one catalog. */
  async function loadCatalog(): Promise<void> {
    try {
      const catalog = toCatalog(await target.listTables());
      rail.setCatalog(catalog);
      view.setSchema(
        Object.fromEntries(
          catalog.map((table) => [table.name, table.columns.map((column) => column.name)]),
        ),
      );
      await explorer.setCatalog(catalog);
    } catch (error) {
      rail.setError(error instanceof Error ? error.message : String(error));
    }
  }

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
  /** The catalog is read when the panel is first opened, never at mount. */
  let catalogLoaded = false;

  function setOpen(next: boolean): void {
    if (destroyed || open === next) return;
    open = next;
    panel.node.hidden = !next;
    launcher?.setOpen(next);
    if (next) {
      panel.layout();
      if (!catalogLoaded) {
        catalogLoaded = true;
        void loadCatalog();
      }
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
