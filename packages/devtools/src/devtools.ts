import { createConfirmLayer } from "./confirm.js";
import { createConsole } from "./console.js";
import { toCatalog } from "./explorer/catalog.js";
import { createExplorer } from "./explorer/explorer.js";
import { createSchemaRail } from "./explorer/tree.js";
import { matchesHotkey } from "./hotkey.js";
import { resolveOptions, type DevtoolsOptions, type DevtoolsTheme } from "./options.js";
import { createLauncher } from "./panel/launcher.js";
import { createPanel, type PanelView } from "./panel/panel.js";
import { createSnapshotActions } from "./panel/snapshot.js";
import { createStorageView, isStorageTarget } from "./panel/storage-view.js";
import { sqlColumn, sqlIdentifier } from "./sql/literal.js";
import { styles } from "./styles.js";
import {
  isSnapshotTarget,
  resolveTarget,
  runsOffMainThread,
  type DevtoolsAttachable,
} from "./target.js";

export interface DevtoolsHandle {
  open(): void;
  close(): void;
  toggle(): void;
  /**
   * Replaces the statement in the console and shows it, opening the panel if it was closed.
   * This is how an embedder offers a query to start from — a docs page suggesting one, or an
   * application dropping the user into the query behind the screen they were looking at.
   */
  setQuery(sql: string): void;
  /**
   * Repaints in the given palette. A page with its own light/dark switch calls this when the
   * switch is flipped, rather than remounting the panel and losing the query in it.
   */
  setTheme(theme: DevtoolsTheme): void;
  readonly isOpen: boolean;
  /** Removes every listener and empties the root. Safe to call twice. */
  destroy(): void;
}

/** One parsed sheet for every panel on the page; `null` once construction has been refused. */
let sharedSheet: CSSStyleSheet | null | undefined;

/**
 * Constructable stylesheets where available, a `<style>` node where not. The sheet is parsed once
 * and shared, added to a root only when it is not already there, and taken off again on destroy,
 * so a panel that remounts — every attribute change on the element does — never stacks copies.
 * Returns what undoes it.
 */
function adoptStyles(root: ShadowRoot): () => void {
  if (sharedSheet === undefined) {
    sharedSheet = null;
    if (typeof CSSStyleSheet === "function" && "adoptedStyleSheets" in root) {
      try {
        const sheet = new CSSStyleSheet();
        sheet.replaceSync(styles);
        sharedSheet = sheet;
      } catch {
        // Left null: the style element below is used instead.
      }
    }
  }
  const sheet = sharedSheet;
  if (sheet !== null) {
    if (!root.adoptedStyleSheets.includes(sheet)) {
      root.adoptedStyleSheets = [...root.adoptedStyleSheets, sheet];
    }
    return () => {
      root.adoptedStyleSheets = root.adoptedStyleSheets.filter((entry) => entry !== sheet);
    };
  }
  const node = document.createElement("style");
  node.textContent = styles;
  root.append(node);
  return () => {
    node.remove();
  };
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
  const dropStyles = adoptStyles(root);

  // The two things the panel needs from its own host element: which layout it is in, so an inline
  // host can be a block with a height, and which palette to paint in. Both are read by the
  // stylesheet from the host rather than set on it in JavaScript, so the page can override them.
  const host = root.host;
  host.setAttribute("data-minnow-devtools", resolved.mode);

  /**
   * The palette follows the OS unless the page pins it. The stylesheet reads the attribute; the
   * editor is told directly, since CodeMirror's base theme has a light and a dark half of its own.
   */
  const systemDark =
    typeof globalThis.matchMedia === "function"
      ? globalThis.matchMedia("(prefers-color-scheme: dark)")
      : undefined;
  let theme: DevtoolsTheme = resolved.theme;

  function isDark(): boolean {
    if (theme === "dark") return true;
    if (theme === "light") return false;
    return systemDark?.matches ?? false;
  }

  function applyTheme(next: DevtoolsTheme): void {
    theme = next;
    if (next === "system") host.removeAttribute("theme");
    else host.setAttribute("theme", next);
    view.setDark(isDark());
  }

  const onSystemTheme = (): void => {
    if (theme === "system") view.setDark(isDark());
  };
  systemDark?.addEventListener("change", onSystemTheme);

  const confirm = createConfirmLayer();
  const explorer = createExplorer({
    target,
    confirm,
    write: resolved.write,
    storageKey: resolved.storageKey,
    // The explorer is the one place that knows which table is showing, whichever route opened
    // it, so the rail's highlight follows it rather than the click that may have caused it.
    onOpen: (table) => {
      rail.setSelected(table);
    },
    onRunQuery: (sql) => {
      panel.show("query");
      void view.runQuery(sql);
    },
  });
  const view = createConsole({
    target,
    confirm,
    root,
    write: resolved.write,
    initialQuery: resolved.initialQuery,
    storageKey: resolved.storageKey,
    onCatalogChange: () => loadCatalog(),
  });

  /**
   * The rail is shared, so what it does depends on which view is open: while writing a query it
   * contributes names to the text, and while browsing it opens the table.
   */
  const rail = createSchemaRail({
    storageKey: resolved.storageKey,
    onRefresh: () => {
      void loadCatalog();
    },
    onPickTable: (table) => {
      if (panel.activeView() === "query") view.insert(sqlIdentifier(table.name));
      else void explorer.open(table.name);
    },
    onPickColumn: (table, column) => {
      if (panel.activeView() === "query") view.insert(sqlColumn(table.name, column.name));
      else void explorer.open(table.name);
    },
    onPickIndex: (table, index) => {
      if (panel.activeView() === "query") view.insert(sqlIdentifier(index.name));
      else void explorer.open(table.name);
    },
    // A foreign key is a join waiting to be written, or the parent table waiting to be opened.
    onPickForeignKey: (table, key) => {
      if (panel.activeView() === "query") {
        const on = key.columns
          .map(
            (column, index) =>
              `${sqlColumn(table.name, column)} = ${sqlColumn(key.parentTable, key.parentColumns[index] ?? column)}`,
          )
          .join(" AND ");
        view.insert(`JOIN ${sqlIdentifier(key.parentTable)} ON ${on}`);
      } else void explorer.open(key.parentTable);
    },
    onPickViewSql: (table) => {
      if (panel.activeView() === "query") {
        setOpen(true);
        view.setQuery(table.view?.sql ?? "");
      } else void explorer.open(table.name);
    },
  });

  /**
   * Copying the database out and loading one back, when the target can. A target that cannot
   * contributes no controls at all, so the title bar never carries a button that would only ever
   * report that it is unavailable.
   */
  const snapshots = isSnapshotTarget(target)
    ? createSnapshotActions({
        target,
        confirm,
        write: resolved.write,
        onRestored: () => {
          // The panel is showing the catalog of the database that was here before the load.
          void loadCatalog();
        },
      })
    : undefined;

  /** Bytes and blocks and the collector's state, for a target that reports them. */
  const storage = isStorageTarget(target) ? createStorageView(target) : undefined;

  const views: PanelView[] = [
    // The console leads: writing a query is what the panel is opened for most often.
    {
      id: "query",
      label: "Query",
      node: view.node,
      // CodeMirror is fetched here, so the panel opens without it.
      onFirstShow: () => {
        void view.upgrade();
      },
    },
    {
      id: "data",
      label: "Data",
      node: explorer.node,
      // The first page usually arrives while this tab is hidden, and a hidden grid measures
      // 0px tall: it pools only its overscan rows until told the real height is available.
      onShow: () => {
        explorer.layout();
      },
    },
    ...(storage === undefined
      ? []
      : [
          {
            id: "storage",
            label: "Storage",
            node: storage.node,
            // Each report walks the store, so it is read when looked at, never in the background.
            onShow: () => {
              void storage.refresh();
            },
          },
        ]),
  ];

  const panel = createPanel({
    options: resolved,
    offMainThread: runsOffMainThread(target),
    rail: rail.node,
    ...(snapshots === undefined ? {} : { actions: snapshots.nodes }),
    views,
    overlay: confirm.node,
    onClose: () => {
      close();
    },
  });

  /** Read once for the whole panel: the rail, the explorer, and completion share one catalog. */
  let catalogGeneration = 0;
  async function loadCatalog(): Promise<void> {
    // A refresh, a DDL statement, and a restore can each start a read, and they can finish in
    // any order; only the newest may land, or an older catalog could replace a newer one.
    catalogGeneration += 1;
    const token = catalogGeneration;
    try {
      const [tables, introspection] = await Promise.all([
        target.listTables(),
        target.introspect?.(),
      ]);
      if (token !== catalogGeneration) return;
      const catalog = toCatalog(tables, introspection);
      rail.setCatalog(catalog);
      view.setSchema(
        Object.fromEntries(
          catalog.map((table) => [table.name, table.columns.map((column) => column.name)]),
        ),
      );
      await explorer.setCatalog(catalog);
    } catch (error) {
      if (token === catalogGeneration) {
        rail.setError(error instanceof Error ? error.message : String(error));
      }
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
  applyTheme(resolved.theme);

  let open = false;
  let destroyed = false;
  /** The catalog is read when the panel is first opened, never at mount. */
  let catalogLoaded = false;

  function setOpen(next: boolean, moveFocus = true): void {
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
      // Focus follows the view that is actually showing; focusing the console while the data
      // grid is up left a keyboard user outside the panel with nothing selected. It is skipped
      // on the initial open of an inline panel, where taking focus would scroll the page down
      // to a panel the reader has not asked for yet.
      if (moveFocus) {
        if (panel.activeView() === "query") view.focus();
        else panel.focusActiveView();
      }
    } else {
      confirm.dismiss();
    }
  }

  /**
   * Closing from inside the panel — its Close button, Escape — hands focus to the launcher, so
   * the next Tab does not start again from the top of the page. (A pressed button is not always
   * focused — WebKit does not focus on click — so this does not ask where focus was.)
   */
  function close(): void {
    setOpen(false);
    launcher?.node.focus();
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

  /**
   * Keys that act on the panel from anywhere inside it. Escape closes a floating panel, once
   * nothing inside it wanted the key — the confirmation, a cell editor, the insert form, and a
   * completion list all take it first. Mod+K jumps to the table filter.
   */
  panel.node.addEventListener("keydown", (event) => {
    if (event.defaultPrevented) return;
    // Escape in a text box belongs to the box — clearing a search, abandoning a value — not to
    // the panel; closing everything over a half-typed filter is never what it meant.
    const typing =
      event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement;
    if (event.key === "Escape" && resolved.mode === "launcher" && !typing) {
      event.preventDefault();
      close();
    } else if (matchesHotkey("mod+k", event)) {
      event.preventDefault();
      rail.focusSearch();
    }
  });

  panel.node.hidden = true;
  launcher?.setOpen(false);
  if (resolved.defaultOpen) setOpen(true, false);

  return {
    open: () => {
      setOpen(true);
    },
    close,
    toggle,
    setQuery: (sql: string) => {
      if (destroyed) return;
      setOpen(true);
      panel.show("query");
      view.setQuery(sql);
    },
    setTheme: (next: DevtoolsTheme) => {
      if (destroyed) return;
      applyTheme(next);
    },
    get isOpen() {
      return open;
    },
    destroy: () => {
      if (destroyed) return;
      destroyed = true;
      window.removeEventListener("keydown", onKeyDown);
      systemDark?.removeEventListener("change", onSystemTheme);
      confirm.dismiss();
      view.destroy();
      explorer.destroy();
      panel.destroy();
      // The theme attribute stays: on the custom element it is the caller's own markup, and a
      // remount reads its options back off the element.
      host.removeAttribute("data-minnow-devtools");
      root.replaceChildren();
      dropStyles();
    },
  };
}
