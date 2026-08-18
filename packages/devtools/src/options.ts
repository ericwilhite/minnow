/** Where the launcher button sits, and which corner the panel opens from. */
export type DevtoolsCorner = "bottom-right" | "bottom-left" | "top-right" | "top-left";

/**
 * `launcher` floats a button over the page that opens the panel; `inline` renders the panel in
 * document flow with no button, which is what the docs playground embeds.
 */
export type DevtoolsMode = "launcher" | "inline";

/**
 * Which palette the panel paints in. `"system"` follows the viewer's OS setting; a page with its
 * own light/dark switch passes `"light"` or `"dark"` so the panel turns with the page.
 */
export type DevtoolsTheme = "system" | "light" | "dark";

export interface DevtoolsPermissions {
  /** When false, statements that would change data are refused before they reach the database. */
  write?: boolean;
}

export interface DevtoolsOptions {
  mode?: DevtoolsMode;
  corner?: DevtoolsCorner;
  /** Keyboard shortcut that toggles the panel, as `"mod+shift+d"`. Empty disables it. */
  hotkey?: string;
  defaultOpen?: boolean;
  zIndex?: number;
  permissions?: DevtoolsPermissions;
  /** SQL the console starts with. */
  initialQuery?: string;
  /** Namespace for persisted panel geometry, so two panels on a page keep their own. */
  storageKey?: string;
  theme?: DevtoolsTheme;
  /**
   * How tall an inline panel is, as a CSS length; a number is pixels. Left out, the panel fills a
   * container that has a height of its own and falls back to a readable default in one that does
   * not. Ignored by a floating panel, which is sized by dragging.
   */
  height?: number | string;
}

export interface ResolvedDevtoolsOptions {
  mode: DevtoolsMode;
  corner: DevtoolsCorner;
  hotkey: string;
  defaultOpen: boolean;
  zIndex: number;
  write: boolean;
  initialQuery: string;
  storageKey: string;
  theme: DevtoolsTheme;
  /** A CSS length, or empty when the caller said nothing and the container decides. */
  height: string;
}

const corners: readonly DevtoolsCorner[] = ["bottom-right", "bottom-left", "top-right", "top-left"];
const modes: readonly DevtoolsMode[] = ["launcher", "inline"];
const themes: readonly DevtoolsTheme[] = ["system", "light", "dark"];

/** A number is pixels, a string is whatever CSS length the caller wrote, anything else is unset. */
function cssLength(value: number | string | undefined): string {
  if (typeof value === "number") return Number.isFinite(value) ? `${String(value)}px` : "";
  if (typeof value !== "string") return "";
  const trimmed = value.trim();
  return /^-?\d+(\.\d+)?$/.test(trimmed) ? `${trimmed}px` : trimmed;
}

function oneOf<T extends string>(allowed: readonly T[], value: unknown, fallback: T): T {
  return typeof value === "string" && (allowed as readonly string[]).includes(value)
    ? (value as T)
    : fallback;
}

export function resolveOptions(options: DevtoolsOptions = {}): ResolvedDevtoolsOptions {
  const mode = oneOf(modes, options.mode, "launcher");
  return {
    mode,
    corner: oneOf(corners, options.corner, "bottom-right"),
    hotkey: options.hotkey ?? "mod+shift+d",
    // Inline panels have no launcher to open them, so they start open whatever the caller said.
    defaultOpen: mode === "inline" ? true : (options.defaultOpen ?? false),
    // A floating panel is an overlay and has to clear whatever the page stacks above its own
    // content. An inline one is a section of the document: given the overlay's z-index it would
    // paint over the page's sticky header the moment the reader scrolled. It therefore sits in
    // the page's own order unless the embedder asks for something else.
    zIndex: Number.isFinite(options.zIndex)
      ? Number(options.zIndex)
      : mode === "inline"
        ? 0
        : 2_147_483_000,
    write: options.permissions?.write ?? true,
    initialQuery: options.initialQuery ?? "",
    storageKey: options.storageKey ?? "minnow-devtools",
    theme: oneOf(themes, options.theme, "system"),
    height: cssLength(options.height),
  };
}

/** Reads the same options off element attributes, so the custom element configures declaratively. */
export function optionsFromAttributes(element: {
  getAttribute(name: string): string | null;
}): DevtoolsOptions {
  const read = (name: string): string | undefined => element.getAttribute(name) ?? undefined;
  const flag = (name: string): boolean | undefined => {
    const value = read(name);
    if (value === undefined) return undefined;
    // A bare attribute (`open`) reads as the empty string and means true.
    return value !== "false";
  };
  const zIndex = read("z-index");
  const write = flag("write");
  const options: DevtoolsOptions = {};
  const mode = read("mode");
  if (mode !== undefined) options.mode = mode as DevtoolsMode;
  const corner = read("corner");
  if (corner !== undefined) options.corner = corner as DevtoolsCorner;
  const hotkey = read("hotkey");
  if (hotkey !== undefined) options.hotkey = hotkey;
  const open = flag("open");
  if (open !== undefined) options.defaultOpen = open;
  if (zIndex !== undefined) options.zIndex = Number(zIndex);
  if (write !== undefined) options.permissions = { write };
  const initialQuery = read("initial-query");
  if (initialQuery !== undefined) options.initialQuery = initialQuery;
  const storageKey = read("storage-key");
  if (storageKey !== undefined) options.storageKey = storageKey;
  const theme = read("theme");
  if (theme !== undefined) options.theme = theme as DevtoolsTheme;
  const height = read("height");
  if (height !== undefined) options.height = height;
  return options;
}
