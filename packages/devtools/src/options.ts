/** Where the launcher button sits, and which corner the panel opens from. */
export type DevtoolsCorner = "bottom-right" | "bottom-left" | "top-right" | "top-left";

/**
 * `launcher` floats a button over the page that opens the panel; `inline` renders the panel in
 * document flow with no button, which is what the docs playground embeds.
 */
export type DevtoolsMode = "launcher" | "inline";

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
}

const corners: readonly DevtoolsCorner[] = ["bottom-right", "bottom-left", "top-right", "top-left"];
const modes: readonly DevtoolsMode[] = ["launcher", "inline"];

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
    zIndex: Number.isFinite(options.zIndex) ? Number(options.zIndex) : 2_147_483_000,
    write: options.permissions?.write ?? true,
    initialQuery: options.initialQuery ?? "",
    storageKey: options.storageKey ?? "minnow-devtools",
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
  return options;
}
