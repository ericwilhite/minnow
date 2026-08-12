/**
 * Hotkeys are written as `"mod+shift+d"`. `mod` matches Cmd or Ctrl, so one string works on every
 * platform. An empty hotkey matches nothing, which is how a caller turns the shortcut off.
 */
export function matchesHotkey(hotkey: string, event: KeyboardEvent): boolean {
  const parts = hotkey
    .toLowerCase()
    .split("+")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  const key = parts.pop();
  if (key === undefined) return false;
  if (event.key.toLowerCase() !== key) return false;

  const wants = new Set(parts);
  const mod = wants.delete("mod");
  if (mod && !(event.metaKey || event.ctrlKey)) return false;
  if (wants.has("shift") !== event.shiftKey) return false;
  if (wants.has("alt") !== event.altKey) return false;
  // A bare `ctrl`/`meta` requirement is only checked when `mod` did not already cover it.
  if (!mod && wants.has("ctrl") !== event.ctrlKey) return false;
  if (!mod && wants.has("meta") !== event.metaKey) return false;
  return true;
}
