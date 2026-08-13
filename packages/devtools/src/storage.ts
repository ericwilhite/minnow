/**
 * Small, forgiving wrappers around localStorage. Storage can be unavailable (private mode, a
 * sandboxed frame) or full, and none of what the panel keeps there — geometry, collapsed
 * sidebars, history — is worth failing an interaction over.
 */

export function readStored(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

export function writeStored(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // Nothing to do: the setting simply is not remembered.
  }
}

export function readFlag(key: string, fallback: boolean): boolean {
  const raw = readStored(key);
  return raw === null ? fallback : raw === "true";
}

export function writeFlag(key: string, value: boolean): void {
  writeStored(key, String(value));
}
