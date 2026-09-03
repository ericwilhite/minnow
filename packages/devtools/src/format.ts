/** `512 B`, `3.4 KB`, `120 MB`: bytes as a person reads them. */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${String(bytes)} B`;
  const units = ["KB", "MB", "GB"] as const;
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const rounded = value < 10 ? value.toFixed(1) : String(Math.round(value));
  return `${rounded} ${units[unit] ?? "GB"}`;
}

/** `1,234` — a count with thousands separators, so 20000 reads at a glance. */
export function formatCount(count: number): string {
  return new Intl.NumberFormat("en-US").format(count);
}
