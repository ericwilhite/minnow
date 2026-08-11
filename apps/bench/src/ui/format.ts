/** Formatting helpers shared by every page. Lifted from the previous dashboard. */

export function formatDuration(value: number): string {
  if (value < 1) return `${value.toFixed(2)} ms`;
  if (value < 1_000) return `${value.toFixed(1)} ms`;
  return `${(value / 1_000).toFixed(2)} s`;
}

export function formatBytes(value: number): string {
  if (value < 1_024) return `${formatInteger(value)} B`;
  if (value < 1_048_576) return `${(value / 1_024).toFixed(1)} KB`;
  if (value < 1_073_741_824) return `${(value / 1_048_576).toFixed(2)} MB`;
  return `${(value / 1_073_741_824).toFixed(2)} GB`;
}

export function formatOptionalBytes(value: number | null | undefined): string {
  return value === null || value === undefined ? "—" : formatBytes(value);
}

export function formatInteger(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value);
}

export function formatDecimal(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}

export function formatQueryValue(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "number")
    return Number.isInteger(value) ? formatInteger(value) : formatDecimal(value);
  if (typeof value === "string" || typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  }
  return JSON.stringify(value);
}

export function compactSql(sql: string): string {
  const compact = sql.replace(/\s+/g, " ").trim();
  return compact.length > 96 ? `${compact.slice(0, 93)}…` : compact;
}

export function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

/** One labeled figure inside a `.stats` grid. */
export function stat(label: string, value: string, detail?: string): string {
  const small = detail === undefined ? "" : `<small>${escapeHtml(detail)}</small>`;
  return `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong>${small}</div>`;
}

// The type parameter gives each statically known selector its concrete DOM element API.
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters
export function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (element === null) throw new Error(`Missing element: ${selector}`);
  return element;
}
