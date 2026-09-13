/** Quotes an identifier for SQL the engine generates about its own catalog. */
export function quoteSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}
