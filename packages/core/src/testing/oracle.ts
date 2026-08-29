/**
 * Shared glue for the differential oracles. Like ./seeds.ts, this module is excluded from the
 * published tarball, so shipped modules must not import it.
 */

/** Rewrites `?` placeholders to PostgreSQL's `$n`; harness strings never contain a literal `?`. */
export function positionalToNumbered(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${String((index += 1))}`);
}
