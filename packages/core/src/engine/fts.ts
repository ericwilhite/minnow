/** Shipped-engine representation adapter and bounded compilation cache; semantics live in the shared leaf. */
export * from "../full-text/semantics.js";
import * as shared from "../full-text/semantics.js";
import { MAX_CACHEABLE_TEXT_CHARACTERS } from "./cache-limits.js";
import { MAX_FTS_QUERY_TERMS } from "../full-text/limits.js";
import { externalSqlDomainValue, fullTextDomainText } from "./sql-domains.js";
import { tokenizeQuery, type FtsQueryTerm, type FtsStats } from "../full-text/semantics.js";

/** Decode legacy tagged values once; the leaf never learns the shipped storage representation. */
function documentValue(value: unknown): unknown {
  return fullTextDomainText(value) ?? externalSqlDomainValue(value);
}
function* documentValues(values: Iterable<unknown>): Iterable<unknown> {
  for (const value of values) yield documentValue(value);
}
export function renderDocumentValue(value: unknown): string | undefined {
  return shared.renderDocumentValue(documentValue(value));
}
export function ftsMatchTruth(
  terms: readonly FtsQueryTerm[],
  values: Iterable<unknown>,
): boolean | null {
  return shared.ftsMatchTruth(terms, documentValues(values));
}
export function ftsBm25Row(
  terms: readonly FtsQueryTerm[],
  values: Iterable<unknown>,
  stats: FtsStats,
): number | null {
  return shared.ftsBm25Row(terms, documentValues(values), stats);
}

const queryTermsCache = new Map<string, FtsQueryTerm[]>();

/** Tokenized query terms with the same bounded caching scheme as the LIKE pattern cache. */
export function cachedQueryTerms(query: string): FtsQueryTerm[] {
  const cached = queryTermsCache.get(query);
  if (cached !== undefined) return cached;
  const terms = tokenizeQuery(query);
  if (query.length > MAX_CACHEABLE_TEXT_CHARACTERS) return terms;
  if (queryTermsCache.size >= 128) queryTermsCache.clear();
  queryTermsCache.set(query, terms);
  return terms;
}

/** Compile-time validation shared by the SQL parser and the DSL expression builder. */
export function validateFtsQuery(query: string): void {
  if (cachedQueryTerms(query).length > MAX_FTS_QUERY_TERMS) {
    throw new TypeError(`Full-text queries support at most ${String(MAX_FTS_QUERY_TERMS)} terms`);
  }
}
