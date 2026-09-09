import type { QueryResult } from "../plan/model.js";

/** Shared boundary state; result copies must not need the SQL compiler or evaluators. */
export const alreadyExternalResults = new WeakSet<QueryResult>();

/** Internal ownership boundary for results assembled entirely from already-public row values. */
export function markQueryResultExternal(result: QueryResult): QueryResult {
  alreadyExternalResults.add(result);
  return result;
}

/** Carries the internal no-conversion proof across a defensive result copy. */
export function copyQueryResultExternalization(
  source: QueryResult,
  copy: QueryResult,
): QueryResult {
  if (alreadyExternalResults.has(source)) alreadyExternalResults.add(copy);
  return copy;
}
