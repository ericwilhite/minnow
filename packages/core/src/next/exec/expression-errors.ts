/** Same precise-replay gate for relational projections and write row rules.
 * Only deterministic SQL data errors select replay. Never retry cancellation,
 * admission, unsupported capabilities or internal failures as SQL evaluation.
 * Volatile kernels remain refused; revisit replay before enabling them.
 */
import { ExecutionError } from "./errors.js";
export function expressionDataError(error: unknown): boolean {
  return error instanceof ExecutionError && error.sqlState !== "57014";
}
