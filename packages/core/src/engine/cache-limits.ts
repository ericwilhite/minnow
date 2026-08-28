/** Valid inputs beyond this length execute normally but are not retained in process-wide caches. */
export const MAX_CACHEABLE_TEXT_CHARACTERS = 16 * 1024;

/**
 * Compilation happens before a query memory context exists. These limits therefore bound parser
 * allocations and recursion independently of the execution budget. They are intentionally much
 * larger than ordinary application SQL while remaining small enough for deterministic refusal.
 */
export const MAX_SQL_TEXT_CHARACTERS = 1024 * 1024;
export const MAX_SQL_TOKENS = 16_384;
export const MAX_SQL_NESTING_DEPTH = 128;
export const MAX_SQL_PARAMETERS = 4_096;

/**
 * Scalar functions that create a new string must not allocate an attacker-selected result before
 * query memory accounting can observe it. Plain projection does not use this limit: a stored wide
 * value can still be selected or streamed without asking a scalar function to expand it.
 */
export const MAX_SQL_SCALAR_RESULT_CHARACTERS = 1024 * 1024;

/** Bounds object/array walks performed before an execution memory context exists. */
export const MAX_SQL_STRUCTURED_VALUE_ITEMS = 65_536;
export const MAX_SQL_STRUCTURED_VALUE_DEPTH = 128;

/** SQL patterns compile outside the execution memory budget and may be retained in small caches. */
export const MAX_SQL_PATTERN_CHARACTERS = 16 * 1024;
/** Deterministic CPU ceiling for one non-trivial LIKE or SIMILAR TO match. */
export const MAX_SQL_PATTERN_MATCH_STEPS = 8 * 1024 * 1024;
export const MAX_SQL_NUMERIC_DIGITS = 100_000;
