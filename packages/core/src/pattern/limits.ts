/** Shared bounded regex syntax/work constants. Native callers additionally admit memory. */
export const MAX_SQL_NESTING_DEPTH = 128;
export const MAX_SQL_PATTERN_CHARACTERS = 16 * 1024;
export const MAX_SQL_PATTERN_MATCH_STEPS = 8 * 1024 * 1024;
/** Released scalar string boundary; native byte input is governed separately. */
export const MAX_SQL_SCALAR_RESULT_CHARACTERS = 1024 * 1024;
