/** Shared v1 semantics; native callers supply portable logical values and charged workspace. */
export * from "../full-text/semantics.js";
export * from "../full-text/limits.js";

/** 1.0 canonical PostgreSQL TIME/ARRAY rendering; shipped adapter remains v1. */
export const FTS_RENDER_VERSION = 2;
