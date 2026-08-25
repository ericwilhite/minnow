/** Runtime imports the in-page TypeScript console can execute, not merely type-check. */
export const PLAYGROUND_RUNTIME_MODULES = [
  "@minnowdb/core",
  "@minnowdb/core/client",
  "@minnowdb/kysely",
  "kysely",
] as const;

export type PlaygroundRuntimeModule = (typeof PLAYGROUND_RUNTIME_MODULES)[number];
