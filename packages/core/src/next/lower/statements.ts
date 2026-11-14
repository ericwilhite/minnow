import type { CompiledStatement } from "../../engine/query.js";
/** Native-only statements do not add rebuild semantics to the shipped compiler DTO. */
export type NativeStatement =
  CompiledStatement | { kind: "drop-type" | "drop-sequence"; name: string; ifExists?: boolean };
