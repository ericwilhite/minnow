/** Semantic identity shared by portable definitions and native FTS page headers. */
import { DurableFormatError } from "./durable-format.js";
import { FTS_TOKENIZER_VERSION, FTS_RENDER_VERSION, FTS_SCORER_VERSION } from "./full-text.js";

export const FTS_CONFIG_VERSION = 1;
/** Version1 selects the complete fixed configuration; individual limits are not tunable. */
export interface FtsConfig {
  readonly version: number;
}
export interface FtsSemantics {
  readonly tokenizerVersion: number;
  readonly rendererVersion: number;
  readonly scorerVersion: number;
  readonly config: FtsConfig;
}
/** Returned nested data is owned; attaching opaque catalog fields cannot affect another def. */
export function ftsSemantics(): FtsSemantics {
  return {
    tokenizerVersion: FTS_TOKENIZER_VERSION,
    rendererVersion: FTS_RENDER_VERSION,
    scorerVersion: FTS_SCORER_VERSION,
    config: { version: FTS_CONFIG_VERSION },
  };
}
/** Callers first validate structural integer fields; valid future versions are unsupported. */
export function requireFtsSemantics(value: FtsSemantics): void {
  const check = (actual: number, expected: number, label: string) => {
    if (actual !== expected)
      throw new DurableFormatError(`Unsupported FTS ${label} version ${String(actual)}`);
  };
  check(value.tokenizerVersion, FTS_TOKENIZER_VERSION, "tokenizer");
  check(value.rendererVersion, FTS_RENDER_VERSION, "renderer");
  check(value.scorerVersion, FTS_SCORER_VERSION, "scorer");
  check(value.config.version, FTS_CONFIG_VERSION, "configuration");
}
