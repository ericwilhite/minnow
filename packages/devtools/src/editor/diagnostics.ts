import matrix from "@minnowdb/core/sql-feature-matrix.json" with { type: "json" };
import { SqlCompileError, compileStatement } from "@minnowdb/core";
import { buildFailureIndex, describeUnsupported, lookupFailure } from "../sql/feature-matrix.js";

/**
 * The matrix is 14 KB of JSON, and it rides in the editor chunk rather than the panel's — the
 * same chunk as CodeMirror, which is already two orders of magnitude larger, so it costs nothing
 * anyone notices and the panel that never opens the console never fetches it.
 */
const index = buildFailureIndex(matrix.features);

/**
 * What the engine records about a failure, beyond the message: which capability it ran into, and
 * what stands in for it. Returns undefined when the matrix has nothing unambiguous to say.
 */
export function explainUnsupported(message: string): string | undefined {
  const feature = lookupFailure(index, message);
  return feature === undefined ? undefined : describeUnsupported(feature);
}

export interface SqlDiagnostic {
  from: number;
  to: number;
  severity: "error";
  message: string;
}

/**
 * Compiles the text and reports where it failed.
 *
 * This is the whole reason the compiler is worth having in the page: `compileStatement` is
 * synchronous and local, so a keystroke costs a parse rather than a round trip to the worker, and
 * the position comes from the compiler itself instead of being guessed from the message.
 *
 * An empty document is not a mistake, and neither is a failure with no position — a plan-level or
 * execution error has nowhere to point, and marking the whole document for it would be noise.
 */
export function diagnose(
  text: string,
  explain?: (message: string) => string | undefined,
): SqlDiagnostic[] {
  if (text.trim().length === 0) return [];
  try {
    compileStatement(text);
    return [];
  } catch (error) {
    if (!(error instanceof SqlCompileError)) return [];
    const extra = explain?.(error.message);
    // A zero-width span marks nothing. Widening forwards is the usual fix, but a query that ended
    // too early reports its position at the very end of the text, where there is nothing ahead to
    // cover — so that case reaches back over the last character instead.
    let from = Math.min(error.offset, text.length);
    const to = Math.min(Math.max(error.offset + error.length, from + 1), text.length);
    if (to <= from) from = Math.max(0, to - 1);
    return [
      {
        from,
        to,
        severity: "error",
        message: extra === undefined ? error.message : `${error.message}\n\n${extra}`,
      },
    ];
  }
}
