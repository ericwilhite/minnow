/**
 * Grammar failures of the SQL front end (engine/query.ts): internal, not part of the package's
 * exports (engine/index.ts re-exports errors.ts, not this module).
 */

/** Failures the front end raised because it could not read the text as a statement (see
 * `grammarFailure`). Weak: an error leaves with its last reference. */
const grammarFailures = new WeakSet();

/**
 * Marks a front-end failure as a grammar failure: the text could not be read as a statement (an
 * unterminated literal or comment, a token where the grammar needs another), which is where
 * PostgreSQL's raw parse fails. A refusal of a statement that was read (an unknown function, a
 * `SELECT *` without FROM) is not marked: PostgreSQL makes those in parse analysis. Nothing
 * about the error changes; the 1.0 owner reads the mark to decide whether a failed statement
 * took its transaction's snapshot (next/ `Lowerer.takesSnapshot`).
 */
export function grammarFailure<E extends Error>(error: E): E {
  grammarFailures.add(error);
  return error;
}

/** Whether `error` itself is marked (`grammarFailure`): the front end's own check. */
export function markedGrammarFailure(error: unknown): boolean {
  return error instanceof Error && grammarFailures.has(error);
}

/** Whether `error`, or an error in its `cause` chain, is a grammar failure (`grammarFailure`). */
export function isGrammarFailure(error: unknown): boolean {
  for (let current = error; current instanceof Error; current = current.cause)
    if (grammarFailures.has(current)) return true;
  return false;
}
