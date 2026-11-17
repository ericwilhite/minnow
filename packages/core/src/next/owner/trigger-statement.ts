/** One already-funded native triggering statement. No commit or independent admission.
 * Graph callbacks and IO drain before failed-run/end rollback or caller-owned bindings retire.
 * A close failure AFTER successful statement.end needs the caller's enclosing transaction /
 * savepoint recovery: Statement.rollback cannot undo an already ended statement. */
import type { BoundParameters } from "../exec/executor.js";
import { NativeCommandRoot, type NativeCommandRootScope } from "./command-root.js";

export async function runTriggerStatement(
  scope: NativeCommandRootScope,
  parameters: BoundParameters,
): Promise<number> {
  let root: NativeCommandRoot | null = null;
  let failure: { reason: unknown } | null = null;
  let cleanup: { reason: unknown } | null = null;
  let affected = 0;
  try {
    root = await NativeCommandRoot.create(scope);
    affected = await root.run(parameters);
    await root.end();
  } catch (reason) {
    failure = { reason };
  }
  try {
    await root?.close();
  } catch (reason) {
    cleanup = { reason };
  }
  if (failure || cleanup) {
    try {
      scope.statement.rollback();
    } catch (reason) {
      const previous = cleanup;
      cleanup = {
        reason: previous
          ? new AggregateError([previous.reason, reason], "Native trigger rollback failed", {
              cause: previous.reason,
            })
          : reason,
      };
    }
    if (cleanup && failure)
      throw new AggregateError([failure.reason, cleanup.reason], "Native trigger cleanup failed", {
        cause: failure.reason,
      });
    if (cleanup) throw cleanup.reason;
    if (failure) throw failure.reason;
  }
  return affected;
}
