/**
 * The shape of the compiler's `SqlCompileError`, matched by name and fields rather than by class.
 * The console runs before the compiler chunk is fetched and never imports it, so it cannot use
 * `instanceof`; the worker client also rebuilds errors from the wire, where the name survives and
 * the class does not.
 */
export interface LocatedError extends Error {
  offset: number;
  length: number;
}

export function isLocatedError(error: unknown): error is LocatedError {
  return (
    error instanceof Error &&
    error.name === "SqlCompileError" &&
    typeof (error as Partial<LocatedError>).offset === "number" &&
    typeof (error as Partial<LocatedError>).length === "number"
  );
}

export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
