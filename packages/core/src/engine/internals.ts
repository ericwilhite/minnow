import type { MinnowDatabase } from "./database.js";

/**
 * Engine operations the stock worker server needs that are not part of the public database
 * surface. The engine registers itself here on construction; nothing outside this package
 * imports the module.
 */
export interface DatabaseInternals {
  /**
   * Rejects every foreground writer waiting, now or later, on another context's lock, without
   * touching the local queue or the turns already taken. A worker connection being disposed
   * calls this so closing answers the writes this engine can still serve itself and never
   * waits for a tab that stopped inside its turn. One-shot: the engine is about to close.
   */
  cancelCrossContextWaits(reason: Error): void;
}

export const databaseInternals = new WeakMap<MinnowDatabase, DatabaseInternals>();

export function internalsOf(database: MinnowDatabase): DatabaseInternals {
  const internals = databaseInternals.get(database);
  if (internals === undefined) throw new Error("Database internals are not registered");
  return internals;
}
