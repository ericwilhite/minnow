import { useSyncExternalStore } from "react";

/** The framework-neutral contract implemented by Minnow live and keyed queries. */
export interface LiveExternalStore<out TSnapshot> {
  readonly getSnapshot: () => TSnapshot;
  readonly subscribe: (listener: () => void) => () => void;
}

/**
 * Read a Minnow live-query snapshot with React's concurrent-safe external-store primitive.
 * The same snapshot is used for server rendering so the hook needs no browser-only fallback.
 */
export function useLiveQuery<TSnapshot>(query: LiveExternalStore<TSnapshot>): TSnapshot {
  return useSyncExternalStore(query.subscribe, query.getSnapshot, query.getSnapshot);
}
