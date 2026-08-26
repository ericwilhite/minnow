import { useRef, useSyncExternalStore } from "react";

/** The framework-neutral contract implemented by Minnow live and keyed queries. */
export interface LiveExternalStore<out TSnapshot> {
  readonly getSnapshot: () => TSnapshot;
  readonly subscribe: (listener: () => void) => () => void;
}

export interface RefreshableLiveExternalStore<out TSnapshot> extends LiveExternalStore<TSnapshot> {
  readonly refresh: () => Promise<void>;
}

export interface UseLiveQueryOptions {
  /** Suspend only while the query has never produced a settled snapshot. */
  readonly suspense?: boolean;
  /** Keep the previous settled snapshot while a different store performs its first load. */
  readonly staleWhileRevalidate?: boolean;
}

interface StatusSnapshot {
  readonly status: "loading" | "ready" | "error";
}
export type SettledLiveSnapshot<TSnapshot> = Exclude<TSnapshot, { readonly status: "loading" }>;

const refreshes = new WeakMap<object, Promise<void>>();

function refreshOnce(store: RefreshableLiveExternalStore<unknown>): Promise<void> {
  const key = store as object;
  const existing = refreshes.get(key);
  if (existing !== undefined) return existing;
  const refresh = store.refresh();
  refreshes.set(key, refresh);
  void refresh.then(
    () => {
      if (refreshes.get(key) === refresh) refreshes.delete(key);
    },
    () => {
      if (refreshes.get(key) === refresh) refreshes.delete(key);
    },
  );
  return refresh;
}

/**
 * Read a Minnow live-query snapshot with React's concurrent-safe external-store primitive.
 * The same snapshot is used for server rendering so the hook needs no browser-only fallback.
 */
export function useLiveQuery<TSnapshot>(
  query: LiveExternalStore<TSnapshot>,
  options: UseLiveQueryOptions = {},
): TSnapshot {
  const previous = useRef<TSnapshot | undefined>(undefined);
  const snapshot = useSyncExternalStore(query.subscribe, query.getSnapshot, query.getSnapshot);
  const status = (snapshot as StatusSnapshot | null)?.status;
  if (status !== "loading") {
    previous.current = snapshot;
    return snapshot;
  }
  if (options.staleWhileRevalidate === true && previous.current !== undefined) {
    return previous.current;
  }
  if (options.suspense === true) {
    const refresh = (query as Partial<RefreshableLiveExternalStore<TSnapshot>>).refresh;
    if (refresh === undefined) {
      throw new TypeError("A suspense live query must provide refresh()");
    }
    // React Suspense consumes the thrown promise and retries this component when it settles.
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw refreshOnce(query as RefreshableLiveExternalStore<unknown>);
  }
  return snapshot;
}

/** Suspense-first form whose return type excludes the cold `loading` snapshot. */
export function useSuspenseLiveQuery<TSnapshot extends StatusSnapshot>(
  query: RefreshableLiveExternalStore<TSnapshot>,
  options: Omit<UseLiveQueryOptions, "suspense"> = {},
): SettledLiveSnapshot<TSnapshot> {
  return useLiveQuery(query, { ...options, suspense: true }) as SettledLiveSnapshot<TSnapshot>;
}
