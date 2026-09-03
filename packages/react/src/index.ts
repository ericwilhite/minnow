import { useCallback, useRef, useSyncExternalStore } from "react";

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

export interface UseLiveSelectorOptions<TSnapshot, TSelected> {
  /**
   * Derives what the component needs from the snapshot. The component re-renders only when the
   * derived value differs, as decided by `isEqual`; a snapshot change that leaves the selection
   * alone — another row updated, a version bump — is ignored. The selector runs synchronously
   * and must not capture anything that changes without re-rendering.
   */
  readonly select: (snapshot: TSnapshot) => TSelected;
  /** Compares two selections; defaults to `Object.is`. Pass a shallow comparer for derived arrays. */
  readonly isEqual?: (previous: TSelected, next: TSelected) => boolean;
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

interface SelectionCache<TSnapshot, TSelected> {
  snapshot: TSnapshot;
  select: (snapshot: TSnapshot) => TSelected;
  selected: TSelected;
}

/**
 * Read one derived value from a live-query snapshot. Where `useLiveQuery` re-renders on every
 * new snapshot, this re-renders only when the selection changes: a row count, one row by key,
 * the ids of a list whose items render themselves from the keyed rows. The selection is cached
 * per snapshot, so React's tearing check sees a stable value between store changes, and an
 * inline `select` is fine — a new function re-selects, and `isEqual` decides whether the
 * previous value stands.
 */
export function useLiveSelector<TSnapshot, TSelected>(
  query: LiveExternalStore<TSnapshot>,
  options: UseLiveSelectorOptions<TSnapshot, TSelected>,
): TSelected {
  const { select, isEqual = Object.is } = options;
  const cache = useRef<SelectionCache<TSnapshot, TSelected> | undefined>(undefined);
  const getSelected = useCallback((): TSelected => {
    const snapshot = query.getSnapshot();
    const cached = cache.current;
    if (cached?.snapshot === snapshot && cached.select === select) return cached.selected;
    const selected = select(snapshot);
    if (cached !== undefined && isEqual(cached.selected, selected)) {
      cache.current = { snapshot, select, selected: cached.selected };
      return cached.selected;
    }
    cache.current = { snapshot, select, selected };
    return selected;
  }, [query, select, isEqual]);
  return useSyncExternalStore(query.subscribe, getSelected, getSelected);
}
