/** Internal cutover entry. Published exports remain held. */
import type { Param, Row } from "../../../core/dist/next/owner/api.js";
import type {
  NativeLiveDriver,
  NativeLiveOptions,
} from "../../../core/dist/next/owner/live-store.js";
export { useLiveQuery, useLiveSelector, useSuspenseLiveQuery } from "../../dist/index.js";
export type {
  LiveExternalStore,
  RefreshableLiveExternalStore,
  UseLiveQueryOptions,
  UseLiveSelectorOptions,
  SettledLiveSnapshot,
} from "../../dist/index.js";

/** The existing hooks consume the native store directly, including Suspense refresh sharing. */
export function nativeLiveStore<R extends object = Row>(
  driver: NativeLiveDriver,
  sql: string,
  params: readonly Param[] = [],
  options: NativeLiveOptions<R> = {},
) {
  return driver.liveQuery<R>(sql, params, options);
}
