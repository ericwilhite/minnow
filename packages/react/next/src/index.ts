/** Internal cutover entry: the structural hooks are unchanged; B8 live creation is not bound. */
export { useLiveQuery, useLiveSelector, useSuspenseLiveQuery } from "../../dist/index.js";
export type {
  LiveExternalStore,
  RefreshableLiveExternalStore,
  UseLiveQueryOptions,
  UseLiveSelectorOptions,
  SettledLiveSnapshot,
} from "../../dist/index.js";

/** The eventual finite-credit live-store bridge belongs here, never a polling substitute. */
export function nativeLiveStore(): never {
  throw new Error("Native React live stores are not yet bound");
}
