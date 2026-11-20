import { NativeKyselyAdapter } from "./adapter.js";
/** Native SELECTs execute once in the owner; only decoding/plugins run in the client. */
import type { QueryExecutorProvider, CompiledQuery } from "kysely";
import type {
  NativeLiveDriver,
  NativeLiveOptions,
  NativeKeyedLiveStore,
} from "../../../core/dist/next/owner/live-store.js";
import type { LiveKeyOf } from "../../../core/dist/next/live/rows.js";
import { nativeParameters, nativeRows, type NativeResultDecoding } from "./driver.js";

interface Selectable {
  compile(): CompiledQuery;
  execute(): Promise<readonly object[]>;
}
interface WindowSelectable extends Selectable {
  limit(n: number): WindowSelectable;
  offset(n: number): WindowSelectable;
}
type RowOf<Q extends Selectable> = Awaited<ReturnType<Q["execute"]>>[number];
export function nativeLiveQuery(config: {
  readonly driver: NativeLiveDriver;
  readonly db?: QueryExecutorProvider;
  readonly resultDecoding?: NativeResultDecoding;
  readonly memoryBytes?: number;
}) {
  const executor = config.db?.getExecutor();
  if (executor !== undefined && !(executor.adapter instanceof NativeKyselyAdapter))
    throw new TypeError("Native live queries require the native Minnow dialect");
  const decoding =
    config.resultDecoding ??
    (executor?.adapter as NativeKyselyAdapter | undefined)?.resultDecoding ??
    {};
  let closed = false;
  const stores = new Set<{ close(): Promise<void>; refresh(): Promise<void> }>();
  const watch = <Q extends Selectable>(query: Q, options: NativeLiveOptions<RowOf<Q>> = {}) => {
    if (closed) throw new Error("Native live factory is closed");
    const compiled = query.compile();
    const queryId = compiled.queryId;
    const store = config.driver.liveQuery<RowOf<Q>>(
      compiled.sql,
      nativeParameters(compiled.parameters),
      {
        ...options,
        ...(config.memoryBytes === undefined ? {} : { memoryBytes: config.memoryBytes }),
        decode: async (result, signal) => {
          let transformed = {
            rows: nativeRows(result, decoding).map((row) => row as Record<string, unknown>),
          };
          for (const plugin of executor?.plugins ?? []) {
            signal.throwIfAborted();
            transformed = await plugin.transformResult({
              result: transformed,
              queryId,
            });
          }
          signal.throwIfAborted();
          return transformed.rows;
        },
      },
    );
    const close = store.close.bind(store);
    store.close = async () => {
      try {
        await close();
      } finally {
        stores.delete(store);
      }
    };
    stores.add(store);
    return store;
  };
  return Object.assign(<Q extends Selectable>(query: Q) => watch(query), {
    changes: <Q extends Selectable, K extends LiveKeyOf<RowOf<Q>>>(
      query: Q,
      options: { readonly key: K; readonly table?: string },
    ) => watch(query, options) as unknown as NativeKeyedLiveStore<RowOf<Q>, K>,
    window: <Q extends WindowSelectable, K extends LiveKeyOf<RowOf<Q>>>(
      query: Q,
      options: {
        readonly key: K;
        readonly limit: number;
        readonly offset?: number;
        readonly table?: string;
      },
    ) => {
      if (
        !Number.isSafeInteger(options.limit) ||
        options.limit < 1 ||
        (options.offset !== undefined &&
          (!Number.isSafeInteger(options.offset) || options.offset < 0))
      )
        throw new RangeError("Invalid live window bounds");
      const root = query.compile().query;
      if (
        root.kind !== "SelectQueryNode" ||
        root.orderBy === undefined ||
        root.orderBy.items.length === 0
      )
        throw new TypeError("Windowed live queries require a top-level ORDER BY");
      let windowed = query.limit(options.limit);
      if (options.offset !== undefined) windowed = windowed.offset(options.offset);
      return watch(windowed as Q, {
        ...options,
        maxRows: options.limit,
      }) as unknown as NativeKeyedLiveStore<RowOf<Q>, K>;
    },
    refresh: async () => {
      await Promise.all([...stores].map((store) => store.refresh()));
    },
    close: async () => {
      closed = true;
      await Promise.all([...stores].map((store) => store.close()));
    },
  });
}
