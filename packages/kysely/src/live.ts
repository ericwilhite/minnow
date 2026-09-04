import {
  KeyedLiveQuery,
  LiveQueryManager,
  type LiveKeyOf,
  type LiveQuery,
  type LiveQueryDriver,
  type LiveQueryManagerOptions,
} from "@minnowdb/core/live";
import type { QueryResult } from "@minnowdb/core";
import type {
  AbortableQueryOptions,
  CompiledQuery,
  QueryExecutor,
  QueryExecutorProvider,
} from "kysely";
import { MinnowKyselyAdapter } from "./dialect.js";
import { decodedRows } from "./driver.js";
import { kyselyQueryValues } from "./query-values.js";

/** The public Kysely SELECT surface the live wrapper consumes. */
export interface KyselyLiveSelectable {
  compile(): CompiledQuery;
  execute(options?: AbortableQueryOptions): Promise<readonly object[]>;
}

export interface KyselyWindowSelectable extends KyselyLiveSelectable {
  limit(limit: number): KyselyWindowSelectable;
  offset(offset: number): KyselyWindowSelectable;
}

export type KyselyLiveRow<TQuery extends KyselyLiveSelectable> = Awaited<
  ReturnType<TQuery["execute"]>
>[number];

export interface CreateKyselyLiveQueriesConfig extends LiveQueryManagerOptions {
  /** An in-thread Minnow database or worker-backed client. */
  readonly driver: LiveQueryDriver;
  /**
   * The Kysely instance the live queries are built from. With it, the engine delivers a changed
   * result to the page and the wrapper decodes it the way the dialect would — its result
   * decoding, then every plugin's `transformResult` — so a change costs one execution in the
   * engine and one transfer, with no round trip back to re-execute. Without it, a change is an
   * invalidation that the query answers by executing again through Kysely. Plugins must be on
   * this instance; a plugin added to one builder with `withPlugin` is not applied to delivered
   * results.
   */
  readonly db?: QueryExecutorProvider;
}

/** Callable wrapper; Kysely's `$call(live)` and direct `live(query)` are equivalent. */
export interface KyselyLiveQueries {
  <TQuery extends KyselyLiveSelectable>(query: TQuery): LiveQuery<KyselyLiveRow<TQuery>>;
  changes<TQuery extends KyselyLiveSelectable, TKey extends LiveKeyOf<KyselyLiveRow<TQuery>>>(
    query: TQuery,
    options: { readonly key: TKey },
  ): KeyedLiveQuery<KyselyLiveRow<TQuery>, TKey>;
  window<TQuery extends KyselyWindowSelectable, TKey extends LiveKeyOf<KyselyLiveRow<TQuery>>>(
    query: TQuery,
    options: { readonly key: TKey; readonly limit: number; readonly offset?: number },
  ): KeyedLiveQuery<KyselyLiveRow<TQuery>, TKey>;
  refresh(): Promise<void>;
  close(): Promise<void>;
}

/** The dialect's decoding and plugin chain, resolved once from the Kysely instance. */
function resultDecoder(
  db: QueryExecutorProvider,
): (result: QueryResult, compiled: CompiledQuery) => Promise<readonly object[]> {
  const executor: QueryExecutor = db.getExecutor();
  const adapter: unknown = executor.adapter;
  if (!(adapter instanceof MinnowKyselyAdapter)) {
    throw new TypeError(
      "createKyselyLiveQueries: db must be a Kysely instance on the Minnow dialect",
    );
  }
  const decoding = adapter.resultDecoding;
  return async (result, compiled) => {
    let transformed = {
      rows: decodedRows<Record<string, unknown>>(
        result.rows,
        result.columns,
        result.columnDomains,
        decoding,
      ),
    };
    for (const plugin of executor.plugins) {
      transformed = await plugin.transformResult({
        result: transformed,
        queryId: compiled.queryId,
      });
    }
    return transformed.rows;
  };
}

export function createKyselyLiveQueries(config: CreateKyselyLiveQueriesConfig): KyselyLiveQueries {
  const manager = new LiveQueryManager(config.driver, {
    ...(config.channelName === undefined ? {} : { channelName: config.channelName }),
    ...(config.pollIntervalMs === undefined ? {} : { pollIntervalMs: config.pollIntervalMs }),
  });
  const decode = config.db === undefined ? undefined : resultDecoder(config.db);
  const live = <TQuery extends KyselyLiveSelectable>(
    query: TQuery,
  ): LiveQuery<KyselyLiveRow<TQuery>> => {
    const compiled = query.compile();
    const statement = {
      kind: "sql-query" as const,
      sql: compiled.sql,
      params: kyselyQueryValues(compiled.parameters),
    };
    return manager.watch({
      query: statement,
      execute: async (signal) => query.execute(signal === undefined ? {} : { signal }),
      ...(decode === undefined
        ? {}
        : {
            decode: (result: QueryResult) =>
              decode(result, compiled) as Promise<ReadonlyArray<KyselyLiveRow<TQuery>>>,
          }),
    });
  };
  return Object.assign(live, {
    changes: <TQuery extends KyselyLiveSelectable, TKey extends LiveKeyOf<KyselyLiveRow<TQuery>>>(
      query: TQuery,
      options: { readonly key: TKey },
    ) => new KeyedLiveQuery(live(query), options),
    window: <TQuery extends KyselyWindowSelectable, TKey extends LiveKeyOf<KyselyLiveRow<TQuery>>>(
      query: TQuery,
      options: { readonly key: TKey; readonly limit: number; readonly offset?: number },
    ) => {
      if (!Number.isSafeInteger(options.limit) || options.limit <= 0) {
        throw new RangeError("Live query window limit must be a positive whole number");
      }
      if (
        options.offset !== undefined &&
        (!Number.isSafeInteger(options.offset) || options.offset < 0)
      ) {
        throw new RangeError("Live query window offset must be a non-negative whole number");
      }
      // Kysely's compiled query carries the final RootOperationNode after every plugin has
      // transformed it. Inspect that source of truth instead of reparsing the emitted SQL: it
      // preserves plugin semantics and keeps Minnow's complete SQL parser out of this adapter.
      const root = query.compile().query;
      if (
        root.kind !== "SelectQueryNode" ||
        root.orderBy === undefined ||
        root.orderBy.items.length === 0
      ) {
        throw new TypeError("Windowed live queries require a top-level ORDER BY");
      }
      let windowed = query.limit(options.limit);
      if (options.offset !== undefined) windowed = windowed.offset(options.offset);
      return new KeyedLiveQuery(live(windowed as TQuery), {
        key: options.key,
        maxRows: options.limit,
      });
    },
    refresh: () => manager.refresh(),
    close: () => manager.close(),
  });
}
