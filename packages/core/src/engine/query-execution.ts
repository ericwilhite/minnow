import {
  compileQuery,
  annotateAvgArgumentScales,
  compileStatement,
  createPreparedColumnarQuery,
  inferResultColumnDomains,
  expandFtsColumns,
  projectResultColumns,
  queryResultNeedsExternalization,
  resolveStatementDatetimes,
  subqueryResolutionSteps,
  transparentProjectionSource,
  type CompiledQuery,
  type CompiledStatement,
  type PreparedQuery,
  type QueryResult,
  type QueryValue,
  type SqlColumnSchema,
} from "./query.js";
import { MAX_CACHEABLE_TEXT_CHARACTERS } from "./cache-limits.js";
import {
  type CatalogProbe,
  type SegmentRecord,
  type SqlDomain,
  type TableColumnRecord,
  type TableRecord,
} from "../storage/types.js";
import { throwIfAborted } from "./cancellation.js";
import { QueryMemoryBudgetError, QueryMemoryContext } from "./memory.js";
import { type ColumnarTable, type QuerySpillStore } from "./vector.js";
import { type FtsStats } from "./fts.js";
import { type LeasedSnapshot } from "../transactions/index.js";
import { collectRealTableNames } from "./catalog-mutations.js";
import { chooseJoinOrder } from "./optimizer.js";
import type { BlockStore } from "../storage/types.js";

/** Distinct SQL statements whose optimized plans stay cached; plans are a few KB each. */
const PLAN_CACHE_LIMIT = 512;

export interface SegmentVisibilityCatalog {
  readonly transactions: ReadonlyMap<string, { readonly committedVersion: number | null }>;
  readonly segmentsByTable: ReadonlyMap<string, readonly SegmentRecord[]>;
  /** One staged writer exposed as a synthetic post-snapshot commit inside a transaction scope. */
  readonly overlayTransactionId?: string;
}

/** What one statement's execution cost, reported by the engine that ran it. */
export interface QueryExecutionStats {
  /**
   * Peak modeled execution memory for this statement, in bytes: the documented vector,
   * row-index, group/result payload, and ordering buffers, which is the same model
   * `executionMemoryBudgetBytes` bounds. Boxed snapshot preparation, JavaScript container
   * overhead, and allocator overhead are outside it. Not reported for a memo hit — nothing ran.
   */
  readonly peakMemoryBytes: number;
}

export interface QueryOptions {
  /**
   * Stops a read between bounded execution or storage batches. An abort never returns a partial
   * result and releases any reader lease and temporary spill owner before the promise rejects.
   */
  readonly signal?: AbortSignal;
  /**
   * Called once with what this execution cost, before the result is returned. Additive and
   * optional: the engine can report its own memory because it reserves before it allocates,
   * which is not something the storage layer or a caller could measure from outside.
   */
  readonly onStats?: (stats: QueryExecutionStats) => void;
  /**
   * false makes this statement compute its results instead of reusing any it has cached: the
   * probe-validated result memo, cached block results, and the columnar forms of derived and
   * windowed sources are all bypassed (the default true serves provably-fresh cached results
   * from each). Block and vector caches stay warm — those cache storage reads, not results.
   *
   * Useful for benchmarking execution itself, and for callers that re-run one statement in a
   * tight loop over changing external state. Note that replaying one statement over unchanging
   * data with the default on measures cache lookups, not query execution.
   */
  memoize?: boolean;
  readonly version?: number | null;
  /**
   * Values for the statement's `?`/`$n` placeholders, in order. Required exactly when the
   * statement has placeholders; the compiled plan is cached on the SQL text and re-bound per
   * execution, so parameterized queries skip re-parsing.
   */
  readonly params?: readonly QueryValue[];
  /**
   * Budget for the documented modeled vector, row-index, group/result payload, and ordering buffers.
   * Boxed snapshot preparation, JavaScript container overhead, returned-result lifetime, and browser
   * allocator overhead are not included in this Phase 7B-B model.
   */
  readonly executionMemoryBudgetBytes?: number;
  /** Forces durable temp pages; with an explicit budget, spill otherwise retries only after exhaustion. */
  readonly spillToStorage?: boolean;
  /** Maximum rows encoded in each merged spill page. */
  readonly spillPageRows?: number;
}

export interface QueryBatchCursorExecution {
  readonly batchRows: number;
  readonly signal: AbortSignal;
  readonly consumeFirstColumn?: (values: readonly QueryValue[]) => Promise<void>;
  readonly consume: (batch: QueryResult) => Promise<void>;
}

/** The columns a MATCH(*) document draws from: everything except booleans. */
export function searchableFtsColumns(
  table: TableRecord | undefined,
): readonly string[] | undefined {
  if (table === undefined) return undefined;
  return visibleTableColumns(table)
    .filter((column) => column.type !== "boolean")
    .map((column) => column.name);
}

export function visibleTableColumns(table: TableRecord): TableColumnRecord[] {
  return table.columns.filter((column) => column.hidden !== true);
}

export function positiveWholeNumber(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive whole number`);
  }
  return value;
}
interface QueryExecutionHost {
  readonly store: BlockStore;
  readonly now: () => Date;
  readonly withSharedCatalogSnapshot: <T>(
    names: readonly string[],
    action: (
      snapshot: LeasedSnapshot,
      realTables: Map<string, TableRecord>,
      visibility: SegmentVisibilityCatalog,
    ) => Promise<T>,
    probe?: CatalogProbe,
  ) => Promise<T>;
  readonly findRealBlockTables: (plan: CompiledQuery) => Promise<Map<string, TableRecord>>;
  readonly blockSegmentVisibility: (
    realTables: ReadonlyMap<string, TableRecord>,
  ) => Promise<SegmentVisibilityCatalog>;
  readonly prepareBlockInputs: (
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    memory: QueryMemoryContext,
    realTables: ReadonlyMap<string, TableRecord>,
    typedSchemas: Map<string, SqlColumnSchema[]>,
    extraInputs?: ReadonlyMap<string, ColumnarTable>,
    cacheResults?: boolean,
    allowSpill?: boolean,
    forceSpill?: boolean,
    spillPageRows?: number,
    signal?: AbortSignal,
  ) => Promise<Map<string, ColumnarTable>>;
  readonly effectiveQueryOptions: (options: QueryOptions) => QueryOptions;
  readonly applyCatalogRewrites: (
    plan: CompiledQuery,
    probe?: CatalogProbe,
  ) => Promise<CompiledQuery>;
  readonly leasedSpillStore: () => QuerySpillStore;
  readonly canStreamPlanShape: (plan: CompiledQuery, options: QueryOptions) => boolean;
  readonly queryStreamed: (
    plan: CompiledQuery,
    options: QueryOptions,
    spillPageRows: number | undefined,
    probe?: CatalogProbe,
    cursor?: QueryBatchCursorExecution,
  ) => Promise<QueryResult | undefined>;
  readonly executeBlockCached: (
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    memory: QueryMemoryContext,
    realTables: ReadonlyMap<string, TableRecord>,
    typedSchemas: Map<string, SqlColumnSchema[]>,
    cacheResults?: boolean,
    allowSpill?: boolean,
    forceSpill?: boolean,
    spillPageRows?: number,
    signal?: AbortSignal,
  ) => Promise<QueryResult>;
  readonly ftsIndexStats: (
    plan: CompiledQuery,
    realTables: ReadonlyMap<string, TableRecord>,
    snapshot: LeasedSnapshot,
    visibility?: SegmentVisibilityCatalog,
  ) => Promise<Map<string, FtsStats> | undefined>;
  readonly withLeasedSnapshot: <T>(
    version: number | null | undefined,
    action: (snapshot: LeasedSnapshot) => Promise<T>,
  ) => Promise<T>;
}
/** Owns immutable compile caches and the streaming/spill preparation lifecycle. Physical
 * readers remain host operations; no extra dispatch is introduced inside row/vector loops. */
export class QueryExecution {
  readonly #planCache = new Map<string, CompiledQuery>();
  readonly #statementCache = new Map<string, CompiledStatement>();
  readonly #effectiveQueryOptions: QueryExecutionHost["effectiveQueryOptions"];
  readonly #now: QueryExecutionHost["now"];
  readonly #executeBlockCached: QueryExecutionHost["executeBlockCached"];
  readonly #ftsIndexStats: QueryExecutionHost["ftsIndexStats"];
  readonly #prepareBlockInputs: QueryExecutionHost["prepareBlockInputs"];
  readonly #findRealBlockTables: QueryExecutionHost["findRealBlockTables"];
  readonly #withLeasedSnapshot: QueryExecutionHost["withLeasedSnapshot"];
  readonly #blockSegmentVisibility: QueryExecutionHost["blockSegmentVisibility"];
  readonly #withSharedCatalogSnapshot: QueryExecutionHost["withSharedCatalogSnapshot"];
  readonly #store: QueryExecutionHost["store"];
  readonly #applyCatalogRewrites: QueryExecutionHost["applyCatalogRewrites"];
  readonly #canStreamPlanShape: QueryExecutionHost["canStreamPlanShape"];
  readonly #queryStreamed: QueryExecutionHost["queryStreamed"];
  readonly #leasedSpillStore: QueryExecutionHost["leasedSpillStore"];
  constructor(host: QueryExecutionHost) {
    this.#effectiveQueryOptions = host.effectiveQueryOptions;
    this.#now = host.now;
    this.#executeBlockCached = host.executeBlockCached;
    this.#ftsIndexStats = host.ftsIndexStats;
    this.#prepareBlockInputs = host.prepareBlockInputs;
    this.#findRealBlockTables = host.findRealBlockTables;
    this.#withLeasedSnapshot = host.withLeasedSnapshot;
    this.#blockSegmentVisibility = host.blockSegmentVisibility;
    this.#withSharedCatalogSnapshot = host.withSharedCatalogSnapshot;
    this.#store = host.store;
    this.#applyCatalogRewrites = host.applyCatalogRewrites;
    this.#canStreamPlanShape = host.canStreamPlanShape;
    this.#queryStreamed = host.queryStreamed;
    this.#leasedSpillStore = host.leasedSpillStore;
  }
  get planCount(): number {
    return this.#planCache.size;
  }
  get statementCount(): number {
    return this.#statementCache.size;
  }
  clearPlans(): void {
    this.#planCache.clear();
  }
  clear(): void {
    this.#planCache.clear();
    this.#statementCache.clear();
  }

  compile(sql: string): CompiledQuery {
    const cacheable = sql.length <= MAX_CACHEABLE_TEXT_CHARACTERS;
    const cached = cacheable ? this.#planCache.get(sql) : undefined;
    if (cached !== undefined) {
      this.#planCache.delete(sql);
      this.#planCache.set(sql, cached);
      return cached;
    }
    const plan = compileQuery(sql);
    if (!cacheable) return plan;
    this.#planCache.set(sql, plan);
    if (this.#planCache.size > PLAN_CACHE_LIMIT) {
      const oldest = this.#planCache.keys().next().value;
      if (oldest !== undefined) this.#planCache.delete(oldest);
    }
    return plan;
  }

  compileStatement(sql: string): CompiledStatement {
    const cacheable = sql.length <= MAX_CACHEABLE_TEXT_CHARACTERS;
    const cached = cacheable ? this.#statementCache.get(sql) : undefined;
    if (cached !== undefined) {
      this.#statementCache.delete(sql);
      this.#statementCache.set(sql, cached);
      return cached;
    }
    const statement = compileStatement(sql);
    if (!cacheable) return statement;
    this.#statementCache.set(sql, statement);
    if (this.#statementCache.size > PLAN_CACHE_LIMIT) {
      const oldest = this.#statementCache.keys().next().value;
      if (oldest !== undefined) this.#statementCache.delete(oldest);
    }
    return statement;
  }

  async prepare(
    plan: CompiledQuery,
    options: QueryOptions = {},
    probe?: CatalogProbe,
  ): Promise<PreparedQuery> {
    options = this.#effectiveQueryOptions(options);
    throwIfAborted(options.signal);
    // One statement clock for the whole plan tree, fixed before any nested block executes on
    // its own: a scalar subquery reading CURRENT_TIMESTAMP is resolved here, not left for the
    // executor that only ever sees the block it runs.
    if (plan.usesStatementDatetime === true) plan = resolveStatementDatetimes(plan, this.#now());
    // The ORDER-BY-expression desugar's wrapper is projection-only: prepare the inner block
    // directly (no derived materialization) and project each result to the visible aliases,
    // so `.search()` costs the same whether or not the caller also selects the score.
    const wrapper = transparentProjectionSource(plan);
    if (wrapper !== undefined) {
      const prepared = await this.prepare(wrapper.inner, options, probe);
      throwIfAborted(options.signal);
      return {
        sql: prepared.sql,
        tables: prepared.tables,
        get memoryUsage() {
          return prepared.memoryUsage;
        },
        execute: () => projectResultColumns(prepared.execute(), wrapper.aliases),
        executeAsync: async (asyncOptions) =>
          projectResultColumns(await prepared.executeAsync(asyncOptions), wrapper.aliases),
        executeBatches: (batchOptions, consume) =>
          prepared.executeBatches(batchOptions, (batch) =>
            consume(projectResultColumns(batch, wrapper.aliases)),
          ),
        close: () => prepared.close(),
      };
    }
    const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
    try {
      let columnarTables = new Map<string, ColumnarTable>();
      let resolvedPlan = plan;
      let ftsStats: Map<string, FtsStats> | undefined;
      let outputNeedsExternalization = true;
      let outputColumnDomains: Array<SqlDomain | null> = [];
      const prepareAtSnapshot = async (
        snapshot: LeasedSnapshot,
        realTables: Map<string, TableRecord>,
        visibility: SegmentVisibilityCatalog,
      ): Promise<void> => {
        throwIfAborted(options.signal);
        const typedSchemas = new Map<string, SqlColumnSchema[]>(
          [...realTables.values()].map((table) => [
            table.name,
            table.columns.map(({ name, type, integer, sqlDomain }) => ({
              name,
              type,
              ...(integer === true ? { integer: true as const } : {}),
              ...(sqlDomain === undefined ? {} : { sqlDomain }),
            })),
          ]),
        );
        // MATCH(*)/BM25(*) expand against the catalog first — copy-on-write, so the compile
        // cache's plan (and the parity tests) keep "*" — and subquery resolution then collects
        // its steps from the expanded plan so substitutions land in the object that executes.
        const expandedPlan = expandFtsColumns(plan, (tableName) =>
          searchableFtsColumns(realTables.get(tableName)),
        );
        const resolution = subqueryResolutionSteps(expandedPlan);
        for (const step of resolution.steps) {
          throwIfAborted(options.signal);
          step.substitute(
            await this.#executeBlockCached(
              step.block,
              snapshot,
              visibility,
              memory,
              realTables,
              typedSchemas,
              options.memoize !== false,
              options.spillToStorage !== false,
              options.spillToStorage === true,
              options.spillPageRows,
              options.signal,
            ),
          );
          throwIfAborted(options.signal);
        }
        resolvedPlan = resolution.plan;
        // Index-served BM25 statistics, computed against the same catalog snapshot the pruner
        // reads, so a pruned scoring scan always carries exact corpus numbers.
        ftsStats = await this.#ftsIndexStats(resolvedPlan, realTables, snapshot, visibility);
        throwIfAborted(options.signal);
        columnarTables = await this.#prepareBlockInputs(
          resolvedPlan,
          snapshot,
          visibility,
          memory,
          realTables,
          typedSchemas,
          undefined,
          // memoize: false means "compute this statement's results" — that covers the
          // columnar forms of derived and windowed sources too, not just the result memo.
          options.memoize !== false,
          options.spillToStorage !== false,
          options.spillToStorage === true,
          options.spillPageRows,
          options.signal,
        );
        throwIfAborted(options.signal);
        // After input preparation, because executing the nested blocks registered their
        // synthetic source schemas in typedSchemas — same reasoning as the domain inference.
        resolvedPlan = annotateAvgArgumentScales(resolvedPlan, typedSchemas);
        outputNeedsExternalization = queryResultNeedsExternalization(resolvedPlan, typedSchemas);
        outputColumnDomains = inferResultColumnDomains(resolvedPlan, typedSchemas);
      };
      if (options.version !== undefined) {
        // Explicit time travel keeps the per-call lease and version-anchored reads.
        const realTables = await this.#findRealBlockTables(plan);
        throwIfAborted(options.signal);
        await this.#withLeasedSnapshot(options.version, async (snapshot) => {
          const visibility = await this.#blockSegmentVisibility(realTables);
          throwIfAborted(options.signal);
          await prepareAtSnapshot(snapshot, realTables, visibility);
        });
      } else {
        await this.#withSharedCatalogSnapshot(
          collectRealTableNames(plan),
          prepareAtSnapshot,
          probe,
        );
      }
      throwIfAborted(options.signal);
      return createPreparedColumnarQuery(
        chooseJoinOrder(resolvedPlan, columnarTables),
        columnarTables,
        memory,
        {
          ...(ftsStats === undefined ? {} : { ftsStats }),
          outputNeedsExternalization,
          outputColumnDomains,
        },
      );
    } catch (error) {
      memory.close();
      throw error;
    }
  }

  /**
   * The one read pipeline: every compiled plan — SQL text, the typed builder, and live-query
   * re-runs — routes through the same streaming-first execution, so builder/SQL parity holds
   * for the execution path as well as the plan.
   */
  async run(
    plan: CompiledQuery,
    options: QueryOptions = {},
    probe?: CatalogProbe,
  ): Promise<QueryResult> {
    options = this.#effectiveQueryOptions(options);
    throwIfAborted(options.signal);
    // One freshness probe per query: read here unless the caller already has one, and handed
    // to the view lookup and the catalog state below, which would otherwise probe again each.
    probe ??= await this.#store.getCatalogProbe();
    throwIfAborted(options.signal);
    plan = await this.#applyCatalogRewrites(plan, probe);
    throwIfAborted(options.signal);
    const spillPageRows =
      options.spillPageRows === undefined
        ? undefined
        : positiveWholeNumber(options.spillPageRows, "Query spill page rows");
    if (this.#canStreamPlanShape(plan, options)) {
      const streamed = await this.#queryStreamed(plan, options, spillPageRows, probe);
      if (streamed !== undefined) return streamed;
    } else {
      // An ORDER-BY-expression wrapper is a pure projection over the real query: stream the
      // inner block and project the hidden ordering column away, so the wrap never costs a
      // query its streaming eligibility.
      const wrapper = transparentProjectionSource(plan);
      if (wrapper !== undefined && this.#canStreamPlanShape(wrapper.inner, options)) {
        const streamed = await this.#queryStreamed(wrapper.inner, options, spillPageRows, probe);
        if (streamed !== undefined) return projectResultColumns(streamed, wrapper.aliases);
      }
    }
    const prepared = await this.prepare(plan, options, probe);
    throwIfAborted(options.signal);
    // Read the peak before close(): closing releases the context and zeroes what it tracked.
    const report = (result: QueryResult): QueryResult => {
      options.onStats?.({ peakMemoryBytes: prepared.memoryUsage.peakBytes });
      return result;
    };
    try {
      const spill = options.spillToStorage ?? options.executionMemoryBudgetBytes !== undefined;
      if (!spill) {
        const result = prepared.execute();
        throwIfAborted(options.signal);
        return report(result);
      }
      if (options.spillToStorage !== true) {
        try {
          const result = prepared.execute();
          throwIfAborted(options.signal);
          return report(result);
        } catch (error) {
          if (!(error instanceof QueryMemoryBudgetError)) throw error;
        }
      }
      return report(
        await prepared.executeAsync({
          ...(spillPageRows === undefined ? {} : { spillPageRows }),
          spillStore: this.#leasedSpillStore(),
          ...(options.signal === undefined ? {} : { signal: options.signal }),
        }),
      );
    } finally {
      prepared.close();
    }
  }
}
