import {
  type CompiledQuery,
  type QueryResult,
  type QueryRow,
  type QueryValue,
} from "@minnowdb/core/plan";
import {
  type AnyTable,
  type AnyView,
  type Catalog,
  type ExecuteResult,
  type QueryOptions,
  type SchemaDefinition,
} from "@minnowdb/core";
import { type LiveQueryServices, type LiveSubscriptionHandle } from "./live-query.js";
import {
  DeleteQueryBuilder,
  InsertQueryBuilder,
  UpdateQueryBuilder,
  type MutationServices,
} from "./mutations.js";
import {
  isAliasedSelectQuery,
  parseTableExpression,
  SelectQueryBuilder,
  type AliasedSelectQuery,
  type CteDefinition,
  type ExecuteServices,
} from "./select-query-builder.js";
import {
  type AnyRow,
  type BlockCompilable,
  type ContextWithTable,
  type InferDatabase,
  type SelectRowOf,
  type TableExpression,
  type ViewShape,
  type WritableTable,
} from "./types.js";
import { type RawSqlFragment, type RenderedSql } from "./sql-tag.js";

/**
 * The Kysely-style client. `Minnow<DB>` wraps either the in-worker `MinnowDatabase` or the
 * main-thread `MinnowDatabaseClient` (both satisfy `DslDriver` structurally) and hands out
 * typed builders: `selectFrom`, `insertInto`, `updateTable`, `deleteFrom`, `with`, and live
 * queries through the builders' `.live()`. `DB` is derived from the runtime schema with
 * `InferDatabase<typeof appSchema>`; migrations stay on the driver (`driver.migrate(schema)`).
 */

export interface DriverLiveSet {
  subscribe(
    query:
      | string
      | { kind: "sql-query"; sql: string; params: readonly QueryValue[] }
      | { kind: "typed-query"; plan: CompiledQuery },
    options: {
      onChange(result: QueryResult): void;
      onError?(error: unknown): void;
      onComplete?(): void;
    },
  ): Promise<LiveSubscriptionHandle>;
  close(): void | Promise<void>;
}

export interface DslLiveOptions {
  /** BroadcastChannel name for cross-tab commit hints (worker client). */
  channelName?: string;
  pollIntervalMs?: number;
}

export interface DslWriteSession {
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult>;
  insertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  upsertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
}

/** The part of MinnowDatabase / MinnowDatabaseClient used by the typed client. */
export interface DslDriver {
  introspect?(): Promise<Catalog>;
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  insertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  upsertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult>;
  write?<T>(
    action: (session: DslWriteSession) => Promise<T>,
  ): Promise<{ result: T; version: number | null }>;
  liveQueries?(options?: DslLiveOptions): DriverLiveSet;
}

export interface MinnowOptions {
  /**
   * The runtime schema the database was migrated with. Passing it avoids a catalog lookup and
   * lets inserts run user-defined default functions. Without it, column names come from the
   * driver's live catalog.
   */
  schema?: SchemaDefinition<readonly AnyTable[]>;
  /** Options for the shared live-query set behind `.live()`; created lazily on first use. */
  live?: DslLiveOptions;
}

type EmptyContext = Record<never, AnyRow>;

async function mapWithConcurrency<TValue, TResult>(
  values: readonly TValue[],
  concurrency: number,
  action: (value: TValue) => Promise<TResult>,
): Promise<TResult[]> {
  const results = new Array<TResult>(values.length);
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, values.length) }, async () => {
    for (;;) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= values.length) return;
      results[index] = await action(values[index] as TValue);
    }
  });
  await Promise.all(workers);
  return results;
}

/**
 * Creates the typed client. The recommended form names the database type once, so every hover,
 * error, and emitted declaration prints `Minnow<DB>` instead of the fully expanded schema —
 * which matters as soon as the schema has more than a table or two:
 *
 * ```ts
 * interface DB extends InferDatabase<typeof appSchema> {}
 * const db = createMinnow<DB>(database, { schema: appSchema });
 * ```
 *
 * Without the type argument, `DB` is inferred from the runtime schema value — fine for
 * scratch code and small scripts, at the cost of expanded types in tooling:
 *
 * ```ts
 * const db = createMinnow(database, { schema: appSchema });
 * ```
 */
export function createMinnow<
  TTables extends readonly AnyTable[],
  TViews extends readonly AnyView[],
>(
  driver: DslDriver,
  options: Omit<MinnowOptions, "schema"> & { schema: SchemaDefinition<TTables, TViews> },
): Minnow<InferDatabase<SchemaDefinition<TTables, TViews>>>;
export function createMinnow<DB>(driver: DslDriver, options?: MinnowOptions): Minnow<DB>;
export function createMinnow<DB>(driver: DslDriver, options: MinnowOptions = {}): Minnow<DB> {
  return new Minnow<DB>(driver, options);
}

interface SharedLiveSet {
  set?: DriverLiveSet | undefined;
}

/** One db.search hit: the owning table, the row (without the score alias), and its BM25 score. */
export type SearchHit<DB, TTable extends WritableTable<DB> = WritableTable<DB>> =
  TTable extends WritableTable<DB>
    ? { table: TTable; row: SelectRowOf<DB[TTable]>; score: number }
    : never;

export class Minnow<in out DB> {
  readonly #driver: DslDriver;
  readonly #options: MinnowOptions;
  readonly #ctes: readonly CteDefinition[];
  readonly #liveBox: SharedLiveSet;

  constructor(
    driver: DslDriver,
    options: MinnowOptions = {},
    ctes: readonly CteDefinition[] = [],
    liveBox: SharedLiveSet = {},
  ) {
    this.#driver = driver;
    this.#options = options;
    this.#ctes = ctes;
    this.#liveBox = liveBox;
  }

  /**
   * The driver this client was created with — the `MinnowDatabase` or `MinnowDatabaseClient`
   * behind it. Tools handed only the client (devtools, inspectors, schema browsers) reach the
   * catalog and the raw SQL entry points through here; application code should keep its own
   * reference to the driver rather than reaching back through the client.
   */
  get driver(): DslDriver {
    return this.#driver;
  }

  #liveServices(): LiveQueryServices | undefined {
    const createLiveSet = this.#driver.liveQueries?.bind(this.#driver);
    if (createLiveSet === undefined) return undefined;
    return {
      subscribe: async (query, handlers) => {
        this.#liveBox.set ??= createLiveSet(this.#options.live ?? {});
        return this.#liveBox.set.subscribe(query, handlers);
      },
    };
  }

  #services(): ExecuteServices {
    return {
      query: async <TRow>(rendered: RenderedSql) =>
        (await this.#driver.query(rendered.sql, { params: rendered.params })).rows as TRow[],
      live: this.#liveServices(),
    };
  }

  async #catalog(): Promise<Catalog> {
    const introspect = this.#driver.introspect?.bind(this.#driver);
    if (introspect === undefined) {
      throw new TypeError("This operation needs schema options or a driver with introspect()");
    }
    // The catalog can change through migrations, raw DDL, or another connection. Read it for
    // each catalog-backed operation so inserts and search never work from an old schema.
    return introspect();
  }

  selectFrom<TE extends TableExpression<DB>>(
    table: TE,
  ): SelectQueryBuilder<DB, ContextWithTable<DB, EmptyContext, TE>, EmptyContext>;
  selectFrom<TDerivedRow extends AnyRow, TAlias extends string>(
    derived: AliasedSelectQuery<TDerivedRow, TAlias>,
  ): SelectQueryBuilder<DB, Record<TAlias, TDerivedRow>, EmptyContext>;
  /* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-return --
     the implementation signature erases the context; the overloads above carry precise types. */
  selectFrom(
    table: string | AliasedSelectQuery<unknown, string>,
  ): SelectQueryBuilder<DB, any, any> {
    if (isAliasedSelectQuery(table)) {
      return SelectQueryBuilder.create(
        { kind: "derived", builder: table.builder, alias: table.alias },
        this.#ctes,
        this.#services(),
      );
    }
    const { table: name, alias } = parseTableExpression(table);
    return SelectQueryBuilder.create(
      { kind: "table", table: name, alias },
      this.#ctes,
      this.#services(),
    );
  }
  /* eslint-enable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-return */

  #mutationServices(): MutationServices {
    const schemaTables = this.#options.schema?.tables;
    return {
      insertBatch: (tableName, rows) => this.#driver.insertBatch(tableName, rows),
      upsertBatch: (tableName, rows) => this.#driver.upsertBatch(tableName, rows),
      execute: (sqlText, params) => this.#driver.execute(sqlText, params),
      tableColumns: async (tableName) => {
        const definition = schemaTables?.find(({ name }) => name === tableName);
        if (definition !== undefined) return Object.keys(definition.columns);
        return (await this.#catalog()).tables
          .find(({ name }) => name === tableName)
          ?.columns.map(({ name }) => name);
      },
      columnDefaultFns: (tableName) => {
        const definition = schemaTables?.find(({ name }) => name === tableName);
        if (definition === undefined) return undefined;
        let fns: Record<string, () => QueryValue> | undefined;
        for (const [name, columnDefinition] of Object.entries(definition.columns)) {
          const fill = columnDefinition.defaultFn;
          if (fill !== undefined) (fns ??= {})[name] = fill;
        }
        return fns;
      },
    };
  }

  insertInto<TTable extends WritableTable<DB>>(table: TTable): InsertQueryBuilder<DB, TTable> {
    return new InsertQueryBuilder(this.#mutationServices(), table);
  }

  updateTable<TTable extends WritableTable<DB>>(table: TTable): UpdateQueryBuilder<DB, TTable> {
    return new UpdateQueryBuilder(this.#mutationServices(), table);
  }

  deleteFrom<TTable extends WritableTable<DB>>(table: TTable): DeleteQueryBuilder<DB, TTable> {
    return new DeleteQueryBuilder(this.#mutationServices(), table);
  }

  /** Runs typed builders and SQL together as one atomic write. A thrown error rolls it all back. */
  async transaction<TResult>(
    action: (transaction: Minnow<DB>) => Promise<TResult>,
  ): Promise<TResult> {
    const write = this.#driver.write?.bind(this.#driver);
    if (write === undefined) throw new TypeError("This driver does not support transactions");
    const introspect = this.#driver.introspect?.bind(this.#driver);
    const { result } = await write(async (session) => {
      const transactionDriver: DslDriver = {
        ...(introspect === undefined ? {} : { introspect }),
        query: (sqlText, options) => session.query(sqlText, options),
        execute: (sqlText, params) => session.execute(sqlText, params),
        insertBatch: (tableName, rows) => session.insertBatch(tableName, rows),
        upsertBatch: (tableName, rows) => session.upsertBatch(tableName, rows),
        write: async () => {
          throw new TypeError("Nested transactions are not supported");
        },
      };
      return action(new Minnow<DB>(transactionDriver, this.#options, this.#ctes));
    });
    return result;
  }

  /**
   * Declares a common table expression usable in `selectFrom`/joins of queries created from the
   * returned client, exactly as SQL `WITH name AS (...)`.
   */
  with<TName extends string, TCteCtx, TCteRow extends AnyRow>(
    name: TName,
    factory: (creator: Minnow<DB>) => SelectQueryBuilder<DB, TCteCtx, TCteRow>,
  ): Minnow<DB & Record<TName, ViewShape<TCteRow>>> {
    const builder = factory(this) as BlockCompilable;
    return new Minnow(
      this.#driver,
      this.#options,
      [...this.#ctes, { name, builder }],
      this.#liveBox,
    );
  }

  /**
   * Zero-ceremony document search across tables: every searched table (all catalog tables by
   * default; `options.tables` narrows the set) runs a MATCH(*)-filtered, BM25-ordered scan,
   * and the per-table hits merge into one relevance-ranked list. Work is bounded across tables
   * so a large catalog does not start every query at once.
   */
  async search<TTable extends WritableTable<DB> = WritableTable<DB>>(
    query: string,
    options: {
      tables?: readonly TTable[];
      limit?: number;
      concurrency?: number;
    } = {},
  ): Promise<Array<SearchHit<DB, TTable>>> {
    const schemaTables = this.#options.schema?.tables;
    const columnEntries =
      schemaTables === undefined
        ? (await this.#catalog()).tables.map(
            (table) => [table.name, table.columns.map(({ name }) => name)] as const,
          )
        : schemaTables.map((table) => [table.name, Object.keys(table.columns)] as const);
    const columnsByTable = new Map(columnEntries);
    const tableNames = options.tables ?? ([...columnsByTable.keys()] as TTable[]);
    const limit = options.limit ?? 10;
    const concurrency = options.concurrency ?? 4;
    if (!Number.isSafeInteger(concurrency) || concurrency < 1) {
      throw new TypeError("search() concurrency must be a positive whole number");
    }
    const perTable = await mapWithConcurrency(tableNames, concurrency, async (tableName) => {
      const columnNames = columnsByTable.get(tableName);
      if (columnNames === undefined) throw new TypeError(`Unknown search table: ${tableName}`);
      /* eslint-disable-next-line @typescript-eslint/no-explicit-any --
           per-table column lists are only known at runtime; hits erase the row type anyway. */
      const builder = this.selectFrom(tableName as any) as unknown as SelectQueryBuilder<
        DB,
        Record<string, AnyRow>,
        Record<string, QueryValue>
      >;
      // The score is selected under an internal alias so hits can rank across tables, then
      // stripped from the row; user-facing rows never carry a synthetic column. The alias
      // grows parentheses until it collides with no real column name.
      let scoreAlias = "(search score)";
      while (columnNames.includes(scoreAlias)) scoreAlias = `(${scoreAlias})`;
      let rows: Array<Record<string, QueryValue>>;
      try {
        rows = await builder
          .select((eb) => [...columnNames, eb.fn.bm25("*", query).as(scoreAlias)])
          .search(query)
          .limit(limit)
          .execute();
      } catch (error) {
        // A schema can list tables that don't exist yet or hold nothing searchable; those
        // tables simply contribute no hits. Anything else is a real failure.
        const message = error instanceof Error ? error.message : "";
        if (
          message.startsWith("Table not found:") ||
          message.startsWith("Unknown table:") ||
          message.includes("no searchable columns")
        ) {
          return [];
        }
        throw error;
      }
      return rows.map((row) => {
        const { [scoreAlias]: score, ...rest } = row;
        return { table: tableName, row: rest, score: typeof score === "number" ? score : 0 };
      });
    });
    return perTable
      .flat()
      .sort((left, right) => right.score - left.score)
      .slice(0, limit) as Array<SearchHit<DB, TTable>>;
  }

  /** Runs a SELECT fragment through the SQL parser, plan cache, and query executor. */
  async query<TRow = QueryRow>(
    input: string | RawSqlFragment<TRow>,
    options: QueryOptions = {},
  ): Promise<TRow[]> {
    if (typeof input === "string") {
      return (await this.#driver.query(input, options)).rows as TRow[];
    }
    if (options.params !== undefined) {
      throw new TypeError("A sql fragment already owns its parameters; do not pass options.params");
    }
    const rendered = input.render();
    return (await this.#driver.query(rendered.sql, { ...options, params: rendered.params }))
      .rows as TRow[];
  }

  /** Runs one SQL statement and returns a result that says what the statement did. */
  async execute(
    input: string | RawSqlFragment<unknown>,
    params?: readonly QueryValue[],
  ): Promise<ExecuteResult> {
    if (typeof input === "string") return this.#driver.execute(input, params);
    if (params !== undefined) {
      throw new TypeError("A sql fragment already owns its parameters; do not pass params");
    }
    const rendered = input.render();
    return this.#driver.execute(rendered.sql, rendered.params);
  }

  /** @internal Raw-SQL escape hatch used by the `sql` template tag. */
  async $executeRaw(sqlText: string, params?: readonly QueryValue[]): Promise<QueryRow[]> {
    const result = await this.#driver.query(sqlText, params === undefined ? undefined : { params });
    return result.rows;
  }

  /** Closes the shared live-query set, if one was created. */
  async close(): Promise<void> {
    const set = this.#liveBox.set;
    this.#liveBox.set = undefined;
    if (set !== undefined) await set.close();
  }
}
