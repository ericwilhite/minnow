import {
  type CompiledQuery,
  type CompiledStatement,
  type QueryResult,
  type QueryRow,
  type QueryValue,
} from "../query.js";
import { type AnyTable, type SchemaDefinition } from "../schema.js";
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
  type TableExpression,
  type TypedQueryEnvelope,
} from "./types.js";

/**
 * The Kysely-style facade. `BrowserDb<DB>` wraps either the in-worker `BrowserDatabase` or the
 * main-thread `BrowserDatabaseClient` (both satisfy `DslDriver` structurally) and hands out
 * typed builders: `selectFrom`, `insertInto`, `updateTable`, `deleteFrom`, `with`, and live
 * queries through the builders' `.live()`. `DB` is derived from the runtime schema with
 * `InferDatabase<typeof appSchema>`; migrations stay on the driver (`driver.migrate(schema)`).
 */

export interface DriverLiveSet {
  subscribe(
    query: string | { kind: "typed-query"; plan: CompiledQuery },
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

/** The slice of BrowserDatabase / BrowserDatabaseClient the facade drives. */
export interface DslDriver {
  run<TRow>(query: { kind: "typed-query"; plan: CompiledQuery; __row?: TRow }): Promise<TRow[]>;
  insertBatch(
    tableName: string,
    input: { columns: Readonly<Record<string, readonly QueryValue[]>> },
  ): Promise<{ rowCount: number }>;
  upsertBatch(
    tableName: string,
    input: { columns: Readonly<Record<string, readonly QueryValue[]>> },
  ): Promise<{ rowCount: number }>;
  runStatement(
    statement: CompiledStatement,
    options?: { returning?: readonly string[] | "*" },
  ): Promise<{ kind: string; rowCount?: number; returnedRows?: QueryRow[] }>;
  execute(sql: string): Promise<{ kind: string; result?: { rows: QueryRow[] } }>;
  liveQueries(options?: DslLiveOptions): DriverLiveSet;
}

export interface BrowserDbOptions {
  /**
   * The runtime schema the database was migrated with. Inserts then pad omitted nullable
   * columns with null (the engine's batch API takes every column); without it, an insert must
   * list every column itself.
   */
  schema?: SchemaDefinition<readonly AnyTable[]>;
  /** Options for the shared live-query set behind `.live()`; created lazily on first use. */
  live?: DslLiveOptions;
}

type EmptyContext = Record<never, AnyRow>;

/**
 * Creates the typed facade with `DB` inferred from the runtime schema: one `schema(...)` value
 * drives migrations, insert padding, and every builder's column types, with no generic passing
 * and no way for the type-level database to drift from the schema the driver migrated with.
 *
 * ```ts
 * const db = createBrowserDb(database, { schema: appSchema });
 * ```
 *
 * When builders are exported across module boundaries, prefer a named interface so declaration
 * emit and hovers print `DB` instead of the expanded schema:
 *
 * ```ts
 * interface DB extends InferDatabase<typeof appSchema> {}
 * const db = new BrowserDb<DB>(database, { schema: appSchema });
 * ```
 */
export function createBrowserDb<TTables extends readonly AnyTable[]>(
  driver: DslDriver,
  options: Omit<BrowserDbOptions, "schema"> & { schema: SchemaDefinition<TTables> },
): BrowserDb<InferDatabase<SchemaDefinition<TTables>>> {
  return new BrowserDb(driver, options);
}

interface SharedLiveSet {
  set?: DriverLiveSet | undefined;
}

export class BrowserDb<in out DB> {
  readonly #driver: DslDriver;
  readonly #options: BrowserDbOptions;
  readonly #ctes: readonly CteDefinition[];
  readonly #liveBox: SharedLiveSet;

  constructor(
    driver: DslDriver,
    options: BrowserDbOptions = {},
    ctes: readonly CteDefinition[] = [],
    liveBox: SharedLiveSet = {},
  ) {
    this.#driver = driver;
    this.#options = options;
    this.#ctes = ctes;
    this.#liveBox = liveBox;
  }

  #liveServices(): LiveQueryServices {
    return {
      subscribe: async (query: TypedQueryEnvelope<unknown>, handlers) => {
        this.#liveBox.set ??= this.#driver.liveQueries(this.#options.live ?? {});
        return this.#liveBox.set.subscribe(query, handlers);
      },
    };
  }

  #services(): ExecuteServices {
    return {
      run: (query) => this.#driver.run(query),
      live: this.#liveServices(),
    };
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
      insertBatch: (tableName, input) => this.#driver.insertBatch(tableName, input),
      upsertBatch: (tableName, input) => this.#driver.upsertBatch(tableName, input),
      runStatement: (statement, options) => this.#driver.runStatement(statement, options),
      tableColumns: (tableName) => {
        const definition = schemaTables?.find(({ name }) => name === tableName);
        return definition === undefined ? undefined : Object.keys(definition.columns);
      },
    };
  }

  insertInto<TTable extends keyof DB & string>(table: TTable): InsertQueryBuilder<DB, TTable> {
    return new InsertQueryBuilder(this.#mutationServices(), table);
  }

  updateTable<TTable extends keyof DB & string>(table: TTable): UpdateQueryBuilder<DB, TTable> {
    return new UpdateQueryBuilder(this.#mutationServices(), table);
  }

  deleteFrom<TTable extends keyof DB & string>(table: TTable): DeleteQueryBuilder<DB, TTable> {
    return new DeleteQueryBuilder(this.#mutationServices(), table);
  }

  /**
   * Declares a common table expression usable in `selectFrom`/joins of queries created from the
   * returned facade, exactly as SQL `WITH name AS (...)`.
   */
  with<TName extends string, TCteCtx, TCteRow extends AnyRow>(
    name: TName,
    factory: (creator: BrowserDb<DB>) => SelectQueryBuilder<DB, TCteCtx, TCteRow>,
  ): BrowserDb<DB & Record<TName, TCteRow>> {
    const builder = factory(this) as BlockCompilable;
    return new BrowserDb(
      this.#driver,
      this.#options,
      [...this.#ctes, { name, builder }],
      this.#liveBox,
    );
  }

  /** @internal Raw-SQL escape hatch used by the `sql` template tag. */
  async $executeRaw(sqlText: string): Promise<QueryRow[]> {
    const result = await this.#driver.execute(sqlText);
    return result.result?.rows ?? [];
  }

  /** Closes the shared live-query set, if one was created. */
  async close(): Promise<void> {
    const set = this.#liveBox.set;
    this.#liveBox.set = undefined;
    if (set !== undefined) await set.close();
  }
}
