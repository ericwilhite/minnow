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
 * The Kysely-style facade. `Minnow<DB>` wraps either the in-worker `MinnowDatabase` or the
 * main-thread `MinnowDatabaseClient` (both satisfy `DslDriver` structurally) and hands out
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

/** The slice of MinnowDatabase / MinnowDatabaseClient the facade drives. */
export interface DslDriver {
  run<TRow>(query: { kind: "typed-query"; plan: CompiledQuery; __row?: TRow }): Promise<TRow[]>;
  insertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  upsertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  runStatement(
    statement: CompiledStatement,
    options?: { returning?: readonly string[] | "*" },
  ): Promise<{ kind: string; rowCount?: number; returnedRows?: QueryRow[] }>;
  execute(sql: string): Promise<{ kind: string; result?: { rows: QueryRow[] } }>;
  liveQueries(options?: DslLiveOptions): DriverLiveSet;
}

export interface MinnowOptions {
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
 * Creates the typed facade. The recommended form names the database type once, so every hover,
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
export function createMinnow<TTables extends readonly AnyTable[]>(
  driver: DslDriver,
  options: Omit<MinnowOptions, "schema"> & { schema: SchemaDefinition<TTables> },
): Minnow<InferDatabase<SchemaDefinition<TTables>>>;
export function createMinnow<DB>(driver: DslDriver, options?: MinnowOptions): Minnow<DB>;
export function createMinnow<DB>(driver: DslDriver, options: MinnowOptions = {}): Minnow<DB> {
  return new Minnow<DB>(driver, options);
}

interface SharedLiveSet {
  set?: DriverLiveSet | undefined;
}

/** One db.search hit: the owning table, the row (without the score alias), and its BM25 score. */
export interface SearchHit<DB> {
  table: keyof DB & string;
  row: Record<string, QueryValue>;
  score: number;
}

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
   * The driver this facade was created with — the `MinnowDatabase` or `MinnowDatabaseClient`
   * behind it. Tools handed only the facade (devtools, inspectors, schema browsers) reach the
   * catalog and the raw SQL entry points through here; application code should keep its own
   * reference to the driver rather than reaching back through the facade.
   */
  get driver(): DslDriver {
    return this.#driver;
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
      insertBatch: (tableName, rows) => this.#driver.insertBatch(tableName, rows),
      upsertBatch: (tableName, rows) => this.#driver.upsertBatch(tableName, rows),
      runStatement: (statement, options) => this.#driver.runStatement(statement, options),
      tableColumns: (tableName) => {
        const definition = schemaTables?.find(({ name }) => name === tableName);
        return definition === undefined ? undefined : Object.keys(definition.columns);
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
    factory: (creator: Minnow<DB>) => SelectQueryBuilder<DB, TCteCtx, TCteRow>,
  ): Minnow<DB & Record<TName, TCteRow>> {
    const builder = factory(this) as BlockCompilable;
    return new Minnow(
      this.#driver,
      this.#options,
      [...this.#ctes, { name, builder }],
      this.#liveBox,
    );
  }

  /**
   * Zero-ceremony document search across tables: every searched table (all schema tables by
   * default; `options.tables` narrows the set) runs a MATCH(*)-filtered, BM25-ordered scan,
   * and the per-table hits merge into one relevance-ranked list. The facade must be created
   * with `{ schema }` — the per-table column lists come from it.
   */
  async search(
    query: string,
    options: { tables?: ReadonlyArray<keyof DB & string>; limit?: number } = {},
  ): Promise<Array<SearchHit<DB>>> {
    const schemaTables = this.#options.schema?.tables;
    if (schemaTables === undefined) {
      throw new TypeError("search() needs the table columns: create the facade with { schema }");
    }
    const tableNames = options.tables ?? schemaTables.map(({ name }) => name as keyof DB & string);
    const limit = options.limit ?? 10;
    const perTable = await Promise.all(
      tableNames.map(async (tableName) => {
        const definition = schemaTables.find(({ name }) => name === tableName);
        if (definition === undefined) {
          throw new TypeError(`search() needs the table's schema: ${tableName}`);
        }
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
        const columnNames = Object.keys(definition.columns);
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
      }),
    );
    return perTable
      .flat()
      .sort((left, right) => right.score - left.score)
      .slice(0, limit);
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
