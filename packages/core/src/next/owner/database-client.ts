import type { CatalogDescription, ExplainDescription } from "./inspection.js";
/** Internal 1.0 facade. ProtocolClient remains the sole sequence/retry/outcome dispatcher.
 * Live external-store binding is added on B8's runtime; public barrels are held for cutover. */
import type {
  CallOptions,
  Cursor,
  CursorOptions,
  InsertOptions,
  Param,
  PreparedStatement,
  Result,
  Row,
  TransactionOptions,
  WriteInput,
  WriteOptions,
  ClientStatus,
  SqlExecutor,
} from "./api.js";
import { resultColumn } from "./api.js";
import { ProtocolClient } from "./client.js";
import { callResult, callTask, type RequestBody } from "./client-call.js";
import { clientCursor } from "./client-cursor.js";
import { writeColumns } from "./client-input.js";
import { ClientClosedError } from "./errors.js";
import { DEFAULT_RESULT_WINDOW_BYTES } from "./protocol.js";
import { NativeLiveStore, type NativeLiveOptions } from "./live-store.js";
export interface ClientLifecycle {
  readonly ready: Promise<void>;
  status?(): ClientStatus;
  close?(): Promise<void>;
}
interface Shared {
  readonly protocol: ProtocolClient;
  readonly calls: Set<Promise<unknown>>;
  readonly cursors: Set<Cursor<unknown>>;
  closing: boolean;
  nextSession: number;
}
interface Scope {
  active: boolean;
  readonly preparations: Set<PreparedStatement>;
  readonly calls: Set<Promise<unknown>>;
  readonly cursors: Set<Cursor<unknown>>;
  failure: { error: unknown } | null;
}
class ClientSql implements SqlExecutor {
  constructor(
    protected readonly shared: Shared,
    protected readonly session = 0,
    private readonly scope: Scope | null = null,
    private readonly scopeSignal?: AbortSignal,
  ) {}
  private signal(signal?: AbortSignal): AbortSignal | undefined {
    if (this.scopeSignal === undefined) return signal;
    if (signal === undefined || signal === this.scopeSignal) return this.scopeSignal;
    return AbortSignal.any([this.scopeSignal, signal]);
  }
  protected check(): void {
    if (this.shared.closing) throw new ClientClosedError();
    if (this.scope !== null) {
      if (!this.scope.active) throw new TypeError("Transaction is closed");
    }
  }
  protected track<T>(promise: Promise<T>): Promise<T> {
    this.shared.calls.add(promise);
    this.scope?.calls.add(promise);
    const finish = () => {
      this.shared.calls.delete(promise);
      this.scope?.calls.delete(promise);
    };
    promise.then(finish, (error: unknown) => {
      if (this.scope !== null) this.scope.failure ??= { error };
      finish();
    });
    return promise;
  }
  protected async request<TRow = Row>(
    body: RequestBody,
    options: CallOptions = {},
  ): Promise<Result<TRow>> {
    this.check();
    return this.track(callResult<TRow>(this.shared.protocol, body, this.signal(options.signal)));
  }
  async query<TRow = Row>(
    sql: string,
    params: readonly Param[] = [],
    options: CallOptions = {},
  ): Promise<Result<TRow>> {
    return this.statement(sql, params, options, true);
  }
  async execute<TRow = Row>(
    sql: string,
    params: readonly Param[] = [],
    options: CallOptions = {},
  ): Promise<Result<TRow>> {
    return this.statement(sql, params, options, false);
  }
  private statement<TRow>(
    sql: string,
    params: readonly Param[],
    options: CallOptions,
    queryOnly: boolean,
    prepared: number | null = null,
  ): Promise<Result<TRow>> {
    return this.request(
      {
        t: "statement",
        session: this.session,
        text: sql,
        prepared,
        knownColumnsVersion: -1,
        params,
        queryOnly,
        ...(this.session === 0 ? {} : { scopedTransaction: true }),
        result: { windowBytes: DEFAULT_RESULT_WINDOW_BYTES, frameRows: null, wantRows: true },
      },
      options,
    );
  }
  cursor<TRow = Row>(
    sql: string,
    params: readonly Param[] = [],
    options: CursorOptions = {},
  ): Cursor<TRow> {
    return this.openCursor(sql, params, options);
  }
  queryCursor<TRow = Row>(
    sql: string,
    params: readonly Param[] = [],
    options: CursorOptions = {},
  ): Cursor<TRow> {
    return this.cursor(sql, params, options);
  }
  private openCursor<TRow>(
    sql: string,
    params: readonly Param[],
    options: CursorOptions,
    prepared: number | null = null,
  ): Cursor<TRow> {
    this.check();
    const signal = this.signal(options.signal);
    const cursor = clientCursor<TRow>(
      this.shared.protocol,
      sql,
      params,
      this.session,
      { ...options, ...(signal === undefined ? {} : { signal }) },
      () => {
        this.shared.cursors.delete(cursor);
        this.scope?.cursors.delete(cursor);
      },
      prepared,
      () => this.check(),
    );
    this.shared.cursors.add(cursor);
    this.scope?.cursors.add(cursor);
    return cursor;
  }
  async prepare(sql: string, options: CallOptions = {}): Promise<PreparedStatement> {
    this.check();
    const signal = this.signal(options.signal);
    signal?.throwIfAborted();
    const task = this.shared.protocol.prepareTask(sql, this.session);
    const info = await this.track(callTask(this.shared.protocol, task, signal));
    try {
      this.check();
    } catch (error) {
      this.shared.protocol.unprepare(info.id);
      throw error;
    }
    let closed = false;
    const check = () => {
      this.check();
      if (closed) throw new TypeError("Prepared statement is closed");
    };
    const prepared: PreparedStatement = {
      sql,
      columns: info.columns.map(resultColumn),
      parameters: info.parameters,
      query: async <TRow = Row>(params: readonly Param[] = [], call: CallOptions = {}) => {
        check();
        return this.statement<TRow>(sql, params, call, true, info.id);
      },
      execute: async <TRow = Row>(params: readonly Param[] = [], call: CallOptions = {}) => {
        check();
        return this.statement<TRow>(sql, params, call, false, info.id);
      },
      cursor: <TRow = Row>(params: readonly Param[] = [], call: CursorOptions = {}) => {
        check();
        return this.openCursor<TRow>(sql, params, call, info.id);
      },
      close: () => {
        if (closed) return;
        closed = true;
        this.scope?.preparations.delete(prepared);
        this.shared.protocol.unprepare(info.id);
      },
    };
    this.scope?.preparations.add(prepared);
    return prepared;
  }
  private async write(
    op: "insert" | "upsert" | "update" | "delete",
    table: string,
    rows: WriteInput,
    options: InsertOptions,
  ): Promise<Result> {
    this.check();
    options.signal?.throwIfAborted();
    return this.request(
      {
        t: "write",
        session: this.session,
        table,
        op,
        ...writeColumns(rows),
        returning: options.returning ?? null,
        ignoreConflicts: options.ignoreConflicts ?? false,
        result: { windowBytes: DEFAULT_RESULT_WINDOW_BYTES, frameRows: null, wantRows: true },
      },
      options,
    );
  }
  insert(table: string, rows: WriteInput, options: InsertOptions = {}): Promise<Result> {
    return this.write("insert", table, rows, options);
  }
  upsert(table: string, rows: WriteInput, options: WriteOptions = {}): Promise<Result> {
    return this.write("upsert", table, rows, options);
  }
  update(table: string, rows: WriteInput, options: WriteOptions = {}): Promise<Result> {
    return this.write("update", table, rows, options);
  }
  delete(table: string, rows: WriteInput, options: WriteOptions = {}): Promise<Result> {
    return this.write("delete", table, rows, options);
  }
}
export class MinnowDatabaseClient extends ClientSql {
  readonly #liveStores = new Set<{ close(): Promise<void> }>();
  liveQuery<R extends object = Row>(
    sql: string,
    params: readonly Param[] = [],
    options: NativeLiveOptions<R> = {},
  ): NativeLiveStore<R> {
    this.check();
    const store = new NativeLiveStore<R>(this.shared.protocol, sql, params, options);
    // A parameter or options getter can close the client while the store captures its source.
    if (this.shared.closing) {
      void this.track(store.close());
      throw new ClientClosedError();
    }
    const close = store.close.bind(store);
    store.close = async () => {
      try {
        await close();
      } finally {
        this.#liveStores.delete(store);
      }
    };
    this.#liveStores.add(store);
    return store;
  }
  readonly #lifecycle: ClientLifecycle;
  #close: Promise<void> | null = null;
  constructor(protocol: ProtocolClient, lifecycle: ClientLifecycle) {
    super({ protocol, calls: new Set(), cursors: new Set(), closing: false, nextSession: 0 });
    this.#lifecycle = lifecycle;
    lifecycle.ready.catch(() => undefined);
  }
  get status(): ClientStatus {
    return this.shared.closing
      ? "closed"
      : (this.#lifecycle.status?.() ?? (this.shared.protocol.connected ? "ready" : "connecting"));
  }
  async ready(): Promise<void> {
    this.check();
    await this.#lifecycle.ready;
    this.check();
  }
  async transaction<T>(
    body: (tx: SqlExecutor) => Promise<T>,
    options: TransactionOptions = {},
  ): Promise<T> {
    this.check();
    const signal = options.signal;
    const readOnly = options.readOnly ?? false;
    signal?.throwIfAborted();
    if (this.shared.nextSession === Number.MAX_SAFE_INTEGER)
      return Promise.reject(new RangeError("Transaction session ids are exhausted"));
    const session = ++this.shared.nextSession;
    const scope: Scope = {
      active: true,
      calls: new Set(),
      cursors: new Set(),
      preparations: new Set(),
      failure: null,
    };
    const tx = new ClientSql(this.shared, session, scope, signal);
    const run = async () => {
      try {
        await this.track(
          callTask(
            this.shared.protocol,
            this.shared.protocol.send({ t: "begin", session, readOnly }),
            signal,
          ),
        );
        this.check();
        signal?.throwIfAborted();
        const value = await body(tx);
        await Promise.allSettled([...scope.cursors].map((cursor) => cursor.close()));
        await Promise.allSettled([...scope.calls]);
        scope.active = false;
        signal?.throwIfAborted();
        this.check();
        const committed = await this.track(
          callTask(
            this.shared.protocol,
            this.shared.protocol.send({ t: "commit", session, retire: true }),
            signal,
          ),
        );
        // An ERROR reply does not carry transaction state. The owner's COMMIT reply is the
        // authority: caught errors may have recovered through a savepoint, or left it failed.
        if (committed.summary.command !== "COMMIT") {
          if (scope.failure !== null) throw scope.failure.error;
          throw new TypeError("The transaction rolled back instead of committing");
        }
        return value;
      } catch (error) {
        scope.active = false;
        await Promise.allSettled([...scope.cursors].map((cursor) => cursor.close()));
        await Promise.allSettled([...scope.calls]);
        try {
          await this.track(
            this.shared.protocol.send({ t: "rollback", session, retire: true }).promise,
          );
        } catch {
          /* Preserve terminal cause, including unknown effect. */
        }
        throw error;
      } finally {
        scope.active = false;
        for (const prepared of scope.preparations) prepared.close();
        this.shared.protocol.retireSession(session);
      }
    };
    // User callbacks may await close(). Only actual requests/cursors are teardown
    // dependencies: tracking the callback itself would make close wait on itself.
    return run();
  }
  async catalog(options: CallOptions = {}): Promise<unknown> {
    this.check();
    options.signal?.throwIfAborted();
    return (
      await this.track(
        callTask(
          this.shared.protocol,
          this.shared.protocol.send({ t: "admin", op: "catalog", args: [] }),
          options.signal,
        ),
      )
    ).value;
  }
  /** Detached caller-owned metadata, against the default SQL session's catalog. */
  async describeCatalog(options: CallOptions = {}): Promise<CatalogDescription> {
    this.check();
    options.signal?.throwIfAborted();
    const task = await this.track(
      callTask(
        this.shared.protocol,
        this.shared.protocol.send({ t: "admin", session: 0, op: "describeCatalog", args: [] }),
        options.signal,
      ),
    );
    return task.value as CatalogDescription;
  }
  /** Logical lowered operators; never executes SQL or claims runtime measurements. */
  async explain(sql: string, options: CallOptions = {}): Promise<ExplainDescription> {
    this.check();
    options.signal?.throwIfAborted();
    const task = await this.track(
      callTask(
        this.shared.protocol,
        this.shared.protocol.send({ t: "admin", session: 0, op: "explain", args: [sql] }),
        options.signal,
      ),
    );
    return task.value as ExplainDescription;
  }
  close(): Promise<void> {
    if (this.#close !== null) return this.#close;
    this.shared.closing = true;
    // Queued startup calls have never reached an owner; draining them needs no connection.
    if (!this.shared.protocol.connected) this.shared.protocol.close();
    return (this.#close = (async () => {
      try {
        await Promise.allSettled([...this.#liveStores].map((store) => store.close()));
        await Promise.allSettled([...this.shared.cursors].map((cursor) => cursor.close()));
        await Promise.allSettled([...this.shared.calls]);
      } finally {
        this.shared.protocol.close();
        await this.#lifecycle.close?.();
      }
    })());
  }
}
