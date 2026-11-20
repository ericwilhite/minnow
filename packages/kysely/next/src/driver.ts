/** Internal 1.0 binding. The published dialect and export map remain unchanged. */
import type { Param, Result, SqlExecutor, Value } from "../../../core/dist/next/owner/api.js";
import { parseJson } from "@minnowdb/core/json";
import {
  IdentifierNode,
  RawNode,
  SelectQueryNode,
  createQueryId,
  type AbortableOperationOptions,
  type CompiledQuery,
  type DatabaseConnection,
  type Driver,
  type QueryCompiler,
  type QueryResult,
  type TransactionSettings,
} from "kysely";

export type { MinnowSqlDriver as NativeSqlDriver } from "../../../core/dist/next/owner/sql-driver.js";
import type { MinnowSqlDriver as NativeSqlDriver } from "../../../core/dist/next/owner/sql-driver.js";
export interface NativeResultDecoding {
  readonly numeric?: "number";
  readonly json?: "parse";
}
function parameter(value: unknown, active: Set<unknown>): Param {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string" ||
    typeof value === "bigint" ||
    value instanceof Uint8Array ||
    value instanceof Date
  )
    return value;
  if (!Array.isArray(value)) throw new TypeError("Unsupported native Kysely parameter");
  if (active.has(value)) throw new TypeError("Cyclic native Kysely parameter");
  active.add(value);
  try {
    return (value as unknown[]).map((item) => parameter(item, active));
  } finally {
    active.delete(value);
  }
}
function parameters(values: readonly unknown[]): Param[] {
  return values.map((value) => parameter(value, new Set()));
}
function rows<R>(result: Result<R>, options: NativeResultDecoding): R[] {
  if (options.numeric === undefined && options.json === undefined) return result.rows();
  const decoders = result.columns.map((column) => {
    if (column.type.kind === "decimal" && options.numeric === "number")
      return (value: Value): unknown => {
        if (value === null) return value;
        const n = Number(value);
        if (!Number.isFinite(n))
          throw new RangeError(
            `NUMERIC result cannot be represented as a finite number: ${String(value)}`,
          );
        return n;
      };
    if (column.type.kind === "json" && options.json === "parse")
      return (value: Value): unknown => (value === null ? null : parseJson(String(value)));
    return undefined;
  });
  return result.tuples().map((tuple) => {
    const row: Record<string, unknown> = {};
    for (const [i, column] of result.columns.entries()) {
      const value = tuple[i] ?? null,
        decode = decoders[i];
      Object.defineProperty(row, column.name, {
        value: decode === undefined ? value : decode(value),
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
    return row as R;
  });
}
export { parameters as nativeParameters, rows as nativeRows };
interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void, reject!: (error: unknown) => void;
  const promise = new Promise<T>((a, b) => {
    resolve = a;
    reject = b;
  });
  return { promise, resolve, reject };
}
const rollback = new Error("native Kysely rollback");

class Connection implements DatabaseConnection {
  #executor: NativeSqlDriver | SqlExecutor;
  #transaction: { end: Deferred<"commit" | "rollback">; settled: Promise<void> } | null = null;
  #release: (() => void) | null;
  constructor(
    readonly driver: NativeSqlDriver,
    readonly decoding: NativeResultDecoding,
    release: () => void,
  ) {
    this.#executor = driver;
    this.#release = release;
  }
  async executeQuery<R>(
    query: CompiledQuery,
    options?: AbortableOperationOptions,
  ): Promise<QueryResult<R>> {
    const result = await this.#executor.execute<R>(
      query.sql,
      parameters(query.parameters),
      options?.signal === undefined ? undefined : { signal: options.signal },
    );
    return {
      rows: rows(result, this.decoding),
      ...(result.affectedRows === null ? {} : { numAffectedRows: BigInt(result.affectedRows) }),
    };
  }
  async *streamQuery<R>(
    query: CompiledQuery,
    chunkSize: number,
    options?: AbortableOperationOptions,
  ): AsyncIterableIterator<QueryResult<R>> {
    if (!Number.isSafeInteger(chunkSize) || chunkSize <= 0)
      throw new RangeError("Query batch rows must be a positive whole number");
    if (!SelectQueryNode.is(query.query)) {
      const result = await this.executeQuery<R>(query, options);
      for (let i = 0; i < result.rows.length; i += chunkSize)
        yield { rows: result.rows.slice(i, i + chunkSize) };
      return;
    }
    for await (const batch of this.#executor.cursor<R>(query.sql, parameters(query.parameters), {
      ...(options?.signal === undefined ? {} : { signal: options.signal }),
      frameRows: chunkSize,
    }))
      yield { rows: rows(batch, this.decoding) };
  }
  async begin(settings: TransactionSettings): Promise<void> {
    const levels = new Set([
      "read uncommitted",
      "read committed",
      "repeatable read",
      "serializable",
    ]);
    if (settings.isolationLevel !== undefined && !levels.has(settings.isolationLevel))
      throw new TypeError("Unsupported native Kysely isolation level");
    if (!new Set<unknown>([undefined, "read only", "read write"]).has(settings.accessMode))
      throw new TypeError("Unsupported native Kysely access mode");
    const ready = deferred<undefined>(),
      end = deferred<"commit" | "rollback">();
    const task = this.driver.transaction(
      async (tx) => {
        if (settings.isolationLevel !== undefined)
          await tx.execute(`SET TRANSACTION ISOLATION LEVEL ${settings.isolationLevel}`);
        this.#executor = tx;
        ready.resolve(undefined);
        if ((await end.promise) === "rollback") throw rollback;
      },
      { readOnly: settings.accessMode === "read only" },
    );
    const settled = task.then(
      () => undefined,
      (error: unknown) => {
        if (error !== rollback) throw error;
      },
    );
    // Observe both paths before awaiting BEGIN. A rejected BEGIN must release the FIFO hold.
    void settled.catch(() => undefined);
    this.#transaction = { end, settled };
    try {
      await Promise.race([
        ready.promise,
        settled.then(() => {
          throw new TypeError("Native transaction ended before BEGIN");
        }),
      ]);
    } catch (error) {
      this.#transaction = null;
      this.#executor = this.driver;
      throw error;
    }
  }
  async finish(action: "commit" | "rollback"): Promise<void> {
    const tx = this.#transaction;
    if (tx === null) return;
    this.#transaction = null;
    tx.end.resolve(action);
    try {
      await tx.settled;
    } finally {
      this.#executor = this.driver;
    }
  }
  async release(): Promise<void> {
    const release = this.#release;
    this.#release = null;
    try {
      await this.finish("rollback");
    } finally {
      release?.();
    }
  }
}
function command(prefix: string, name: string): RawNode {
  return RawNode.createWithChildren([
    RawNode.createWithSql(`${prefix} `),
    IdentifierNode.create(name),
  ]);
}
export class NativeKyselyDriver implements Driver {
  #tail: Promise<void> = Promise.resolve();
  constructor(
    readonly driver: NativeSqlDriver,
    readonly decoding: NativeResultDecoding = {},
  ) {}
  init(): Promise<void> {
    return Promise.resolve();
  }
  async acquireConnection(): Promise<DatabaseConnection> {
    const previous = this.#tail,
      gate = deferred<undefined>();
    this.#tail = gate.promise;
    await previous;
    return new Connection(this.driver, this.decoding, () => gate.resolve(undefined));
  }
  async beginTransaction(
    connection: DatabaseConnection,
    settings: TransactionSettings,
  ): Promise<void> {
    if (!(connection instanceof Connection))
      throw new TypeError("Foreign native Kysely connection");
    try {
      await connection.begin(settings);
    } catch (error) {
      await connection.release();
      throw error;
    }
  }
  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    if (connection instanceof Connection) await connection.finish("commit");
  }
  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    if (connection instanceof Connection) await connection.finish("rollback");
  }
  async releaseConnection(connection: DatabaseConnection): Promise<void> {
    if (connection instanceof Connection) await connection.release();
  }
  async savepoint(
    connection: DatabaseConnection,
    name: string,
    compile: QueryCompiler["compileQuery"],
  ): Promise<void> {
    await connection.executeQuery(compile(command("savepoint", name), createQueryId()));
  }
  async rollbackToSavepoint(
    connection: DatabaseConnection,
    name: string,
    compile: QueryCompiler["compileQuery"],
  ): Promise<void> {
    await connection.executeQuery(compile(command("rollback to", name), createQueryId()));
  }
  async releaseSavepoint(
    connection: DatabaseConnection,
    name: string,
    compile: QueryCompiler["compileQuery"],
  ): Promise<void> {
    await connection.executeQuery(compile(command("release", name), createQueryId()));
  }
  destroy(): Promise<void> {
    return Promise.resolve();
  }
}
