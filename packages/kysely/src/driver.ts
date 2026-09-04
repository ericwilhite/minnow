import type { ExecuteResult, MinnowSqlDriver, QueryValue } from "@minnowdb/core";
import {
  IdentifierNode,
  RawNode,
  SelectQueryNode,
  createQueryId,
  type CompiledQuery,
  type DatabaseConnection,
  type Driver,
  type AbortableOperationOptions,
  type QueryCompiler,
  type QueryResult,
  type TransactionSettings,
} from "kysely";
import { kyselyQueryValues } from "./query-values.js";

export interface MinnowResultDecoding {
  /** Convert exact NUMERIC text to JavaScript numbers. This may lose decimal precision. */
  readonly numeric?: "number";
  /** Parse JSON and JSONB text into JavaScript arrays, objects, and scalar values. */
  readonly json?: "parse";
}

function affectedRows(result: ExecuteResult): number | undefined {
  return result.kind === "insert" ||
    result.kind === "update" ||
    result.kind === "delete" ||
    result.kind === "merge"
    ? result.rowCount
    : undefined;
}

function returnedResult(result: ExecuteResult): {
  rows: Array<Record<string, QueryValue>>;
  columns: readonly string[];
  columnDomains: ReadonlyArray<{ readonly kind: string } | null>;
} {
  if (result.kind === "rows") return result.result;
  if (result.kind === "insert" || result.kind === "update" || result.kind === "delete") {
    const rows = result.returnedRows ?? [];
    const columns = result.returnedColumns ?? Object.keys(rows[0] ?? {});
    return {
      rows,
      columns,
      columnDomains: result.returnedColumnDomains ?? columns.map(() => null),
    };
  }
  return { rows: [], columns: [], columnDomains: [] };
}

/** Rows as the dialect hands them to Kysely: domain values decoded per `resultDecoding`. */
export function decodedRows<R>(
  rows: Array<Record<string, QueryValue>>,
  columns: readonly string[],
  columnDomains: ReadonlyArray<{ readonly kind: string } | null>,
  decoding: MinnowResultDecoding,
): R[] {
  if (decoding.numeric === undefined && decoding.json === undefined) return rows as R[];
  const decoders = columnDomains.map((domain) => {
    if (domain?.kind === "numeric" && decoding.numeric === "number") {
      return (value: QueryValue | undefined): unknown => {
        if (value === null || value === undefined) return value;
        if (typeof value !== "string") return value;
        const decoded = Number(value);
        if (!Number.isFinite(decoded)) {
          throw new RangeError(`NUMERIC result cannot be represented as a finite number: ${value}`);
        }
        return decoded;
      };
    }
    if ((domain?.kind === "json" || domain?.kind === "jsonb") && decoding.json === "parse") {
      return (value: QueryValue | undefined): unknown => {
        if (value === null || value === undefined) return value;
        if (typeof value !== "string") return value;
        return JSON.parse(value) as unknown;
      };
    }
    return undefined;
  });
  if (decoders.every((decoder) => decoder === undefined)) return rows as R[];
  return rows.map((row) => {
    let decoded: Record<string, unknown> = row;
    columns.forEach((column, index) => {
      const decode = decoders[index];
      if (decode === undefined) return;
      const before = row[column];
      const after = decode(before);
      if (after === before) return;
      if (decoded === row) decoded = { ...row };
      decoded[column] = after;
    });
    return decoded as R;
  });
}

class MinnowKyselyConnection implements DatabaseConnection {
  readonly #driver: MinnowSqlDriver;
  readonly #resultDecoding: MinnowResultDecoding;

  constructor(driver: MinnowSqlDriver, resultDecoding: MinnowResultDecoding) {
    this.#driver = driver;
    this.#resultDecoding = resultDecoding;
  }

  async executeQuery<R>(
    compiledQuery: CompiledQuery,
    options?: AbortableOperationOptions,
  ): Promise<QueryResult<R>> {
    const result = await this.#driver.execute(
      compiledQuery.sql,
      kyselyQueryValues(compiledQuery.parameters),
      options?.signal === undefined ? undefined : { signal: options.signal },
    );
    const count = affectedRows(result);
    const returned = returnedResult(result);
    const rows = decodedRows<R>(
      returned.rows,
      returned.columns,
      returned.columnDomains,
      this.#resultDecoding,
    );
    return {
      rows,
      ...(count === undefined ? {} : { numAffectedRows: BigInt(count) }),
    };
  }

  async *streamQuery<R>(
    compiledQuery: CompiledQuery,
    chunkSize: number,
    options?: AbortableOperationOptions,
  ): AsyncIterableIterator<QueryResult<R>> {
    if (!SelectQueryNode.is(compiledQuery.query)) {
      // Minnow's cursor reads SELECT statements only. Kysely also streams INSERT, UPDATE, and
      // DELETE builders with RETURNING, so a mutation runs buffered and hands its returned rows
      // out in chunks, as PostgreSQL's cursor-backed dialect would.
      // Validate before execution: a bad chunk size must never commit a mutation first and fail
      // (or silently yield the wrong rows) afterward.
      if (!Number.isSafeInteger(chunkSize) || chunkSize <= 0) {
        throw new RangeError("Query batch rows must be a positive whole number");
      }
      const result = await this.executeQuery<R>(compiledQuery, options);
      for (let start = 0; start < result.rows.length; start += chunkSize) {
        yield { rows: result.rows.slice(start, start + chunkSize) };
      }
      return;
    }
    for await (const batch of this.#driver.queryCursor(compiledQuery.sql, {
      params: kyselyQueryValues(compiledQuery.parameters),
      batchRows: chunkSize,
      ...(options?.signal === undefined ? {} : { signal: options.signal }),
    })) {
      yield {
        rows: decodedRows<R>(batch.rows, batch.columns, batch.columnDomains, this.#resultDecoding),
      };
    }
  }
}

/** `SAVEPOINT name`, `ROLLBACK TO name`, or `RELEASE name` with the name sanitized by Kysely. */
function savepointCommand(command: string, savepointName: string): RawNode {
  return RawNode.createWithChildren([
    RawNode.createWithSql(`${command} `),
    IdentifierNode.create(savepointName),
  ]);
}

/**
 * One acquisition of the driver's single logical connection. Kysely hands this exact object back
 * to `releaseConnection` and the transaction hooks, so each hold can release itself exactly once:
 * `beginTransaction` frees a hold it is about to strand (Kysely's controlled-transaction builder
 * has no catch around BEGIN, so a thrown begin would otherwise leave the FIFO mutex held
 * forever), and the later `releaseConnection` from a callback transaction's `finally` finds the
 * hold already empty instead of freeing a successor's.
 */
class MinnowAcquiredConnection implements DatabaseConnection {
  readonly #inner: MinnowKyselyConnection;
  #release: (() => void) | undefined;

  constructor(inner: MinnowKyselyConnection, release: () => void) {
    this.#inner = inner;
    this.#release = release;
  }

  executeQuery<R>(
    compiledQuery: CompiledQuery,
    options?: AbortableOperationOptions,
  ): Promise<QueryResult<R>> {
    return this.#inner.executeQuery(compiledQuery, options);
  }

  streamQuery<R>(
    compiledQuery: CompiledQuery,
    chunkSize: number,
    options?: AbortableOperationOptions,
  ): AsyncIterableIterator<QueryResult<R>> {
    return this.#inner.streamQuery(compiledQuery, chunkSize, options);
  }

  release(): void {
    const release = this.#release;
    this.#release = undefined;
    release?.();
  }
}

/**
 * Kysely's single logical connection over a Minnow SQL driver.
 *
 * The engine binds an open SQL transaction to the database instance, so a statement that ran
 * beside an open transaction would silently join it and roll back with it. `acquireConnection`
 * therefore hands out the one connection through a FIFO mutex: each caller waits until the
 * previous holder releases. Kysely acquires per statement outside transactions and holds the
 * connection across a transaction or stream, so concurrent work queues instead of interleaving.
 * The trade-off matches Kysely's own single-connection dialects: awaiting a `db`-level query
 * inside a transaction callback deadlocks, because it waits for the connection the transaction
 * holds. Use the transaction's own handle inside the callback.
 */
export class MinnowKyselyDriver implements Driver {
  readonly #driver: MinnowSqlDriver;
  readonly #connection: MinnowKyselyConnection;
  #queueTail: Promise<void> = Promise.resolve();

  constructor(driver: MinnowSqlDriver, resultDecoding: MinnowResultDecoding = {}) {
    this.#driver = driver;
    this.#connection = new MinnowKyselyConnection(driver, resultDecoding);
  }

  init(): Promise<void> {
    return Promise.resolve();
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    const previous = this.#queueTail;
    let release!: () => void;
    this.#queueTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    return new MinnowAcquiredConnection(this.#connection, release);
  }

  async beginTransaction(
    connection: DatabaseConnection,
    settings: TransactionSettings,
  ): Promise<void> {
    // A begin that throws strands its connection: `startTransaction()` never releases after a
    // failed BEGIN, which would wedge the mutex for the life of the Kysely instance. No
    // transaction opened, so freeing this hold is always sound; the release is one-shot, so the
    // callback path's own `finally` release stays a no-op rather than freeing a successor.
    const abandon = (error: unknown): never => {
      if (connection instanceof MinnowAcquiredConnection) connection.release();
      throw error;
    };
    if (settings.accessMode !== undefined || settings.isolationLevel !== undefined) {
      abandon(
        new TypeError(
          "Minnow has one transaction mode; access mode and isolation settings are not supported",
        ),
      );
    }
    try {
      await this.#driver.execute("BEGIN");
    } catch (error) {
      abandon(error);
    }
  }

  async commitTransaction(): Promise<void> {
    await this.#driver.execute("COMMIT");
  }

  async rollbackTransaction(): Promise<void> {
    try {
      await this.#driver.execute("ROLLBACK");
    } catch (error) {
      // A failed COMMIT closes the engine transaction, so the documented recovery — catch, then
      // `trx.rollback()` — meets a transaction that is already gone. That is the state ROLLBACK
      // exists to reach; treating it as success lets the rollback command release its connection
      // instead of wedging the instance. The engine's wording is matched by name and message
      // because a worker client rehydrates the TypeError as a plain Error named "TypeError".
      if (
        error instanceof Error &&
        error.name === "TypeError" &&
        error.message.includes("ROLLBACK without an open transaction")
      ) {
        return;
      }
      throw error;
    }
  }

  async savepoint(
    connection: DatabaseConnection,
    savepointName: string,
    compileQuery: QueryCompiler["compileQuery"],
  ): Promise<void> {
    await connection.executeQuery(
      compileQuery(savepointCommand("savepoint", savepointName), createQueryId()),
    );
  }

  async rollbackToSavepoint(
    connection: DatabaseConnection,
    savepointName: string,
    compileQuery: QueryCompiler["compileQuery"],
  ): Promise<void> {
    await connection.executeQuery(
      compileQuery(savepointCommand("rollback to", savepointName), createQueryId()),
    );
  }

  async releaseSavepoint(
    connection: DatabaseConnection,
    savepointName: string,
    compileQuery: QueryCompiler["compileQuery"],
  ): Promise<void> {
    await connection.executeQuery(
      compileQuery(savepointCommand("release", savepointName), createQueryId()),
    );
  }

  releaseConnection(connection: DatabaseConnection): Promise<void> {
    if (connection instanceof MinnowAcquiredConnection) connection.release();
    return Promise.resolve();
  }

  /** Destroying Kysely never closes the caller-owned Minnow database. */
  destroy(): Promise<void> {
    return Promise.resolve();
  }
}
