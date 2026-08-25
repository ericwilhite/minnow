import type { ExecuteResult, MinnowSqlDriver, QueryValue } from "@minnowdb/core";
import type {
  CompiledQuery,
  DatabaseConnection,
  Driver,
  AbortableOperationOptions,
  QueryResult,
  TransactionSettings,
} from "kysely";
import { kyselyQueryValues } from "./query-values.js";

function affectedRows(result: ExecuteResult): number | undefined {
  return result.kind === "insert" ||
    result.kind === "update" ||
    result.kind === "delete" ||
    result.kind === "merge"
    ? result.rowCount
    : undefined;
}

function returnedRows(result: ExecuteResult): Array<Record<string, QueryValue>> {
  if (result.kind === "rows") return result.result.rows;
  if (result.kind === "insert" || result.kind === "update" || result.kind === "delete") {
    return result.returnedRows ?? [];
  }
  return [];
}

class MinnowKyselyConnection implements DatabaseConnection {
  readonly #driver: MinnowSqlDriver;

  constructor(driver: MinnowSqlDriver) {
    this.#driver = driver;
  }

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    const result = await this.#driver.execute(
      compiledQuery.sql,
      kyselyQueryValues(compiledQuery.parameters),
    );
    const count = affectedRows(result);
    return {
      rows: returnedRows(result) as R[],
      ...(count === undefined ? {} : { numAffectedRows: BigInt(count) }),
    };
  }

  async *streamQuery<R>(
    compiledQuery: CompiledQuery,
    chunkSize: number,
    options?: AbortableOperationOptions,
  ): AsyncIterableIterator<QueryResult<R>> {
    for await (const batch of this.#driver.queryCursor(compiledQuery.sql, {
      params: kyselyQueryValues(compiledQuery.parameters),
      batchRows: chunkSize,
      ...(options?.signal === undefined ? {} : { signal: options.signal }),
    })) {
      yield { rows: batch.rows as R[] };
    }
  }
}

/** Kysely's single logical connection over a Minnow SQL driver. */
export class MinnowKyselyDriver implements Driver {
  readonly #driver: MinnowSqlDriver;
  readonly #connection: DatabaseConnection;

  constructor(driver: MinnowSqlDriver) {
    this.#driver = driver;
    this.#connection = new MinnowKyselyConnection(driver);
  }

  init(): Promise<void> {
    return Promise.resolve();
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    return this.#connection;
  }

  async beginTransaction(
    connection: DatabaseConnection,
    settings: TransactionSettings,
  ): Promise<void> {
    void connection;
    if (settings.accessMode !== undefined || settings.isolationLevel !== undefined) {
      throw new TypeError(
        "Minnow has one transaction mode; access mode and isolation settings are not supported",
      );
    }
    await this.#driver.execute("BEGIN");
  }

  async commitTransaction(): Promise<void> {
    await this.#driver.execute("COMMIT");
  }

  async rollbackTransaction(): Promise<void> {
    await this.#driver.execute("ROLLBACK");
  }

  releaseConnection(): Promise<void> {
    return Promise.resolve();
  }

  /** Destroying Kysely never closes the caller-owned Minnow database. */
  destroy(): Promise<void> {
    return Promise.resolve();
  }
}
