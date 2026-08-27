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

function decodedRows<R>(
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

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    const result = await this.#driver.execute(
      compiledQuery.sql,
      kyselyQueryValues(compiledQuery.parameters),
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

/** Kysely's single logical connection over a Minnow SQL driver. */
export class MinnowKyselyDriver implements Driver {
  readonly #driver: MinnowSqlDriver;
  readonly #connection: DatabaseConnection;

  constructor(driver: MinnowSqlDriver, resultDecoding: MinnowResultDecoding = {}) {
    this.#driver = driver;
    this.#connection = new MinnowKyselyConnection(driver, resultDecoding);
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
