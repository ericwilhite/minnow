/**
 * Getting the generated dataset into a database, with progress.
 *
 * The rows are generated on the main thread — 590,000 of them in about a third of a second — and
 * handed to a worker-hosted database in batches. Everything expensive after that (encoding,
 * compression, the IndexedDB writes) happens in the worker, so the page keeps painting while a
 * database is being built underneath it. That is also exactly how an application would do this,
 * which is the point of doing it this way rather than the convenient way.
 */
import {
  column,
  schema,
  table,
  type AnySchema,
  type BatchRow,
  type InsertBatchInput,
  type MigrateOptions,
  type QueryOptions,
  type QueryResult,
} from "@minnowdb/core";
import { retailBatches, retailEstimatedRows, retailSchema, retailTables } from "./retail";

export interface LoadProgress {
  phase: "schema" | "rows" | "ready";
  table: string;
  rows: number;
  estimatedRows: number;
}

/** Stable IndexedDB name owned only by the disposable generated playground dataset. */
export const PLAYGROUND_DATABASE = "minnow-playground";

const DATASET_REVISION = 2;
const RECEIPT_TABLE = "__minnow_playground_dataset";
const receiptTable = table(RECEIPT_TABLE, {
  id: column.integer().unique(),
  revision: column.integer(),
  scale: column.number(),
  row_count: column.integer(),
});
const playgroundDefinition = schema([...retailTables, receiptTable]);
const emptyDefinition = schema([]);
const playgroundTables = [...retailSchema.map(({ name }) => name), RECEIPT_TABLE] as const;

/**
 * The small shared surface used by both the worker client and in-memory lifecycle tests. Method
 * signatures on purpose: the loader addresses tables by runtime name, including its private load
 * receipt, and a client declared against the retail schema must still satisfy this — which its
 * generic, table-name-typed methods do only under method bivariance.
 */
interface DatasetClient {
  listTables(): Promise<Array<{ name: string }>>;
  migrate(definition: AnySchema, options?: MigrateOptions): Promise<unknown>;
  insertBatch(tableName: string, input: InsertBatchInput): Promise<unknown>;
  insert(tableName: string, row: BatchRow): Promise<unknown>;
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
}

export interface LoadedDataset {
  rows: number;
}

/** Returns the durable load receipt only after the exact requested dataset finished building. */
export async function isLoaded(
  client: DatasetClient,
  scale: number,
): Promise<LoadedDataset | undefined> {
  const tables = await client.listTables();
  const present = new Set(tables.map((table) => table.name));
  if (!playgroundTables.every((name) => present.has(name))) return undefined;

  const receipt = (
    await client.query(`SELECT revision, scale, row_count FROM ${RECEIPT_TABLE} WHERE id = 1`)
  ).rows[0];
  if (
    receipt?.revision !== DATASET_REVISION ||
    receipt.scale !== scale ||
    typeof receipt.row_count !== "number" ||
    !Number.isSafeInteger(receipt.row_count) ||
    receipt.row_count < 0
  ) {
    return undefined;
  }
  return { rows: receipt.row_count };
}

export async function loadRetailDataset(
  client: DatasetClient,
  options: { scale: number; onProgress?: (progress: LoadProgress) => void },
): Promise<number> {
  const estimatedRows = retailEstimatedRows(options.scale);
  const report = (phase: LoadProgress["phase"], table: string, rows: number): void => {
    options.onProgress?.({ phase, table, rows, estimatedRows });
  };

  report("schema", "", 0);
  // A receipt is written only after every batch. If a tab closed halfway through a prior build,
  // clearing the managed schema here prevents a partial dataset from becoming the new baseline.
  await client.migrate(emptyDefinition, { allowDestructive: true, schemaOwnsDatabase: true });
  // The declaration in ./retail is the same value the typed console infers its row types from,
  // so the tables a query is written against are the tables that were created.
  await client.migrate(playgroundDefinition);

  let rows = 0;
  for (const batch of retailBatches({ scale: options.scale })) {
    await client.insertBatch(batch.table, batch.rows);
    rows += batch.rows.length;
    report("rows", batch.table, rows);
    // One frame between batches. The insert itself is already off-thread; this is what lets the
    // progress bar actually paint rather than jumping from 0 to 100 at the end.
    await new Promise((resolve) => {
      setTimeout(resolve, 0);
    });
  }

  // This is the commit marker for the whole build. Table existence is not sufficient because all
  // tables are created before the first row batch and a browser can close at any await above.
  await client.insert(RECEIPT_TABLE, {
    id: 1,
    revision: DATASET_REVISION,
    scale: options.scale,
    row_count: rows,
  });
  report("ready", "", rows);
  return rows;
}
