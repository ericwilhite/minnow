/**
 * Getting the generated dataset into a database, with progress.
 *
 * The rows are generated on the main thread — 590,000 of them in about a third of a second — and
 * handed to a worker-hosted database in batches. Everything expensive after that (encoding,
 * compression, the IndexedDB writes) happens in the worker, so the page keeps painting while a
 * database is being built underneath it. That is also exactly how an application would do this,
 * which is the point of doing it this way rather than the convenient way.
 */
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import { retailBatches, retailEstimatedRows, retailSchema } from "./retail";

export interface LoadProgress {
  phase: "schema" | "rows" | "ready";
  table: string;
  rows: number;
  estimatedRows: number;
}

/** The name of the database in IndexedDB. Bumped whenever the generated schema changes. */
export const PLAYGROUND_DATABASE = "minnow-playground-v1";

/** True when this database already holds the dataset, so a revisit skips straight to querying. */
export async function isLoaded(client: MinnowDatabaseClient): Promise<boolean> {
  const tables = await client.listTables();
  const present = new Set(tables.map((table) => table.name));
  return retailSchema.every((table) => present.has(table.name));
}

export async function loadRetailDataset(
  client: MinnowDatabaseClient,
  options: { scale: number; onProgress?: (progress: LoadProgress) => void },
): Promise<number> {
  const estimatedRows = retailEstimatedRows(options.scale);
  const report = (phase: LoadProgress["phase"], table: string, rows: number): void => {
    options.onProgress?.({ phase, table, rows, estimatedRows });
  };

  report("schema", "", 0);
  for (const table of retailSchema) {
    await client.createTable({
      name: table.name,
      uniqueKey: table.uniqueKey,
      columns: table.columns,
    });
  }

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

  report("ready", "", rows);
  return rows;
}
