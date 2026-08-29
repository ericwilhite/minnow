import type { PGlite } from "@electric-sql/pglite";
import { loadPglite } from "./vendored";
import { commerceEntities, generateEntityBatch } from "../benchmark";
import type { DatasetRecord, EngineMaterialization } from "../protocol";
import {
  canonicalizeRow,
  columnList,
  createSecondaryIndexes,
  createTableSql,
  normalizeRows,
  quoteIdentifier,
  rowsFromColumns,
  sqlType,
} from "./shared";
import type {
  EngineDriver,
  EngineSession,
  LoadContext,
  WriteSession,
  WriteTableSchema,
} from "./session";

const BATCH_ROWS = 50_000;

function dataDirName(record: DatasetRecord): string {
  return `mdb-dataset-${record.id}`;
}

/**
 * Datetime columns come back as raw Postgres text (via identity parsers) instead of JS
 * Date objects: the generator writes UTC wall-clock values, and letting a Date parser
 * reinterpret them in the local zone would corrupt cross-engine comparison.
 *
 * Under strict durability PGlite flushes its IDBFS image to IndexedDB after every statement,
 * which puts a fixed ~13-20ms IndexedDB round trip on every query — including a single-row
 * key lookup. Relaxed durability removes it: measured in Chromium against a 400,000-row
 * table, a prepared point lookup goes from 13.5ms to 0.09ms and a full aggregate from 16.6ms
 * to 2.1ms. Reads therefore open relaxed whenever the dataset asks for relaxed durability,
 * which is the same setting Minnow's store runs under.
 *
 * Writes stay strict in every mode. PGlite 0.5.5's relaxed flush races its own IDBFS
 * connection under sustained writes: a bulk load loses its own CREATE TABLE ("relation does
 * not exist") and the worker dies with "The database connection is closing". Strict is the
 * only mode the write path survives, so the loader and the write suite pay for it.
 */
async function openPglite(
  name: string,
  durability: DatasetRecord["durability"],
  access: "read" | "write",
): Promise<PGlite> {
  const identity = (value: string | null): string | null => value;
  const relaxed = access === "read" && durability === "relaxed";
  const { PGlite: Pglite, types } = await loadPglite();
  const database = await Pglite.create(`idb://${name}`, {
    ...(relaxed ? { relaxedDurability: true } : {}),
    parsers: {
      [types.TIMESTAMP]: identity,
      [types.TIMESTAMPTZ]: identity,
      [types.DATE]: identity,
      // NUMERIC arrives as text to preserve precision; the other engines produce
      // doubles for the same expressions, so compare on the same footing.
      [types.NUMERIC]: (value: string | null) => (value === null ? null : Number(value)),
    },
  });
  // PGlite runs on its shipped defaults otherwise — no work_mem, planner, or JIT overrides.
  // Measured both ways, the tuning moved these query shapes by 1-12% (mostly noise), so the
  // honest simplification costs the comparison nothing.
  return database;
}

/**
 * How long a relaxed handle is left alone before closing it. PGlite schedules its relaxed
 * flush without awaiting it — `relaxed ? run() : await run()` in its own source, with no
 * public way to observe the pending one — so the IndexedDB transaction can still be in flight
 * when `close()` tears the connection down, and it then throws from inside IDBFS. Waiting
 * while the connection is still open is what makes the flush land; two seconds is far longer
 * than the ~20ms a flush takes, and teardown is not part of any measurement.
 */
const RELAXED_CLOSE_SETTLE_MS = 2_000;

/**
 * Closes a handle, settling first when it was opened relaxed. A close that fails anyway is
 * logged and stepped over: the dataset is about to be dropped, so a stuck handle is not worth
 * losing a capture that has already produced its numbers.
 */
async function closePglite(database: PGlite, relaxed = false): Promise<void> {
  if (database.closed) return;
  if (relaxed) {
    await new Promise((resolve) => setTimeout(resolve, RELAXED_CLOSE_SETTLE_MS));
  }
  try {
    await database.close();
  } catch (error) {
    console.warn(`PGlite close failed, continuing: ${String(error)}`);
  }
}

/**
 * Emscripten's IDBFS names its IndexedDB database after the mount path, not the
 * `idb://` label — historically `/pglite/<name>`. Ask the browser when it can say;
 * otherwise fall back to the expected mount path.
 */
async function observedIndexedDbName(name: string): Promise<string> {
  try {
    const databases = await indexedDB.databases();
    const match = databases.find(
      (database) => typeof database.name === "string" && database.name.includes(name),
    );
    if (match?.name !== undefined) return match.name;
  } catch {
    // indexedDB.databases() is not universally available.
  }
  return `/pglite/${name}`;
}

export const pgliteDriver: EngineDriver = {
  id: "pglite",

  async loadDataset(context: LoadContext): Promise<EngineMaterialization> {
    const { record } = context;
    const started = performance.now();
    const name = dataDirName(record);
    let database = await openPglite(name, record.durability, "write");
    let insertMs = 0;
    let indexMs: number;
    let dataStoredBytes: number | null;
    let indexStoredBytes: number | null;
    try {
      const entities = commerceEntities;
      for (const entity of entities) await database.exec(createTableSql(entity));
      let completedRows = 0;
      for (const entity of entities) {
        const entityRows = entity.rows(record.scale);
        for (let start = 0; start < entityRows; start += BATCH_ROWS) {
          context.checkCancelled();
          const rowCount = Math.min(BATCH_ROWS, entityRows - start);
          const columns = generateEntityBatch(entity, start, rowCount, entityRows, record.scale);
          const rows = rowsFromColumns(entity, columns);
          const maxRowsPerStatement = Math.max(1, Math.floor(10_000 / entity.columns.length));
          const insertStarted = performance.now();
          await database.transaction(async (transaction) => {
            for (let offset = 0; offset < rows.length; offset += maxRowsPerStatement) {
              const statementRows = rows.slice(offset, offset + maxRowsPerStatement);
              const values: unknown[] = [];
              const tuples = statementRows.map((row) => {
                const placeholders = row.map((value) => {
                  values.push(value);
                  return `$${String(values.length)}`;
                });
                return `(${placeholders.join(", ")})`;
              });
              await transaction.query(
                `INSERT INTO ${quoteIdentifier(entity.name)} (${entity.columns.map((column) => quoteIdentifier(column.name)).join(", ")}) VALUES ${tuples.join(", ")}`,
                values,
              );
            }
          });
          insertMs += performance.now() - insertStarted;
          completedRows += rowCount;
          context.report(`PGlite · ${entity.name}`, completedRows);
        }
      }
      const dataSizeResult = await database.query<{ bytes: number | bigint }>(
        "SELECT pg_database_size(current_database()) AS bytes",
      );
      dataStoredBytes =
        dataSizeResult.rows[0]?.bytes === undefined ? null : Number(dataSizeResult.rows[0].bytes);
      indexMs =
        record.secondaryIndexes === "foreign-keys"
          ? await createSecondaryIndexes(entities, (sql) => database.exec(sql))
          : 0;
      const indexedSizeResult = await database.query<{ bytes: number | bigint }>(
        "SELECT pg_database_size(current_database()) AS bytes",
      );
      const indexedStoredBytes =
        indexedSizeResult.rows[0]?.bytes === undefined
          ? null
          : Number(indexedSizeResult.rows[0].bytes);
      indexStoredBytes =
        dataStoredBytes === null || indexedStoredBytes === null
          ? null
          : Math.max(0, indexedStoredBytes - dataStoredBytes);
      await database.exec("ANALYZE");
      await database.syncToFs();
      // Close and reopen so what the record marks "ready" is what actually persisted.
      await database.close();
      database = await openPglite(name, record.durability, "write");
      const orderRows =
        entities.find((entity) => entity.name === "orders")?.rows(record.scale) ?? 0;
      const counted = await database.query<{ row_count: number | bigint }>(
        "SELECT COUNT(*) AS row_count FROM orders",
      );
      const countedRows = Number(counted.rows[0]?.row_count ?? -1);
      if (countedRows !== orderRows) {
        throw new Error(
          `PGlite verification failed after reopen: expected ${String(orderRows)} orders, found ${String(countedRows)}`,
        );
      }
      const sizeResult = await database.query<{ bytes: number | bigint }>(
        "SELECT pg_database_size(current_database()) AS bytes",
      );
      const storedBytes =
        sizeResult.rows[0]?.bytes === undefined ? null : Number(sizeResult.rows[0].bytes);
      return {
        engine: "pglite",
        status: "ready",
        storageName: await observedIndexedDbName(name),
        version: "0.5.x",
        persistence:
          record.durability === "relaxed"
            ? "IndexedDB VFS · persistent PostgreSQL data directory · relaxed flush on reads, strict on writes"
            : "IndexedDB VFS · persistent PostgreSQL data directory · strict flush",
        dataStoredBytes,
        indexStoredBytes,
        storedBytes,
        buildMs: performance.now() - started,
        insertMs,
        indexMs,
      };
    } finally {
      await closePglite(database);
    }
  },

  async openSession(record: DatasetRecord): Promise<EngineSession> {
    const relaxed = record.durability === "relaxed";
    const database = await openPglite(dataDirName(record), record.durability, "read");
    let nextStatement = 0;
    return {
      engine: "pglite",
      async prepare(sql) {
        // A genuine server-side prepared statement, so prepareMs covers parse+plan once and
        // executions reuse the plan.
        const name = `bench_stmt_${String(nextStatement++)}`;
        await database.exec(`PREPARE ${name} AS ${sql}`);
        return {
          execute: async () =>
            normalizeRows(
              (await database.query<Record<string, unknown>>(`EXECUTE ${name}`)).rows,
            ).map(canonicalizeRow),
          close: () => {
            void database.exec(`DEALLOCATE ${name}`).catch(() => undefined);
          },
        };
      },
      async close() {
        await closePglite(database, relaxed);
      },
    };
  },

  async openWriteSession(record: DatasetRecord): Promise<WriteSession> {
    const database = await openPglite(dataDirName(record), record.durability, "write");
    return {
      engine: "pglite",
      async createTable(schema: WriteTableSchema) {
        await database.exec(createTableSql(schema));
        const table = quoteIdentifier(schema.name);
        const key = quoteIdentifier(schema.primaryKey);
        const columns = columnList(schema);
        const assignments = schema.columns
          .filter((column) => column.name !== schema.primaryKey)
          .map(
            (column) =>
              `${quoteIdentifier(column.name)} = EXCLUDED.${quoteIdentifier(column.name)}`,
          )
          .join(", ");
        const keyType = sqlType(
          schema.columns.find((column) => column.name === schema.primaryKey)?.type ?? "number",
        );
        // Postgres bulk writes are multi-row statements inside one transaction, chunked so a
        // statement stays under the parameter limit — the same shape the dataset loader uses.
        // Statement text and the flat parameter list are built before the timed call; parsing,
        // planning, executing, and committing them is what gets measured.
        const chunkRows = Math.max(1, Math.floor(10_000 / Math.max(1, schema.columns.length)));
        const runStatements =
          (statements: ReadonlyArray<{ sql: string; values: unknown[] }>) => async () => {
            await database.transaction(async (transaction) => {
              for (const statement of statements) {
                await transaction.query(statement.sql, statement.values);
              }
            });
          };
        const insertStatements = (
          suffix: string,
          rows: Array<Array<boolean | number | string | null>>,
        ): Array<{ sql: string; values: unknown[] }> => {
          const statements: Array<{ sql: string; values: unknown[] }> = [];
          for (let offset = 0; offset < rows.length; offset += chunkRows) {
            const values: unknown[] = [];
            const tuples = rows.slice(offset, offset + chunkRows).map((row) => {
              const placeholders = row.map((value) => {
                values.push(value);
                return `$${String(values.length)}`;
              });
              return `(${placeholders.join(", ")})`;
            });
            statements.push({
              sql: `INSERT INTO ${table} (${columns}) VALUES ${tuples.join(", ")}${suffix}`,
              values,
            });
          }
          return statements;
        };
        return {
          prepareInsert: (batch) =>
            runStatements(insertStatements("", rowsFromColumns(schema, batch.columns))),
          prepareUpsert: (batch) =>
            runStatements(
              insertStatements(
                ` ON CONFLICT (${key}) DO UPDATE SET ${assignments}`,
                rowsFromColumns(schema, batch.columns),
              ),
            ),
          // The idiomatic Postgres bulk update: join the table against the new values
          // carried inline, rather than one round trip per row. Parameters inside VALUES
          // have no inferable type, so every element is cast.
          prepareUpdate: (keys, column, values) => {
            const valueType = sqlType(
              schema.columns.find((candidate) => candidate.name === column)?.type ?? "number",
            );
            const pairsPerStatement = 5_000;
            const statements: Array<{ sql: string; values: unknown[] }> = [];
            for (let offset = 0; offset < keys.length; offset += pairsPerStatement) {
              const bound: unknown[] = [];
              const tuples: string[] = [];
              const end = Math.min(offset + pairsPerStatement, keys.length);
              for (let index = offset; index < end; index += 1) {
                bound.push(keys[index], values[index] ?? null);
                tuples.push(
                  `($${String(bound.length - 1)}::${keyType}, $${String(bound.length)}::${valueType})`,
                );
              }
              statements.push({
                sql: `UPDATE ${table} SET ${quoteIdentifier(column)} = source.value FROM (VALUES ${tuples.join(", ")}) AS source(key, value) WHERE ${table}.${key} = source.key`,
                values: bound,
              });
            }
            return runStatements(statements);
          },
          readAll: async () =>
            normalizeRows(
              (
                await database.query<Record<string, unknown>>(
                  `SELECT ${columns} FROM ${table} ORDER BY ${key}`,
                )
              ).rows,
            ).map(canonicalizeRow),
          drop: async () => {
            await database.exec(`DROP TABLE IF EXISTS ${table}`);
          },
        };
      },
      async close() {
        await closePglite(database);
      },
    };
  },

  async deleteDataset(materialization: EngineMaterialization): Promise<void> {
    // The record stores the observed IndexedDB name, but delete the other historical
    // spellings too — deleteDatabase on a nonexistent name succeeds silently, so the
    // extra attempts cost nothing and cover records written before the name was observed.
    const label = materialization.storageName.replace(/^\/pglite\//, "");
    const candidates = new Set([materialization.storageName, label, `/pglite/${label}`]);
    for (const name of candidates) {
      await new Promise<void>((resolve) => {
        const request = indexedDB.deleteDatabase(name);
        request.onsuccess = () => {
          resolve();
        };
        request.onerror = () => {
          resolve();
        };
        request.onblocked = () => {
          resolve();
        };
      });
    }
  },
};
