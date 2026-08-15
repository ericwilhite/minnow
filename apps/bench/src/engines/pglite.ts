import { PGlite, types } from "@electric-sql/pglite";
import { generateEntityBatch, getScenario } from "../benchmark.js";
import type { DatasetRecord, EngineMaterialization } from "../protocol.js";
import {
  canonicalizeRow,
  columnList,
  createTableSql,
  normalizeRows,
  quoteIdentifier,
  rowsFromColumns,
  secondaryIndexSql,
  sqlType,
} from "./shared.js";
import type {
  EngineDriver,
  EngineSession,
  LoadContext,
  WriteSession,
  WriteTableSchema,
} from "./session.js";

const BATCH_ROWS = 50_000;

function dataDirName(record: DatasetRecord): string {
  return `mdb-dataset-${record.id}`;
}

/**
 * Datetime columns come back as raw Postgres text (via identity parsers) instead of JS
 * Date objects: the generator writes UTC wall-clock values, and letting a Date parser
 * reinterpret them in the local zone would corrupt cross-engine comparison.
 */
async function openPglite(name: string, durability: DatasetRecord["durability"]): Promise<PGlite> {
  const identity = (value: string | null): string | null => value;
  // relaxedDurability would mirror Minnow's relaxed IndexedDB mode, but PGlite 0.5.4's
  // relaxed flush races its own IDBFS connection during bulk loads and crashes the worker
  // ("The database connection is closing"), so the driver stays on strict durability —
  // the only mode that survives the workload.
  void durability;
  const database = await PGlite.create(`idb://${name}`, {
    parsers: {
      [types.TIMESTAMP]: identity,
      [types.TIMESTAMPTZ]: identity,
      [types.DATE]: identity,
      // NUMERIC arrives as text to preserve precision; the other engines produce
      // doubles for the same expressions, so compare on the same footing.
      [types.NUMERIC]: (value: string | null) => (value === null ? null : Number(value)),
    },
  });
  // PGlite runs on its shipped defaults — no work_mem, planner, or JIT overrides. Measured
  // both ways, the tuning moved these query shapes by 1-12% (mostly noise), so the honest
  // simplification costs the comparison nothing.
  return database;
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
    let database = await openPglite(name, record.durability);
    let insertMs = 0;
    try {
      const entities = getScenario("commerce").entities;
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
      for (const sql of secondaryIndexSql(entities)) await database.exec(sql);
      await database.exec("ANALYZE");
      await database.syncToFs();
      // Close and reopen so what the record marks "ready" is what actually persisted.
      await database.close();
      database = await openPglite(name, record.durability);
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
      return {
        engine: "pglite",
        status: "ready",
        storageName: await observedIndexedDbName(name),
        version: "0.5.x",
        persistence: "IndexedDB VFS · persistent PostgreSQL data directory",
        storedBytes:
          sizeResult.rows[0]?.bytes === undefined ? null : Number(sizeResult.rows[0].bytes),
        buildMs: performance.now() - started,
        insertMs,
      };
    } finally {
      if (!database.closed) await database.close();
    }
  },

  async openSession(record: DatasetRecord): Promise<EngineSession> {
    const database = await openPglite(dataDirName(record), record.durability);
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
        if (!database.closed) await database.close();
      },
    };
  },

  async openWriteSession(record: DatasetRecord): Promise<WriteSession> {
    const database = await openPglite(dataDirName(record), record.durability);
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
        if (!database.closed) await database.close();
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
