/** Actual S5/W5M generator inputs, inserted through the real write path, with default tuning.
 * The 50,000-row input vectors are harness-owned data, outside the engine's 256 MiB governor.
 * No encoded group is copied from another backend. Partial memtables/log tails are left as the
 * default writer produces them; quiesce drains pending flushes/merges, not a synthetic checkpoint.
 */
import { commerceEntities, generateEntityBatch } from "../../../apps/site/bench/benchmark.js";
import { STAR_TABLES } from "../../../scripts/browser-perf/catalogue.js";
import { wideSpec, dimensionTables } from "../../../scripts/browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "../../../scripts/browser-perf/page/wide-vectors.js";
import { createLowerer } from "../src/next/lower/create.js";
import { EXECUTOR_COSTS } from "../src/next/exec/executor.js";
import { autocommit } from "../src/next/write/test-support.js";
import type { WritePathImpl } from "../src/next/write/engine.js";
import type { PhysicalType } from "../src/next/types.js";
import type { FlatVector, Batch } from "../src/next/vector/vector.js";
import { openRealStorage, type OwnerReopenStore } from "./next-storage-owner-reopen.js";

export type OwnerDataset = "S5" | "W5M";
const BATCH_ROWS = 50_000; // same batch size as scripts/browser-perf/page/app.ts
const encoder = new TextEncoder();
type InputValue = number | boolean | string | Date | null;
export function inputVector(type: PhysicalType, values: readonly InputValue[]): FlatVector {
  const length = values.length;
  const validity = values.includes(null) ? new Uint32Array(Math.ceil(length / 32)) : null;
  if (validity !== null)
    values.forEach((value, k) => {
      if (value !== null) validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
    });
  const flat = { form: "flat" as const, type, length, validity };
  switch (type.kind) {
    case "float64":
      return {
        ...flat,
        data: { layout: "f64", values: Float64Array.from(values, (value) => Number(value ?? 0)) },
      };
    case "bool":
      return {
        ...flat,
        data: {
          layout: "bool",
          values: Uint8Array.from(values, (value) => (value === true ? 1 : 0)),
        },
      };
    case "timestamp":
      return {
        ...flat,
        data: {
          layout: "int64",
          values: {
            repr: "safe",
            values: Float64Array.from(values, (value) =>
              value === null ? 0 : (Number(value) - 946_684_800_000) * 1000,
            ),
          },
        },
      };
    case "text": {
      const parts = values.map((value) => encoder.encode(String(value ?? "")));
      const offsets = new Int32Array(length + 1);
      let size = 0;
      parts.forEach((part, k) => {
        size += part.length;
        offsets[k + 1] = size;
      });
      const bytes = new Uint8Array(size);
      parts.forEach((part, k) => bytes.set(part, offsets[k] ?? 0));
      return { ...flat, data: { layout: "varlen", bytes, offsets } };
    }
    default:
      throw new Error(`Unexpected commerce/dimension physical type: ${type.kind}`);
  }
}
const batch = (columns: readonly FlatVector[]): Batch => ({
  length: columns[0]?.length ?? 0,
  columns,
  selection: null,
});

export async function loadOwnerDataset(
  path: WritePathImpl,
  dataset: OwnerDataset,
  progress: (message: string) => void,
  testRows?: number,
) {
  const lowerer = createLowerer();
  const ddl = async (text: string) => {
    const statement = lowerer.lower(text, {
      catalog: path.catalog,
      costs: EXECUTOR_COSTS,
      statistics: { table: (id) => path.statistics(id) },
    });
    if (statement.kind !== "ddl" || statement.change === null || statement.followUp !== null)
      throw new Error(`Expected plain DDL: ${text}`);
    const change = statement.change;
    await autocommit(path, (statement) => statement.changeCatalog(change));
  };
  const insert = async (name: string, vectors: readonly FlatVector[]) => {
    const table = path.catalog.tableByName(name);
    if (table === undefined) throw new Error(`Missing table ${name}`);
    await autocommit(path, (statement) => statement.table(table.id).insert(batch(vectors), null));
  };
  if (dataset === "W5M") {
    const rows = testRows ?? 5_000_000;
    const spec = wideSpec(200);
    await ddl(
      `CREATE TABLE events_wide (${spec.map((column) => `${column.name} ${column.minnow}`).join(", ")})`,
    );
    for (let start = 0; start < rows; start += BATCH_ROWS) {
      const count = Math.min(BATCH_ROWS, rows - start);
      await insert(
        "events_wide",
        spec.map((column) => vectorOf(column, typeOf(column), start, count, rows)),
      );
      progress(`events_wide ${String(start + count)}/${String(rows)}`);
    }
    for (const dimension of dimensionTables()) {
      await ddl(
        `CREATE TABLE ${dimension.name} (k TEXT PRIMARY KEY, label TEXT, tier TEXT, weight DOUBLE PRECISION, active BOOLEAN)`,
      );
      await insert(dimension.name, [
        inputVector(
          { kind: "text" },
          dimension.rows.map((row) => row.key),
        ),
        inputVector(
          { kind: "text" },
          dimension.rows.map((row) => row.label),
        ),
        inputVector(
          { kind: "text" },
          dimension.rows.map((row) => row.tier),
        ),
        inputVector(
          { kind: "float64" },
          dimension.rows.map((row) => row.weight),
        ),
        inputVector(
          { kind: "bool" },
          dimension.rows.map((row) => row.active),
        ),
      ]);
    }
  } else {
    const scale = 1667;
    for (const entity of commerceEntities.filter((entity) => STAR_TABLES.includes(entity.name))) {
      const rows =
        testRows === undefined ? entity.rows(scale) : Math.min(testRows, entity.rows(scale));
      const typeName = {
        number: "DOUBLE PRECISION",
        string: "TEXT",
        boolean: "BOOLEAN",
        datetime: "TIMESTAMP",
      };
      await ddl(
        `CREATE TABLE ${entity.name} (${entity.columns.map((column) => `${column.name} ${typeName[column.type]}${column.name === entity.primaryKey ? " PRIMARY KEY" : ""}`).join(", ")})`,
      );
      const def = path.catalog.tableByName(entity.name);
      if (def === undefined) throw new Error("DDL did not create commerce table");
      for (let start = 0; start < rows; start += BATCH_ROWS) {
        const count = Math.min(BATCH_ROWS, rows - start);
        const columns = generateEntityBatch(entity, start, count, entity.rows(scale), scale);
        await insert(
          entity.name,
          def.columns.map((column) => inputVector(column.type, columns[column.name] ?? [])),
        );
        progress(`${entity.name} ${String(start + count)}/${String(rows)}`);
      }
    }
  }
  await path.quiesce();
}

export async function buildOwnerDataset(
  store: OwnerReopenStore,
  db: string,
  dataset: OwnerDataset,
  progress: (message: string) => void,
  testRows?: number,
) {
  const opened = await openRealStorage(store, db);
  const started = performance.now();
  try {
    if (opened.path.catalog.tables.length !== 0)
      throw new Error("Actual-data builder requires an empty store");
    await loadOwnerDataset(opened.path, dataset, progress, testRows);
    const records =
      store === "indexeddb"
        ? await new Promise<Record<string, number>>((resolve, reject) => {
            const request = indexedDB.open(`b1-owner-data-${db}`);
            request.onerror = () => reject(request.error ?? new Error("Record count open failed"));
            request.onsuccess = () => {
              const connection = request.result;
              const names = ["data", "data-index", "log", "log-index"];
              const transaction = connection.transaction(names, "readonly");
              const counts: Record<string, number> = {};
              for (const name of names) {
                const count = transaction.objectStore(name).count();
                count.onsuccess = () => {
                  counts[name] = count.result;
                };
              }
              transaction.oncomplete = () => {
                connection.close();
                resolve(counts);
              };
              transaction.onabort = () => {
                connection.close();
                reject(transaction.error ?? new Error("Record count transaction aborted"));
              };
            };
          })
        : null;
    return {
      records,
      buildMs: performance.now() - started,
      rows: opened.path.catalog.tables.map((table) => ({
        name: table.name,
        rows: opened.path.statistics(table.id)?.rowCount,
      })),
    };
  } finally {
    await opened.close();
  }
}
