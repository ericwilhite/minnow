/**
 * Dataset lifecycle: materialize the deterministic commerce dataset into each requested
 * engine sequentially, recording per-engine outcomes; list; delete.
 */
import { getScenario, relationalTotalRows } from "../benchmark";
import type {
  DatasetCreatePayload,
  DatasetDeletePayload,
  DatasetListResult,
  DatasetRecord,
  EngineId,
} from "../protocol";
import { engineIds, engineNames } from "../protocol";
import { loadDriver } from "../engines/session";
import { getDataset, listDatasets, putDataset, removeDatasetRecord } from "./registry";
import { assertNotCancelled, progress } from "./support";

export function validateCreatePayload(value: unknown): DatasetCreatePayload {
  if (typeof value !== "object" || value === null) throw new Error("Invalid dataset config");
  const payload = value as Partial<DatasetCreatePayload>;
  if (
    typeof payload.scale !== "number" ||
    !Number.isFinite(payload.scale) ||
    payload.scale < 0.1 ||
    payload.scale > 100
  ) {
    throw new Error("Scale must be between 0.1 and 100");
  }
  const { compression, durability } = payload;
  if (compression !== "raw" && compression !== "gzip") {
    throw new Error("Invalid compression");
  }
  if (
    !Number.isSafeInteger(payload.targetBlockBytes) ||
    (payload.targetBlockBytes ?? 0) < 64 * 1024 ||
    (payload.targetBlockBytes ?? 0) > 8 * 1024 * 1024
  ) {
    throw new Error("Invalid target block size");
  }
  if (durability !== "relaxed" && durability !== "strict") {
    throw new Error("Invalid durability");
  }
  if (payload.secondaryIndexes !== "none" && payload.secondaryIndexes !== "foreign-keys") {
    throw new Error("Invalid secondary-index mode");
  }
  if (
    !Array.isArray(payload.engines) ||
    payload.engines.length === 0 ||
    !payload.engines.every((engine) => engineIds.includes(engine))
  ) {
    throw new Error("Select at least one engine");
  }
  return {
    scale: payload.scale,
    compression,
    targetBlockBytes: payload.targetBlockBytes ?? 1_048_576,
    durability,
    secondaryIndexes: payload.secondaryIndexes,
    engines: [...new Set(payload.engines)],
  };
}

export async function datasetCreate(
  requestId: string,
  payload: DatasetCreatePayload,
): Promise<DatasetRecord> {
  const entities = getScenario("commerce").entities;
  const record: DatasetRecord = {
    id: crypto.randomUUID(),
    createdAt: new Date().toISOString(),
    scale: payload.scale,
    totalRows: relationalTotalRows(payload.scale),
    tableRows: Object.fromEntries(
      entities.map((entity) => [entity.name, entity.rows(payload.scale)]),
    ),
    compression: payload.compression,
    targetBlockBytes: payload.targetBlockBytes,
    durability: payload.durability,
    secondaryIndexes: payload.secondaryIndexes,
    engines: {},
  };
  const totalRows = record.totalRows * payload.engines.length;
  for (const [index, engine] of payload.engines.entries()) {
    assertNotCancelled(requestId);
    progress(requestId, {
      phase: "loading",
      completed: index * record.totalRows,
      total: totalRows,
      message: `Loading ${engineNames[engine]}`,
    });
    try {
      const driver = await loadDriver(engine);
      const materialization = await driver.loadDataset({
        record,
        report(message, completedRows) {
          progress(requestId, {
            phase: "loading",
            completed: index * record.totalRows + completedRows,
            total: totalRows,
            message,
          });
        },
        checkCancelled() {
          assertNotCancelled(requestId);
        },
      });
      record.engines[engine] = materialization;
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        // A cancelled run keeps whatever engines already finished; an empty record is
        // not worth keeping at all.
        await cleanupPartial(record, engine);
        if (Object.keys(record.engines).length > 0) await putDataset(record);
        throw error;
      }
      record.engines[engine] = {
        engine,
        status: "failed",
        storageName: "",
        version: "unavailable",
        persistence: "not materialized",
        dataStoredBytes: null,
        indexStoredBytes: null,
        storedBytes: null,
        buildMs: 0,
        insertMs: 0,
        indexMs: 0,
        error: error instanceof Error ? error.message : String(error),
      };
    }
    // Persist after every engine so a crash mid-run loses at most the engine in flight.
    await putDataset(record);
  }
  progress(requestId, {
    phase: "complete",
    completed: totalRows,
    total: totalRows,
    message: "Dataset ready",
  });
  return record;
}

/** Removes the partially written copy of the engine that was cancelled mid-load. */
async function cleanupPartial(record: DatasetRecord, engine: EngineId): Promise<void> {
  try {
    const driver = await loadDriver(engine);
    await driver.deleteDataset({
      engine,
      status: "failed",
      storageName:
        engine === "sqlite"
          ? `/mdb-dataset-${record.id}.sqlite3`
          : engine === "pglite"
            ? `mdb-dataset-${record.id}`
            : `mdb-dataset-${record.id}`,
      version: "",
      persistence: "",
      dataStoredBytes: null,
      indexStoredBytes: null,
      storedBytes: null,
      buildMs: 0,
      insertMs: 0,
      indexMs: 0,
    });
  } catch {
    // Best effort; orphaned partial storage is reclaimed on delete.
  }
}

export async function datasetList(): Promise<DatasetListResult> {
  return { datasets: await listDatasets() };
}

export async function datasetDelete(payload: DatasetDeletePayload): Promise<DatasetListResult> {
  if (typeof payload.id !== "string") throw new TypeError("Dataset id must be a string");
  const record = await getDataset(payload.id);
  for (const materialization of Object.values(record.engines)) {
    if (materialization.storageName === "") continue;
    try {
      const driver = await loadDriver(materialization.engine);
      await driver.deleteDataset(materialization);
    } catch {
      // The registry record still goes away; storage cleanup is best effort.
    }
  }
  await removeDatasetRecord(record.id);
  return datasetList();
}
