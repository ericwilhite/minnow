/**
 * The write suite: insert, update, and upsert at several batch sizes, on every engine, each
 * through that engine's own bulk path — minnow's batch write API, one sqlite prepared
 * statement stepped inside a transaction, PGlite's multi-row statements inside a
 * transaction, and duckdb's Arrow append. No engine is tuned; every one runs on its shipped
 * defaults, exactly as in the read comparison.
 *
 * Four rules keep the numbers meaningful:
 *
 *  - Every sample writes into a table created for that sample, so sample two never measures
 *    a table sample one already grew, and no measurement inherits another's fragmentation.
 *  - Only the engine call is timed. Each driver first converts the batch into the form its
 *    API takes (see WriteTarget in engines/session.ts); pivoting 100,000 columnar rows into
 *    tuples costs ~60ms of JavaScript, which would otherwise be charged to the engines whose
 *    APIs want rows and to none of the others.
 *  - The table is dedicated to this suite and lives beside the dataset's own tables, which
 *    are never read or written here. Writing into a database that already holds data is the
 *    case worth measuring, and it keeps each engine's persistence settings identical to the
 *    read comparison. minnow has no DROP TABLE, so its per-sample tables stay until the
 *    dataset is deleted; the others drop theirs.
 *  - The final table is read back — with minnow's result memo off — and compared row for row
 *    against an oracle built in JavaScript from the same deterministic inputs. A timing an
 *    engine cannot back with the right table state is reported unverified, never as a win.
 */
import { engineIds } from "../protocol.js";
import type {
  EngineId,
  WriteCaseReport,
  WriteEngineMeasurement,
  WriteOperation,
  WriteSuitePayload,
  WriteSuiteResult,
} from "../protocol.js";
import {
  loadDriver,
  requireMaterialization,
  type WriteBatch,
  type WriteSession,
  type WriteTableSchema,
  type WriteTarget,
} from "../engines/session.js";
import { canonicalTuples, referenceChecksum, tuplesMatch } from "./canonical.js";
import { getDataset } from "./registry.js";
import { assertNotCancelled, progress, summarizeSamples } from "./support.js";

const SAMPLE_COUNT = 5;

/** Column names in table order; the read-back tuple is compared in exactly this order. */
const COLUMNS = ["key_id", "payload_num", "payload_text", "updated_at"] as const;

/** The one column an update changes. */
const UPDATED_COLUMN = "payload_num";

const EPOCH = Date.UTC(2026, 0, 1);

export interface WriteCaseDefinition {
  id: string;
  name: string;
  operation: WriteOperation;
  /** Rows the measured statement writes. */
  rows: number;
  /** Rows written before the measurement, unmeasured, so the operation has something to hit. */
  seedRows: number;
  /** First key the measured batch writes; seeded rows always start at 1. */
  firstKey: number;
  expectedTableRows: number;
}

export function validateWritePayload(value: unknown): WriteSuitePayload {
  if (typeof value !== "object" || value === null) throw new Error("Invalid suite payload");
  const payload = value as Partial<WriteSuitePayload>;
  if (typeof payload.datasetId !== "string") throw new TypeError("Dataset id must be a string");
  if (
    !Array.isArray(payload.engines) ||
    payload.engines.length === 0 ||
    !payload.engines.every((engine): engine is EngineId => engineIds.includes(engine))
  ) {
    throw new Error("Select at least one engine");
  }
  return { datasetId: payload.datasetId, engines: [...new Set(payload.engines)] };
}

/**
 * Inserts start from an empty table. Updates and upserts seed the same number of rows they
 * then write, so the measured statement always addresses a table its own size. An upsert's
 * keys start halfway through the seeded range: half land on existing rows and update in
 * place, half are new. (At one row that halving degenerates — the single key collides.)
 */
export function writeCaseDefinitions(): WriteCaseDefinition[] {
  const insertSizes = [1, 100, 10_000, 100_000];
  const keyedSizes = [1, 100, 10_000];
  const cases: WriteCaseDefinition[] = insertSizes.map((rows) => ({
    id: `insert-${String(rows)}`,
    name: `Insert ${formatRows(rows)}`,
    operation: "insert",
    rows,
    seedRows: 0,
    firstKey: 1,
    expectedTableRows: rows,
  }));
  for (const rows of keyedSizes) {
    cases.push({
      id: `update-${String(rows)}`,
      name: `Update ${formatRows(rows)}`,
      operation: "update",
      rows,
      seedRows: rows,
      firstKey: 1,
      expectedTableRows: rows,
    });
  }
  for (const rows of keyedSizes) {
    const firstKey = Math.floor(rows / 2) + 1;
    cases.push({
      id: `upsert-${String(rows)}`,
      name: `Upsert ${formatRows(rows)}`,
      operation: "upsert",
      rows,
      seedRows: rows,
      firstKey,
      expectedTableRows: firstKey - 1 + rows,
    });
  }
  return cases;
}

function formatRows(rows: number): string {
  return `${new Intl.NumberFormat("en-US").format(rows)} row${rows === 1 ? "" : "s"}`;
}

export async function runWriteSuite(
  requestId: string,
  payload: WriteSuitePayload,
): Promise<WriteSuiteResult> {
  const record = await getDataset(payload.datasetId);
  const definitions = writeCaseDefinitions();
  const sessions = new Map<EngineId, WriteSession | Error>();
  for (const engine of payload.engines) {
    try {
      requireMaterialization(record, engine);
      const driver = await loadDriver(engine);
      sessions.set(engine, await driver.openWriteSession(record));
    } catch (error) {
      sessions.set(engine, error instanceof Error ? error : new Error(String(error)));
    }
  }
  // Every run gets its own table names: minnow has no DROP TABLE, so a fixed name would
  // collide with a previous run on the same dataset.
  const runToken = Math.random().toString(36).slice(2, 8);
  const totalSteps = definitions.length * payload.engines.length;
  let completed = 0;
  const cases: WriteCaseReport[] = [];
  try {
    for (const definition of definitions) {
      assertNotCancelled(requestId);
      const oracleTuples = canonicalTuples(oracleRows(definition), projectRow);
      const oracleChecksum = referenceChecksum(oracleTuples);
      const measurements: WriteEngineMeasurement[] = [];
      for (const engine of payload.engines) {
        assertNotCancelled(requestId);
        progress(requestId, {
          phase: "writes",
          completed,
          total: totalSteps,
          message: `${definition.name} · ${engine}`,
        });
        const session = sessions.get(engine);
        measurements.push(
          session === undefined || session instanceof Error
            ? unsupported(
                engine,
                session instanceof Error ? session.message : "session unavailable",
              )
            : await measureCase(requestId, session, engine, definition, runToken, oracleTuples),
        );
        completed += 1;
      }
      cases.push({
        id: definition.id,
        name: definition.name,
        operation: definition.operation,
        rows: definition.rows,
        seedRows: definition.seedRows,
        expectedTableRows: definition.expectedTableRows,
        oracleChecksum,
        engines: measurements,
        checksumAgreement: agreement(measurements),
      });
    }
  } finally {
    for (const session of sessions.values()) {
      if (!(session instanceof Error)) await session.close().catch(() => undefined);
    }
  }
  const totalMsByEngine: Partial<Record<EngineId, number>> = {};
  const supportedByEngine: Partial<Record<EngineId, number>> = {};
  for (const engine of payload.engines) {
    const engineMeasurements = cases.flatMap((report) =>
      report.engines.filter((measurement) => measurement.engine === engine),
    );
    totalMsByEngine[engine] = engineMeasurements.reduce(
      (total, measurement) => total + measurement.medianMs,
      0,
    );
    supportedByEngine[engine] = engineMeasurements.filter(
      (measurement) => measurement.supported,
    ).length;
  }
  progress(requestId, {
    phase: "complete",
    completed: totalSteps,
    total: totalSteps,
    message: "Write suite complete",
  });
  return {
    datasetId: record.id,
    scale: record.scale,
    sampleCount: SAMPLE_COUNT,
    engines: payload.engines,
    tableColumns: [...COLUMNS],
    cases,
    totalMsByEngine,
    supportedByEngine,
    passed: cases.every((report) =>
      report.engines.every((measurement) => !measurement.supported || measurement.verified),
    ),
  };
}

function unsupported(engine: EngineId, error: string): WriteEngineMeasurement {
  return {
    engine,
    supported: false,
    error,
    medianMs: 0,
    p95Ms: 0,
    rowsPerSecond: 0,
    tableRows: 0,
    checksum: 0,
    verified: false,
  };
}

/** "match" once at least two engines ran the case and every checksum is the same value. */
function agreement(measurements: readonly WriteEngineMeasurement[]): "match" | "mismatch" | null {
  const checksums = measurements
    .filter((measurement) => measurement.supported)
    .map((measurement) => measurement.checksum);
  if (checksums.length < 2) return null;
  return checksums.every((checksum) => checksum === checksums[0]) ? "match" : "mismatch";
}

async function measureCase(
  requestId: string,
  session: WriteSession,
  engine: EngineId,
  definition: WriteCaseDefinition,
  runToken: string,
  oracleTuples: readonly unknown[][],
): Promise<WriteEngineMeasurement> {
  const samples: number[] = [];
  const tableBase = `bw_${runToken}_${definition.id.replaceAll("-", "_")}`;
  // Payloads are built once, before anything is timed: materializing 100,000 rows of
  // JavaScript is the harness's cost, not the engine's, and it must not land in the number.
  const seed = definition.seedRows > 0 ? batchFor(1, definition.seedRows, 0) : undefined;
  const prepare = prepareStep(definition);
  let target: WriteTarget | undefined;
  try {
    for (let sample = 0; sample < SAMPLE_COUNT; sample += 1) {
      assertNotCancelled(requestId);
      // The previous sample's table is dropped where the engine can, and always abandoned:
      // every sample runs against a table it created empty.
      if (target !== undefined) await target.drop().catch(() => undefined);
      target = await session.createTable(tableSchema(`${tableBase}_${String(sample)}`));
      if (seed !== undefined) await target.prepareInsert(seed)();
      // The batch is converted into the engine's own argument form here, outside the timer;
      // the returned call is the engine doing the write and nothing else.
      const apply = prepare(target);
      const started = performance.now();
      await apply();
      samples.push(performance.now() - started);
    }
    // Samples are identical by construction, so the last table's state is every sample's
    // state; verifying it once keeps a 100k read-back out of the measured loop.
    const rows = await (target?.readAll() ?? Promise.resolve([]));
    const tuples = canonicalTuples(rows, projectRow);
    const { medianMs, p95Ms } = summarizeSamples(samples);
    return {
      engine,
      supported: true,
      medianMs,
      p95Ms,
      rowsPerSecond: medianMs > 0 ? (definition.rows / medianMs) * 1_000 : 0,
      tableRows: rows.length,
      checksum: referenceChecksum(tuples),
      verified: rows.length === definition.expectedTableRows && tuplesMatch(tuples, oracleTuples),
    };
  } catch (error) {
    return unsupported(engine, error instanceof Error ? error.message : String(error));
  } finally {
    if (target !== undefined) await target.drop().catch(() => undefined);
  }
}

/**
 * Builds the case's payload once and returns how to hand it to a table: the engine converts
 * it into its own argument form when the step is prepared, and the call that comes back is
 * the only thing timed.
 */
function prepareStep(
  definition: WriteCaseDefinition,
): (target: WriteTarget) => () => Promise<void> {
  if (definition.operation === "update") {
    const { keys, values } = updateVectors(definition);
    return (target) => target.prepareUpdate(keys, UPDATED_COLUMN, values);
  }
  const batch = batchFor(definition.firstKey, definition.rows, 1);
  return definition.operation === "insert"
    ? (target) => target.prepareInsert(batch)
    : (target) => target.prepareUpsert(batch);
}

function tableSchema(name: string): WriteTableSchema {
  return {
    name,
    primaryKey: COLUMNS[0],
    columns: [
      { name: COLUMNS[0], type: "number" },
      { name: COLUMNS[1], type: "number" },
      { name: COLUMNS[2], type: "string" },
      { name: COLUMNS[3], type: "datetime" },
    ],
  };
}

/**
 * One deterministic batch of `rowCount` rows keyed `firstKey…firstKey+rowCount-1`.
 * `generation` separates rows a measured statement writes (1) from seeded rows (0), so a
 * read-back proves the write actually landed instead of merely counting rows. Values are
 * exactly representable as doubles, so every engine round-trips them bit for bit.
 */
function batchFor(firstKey: number, rowCount: number, generation: number): WriteBatch {
  const keys = new Array<number>(rowCount);
  const numbers = new Array<number>(rowCount);
  const texts = new Array<string>(rowCount);
  const timestamps = new Array<Date>(rowCount);
  for (let index = 0; index < rowCount; index += 1) {
    const key = firstKey + index;
    keys[index] = key;
    numbers[index] = payloadNumber(key, generation);
    texts[index] = payloadText(key, generation);
    timestamps[index] = payloadTimestamp(key, generation);
  }
  return {
    rowCount,
    columns: {
      [COLUMNS[0]]: keys,
      [COLUMNS[1]]: numbers,
      [COLUMNS[2]]: texts,
      [COLUMNS[3]]: timestamps,
    },
  };
}

/** The keys an update addresses and the one column's worth of new values, aligned. */
function updateVectors(definition: WriteCaseDefinition): { keys: number[]; values: number[] } {
  const keys = new Array<number>(definition.rows);
  const values = new Array<number>(definition.rows);
  for (let index = 0; index < definition.rows; index += 1) {
    const key = definition.firstKey + index;
    keys[index] = key;
    values[index] = payloadNumber(key, 1);
  }
  return { keys, values };
}

function payloadNumber(key: number, generation: number): number {
  return (key % 4_096) + generation * 0.5;
}

function payloadText(key: number, generation: number): string {
  return `g${String(generation)}-row-${String(key)}`;
}

function payloadTimestamp(key: number, generation: number): Date {
  return new Date(EPOCH + (key % 365) * 86_400_000 + generation * 3_600_000);
}

function projectRow(row: Record<string, unknown>): unknown[] {
  return COLUMNS.map((column) => row[column]);
}

/**
 * The independent expectation: what the table must hold once the operation has run,
 * rebuilt from the same rules the batches were built from rather than from any engine.
 * An update changes one column and leaves the rest at generation 0; an upsert replaces a
 * colliding row entirely and appends the rest.
 */
export function oracleRows(definition: WriteCaseDefinition): Array<Record<string, unknown>> {
  const rows = new Map<number, Record<string, unknown>>();
  for (let key = 1; key <= definition.seedRows; key += 1) rows.set(key, oracleRow(key, 0));
  const lastKey = definition.firstKey + definition.rows - 1;
  for (let key = definition.firstKey; key <= lastKey; key += 1) {
    if (definition.operation === "update") {
      const existing = rows.get(key);
      if (existing === undefined) continue;
      existing[COLUMNS[1]] = payloadNumber(key, 1);
      continue;
    }
    rows.set(key, oracleRow(key, 1));
  }
  return [...rows.values()];
}

function oracleRow(key: number, generation: number): Record<string, unknown> {
  return {
    [COLUMNS[0]]: key,
    [COLUMNS[1]]: payloadNumber(key, generation),
    [COLUMNS[2]]: payloadText(key, generation),
    [COLUMNS[3]]: payloadTimestamp(key, generation),
  };
}
