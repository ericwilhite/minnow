/**
 * The benchmark worker's wire contract: the versioned request/response envelopes and the
 * data shapes for the dataset, query, and suite operations. This module is imported from
 * both sides of the worker boundary and must stay free of heavy imports.
 */
import { protocolVersion } from "@minnowdb/core/worker-protocol";
import type { DurabilityMode } from "./benchmark";

// --- Request/response envelopes -----------------------------------------------------------------
//
// The versioned envelope every benchmark-worker message travels in. Shares the engine's
// protocol version so a stale worker bundle is rejected rather than misread.

const workerOperations = [
  "cancelBenchmark",
  "datasetList",
  "datasetCreate",
  "datasetDelete",
  "runQuery",
  "suiteReference",
  "suiteWrite",
  "suiteFeatureMatrix",
  "suiteLive",
] as const;

export type WorkerOperation = (typeof workerOperations)[number];

export interface WorkerRequest<T = unknown> {
  version: typeof protocolVersion;
  requestId: string;
  operation: WorkerOperation;
  payload: T;
}

export interface SuccessResponse<T = unknown> {
  version: typeof protocolVersion;
  requestId: string;
  kind: "success";
  result: T;
}

export interface FailureResponse {
  version: typeof protocolVersion;
  requestId: string;
  kind: "failure";
  error: { name: string; message: string };
}

export interface ProgressResponse<T = unknown> {
  version: typeof protocolVersion;
  requestId: string;
  kind: "progress";
  progress: T;
}

export type WorkerResponse<T = unknown> = SuccessResponse<T> | FailureResponse | ProgressResponse;

export function parseRequest(value: unknown): WorkerRequest {
  if (typeof value !== "object" || value === null) throw new TypeError("Request must be an object");
  const candidate = value as Partial<WorkerRequest>;
  if (candidate.version !== protocolVersion) throw new Error("Unsupported protocol version");
  if (typeof candidate.requestId !== "string" || candidate.requestId.length === 0) {
    throw new TypeError("Request ID must be a non-empty string");
  }
  if (!isOperation(candidate.operation)) throw new Error("Unsupported worker operation");
  return candidate as WorkerRequest;
}

export function success<T>(requestId: string, result: T): SuccessResponse<T> {
  return { version: protocolVersion, requestId, kind: "success", result };
}

export function failure(requestId: string, error: unknown): FailureResponse {
  const normalized = error instanceof Error ? error : new Error(String(error));
  return {
    version: protocolVersion,
    requestId,
    kind: "failure",
    error: { name: normalized.name, message: normalized.message },
  };
}

function isOperation(value: unknown): value is WorkerOperation {
  return (workerOperations as readonly string[]).includes(String(value));
}

// --- Payloads and results -----------------------------------------------------------------------

export type EngineId = "minnow" | "minnow-opfs" | "sqlite" | "pglite";
export type WorkloadKind = "oltp" | "olap";

export const engineIds: readonly EngineId[] = ["minnow", "minnow-opfs", "sqlite", "pglite"];

export const engineNames: Record<EngineId, string> = {
  minnow: "MinnowDatabase",
  "minnow-opfs": "MinnowDatabase (OPFS)",
  sqlite: "SQLite Wasm",
  pglite: "PGlite",
};

export type CompressionCodec = "raw" | "gzip";
export type SecondaryIndexMode = "none" | "foreign-keys";
export type RecordedSecondaryIndexMode = SecondaryIndexMode | "unknown";

/** One engine's materialized copy of a dataset. */
export interface EngineMaterialization {
  engine: EngineId;
  status: "ready" | "failed";
  /**
   * Where the copy physically lives: an IndexedDB database name for minnow, an OPFS
   * directory name for minnow-opfs, an OPFS file path for sqlite, and for pglite the
   * IndexedDB database name observed after creation (Emscripten names it after the mount
   * path, not the dataDir label).
   */
  storageName: string;
  /** sqlite only — the copy is reachable only through the VFS that wrote it. */
  vfs?: "opfs" | "sah-pool";
  version: string;
  persistence: string;
  /** Storage occupied after tables and primary keys, before optional secondary indexes. */
  dataStoredBytes: number | null;
  /** Additional logical bytes attributed to the optional secondary indexes. */
  indexStoredBytes: number | null;
  storedBytes: number | null;
  /** End-to-end materialization, including schema, inserts, indexes, verification, and reopen. */
  buildMs: number;
  /** Engine calls that insert the generated rows; harness-side row generation is excluded. */
  insertMs: number;
  /** Engine DDL calls that build the workload's secondary indexes after the bulk insert. */
  indexMs: number;
  error?: string;
}

/** One generated dataset, materialized into any of the registered engines. */
export interface DatasetRecord {
  id: string;
  createdAt: string;
  scale: number;
  totalRows: number;
  tableRows: Record<string, number>;
  compression: CompressionCodec;
  targetBlockBytes: number;
  durability: DurabilityMode;
  secondaryIndexes: RecordedSecondaryIndexMode;
  engines: Partial<Record<EngineId, EngineMaterialization>>;
}

export interface DatasetCreatePayload {
  scale: number;
  compression: CompressionCodec;
  targetBlockBytes: number;
  durability: DurabilityMode;
  secondaryIndexes: SecondaryIndexMode;
  engines: EngineId[];
}

export interface DatasetDeletePayload {
  id: string;
}

export interface DatasetListResult {
  datasets: DatasetRecord[];
}

/** Progress messages for dataset creation and suites. */
export interface WorkProgress {
  phase: string;
  completed: number;
  total: number;
  message: string;
}

export interface RunQueryPayload {
  datasetId: string;
  engines: EngineId[];
  sql: string;
}

export interface EngineQueryRun {
  engine: EngineId;
  ok: boolean;
  error?: string;
  prepareMs: number;
  medianMs: number;
  p95Ms: number;
  iterations: number;
  rowCount: number;
  columns: string[];
  previewRows: Array<Record<string, unknown>>;
  truncated: boolean;
  checksum: number;
  /** minnow only: the optimized plan from explain(). */
  plan?: string;
}

export interface RunQueryResult {
  datasetId: string;
  sql: string;
  runs: EngineQueryRun[];
  /** null when fewer than two engines returned rows. */
  checksumAgreement: "match" | "mismatch" | null;
}

export interface ReferenceSuitePayload {
  datasetId: string;
  engines: EngineId[];
}

export interface ReferenceEngineMeasurement {
  engine: EngineId;
  supported: boolean;
  error?: string;
  prepareMs: number;
  medianMs: number;
  p95Ms: number;
  /**
   * Executions per timed window. A statement quicker than the clock's resolution is run many
   * times inside one window and divided back down, so the reported median is its actual cost
   * rather than the nearest tick. One means it was timed directly.
   */
  batchSize?: number;
  /**
   * Minnow engines only: median of the same statement with its result memo left on — the
   * default an application gets. Absent for engines with no result cache, which re-execute
   * every call.
   */
  cachedMedianMs?: number;
  cachedBatchSize?: number;
  /**
   * Peak modeled execution memory in bytes, self-reported by engines that measure their own
   * execution. The browser's cross-engine memory API is unavailable in this harness (Chromium
   * refuses it without full site isolation, which Playwright's build lacks, and it does not
   * exist in workers or in Firefox at all), so this is an engine's own accounting rather than
   * an observation from outside — comparable across queries, not across engines.
   */
  peakMemoryBytes?: number;
  resultRows: number;
  checksum: number;
  /** Result tuples match the independent JavaScript oracle. */
  verified: boolean;
}

export interface ReferenceQueryReport {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
  /** OLTP is a selective lookup; OLAP is a scan, join, or aggregate. */
  workload: WorkloadKind;
  sql: string;
  tables: string[];
  expectedRows: number;
  oracleRows: number;
  surfaceGap?: string;
  engines: ReferenceEngineMeasurement[];
}

export interface ReferenceSuiteResult {
  datasetId: string;
  scale: number;
  secondaryIndexes: RecordedSecondaryIndexMode;
  sampleCount: number;
  engines: EngineId[];
  queries: ReferenceQueryReport[];
  /** Summed median execution per engine over its supported queries. */
  totalMsByEngine: Partial<Record<EngineId, number>>;
  supportedByEngine: Partial<Record<EngineId, number>>;
  /** Every query an engine could run agreed with the oracle. */
  passed: boolean;
}

export interface WriteSuitePayload {
  datasetId: string;
  engines: EngineId[];
}

export type WriteOperation = "insert" | "update" | "upsert";

export interface WriteEngineMeasurement {
  engine: EngineId;
  /** The engine ran the operation. False carries the engine's own error in `error`. */
  supported: boolean;
  error?: string;
  medianMs: number;
  p95Ms: number;
  /**
   * Writes per timed window. A write quicker than the clock's resolution is repeated across that
   * many freshly prepared tables and divided back down; one means it was timed directly.
   */
  batchSize?: number;
  /** Batch rows divided by the median, so sizes compare on one axis. */
  rowsPerSecond: number;
  /** Rows in the table after the operation; the oracle's expectation is on the case. */
  tableRows: number;
  /** FNV-1a over the final table's canonical tuples, sorted by key. */
  checksum: number;
  /** Final table state matched the independent JavaScript oracle, row for row. */
  verified: boolean;
}

export interface WriteCaseReport {
  id: string;
  name: string;
  operation: WriteOperation;
  /** OLTP is a point/small-batch mutation; OLAP is a bulk data-loading mutation. */
  workload: WorkloadKind;
  /** Rows the measured statement writes. */
  rows: number;
  /** Rows seeded (unmeasured) before the measured statement. */
  seedRows: number;
  /** Rows the oracle expects in the table afterwards. */
  expectedTableRows: number;
  /** Oracle checksum every engine's final table is compared against. */
  oracleChecksum: number;
  engines: WriteEngineMeasurement[];
  /** null when fewer than two engines ran the case. */
  checksumAgreement: "match" | "mismatch" | null;
}

export interface WriteSuiteResult {
  datasetId: string;
  scale: number;
  sampleCount: number;
  engines: EngineId[];
  /** The dedicated table this suite writes; the read dataset is never touched. */
  tableColumns: string[];
  cases: WriteCaseReport[];
  /** Summed median write time per engine over its supported cases. */
  totalMsByEngine: Partial<Record<EngineId, number>>;
  supportedByEngine: Partial<Record<EngineId, number>>;
  /** Every case an engine could run left the table exactly as the oracle predicts. */
  passed: boolean;
}

export interface LiveSuitePayload {
  datasetId: string;
  engines: EngineId[];
}

export interface LiveEngineMeasurement {
  engine: EngineId;
  /** The engine has a live-query layer and ran the case. False carries the reason in `error`. */
  supported: boolean;
  error?: string;
  /** Time to register every subscription and receive each one's initial result. */
  subscribeMs: number;
  /**
   * Commit to notification: from issuing the write until the last affected subscription's
   * `onChange` has fired with rows in hand, through the client — the RPC, the worker's commit,
   * the live-query sweep, and the change event crossing the channel.
   */
  medianMs: number;
  p95Ms: number;
  /** Change notifications received per commit, which must equal the affected subscriptions. */
  notifications: number;
  /** Every affected subscription ended on the row count the commits imply, and no other fired. */
  verified: boolean;
}

export interface LiveCaseReport {
  id: string;
  name: string;
  /** Subscriptions registered before the commit. */
  subscriptions: number;
  /** How many of them depend on the table the commit writes. */
  affected: number;
  engines: LiveEngineMeasurement[];
}

export interface LiveSuiteResult {
  datasetId: string;
  scale: number;
  sampleCount: number;
  engines: EngineId[];
  cases: LiveCaseReport[];
  supportedByEngine: Partial<Record<EngineId, number>>;
  /** Every case an engine could run delivered exactly the expected notifications. */
  passed: boolean;
}

export interface FeatureSuitePayload {
  engines: EngineId[];
}

export interface FeatureEngineOutcome {
  engine: EngineId;
  /**
   * supported feature: "pass" means the example executed. unsupported feature: for
   * minnow, "pass" means the engine still fails with the recorded error (no
   * drift); for other engines it reports whether they accept the example.
   */
  outcome: "pass" | "fail" | "accepts" | "rejects";
  /**
   * Whether the statement ran at all. The verdict above answers a different question for Minnow
   * than for the others — matrix drift versus surface survey — so the plain fact is carried
   * separately, and a count of "what does this engine accept" reads it rather than inferring it
   * from a verdict that does not mean the same thing in both columns.
   */
  accepted: boolean;
  ms: number;
  detail?: string;
}

export interface FeatureReport {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  expectedError?: string;
  notes?: string;
  engines: FeatureEngineOutcome[];
}

export interface FeatureSuiteResult {
  matrixVersion: number;
  supportedCount: number;
  unsupportedCount: number;
  engines: EngineId[];
  features: FeatureReport[];
  /** minnow results that contradict the shipped matrix. */
  driftFailures: number;
}
