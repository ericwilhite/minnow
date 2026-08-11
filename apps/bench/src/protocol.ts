/**
 * Data shapes exchanged between the pages and the benchmark worker for the dataset,
 * query, and suite operations. Types only, plus small constant tables — this module is
 * imported from both sides of the worker boundary and must stay free of heavy imports.
 */
import type { DurabilityMode } from "./benchmark.js";

export type EngineId = "browserdatabase" | "sqlite" | "pglite";

export const engineIds: readonly EngineId[] = ["browserdatabase", "sqlite", "pglite"];

export const engineNames: Record<EngineId, string> = {
  browserdatabase: "BrowserDatabase",
  sqlite: "SQLite Wasm",
  pglite: "PGlite",
};

export type CompressionCodec = "raw" | "rle" | "gzip";

/** One engine's materialized copy of a dataset. */
export interface EngineMaterialization {
  engine: EngineId;
  status: "ready" | "failed";
  /**
   * Where the copy physically lives: an IndexedDB database name for browserdatabase, an
   * OPFS file path for sqlite, and for pglite the IndexedDB database name observed after
   * creation (Emscripten names it after the mount path, not the dataDir label).
   */
  storageName: string;
  /** sqlite only — the copy is reachable only through the VFS that wrote it. */
  vfs?: "opfs" | "sah-pool";
  version: string;
  persistence: string;
  storedBytes: number | null;
  buildMs: number;
  insertMs: number;
  error?: string;
}

/** One generated dataset, materialized into up to three engines. */
export interface DatasetRecord {
  id: string;
  createdAt: string;
  scale: number;
  totalRows: number;
  tableRows: Record<string, number>;
  compression: CompressionCodec;
  targetBlockBytes: number;
  durability: DurabilityMode;
  engines: Partial<Record<EngineId, EngineMaterialization>>;
}

export interface DatasetCreatePayload {
  scale: number;
  compression: CompressionCodec;
  targetBlockBytes: number;
  durability: DurabilityMode;
  engines: EngineId[];
}

export interface DatasetDeletePayload {
  id: string;
}

export interface DatasetListResult {
  datasets: DatasetRecord[];
}

/** Progress messages for dataset creation and suites; a superset of BenchmarkProgress. */
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
  /** browserdatabase only: the optimized plan from explain(). */
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
  resultRows: number;
  checksum: number;
  /** Result tuples match the independent JavaScript oracle. */
  verified: boolean;
}

export interface ReferenceQueryReport {
  id: string;
  name: string;
  complexity: "simple" | "moderate" | "complex";
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
  sampleCount: number;
  engines: EngineId[];
  queries: ReferenceQueryReport[];
  /** Summed median execution per engine over its supported queries. */
  totalMsByEngine: Partial<Record<EngineId, number>>;
  supportedByEngine: Partial<Record<EngineId, number>>;
  /** Every query an engine could run agreed with the oracle. */
  passed: boolean;
}

export interface FeatureSuitePayload {
  engines: EngineId[];
}

export interface FeatureEngineOutcome {
  engine: EngineId;
  /**
   * supported feature: "pass" means the example executed. unsupported feature: for
   * browserdatabase, "pass" means the engine still fails with the recorded error (no
   * drift); for other engines it reports whether they accept the example.
   */
  outcome: "pass" | "fail" | "accepts" | "rejects";
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
  /** browserdatabase results that contradict the shipped matrix. */
  driftFailures: number;
}
