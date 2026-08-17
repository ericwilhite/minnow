/**
 * What the benchmarks page lets a visitor choose, and why the choices stop where they do.
 */
import type { EngineId } from "@/bench/protocol";
import { relationalTotalRows } from "@/bench/benchmark";

export interface EngineChoice {
  id: EngineId;
  label: string;
  note: string;
  /** Roughly what the browser downloads to run it, from scripts/measure-library-sizes.mts. */
  download: string;
}

export const ENGINES: readonly EngineChoice[] = [
  {
    id: "minnow",
    label: "Minnow",
    note: "This engine. Columnar blocks on IndexedDB, plain JavaScript.",
    download: "143 KB",
  },
  {
    id: "sqlite",
    label: "SQLite Wasm",
    note: "The official build, persisting through an OPFS VFS.",
    download: "1.2 MB",
  },
  {
    id: "pglite",
    label: "PGlite",
    note: "Postgres compiled to WebAssembly, persisting through IndexedDB.",
    download: "6.0 MB",
  },
];

export interface SuiteChoice {
  id: "reference" | "write" | "features";
  label: string;
  note: string;
  /** Whether the suite needs a materialized dataset before it can run. */
  needsDataset: boolean;
}

export const SUITES: readonly SuiteChoice[] = [
  {
    id: "reference",
    label: "Reads",
    note: "21 queries split between selective lookups and scans, joins and aggregates. Every result is checked against an independent oracle before a timing counts.",
    needsDataset: true,
  },
  {
    id: "write",
    label: "Writes",
    note: "Insert, update, and upsert at 1 to 100,000 rows. Only the engine's own call is timed, and the final table is compared row for row against the oracle.",
    needsDataset: true,
  },
  {
    id: "features",
    label: "SQL conformance",
    note: "Every example in the feature matrix, executed live. No dataset needed, so this one is quick.",
    needsDataset: false,
  },
];

export interface ScaleChoice {
  scale: number;
  label: string;
  rows: number;
}

/**
 * The ceiling is deliberate. At scale 10 the dataset costs about 18 MB of IndexedDB for Minnow,
 * 77 MB for SQLite and 143 MB for PGlite, and PGlite alone takes a minute or more to load it —
 * which is a reasonable thing to ask of a release capture and an unreasonable thing to do to
 * somebody who clicked a link.
 */
export const SCALES: readonly ScaleChoice[] = [0.1, 0.5, 1, 2, 5].map((scale) => ({
  scale,
  label: `${String(scale)}×`,
  rows: relationalTotalRows(scale),
}));

/** Rough stored bytes per engine at a scale, for the warning before a run starts. */
const BYTES_PER_ROW: Record<EngineId, number> = {
  minnow: 19,
  sqlite: 80,
  pglite: 150,
};

export function estimateBytes(engines: readonly EngineId[], scale: number): number {
  const rows = relationalTotalRows(scale);
  return engines.reduce((total, engine) => total + rows * BYTES_PER_ROW[engine], 0);
}

export function formatBytes(bytes: number): string {
  if (bytes >= 1_000_000_000) return `${(bytes / 1_000_000_000).toFixed(1)} GB`;
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(0)} MB`;
  return `${(bytes / 1_000).toFixed(0)} KB`;
}

export function formatRows(rows: number): string {
  if (rows >= 1_000_000) return `${(rows / 1_000_000).toFixed(1)}M`;
  if (rows >= 1_000) return `${(rows / 1_000).toFixed(0)}k`;
  return String(rows);
}

export function formatMs(ms: number): string {
  if (ms >= 1_000) return `${(ms / 1_000).toFixed(2)} s`;
  if (ms >= 10) return `${ms.toFixed(0)} ms`;
  if (ms >= 1) return `${ms.toFixed(1)} ms`;
  return `${ms.toFixed(2)} ms`;
}
