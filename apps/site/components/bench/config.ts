/**
 * What the benchmarks page lets a visitor choose, and why the choices stop where they do.
 */
import type { EngineId } from "@/bench/protocol";
import { relationalTotalRows } from "@/bench/benchmark";

/**
 * A column in the results, which is not quite the same thing as an engine: Minnow appears twice,
 * once executing every call and once with the result memo an application gets by default. Both
 * are the same database doing the same work, so they share one materialized copy — the second
 * column reports what a repeat costs rather than a second engine.
 */
export interface EngineChoice {
  id: ColumnId;
  /** The engine that actually runs, and the one the worker is asked for. */
  engine: EngineId;
  label: string;
  note: string;
  /** Read the memoized repeat instead of a fresh execution. */
  cached?: boolean;
  /** Roughly what the browser downloads to run it, from scripts/measure-library-sizes.mts. */
  download?: string;
}

export type ColumnId = "minnow" | "minnow-cached" | "sqlite" | "pglite";

export const ENGINES: readonly EngineChoice[] = [
  {
    id: "minnow",
    engine: "minnow",
    label: "Minnow",
    note: "This engine. Columnar blocks on IndexedDB, plain JavaScript.",
    download: "159 KB",
  },
  {
    id: "minnow-cached",
    engine: "minnow",
    cached: true,
    label: "Minnow (cached)",
    note: "The same statement repeated with the probe-validated result memo left on, which is the default an application gets. It measures the cache, not execution.",
  },
  {
    id: "sqlite",
    engine: "sqlite",
    label: "SQLite Wasm",
    note: "The official build, persisting through an OPFS VFS.",
    download: "1.2 MB",
  },
  {
    id: "pglite",
    engine: "pglite",
    label: "PGlite",
    note: "Postgres compiled to WebAssembly, persisting through IndexedDB.",
    download: "6.0 MB",
  },
];

/** The engines that have to be materialized and run for a set of chosen columns. */
export function enginesForColumns(columns: readonly ColumnId[]): EngineId[] {
  return [
    ...new Set(
      ENGINES.filter((choice) => columns.includes(choice.id)).map((choice) => choice.engine),
    ),
  ];
}

export function columnsFor(selected: readonly ColumnId[]): EngineChoice[] {
  return ENGINES.filter((choice) => selected.includes(choice.id));
}

/**
 * Where an engine keeps its data. Before a run this is the storage it is going to use; after one
 * it is the VFS the engine itself reported installing, which is not always predictable from the
 * outside — SQLite chooses between its plain OPFS VFS and the synchronous-access-handle pool at
 * load time, based on what the browser offers it. Reporting the observed choice beats explaining
 * the rule and hoping it held.
 */
export function storageLabel(choice: EngineChoice, observed?: string): string {
  if (observed !== undefined) {
    // The engine's own sentence is written for the storage table; the selector wants the name.
    const vfs = observed.split(" · ")[0];
    return choice.cached === true ? `${vfs ?? observed} · result memo on` : (vfs ?? observed);
  }
  switch (choice.engine) {
    case "minnow":
      return choice.cached === true ? "IndexedDB · result memo on" : "IndexedDB";
    case "sqlite":
      return "OPFS";
    case "pglite":
      return "IndexedDB VFS";
  }
}

export interface SuiteChoice {
  id: "reference" | "write" | "features";
  label: string;
  note: string;
  /** Whether the suite needs a materialized dataset before it can run. */
  needsDataset: boolean;
}

export const SUITES: readonly SuiteChoice[] = [
  {
    id: "write",
    label: "Writes",
    note: "Insert, update, and upsert at 1 to 100,000 rows. Only the engine's own call is timed, and the final table is compared row for row against the oracle.",
    needsDataset: true,
  },
  {
    id: "reference",
    label: "Reads",
    note: "21 queries split between selective lookups and scans, joins and aggregates. Every result is checked against an independent oracle before a timing counts.",
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

/**
 * Three significant figures, wherever the value lands. Timings are measured by the batch, so a
 * lookup that costs four microseconds is known to be four microseconds — printing it as 0.00 ms
 * would throw away the measurement, and printing every column to two decimals made a table of
 * quantized clock ticks look like agreement between engines.
 */
export function formatMs(ms: number): string {
  if (ms >= 1_000) return `${(ms / 1_000).toFixed(2)} s`;
  if (ms >= 100) return `${ms.toFixed(0)} ms`;
  if (ms >= 10) return `${ms.toFixed(1)} ms`;
  if (ms >= 1) return `${ms.toFixed(2)} ms`;
  if (ms >= 0.001) return `${ms.toPrecision(3)} ms`;
  return ms === 0 ? "0 ms" : `${(ms * 1_000).toPrecision(3)} µs`;
}
