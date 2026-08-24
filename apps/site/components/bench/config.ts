/**
 * What the benchmarks page lets a visitor choose, and why the choices stop where they do.
 */
import type { EngineId, SecondaryIndexMode } from "@/bench/protocol";
import { relationalTotalRows } from "@/bench/benchmark";

/**
 * A column in the results, which is not quite the same thing as an engine: Minnow appears twice
 * per store — executing every call and with the result memo an application gets by default.
 * Both are the same database doing the same work, so they share one materialized copy; the variant
 * column reports what a repeat costs rather than a second engine.
 */
export interface EngineChoice {
  id: ColumnId;
  /** The engine that actually runs, and the one the worker is asked for. */
  engine: EngineId;
  label: string;
  note: string;
  /**
   * Which of the engine's read measurements the column shows. Absent is plain execution.
   */
  variant?: "cached";
  /** Roughly what the browser downloads to run it, from scripts/measure-library-sizes.mts. */
  download?: string;
}

export type ColumnId =
  "minnow" | "minnow-cached" | "minnow-opfs" | "minnow-opfs-cached" | "sqlite" | "pglite";

export const ENGINES: readonly EngineChoice[] = [
  {
    id: "minnow",
    engine: "minnow",
    label: "Minnow",
    note: "This engine. Columnar blocks on IndexedDB, plain JavaScript.",
    download: "220 KB",
  },
  {
    id: "minnow-cached",
    engine: "minnow",
    variant: "cached",
    label: "Minnow (cached)",
    note: "The same statement repeated with the probe-validated result memo left on, which is the default an application gets. It measures the cache, not execution.",
  },
  {
    id: "minnow-opfs",
    engine: "minnow-opfs",
    label: "Minnow (OPFS)",
    note: "The same engine over the Origin Private File System: a leader tab holds the file handles and commits through a write-ahead log at synchronous-I/O speed. Same download as Minnow.",
  },
  {
    id: "minnow-opfs-cached",
    engine: "minnow-opfs",
    variant: "cached",
    label: "Minnow (OPFS, cached)",
    note: "The OPFS store's repeat with the probe-validated result memo left on, which is the default an application gets. It measures the cache, not execution.",
  },
  {
    id: "sqlite",
    engine: "sqlite",
    label: "SQLite Wasm",
    note: "The official build, persisting through an OPFS VFS.",
    download: "457 KB",
  },
  {
    id: "pglite",
    engine: "pglite",
    label: "PGlite",
    note: "Postgres compiled to WebAssembly, persisting through IndexedDB.",
    download: "5.6 MB",
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
  const suffix = choice.variant === "cached" ? " · result memo on" : "";
  if (observed !== undefined) {
    // The engine's own sentence is written for the storage table; the selector wants the name.
    const vfs = observed.split(" · ")[0];
    return `${vfs ?? observed}${suffix}`;
  }
  switch (choice.engine) {
    case "minnow":
      return `IndexedDB${suffix}`;
    case "minnow-opfs":
      return `OPFS${suffix}`;
    case "sqlite":
      return "OPFS";
    case "pglite":
      return "IndexedDB VFS";
  }
}

export interface SuiteChoice {
  id: "reference" | "write" | "live" | "features";
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
    id: "live",
    label: "Live queries",
    note: "1 to 100 subscriptions registered through the worker client, then one commit: the time until every affected subscription has been notified, with its rows in hand. Minnow only — the other engines have no live-query layer.",
    needsDataset: true,
  },
  {
    id: "features",
    label: "PostgreSQL compatibility",
    note: "Every documented SQL example, executed live. No dataset is needed, so this check is quick.",
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
const BASE_BYTES_PER_ROW: Record<EngineId, number> = {
  minnow: 19,
  "minnow-opfs": 20,
  sqlite: 60,
  pglite: 150,
};

const INDEX_BYTES_PER_ROW: Record<EngineId, number> = {
  minnow: 41,
  "minnow-opfs": 20,
  sqlite: 25,
  pglite: 20,
};

export function estimateBytes(
  engines: readonly EngineId[],
  scale: number,
  secondaryIndexes: SecondaryIndexMode,
): number {
  const rows = relationalTotalRows(scale);
  return engines.reduce(
    (total, engine) =>
      total +
      rows *
        (BASE_BYTES_PER_ROW[engine] +
          (secondaryIndexes === "foreign-keys" ? INDEX_BYTES_PER_ROW[engine] : 0)),
    0,
  );
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
