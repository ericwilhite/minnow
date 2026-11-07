/**
 * Scan-side diagnostic only, not Gate A export: real TableScan/driver and page reads,
 * with borrowed vectors consumed synchronously before advancing. No frame assembler,
 * ResultChannel, ownership copies, MessagePort or client row construction. Fixture
 * descriptors replace SQL/catalog/root reads. Endpoint fingerprints are consumption
 * guards, not an all-value oracle; dictionary values stay encoded.
 */
import type { ByteBudget, ExtentReader } from "../../../packages/core/src/next/storage/extents.js";
import { chunkReads } from "../../../packages/core/src/next/exec/scan.js";
import { TableScan, type ScanStats } from "../../../packages/core/src/next/exec/table-scan.js";
import { OutputArena } from "../../../packages/core/src/next/exec/kernels/vectors.js";
import { drive, type Operator } from "../../../packages/core/src/next/exec/pipeline.js";
import type { FlatVector, Vector } from "../../../packages/core/src/next/vector/vector.js";
import type { ExportGroup, ExportResult } from "./export-stream.js";
import { benchTableOf } from "./scan-fixture.js";

export interface ScanCoreStats {
  readonly batches: number;
  readonly columns: number;
  readonly dictionaryColumns: number;
  readonly validityColumns: number;
  readonly offsetColumns: number;
  readonly maxBatchRows: number;
  readonly fingerprint: number;
  /** Query allocation sampled at batch consumption, not a native read peak or RSS. */
  readonly queryPeakBytes: number;
  readonly scan: ScanStats;
}

function valid(vector: FlatVector, row: number): boolean {
  return vector.validity === null || ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/** Touch typed output without decoding text, copying buffers or retaining borrowed state. */
function endpoint(vector: Vector, row: number): number {
  if (vector.form === "constant") return endpoint(vector.value, 0);
  if (vector.form === "dictionary") {
    if (vector.validity !== null && ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0)
      return 0;
    return vector.codes[row] ?? 0;
  }
  if (!valid(vector, row)) return 0;
  const data = vector.data;
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return data.values[row] ?? 0;
    case "int64":
      return Number(data.values.values[row] ?? 0);
    case "decimal":
      return data.coefficients.repr === "int128"
        ? (data.coefficients.limbs[row * 4] ?? 0)
        : Number(data.coefficients.values[row] ?? 0);
    case "varlen": {
      const from = data.offsets[row] ?? 0,
        to = data.offsets[row + 1] ?? 0;
      if (from < 0 || from > to || to > data.bytes.length) throw new Error("Bad scan offsets");
      return to - from + (from < to ? (data.bytes[from] ?? 0) : 0);
    }
    case "fixed16":
      return data.bytes[row * 16] ?? 0;
    case "interval":
      return data.months[row] ?? 0;
  }
}

export async function scanCore(
  space: ExtentReader,
  groups: readonly ExportGroup[],
  options: { budget: ByteBudget & { readonly allocated: number }; readWindowBytes: number },
): Promise<ExportResult & { scanCoreStats: ScanCoreStats }> {
  const table = benchTableOf(groups),
    arena = new OutputArena(options.budget, 2048),
    scan = new TableScan({
      table,
      pages: space,
      reads: chunkReads,
      readWindowBytes: options.readWindowBytes,
      transferOutputs: false,
      budget: options.budget,
      columns: table.columns.map((c) => c.id),
      filter: null,
      emitLocator: false,
      context: { arena, params: { slots: [] }, input: [] },
    });
  let rows = 0,
    batches = 0,
    columns = 0,
    dictionaryColumns = 0,
    validityColumns = 0,
    offsetColumns = 0,
    maxBatchRows = 0,
    fingerprint = 0,
    queryPeakBytes = 0;
  const sink: Operator = {
    push(batch, transfer) {
      if (batch.selection !== null || batch.columns.length !== table.columns.length)
        throw new Error("Unexpected scan-only batch shape");
      rows += batch.length;
      batches++;
      maxBatchRows = Math.max(maxBatchRows, batch.length);
      queryPeakBytes = Math.max(queryPeakBytes, options.budget.allocated);
      for (const vector of batch.columns) {
        if (vector.length !== batch.length) throw new Error("Short scan vector");
        columns++;
        if (vector.form === "dictionary") dictionaryColumns++;
        const flat = vector.form === "constant" ? vector.value : vector;
        if (flat.validity !== null) validityColumns++;
        if (flat.form === "flat" && flat.data.layout === "varlen") offsetColumns++;
        fingerprint =
          (fingerprint + endpoint(vector, 0) + endpoint(vector, batch.length - 1)) >>> 0;
      }
      transfer?.release();
      return "ready";
    },
    ready: () => Promise.resolve(),
    finish: () => undefined,
    abort: () => undefined,
  };
  const started = performance.now();
  try {
    await drive(scan, sink);
    if (rows !== table.liveRowCount) throw new Error("Incomplete scan-only consumption");
    return {
      rows,
      ms: performance.now() - started,
      loadMs: 0,
      openMs: 0,
      loads: 0,
      scanCoreStats: {
        batches,
        columns,
        dictionaryColumns,
        validityColumns,
        offsetColumns,
        maxBatchRows,
        fingerprint,
        queryPeakBytes,
        scan: { ...scan.stats },
      },
    };
  } finally {
    scan.close();
    arena.close();
  }
}
