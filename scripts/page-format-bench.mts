/**
 * Benchmarks the rebuild core's page format (packages/core/src/next/page) against today's block
 * format on realistic columns of the wide analytics table (scripts/browser-perf/page/wide-data.ts).
 *
 *   npx vite-node scripts/page-format-bench.mts [--rows 1048576] [--repeats 7]
 *
 * For each column it writes `rows` values as 64k-row groups into an in-memory data space, then:
 * - encode: the group writer, from vectors (values/s, MB/s of vector bytes);
 * - load: read + xxHash32 verify + parse of every page (MB/s of stored bytes);
 * - decode: every row into a reused 2,048-row target, batch by batch (values/s, MB/s of output
 *   vector bytes); text dictionary columns decode to codes (no strings);
 * - select 0.1%: the same with a random 0.1 % of rows gathered (late materialization), reported
 *   per selected row and as a share of the full decode;
 * - bytes per value: stored pages + directories, against today's block format (raw and gzip) on
 *   the same values, and today's verified decode (`decodeBlock`) for the decode ratio.
 * Each timing is the median of `repeats` runs after one warm-up. The machine is shared: compare
 * ratios, not absolute numbers.
 */
import { performance } from "node:perf_hooks";
import { decodeBlock, encodeBlock } from "../packages/core/src/block-format/index.ts";
import type { ColumnInput } from "../packages/core/src/block-format/types.ts";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import { MemoryExtentSpace, UNLIMITED } from "../packages/core/src/next/page/memory-extents.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import { InflatedPageCache } from "../packages/core/src/next/page/inflated-cache.ts";
import { targetFor } from "../packages/core/src/next/page/test-support.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";
import type { PhysicalType } from "../packages/core/src/next/types.ts";
import type { FlatVector, VectorData } from "../packages/core/src/next/vector/vector.ts";
import { wideSpec, wideValue, type WideColumn } from "./browser-perf/page/wide-data.ts";

const args = process.argv.slice(2);
const option = (name: string, fallback: number): number => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? Number(args[index + 1]) : fallback;
};
const ROWS = option("rows", 1_048_576);
const REPEATS = option("repeats", 7);
const GROUP = 65_536;
const BATCH = 2048;
const encoder = new TextEncoder();
const EPOCH_2000_MS = 946_684_800_000;

interface Bench {
  readonly label: string;
  readonly column: string;
  readonly type: PhysicalType;
  /** Old block format logical type and value. */
  readonly old: "number" | "string" | "boolean" | "datetime";
  readonly gzip?: boolean;
}

const BENCHES: Bench[] = [
  {
    label: "int32 FOR u8 (i00, 0–99)",
    column: "i00",
    type: { kind: "int32", bits: 32 },
    old: "number",
  },
  {
    label: "int32 FOR u16 (i06, 0–65535)",
    column: "i06",
    type: { kind: "int32", bits: 32 },
    old: "number",
  },
  {
    label: "int32 plain (i04, ±1e9)",
    column: "i04",
    type: { kind: "int32", bits: 32 },
    old: "number",
  },
  {
    label: "int32 delta (i03, row/100)",
    column: "i03",
    type: { kind: "int32", bits: 32 },
    old: "number",
  },
  { label: "int64 id (delta w0)", column: "id", type: { kind: "int64" }, old: "number" },
  {
    label: "timestamp (ts)",
    column: "ts",
    type: { kind: "timestamp", zoned: false },
    old: "datetime",
  },
  { label: "float64 (f02)", column: "f02", type: { kind: "float64" }, old: "number" },
  {
    label: "numeric(12,2) (n01)",
    column: "n01",
    type: { kind: "decimal", precision: 12, scale: 2 },
    old: "string",
  },
  { label: "bool (b02)", column: "b02", type: { kind: "bool" }, old: "boolean" },
  { label: "text dict → codes (d07, 1000)", column: "d07", type: { kind: "text" }, old: "string" },
  { label: "text plain (h08, 20–60 B)", column: "h08", type: { kind: "text" }, old: "string" },
  { label: "text plain (h01, emails)", column: "h01", type: { kind: "text" }, old: "string" },
  {
    label: "text plain + gzip (h08)",
    column: "h08",
    type: { kind: "text" },
    old: "string",
    gzip: true,
  },
];

const spec = new Map(wideSpec(200).map((column) => [column.name, column]));

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

/**
 * Median ms per call over `REPEATS` samples. Warms up for ≥ 200 ms (JIT tiers), then repeats the
 * call enough times per sample that each sample lasts ≥ 20 ms.
 */
async function timed(run: () => Promise<void> | void): Promise<number> {
  let calls = 0;
  const warm = performance.now();
  while (performance.now() - warm < 200 || calls < 3) {
    await run();
    calls++;
  }
  const perCall = (performance.now() - warm) / calls;
  const inner = Math.max(1, Math.ceil(20 / Math.max(perCall, 1e-3)));
  const times: number[] = [];
  for (let repeat = 0; repeat < REPEATS; repeat++) {
    const start = performance.now();
    for (let k = 0; k < inner; k++) await run();
    times.push((performance.now() - start) / inner);
  }
  return median(times);
}

/** One group's vector and the matching old-format values. */
function groupData(
  bench: Bench,
  column: WideColumn,
  first: number,
  count: number,
): { vector: FlatVector; old: unknown[]; bytes: number } {
  const raw = Array.from({ length: count }, (_, k) => wideValue(column, first + k, ROWS));
  let data: VectorData;
  let old: unknown[] = raw;
  let bytes: number;
  switch (bench.type.kind) {
    case "int32":
      data = { layout: "i32", values: Int32Array.from(raw as number[]) };
      bytes = count * 4;
      break;
    case "int64":
      data = {
        layout: "int64",
        values: { repr: "safe", values: Float64Array.from(raw as number[]) },
      };
      bytes = count * 8;
      break;
    case "timestamp":
      data = {
        layout: "int64",
        values: {
          repr: "safe",
          values: Float64Array.from(raw as number[], (ms) => (ms - EPOCH_2000_MS) * 1000),
        },
      };
      old = (raw as number[]).map((ms) => new Date(ms));
      bytes = count * 8;
      break;
    case "float64":
      data = { layout: "f64", values: Float64Array.from(raw as number[]) };
      bytes = count * 8;
      break;
    case "decimal":
      data = {
        layout: "decimal",
        scale: 2,
        coefficients: {
          repr: "safe",
          values: Float64Array.from(raw as string[], (text) => Math.round(Number(text) * 100)),
        },
        scales: null,
        specials: null,
      };
      bytes = count * 8;
      break;
    case "bool":
      data = {
        layout: "bool",
        values: Uint8Array.from(raw as boolean[], (value) => (value ? 1 : 0)),
      };
      bytes = count;
      break;
    default: {
      const parts = (raw as string[]).map((text) => encoder.encode(text));
      const offsets = new Int32Array(count + 1);
      let total = 0;
      parts.forEach((part, row) => (offsets[row + 1] = total += part.length));
      const out = new Uint8Array(total);
      parts.forEach((part, row) => out.set(part, offsets[row] ?? 0));
      data = { layout: "varlen", bytes: out, offsets };
      bytes = total + count * 4;
    }
  }
  return {
    vector: { form: "flat", type: bench.type, length: count, validity: null, data },
    old,
    bytes,
  };
}

interface Result {
  label: string;
  encodeMs: number;
  loadMs: number;
  warmMs: number;
  decodeMs: number;
  selectMs: number;
  selected: number;
  storedBytes: number;
  pages: number;
  allocatedBytes: number;
  inputBytes: number;
  outputBytes: number;
  oldRaw: number;
  oldGzip: number;
  oldDecodeMs: number;
  encodings: string;
}

const ENCODING_NAMES: Record<number, string> = {
  1: "plain",
  2: "constant",
  3: "bitmap",
  4: "for",
  5: "dict",
};

async function run(bench: Bench): Promise<Result> {
  const column = spec.get(bench.column);
  if (column === undefined) throw new Error(`No wide column ${bench.column}`);
  const groups = Array.from({ length: Math.ceil(ROWS / GROUP) }, (_, group) =>
    groupData(bench, column, group * GROUP, Math.min(GROUP, ROWS - group * GROUP)),
  );
  const inputBytes = groups.reduce((sum, group) => sum + group.bytes, 0);

  let space = new MemoryExtentSpace();
  let chunks: ColumnChunk[] = [];
  const write = async (): Promise<void> => {
    space = new MemoryExtentSpace();
    const extents = space.beginWrite(bench.label);
    chunks = [];
    for (const group of groups) {
      const writer = new GroupWriter({
        columns: [{ columnId: 1, type: bench.type }],
        writer: extents,
        gzip: bench.gzip ?? false,
      });
      await writer.append([group.vector], group.vector.length);
      const written = await writer.finish();
      chunks.push(written.columns[0] ?? (undefined as never));
    }
  };
  const encodeMs = await timed(write);

  // Cold: every page read through the space (xxHash32 verify on the miss) and parsed.
  // Warm: every blob cached (gzip pages in the inflated-page cache), so open and load run
  // synchronously (peek + parse).
  let readers: ChunkReader[] = [];
  const inflated = new InflatedPageCache(() => ({ charge: () => true, release: () => undefined }));
  const loadWith = async (pages: MemoryExtentSpace): Promise<void> => {
    for (const reader of readers) reader.close();
    readers = [];
    for (const chunk of chunks) {
      const reader = await ChunkReader.open(pages, chunk, { budget: UNLIMITED, inflated });
      await reader.load(reader.directory.pages.map((_, index) => index));
      readers.push(reader);
    }
  };
  const loadMs = await timed(() => {
    for (const reader of readers) reader.close();
    readers = [];
    inflated.clear();
    return loadWith(space.view({ cache: false }));
  });
  const warmSpace = space.view({});
  await loadWith(warmSpace);
  const warmMs = await timed(() => loadWith(warmSpace));
  const encodingSet = new Set<string>();
  let storedBytes = 0;
  let pages = 0;
  let allocatedBytes = 0;
  const extents = new Set<string>();
  for (const [index, reader] of readers.entries()) {
    const chunk = chunks[index] ?? (undefined as never);
    for (const page of reader.directory.pages) {
      encodingSet.add(
        `${ENCODING_NAMES[page.encoding & 0x7f] ?? "?"}${(page.encoding & 128) !== 0 ? "+gz" : ""}`,
      );
      storedBytes += page.length;
      pages++;
    }
    storedBytes += (reader.directory.dictionary?.length ?? 0) + chunk.directory.length;
    const key = `${String(chunk.data.extent.offset)}`;
    if (!extents.has(key)) {
      extents.add(key);
      allocatedBytes += Math.ceil(chunk.data.extent.length / 4096) * 4096;
    }
  }

  const sample = readers[0] ?? (undefined as never);
  const dictionary = sample.isDictionaryRange(0, sample.rowCount);
  const varlen = bench.type.kind === "text";
  const target = targetFor(sample, BATCH, varlen ? 4 * 1024 * 1024 : 0);
  const codes = new Int32Array(BATCH);
  let outputBytes = 0;
  const decode = (): void => {
    outputBytes = 0;
    for (const reader of readers) {
      const rows = reader.rowCount;
      for (let from = 0; from < rows; from += BATCH) {
        const to = Math.min(rows, from + BATCH);
        if (dictionary) {
          reader.decodeCodes(from, to, codes, null, 0);
          outputBytes += (to - from) * 4;
        } else {
          if (target.data.layout === "varlen") target.data.offsets[0] = 0;
          reader.decodeRange(from, to, target, 0);
          outputBytes +=
            target.data.layout === "varlen"
              ? (target.data.offsets[to - from] ?? 0) + (to - from) * 4
              : (to - from) * widthOf(target);
        }
      }
    }
  };
  const decodeMs = await timed(decode);

  // 0.1 % selection, ascending, per group.
  let state = 0x9e37_79b9;
  const random = (): number =>
    (state = (Math.imul(state, 1_103_515_245) + 12_345) >>> 0) / 4_294_967_296;
  const selections = readers.map((reader) => {
    const picked: number[] = [];
    for (let row = 0; row < reader.rowCount; row++) if (random() < 0.001) picked.push(row);
    return Uint16Array.from(picked);
  });
  const selected = selections.reduce((sum, rows) => sum + rows.length, 0);
  const gatherTarget = targetFor(sample, 1024, varlen ? 1024 * 1024 : 0);
  const select = (): void => {
    readers.forEach((reader, index) => {
      const rows = selections[index] ?? new Uint16Array(0);
      if (dictionary) reader.gatherCodes(rows, codes, null, 0);
      else {
        if (gatherTarget.data.layout === "varlen") gatherTarget.data.offsets[0] = 0;
        reader.gather(rows, gatherTarget, 0);
      }
    });
  };
  const selectMs = await timed(select);

  // Today's block format on the same values (one block per group).
  const oldBlocks: Uint8Array[] = [];
  let oldRaw = 0;
  let oldGzip = 0;
  for (const group of groups) {
    const input = { type: bench.old, values: group.old } as ColumnInput;
    const raw = await encodeBlock(input, "raw");
    oldBlocks.push(raw);
    oldRaw += raw.length;
    oldGzip += (await encodeBlock(input, "gzip")).length;
  }
  const oldDecodeMs = await timed(async () => {
    for (const block of oldBlocks) await decodeBlock(block);
  });

  return {
    label: bench.label,
    encodeMs,
    loadMs,
    warmMs,
    decodeMs,
    selectMs,
    selected,
    storedBytes,
    pages,
    allocatedBytes,
    inputBytes,
    outputBytes,
    oldRaw,
    oldGzip,
    oldDecodeMs,
    encodings: [...encodingSet].join(","),
  };
}

function widthOf(vector: FlatVector): number {
  switch (vector.data.layout) {
    case "bool":
      return 1;
    case "i32":
    case "f32":
      return 4;
    default:
      return 8;
  }
}

const mb = (bytes: number, ms: number): string => (bytes / 1e6 / (ms / 1000)).toFixed(0);
const mvals = (count: number, ms: number): string => (count / 1e6 / (ms / 1000)).toFixed(0);

console.log(
  `rows ${String(ROWS)} per column, groups of ${String(GROUP)}, batches of ${String(BATCH)}, median of ${String(REPEATS)}`,
);
console.log(
  [
    "column",
    "encodings",
    "encode Mv/s",
    "cold load MB/s (µs/page)",
    "warm load µs/page",
    "decode Mv/s",
    "decode MB/s",
    "0.1% ns/row",
    "0.1% vs full",
    "B/value",
    "old raw",
    "old gzip",
    "old decode Mv/s",
    "decode ×old",
  ].join(" | "),
);
for (const bench of BENCHES) {
  const r = await run(bench);
  console.log(
    [
      r.label,
      r.encodings,
      mvals(ROWS, r.encodeMs),
      `${mb(r.storedBytes, r.loadMs)} (${((r.loadMs * 1000) / r.pages).toFixed(1)})`,
      ((r.warmMs * 1000) / r.pages).toFixed(1),
      mvals(ROWS, r.decodeMs),
      mb(r.outputBytes, r.decodeMs),
      ((r.selectMs * 1e6) / Math.max(1, r.selected)).toFixed(0),
      `${((r.selectMs / r.decodeMs) * 100).toFixed(2)}%`,
      `${(r.storedBytes / ROWS).toFixed(2)} (alloc ${(r.allocatedBytes / ROWS).toFixed(2)})`,
      (r.oldRaw / ROWS).toFixed(2),
      (r.oldGzip / ROWS).toFixed(2),
      mvals(ROWS, r.oldDecodeMs),
      `${(r.oldDecodeMs / r.decodeMs).toFixed(1)}×`,
    ].join(" | "),
  );
}
