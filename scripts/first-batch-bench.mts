/**
 * Benchmarks the first batch of `SELECT *` (and `SELECT * ORDER BY id`, the same rows: groups
 * are written in id order) from the page format: Gate B's "first page ≤ 20 ms" at W1M (200
 * columns) and Gate C's "first page ≤ 32 ms" at W500 (500 columns).
 *
 *   npx vite-node scripts/first-batch-bench.mts [--repeats 15]
 *
 * One group of the wide table (scripts/browser-perf/page/wide-data.ts) is written; the first
 * batch (2,048 rows) of every column is then read: open the column's chunk (its directory),
 * load only the pages the batch needs (with the dictionary for dictionary pages), decode the
 * rows (dictionary pages as codes, the dictionary as one immutable vector, as a scan does).
 * Nothing is done for the rest of the group. Reported: total ms, and the open / load / decode
 * share, warm (pages, directories and descriptors cached) and cold (no byte cache, no
 * directory cache), with descriptors on and off. Medians of `repeats` runs after a warm-up.
 */
import { performance } from "node:perf_hooks";
import { BlobCache } from "../packages/core/src/next/format/blob-cache.ts";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import type { ChunkDirectory } from "../packages/core/src/next/page/directory.ts";
import { MemoryExtentSpace, UNLIMITED } from "../packages/core/src/next/page/memory-extents.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import { targetFor } from "../packages/core/src/next/page/test-support.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";
import type { ExtentReader } from "../packages/core/src/next/storage/extents.ts";
import { wideSpec } from "./browser-perf/page/wide-data.ts";
import { typeOf, vectorOf } from "./wide-table.mts";

const args = process.argv.slice(2);
const index = args.indexOf("--repeats");
const REPEATS = index >= 0 ? Number(args[index + 1]) : 15;
const GROUP = 65_536;
const BATCH = 2048;

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

async function table(columns: 200 | 500, rows: number) {
  const spec = wideSpec(columns);
  const space = new MemoryExtentSpace();
  const writer = new GroupWriter({
    columns: spec.map((column, k) => ({ columnId: k + 1, type: typeOf(column) })),
    writer: space.beginWrite("first batch"),
  });
  await writer.append(
    spec.map((column) => vectorOf(column, typeOf(column), 0, GROUP, rows)),
    GROUP,
  );
  const written = await writer.finish();
  return { space, chunks: written.columns };
}

interface Stages {
  open: number;
  load: number;
  decode: number;
}

/** Batch buffers reused across batches, as the executor reuses its own (one set per column). */
const targets = new Map<ColumnChunk, ReturnType<typeof targetFor>>();
const codeBuffers = { codes: new Int32Array(BATCH), validity: new Uint32Array(BATCH / 32) };

function batchTarget(
  chunk: ColumnChunk,
  reader: ChunkReader,
  bytes: number,
): ReturnType<typeof targetFor> {
  const kept = targets.get(chunk);
  if (kept !== undefined && (kept.data.layout !== "varlen" || kept.data.bytes.length >= bytes))
    return kept;
  const target = targetFor(reader, BATCH, Math.max(bytes, 64 * 1024));
  targets.set(chunk, target);
  return target;
}

async function firstBatch(
  view: ExtentReader,
  chunks: readonly ColumnChunk[],
  directories: BlobCache<ChunkDirectory> | undefined,
): Promise<Stages> {
  const stages = { open: 0, load: 0, decode: 0 };
  const options =
    directories === undefined ? { budget: UNLIMITED } : { budget: UNLIMITED, directories };
  for (const chunk of chunks) {
    let t = performance.now();
    const reader = await ChunkReader.open(view, chunk, options);
    stages.open += performance.now() - t;
    t = performance.now();
    const [first, end] = reader.pagesInRange(0, BATCH);
    await reader.load(Array.from({ length: end - first }, (_, k) => first + k));
    stages.load += performance.now() - t;
    t = performance.now();
    if (reader.isDictionaryRange(0, BATCH)) {
      reader.decodeCodes(0, BATCH, codeBuffers.codes, codeBuffers.validity);
      void reader.dictionary;
    } else {
      const target = batchTarget(chunk, reader, reader.varlenBytes(0, BATCH));
      reader.decodeRange(0, BATCH, target, 0);
    }
    stages.decode += performance.now() - t;
    reader.close();
  }
  return stages;
}

const without = (view: MemoryExtentSpace): ExtentReader => ({
  read: (refs, options) => view.read(refs, options),
  peek: (ref) => view.peek(ref),
});

console.log(`first batch of ${String(BATCH)} rows, medians of ${String(REPEATS)}`);
console.log(
  "table | descriptors | warm ms (open / load / decode) | cold ms (open / load / decode)",
);
for (const [label, columns, rows] of [
  ["W1M (200 columns)", 200, 1_048_576],
  ["W500 (500 columns)", 500, 250_000],
] as const) {
  const { space, chunks } = await table(columns, rows);
  for (const descriptors of [false, true]) {
    const directories = new BlobCache<ChunkDirectory>(() => ({
      charge: () => true,
      release: () => undefined,
    }));
    const cache = space.view({ cache: true });
    const warmView = descriptors ? cache : without(cache);
    await firstBatch(warmView, chunks, directories);
    const warm: Stages[] = [];
    for (let r = 0; r < REPEATS; r++) warm.push(await firstBatch(warmView, chunks, directories));
    const cold: Stages[] = [];
    for (let r = 0; r < REPEATS; r++) {
      const fresh = space.view({ cache: false });
      cold.push(await firstBatch(descriptors ? fresh : without(fresh), chunks, undefined));
    }
    const show = (runs: Stages[]): string => {
      const total = median(runs.map((run) => run.open + run.load + run.decode));
      return `${total.toFixed(1)} (${median(runs.map((run) => run.open)).toFixed(1)} / ${median(runs.map((run) => run.load)).toFixed(1)} / ${median(runs.map((run) => run.decode)).toFixed(1)})`;
    };
    console.log([label, descriptors ? "on" : "off", show(warm), show(cold)].join(" | "));
  }
}
