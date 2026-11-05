/**
 * Benchmarks out-of-line large values (packages/core/src/next/page/large-value.ts, ruling R15.4):
 * values over 64 KiB written as ≤ 1 MiB parts through the chunk's extent builder and read back
 * through the in-memory data space, cold (every part read and xxHash32-verified).
 *
 *   npx vite-node scripts/large-value-bench.mts [--repeats 5]
 *
 * Cases: 16 values of 4 MiB among 4,096 small rows, and one value of 64 MiB. For each:
 * - write: the group writer from a vector (MB/s), and its peak working memory;
 * - load + decode: pages, then `loadValues`, then `decodeRange` into one target (MB/s), and the
 *   most the reader's budget held beyond the target;
 * - stream: `valueParts` over every out-of-line value (MB/s), and the most its budget held.
 * Each figure is the median of `repeats` runs after one warm-up. The machine is shared: compare
 * ratios, not absolute numbers.
 */
import { performance } from "node:perf_hooks";
import type { ByteBudget } from "../packages/core/src/next/storage/extents.ts";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import { MemoryExtentSpace } from "../packages/core/src/next/page/memory-extents.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import { targetFor, vectorOf } from "../packages/core/src/next/page/test-support.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";

const args = process.argv.slice(2);
const index = args.indexOf("--repeats");
const REPEATS = index >= 0 ? Number(args[index + 1]) : 5;
const MiB = 1024 * 1024;

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

async function timed(run: () => Promise<void>): Promise<number> {
  await run();
  const samples: number[] = [];
  for (let k = 0; k < REPEATS; k++) {
    const start = performance.now();
    await run();
    samples.push(performance.now() - start);
  }
  return median(samples);
}

function value(length: number, seed: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let k = 0; k < length; k++) out[k] = 97 + ((k * 7 + seed) % 26);
  return out;
}

function peakBudget(): ByteBudget & { held: number; peak: number } {
  const budget = {
    held: 0,
    peak: 0,
    allocate(bytes: number): boolean {
      budget.held += bytes;
      budget.peak = Math.max(budget.peak, budget.held);
      return true;
    },
    free(bytes: number): void {
      budget.held -= bytes;
    },
  };
  return budget;
}

const mbps = (bytes: number, ms: number): string => (bytes / 1e6 / (ms / 1000)).toFixed(0);
const mib = (bytes: number): string => (bytes / MiB).toFixed(2);

async function bench(name: string, values: Uint8Array[]): Promise<string> {
  const large = values.reduce((sum, each) => sum + (each.length > 65_536 ? each.length : 0), 0);
  const total = values.reduce((sum, each) => sum + each.length, 0);
  const vector = vectorOf({ kind: "bytes" }, values);
  let space = new MemoryExtentSpace();
  let chunk: ColumnChunk | null = null;
  let writerPeak = 0;
  const writeMs = await timed(async () => {
    space = new MemoryExtentSpace();
    const group = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "bytes" } }],
      writer: space.beginWrite("bench"),
    });
    await group.append([vector], values.length);
    chunk = (await group.finish()).columns[0] ?? null;
    writerPeak = group.peakWorkingBytes;
  });
  if (chunk === null) throw new Error("nothing written");
  const written: ColumnChunk = chunk;
  const rows = values.length;
  let readerExtra = 0;
  const target = { current: null as ReturnType<typeof targetFor> | null };
  const readMs = await timed(async () => {
    const budget = peakBudget();
    const reader = await ChunkReader.open(space.view({ cache: false }), written, { budget });
    await reader.load(Array.from({ length: reader.directory.pages.length }, (_, page) => page));
    await reader.loadValues(0, rows);
    target.current ??= targetFor(reader, rows, reader.varlenBytes(0, rows));
    reader.decodeRange(0, rows, target.current, 0);
    reader.close();
    readerExtra = budget.peak;
  });
  let streamPeak = 0;
  const streamMs = await timed(async () => {
    const budget = peakBudget();
    const reader = await ChunkReader.open(space.view({ cache: false }), written, { budget });
    await reader.load(Array.from({ length: reader.directory.pages.length }, (_, page) => page));
    const pages = budget.held;
    for (const row of reader.largeRows(0, rows))
      for await (const part of reader.valueParts(row)) void part;
    streamPeak = budget.peak - pages;
    reader.close();
  });
  return [
    name,
    `${mbps(total, writeMs)} MB/s (peak working ${mib(writerPeak)} MiB)`,
    `${mbps(total, readMs)} MB/s (budget peak ${mib(readerExtra)} MiB)`,
    `${mbps(large, streamMs)} MB/s (budget peak ${mib(streamPeak)} MiB beyond pages)`,
  ].join(" | ");
}

const mixed = Array.from({ length: 4096 + 16 }, (_, k) =>
  k % 257 === 0 && k / 257 < 16 ? value(4 * MiB, k) : value(24, k % 50),
);
const rows = [
  await bench("16 × 4 MiB among 4,096 small rows (64 MiB)", mixed),
  await bench("one 64 MiB value", [value(64 * MiB, 3)]),
];
console.log(`median of ${String(REPEATS)}`);
console.log("case | write | load + decode | stream");
for (const row of rows) console.log(row);
