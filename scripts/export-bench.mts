/**
 * Gate A's streaming export, page-format half: `SELECT *` over every row of the wide table
 * (200 columns) as a sequential decode stream, from files on disk (scripts/export-bench-build.mts)
 * through the real file store, page cache and memory governor, in this fresh process.
 *
 *   node --expose-gc node_modules/vite-node/dist/cli.mjs scripts/export-bench.mts \
 *     --dir ~/.cache/minnow-export [--groups 16] [--once on] [--passes 1] [--cap-mib 512]
 *
 * `--once off --cap-mib 4096 --passes 2`: the second pass reads only from the page cache, which
 * isolates the page-format half (decode, parse, release) from the file reads.
 *
 * Per group, each column's chunk is opened (directory only); the stream then emits batches of
 * 2,048 rows: for each column, load the pages the batch needs, release those behind it, decode
 * the batch into that column's reused buffer (dictionary pages as codes). Nothing is materialized
 * for a whole group, and with `--once on` (the default) reads are a one-pass scan
 * (storage/extents.ts `ReadOptions.once`): nothing they read is kept in the page cache.
 * Reported: rows/s, process CPU, peak RSS and heap against the baseline taken after opening, the
 * protected segment's size, and whether a hot working set survives the export when everything
 * but its size is evicted (the scan must have promoted none of the pages it hit).
 */
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { BlobCache } from "../packages/core/src/next/format/blob-cache.ts";
import type { ColumnChunk, PageEntry } from "../packages/core/src/next/format/manifest.ts";
import { Governor } from "../packages/core/src/next/memory/governor.ts";
import type { ChunkDirectory } from "../packages/core/src/next/page/directory.ts";
import { ChunkReader } from "../packages/core/src/next/page/reader.ts";
import { targetFor } from "../packages/core/src/next/page/test-support.ts";
import type { BlobRef, ExtentReader } from "../packages/core/src/next/storage/extents.ts";
import { FileStoreSession } from "../packages/core/src/next/storage/file-store.ts";
import { ENTRY_OVERHEAD, PageCache } from "../packages/core/src/next/storage/page-cache.ts";
import type { FlatVector } from "../packages/core/src/next/vector/vector.ts";
import { jsonReviver, openFileSet } from "./node-sync-file.mts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};
const DIR = option("dir", join(homedir(), ".cache", "minnow-export"));
const ONCE = option("once", "on") === "on";
const CAP_MIB = Number(option("cap-mib", "512"));
/** Stored bytes of pages a column loads ahead of its batch (`ChunkReader.pagesAhead`). */
const READAHEAD = Number(option("readahead-kib", "256")) * 1024;
const BATCH = 2048;
const MiB = 1024 * 1024;

const allGroups = JSON.parse(readFileSync(join(DIR, "groups.json"), "utf8"), jsonReviver) as Array<{
  rows: number;
  chunks: ColumnChunk[];
}>;
/** `--skip`: start at another copy of the table (distinct file bytes, not in the OS cache yet). */
const SKIP = Number(option("skip", "0"));
const groups = allGroups.slice(SKIP, SKIP + Number(option("groups", String(allGroups.length))));
const files = openFileSet(DIR, false);
const session = new FileStoreSession(files, 1, () => undefined);
const governor = new Governor(CAP_MIB * MiB);
const cache = new PageCache(session);
cache.attach(governor.registerCache(cache));
const space: ExtentReader = {
  read: (refs, options) => cache.read(refs, options),
  peek: (ref, options) => cache.peek(ref, options),
  parsed: (ref) => cache.parsed(ref),
  attach: (ref, parsed, bytes, of) => cache.attachParsed(ref, parsed, bytes, of),
};
const directories = new BlobCache<ChunkDirectory>((evictable) => governor.registerCache(evictable));
const grant = await governor.admit({ label: "export", minimumBytes: 32 * MiB, spillable: true });

const pageRef = (chunk: ColumnChunk, entry: PageEntry): BlobRef => ({
  extent: chunk.data.extent,
  offset: chunk.data.offset + entry.offset,
  length: entry.length,
  checksum: entry.checksum,
});

// The measured working set (review B2-p2g #2): a hot set, the first page of 30 columns of a group
// the export does not read (the table's last, else the export's last), used twice (protected);
// and the pages of those columns in the export's first group, used once (probation), which the
// export then hits. A scan that promoted its hits would grow protected past the hot set and push
// it out; after the last pass everything but the hot set's size is evicted, so only an
// unpromoted hot set survives whole.
const first = groups[0];
const outside = allGroups.at(-1);
const last = outside !== undefined && !groups.includes(outside) ? outside : groups.at(-1);
if (first === undefined || last === undefined) throw new Error("no groups");
async function pagesOf(chunks: readonly ColumnChunk[], all: boolean): Promise<BlobRef[][]> {
  const out: BlobRef[][] = [];
  for (const chunk of chunks) {
    const reader = await ChunkReader.open(space, chunk, { budget: grant, directories });
    const entries = all ? reader.directory.pages : reader.directory.pages.slice(0, 1);
    reader.close();
    out.push(entries.map((entry) => pageRef(chunk, entry)));
  }
  return out;
}
const hot = (await pagesOf(last.chunks.slice(0, 30), false)).flat();
const scanned = await pagesOf(first.chunks.slice(0, 30), true);
for (let use = 0; use < 2; use++) (await cache.read(hot, { budget: grant })).release();
for (const column of scanned)
  for (let at = 0; at < column.length; at += 32)
    (await cache.read(column.slice(at, at + 32), { budget: grant })).release();
const hotBytes = hot.reduce((sum, ref) => sum + ref.length + ENTRY_OVERHEAD, 0);
const protectedBefore = cache.stats.protectedBytes / MiB;

const PASSES = Number(option("passes", "1"));
const gc = (globalThis as { gc?: () => void }).gc;
/** One reused batch buffer per column (the executor reuses its own). */
const buffers = new Map<
  number,
  { target: FlatVector | null; codes: Int32Array; validity: Uint32Array }
>();

async function exportOnce(pass: number): Promise<void> {
  gc?.();
  const baseline = process.memoryUsage();
  let peakRss = baseline.rss;
  let peakHeap = baseline.heapUsed;
  const cpu = process.cpuUsage();
  const started = performance.now();

  let emitted = 0;
  let batches = 0;
  for (const group of groups) {
    const readers: ChunkReader[] = [];
    for (const chunk of group.chunks)
      readers.push(
        await ChunkReader.open(space, chunk, { budget: grant, directories, once: ONCE }),
      );
    const loadedFrom = readers.map(() => 0);
    for (let from = 0; from < group.rows; from += BATCH) {
      const to = Math.min(group.rows, from + BATCH);
      for (const [column, reader] of readers.entries()) {
        const [firstPage, endPage] = reader.pagesInRange(from, to);
        // Pages behind the batch were emitted: let them go.
        const behind = loadedFrom[column] ?? 0;
        if (firstPage > behind) {
          reader.release(Array.from({ length: firstPage - behind }, (_, k) => behind + k));
          loadedFrom[column] = firstPage;
        }
        if (!reader.isLoaded(Math.max(firstPage, endPage - 1))) {
          // Read ahead: the batch's pages and the next ones within READAHEAD bytes, in one call.
          const missing = reader
            .pagesAhead(from, to, READAHEAD)
            .filter((page) => !reader.isLoaded(page));
          if (missing.length > 0) await reader.load(missing);
        }
        let buffer = buffers.get(column);
        if (buffer === undefined) {
          buffer = {
            target: null,
            codes: new Int32Array(BATCH),
            validity: new Uint32Array(BATCH / 32),
          };
          buffers.set(column, buffer);
        }
        if (reader.isDictionaryRange(from, to))
          reader.decodeCodes(from, to, buffer.codes, buffer.validity);
        else {
          const bytes = reader.varlenBytes(from, to);
          const target = buffer.target;
          if (
            target === null ||
            (target.data.layout === "varlen" && target.data.bytes.length < bytes)
          )
            buffer.target = targetFor(reader, BATCH, Math.max(bytes, 256 * 1024));
          reader.decodeRange(from, to, defined(buffer.target), 0);
        }
      }
      emitted += to - from;
      if ((++batches & 7) === 0) {
        const memory = process.memoryUsage();
        peakRss = Math.max(peakRss, memory.rss);
        peakHeap = Math.max(peakHeap, memory.heapUsed);
      }
    }
    for (const reader of readers) reader.close();
  }
  const seconds = (performance.now() - started) / 1000;
  const used = process.cpuUsage(cpu);
  const protectedMiB = cache.stats.protectedBytes / MiB;
  let kept = "";
  if (pass === PASSES) {
    cache.evict(cache.evictableBytes - hotBytes);
    const survived = hot.filter((ref) => cache.peek(ref, { once: true }) !== null).length;
    const left = scanned.flat().filter((ref) => cache.peek(ref, { once: true }) !== null).length;
    kept = `hot set kept ${String(survived)}/${String(hot.length)}, scanned kept ${String(left)}/${String(scanned.flat().length)}`;
  }
  console.log(
    [
      `pass ${String(pass)}: ${String(emitted)} rows × 200 columns`,
      `once ${ONCE ? "on" : "off"}`,
      `readahead ${String(READAHEAD / 1024)} KiB, ${String(cache.stats.platformReads)} platform reads`,
      `${(emitted / seconds / 1000).toFixed(0)}k rows/s (${seconds.toFixed(1)} s)`,
      `CPU ${((used.user + used.system) / 1e6).toFixed(1)} s`,
      `peak RSS +${((peakRss - baseline.rss) / MiB).toFixed(0)} MiB (baseline ${(baseline.rss / MiB).toFixed(0)} MiB)`,
      `peak heap +${((peakHeap - baseline.heapUsed) / MiB).toFixed(0)} MiB`,
      `governor peak ${(governor.peakBytes / MiB).toFixed(0)} MiB`,
      `cache resident ${(cache.stats.residentBytes / MiB).toFixed(1)} MiB (protected ${protectedBefore.toFixed(1)} → ${protectedMiB.toFixed(1)} MiB)`,
      ...(kept === "" ? [] : [kept]),
    ].join(" | "),
  );
}

for (let pass = 1; pass <= PASSES; pass++) await exportOnce(pass);

function defined<T>(value: T | null | undefined): T {
  if (value === null || value === undefined) throw new Error("expected a value");
  return value;
}
