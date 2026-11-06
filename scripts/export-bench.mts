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
 * The stream is scripts/browser-perf/page/export-stream.ts (also run over OPFS in Chromium by
 * packages/core/browser-tests/next-export-bench.spec.ts). With `--once on` (the default) reads
 * are a one-pass scan (storage/extents.ts `ReadOptions.once`): nothing they read is kept.
 * Reported: rows/s, process CPU, peak RSS and heap against the baseline taken after opening, the
 * protected segment's size, and whether a hot working set survives the export when everything
 * but its size is evicted (the scan must have promoted none of the pages it hit).
 */
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { BlobCache } from "../packages/core/src/next/format/blob-cache.ts";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import { Governor } from "../packages/core/src/next/memory/governor.ts";
import type { ChunkDirectory } from "../packages/core/src/next/page/directory.ts";
import type { ExtentReader } from "../packages/core/src/next/storage/extents.ts";
import { FileStoreSession } from "../packages/core/src/next/storage/file-store.ts";
import { PageCache } from "../packages/core/src/next/storage/page-cache.ts";
import { establishWorkingSet, exportStream, survivors } from "./browser-perf/page/export-stream.ts";
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
const MiB = 1024 * 1024;
/** Row-window readahead across all columns (export-stream.ts `windowBytes`); 0: per column. */
const WINDOW = Number(option("window-mib", "0")) * MiB;

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

// The measured working set (export-stream.ts `establishWorkingSet`): its hot set comes from a
// group the export does not read (the table's last, else the export's last).
const first = groups[0];
const outside = allGroups.at(-1);
const last = outside !== undefined && !groups.includes(outside) ? outside : groups.at(-1);
if (first === undefined || last === undefined) throw new Error("no groups");
const working = await establishWorkingSet(space, first, last, { budget: grant, directories });
const protectedBefore = cache.stats.protectedBytes / MiB;

const PASSES = Number(option("passes", "1"));
const gc = (globalThis as { gc?: () => void }).gc;

async function exportOnce(pass: number): Promise<void> {
  gc?.();
  const baseline = process.memoryUsage();
  let peakRss = baseline.rss;
  let peakHeap = baseline.heapUsed;
  const cpu = process.cpuUsage();
  const started = performance.now();
  const { rows: emitted } = await exportStream(space, groups, {
    budget: grant,
    directories,
    once: ONCE,
    readahead: READAHEAD,
    windowBytes: WINDOW,
    sample: () => {
      const memory = process.memoryUsage();
      peakRss = Math.max(peakRss, memory.rss);
      peakHeap = Math.max(peakHeap, memory.heapUsed);
    },
  });
  const seconds = (performance.now() - started) / 1000;
  const used = process.cpuUsage(cpu);
  console.log(
    `EXPORT_SAMPLE ${JSON.stringify({
      rows: emitted,
      seconds,
      cpuSeconds: (used.user + used.system) / 1e6,
      rowsPerSecond: emitted / seconds,
      rowsPerCpuSecond: emitted / ((used.user + used.system) / 1e6),
      rssDeltaMiB: (peakRss - baseline.rss) / MiB,
    })}`,
  );
  const protectedMiB = cache.stats.protectedBytes / MiB;
  let kept = "";
  if (pass === PASSES) {
    const left = survivors(space, cache, working);
    kept = `hot set kept ${String(left.hot)}/${String(working.hot.length)}, scanned kept ${String(left.scanned)}/${String(working.scanned.length)}`;
  }
  console.log(
    [
      `pass ${String(pass)}: ${String(emitted)} rows × 200 columns`,
      `once ${ONCE ? "on" : "off"}`,
      `${WINDOW > 0 ? `window ${String(WINDOW / MiB)} MiB` : `readahead ${String(READAHEAD / 1024)} KiB`}, ${String(cache.stats.platformReads)} platform reads`,
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
