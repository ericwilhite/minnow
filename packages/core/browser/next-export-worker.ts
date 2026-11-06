/**
 * Worker for the OPFS export bench: build the wide table, then run either the one-pass page
 * stream or the real TableScan and owner-format transport. Scan frame assembly uses the
 * labelled bench stand-in in scripts/browser-perf/page/scan-export.ts, not a production sink.
 */
import {
  scanExport,
  type ScanExportStats,
} from "../../../scripts/browser-perf/page/scan-export.js";
import { BlobCache } from "../src/next/format/blob-cache.js";
import { Governor, type EvictableCache } from "../src/next/memory/governor.js";
import type { ChunkDirectory } from "../src/next/page/directory.js";
import { GroupWriter } from "../src/next/page/writer.js";
import { MAX_READ_BYTES, type ExtentRef, type ExtentWriter } from "../src/next/storage/extents.js";
import { createOpfsBackend, webLocks } from "../src/next/storage/opfs.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import { countingBudget, jsonReachable } from "../src/next/storage/space-test-support.js";
import { VERIFY_CACHED_BYTES } from "../src/next/storage/verify-cached.js";
import {
  establishWorkingSet,
  exportPhases,
  EXPORT_BATCH,
  type ExportPhases,
  exportStream,
  rebaseExtents,
  survivors,
  type ExportGroup,
} from "../../../scripts/browser-perf/page/export-stream.js";
import { wideSpec } from "../../../scripts/browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "../../../scripts/browser-perf/page/wide-vectors.js";

const MiB = 1024 * 1024;
const GROUP = 65_536;

export type ExportRequest =
  | {
      readonly op: "build";
      readonly dir: string;
      readonly rows: number;
      /** Groups generated; the rest are byte copies of them at new addresses. */
      readonly distinct: number;
    }
  | {
      readonly op: "export";
      readonly dir: string;
      readonly groups: readonly ExportGroup[];
      /** A group the export does not read: the measured hot set comes from it. */
      readonly outside: ExportGroup;
      readonly once: boolean;
      readonly readaheadKiB: number;
      /** Row-window readahead across all columns (export-stream.ts `windowBytes`); 0: per column. */
      readonly windowMiB?: number;
      readonly capMiB: number;
      readonly passes: number;
      /** Brackets the passes with console.profile / profileEnd (the spec's CDP profiler). */
      readonly profile?: boolean;
      /** Empty engine cache, exclusive export wall phases; excludes open/hot-set setup. */
      readonly profilePhases?: boolean;
      readonly auditWorkingSet?: boolean;
      /** Bench-only comparison: both loop modules are imported before timing. */
      readonly baselineStream?: boolean;
      readonly readMiB?: 4 | 8;
      readonly groupDepth?: 1 | 2;
      readonly mode?: "pages" | "scan";
      readonly frameCopy?: "baseline" | "candidate";
      readonly scanRange?: "baseline" | "candidate";
      readonly resultPort?: MessagePort;
    }
  | { readonly op: "remove"; readonly dir: string };

export interface BuildResult {
  readonly groups: ExportGroup[];
  readonly generatedGroups: number;
  readonly dataBytes: number;
  readonly ms: number;
}

export interface ExportPass {
  /** Whether the cached-bytes guard ran (it re-hashes every release: never time with it on). */
  readonly verifying: boolean;
  readonly rows: number;
  readonly ms: number;
  readonly loadMs: number;
  readonly openMs: number;
  readonly loads: number;
  readonly platformReads: number;
  readonly governorPeakMiB: number;
  readonly cacheResidentMiB: number;
  /** Idle pooled one-pass buffers at the end of the pass (charged to the cache). */
  readonly poolMiB: number;
  /** The page cache's protected segment before the export and after this pass. */
  readonly protectedBeforeMiB: number;
  readonly protectedAfterMiB: number;
  /** After the last pass, everything but the hot set's size evicted: what survived (else null). */
  readonly kept: { hot: string; scanned: string } | null;
  readonly phases?: ExportPhases;
  readonly baselineCommit?: string;
  readonly readMiB: number;
  readonly mode: "pages" | "scan";
  readonly scanStats?: ScanExportStats;
  readonly afterWorkerClose?: { governorMiB: number; poolMiB: number; residentMiB: number };
}

const scope = self as unknown as {
  postMessage(message: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
};

async function directory(name: string): Promise<FileSystemDirectoryHandle> {
  const root = await navigator.storage.getDirectory();
  const base = await root.getDirectoryHandle("next-export", { create: true });
  return await base.getDirectoryHandle(name, { create: true });
}

const locks = webLocks(navigator.locks);

async function open(
  name: string,
  governor: Governor | null,
  registered?: (cache: EvictableCache) => void,
  phases?: ExportPhases,
) {
  const backend = createOpfsBackend({
    directory: await directory(name),
    name: `next-export:${name}`,
    locks,
  });
  const lease = await backend.acquire({ timeoutMs: 10_000 });
  const session = await backend.open(lease);
  if (phases) {
    const read = session.readDataInto.bind(session);
    let pending = 0;
    let synchronous = 0;
    // This backend is FileStoreSession: its async method performs synchronous OPFS reads
    // before returning. Time that synchronous call, not overlapping promise continuations.
    session.readDataInto = (offset, target) => {
      if (!phases.active) return read(offset, target);
      const start = performance.now();
      phases.maxPendingReads = Math.max(phases.maxPendingReads, ++pending);
      phases.maxSyncReads = Math.max(phases.maxSyncReads, ++synchronous);
      try {
        return read(offset, target).finally(() => {
          pending--;
        });
      } finally {
        synchronous--;
        const ms = performance.now() - start;
        phases.readMs += ms;
        phases.readCalls++;
        phases.readEvents.push({
          offset,
          bytes: target.length,
          backingBytes: target.buffer.byteLength,
          ms,
        });
      }
    };
  }
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget: countingBudget(Number.POSITIVE_INFINITY),
    registerCache: (cache) => {
      registered?.(cache);
      return governor === null
        ? { charge: () => true, release: () => undefined }
        : governor.registerCache(cache);
    },
  });
  return { lease, session, space };
}

async function build(dir: string, rows: number, distinct: number): Promise<BuildResult> {
  const started = performance.now();
  const { lease, session, space } = await open(dir, null);
  const spec = wideSpec(200);
  const types = spec.map((column) => typeOf(column));
  const generated: Array<ExportGroup & { readonly extents: readonly ExtentRef[] }> = [];
  const groups: ExportGroup[] = [];
  const extents: ExtentRef[] = [];
  // Each group is published (reachable from the root), as a flush would leave it.
  const publish = (writer: ExtentWriter) =>
    space.publish(new TextEncoder().encode(JSON.stringify({ extents })), {
      writers: [writer],
      retired: [],
    });
  for (let first = 0, index = 0; first < rows; first += GROUP, index++) {
    const count = Math.min(GROUP, rows - first);
    const writer = space.beginWrite(`group ${String(first)}`);
    const source = generated[index % Math.max(1, distinct)];
    if (index >= distinct && source?.rows === count) {
      // A byte copy of a generated group's extents at new addresses.
      const moved = new Map<number, ExtentRef>();
      for (const extent of source.extents) {
        const bytes = new Uint8Array(extent.length);
        await session.readDataInto(extent.offset, bytes);
        const parts: Uint8Array[] = [];
        for (let at = 0; at < bytes.length; at += MiB)
          parts.push(bytes.subarray(at, Math.min(bytes.length, at + MiB)));
        const written = await writer.write(parts);
        moved.set(extent.offset, written.extent);
        extents.push(written.extent);
      }
      groups.push({ rows: count, chunks: rebaseExtents(source.chunks, moved) });
    } else {
      const group = new GroupWriter({
        columns: spec.map((_, k) => ({ columnId: k + 1, type: types[k] ?? { kind: "int64" } })),
        writer,
      });
      await group.append(
        spec.map((column, k) =>
          vectorOf(column, types[k] ?? { kind: "int64" }, first, count, rows),
        ),
        count,
      );
      const written = await group.finish();
      extents.push(...written.extents);
      generated.push({ rows: count, chunks: written.columns, extents: written.extents });
      groups.push({ rows: count, chunks: written.columns });
    }
    await publish(writer);
  }
  const dataBytes = space.stats.dataBytes;
  await space.close();
  await lease.release();
  return { groups, generatedGroups: generated.length, dataBytes, ms: performance.now() - started };
}

async function runExport(request: Extract<ExportRequest, { op: "export" }>): Promise<ExportPass[]> {
  if (request.readMiB !== undefined && request.readMiB * MiB !== MAX_READ_BYTES)
    throw new Error("Read candidate compiler is not enabled");
  if (request.profilePhases && request.passes !== 1)
    throw new Error("Phase profiling expects one pass");
  if (
    request.mode === "scan" &&
    (!request.resultPort || request.profilePhases || request.passes !== 1)
  )
    throw new Error("Scan bench needs a result port and its own timings");
  let stream = exportStream;
  let baselineCommit: string | undefined;
  if (request.baselineStream !== undefined) {
    const url = new URL(
      "../../../scripts/browser-perf/page/export-stream.ts?exportBaseline",
      import.meta.url,
    );
    const baseline = (await import(
      /* @vite-ignore */ url.href
    )) as typeof import("../../../scripts/browser-perf/page/export-stream.js") & {
      EXPORT_BASELINE_REVISION?: string;
    };
    baselineCommit = baseline.EXPORT_BASELINE_REVISION;
    if (!baselineCommit) throw new Error("Export baseline compiler plugin is not enabled");
    if (request.baselineStream) stream = baseline.exportStream;
  }
  const governor = new Governor(request.capMiB * MiB);
  let cache: EvictableCache | null = null;
  const phases = request.profilePhases ? exportPhases() : undefined;
  Object.assign(globalThis, { __minnowExportPhases: phases });
  const { lease, space } = await open(
    request.dir,
    governor,
    (registered) => {
      cache = registered;
    },
    phases,
  );
  const directories = new BlobCache<ChunkDirectory>((evictable) =>
    governor.registerCache(evictable),
  );
  const grant = await governor.admit({ label: "export", minimumBytes: 32 * MiB, spillable: true });
  const resultGrant =
    request.mode === "scan"
      ? await governor.admit({ label: "bench results", minimumBytes: 4 * MiB, spillable: true })
      : null;
  const first = request.groups[0];
  if (first === undefined) throw new Error("no groups");
  const working =
    request.profilePhases || request.auditWorkingSet === false
      ? null
      : await establishWorkingSet(space, first, request.outside, {
          budget: grant,
          directories,
        });
  const protectedBefore = space.stats.cache.protectedBytes / MiB;
  const passes: ExportPass[] = [];
  if (request.profile === true) console.profile("export");
  if (phases) performance.mark("minnow-export-start");
  for (let pass = 0; pass < request.passes; pass++) {
    const readsBefore = space.stats.cache.platformReads;
    if (phases) phases.active = true;
    const result =
      request.mode === "scan" && request.resultPort && resultGrant
        ? await scanExport(space, request.groups, {
            budget: grant,
            resultBudget: resultGrant,
            port: request.resultPort,
          })
        : await stream(space, request.groups, {
            budget: grant,
            directories,
            once: request.once,
            readahead: request.readaheadKiB * 1024,
            windowBytes: (request.windowMiB ?? 0) * MiB,
            ...(phases ? { phases } : {}),
            ...(request.groupDepth === undefined ? {} : { groupDepth: request.groupDepth }),
            ...(phases
              ? {
                  sample: (rows: number) =>
                    phases.memory.push({
                      rows,
                      pool: space.stats.cache.poolBytes,
                      resident: space.stats.cache.residentBytes,
                      pinned: space.stats.cache.pinnedBytes,
                      governor: governor.usedBytes,
                    }),
                }
              : {}),
          });
    if (phases) {
      phases.active = false;
      const expected = request.groups.reduce(
        (sum, group) => sum + Math.ceil(group.rows / EXPORT_BATCH) * group.chunks.length,
        0,
      );
      if (phases.decodeCalls !== expected)
        throw new Error("Unaccounted batch decode in phase profile");
      if (phases.readCalls !== space.stats.cache.platformReads - readsBefore)
        throw new Error("Unaccounted platform read in phase profile");
    }
    passes.push({
      ...(phases ? { phases: { ...phases } } : {}),
      ...(baselineCommit ? { baselineCommit } : {}),
      mode: request.mode ?? "pages",
      readMiB: MAX_READ_BYTES / MiB,
      verifying: VERIFY_CACHED_BYTES,
      ...result,
      platformReads: space.stats.cache.platformReads - readsBefore,
      governorPeakMiB: governor.peakBytes / MiB,
      cacheResidentMiB: space.stats.cache.residentBytes / MiB,
      poolMiB: space.stats.cache.poolBytes / MiB,
      protectedBeforeMiB: protectedBefore,
      protectedAfterMiB: space.stats.cache.protectedBytes / MiB,
      kept: null,
    });
  }
  if (phases) performance.mark("minnow-export-end");
  if (request.profile === true) {
    console.profileEnd("export");
    // Let the profile reach the inspector before the page terminates this worker.
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  const evictable = cache as EvictableCache | null;
  const last = passes.at(-1);
  if (evictable !== null && last !== undefined && working !== null) {
    const left = survivors(space, evictable, working);
    passes[passes.length - 1] = {
      ...last,
      kept: {
        hot: `${String(left.hot)}/${String(working.hot.length)}`,
        scanned: `${String(left.scanned)}/${String(working.scanned.length)}`,
      },
    };
  }
  resultGrant?.close();
  grant.close();
  directories.clear();
  await space.close();
  const afterWorkerClose = {
    governorMiB: governor.usedBytes / MiB,
    poolMiB: space.stats.cache.poolBytes / MiB,
    residentMiB: space.stats.cache.residentBytes / MiB,
  };
  if (
    afterWorkerClose.governorMiB !== 0 ||
    afterWorkerClose.poolMiB !== 0 ||
    afterWorkerClose.residentMiB !== 0
  )
    throw new Error("Bench memory charge survived worker close");
  const final = passes.at(-1);
  if (final) passes[passes.length - 1] = { ...final, afterWorkerClose };
  await lease.release();
  return passes;
}

async function handle(request: ExportRequest): Promise<unknown> {
  switch (request.op) {
    case "build":
      return build(request.dir, request.rows, request.distinct);
    case "export":
      return runExport(request);
    case "remove": {
      const root = await navigator.storage.getDirectory();
      const base = await root.getDirectoryHandle("next-export", { create: true });
      await base.removeEntry(request.dir, { recursive: true }).catch(() => undefined);
      return null;
    }
  }
}

scope.addEventListener("message", (event: MessageEvent<unknown>) => {
  const { id, request } = event.data as { id: number; request: ExportRequest };
  handle(request).then(
    (result) => {
      scope.postMessage({ id, result });
    },
    (error: unknown) => {
      scope.postMessage({
        id,
        error: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
      });
    },
  );
});
scope.postMessage("next-export-ready");
