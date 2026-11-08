/**
 * Worker for the OPFS export bench: build the wide table, then run either the one-pass page
 * stream, scan-only diagnostic, or production Executor/ResultSink and owner-format transport.
 * Historical comparison flags use the explicit scan-adapter diagnostic, excluded from Gate A.
 */
import {
  scanExport,
  type ScanExportStats,
} from "../../../scripts/browser-perf/page/scan-export.js";
import {
  prepareExecutorExport,
  type ExecutorExportStats,
} from "../../../scripts/browser-perf/page/executor-export.js";
import { scanCore, type ScanCoreStats } from "../../../scripts/browser-perf/page/scan-core.js";
import { prepareOwnerExport } from "../../../scripts/browser-perf/page/owner-export.js";
import { ReturnPool } from "../src/next/exec/return-pool.js";
import { TableScan } from "../src/next/exec/table-scan.js";
import { ColumnTarget } from "../src/next/exec/column-target.js";
import { ChunkReader } from "../src/next/page/reader.js";
import { BlobCache } from "../src/next/format/blob-cache.js";
import { Governor, type EvictableCache } from "../src/next/memory/governor.js";
import type { ChunkDirectory } from "../src/next/page/directory.js";
import { GroupWriter } from "../src/next/page/writer.js";
import { MAX_READ_BYTES, type ExtentRef, type ExtentWriter } from "../src/next/storage/extents.js";
import { createOpfsBackend, webLocks } from "../src/next/storage/opfs.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import { countingBudget, jsonReachable } from "../src/next/storage/space-test-support.js";
import { PageCache } from "../src/next/storage/page-cache.js";
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
import { fixtureReviver } from "../../../scripts/browser-perf/page/fixture-json.js";
import { wideSpec } from "../../../scripts/browser-perf/page/wide-data.js";
import { typeOf, vectorOf } from "../../../scripts/browser-perf/page/wide-vectors.js";

const MiB = 1024 * 1024;
const GROUP = 65_536;

export type ExportRequest =
  | { readonly op: "install-fixture"; readonly dir: string; readonly base: string }
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
      readonly recycle?: boolean;
      readonly ownerAPI?: boolean;
      readonly poolTrace?: boolean;
      readonly outputClasses?: boolean;
      readonly dictionarySizing?: "baseline" | "candidate";
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
      readonly mode?: "pages" | "scan" | "scan-core" | "scan-adapter";
      readonly frameCopy?: "baseline" | "candidate";
      readonly scanRange?: "baseline" | "candidate";
      readonly scanShape?: "baseline" | "candidate";
      readonly scanTargetMiB?: 1 | 2 | 3;
      readonly scanIO?: "batch" | "window";
      readonly scanOutput?: "borrowed" | "flat" | "dictionary";
      readonly resultPort?: MessagePort;
      readonly executorVariant?: "baseline" | "candidate";
      readonly executorReference?: string;
    }
  | { readonly op: "series-open"; readonly request: Extract<ExportRequest, { op: "export" }> }
  | { readonly op: "series-pass"; readonly request: Extract<ExportRequest, { op: "export" }> }
  | { readonly op: "series-close" }
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
  readonly shapeComparison?: {
    readonly reference: string;
    readonly variant: string;
    readonly classes: readonly string[];
  };
  readonly readMiB: number;
  readonly mode: "pages" | "scan" | "scan-core" | "scan-adapter";
  readonly executorComparison?: {
    readonly reference: string;
    readonly variant: "baseline" | "candidate" | undefined;
  };
  readonly scanStats?: ScanExportStats;
  readonly executorStats?: ExecutorExportStats;
  readonly scanCoreStats?: ScanCoreStats;
  readonly series?: {
    readonly poolTrace?: import("../../../scripts/browser-perf/page/return-pool-trace.js").PoolTraceSummary;
    readonly requestedBuffers?: number;
    readonly reusedBuffers?: number;
    readonly returnPoolMiB?: number;
    readonly returnHits?: number;
    readonly returnAccepted?: number;
    readonly timeOrigin: number;
    readonly pass: number;
    readonly governorMiB: number;
    readonly poolMiB: number;
    readonly residentMiB: number;
  };
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
      // R25 also registers retained metadata; the hot-set audit must evict page data.
      if (cache instanceof PageCache) registered?.(cache);
      return governor === null
        ? { charge: () => true, release: () => undefined }
        : governor.registerCache(cache);
    },
  });
  return { lease, session, space };
}

/** Acceptance setup only: every cached group was independently generated by the real writer. */
async function installFixture(dir: string, base: string): Promise<BuildResult> {
  const start = performance.now();
  const response = await fetch(base + "manifest.json");
  if (!response.ok) throw new Error("Missing fixture manifest");
  const manifest = (await response.json()) as {
    generatedGroups: number;
    files: Array<{ name: string; bytes: number }>;
  };
  const folder = await directory(dir);
  for (const name of ["data", "log", "root-0", "root-1"]) {
    const expected = manifest.files.find((f) => f.name === name);
    if (!expected) throw new Error("Missing fixture file");
    const source = await fetch(base + name);
    if (!source.ok || !source.body) throw new Error("Fixture fetch failed");
    const handle = await folder.getFileHandle(name, { create: true }),
      out = await handle.createSyncAccessHandle();
    let at = 0;
    try {
      out.truncate(0);
      const reader = source.body.getReader();
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        let offset = 0;
        while (offset < part.value.length) {
          const written = out.write(part.value.subarray(offset), { at });
          if (written <= 0) throw new Error("Short fixture write");
          at += written;
          offset += written;
        }
      }
      if (at !== expected.bytes) throw new Error("Fixture length mismatch");
      out.flush();
    } finally {
      out.close();
    }
  }
  const descriptors = await fetch(base + "groups.json");
  if (!descriptors.ok) throw new Error("Missing fixture descriptors");
  const groups = JSON.parse(await descriptors.text(), fixtureReviver) as ExportGroup[];
  if (groups.length !== manifest.generatedGroups || groups.some((g) => g.chunks.length !== 200))
    throw new Error("Fixture group mismatch");
  return {
    groups,
    generatedGroups: manifest.generatedGroups,
    dataBytes: manifest.files.find((f) => f.name === "data")?.bytes ?? 0,
    ms: performance.now() - start,
  };
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

async function executorReference(request: Extract<ExportRequest, { op: "export" }>) {
  const frozenExecutor = request.executorReference
    ? ((await import(
        /* @vite-ignore */ new URL("../src/next/exec/create.ts?executorBaseline", import.meta.url)
          .href
      )) as typeof import("../src/next/exec/create.js") & {
        EXECUTOR_BASELINE_REVISION?: string;
        EXECUTOR_BASELINE_READS: typeof import("../src/next/exec/scan.js").chunkReads;
      })
    : null;
  if (frozenExecutor && frozenExecutor.EXECUTOR_BASELINE_REVISION !== request.executorReference)
    throw new Error("Wrong frozen executor revision");
  return frozenExecutor;
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
  if (request.mode === "scan-core" && (request.profilePhases || request.passes !== 1))
    throw new Error("Scan-only diagnostic expects one pass and its own timings");
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
    request.mode === "scan-adapter"
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
  const shapeComparison =
    request.scanShape === undefined
      ? undefined
      : {
          reference: "74c79c9b",
          variant: request.scanShape,
          classes: [ChunkReader.name, ColumnTarget.name, TableScan.name],
        };
  if (
    shapeComparison?.classes.some(
      (name, k) =>
        name !==
        (request.scanShape === "candidate" ? "Candidate" : "") +
          (["ChunkReader", "ColumnTarget", "TableScan"][k] ?? ""),
    )
  )
    throw new Error("Shape comparison did not select a consistent module family");
  if ((request.mode === "scan" || request.mode === "scan-adapter") && request.passes !== 1)
    throw new Error("Transport export requires one pass per worker");
  if (
    request.mode === "scan" &&
    (!request.resultPort ||
      request.scanIO !== undefined ||
      request.scanOutput !== undefined ||
      request.scanTargetMiB !== undefined ||
      request.scanShape !== undefined ||
      request.scanRange !== undefined ||
      request.frameCopy !== undefined)
  )
    throw new Error("Production executor export does not accept historical adapter overrides");
  // Hot-set setup is outside timing. The real execution owns its own admission/result grants.
  if (request.mode === "scan") grant.close();
  const frozenExecutor = await executorReference(request);
  const executeExport =
    request.mode === "scan" && request.resultPort
      ? prepareExecutorExport(space, request.groups, {
          governor,
          ...(request.executorVariant === "baseline" && frozenExecutor
            ? {
                executor: frozenExecutor.createExecutor(),
                reads: frozenExecutor.EXECUTOR_BASELINE_READS,
              }
            : {}),
          port:
            request.resultPort ??
            (() => {
              throw new Error("Missing prototype port");
            })(),
          memory: () => ({
            governorHeld: governor.usedBytes,
            idlePool: space.stats.cache.poolBytes,
            cacheResident: space.stats.cache.residentBytes,
            cachePinned: space.stats.cache.pinnedBytes,
          }),
        })
      : null;
  if (request.profile === true) console.profile("export");
  if (phases) performance.mark("minnow-export-start");
  for (let pass = 0; pass < request.passes; pass++) {
    const readsBefore = space.stats.cache.platformReads;
    if (phases) phases.active = true;
    const result =
      executeExport !== null
        ? await executeExport()
        : request.mode === "scan-core"
          ? await scanCore(space, request.groups, {
              budget: grant,
              readWindowBytes: request.scanIO === "batch" ? 0 : (request.windowMiB ?? 64) * MiB,
            })
          : request.mode === "scan-adapter" && request.resultPort && resultGrant
            ? await scanExport(space, request.groups, {
                budget: grant,
                resultBudget: resultGrant,
                ...(request.scanTargetMiB === undefined
                  ? {}
                  : { targetBytes: request.scanTargetMiB * MiB }),
                readWindowBytes: request.scanIO === "batch" ? 0 : (request.windowMiB ?? 64) * MiB,
                transferOutputs:
                  request.scanOutput === "dictionary"
                    ? "dictionary"
                    : request.scanOutput === "flat",
                memory: () => ({
                  queryAllocated: grant.allocated,
                  queryHeld: grant.held,
                  resultHeld: resultGrant.held,
                  cacheCharge: governor.usedBytes - grant.held - resultGrant.held,
                  resultAllocated: resultGrant.allocated,
                  governorHeld: governor.usedBytes,
                  idlePool: space.stats.cache.poolBytes,
                  cacheResident: space.stats.cache.residentBytes,
                  cachePinned: space.stats.cache.pinnedBytes,
                }),
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
      ...(shapeComparison ? { shapeComparison } : {}),
      ...(request.executorReference
        ? {
            executorComparison: {
              reference: request.executorReference,
              variant: request.executorVariant,
            },
          }
        : {}),
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
  if (last !== undefined && working !== null) {
    if (!(evictable instanceof PageCache))
      throw new Error("Hot-set audit requires the actual PageCache");
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

/** Bench-only actor: one worker, OPFS handles, governor and page pool across queries. */
let series: {
  readonly opened: Awaited<ReturnType<typeof open>>;
  readonly governor: Governor;
  readonly returns: ReturnPool;
  readonly owner: ReturnType<typeof prepareOwnerExport> | null;
  pass: number;
  busy: boolean;
} | null = null;
function seriesStats() {
  if (series === null) throw new Error("No open export series");
  return {
    timeOrigin: performance.timeOrigin,
    pass: series.pass,
    governorMiB: series.governor.usedBytes / MiB,
    poolMiB: series.opened.space.stats.cache.poolBytes / MiB,
    residentMiB: series.opened.space.stats.cache.residentBytes / MiB,
  };
}
async function seriesPass(
  request: Extract<ExportRequest, { op: "export" }>,
): Promise<ExportPass[]> {
  const active = series;
  if (!active || active.busy) throw new Error("No idle export series");
  if (request.mode !== "scan" || request.passes !== 1 || (!request.ownerAPI && !request.resultPort))
    throw new Error("Series requires one production scan and a fresh result port");
  if (request.dictionarySizing) {
    const select = (
      globalThis as typeof globalThis & { __minnowDictionarySizing?: (variant: string) => void }
    ).__minnowDictionarySizing;
    if (!select) throw new Error("Dictionary comparison selector missing");
    select(request.dictionarySizing);
  }
  active.busy = true;
  const { space } = active.opened,
    { governor } = active;
  try {
    const frozenExecutor = await executorReference(request);
    const execute = request.ownerAPI
      ? () => {
          if (!active.owner) throw new Error("No production owner export actor");
          const result = active.owner.arm(request.outputClasses);
          scope.postMessage({ id: activeControlId, t: "owner-start" });
          return result;
        }
      : prepareExecutorExport(space, request.groups, {
          governor,
          ...(request.executorVariant !== "baseline" && request.recycle
            ? { returnPool: active.returns }
            : {}),
          ...(request.executorVariant === "baseline" && frozenExecutor
            ? {
                executor: frozenExecutor.createExecutor(),
                reads: frozenExecutor.EXECUTOR_BASELINE_READS,
              }
            : {}),
          port:
            request.resultPort ??
            (() => {
              throw new Error("Missing prototype port");
            })(),
          memory: () => ({
            governorHeld: governor.usedBytes,
            idlePool: space.stats.cache.poolBytes,
            cacheResident: space.stats.cache.residentBytes,
            cachePinned: space.stats.cache.pinnedBytes,
          }),
        });
    const reads = space.stats.cache.platformReads;
    if (request.profile) console.profile("export");
    let result;
    try {
      result = await execute();
    } finally {
      if (request.profile) console.profileEnd("export");
    }
    active.pass++;
    if (request.profile) await new Promise((resolve) => setTimeout(resolve, 150));
    return [
      {
        ...result,
        mode: "scan",
        readMiB: MAX_READ_BYTES / MiB,
        verifying: VERIFY_CACHED_BYTES,
        platformReads: space.stats.cache.platformReads - reads,
        governorPeakMiB: governor.peakBytes / MiB,
        cacheResidentMiB: space.stats.cache.residentBytes / MiB,
        poolMiB: space.stats.cache.poolBytes / MiB,
        protectedBeforeMiB: 0,
        protectedAfterMiB: space.stats.cache.protectedBytes / MiB,
        kept: null,
        series: {
          ...seriesStats(),
          ...(request.ownerAPI && active.owner
            ? active.owner.returnStats()
            : {
                returnPoolMiB: active.returns.evictableBytes / MiB,
                returnHits: active.returns.hits,
                returnAccepted: active.returns.accepted,
              }),
        },
        ...(request.executorReference
          ? {
              executorComparison: {
                reference: request.executorReference,
                variant: request.executorVariant,
              },
            }
          : {}),
      },
    ];
  } finally {
    active.busy = false;
  }
}

async function handle(request: ExportRequest): Promise<unknown> {
  switch (request.op) {
    case "series-open": {
      if (series) throw new Error("Export series already open");
      await executorReference(request.request);
      const governor = new Governor(request.request.capMiB * MiB);
      const opened = await open(request.request.dir, governor);
      series = {
        governor,
        opened,
        returns: new ReturnPool(governor, 4 * MiB),
        owner: request.request.ownerAPI
          ? prepareOwnerExport(opened.space, request.request.groups, {
              governor,
              ...(request.request.poolTrace ? { poolTrace: true } : {}),
              port:
                request.request.resultPort ??
                (() => {
                  throw new Error("Missing owner port");
                })(),
              memory: () => ({
                governorHeld: governor.usedBytes,
                idlePool: opened.space.stats.cache.poolBytes,
                cacheResident: opened.space.stats.cache.residentBytes,
                cachePinned: opened.space.stats.cache.pinnedBytes,
              }),
            })
          : null,
        pass: 0,
        busy: false,
      };
      return seriesStats();
    }
    case "series-pass":
      return seriesPass(request.request);
    case "series-close": {
      const active = series;
      if (!active || active.busy) throw new Error("No idle export series to close");
      series = null;
      active.returns.close();
      await active.owner?.close();
      try {
        await active.opened.space.close();
      } finally {
        await active.opened.lease.release();
      }
      if (active.governor.usedBytes !== 0) throw new Error("Export series leaked a charge");
      return {
        governorMiB: active.governor.usedBytes / MiB,
        poolMiB: active.opened.space.stats.cache.poolBytes / MiB,
        residentMiB: active.opened.space.stats.cache.residentBytes / MiB,
      };
    }
    case "install-fixture":
      return installFixture(request.dir, request.base);
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

let activeControlId = -1;
scope.addEventListener("message", (event: MessageEvent<unknown>) => {
  const { id, request } = event.data as { id: number; request: ExportRequest };
  activeControlId = id;
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
