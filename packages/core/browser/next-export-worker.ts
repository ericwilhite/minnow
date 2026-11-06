/**
 * Gate A's streaming export on real OPFS (browser-tests/next-export-bench.spec.ts): the wide
 * table (200 columns, scripts/browser-perf/page/wide-vectors.ts) written through the rebuild
 * core's OPFS backend and extent space in this dedicated worker, then read back as one
 * sequential decode stream (scripts/browser-perf/page/export-stream.ts) through the space's page
 * cache under a memory governor, as an export would.
 */
import { BlobCache } from "../src/next/format/blob-cache.js";
import { Governor, type EvictableCache } from "../src/next/memory/governor.js";
import type { ChunkDirectory } from "../src/next/page/directory.js";
import { GroupWriter } from "../src/next/page/writer.js";
import type { ExtentRef, ExtentWriter } from "../src/next/storage/extents.js";
import { createOpfsBackend, webLocks } from "../src/next/storage/opfs.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import { countingBudget, jsonReachable } from "../src/next/storage/space-test-support.js";
import { VERIFY_CACHED_BYTES } from "../src/next/storage/verify-cached.js";
import {
  establishWorkingSet,
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
      readonly capMiB: number;
      readonly passes: number;
      /** Brackets the passes with console.profile / profileEnd (the spec's CDP profiler). */
      readonly profile?: boolean;
    }
  | { readonly op: "remove"; readonly dir: string };

export interface BuildResult {
  readonly groups: ExportGroup[];
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
  /** The page cache's protected segment before the export and after this pass. */
  readonly protectedBeforeMiB: number;
  readonly protectedAfterMiB: number;
  /** After the last pass, everything but the hot set's size evicted: what survived (else null). */
  readonly kept: { hot: string; scanned: string } | null;
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
) {
  const backend = createOpfsBackend({
    directory: await directory(name),
    name: `next-export:${name}`,
    locks,
  });
  const lease = await backend.acquire({ timeoutMs: 10_000 });
  const session = await backend.open(lease);
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
  return { groups, dataBytes, ms: performance.now() - started };
}

async function runExport(request: Extract<ExportRequest, { op: "export" }>): Promise<ExportPass[]> {
  const governor = new Governor(request.capMiB * MiB);
  let cache: EvictableCache | null = null;
  const { lease, space } = await open(request.dir, governor, (registered) => {
    cache = registered;
  });
  const directories = new BlobCache<ChunkDirectory>((evictable) =>
    governor.registerCache(evictable),
  );
  const grant = await governor.admit({ label: "export", minimumBytes: 32 * MiB, spillable: true });
  const first = request.groups[0];
  if (first === undefined) throw new Error("no groups");
  const working = await establishWorkingSet(space, first, request.outside, {
    budget: grant,
    directories,
  });
  const protectedBefore = space.stats.cache.protectedBytes / MiB;
  const passes: ExportPass[] = [];
  if (request.profile === true) console.profile("export");
  for (let pass = 0; pass < request.passes; pass++) {
    const readsBefore = space.stats.cache.platformReads;
    const result = await exportStream(space, request.groups, {
      budget: grant,
      directories,
      once: request.once,
      readahead: request.readaheadKiB * 1024,
    });
    passes.push({
      verifying: VERIFY_CACHED_BYTES,
      ...result,
      platformReads: space.stats.cache.platformReads - readsBefore,
      governorPeakMiB: governor.peakBytes / MiB,
      cacheResidentMiB: space.stats.cache.residentBytes / MiB,
      protectedBeforeMiB: protectedBefore,
      protectedAfterMiB: space.stats.cache.protectedBytes / MiB,
      kept: null,
    });
  }
  if (request.profile === true) {
    console.profileEnd("export");
    // Let the profile reach the inspector before the page terminates this worker.
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  const evictable = cache as EvictableCache | null;
  const last = passes.at(-1);
  if (evictable !== null && last !== undefined) {
    const left = survivors(space, evictable, working);
    passes[passes.length - 1] = {
      ...last,
      kept: {
        hot: `${String(left.hot)}/${String(working.hot.length)}`,
        scanned: `${String(left.scanned)}/${String(working.scanned.length)}`,
      },
    };
  }
  grant.close();
  await space.close();
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
