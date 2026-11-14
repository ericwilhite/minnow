/** R25 storage startup lane. Real manifest/group codecs, one extent per group, batched publications.
 * Empty-column groups isolate metadata startup from data page I/O. This is not write-path/log replay.
 */
import { writeCatalogStorage } from "../src/next/write/catalog-storage.js";
import { Catalog } from "../src/next/write/catalog.js";
import type { BlobRef } from "../src/next/storage/extents.js";
import type { StorageBackend } from "../src/next/storage/contract.js";
import { ExtentListKeeper } from "../src/next/storage/extent-list.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import { countingBudget, countingMemory } from "../src/next/storage/space-test-support.js";
import {
  encodeGroupRoot,
  encodeManifestRoot,
  encodeTableStatistics,
  writeGroupDirectory,
  type GroupPageEntry,
} from "../src/next/format/manifest-codec.js";
import {
  MANIFEST_FORMAT_VERSION,
  type GroupRef,
  type ManifestRoot,
} from "../src/next/format/manifest.js";
import { listedRootExtents, reachableExtents } from "../src/next/format/reachable.js";

function required<T>(value: T | null | undefined): T {
  if (value === null || value === undefined)
    throw new Error("Expected a stored reopen fixture value");
  return value;
}

const options = () => ({
  reachable: listedRootExtents(),
  walk: reachableExtents(),
  budget: countingBudget(Infinity),
  registerCache: () => countingMemory(64 << 20),
});

export async function buildListedReopenSpace(
  backend: StorageBackend,
  groups: number,
  batch = 16_384,
) {
  if (groups < 1 || !Number.isSafeInteger(groups) || batch < 1 || batch % 4096 !== 0)
    throw new RangeError("Positive groups and a publication batch divisible by 4096 required");
  const start = performance.now();
  const opened = await openListedReopen(backend);
  const { space } = opened;
  try {
    if (space.root !== null) throw new Error("Reopen builder requires a fresh store");
    const keeper = new ExtentListKeeper([], { reader: space, budget: countingBudget(Infinity) });
    let root: ManifestRoot | null = null;
    let pages: readonly GroupPageEntry[] = [];
    const groupBytes = encodeGroupRoot({ rowCount: 1, rowIds: { kind: "dense" }, columns: [] });
    // A multiple of GROUPS_PER_PAGE keeps previous directory pages unchanged.
    const catalogData = Catalog.empty().change(
      {
        kind: "createTable",
        table: {
          name: "reopen",
          columns: [],
          primaryKey: null,
          unique: [],
          foreignKeys: [],
          checks: [],
        },
      },
      1,
    ).catalog;
    for (let at = 0; at < groups; at += batch) {
      const writer = space.beginWrite("reopen-groups");
      const catalog: BlobRef =
        root?.catalog ?? (await writeCatalogStorage(writer, catalogData, countingBudget(Infinity)));
      const refs: GroupRef[] = [];
      const end = Math.min(groups, at + batch);
      for (let i = at; i < end; i++) {
        const ref = required((await writer.write([groupBytes])).blobs[0]);
        refs.push({
          groupId: i + 1,
          firstRowId: i + 1,
          lastRowId: i + 1,
          rowCount: 1,
          liveRowCount: 1,
          root: ref,
          deletions: null,
        });
      }
      const directory = await writeGroupDirectory(writer, refs, pages);
      pages = directory.pages;
      const statistics = required(
        (await writer.write([encodeTableStatistics({ groupCount: end, columns: [] })])).blobs[0],
      );
      const retired =
        root === null
          ? []
          : [
              required(root.tables[0]).groups.extent,
              required(required(root.tables[0]).statistics).extent,
            ];
      const sequence: number = (root?.sequence ?? 0) + 1;
      const prepared = await keeper.prepare(sequence, {
        added: [...writer.extents],
        released: retired,
        writer,
      });
      root = {
        formatVersion: MANIFEST_FORMAT_VERSION,
        sequence,
        epoch: opened.epoch,
        log: { salt: 1, firstLsn: 1, rootSequence: 1 },
        globalLsn: 0,
        catalogVersion: 1,
        catalog,
        nextTableId: 2,
        sequences: [],
        clients: [],
        extentList: prepared.list,
        tables: [
          {
            tableId: 1,
            schemaVersion: 1,
            nextRowId: end + 1,
            flushedThroughLsn: 0,
            liveRowCount: end,
            groups: directory.ref,
            groupCount: end,
            staged: [],
            keyIndex: null,
            secondaryIndexes: [],
            statistics,
          },
        ],
      };
      await space.publish(encodeManifestRoot(root), {
        writers: [writer],
        retired: [...retired, ...prepared.retire],
      });
      keeper.commit(sequence, sequence);
    }
    const dataBytes = space.stats.dataBytes;
    return { groups, dataBytes, buildMs: Math.round(performance.now() - start) };
  } finally {
    await opened.close();
  }
}

export async function openListedReopen(backend: StorageBackend | (() => Promise<StorageBackend>)) {
  const t0 = performance.now();
  const resolved = typeof backend === "function" ? await backend() : backend;
  const factoryDone = performance.now();
  const lease = await resolved.acquire({ timeoutMs: 30_000 });
  const t1 = performance.now();
  let session: import("../src/next/storage/contract.js").StoreSession | null = null;
  try {
    session = await resolved.open(lease);
    const t2 = performance.now();
    const space = await openExtentSpace(session, options());
    const {
      dataBytes,
      cache: { bytesRead, platformReads },
    } = space.stats;
    const rootBytes = space.root?.length ?? 0;
    const previousRootBytes = (await session.readPreviousRoot())?.length ?? 0;
    const t3 = performance.now();
    const activeSession = session;
    return {
      space,
      epoch: activeSession.epoch,
      timing: {
        backendMs: factoryDone - t0,
        acquireMs: t1 - factoryDone,
        sessionMs: t2 - t1,
        spaceMs: t3 - t2,
        totalMs: t3 - t0,
        dataBytes,
        bytesRead,
        platformReads,
        rootBytes,
        previousRootBytes,
      },
      async close() {
        try {
          await space.close();
        } finally {
          try {
            await activeSession.close();
          } finally {
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    try {
      await session?.close();
    } finally {
      await lease.release();
    }
    throw error;
  }
}

/** Linux clock ticks sampled outside the worker, without profiling its JavaScript. */
export interface ReopenCpuSnapshot {
  readonly ticksPerSecond: number;
  readonly processes: Readonly<Record<string, number>>;
  readonly workers: Readonly<Record<string, number>>;
}

export function cpuDelta(before: ReopenCpuSnapshot, after: ReopenCpuSnapshot) {
  const delta = (a: Readonly<Record<string, number>>, b: Readonly<Record<string, number>>) =>
    (Object.entries(b).reduce((sum, [id, ticks]) => sum + Math.max(0, ticks - (a[id] ?? 0)), 0) *
      1000) /
    after.ticksPerSecond;
  if (Object.keys(before.workers).length === 0 || Object.keys(after.workers).length === 0)
    throw new Error("No Chromium DedicatedWorker CPU counters found");
  return {
    ownerCpuMs: delta(before.workers, after.workers),
    browserCpuMs: delta(before.processes, after.processes),
  };
}
