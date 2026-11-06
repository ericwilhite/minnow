/**
 * Gate A owner-ready measurement: backend/ByteStore -> extent space -> real WritePathImpl
 * recovery -> owner host ready. The executor is a stand-in and is UNUSED during startup.
 * Its optional first SELECT count(*) reads write-path row statistics only; it measures planning,
 * protocol and delivery, not a scan or the integrated executor. No startup phase uses fake data.
 * The factory acquires handles in openFence; session/space/write-path recovery runs in open,
 * as host.ts requires, outside the fence-acquisition deadline. The ready timer includes both.
 * Native reads count returned OPFS bytes / IndexedDB cloned byte payloads; IndexedDB's physical
 * disk I/O is opaque. Index/meta numeric values and keys are reported separately (8 B each).
 */
import { createOpfsBackend, webLocks, type OpfsDirectoryLike } from "../src/next/storage/opfs.js";
import { createIndexedDbBackend } from "../src/next/storage/indexeddb.js";
import { openIndexedDb, type IdbDatabaseLike } from "../src/next/storage/idb-like.js";
import type { StorageBackend } from "../src/next/storage/contract.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import { listedRootExtents, reachableExtents } from "../src/next/format/reachable.js";
import { decodeManifestRoot } from "../src/next/format/manifest-codec.js";
import { WritePathImpl } from "../src/next/write/engine.js";
import { Governor } from "../src/next/memory/governor.js";
import { ScratchSpillService } from "../src/next/spill/scratch-spill.js";
import { composeEngine } from "../src/next/owner/compose.js";
import { startOwnerHost, type OwnerHost, type OwnerEngineFactory } from "../src/next/owner/host.js";
import { OwnerPhaseMeter } from "./next-storage-owner-phase.js";
import { ProtocolClient } from "../src/next/owner/client.js";
import { createLowerer } from "../src/next/lower/create.js";
import { StandinExecutor } from "../src/next/testing/owner-standins.js";
import type { PhysicalPlan } from "../src/next/ir/ir.js";
import type { ChunkReads } from "../src/next/exec/scan.js";

export type OwnerReopenStore = "opfs" | "indexeddb";
export type OwnerReopenPhase = "backend" | "space" | "recovery" | "host" | "countSetup" | "count";
export interface ReadCounts {
  payloadBytes: number;
  numericBytes: number;
  reads: number;
}
export interface OwnerPhase {
  phase: OwnerReopenPhase;
  wallMs: number;
  reads: ReadCounts;
}
const bytesOf = (value: unknown): number => {
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return value.byteLength;
  if (Array.isArray(value)) return value.reduce<number>((sum, item) => sum + bytesOf(item), 0);
  if (typeof value === "object" && value !== null && "bytes" in value) return bytesOf(value.bytes);
  return 0;
};
const numbersOf = (value: unknown): number => {
  if (typeof value === "number") return 8;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return 0;
  const values = Array.isArray(value)
    ? value
    : typeof value === "object" && value !== null
      ? Object.values(value)
      : [];
  return values.reduce<number>((sum, item) => sum + numbersOf(item), 0);
};

export function countDatabase(database: IdbDatabaseLike, counts: ReadCounts): IdbDatabaseLike {
  const observed = (value: unknown) => {
    counts.reads++;
    counts.payloadBytes += bytesOf(value);
    counts.numericBytes += numbersOf(value);
  };
  return {
    transaction: (stores, mode, durability, body) =>
      database.transaction(stores, mode, durability, (tx) =>
        body({
          abort: (reason) => tx.abort(reason),
          store(name) {
            const store = tx.store(name);
            return {
              ...store,
              get: (key, then) =>
                store.get(key, (value) => {
                  observed(value);
                  then(value);
                }),
              getAll: (range, then, count) =>
                store.getAll(
                  range,
                  (values) => {
                    observed(values);
                    then(values);
                  },
                  count,
                ),
              getAllKeys: (range, then, count) =>
                store.getAllKeys(
                  range,
                  (keys) => {
                    observed(keys);
                    then(keys);
                  },
                  count,
                ),
              last: (range, then) =>
                store.last(range, (value) => {
                  observed(value);
                  then(value);
                }),
            };
          },
        }),
      ),
    close: () => database.close(),
  };
}
async function backendFor(
  store: OwnerReopenStore,
  name: string,
  counts: ReadCounts,
  governor: Governor,
): Promise<StorageBackend> {
  const locks = webLocks(navigator.locks);
  if (store === "indexeddb")
    return createIndexedDbBackend({
      name,
      locks,
      memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
      database: async () => countDatabase(await openIndexedDb(indexedDB, name), counts),
    });
  const root = await navigator.storage.getDirectory();
  const directory = await root.getDirectoryHandle(name, { create: true });
  const wrapped: OpfsDirectoryLike = {
    getFileHandle: async (file, options) => ({
      createSyncAccessHandle: async () => {
        const handle = await (
          await directory.getFileHandle(file, options)
        ).createSyncAccessHandle();
        return {
          getSize: () => handle.getSize(),
          read(target, options) {
            const read = handle.read(target, options);
            counts.payloadBytes += read;
            counts.reads++;
            return read;
          },
          write: (source, options) => handle.write(source, options),
          truncate: (size) => handle.truncate(size),
          flush: () => handle.flush(),
          close: () => handle.close(),
        };
      },
    }),
  };
  return createOpfsBackend({ name, locks, directory: wrapped });
}

export async function openRealStorage(
  store: OwnerReopenStore,
  db: string,
  phase: (phase: OwnerPhase) => void | Promise<void> = () => undefined,
  started = performance.now(),
  capBytes = 256 << 20,
) {
  return recoverRealStorage(await openRealFence(store, db, phase, started, capBytes));
}

async function openRealFence(
  store: OwnerReopenStore,
  db: string,
  phase: (phase: OwnerPhase) => void | Promise<void>,
  started: number,
  capBytes = 256 << 20,
) {
  const counts: ReadCounts = { payloadBytes: 0, numericBytes: 0, reads: 0 };
  const meter = new OwnerPhaseMeter(counts, phase, () => performance.now(), started);
  const finish = (name: OwnerReopenPhase) => meter.finish(name);
  const governor = new Governor(capBytes);
  const backend = await backendFor(store, `b1-owner-data-${db}`, counts, governor);
  const lease = await backend.acquire({ timeoutMs: 30_000 });
  return { backend, lease, governor, finish, counts };
}

async function recoverRealStorage(fence: Awaited<ReturnType<typeof openRealFence>>) {
  const { backend, lease, governor, finish, counts } = fence;
  const session = await backend.open(lease);
  await finish("backend");
  const startup = await governor.admit({
    label: "extent space startup",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  const space = await openExtentSpace(session, {
    reachable: listedRootExtents(),
    walk: reachableExtents(),
    budget: startup,
    registerCache: (cache) => governor.registerCache(cache),
  });
  // The extent-space startup pipeline has finished. Keep its spillable handle usable for an
  // explicit later audit, but return its unused admission before the writer starts.
  startup.release(startup.held - startup.allocated);
  await finish("space");
  const path = await WritePathImpl.open({
    session,
    space,
    governor,
    options: { durability: "strict" },
  });
  await finish("recovery");
  const scratch = backend.scratch(lease);
  if (scratch === null) throw new Error("Native benchmark requires scratch storage");
  const spill = new ScratchSpillService(scratch, {
    memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
  });
  await spill.sweep();
  return {
    path,
    space,
    session,
    lease,
    governor,
    spill,
    finish,
    counts,
    async close() {
      await path.close();
      await spill.close();
      await space.close();
      startup.close();
      await session.close();
      await lease.release();
    },
  };
}

export async function startMeasuredOwner(
  store: OwnerReopenStore,
  db: string,
  phase: (phase: OwnerPhase) => void | Promise<void>,
  onReady: () => void = () => undefined,
) {
  const started = performance.now();
  // Distinct host/storage lock names: the native backend owns its own lease under the host fence.
  const plans = new WeakMap<PhysicalPlan, number>();
  const lowerer = createLowerer();
  let opened: Awaited<ReturnType<typeof openRealStorage>> | null = null;
  const getOpened = () => {
    if (opened === null) throw new Error("Missing real storage");
    return opened;
  };
  let release: Promise<unknown> = Promise.resolve();
  const executor = new StandinExecutor({
    countFromStatistics: (plan) => {
      const table = plans.get(plan);
      return table === undefined ? null : (opened?.path.statistics(table)?.rowCount ?? null);
    },
  });
  // Acquisition/session open belongs to the backend phase, including the outer host lock wait.
  const factory: OwnerEngineFactory<Awaited<ReturnType<typeof openRealFence>>> = {
    openFence() {
      return openRealFence(store, db, phase, started);
    },
    epochOf: (fence) => fence.lease.epoch,
    closeFence(fence) {
      release = fence.lease.release();
    },
    async open(fence) {
      // OwnerEngineFactory.openFence acquires handles only. Session loading and recovery are
      // startup step 2 (host.ts), outside acquireFenced's 5s handle-acquisition deadline.
      opened = await recoverRealStorage(fence);
      const storage = opened;
      const engine = composeEngine({
        planner: {
          ...lowerer,
          lower(text, context) {
            const lowered = lowerer.lower(text, context);
            const match = /^SELECT count\(\*\) FROM ([a-z_][a-z_0-9]*)$/u.exec(text);
            if (match !== null && lowered.kind === "query") {
              const table = context.catalog.tableByName(match[1] ?? "");
              if (table !== undefined) plans.set(lowered.plan, table.id);
            }
            return lowered;
          },
        },
        executor,
        writePath: storage.path,
        governor: storage.governor,
        spill: storage.spill,
        reads: {} as ChunkReads,
        statistics: { table: (id) => storage.path.statistics(id) },
      });
      const original = engine.close.bind(engine);
      engine.close = async () => {
        await original();
        await storage.close();
      };
      return engine;
    },
  };
  const channel = new MessageChannel();
  const announcement = { phase: null as Promise<OwnerPhase> | null };
  const maintenanceAtReady: Partial<WritePathImpl["maintenance"]> = {};
  let host: OwnerHost | null = null;
  try {
    host = await startOwnerHost(
      {
        t: "minnow:host",
        database: `b1-owner-host-${db}`,
        build: "r25-bench",
        clientPort: channel.port1,
        brokerPort: null,
        settings: {},
      },
      factory,
      {
        locks: navigator.locks,
        post: (status) => {
          if (status.t === "minnow:ready") {
            const storage = getOpened();
            Object.assign(maintenanceAtReady, storage.path.maintenance);
            announcement.phase = storage.finish("host"); // exact host-ready endpoint
            onReady();
          } else if (status.t === "minnow:failed") throw new Error(JSON.stringify(status.error));
        },
      },
    );
    if (announcement.phase === null) throw new Error("Owner did not announce ready");
    // TypeScript cannot see the asynchronous factory assignment.
    const storage = getOpened();
    const hostPhase = await announcement.phase;
    if (executor.prepared !== 0) throw new Error("The executor was used before owner ready");
    const rootGrant = await storage.governor.admit({
      label: "bench root metadata",
      minimumBytes: 0,
      spillable: true,
    });
    const parsed =
      storage.space.root === null ? null : decodeManifestRoot(storage.space.root, rootGrant);
    const root = parsed?.value ?? null;
    const rootInfo =
      root === null
        ? null
        : {
            maintenanceAtReady,
            groups: root.tables.reduce((sum, table) => sum + table.groupCount, 0),
            staged: root.tables.reduce((sum, table) => sum + table.staged.length, 0),
            listPages: root.extentList.length,
            extents: root.extentList.reduce((sum, page) => sum + page.count, 0),
            dataBytes: storage.space.stats.dataBytes,
            logBytes: storage.path.stats().logBytes,
            rows: storage.path.catalog.tables.map((table) => ({
              name: table.name,
              rows: storage.path.statistics(table.id)?.rowCount,
            })),
          };
    if (parsed !== null) rootGrant.free(parsed.charge);
    rootGrant.close();
    return {
      storage,
      rootInfo,
      hostPhase,
      executor,
      port: channel.port2,
      async close() {
        await host?.stop("closing", true);
        await release;
        channel.port2.close();
      },
    };
  } catch (error) {
    await host?.stop("closing", true);
    await release;
    throw error;
  }
}

/** Metadata-only count through the real owner/client protocol, with its liveness lock held. */
export async function firstStatisticsCount(
  owner: Awaited<ReturnType<typeof startMeasuredOwner>>,
  table: string,
) {
  const client = new ProtocolClient({ build: "r25-bench" });
  let unlock!: () => void;
  let locked!: () => void;
  const held = new Promise<void>((resolve) => {
    locked = resolve;
  });
  const untilClose = new Promise<void>((resolve) => {
    unlock = resolve;
  });
  const lock = navigator.locks.request(client.livenessLock, { mode: "exclusive" }, async () => {
    locked();
    await untilClose;
  });
  await held;
  try {
    await client.connect(owner.port);
    await owner.storage.finish("countSetup"); // caller discards this connection/metadata interval
    const result = await client.query(`SELECT count(*) FROM ${table}`);
    const sample = await owner.storage.finish("count");
    const rows = result.rows();
    const expected = owner.rootInfo?.rows.find((entry) => entry.name === table)?.rows;
    if (String(rows[0]?.[0]) !== String(expected))
      throw new Error(`Statistics count mismatch: ${String(rows[0]?.[0])} / ${String(expected)}`);
    return { sample, count: String(rows[0]?.[0]), executor: "statistics-only stand-in" };
  } finally {
    client.close();
    unlock();
    await lock;
  }
}
