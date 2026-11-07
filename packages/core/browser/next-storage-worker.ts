import { ChaosRequestGate, gatedChaosPoster } from "./next-storage-gate.js";
import { buildListedReopenSpace, openListedReopen } from "./next-storage-listed-reopen.js";
let reopenClose: (() => Promise<void>) | null = null;
/**
 * The rebuild core's storage in a real dedicated worker over real OPFS sync access handles
 * (packages/core/src/next/storage). Driven by next-storage-run.ts through a small RPC; each
 * operation names its database directory, so the page can kill this worker and verify the same
 * files from another one.
 */
import { storeConformanceCases } from "../src/next/storage/conformance.js";
import type { StorageBackend } from "../src/next/storage/contract.js";
import type { BlobRef, ExtentRef, ExtentSpace } from "../src/next/storage/extents.js";
import type { StorageObserver, SyncFile } from "../src/next/storage/contract.js";
import { createOpfsBackend, webLocks, type OpfsDirectoryLike } from "../src/next/storage/opfs.js";
import { openIndexedDb, type IdbDatabaseLike } from "../src/next/storage/idb-like.js";
import { createIndexedDbBackend } from "../src/next/storage/indexeddb.js";
import { chooseBackend } from "../src/next/storage/select.js";
import { runIdbScenarios } from "../src/next/storage/idb-scenarios.js";
import { SimDisk } from "../src/next/storage/sim-disk.js";
import { simIndexedDb } from "../src/next/storage/sim-idb.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import {
  bytesOf,
  countingBudget,
  countingMemory,
  jsonReachable,
} from "../src/next/storage/space-test-support.js";
import { ScratchSpillService } from "../src/next/spill/scratch-spill.js";
import {
  identity,
  postingObserver,
  runChaos,
  type ChaosExpect,
  type ChaosInjection,
  type ChaosMode,
  type StepDownRelease,
} from "./next-storage-chaos.js";
import {
  buildReopenSpace,
  measureIdbOpenRequests,
  measureReopen,
  type ReopenLayout,
} from "./next-storage-reopen.js";

/** Which backend an operation runs on (OPFS by default). */
export type StoreKind = "opfs" | "indexeddb";

export type NextStorageRequest = {
  readonly store?: StoreKind;
  /** IndexedDB: one serialized transaction chain per connection (the default) or not. */
  readonly serialize?: boolean;
} & (
  | { readonly op: "listed-reopen-build"; readonly dir: string; readonly groups: number }
  | { readonly op: "listed-reopen-measure"; readonly dir: string }
  | { readonly op: "listed-reopen-close" }
  | { readonly op: "conformance"; readonly prefix: string }
  | { readonly op: "choose"; readonly name?: string }
  | { readonly op: "idb-scenarios" }
  | { readonly op: "bench-idb"; readonly dir: string; readonly batching: boolean }
  | { readonly op: "publish-loop"; readonly dir: string; readonly rounds: number }
  | { readonly op: "verify"; readonly dir: string; readonly minAcked: number }
  | { readonly op: "hold"; readonly dir: string }
  | { readonly op: "write-after-loss" }
  | {
      readonly op: "acquire";
      readonly dir: string;
      readonly steal: boolean;
      readonly timeoutMs: number;
    }
  | { readonly op: "spill"; readonly dir: string }
  | {
      readonly op: "bench-builders";
      readonly dir: string;
      readonly columns: number;
      readonly pagesPerColumn: number;
      readonly pageBytes: number;
      readonly alternating: boolean;
      readonly buffered: boolean;
    }
  | { readonly op: "remove"; readonly dir: string }
  | {
      readonly op: "heavy-open";
      readonly dir: string;
      /** The root a reopen must find (round and blob identities); a missing root is refused. */
      readonly expect?: { readonly round: number; readonly identity: string };
    }
  | { readonly op: "heavy-fill"; readonly mib: number; readonly publishEveryMib: number }
  | { readonly op: "heavy-reclaim"; readonly refillMib: number }
  | { readonly op: "heavy-quota-write"; readonly mib: number }
  | { readonly op: "heavy-continue"; readonly mib: number }
  | { readonly op: "heavy-quota-publish"; readonly fillerMib: number }
  | { readonly op: "heavy-close" }
  | { readonly op: "microbench"; readonly dir: string }
  | {
      readonly op: "reopen-build";
      readonly dir: string;
      readonly layout: ReopenLayout;
      readonly blobs: number;
    }
  | { readonly op: "reopen-measure"; readonly dir: string }
  | { readonly op: "reopen-idb-requests"; readonly dir: string }
  | { readonly op: "diagnose-wedge"; readonly dir: string }
  | { readonly op: "chaos-release" }
  | {
      readonly op: "chaos";
      readonly dir: string;
      readonly seed: number;
      readonly expect: ChaosExpect | null;
      readonly inject?: ChaosInjection;
      readonly mode?: ChaosMode;
      readonly lanes?: readonly string[];
    }
);

const scope = self as unknown as {
  postMessage(message: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
};

async function directory(name: string): Promise<OpfsDirectoryLike & FileSystemDirectoryHandle> {
  const root = await navigator.storage.getDirectory();
  const base = await root.getDirectoryHandle("next-storage", { create: true });
  return await base.getDirectoryHandle(name, { create: true });
}

const locks = webLocks(navigator.locks);

/** The backend of the request being handled (set by `handle`). */
let store: StoreKind = "opfs";
let serializeIdb = true;
/** The chaos campaign's observer of platform requests (null outside a chaos run). */
let storageObserver: StorageObserver | null = null;

/** Platform requests, counted below the backends: OPFS data-file reads and IndexedDB `get`s. */
const platform = { dataFileReads: 0, idbGets: 0 };

/** The directory, with the data file's sync handle counting its `read` calls. */
function countingDirectory(inner: OpfsDirectoryLike): OpfsDirectoryLike {
  return {
    async getFileHandle(name, options) {
      const handle = await inner.getFileHandle(name, options);
      if (name !== "data") return handle;
      return {
        async createSyncAccessHandle(): Promise<SyncFile> {
          const file = await handle.createSyncAccessHandle();
          return new Proxy(file, {
            get(target, key) {
              const value: unknown = Reflect.get(target, key);
              if (typeof value !== "function") return value;
              const method = value as (...args: unknown[]) => unknown;
              return (...args: unknown[]) => {
                if (key === "read") platform.dataFileReads += 1;
                return method.apply(target, args);
              };
            },
          });
        },
      };
    },
  };
}

/** The database, counting every `get` request its transactions issue. */
function countingDatabase(inner: IdbDatabaseLike): IdbDatabaseLike {
  return {
    transaction: (stores, mode, durability, body) =>
      inner.transaction(stores, mode, durability, (tx) => {
        body({
          store: (name) => {
            const target = tx.store(name);
            return {
              ...target,
              get: (key, then) => {
                platform.idbGets += 1;
                target.get(key, then);
              },
            };
          },
          abort: (reason) => {
            tx.abort(reason);
          },
        });
      }),
    close: () => {
      inner.close();
    },
  };
}

async function backendFor(name: string): Promise<StorageBackend> {
  if (store === "indexeddb")
    return createIndexedDbBackend({
      database: async () => countingDatabase(await openIndexedDb(indexedDB, `next-test:${name}`)),
      name: `next-test:${name}`,
      locks,
      serializeTransactions: serializeIdb,
      ...(storageObserver === null ? {} : { observe: storageObserver }),
    });
  return createOpfsBackend({
    directory: countingDirectory(await directory(name)),
    name: `next-test:${name}`,
    locks,
    ...(storageObserver === null ? {} : { observe: storageObserver }),
  });
}

const decoder = new TextDecoder();
interface TestRoot {
  readonly round: number;
  readonly blobs: readonly BlobRef[];
  /** Padding that makes a root large (the quota-pressure publish). */
  readonly filler?: string;
}
const encodeRoot = (root: TestRoot) => new TextEncoder().encode(JSON.stringify(root));
const decodeRoot = (bytes: Uint8Array | null): TestRoot | null =>
  bytes === null ? null : (JSON.parse(decoder.decode(bytes)) as TestRoot);

async function openSpace(name: string, timeoutMs = 10_000) {
  const backend = await backendFor(name);
  const lease = await backend.acquire({ timeoutMs });
  const session = await backend.open(lease);
  const space = await openExtentSpace(session, {
    reachable: jsonReachable,
    budget: countingBudget(Infinity),
    registerCache: () => countingMemory(Infinity),
  });
  return { backend, lease, session, space };
}

/** One round: a streamed chunk (extended, sometimes relocated) and a small blob; retire `previous`. */
/** The phases of one round the page can aim a kill at (next-storage-run.ts). */
export type PublishPhase = "writing" | "publishing" | "published";

async function round(
  space: ExtentSpace,
  number: number,
  previous: readonly ExtentRef[],
  phase: (phase: PublishPhase) => void = () => undefined,
): Promise<ExtentRef[]> {
  phase("writing");
  const writer = space.beginWrite(`round ${String(number)}`);
  const chunk = writer.open(16_384);
  const small = number % 2 === 0 ? await writer.write([bytesOf(300 + number, number)]) : null;
  for (let page = 0; page < 4; page++) {
    const part = bytesOf(9_000 + ((number * 131 + page * 17) % 5_000), number * 8 + page);
    if (chunk.length + part.length > chunk.capacity) chunk.extend(Math.max(part.length, 16_384));
    await chunk.append([part]);
  }
  const sealed = await chunk.seal();
  const tail = small ?? (await writer.write([bytesOf(300 + number, number)]));
  if (sealed === null) throw new Error("sealed empty");
  phase("publishing");
  await space.publish(encodeRoot({ round: number, blobs: [...sealed.blobs, ...tail.blobs] }), {
    writers: [writer],
    retired: previous,
  });
  phase("published");
  return [sealed.extent, tail.extent];
}

const extentsOf = (root: TestRoot | null): ExtentRef[] => {
  const seen = new Map<number, ExtentRef>();
  for (const blob of root?.blobs ?? []) seen.set(blob.extent.offset, blob.extent);
  return [...seen.values()];
};

async function readAll(space: ExtentSpace, blobs: readonly BlobRef[]): Promise<void> {
  const lease = await space.read(blobs, { budget: countingBudget(Infinity) });
  lease.release();
}

let held: Awaited<ReturnType<typeof openSpace>> | null = null;

const median = (samples: number[]) => {
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
};
const p95 = (samples: number[]) => {
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))] ?? 0;
};

/**
 * The storage microbench (the nightly browser lane records it): the cache's hit paths, a cold
 * coalesced slab read, publish latency, and commit latency with and without a log flush, on the
 * backend `store` selects. Times in microseconds unless named otherwise.
 */
async function microbench(dir: string): Promise<Record<string, number>> {
  const first = await openSpace(dir);
  const space = first.space;
  const writer = space.beginWrite("microbench");
  const pages = Array.from({ length: 32 }, (_, index) => bytesOf(32_768, index));
  const written = await writer.write(pages);
  await space.publish(encodeRoot({ round: 1, blobs: written.blobs }), {
    writers: [writer],
    retired: [],
  });
  const budget = countingBudget(Infinity);
  const hot = written.blobs[0];
  if (hot === undefined) throw new Error("no blob");
  await readAll(space, [hot]);
  let start = performance.now();
  let sink = 0;
  for (let index = 0; index < 100_000; index++) sink += space.peek(hot)?.length ?? 0;
  const peekNs = ((performance.now() - start) * 1e6) / 100_000;
  start = performance.now();
  for (let index = 0; index < 10_000; index++) {
    const lease = await space.read([hot], { budget });
    sink += lease.bytes.length;
    lease.release();
  }
  const leasedHitUs = ((performance.now() - start) * 1000) / 10_000;
  // Publish latency: a small write and a publish (data flush, root slot, flush).
  const publish: number[] = [];
  let previous = [written.extent];
  for (let round = 2; round < 32; round++) {
    const small = space.beginWrite("small");
    start = performance.now();
    const { blobs, extent } = await small.write([bytesOf(400, round)]);
    await space.publish(encodeRoot({ round, blobs: [...written.blobs, ...blobs] }), {
      writers: [small],
      retired: round > 2 ? previous : [],
    });
    publish.push((performance.now() - start) * 1000);
    previous = [extent];
  }
  // Commit latency: 200-byte log appends, flushed each (strict) or not (relaxed).
  const strict: number[] = [];
  const relaxed: number[] = [];
  const frame = bytesOf(200, 1);
  for (let index = 0; index < 50; index++) {
    start = performance.now();
    await first.session.appendLog(frame);
    await first.session.flushLog();
    strict.push((performance.now() - start) * 1000);
    start = performance.now();
    await first.session.appendLog(frame);
    relaxed.push((performance.now() - start) * 1000);
  }
  await space.close();
  await first.lease.release();
  // Cold: a new owner (empty cache) reads the 32 pages: one coalesced 1 MiB slab, so one store
  // read (`readDataInto`). Below it, OPFS makes one file read; IndexedDB makes one `get` for the
  // epoch and one per record the slab spans.
  const second = await openSpace(dir);
  platform.dataFileReads = 0;
  platform.idbGets = 0;
  start = performance.now();
  await readAll(second.space, written.blobs);
  const coldSlabMs = performance.now() - start;
  const storeReads = second.space.stats.cache.platformReads;
  const below =
    store === "opfs"
      ? { coldSlabFileReads: platform.dataFileReads }
      : { coldSlabIdbGets: platform.idbGets };
  await second.space.close();
  await second.lease.release();
  if (sink === 0) throw new Error("unreachable");
  return {
    peekNs,
    leasedHitUs,
    coldSlabMs,
    coldSlabStoreReads: storeReads,
    ...below,
    publishP50Us: median(publish),
    publishP95Us: p95(publish),
    commitStrictP50Us: median(strict),
    commitStrictP95Us: p95(strict),
    commitRelaxedP50Us: median(relaxed),
    commitRelaxedP95Us: p95(relaxed),
  };
}

/** The multi-GB run's state: the open space and every blob its current root names. */
let heavy: {
  dir: string;
  opened: Awaited<ReturnType<typeof openSpace>>;
  blobs: BlobRef[];
  round: number;
} | null = null;

const MIB = 1 << 20;
/** One MiB blob whose first bytes are its sequence number (so every blob has its own checksum). */
const heavyBlob = (() => {
  const buffer = bytesOf(MIB, 3);
  let sequence = 0;
  return () => {
    sequence += 1;
    new DataView(buffer.buffer).setFloat64(0, sequence, true);
    return buffer;
  };
})();

function heavyState() {
  if (heavy === null) throw new Error("heavy-open first");
  return heavy;
}

/** Writes `mib` one-MiB blobs, publishing a root naming every blob each `every` MiB. */
async function heavyFill(mib: number, every: number): Promise<void> {
  const state = heavyState();
  const space = state.opened.space;
  let writer = space.beginWrite("heavy");
  for (let written = 1; written <= mib; written++) {
    state.blobs.push(...(await writer.write([heavyBlob()])).blobs);
    if (written % every === 0 || written === mib) {
      state.round += 1;
      await space.publish(encodeRoot({ round: state.round, blobs: state.blobs }), {
        writers: [writer],
        retired: [],
      });
      writer = space.beginWrite("heavy");
    }
  }
  await writer.abort();
}

/** Reads `count` random blobs of the current root (each verified by the space). */
async function heavySample(count: number): Promise<void> {
  const state = heavyState();
  for (let index = 0; index < count; index++) {
    const blob = state.blobs[Math.floor(Math.random() * state.blobs.length)];
    if (blob !== undefined) await readAll(state.opened.space, [blob]);
  }
}

async function handle(request: NextStorageRequest): Promise<unknown> {
  store = request.store ?? "opfs";
  serializeIdb = request.serialize ?? true;
  switch (request.op) {
    case "choose": {
      try {
        const chosen = await chooseBackend(request.name ?? `choose-${String(Date.now())}`);
        return { kind: chosen.kind, opfsRefused: chosen.opfsRefused, error: null };
      } catch (error) {
        const failure = error as Error & { store?: string; location?: string };
        return {
          kind: null,
          opfsRefused: null,
          error: { name: failure.name, store: failure.store, location: failure.location },
        };
      }
    }
    case "idb-scenarios": {
      // The same scenarios on the platform's IndexedDB and on the simulator: they must agree.
      const name = `next-test:scenarios-${String(Date.now())}-${String(Math.random()).slice(2, 8)}`;
      const real = await openIndexedDb(indexedDB, name);
      try {
        return {
          real: await runIdbScenarios(real),
          simulated: await runIdbScenarios(simIndexedDb(new SimDisk())),
        };
      } finally {
        real.close();
        indexedDB.deleteDatabase(name);
      }
    }
    case "bench-idb": {
      const backend = createIndexedDbBackend({
        database: () => openIndexedDb(indexedDB, `next-test:${request.dir}`),
        name: `next-test:${request.dir}`,
        locks,
        batching: request.batching,
        serializeTransactions: serializeIdb,
      });
      const lease = await backend.acquire();
      const session = await backend.open(lease);
      const space = await openExtentSpace(session, {
        reachable: jsonReachable,
        budget: countingBudget(Infinity),
        registerCache: () => countingMemory(Infinity),
      });
      // Bulk: 16 MiB as 32 KiB pages through 16 interleaved builders, then one publish.
      let start = performance.now();
      const writer = space.beginWrite("bench", { budget: countingBudget(Infinity) });
      const builders = Array.from({ length: 16 }, () => writer.open(1 << 20));
      const page = bytesOf(32_768, 3);
      for (let index = 0; index < 32; index++)
        for (const builder of builders) await builder.append([page]);
      const blobs: BlobRef[] = [];
      for (const builder of builders) blobs.push(...((await builder.seal())?.blobs ?? []));
      await space.publish(encodeRoot({ round: 1, blobs }), { writers: [writer], retired: [] });
      const bulkMs = performance.now() - start;
      // Commits: 100 groups of 4 small log appends and one flush.
      start = performance.now();
      for (let group = 0; group < 100; group++) {
        for (let frame = 0; frame < 4; frame++) await session.appendLog(bytesOf(200, frame));
        await session.flushLog();
      }
      const commitMs = performance.now() - start;
      await space.close();
      await lease.release();
      return { bulkMs, commitMs };
    }
    case "conformance": {
      const results: Array<{ name: string; error: string | null }> = [];
      let index = 0;
      for (const testCase of storeConformanceCases) {
        const name = `${request.prefix}-${String(index++)}`;
        try {
          await testCase.run(() => backendFor(name));
          results.push({ name: testCase.name, error: null });
        } catch (error) {
          results.push({ name: testCase.name, error: String(error) });
        }
      }
      return results;
    }
    case "publish-loop": {
      const { space } = await openSpace(request.dir);
      let previous = extentsOf(decodeRoot(space.root));
      let number = decodeRoot(space.root)?.round ?? 0;
      for (let left = 0; left < request.rounds; left++) {
        number += 1;
        const current = number;
        previous = await round(space, current, previous, (phase) => {
          scope.postMessage({ progress: { round: current, phase } });
        });
      }
      return { finished: number };
    }
    case "verify": {
      const started = performance.now();
      const { space, lease } = await openSpace(request.dir);
      const openedMs = performance.now() - started;
      const root = decodeRoot(space.root);
      const previous = decodeRoot(space.previousRoot);
      if (root === null) throw new Error("no root after a published round");
      if (root.round < request.minAcked)
        throw new Error(
          `root round ${String(root.round)} is older than acknowledged ${String(request.minAcked)}`,
        );
      if (root.round > 1 && previous?.round !== root.round - 1)
        throw new Error(
          `previous root ${String(previous?.round)} is not ${String(root.round - 1)}`,
        );
      await readAll(space, root.blobs);
      if (previous !== null) await readAll(space, previous.blobs);
      await space.audit();
      // The reopened owner keeps writing.
      await round(space, root.round + 1, extentsOf(root));
      await space.audit();
      await space.close();
      await lease.release();
      return { round: root.round, previous: previous?.round ?? null, openedMs };
    }
    case "hold": {
      held = await openSpace(request.dir);
      return { epoch: held.lease.epoch };
    }
    case "write-after-loss": {
      if (held === null) throw new Error("nothing held");
      const lost = held.lease.signal.aborted;
      try {
        await held.session.appendLog(new Uint8Array([1]));
        return { lost, error: null };
      } catch (error) {
        return { lost, error: error instanceof Error ? error.name : String(error) };
      }
    }
    case "acquire": {
      const backend = await backendFor(request.dir);
      const started = performance.now();
      try {
        const lease = await backend.acquire({ steal: request.steal, timeoutMs: request.timeoutMs });
        const ms = performance.now() - started;
        await lease.release();
        return { ok: true, ms, epoch: lease.epoch };
      } catch (error) {
        return { ok: false, error: error instanceof Error ? error.name : String(error) };
      }
    }
    case "spill": {
      const backend = await backendFor(request.dir);
      const lease = await backend.acquire();
      const scratch = backend.scratch(lease);
      if (scratch === null) throw new Error("this backend has no scratch space");
      const service = new ScratchSpillService(scratch);
      await service.sweep();
      const file = await service.create(1, countingBudget(1 << 20));
      const refs = [];
      for (let index = 0; index < 64; index++) refs.push(await file.append(bytesOf(30_000, index)));
      for (let index = 0; index < 64; index++) {
        const ref = refs[index];
        if (ref === undefined) throw new Error("missing spill ref");
        const bytes = await file.read(ref);
        const expected = bytesOf(30_000, index);
        if (bytes.some((byte, at) => byte !== expected[at]))
          throw new Error(`spill page ${String(index)} differs`);
      }
      const inUse = service.bytesInUse;
      await file.close();
      await service.sweep();
      await lease.release();
      return { pages: refs.length, inUse, after: service.bytesInUse };
    }
    case "bench-builders": {
      const { space, lease } = await openSpace(request.dir);
      const writer = space.beginWrite(
        "bench",
        request.buffered ? { budget: countingBudget(Infinity) } : {},
      );
      const capacity = request.pagesPerColumn * request.pageBytes;
      const builders = Array.from({ length: request.columns }, () => writer.open(capacity));
      const page = bytesOf(request.pageBytes, 7);
      let start = performance.now();
      if (request.alternating) {
        for (let index = 0; index < request.pagesPerColumn; index++)
          for (const builder of builders) await builder.append([page]);
      } else {
        for (const builder of builders)
          for (let index = 0; index < request.pagesPerColumn; index++) await builder.append([page]);
      }
      const appendMs = performance.now() - start;
      start = performance.now();
      const blobs: BlobRef[] = [];
      for (const builder of builders) blobs.push(...((await builder.seal())?.blobs ?? []));
      const sealMs = performance.now() - start;
      start = performance.now();
      await space.publish(encodeRoot({ round: 1, blobs }), { writers: [writer], retired: [] });
      const publishMs = performance.now() - start;
      await space.close();
      await lease.release();
      return { appendMs, sealMs, publishMs, bytes: blobs.length * request.pageBytes };
    }
    case "heavy-open": {
      const started = performance.now();
      const opened = await openSpace(request.dir, 30_000);
      const root = decodeRoot(opened.space.root);
      if (request.expect !== undefined && root === null) {
        await opened.space.close().catch(() => undefined);
        await opened.lease.release();
        throw new Error(
          `Reopened without a root; expected round ${String(request.expect.round)} (${request.expect.identity})`,
        );
      }
      heavy = {
        dir: request.dir,
        opened,
        blobs: [...(root?.blobs ?? [])],
        round: root?.round ?? 0,
      };
      const openMs = performance.now() - started;
      await heavySample(64);
      const auditStarted = performance.now();
      await opened.space.audit();
      return {
        openMs,
        auditMs: performance.now() - auditStarted,
        round: heavy.round,
        identity: identity(heavy.blobs),
        blobs: heavy.blobs.length,
        dataBytes: opened.space.stats.dataBytes,
      };
    }
    case "heavy-fill": {
      const started = performance.now();
      await heavyFill(request.mib, request.publishEveryMib);
      const state = heavyState();
      return {
        ms: performance.now() - started,
        blobs: state.blobs.length,
        dataBytes: state.opened.space.stats.dataBytes,
      };
    }
    case "heavy-reclaim": {
      // Retire half the blobs, publish once more so their space is reusable (two generations),
      // then write `refillMib` again: the data space must not grow.
      const state = heavyState();
      const space = state.opened.space;
      const filled = space.stats.dataBytes;
      const kept = state.blobs.filter((_, index) => index % 2 === 0);
      const retired = state.blobs.filter((_, index) => index % 2 === 1).map((blob) => blob.extent);
      state.blobs = kept;
      state.round += 1;
      await space.publish(encodeRoot({ round: state.round, blobs: kept }), {
        writers: [],
        retired,
      });
      state.round += 1;
      await space.publish(encodeRoot({ round: state.round, blobs: kept }), {
        writers: [],
        retired: [],
      });
      await (space as unknown as { settled(): Promise<void> }).settled();
      const before = space.stats.dataBytes;
      await heavyFill(request.refillMib, 256);
      await space.audit();
      return { filled, before, after: space.stats.dataBytes, freeAfter: space.stats.freeBytes };
    }
    case "heavy-quota-write": {
      // Write until the platform refuses: the writer fails, the space does not. The same space
      // then reads retained blobs and audits exactly, still under the lowered quota (review
      // B1-p2d finding 5); `heavy-continue` writes and publishes on it.
      const space = heavyState().opened.space;
      const writer = space.beginWrite("over quota");
      let error: string | null = null;
      let writtenMib = 0;
      try {
        for (let index = 0; index < request.mib; index++) {
          await writer.write([heavyBlob()]);
          writtenMib += 1;
        }
      } catch (failure) {
        error = failure instanceof Error ? failure.name : String(failure);
      }
      if (writer.state === "open") await writer.abort();
      const spaceFailed = space.failed?.name ?? null;
      await heavySample(16);
      await space.audit();
      return { error, spaceFailed, writtenMib, afterAudit: space.failed?.name ?? null };
    }
    case "heavy-continue": {
      // The space that saw a writer refused writes and publishes again.
      const state = heavyState();
      const before = state.round;
      await heavyFill(request.mib, request.mib);
      await heavySample(4);
      return {
        published: state.round - before,
        blobs: state.blobs.length,
        spaceFailed: state.opened.space.failed?.name ?? null,
      };
    }
    case "heavy-quota-publish": {
      // A root too large for what is left fails inside publish: the space is then failed.
      const state = heavyState();
      const space = state.opened.space;
      // The last successful root, captured before the attempt: recovery must find exactly it.
      const lastRoot = { round: state.round, identity: identity(state.blobs) };
      let error: string | null = null;
      try {
        await space.publish(
          encodeRoot({
            round: state.round + 1,
            blobs: state.blobs,
            filler: "x".repeat(request.fillerMib * MIB),
          }),
          { writers: [], retired: [] },
        );
      } catch (failure) {
        error = failure instanceof Error ? failure.name : String(failure);
      }
      return { error, spaceFailed: space.failed?.name ?? null, lastRoot };
    }
    case "listed-reopen-build":
      return buildListedReopenSpace(await backendFor(request.dir), request.groups);
    case "listed-reopen-measure": {
      const opened = await openListedReopen(() => backendFor(request.dir));
      reopenClose = () => opened.close();
      return opened.timing;
    }
    case "listed-reopen-close":
      await reopenClose?.();
      reopenClose = null;
      return null;
    case "microbench":
      return microbench(request.dir);
    case "reopen-build":
      return buildReopenSpace(await backendFor(request.dir), request.layout, request.blobs);
    case "reopen-measure":
      return measureReopen(await backendFor(request.dir));
    case "chaos-release": {
      if (chaosRelease === null) throw new Error("no chaos workload to release");
      return chaosRelease();
    }
    case "diagnose-wedge": {
      // Where a recovering owner is stuck (the WebKit wedge): the lock manager's view, a bounded
      // plain lock request, and a bounded open and write on the database, each alone.
      const lockName = `minnow:next-test:${request.dir}`;
      const bounded = <T>(work: Promise<T>, ms: number) =>
        Promise.race([
          work.then((value) => ({ ok: true as const, value })),
          new Promise<{ ok: false; value: string }>((resolve) => {
            setTimeout(() => {
              resolve({ ok: false, value: "timeout" });
            }, ms);
          }),
        ]);
      const query = await navigator.locks.query();
      const describe = (list: readonly LockInfo[] | undefined) =>
        (list ?? []).filter((info) => info.name === lockName).map((info) => info.mode);
      const lock = await bounded(
        navigator.locks.request(lockName, { signal: AbortSignal.timeout(3_000) }, () => "granted"),
        3_500,
      ).catch((error: unknown) => ({ ok: false as const, value: String(error) }));
      // Then keep probing (one bounded write every 3 s) for up to 120 s: does it ever clear?
      const probeStarted = performance.now();
      let clearedAfterMs: number | null = null;
      while (performance.now() - probeStarted < 120_000) {
        const attempt = await bounded(
          (async () => {
            const db = await openIndexedDb(indexedDB, `next-test:${request.dir}`);
            try {
              await db.transaction(["meta"], "readwrite", "strict", (tx) => {
                tx.store("meta").put("diagnose", 1);
              });
            } finally {
              db.close();
            }
          })(),
          3_000,
        );
        if (attempt.ok) {
          clearedAfterMs = Math.round(performance.now() - probeStarted);
          break;
        }
      }
      const write = await bounded(
        (async () => {
          const db = await openIndexedDb(indexedDB, `next-test:${request.dir}`);
          try {
            await db.transaction(["meta"], "readwrite", "strict", (tx) => {
              tx.store("meta").put("diagnose", 1);
            });
            return "committed";
          } finally {
            db.close();
          }
        })(),
        3_000,
      );
      return {
        held: describe(query.held),
        pending: describe(query.pending),
        lock,
        clearedAfterMs,
        write,
      };
    }
    case "reopen-idb-requests": {
      const db = await openIndexedDb(indexedDB, `next-test:${request.dir}`);
      try {
        return await measureIdbOpenRequests(db);
      } finally {
        db.close();
      }
    }
    case "chaos": {
      const port = chaosPort;
      if (port === null) throw new Error("no chaos channel");
      // Claim each numbered message atomically BEFORE posting it (review B1-p2h-4).
      // A kill between postMessage and the next instruction cannot hide a posted ack.
      const gate =
        killControl === null ? null : new ChaosRequestGate(killControl, store === "indexeddb");
      const post = gatedChaosPoster(port, postedCounter, gate);
      storageObserver = postingObserver(post);
      // A failure goes out on the campaign's channel too: a worker killed just after it still
      // delivers it there (the request's own reply may never leave a terminated worker).
      return runChaos(
        request.dir,
        request.seed,
        request.expect,
        {
          backendFor: async (name) => {
            const backend = await backendFor(name);
            if (gate === null || store !== "indexeddb") return backend;
            const open = backend.open.bind(backend);
            backend.open = async (lease) => {
              const session = await open(lease);
              const write = session.writeData.bind(session);
              const append = session.appendLog.bind(session);
              session.writeData = async (...args) => {
                await write(...args);
                await gate.queuedWrite();
              };
              session.appendLog = async (...args) => {
                const offset = await append(...args);
                await gate.queuedWrite();
                return offset;
              };
              return session;
            };
            return backend;
          },
          post,
          go: (sequence) =>
            new Promise<void>((resolve) => {
              goes.set(sequence, resolve);
            }),
          registerRelease: (release) => {
            chaosRelease = release;
          },
        },
        {
          ...(request.inject === undefined ? {} : { inject: request.inject }),
          ...(request.mode === undefined ? {} : { mode: request.mode }),
          ...(request.lanes === undefined ? {} : { lanes: request.lanes }),
        },
      ).catch((error: unknown) => {
        post({
          chaos: "failure",
          message: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
        });
        throw error;
      });
    }
    case "heavy-close": {
      const state = heavyState();
      await state.opened.space.close().catch(() => undefined);
      await state.opened.lease.release();
      heavy = null;
      return null;
    }
    case "remove": {
      if (store === "indexeddb") {
        await new Promise<void>((resolve) => {
          const request_ = indexedDB.deleteDatabase(`next-test:${request.dir}`);
          request_.onsuccess =
            request_.onerror =
            request_.onblocked =
              () => {
                resolve();
              };
        });
        return null;
      }
      const root = await navigator.storage.getDirectory();
      const base = await root.getDirectoryHandle("next-storage", { create: true });
      await base.removeEntry(request.dir, { recursive: true }).catch(() => undefined);
      return null;
    }
  }
}

/**
 * The page's answers to chaos intents (next-storage-chaos.ts), on the campaign's own channel:
 * the page drains its claimed prefix before judging recovery (see chaosPoster).
 */
const goes = new Map<number, () => void>();
let chaosPort: MessagePort | null = null;
/** Claimed campaign-message prefix that the page reads in shared memory. */
let postedCounter: Int32Array | null = null;
let killControl: Int32Array | null = null;
/** The running chaos workload's graceful step-down (next-storage-chaos.ts). */
let chaosRelease: (() => Promise<StepDownRelease>) | null = null;

scope.addEventListener("message", (event: MessageEvent<unknown>) => {
  const {
    chaosPort: port,
    counter,
    killGate,
  } = event.data as {
    chaosPort?: MessagePort;
    counter?: SharedArrayBuffer;
    killGate?: SharedArrayBuffer;
  };
  if (port !== undefined) {
    chaosPort = port;
    postedCounter = counter === undefined ? null : new Int32Array(counter);
    killControl = killGate === undefined ? null : new Int32Array(killGate);
    port.onmessage = (message: MessageEvent<{ go?: number }>) => {
      const go = message.data.go;
      if (go === undefined) return;
      goes.get(go)?.();
      goes.delete(go);
    };
    return;
  }
  const { id, request } = event.data as { id: number; request: NextStorageRequest };
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
scope.postMessage("next-storage-ready");
