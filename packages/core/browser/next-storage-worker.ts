/**
 * The rebuild core's storage in a real dedicated worker over real OPFS sync access handles
 * (packages/core/src/next/storage). Driven by next-storage-run.ts through a small RPC; each
 * operation names its database directory, so the page can kill this worker and verify the same
 * files from another one.
 */
import { storeConformanceCases } from "../src/next/storage/conformance.js";
import type { StorageBackend } from "../src/next/storage/contract.js";
import type { BlobRef, ExtentRef, ExtentSpace } from "../src/next/storage/extents.js";
import { createOpfsBackend, webLocks, type OpfsDirectoryLike } from "../src/next/storage/opfs.js";
import { openIndexedDb } from "../src/next/storage/idb-like.js";
import { createIndexedDbBackend } from "../src/next/storage/indexeddb.js";
import { chooseBackend } from "../src/next/storage/select.js";
import { openExtentSpace } from "../src/next/storage/space.js";
import {
  bytesOf,
  countingBudget,
  countingMemory,
  jsonReachable,
} from "../src/next/storage/space-test-support.js";
import { ScratchSpillService } from "../src/next/spill/scratch-spill.js";

/** Which backend an operation runs on (OPFS by default). */
export type StoreKind = "opfs" | "indexeddb";

export type NextStorageRequest = { readonly store?: StoreKind } & (
  | { readonly op: "conformance"; readonly prefix: string }
  | { readonly op: "choose" }
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

async function backendFor(name: string): Promise<StorageBackend> {
  if (store === "indexeddb")
    return createIndexedDbBackend({
      database: () => openIndexedDb(indexedDB, `next-test:${name}`),
      name: `next-test:${name}`,
      locks,
    });
  return createOpfsBackend({ directory: await directory(name), name: `next-test:${name}`, locks });
}

const decoder = new TextDecoder();
interface TestRoot {
  readonly round: number;
  readonly blobs: readonly BlobRef[];
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

async function handle(request: NextStorageRequest): Promise<unknown> {
  store = request.store ?? "opfs";
  switch (request.op) {
    case "choose": {
      const chosen = await chooseBackend(`choose-${String(Date.now())}`);
      return { kind: chosen.kind, opfsRefused: chosen.opfsRefused };
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

scope.addEventListener("message", (event: MessageEvent<unknown>) => {
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
