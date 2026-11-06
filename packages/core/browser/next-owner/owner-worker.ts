/**
 * The owner worker of the browser lane: the next core's owner host (owner/host.ts) over real
 * lowering (lower/, B6) and the stand-in executor and write path (testing/owner-standins.ts), whose durable state lives in one OPFS file opened
 * with a sync access handle (the fence) — or, where workers have no OPFS (WebKit's pinned Linux
 * build), in IndexedDB, fenced by the Web Lock alone.
 *
 * Test knobs arrive on the BroadcastChannel `minnow-next-test:<db>`:
 * - `{ t: "hold", value }`: hold scheduled flushes (commits stay applied, not durable);
 * - `{ t: "dieOnSave" }`: after the next save is written, hang the worker forever, so the commit
 *   is durable and its reply never leaves (the test then closes the tab).
 */
import { composeEngine } from "../../dist/next/owner/compose.js";
import { serveOwnerWorker, type OwnerEngineFactory } from "../../dist/next/owner/host.js";
import type { WebLockManager } from "../../dist/next/owner/web-locks.js";
import { Governor } from "../../dist/next/memory/governor.js";
import { MemorySpillService } from "../../dist/next/spill/memory-spill.js";
import { createLowerer } from "../../dist/next/lower/create.js";
import {
  StandinExecutor,
  StandinWritePath,
  withTestStatements,
} from "../../dist/next/testing/owner-standins.js";
import type { ChunkReads } from "../../dist/next/exec/scan.js";

type Fence =
  | {
      readonly kind: "opfs";
      readonly db: string;
      readonly handle: FileSystemSyncAccessHandle;
      readonly epoch: number;
    }
  | {
      readonly kind: "idb";
      readonly db: string;
      readonly idb: IDBDatabase;
      readonly epoch: number;
    };

/** Bumps and returns the database's fence epoch in OPFS, under the fence (its own small file). */
async function bumpOpfsEpoch(root: FileSystemDirectoryHandle, db: string): Promise<number> {
  const file = await root.getFileHandle(`${FILE_PREFIX}${db}.epoch`, { create: true });
  const handle = await file.createSyncAccessHandle();
  try {
    const bytes = new Float64Array(1);
    if (handle.getSize() >= 8) handle.read(new Uint8Array(bytes.buffer), { at: 0 });
    bytes[0] = (bytes[0] ?? 0) + 1;
    handle.write(new Uint8Array(bytes.buffer), { at: 0 });
    handle.flush();
    return bytes[0];
  } finally {
    handle.close();
  }
}

/** Bumps and returns the database's fence epoch in IndexedDB, in one readwrite transaction. */
function bumpIdbEpoch(idb: IDBDatabase): Promise<number> {
  return new Promise((resolve, reject) => {
    const transaction = idb.transaction("file", "readwrite", { durability: "strict" });
    const store = transaction.objectStore("file");
    let epoch = 0;
    const read = store.get("epoch");
    read.onsuccess = () => {
      epoch = (typeof read.result === "number" ? read.result : 0) + 1;
      store.put(epoch, "epoch");
    };
    transaction.oncomplete = () => {
      resolve(epoch);
    };
    transaction.onerror = () => {
      reject(transaction.error ?? new Error("IndexedDB epoch bump failed"));
    };
  });
}

const FILE_PREFIX = "minnow-next-standin-";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function openIdb(name: string): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(`${FILE_PREFIX}${name}`, 1);
    request.onupgradeneeded = () => request.result.createObjectStore("file");
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onerror = () => {
      reject(request.error ?? new Error("IndexedDB open failed"));
    };
  });
}

function idbLoad(idb: IDBDatabase): Promise<string | null> {
  return new Promise((resolve, reject) => {
    const request = idb.transaction("file").objectStore("file").get("state");
    request.onsuccess = () => {
      resolve(typeof request.result === "string" ? request.result : null);
    };
    request.onerror = () => {
      reject(request.error ?? new Error("IndexedDB read failed"));
    };
  });
}

function idbSave(idb: IDBDatabase, text: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const transaction = idb.transaction("file", "readwrite", { durability: "strict" });
    transaction.objectStore("file").put(text, "state");
    transaction.oncomplete = () => {
      resolve();
    };
    transaction.onerror = () => {
      reject(transaction.error ?? new Error("IndexedDB write failed"));
    };
  });
}

const factory: OwnerEngineFactory<Fence> = {
  async openFence(db) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- WebKit's pinned Linux build has no navigator.storage
    if (typeof navigator.storage?.getDirectory === "function") {
      const root = await navigator.storage.getDirectory();
      const file = await root.getFileHandle(`${FILE_PREFIX}${db}`, { create: true });
      const handle = await file.createSyncAccessHandle();
      // The state file's handle is the fence: the epoch is bumped only while holding it.
      return { kind: "opfs", db, handle, epoch: await bumpOpfsEpoch(root, db) };
    }
    const idb = await openIdb(db);
    return { kind: "idb", db, idb, epoch: await bumpIdbEpoch(idb) };
  },
  epochOf: (fence) => fence.epoch,
  closeFence(fence) {
    if (fence.kind === "opfs") fence.handle.close();
    else fence.idb.close();
  },
  async open(fence, settings) {
    const gate = { held: false, dieOnSave: false };
    const knobs = new BroadcastChannel(`minnow-next-test:${fence.db}`);
    knobs.onmessage = (event: MessageEvent) => {
      const message = event.data as { t: string; value?: boolean };
      if (message.t === "hold") gate.held = message.value === true;
      if (message.t === "dieOnSave") gate.dieOnSave = true;
    };
    let text: string | null;
    if (fence.kind === "opfs") {
      const size = fence.handle.getSize();
      const bytes = new Uint8Array(size);
      fence.handle.read(bytes, { at: 0 });
      text = size === 0 ? null : decoder.decode(bytes);
    } else text = await idbLoad(fence.idb);
    const die = () => {
      if (!gate.dieOnSave) return;
      // Durable, and the reply never leaves: a hung owner.
      for (;;) {
        // spin
      }
    };
    const writePath = new StandinWritePath({
      durability: settings.durability === "relaxed" ? "relaxed" : "strict",
      flushDelayMs: typeof settings.flushDelayMs === "number" ? settings.flushDelayMs : 0,
      flushHeld: () => gate.held,
      persistence: {
        load: () => text,
        save: (next) => {
          if (fence.kind === "opfs") {
            const bytes = encoder.encode(next);
            fence.handle.write(bytes, { at: 0 });
            fence.handle.truncate(bytes.length);
            fence.handle.flush();
            die();
            return;
          }
          return idbSave(fence.idb, next).then(die);
        },
      },
    });
    return composeEngine({
      planner: withTestStatements(createLowerer()),
      executor: new StandinExecutor(),
      writePath,
      governor: new Governor(64 * 1024 * 1024),
      spill: new MemorySpillService(),
      reads: {} as ChunkReads,
    });
  },
};

/** Counts this worker's Web Lock calls, for the latency lane (`{ t: "lockCalls", db }`). */
let lockCalls = 0;
const counted: WebLockManager = {
  request: (name, options, callback) => {
    lockCalls++;
    return navigator.locks.request(
      name,
      options as LockOptions,
      callback as LockGrantedCallback<unknown>,
    );
  },
  query: () => navigator.locks.query(),
};
const stats = new BroadcastChannel("minnow-next-test:stats");
stats.onmessage = (event: MessageEvent) => {
  if ((event.data as { t?: string }).t === "lockCalls?")
    stats.postMessage({ t: "lockCalls", count: lockCalls });
};

serveOwnerWorker(self as unknown as Parameters<typeof serveOwnerWorker>[0], factory, counted);
