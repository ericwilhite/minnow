/** Real native owner composition over platform shims. Snapshot calls stay on ProtocolClient. */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { SimLocks } from "../storage/sim-disk.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { openIndexedDb } from "../storage/idb-like.js";
import { StoreQuotaError, type StorageBackend } from "../storage/contract.js";
import { Governor } from "../memory/governor.js";
import { openNativeOwnerEngine } from "../owner/native.js";
import { createLowerer } from "../lower/create.js";
import { OwnerServer } from "../owner/server.js";
import { ProtocolClient } from "../owner/client.js";
import type { WritePathImpl } from "../write/engine.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;
export type SnapshotBackend = "OPFS" | "IndexedDB";
export async function snapshotOwner(kind: SnapshotBackend, name: string) {
  const locks = new SimLocks(),
    disk = new MemoryOpfs(),
    factory = new IDBFactory();
  const real =
    kind === "OPFS"
      ? createOpfsBackend({ name, directory: disk.root, locks })
      : createIndexedDbBackend({ name, locks, database: () => openIndexedDb(factory, name) });
  let failSpool = false,
    failRelease = false,
    releaseFailed = false,
    cleanupSweeps = 0,
    fatalEvents = 0;
  let flushGate: { after: number; entered(): void; waiting: Promise<void> } | undefined;
  let activePath: (() => WritePathImpl) | undefined;
  const backend: StorageBackend = {
    fence: real.fence,
    acquire: (options) => real.acquire(options),
    async open(lease) {
      const session = await real.open(lease);
      return new Proxy(session, {
        get(target, key) {
          if (key === "flushLog")
            return async () => {
              if (flushGate !== undefined && (activePath?.().appliedLsn ?? 0) > flushGate.after) {
                const gate = flushGate;
                flushGate = undefined;
                gate.entered();
                await gate.waiting;
              }
              await target.flushLog();
            };
          const value: unknown = Reflect.get(target, key, target);
          return typeof value === "function" ? (value.bind(target) as unknown) : value;
        },
      });
    },
    scratch(lease) {
      const scratch = real.scratch(lease);
      if (scratch === null) return null;
      return new Proxy(scratch, {
        get(target, key) {
          if (key === "release")
            return async (ranges: Parameters<typeof target.release>[0]) => {
              if (failRelease) {
                failRelease = false;
                releaseFailed = true;
                throw new StoreQuotaError("injected source reclaim quota");
              }
              await target.release(ranges);
            };
          if (key === "truncate")
            return async (size: number) => {
              await target.truncate(size);
              if (releaseFailed) cleanupSweeps++;
            };
          if (key === "write")
            return async (at: number, bytes: Uint8Array) => {
              if (failSpool) {
                failSpool = false;
                throw new StoreQuotaError("injected owner spool quota");
              }
              await target.write(at, bytes);
            };
          const value: unknown = Reflect.get(target, key);
          return typeof value === "function" ? (value.bind(target) as unknown) : value;
        },
      });
    },
  };
  const start = async () => {
    const governor = new Governor(64 << 20),
      lease = await backend.acquire();
    const engine = await openNativeOwnerEngine({
      backend,
      lease,
      governor,
      planner: createLowerer(),
    });
    activePath = () => (engine as unknown as { readonly writePath: WritePathImpl }).writePath;
    const server = new OwnerServer(engine, {
      ownerId: `${name}-${String(lease.epoch)}`,
      epoch: lease.epoch,
      build: "snapshot-api",
      onFatal: () => {
        fatalEvents++;
      },
    });
    const ports: MessagePort[] = [],
      clients: ProtocolClient[] = [];
    let stopped = false;
    const port = (loseDone = false) => {
      const pair = new MessageChannel();
      ports.push(pair.port1, pair.port2);
      server.accept(
        loseDone
          ? {
              get onmessage() {
                return pair.port1.onmessage;
              },
              set onmessage(value) {
                pair.port1.onmessage = value;
              },
              close: () => pair.port1.close(),
              postMessage(message, transfer) {
                if ((message as { t?: string }).t !== "done")
                  pair.port1.postMessage(message, transfer);
              },
            }
          : pair.port1,
      );
      return pair.port2;
    };
    return {
      governor,
      engine,
      server,
      // Setup/inspection only: publication must be real, not a log-only flush.
      path: engine as unknown as { readonly writePath: WritePathImpl },
      async connect(loseDone = false, clientId?: string) {
        const client = new ProtocolClient(clientId === undefined ? {} : { clientId });
        clients.push(client);
        await client.connect(port(loseDone));
        return client;
      },
      async reconnect(client: ProtocolClient) {
        await client.reconnect(port());
      },
      async close() {
        if (stopped) return;
        stopped = true;
        for (const client of clients) client.close();
        const draining = server.quiesce("closing");
        try {
          await draining.drained;
          draining.finish();
          await engine.close();
        } finally {
          for (const p of ports) p.close();
          await lease.release();
        }
        expect(governor.usedBytes).toBe(0);
      },
    };
  };
  let current = await start();
  return {
    get current() {
      return current;
    },
    failReclaim() {
      failRelease = true;
    },
    get cleanupSweeps() {
      return cleanupSweeps;
    },
    get fatalEvents() {
      return fatalEvents;
    },
    failSpool() {
      failSpool = true;
    },
    blockFlush() {
      let entered!: () => void, release!: () => void;
      const waiting = new Promise<void>((resolve) => {
        release = resolve;
      });
      const reached = new Promise<void>((resolve) => {
        entered = resolve;
      });
      flushGate = { after: current.path.writePath.appliedLsn, entered, waiting };
      return { reached, release };
    },
    async restart() {
      await current.close();
      current = await start();
      return current;
    },
    async close() {
      await current.close();
    },
  };
}
export async function ownerCatalog(client: ProtocolClient): Promise<unknown> {
  return (await client.send({ t: "admin", op: "catalog", args: [] }).promise).value;
}

/** Real executor function calls are outside this seam; verify captured sequence state through
 * native SequenceAccess, not the stand-in executor used by owner sequence SQL suites. */
export async function nativeNext(
  owner: Awaited<ReturnType<typeof snapshotOwner>>,
): Promise<bigint | undefined> {
  const values = new BigInt64Array(1);
  await owner.current.path.writePath.sequences.next(1, 1, { repr: "int64", values }, 0);
  return values[0];
}
