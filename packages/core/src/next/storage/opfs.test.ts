/**
 * The OPFS backend's own code (opfs.ts) in Node: the storage kit (conformance + the physical
 * crash sweep, two wrong-store mutants included by the kit) over simulated sync handles, quota
 * mapping, epochs, and the Web Locks adapter. Real browsers run it in
 * browser-tests/next-storage.spec.ts.
 */
import { describe, expect, it } from "vitest";
import { runStorageKit } from "./kit.js";
import { OwnershipLostError, StoreQuotaError, type SyncFile } from "./contract.js";
import { createOpfsBackend, webLocks, type OpfsDirectoryLike, type WebLocksLike } from "./opfs.js";
import { SimDisk, SimLocks } from "./sim-disk.js";

/** An OPFS directory whose files are the simulated disk's (exclusive opens, crash model). */
function simDirectory(
  disk: SimDisk,
  wrap: (name: string, file: SyncFile) => SyncFile = (_, file) => file,
): OpfsDirectoryLike {
  return {
    getFileHandle: async (name) => ({
      createSyncAccessHandle: async () => wrap(name, disk.open(name, true)),
    }),
  };
}

const backendOver = (disk: SimDisk) =>
  createOpfsBackend({ directory: simDirectory(disk), name: "db", locks: new SimLocks() });

describe("OPFS backend over simulated handles", () => {
  it("passes the storage kit (conformance + physical durability sweep)", async () => {
    await runStorageKit(backendOver);
  });

  it("passes the kit with WebKit's busy error name", async () => {
    // WebKit reports a contended handle as InvalidStateError (bug 326135).
    const webkitDirectory = (disk: SimDisk): OpfsDirectoryLike => ({
      getFileHandle: async (name) => ({
        createSyncAccessHandle: async () => {
          try {
            return disk.open(name, true);
          } catch (error) {
            if (error instanceof Error && error.name === "NoModificationAllowedError")
              error.name = "InvalidStateError";
            throw error;
          }
        },
      }),
    });
    await runStorageKit((disk) =>
      createOpfsBackend({ directory: webkitDirectory(disk), name: "db", locks: new SimLocks() }),
    );
  });

  it("increases the epoch across acquisitions, and refuses a fenced owner's scratch", async () => {
    const disk = new SimDisk();
    const backend = backendOver(disk);
    const first = await backend.acquire();
    await first.release();
    const second = await backend.acquire({ steal: true });
    expect(second.epoch).toBe(first.epoch + 1);
    const scratch = backend.scratch(second);
    if (scratch === null) throw new Error("no scratch");
    await scratch.write(0, new Uint8Array([1, 2]));
    const third = await backend.acquire({ steal: true, timeoutMs: 2_000 });
    expect(third.epoch).toBe(second.epoch + 1);
    await expect(scratch.write(0, new Uint8Array([3]))).rejects.toThrow(OwnershipLostError);
    await third.release();
  });

  it("maps quota signals to StoreQuotaError by name, and a closed handle's error after loss", async () => {
    const failing = (name: string) => () => {
      const error = new Error(name);
      error.name = name;
      throw error;
    };
    const disk = new SimDisk();
    let mode: string | null = null;
    const backend = createOpfsBackend({
      name: "db",
      locks: new SimLocks(),
      directory: simDirectory(disk, (name, file) =>
        name === "data"
          ? {
              read: (buffer, options) => file.read(buffer, options),
              write: (buffer, options) =>
                mode === null ? file.write(buffer, options) : failing(mode)(),
              truncate: (size) =>
                mode === "QuotaExceededError" ? failing(mode)() : file.truncate(size),
              getSize: () => file.getSize(),
              flush: () => file.flush(),
              close: () => file.close(),
            }
          : file,
      ),
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    mode = "QuotaExceededError";
    await expect(session.writeData(0, new Uint8Array(1))).rejects.toThrow(StoreQuotaError);
    mode = "InvalidStateError";
    await expect(session.writeData(0, new Uint8Array(1))).rejects.toThrow(StoreQuotaError);
    mode = "NotReadableError";
    await expect(session.writeData(0, new Uint8Array(1))).rejects.toThrow("NotReadableError");
    await lease.release();
    mode = "InvalidStateError";
    await expect(session.writeData(0, new Uint8Array(1))).rejects.toThrow(OwnershipLostError);
  });
});

/** The Web Locks API as the platform defines it: FIFO, steal rejects the holder with AbortError. */
function fakeNavigatorLocks(): WebLocksLike {
  const queues = new Map<string, Array<() => void>>();
  const holders = new Map<string, { reject: (error: Error) => void } | null>();
  return {
    request(name, options, callback) {
      return new Promise<unknown>((resolve, reject) => {
        const run = () => {
          const holder = { reject };
          holders.set(name, holder);
          void callback(null).then((value) => {
            if (holders.get(name) !== holder) return;
            holders.set(name, null);
            queues.get(name)?.shift()?.();
            resolve(value);
          }, reject);
        };
        if (options.steal === true) {
          const error = new Error("stolen");
          error.name = "AbortError";
          holders.get(name)?.reject(error);
          run();
          return;
        }
        if (holders.get(name) == null) {
          run();
          return;
        }
        const queue = queues.get(name) ?? [];
        queues.set(name, queue);
        queue.push(run);
        options.signal?.addEventListener("abort", () => {
          queue.splice(queue.indexOf(run), 1);
          const error = new Error("aborted");
          error.name = "AbortError";
          reject(error);
        });
      });
    },
  };
}

describe("Web Locks adapter", () => {
  it("holds until release, queues, cancels a wait, and reports a steal as lost", async () => {
    const locks = webLocks(fakeNavigatorLocks());
    const first = await locks.request("db", {});
    const cancel = new AbortController();
    const waiting = locks.request("db", { signal: cancel.signal });
    cancel.abort();
    await expect(waiting).rejects.toThrow("aborted");
    const queued = locks.request("db", {});
    first.release();
    const second = await queued;
    expect(second.lost.aborted).toBe(false);
    const thief = await locks.request("db", { steal: true });
    await Promise.resolve();
    await Promise.resolve();
    expect(second.lost.aborted).toBe(true);
    expect(thief.lost.aborted).toBe(false);
    thief.release();
  });
});
