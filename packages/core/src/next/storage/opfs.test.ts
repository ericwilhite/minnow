/**
 * The OPFS backend's own code (opfs.ts) in Node: the storage kit (conformance + the physical
 * crash sweep, two wrong-store mutants included by the kit) over simulated sync handles, quota
 * mapping, epochs, and the Web Locks adapter. Real browsers run it in
 * browser-tests/next-storage.spec.ts.
 */
import { describe, expect, it } from "vitest";
import { runStorageKit } from "./kit.js";
import {
  OwnershipLostError,
  StoreCorruptionError,
  StoreQuotaError,
  type SyncFile,
} from "./contract.js";
import {
  EPOCH_SLOT_BYTES,
  createOpfsBackend,
  encodeEpochSlot,
  webLocks,
  type OpfsDirectoryLike,
  type WebLocksLike,
} from "./opfs.js";
import { SimDisk, SimLocks, SimulatedCrash } from "./sim-disk.js";

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

/** Each probe of review B1-p2b (9ae988c), kept as a test. */
describe("review B1-p2b: acquisition cleanup and the epoch counter", () => {
  const named = (name: string) => Object.assign(new Error(name), { name });
  /** Wraps one file's handle; `fail` decides whether its writes throw `QuotaExceededError`. */
  const failingEpoch = (disk: SimDisk, fail: () => boolean, closeThrows = false) =>
    createOpfsBackend({
      name: "db",
      locks: new SimLocks(),
      directory: simDirectory(disk, (name, file) =>
        name === "epoch"
          ? {
              read: (buffer, options) => file.read(buffer, options),
              write: (buffer, options) => {
                if (fail()) throw named("QuotaExceededError");
                return file.write(buffer, options);
              },
              truncate: (size) => file.truncate(size),
              getSize: () => file.getSize(),
              flush: () => file.flush(),
              close: () => {
                file.close();
                if (closeThrows) throw named("InvalidStateError");
              },
            }
          : file,
      ),
    });
  const openHandles = (disk: SimDisk) =>
    [...disk.files.values()].reduce((count, state) => count + state.handles.size, 0);

  it("1: an epoch write refused for quota is StoreQuotaError, leaks no handle, and a retry works", async () => {
    const disk = new SimDisk();
    let full = true;
    const backend = failingEpoch(disk, () => full, true);
    await expect(backend.acquire({ timeoutMs: 200 })).rejects.toThrow(StoreQuotaError);
    expect(openHandles(disk)).toBe(0);
    full = false;
    const lease = await backend.acquire({ timeoutMs: 200 });
    expect(lease.epoch).toBe(1);
    await lease.release();
  });

  it("2: an epoch handed to an owner never repeats after a power loss (every seed)", async () => {
    for (let seed = 1; seed <= 40; seed++) {
      const disk = new SimDisk({ seed });
      const first = await backendOver(disk).acquire();
      disk.powerLoss();
      const second = await backendOver(disk).acquire();
      expect(second.epoch, `seed ${String(seed)}`).toBeGreaterThan(first.epoch);
      await second.release();
    }
  });

  it("2: a crash at any step of acquisition never repeats an epoch an owner held", async () => {
    const run = async (disk: SimDisk, held: number[]) => {
      for (let round = 0; round < 4; round++) {
        const lease = await backendOver(disk).acquire();
        held.push(lease.epoch);
        await lease.release();
      }
    };
    const clean = new SimDisk();
    await run(clean, []);
    for (let step = 0; step <= clean.steps; step++)
      for (let seed = 1; seed <= 8; seed++) {
        const disk = new SimDisk({ crashAtStep: step, seed: seed * 977 + step });
        const held: number[] = [];
        try {
          await run(disk, held);
          disk.powerLoss();
        } catch (error) {
          if (!(error instanceof SimulatedCrash)) throw error;
        }
        const next = await backendOver(disk).acquire();
        expect(next.epoch, `step ${String(step)} seed ${String(seed)}`).toBeGreaterThan(
          Math.max(0, ...held),
        );
        await next.release();
      }
  });

  it("2: refuses an exhausted counter and a malformed one in a used store; a short read is no reset", async () => {
    const exhausted = new SimDisk();
    const epochFile = exhausted.open("epoch", true);
    epochFile.write(encodeEpochSlot(Number.MAX_SAFE_INTEGER), { at: 0 });
    epochFile.close();
    await expect(backendOver(exhausted).acquire({ timeoutMs: 200 })).rejects.toThrow(/exhausted/u);

    const malformed = new SimDisk();
    const used = await backendOver(malformed).acquire();
    await used.release();
    const garbage = malformed.open("data", true);
    garbage.write(new Uint8Array([1]), { at: 0 });
    garbage.close();
    const file = malformed.open("epoch", true);
    file.write(new Uint8Array(EPOCH_SLOT_BYTES + 16).fill(0x5a), { at: 0 });
    file.close();
    await expect(backendOver(malformed).acquire({ timeoutMs: 200 })).rejects.toThrow(
      StoreCorruptionError,
    );

    // A handle that returns at most three bytes per read still reads the whole counter.
    const short = new SimDisk();
    const trickle = () =>
      createOpfsBackend({
        name: "db",
        locks: new SimLocks(),
        directory: simDirectory(short, (_name, handle) => ({
          read: (buffer, options) => handle.read(buffer.subarray(0, 3), options),
          write: (buffer, options) => handle.write(buffer, options),
          truncate: (size) => handle.truncate(size),
          getSize: () => handle.getSize(),
          flush: () => handle.flush(),
          close: () => handle.close(),
        })),
      });
    for (let expected = 1; expected <= 4; expected++) {
      const lease = await trickle().acquire();
      expect(lease.epoch).toBe(expected);
      await lease.release();
    }
  });
});
