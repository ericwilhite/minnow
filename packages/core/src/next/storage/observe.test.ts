/**
 * The backends' platform-request observer (`StorageObserver`, review B1-p2g finding 2): the kill
 * campaign counts a kill as inside storage only with a request open, so the observer must report a
 * request exactly while it exists. IndexedDB: from the moment the transaction is created (its body
 * runs), never while it waits on the connection's chain, until it completes or aborts. OPFS: around
 * every sync access handle call the session makes.
 */
import { describe, expect, it } from "vitest";
import type { PlatformRequest, StorageObserver } from "./contract.js";
import type { IdbDatabaseLike } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { createOpfsBackend, type OpfsDirectoryLike } from "./opfs.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

/** Records every request and which are open. */
function recorder() {
  const open = new Map<number, PlatformRequest>();
  const log: string[] = [];
  let next = 0;
  const observe: StorageObserver = (request) => {
    const id = (next += 1);
    open.set(id, request);
    log.push(`begin ${request.target}:${request.op}`);
    return () => {
      open.delete(id);
      log.push(`end ${request.target}:${request.op}`);
    };
  };
  return { observe, open, log };
}

describe("IndexedDB reports a transaction from its creation to its end", () => {
  it("a publish is one strict transaction over the roots, open while it runs", async () => {
    const { observe, open, log } = recorder();
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(new SimDisk()),
      locks: new SimLocks(),
      name: "observe",
      observe,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    log.length = 0;
    await session.publish(new Uint8Array([1]));
    expect(open.size).toBe(0);
    expect(log).toEqual(["begin roots:readwrite/strict", "end roots:readwrite/strict"]);
    await lease.release();
  });

  it("a transaction queued on the chain is not reported until it is created", async () => {
    const inner = simIndexedDb(new SimDisk());
    let hold: (() => void) | null = null;
    let holding = false;
    const database: IdbDatabaseLike = {
      async transaction(stores, mode, durability, body) {
        await inner.transaction(stores, mode, durability, body);
        if (holding) {
          holding = false;
          await new Promise<void>((resolve) => {
            hold = resolve;
          });
        }
      },
      close: () => undefined,
    };
    const { observe, open } = recorder();
    const backend = createIndexedDbBackend({
      database: () => database,
      locks: new SimLocks(),
      name: "queued",
      observe,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    holding = true;
    const publishing = session.publish(new Uint8Array([1]));
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 10);
    });
    // The spill's first write loads its record index: a transaction queued on the connection's
    // chain behind the publish, whose end is held.
    const scratch = backend.scratch(lease);
    if (scratch === null) throw new Error("no spill store");
    const spilling = scratch.write(0, new Uint8Array(8));
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 10);
    });
    expect([...open.values()].map((request) => request.target)).toEqual(["roots"]);
    (hold as (() => void) | null)?.();
    await Promise.all([publishing, spilling]);
    expect(open.size).toBe(0);
    await lease.release();
  });

  it("an aborted transaction is reported ended too", async () => {
    const { observe, open, log } = recorder();
    const disk = new SimDisk();
    const backend = createIndexedDbBackend({
      database: () => simIndexedDb(disk),
      locks: new SimLocks(),
      name: "aborted",
      observe,
    });
    const first = await backend.acquire();
    const session = await backend.open(first);
    // A successor moves the epoch: the old owner's next write aborts inside its transaction.
    const second = await backend.acquire({ steal: true });
    log.length = 0;
    await expect(session.publish(new Uint8Array([1]))).rejects.toThrow();
    expect(open.size).toBe(0);
    expect(log.filter((line) => line.startsWith("begin")).length).toBe(
      log.filter((line) => line.startsWith("end")).length,
    );
    await first.release();
    await second.release();
  });
});

describe("OPFS reports every sync call the session makes", () => {
  it("a publish writes and flushes the data and a root file, each call open while it runs", async () => {
    const disk = new SimDisk();
    const directory: OpfsDirectoryLike = {
      getFileHandle: async (name) => ({
        createSyncAccessHandle: async () => disk.open(name, true),
      }),
    };
    const { observe, open, log } = recorder();
    const backend = createOpfsBackend({
      directory,
      name: "observe",
      locks: new SimLocks(),
      observe,
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    log.length = 0;
    await session.writeData(0, new Uint8Array(4096).fill(1));
    await session.publish(new Uint8Array([1]));
    expect(open.size).toBe(0);
    const targets = new Set(log.map((line) => line.split(" ")[1]));
    expect(targets).toContain("data:write");
    expect(targets).toContain("data:flush");
    expect([...targets].some((target) => target?.startsWith("root-") === true)).toBe(true);
    // Every begin is followed by its end before the next begins (synchronous calls).
    log.forEach((line, index) => {
      if (index % 2 === 0) expect(line.startsWith("begin")).toBe(true);
      else expect(line).toBe(log[index - 1]?.replace("begin", "end"));
    });
    await lease.release();
  });
});
