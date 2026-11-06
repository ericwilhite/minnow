/**
 * The IndexedDB backend (indexeddb.ts) over the simulated IndexedDB (sim-idb.ts): the storage kit
 * (conformance + the physical crash sweep), the epoch fence, the byte-record layout, the spill
 * store, and the simulated database's own crash model. Real browsers run it in
 * browser-tests/next-storage.spec.ts.
 */
import { describe, expect, it } from "vitest";
import { expectBytes } from "./space-test-support.js";
import {
  OwnershipLostError,
  StoreCorruptionError,
  type StorageBackend,
  type StoreSession,
} from "./contract.js";
import type { IdbDatabaseLike } from "./idb-like.js";
import { EXPECTED_IDB_OUTCOMES, runIdbScenarios } from "./idb-scenarios.js";
import { runStorageKit } from "./kit.js";
import { IDB_BATCH_BYTES, createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks, SimulatedCrash } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";

const backendOver = (disk: SimDisk, locks = new SimLocks()): StorageBackend =>
  createIndexedDbBackend({ database: () => simIndexedDb(disk), name: "db", locks });

const bytes = (length: number, seed: number) =>
  Uint8Array.from({ length }, (_, index) => (seed * 17 + index * 3) & 0xff);

describe("IndexedDB backend over the simulated IndexedDB", () => {
  it("passes the storage kit (conformance + physical durability sweep)", async () => {
    await runStorageKit((disk) => backendOver(disk));
  });

  it("fences a stale owner inside every transaction, whatever it staged", async () => {
    const disk = new SimDisk();
    const locks = new SimLocks();
    const backend = backendOver(disk, locks);
    const first = await backend.acquire();
    const session = await backend.open(first);
    await session.writeData(0, bytes(100, 1));
    await session.publish(new Uint8Array([1])); // queued writes reach the store with a barrier
    // A second owner in another tab: its own backend (and Web Lock view) over the same database.
    const other = backendOver(disk, locks);
    const second = await other.acquire({ steal: true });
    expect(second.epoch).toBe(first.epoch + 1);
    const fresh = await other.open(second);
    await fresh.publish(new Uint8Array([2]));
    for (const action of [
      () => session.writeData(0, bytes(10, 9)),
      () => session.publish(new Uint8Array([9])),
      () => session.appendLog(new Uint8Array([9])),
      () => session.flushLog(),
      () => session.releaseData([{ offset: 0, length: 4096 }]),
    ])
      await expect(action()).rejects.toThrow(OwnershipLostError);
    expect(await fresh.readRoot()).toEqual(new Uint8Array([2]));
    const [kept] = await fresh.readData([{ offset: 0, length: 100 }]);
    expect(kept).toEqual(bytes(100, 1));
  });

  it("refuses a stale owner that never heard of its lock loss (Safari drops locks on bfcache)", async () => {
    const disk = new SimDisk();
    // Separate lock managers: the first owner's lock is gone without an AbortError reaching it.
    const first = backendOver(disk, new SimLocks());
    const stale = await first.open(await first.acquire());
    await stale.publish(new Uint8Array([1]));
    const second = backendOver(disk, new SimLocks());
    const owner = await second.open(await second.acquire());
    for (const action of [
      () => stale.publish(new Uint8Array([9])),
      () => stale.writeData(0, bytes(10, 9)),
      () => stale.appendLog(new Uint8Array([9])),
    ])
      await expect(action()).rejects.toThrow(OwnershipLostError);
    expect(await owner.readRoot()).toEqual(new Uint8Array([1]));
    expect(owner.logLength()).toBe(0);
  });

  it("keeps byte records disjoint: overwrites, straddling releases and gaps read back exactly", async () => {
    const backend = backendOver(new SimDisk());
    const session = await backend.open(await backend.acquire());
    const image = new Uint8Array(5 * 4096);
    const write = async (offset: number, data: Uint8Array) => {
      await session.writeData(offset, data);
      image.set(data, offset);
    };
    await write(0, bytes(3 * 4096 + 100, 1));
    await write(4096, bytes(500, 2)); // inside the first record
    await write(2 * 4096, bytes(2 * 4096 + 50, 3)); // straddles its end
    await session.releaseData([{ offset: 4096, length: 4096 }]);
    image.fill(0, 4096, 2 * 4096);
    expect(await session.dataLength()).toBe(4 * 4096 + 50);
    const [all] = await session.readData([{ offset: 0, length: 4 * 4096 + 50 }]);
    expectBytes(all, image.subarray(0, 4 * 4096 + 50));
    const target = new Uint8Array(3000).fill(7);
    await session.readDataInto(4096 + 10, target);
    expect(target.every((byte) => byte === 0)).toBe(true);
    await session.releaseData([{ offset: 2 * 4096, length: 3 * 4096 }]);
    expect(await session.dataLength()).toBe(4096);
  });

  it("queues a transferred whole buffer as it is and copies everything else (p2e)", async () => {
    const backend = backendOver(new SimDisk());
    const session = await backend.open(await backend.acquire());
    expect(session.keepsWrites).toBe(true);
    // Writes are queued (batched) until the publish. A caller that changes its buffer after the
    // call resolves changes nothing unless it transferred the buffer (which it promised not to do:
    // the change here only shows that no copy was made).
    const copied = bytes(4096, 1);
    const transferred = bytes(4096, 2);
    const view = bytes(2 * 4096, 3).subarray(4096); // not a whole buffer: copied even if offered
    await session.writeData(0, copied);
    await session.writeData(4096, transferred, { transfer: true });
    await session.writeData(2 * 4096, view, { transfer: true });
    copied.fill(9);
    transferred.fill(9);
    view.fill(9);
    await session.publish(new Uint8Array([1]));
    const [first, second, third] = await session.readData([
      { offset: 0, length: 4096 },
      { offset: 4096, length: 4096 },
      { offset: 2 * 4096, length: 4096 },
    ]);
    expectBytes(first, bytes(4096, 1), "copied");
    expectBytes(second, new Uint8Array(4096).fill(9), "transferred");
    expectBytes(third, bytes(2 * 4096, 3).subarray(4096), "view");
  });
});

describe("simulated IndexedDB", () => {
  it("keeps a prefix of committed transactions across a crash: strict ones always", async () => {
    for (let step = 0; step < 40; step++)
      for (let seed = 1; seed <= 6; seed++) {
        const disk = new SimDisk({ crashAtStep: step, seed: seed * 101 + step });
        const committed: Array<{ key: number; strict: boolean }> = [];
        try {
          const db = simIndexedDb(disk);
          for (let key = 1; key <= 12; key++) {
            const strict = key % 3 === 0;
            await db.transaction(["meta"], "readwrite", strict ? "strict" : "relaxed", (tx) => {
              tx.store("meta").put(key, new Uint8Array(200).fill(key).buffer);
            });
            committed.push({ key, strict });
          }
          disk.powerLoss();
        } catch (error) {
          if (!(error instanceof SimulatedCrash)) throw error;
        }
        const keys: number[] = [];
        await simIndexedDb(disk).transaction(["meta"], "readonly", "relaxed", (tx) => {
          tx.store("meta").getAll({}, (values) => {
            for (const value of values as ArrayBuffer[]) keys.push(new Uint8Array(value)[0] ?? 0);
          });
        });
        const label = `step ${String(step)} seed ${String(seed)}`;
        // A prefix: 1, 2, …, n with nothing missing in between.
        expect(keys, label).toEqual(keys.map((_, index) => index + 1));
        const lastStrict = committed.filter((entry) => entry.strict).at(-1)?.key ?? 0;
        expect(keys.length, label).toBeGreaterThanOrEqual(lastStrict);
      }
  });

  it("never replays a dead process's record after a newer process's record at its place", async () => {
    const disk = new SimDisk();
    const put = (key: number, value: number, durability: "strict" | "relaxed" = "relaxed") =>
      simIndexedDb(disk).transaction(["meta"], "readwrite", durability, (tx) => {
        tx.store("meta").put(key, new Uint8Array(64).fill(value).buffer);
      });
    await put(1, 1);
    await put(2, 2);
    await put(3, 3, "strict");
    disk.powerLoss();
    // Record 2 is lost and record 3 survives (a reordered write-back), so a new process keeps 1.
    const wal = disk.files.get("idb-wal");
    if (wal === undefined) throw new Error("no log");
    const recordLength = wal.current.length / 3;
    wal.current = wal.current.slice();
    wal.current.fill(0, recordLength, 2 * recordLength);
    wal.durable = wal.current;
    disk.powerLoss();
    // The same size as the lost record: it ends where old record 3 begins. Strict: flushed.
    await put(2, 22, "strict");
    disk.powerLoss();
    const found: number[] = [];
    await simIndexedDb(disk).transaction(["meta"], "readonly", "relaxed", (tx) => {
      tx.store("meta").getAll({}, (values) => {
        for (const value of values as ArrayBuffer[]) found.push(new Uint8Array(value)[0] ?? 0);
      });
    });
    expect(found).toEqual([1, 22]);
  });

  it("aborts a transaction whole: nothing it put applies", async () => {
    const db = simIndexedDb(new SimDisk());
    await expect(
      db.transaction(["meta"], "readwrite", "strict", (tx) => {
        tx.store("meta").put("a", 1);
        tx.store("meta").get("a", () => {
          tx.abort(new Error("changed my mind"));
        });
      }),
    ).rejects.toThrow("changed my mind");
    let value: unknown = "unset";
    await db.transaction(["meta"], "readonly", "relaxed", (tx) => {
      tx.store("meta").get("a", (found) => {
        value = found;
      });
    });
    expect(value).toBeUndefined();
  });

  it("matches IndexedDB's semantics on the pinned scenarios (order, clones, activity, ranges)", async () => {
    expect(await runIdbScenarios(simIndexedDb(new SimDisk()))).toEqual(EXPECTED_IDB_OUTCOMES);
  });

  it("does not flush a strict transaction that changed nothing", async () => {
    const disk = new SimDisk();
    const db = simIndexedDb(disk);
    const before = disk.steps;
    await db.transaction(["meta"], "readwrite", "strict", (tx) => {
      tx.store("meta").get("nothing", () => undefined);
    });
    expect(disk.steps).toBe(before);
  });
});

/** Each finding of review B1-p2c, kept as tests. */
describe("review B1-p2c: IndexedDB epochs, fencing, batching and empty ranges", () => {
  const setEpoch = (disk: SimDisk, value: unknown) =>
    simIndexedDb(disk).transaction(["meta"], "readwrite", "strict", (tx) => {
      tx.store("meta").put("epoch", value);
    });

  it("1: a missing epoch starts at 1; malformed, unsafe or exhausted ones refuse ownership", async () => {
    expect((await backendOver(new SimDisk()).acquire()).epoch).toBe(1);
    for (const value of [Number.MAX_SAFE_INTEGER, 2 ** 53, "7", -1, 0, 1.5, Number.NaN, null]) {
      const disk = new SimDisk();
      await setEpoch(disk, value);
      await expect(backendOver(disk).acquire({ timeoutMs: 200 }), String(value)).rejects.toThrow(
        StoreCorruptionError,
      );
    }
  });

  it("1: refuses an unaware stale owner on every mutation path", async () => {
    type Path = (
      session: StoreSession,
      backend: StorageBackend,
      lease: Awaited<ReturnType<StorageBackend["acquire"]>>,
    ) => Promise<unknown>;
    const barrier = (session: StoreSession) => session.flushLog();
    const paths: Record<string, Path> = {
      publish: (session) => session.publish(new Uint8Array([9])),
      "writeData then a barrier": async (session) => {
        await session.writeData(0, bytes(10, 9));
        await barrier(session);
      },
      "appendLog then a barrier": async (session) => {
        await session.appendLog(new Uint8Array([9]));
        await barrier(session);
      },
      "writeLogHeader then a barrier": async (session) => {
        await session.writeLogHeader(0, new Uint8Array([9]));
        await barrier(session);
      },
      flushLog: (session) => session.flushLog(),
      releaseData: (session) => session.releaseData([{ offset: 0, length: 4096 }]),
      truncateLog: (session) => session.truncateLog(0),
      resetLog: (session) => session.resetLog(),
      "scratch write": (_session, backend, lease) =>
        backend.scratch(lease)?.write(0, bytes(10, 9)) ?? Promise.resolve(),
      "scratch release": (_session, backend, lease) =>
        backend.scratch(lease)?.release([{ offset: 0, length: 4096 }]) ?? Promise.resolve(),
      "scratch truncate": (_session, backend, lease) =>
        backend.scratch(lease)?.truncate(0) ?? Promise.resolve(),
    };
    for (const [name, path] of Object.entries(paths)) {
      const disk = new SimDisk();
      // Separate lock managers: the stale owner never hears that its lock is gone.
      const staleBackend = backendOver(disk, new SimLocks());
      const staleLease = await staleBackend.acquire();
      const stale = await staleBackend.open(staleLease);
      await stale.writeData(0, bytes(100, 1));
      await stale.appendLog(new Uint8Array([1, 2]));
      await stale.publish(new Uint8Array([1]));
      await stale.flushLog();
      const ownerBackend = backendOver(disk, new SimLocks());
      const owner = await ownerBackend.open(await ownerBackend.acquire());
      await expect(path(stale, staleBackend, staleLease), name).rejects.toThrow(OwnershipLostError);
      expect(await owner.readRoot(), name).toEqual(new Uint8Array([1]));
      expect((await owner.readData([{ offset: 0, length: 100 }]))[0], name).toEqual(bytes(100, 1));
      expect(await owner.readLog(0, 2), name).toEqual(new Uint8Array([1, 2]));
    }
  });

  it("6: commits a burst of writes in one transaction, bounds a batch, and keeps call order", async () => {
    const disk = new SimDisk();
    let writes = 0;
    const counted = (): IdbDatabaseLike => {
      const inner = simIndexedDb(disk);
      return {
        transaction: (names, mode, durability, body) => {
          if (mode === "readwrite") writes += 1;
          return inner.transaction(names, mode, durability, body);
        },
        close: () => undefined,
      };
    };
    const backend = createIndexedDbBackend({
      database: counted,
      name: "db",
      locks: new SimLocks(),
    });
    const session = await backend.open(await backend.acquire());
    writes = 0;
    const image = new Uint8Array(64 * 4096);
    for (let index = 0; index < 100; index++) {
      const offset = (index % 50) * 4096;
      const data = bytes(3000 + index, index);
      await session.writeData(offset, data);
      image.set(data, offset);
      await session.appendLog(new Uint8Array([index]));
    }
    await session.flushLog();
    expect(writes).toBe(1); // 200 queued writes and the barrier: one strict transaction
    const length = await session.dataLength();
    const [read] = await session.readData([{ offset: 0, length }]);
    expectBytes(read, image.subarray(0, length));
    expect(await session.readLog(0, 100)).toEqual(Uint8Array.from({ length: 100 }, (_, i) => i));
    writes = 0;
    for (let index = 0; index < 6; index++)
      await session.writeData(index << 20, bytes(1 << 20, index));
    expect(writes).toBeGreaterThanOrEqual(1); // the 4 MiB bound committed without a barrier
    expect(IDB_BATCH_BYTES).toBe(4 << 20);
  });

  it("4: flushLog makes appends durable even when their batch already committed (the flush marker)", async () => {
    for (let seed = 1; seed <= 24; seed++) {
      const disk = new SimDisk({ seed });
      const backend = backendOver(disk);
      const session = await backend.open(await backend.acquire());
      await session.appendLog(new Uint8Array([1, 2, 3]));
      // Let the queued append commit on its own (relaxed: written, not flushed).
      await new Promise((resolve) => setTimeout(resolve, 5));
      await session.flushLog();
      disk.powerLoss();
      const reopened = backendOver(disk);
      const after = await reopened.open(await reopened.acquire());
      expect(after.logLength(), `seed ${String(seed)}`).toBeGreaterThanOrEqual(3);
    }
  });

  it("5: empty ranges never reach IndexedDB (the real one refuses them)", async () => {
    const backend = backendOver(new SimDisk());
    const session = await backend.open(await backend.acquire());
    await session.writeData(0, new Uint8Array(0));
    await session.writeData(4096, bytes(10, 1));
    await session.releaseData([{ offset: 0, length: 0 }]);
    expect((await session.readData([{ offset: 4096, length: 0 }]))[0]).toEqual(new Uint8Array(0));
    expect(await session.readLog(0, 0)).toEqual(new Uint8Array(0));
    await session.appendLog(new Uint8Array(0));
    await session.truncateLog(session.logLength());
    await session.publish(new Uint8Array([1]));
    const scratch = backend.scratch(await backend.acquire({ steal: true }));
    void scratch;
  });
});

describe("release drains the connection, then closes it (the WebKit wedge mitigation)", () => {
  it("lets in-flight transactions finish, closes, refuses new ones, and a new lease reopens", async () => {
    const inner = simIndexedDb(new SimDisk());
    const events: string[] = [];
    let connections = 0;
    const backend = createIndexedDbBackend({
      database: () => {
        connections += 1;
        return {
          transaction: async (stores, mode, durability, body) => {
            await inner.transaction(stores, mode, durability, body);
            // A transaction that settles a moment later, as a real one in flight would.
            await new Promise<void>((resolve) => {
              setTimeout(resolve, 20);
            });
            events.push(`settled ${mode}`);
          },
          close: () => {
            events.push("closed");
          },
        };
      },
      name: "drain",
      locks: new SimLocks(),
    });
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    const publishing = session.publish(new Uint8Array([1]));
    // One task later the publish's transaction is in flight (it settles 20 ms after committing).
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 0);
    });
    events.length = 0;
    await Promise.all([publishing, lease.release()]);
    // Every transaction that was in flight settled before the connection closed.
    expect(events.at(-1)).toBe("closed");
    expect(events.filter((event) => event === "closed")).toHaveLength(1);
    await expect(session.readRoot()).rejects.toThrow();
    // The next owner opens a new connection and sees what was published.
    const next = await backend.open(await backend.acquire());
    expect(connections).toBe(2);
    expect(await next.readRoot()).toEqual(new Uint8Array([1]));
  });
});
