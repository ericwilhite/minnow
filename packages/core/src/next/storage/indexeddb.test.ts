/**
 * The IndexedDB backend (indexeddb.ts) over the simulated IndexedDB (sim-idb.ts): the storage kit
 * (conformance + the physical crash sweep), the epoch fence, the byte-record layout, the spill
 * store, and the simulated database's own crash model. Real browsers run it in
 * browser-tests/next-storage.spec.ts.
 */
import { describe, expect, it } from "vitest";
import { OwnershipLostError, type StorageBackend } from "./contract.js";
import { runStorageKit } from "./kit.js";
import { createIndexedDbBackend } from "./indexeddb.js";
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
    expect(all).toEqual(image.subarray(0, 4 * 4096 + 50));
    const target = new Uint8Array(3000).fill(7);
    await session.readDataInto(4096 + 10, target);
    expect(target.every((byte) => byte === 0)).toBe(true);
    await session.releaseData([{ offset: 2 * 4096, length: 3 * 4096 }]);
    expect(await session.dataLength()).toBe(4096);
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
    await put(2, 22); // the same size as the lost record: it ends where old record 3 begins
    await simIndexedDb(disk).transaction(["meta"], "readwrite", "strict", () => undefined);
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
});
