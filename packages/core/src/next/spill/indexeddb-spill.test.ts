/** The spill service over the IndexedDB backend's `spill` store (storage/indexeddb.ts). */
import { describe, expect, it } from "vitest";
import { countingBudget, expectBytes } from "../storage/space-test-support.js";
import { OwnershipLostError, type StorageBackend } from "../storage/contract.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { ScratchSpillService } from "./scratch-spill.js";

const backendOver = (disk: SimDisk): StorageBackend =>
  createIndexedDbBackend({ database: () => simIndexedDb(disk), name: "db", locks: new SimLocks() });

const bytes = (length: number, seed: number) =>
  Uint8Array.from({ length }, (_, index) => (seed * 17 + index * 3) & 0xff);

describe("spill on the IndexedDB spill store", () => {
  it("serves the spill store under the fence and sweeps it", async () => {
    const disk = new SimDisk();
    const backend = backendOver(disk);
    const lease = await backend.acquire();
    const scratch = backend.scratch(lease);
    if (scratch === null) throw new Error("IndexedDB has a spill store");
    const service = new ScratchSpillService(scratch, { budget: countingBudget(Infinity) });
    await service.sweep();
    const file = await service.create(1, {
      allocate: () => false,
      free: (bytes: number) => void bytes,
    });
    const refs = [];
    for (let index = 0; index < 20; index++) refs.push(await file.append(bytes(9000, index)));
    for (const [index, ref] of refs.entries())
      expectBytes(await file.read(ref), bytes(9000, index), `page ${String(index)}`);
    await file.close();
    await service.sweep();
    await backend.acquire({ steal: true, timeoutMs: 2_000 });
    await expect(scratch.write(0, bytes(1, 1))).rejects.toThrow(OwnershipLostError);
  });
});
