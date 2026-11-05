import { describe, expect, it } from "vitest";
import { SpillCorruptionError } from "./contract.js";
import { MemorySpillFile, MemorySpillService } from "./memory-spill.js";

describe("memory spill service", () => {
  it("appends, verifies, accounts and deletes spill pages", async () => {
    const service = new MemorySpillService();
    const file = await service.create(1);
    const first = await file.append(new Uint8Array([1, 2, 3]));
    const second = await file.append(new Uint8Array([4, 5]));
    expect(second.offset).toBe(3);
    expect(service.bytesInUse).toBe(5);
    expect([...(await file.read(first))]).toEqual([1, 2, 3]);
    (file as MemorySpillFile).corrupt(second, 1);
    await expect(file.read(second)).rejects.toThrow(SpillCorruptionError);
    await file.close();
    await file.close();
    expect(service.bytesInUse).toBe(0);
    await expect(file.append(new Uint8Array(1))).rejects.toThrow(TypeError);
  });

  it("sweeps every file", async () => {
    const service = new MemorySpillService();
    await (await service.create(1)).append(new Uint8Array(10));
    await (await service.create(2)).append(new Uint8Array(20));
    await service.sweep();
    expect(service.bytesInUse).toBe(0);
  });
});
