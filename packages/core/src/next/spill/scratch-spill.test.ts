import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import {
  OwnershipLostError,
  StoreQuotaError,
  type DataRange,
  type ScratchSpace,
} from "../storage/contract.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import { SimBackend, SimDisk } from "../storage/sim-disk.js";
import { sameBytes as same } from "../storage/space-test-support.js";
import { SpillCorruptionError, SpillQuotaError, type SpillPageRef } from "./contract.js";
import { SPILL_BATCH_BYTES, ScratchSpillService } from "./scratch-spill.js";

const page = (length: number, seed: number) =>
  Uint8Array.from({ length }, (_, index) => (seed * 31 + index * 7) & 0xff);

function budget(limit: number) {
  const state = {
    used: 0,
    allocate(bytes: number) {
      if (state.used + bytes > limit) return false;
      state.used += bytes;
      return true;
    },
    free(bytes: number) {
      state.used -= bytes;
    },
  };
  return state;
}

async function scratchOver(disk = new SimDisk()) {
  const backend = new SimBackend(disk);
  const lease = await backend.acquire();
  const scratch = backend.scratch(lease);
  if (scratch === null) throw new Error("no scratch");
  return { backend, lease, scratch, disk };
}

describe("scratch spill service", () => {
  it("batches pages, serves reads from the batch and from the scratch space, and frees on close", async () => {
    // An in-memory file: the simulated disk copies its whole image on every write.
    const file_ = new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null });
    const scratch = new FileScratch(file_, () => undefined);
    const service = new ScratchSpillService(scratch);
    const charge = budget(1 << 20);
    const file = await service.create(1, charge);
    expect(charge.used).toBe(SPILL_BATCH_BYTES);
    const refs: SpillPageRef[] = [];
    for (let index = 0; index < 40; index++) refs.push(await file.append(page(20_000, index)));
    // 40 × 20 KB crossed the 256 KiB batch several times; every page reads back.
    for (const [index, ref] of refs.entries())
      expect(same(await file.read(ref), page(20_000, index)), `page ${String(index)}`).toBe(true);
    const target = new Uint8Array(20_000);
    const last = refs.at(-1);
    if (last === undefined) throw new Error("no refs");
    await file.readInto(last, target);
    expect(target).toEqual(page(20_000, 39));
    // Batches are written as consecutive extents: a run reads back sequentially.
    const offsets = refs.map((ref) => ref.offset);
    expect(offsets).toEqual([...offsets].sort((a, b) => a - b));
    expect(file_.getSize()).toBeGreaterThan(0);
    await file.close();
    await file.close();
    expect(charge.used).toBe(0);
    expect(service.bytesInUse).toBe(0);
    const next = await service.create(2, charge);
    expect((await next.append(page(10, 1))).offset).toBe(0);
    await expect(file.append(page(1, 1))).rejects.toThrow(TypeError);
  });

  it("writes each page directly when the budget refuses the batch, and oversized pages alone", async () => {
    const scratch = new FileScratch(
      new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
      () => undefined,
    );
    const service = new ScratchSpillService(scratch);
    const refused = budget(0);
    const file = await service.create(1, refused);
    const small = await file.append(page(100, 1));
    const big = await file.append(page(SPILL_BATCH_BYTES + 5, 2));
    expect(await file.read(small)).toEqual(page(100, 1));
    expect(await file.read(big)).toEqual(page(SPILL_BATCH_BYTES + 5, 2));
    const batched = await service.create(2, budget(1 << 20));
    const before = await batched.append(page(10, 3));
    const huge = await batched.append(page(SPILL_BATCH_BYTES + 1, 4));
    expect(await batched.read(before)).toEqual(page(10, 3));
    expect(await batched.read(huge)).toEqual(page(SPILL_BATCH_BYTES + 1, 4));
  });

  it("refuses a corrupted page, maps quota to SpillQuotaError, and fences a lost owner", async () => {
    const { backend, scratch, disk } = await scratchOver();
    const service = new ScratchSpillService(scratch);
    const file = await service.create(1, budget(0));
    const ref = await file.append(page(5000, 1));
    const state = disk.files.get("spill");
    if (state === undefined) throw new Error("no spill file");
    state.current[ref.offset + 10] = (state.current[ref.offset + 10] ?? 0) ^ 0xff;
    await expect(file.read(ref)).rejects.toThrow(SpillCorruptionError);

    const full: ScratchSpace = {
      write: () => Promise.reject(new StoreQuotaError("full")),
      readInto: (offset, target) => scratch.readInto(offset, target),
      release: (ranges: readonly DataRange[]) => scratch.release(ranges),
      truncate: (size) => scratch.truncate(size),
    };
    const quota = await new ScratchSpillService(full).create(1, budget(0));
    await expect(quota.append(page(10, 1))).rejects.toThrow(SpillQuotaError);

    await backend.acquire({ steal: true, timeoutMs: 2_000 });
    await expect(file.append(page(10, 1))).rejects.toThrow(OwnershipLostError);
  });

  it("sweeps everything at startup: the space is empty and old files are dead", async () => {
    const { scratch, disk } = await scratchOver();
    const service = new ScratchSpillService(scratch);
    const file = await service.create(1, budget(0));
    await file.append(page(9000, 1));
    await service.sweep();
    expect(disk.files.get("spill")?.current.length).toBe(0);
    expect(service.bytesInUse).toBe(0);
    await expect(file.append(page(1, 1))).rejects.toThrow(TypeError);
    await file.close();
    const fresh = await service.create(2, budget(0));
    expect((await fresh.append(page(10, 2))).offset).toBe(0);
  });

  it.each(seedsFor("next-scratch-spill", [0x5b11, 23]))(
    "keeps every page of interleaved files intact (seed %i)",
    async (seed) => {
      const random = mulberry32(seed);
      // A growable in-memory file: the simulated disk copies its whole image on every write.
      const scratch = new FileScratch(
        new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
        () => undefined,
      );
      const service = new ScratchSpillService(scratch);
      const files = new Map<
        number,
        {
          file: Awaited<ReturnType<ScratchSpillService["create"]>>;
          pages: Array<[SpillPageRef, Uint8Array]>;
        }
      >();
      let next = 1;
      for (let step = 0; step < 300; step++) {
        const roll = random();
        if (files.size === 0 || roll < 0.1) {
          const id = next++;
          files.set(id, {
            file: await service.create(id, budget(random() < 0.5 ? 0 : 1 << 20)),
            pages: [],
          });
          continue;
        }
        const ids = [...files.keys()];
        const id = ids[Math.floor(random() * ids.length)] ?? 0;
        const entry = files.get(id);
        if (entry === undefined) continue;
        if (roll < 0.15) {
          await entry.file.close();
          files.delete(id);
        } else if (roll < 0.7) {
          const bytes = page(1 + Math.floor(random() * 70_000), step);
          entry.pages.push([await entry.file.append(bytes), bytes]);
        } else if (entry.pages.length > 0) {
          const [ref, bytes] = entry.pages[Math.floor(random() * entry.pages.length)] ?? [];
          if (ref !== undefined) expect(same(await entry.file.read(ref), bytes)).toBe(true);
        }
      }
      for (const { file, pages } of files.values()) {
        for (const [ref, bytes] of pages) expect(same(await file.read(ref), bytes)).toBe(true);
        await file.close();
      }
      expect(service.bytesInUse).toBe(0);
    },
  );

  it("keeps O(1) bookkeeping per flush: a sequential file is one coalesced run (review B1-p2b 3)", async () => {
    const scratch = new FileScratch(
      new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
      () => undefined,
    );
    const service = new ScratchSpillService(scratch);
    const charge = budget(1 << 20);
    const file = await service.create(1, charge);
    for (let index = 0; index < 1000; index++)
      await file.append(new Uint8Array(SPILL_BATCH_BYTES / 2));
    expect((file as unknown as { extentRuns: number }).extentRuns).toBe(1);
    expect(service.bytesInUse).toBe(1000 * (SPILL_BATCH_BYTES / 2));
    await file.close();
    expect(charge.used).toBe(0);
  });

  it("shares one cleanup between concurrent close calls (review B1-p2b 4)", async () => {
    let unblock!: () => void;
    const gate = new Promise<void>((resolve) => (unblock = resolve));
    const backing = new FileScratch(
      new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
      () => undefined,
    );
    const slow: ScratchSpace = {
      write: async (offset, bytes) => {
        await gate;
        await backing.write(offset, bytes);
      },
      readInto: (offset, target) => backing.readInto(offset, target),
      release: (ranges) => backing.release(ranges),
      truncate: (size) => backing.truncate(size),
    };
    const service = new ScratchSpillService(slow);
    const charge = budget(1 << 20);
    const file = await service.create(2, charge);
    const appending = file.append(page(SPILL_BATCH_BYTES + 1, 1));
    await new Promise((resolve) => setTimeout(resolve, 0));
    let settled = 0;
    const first = file.close().then(() => (settled += 1));
    const second = file.close().then(() => (settled += 1));
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(settled).toBe(0);
    unblock();
    await appending;
    await Promise.all([first, second]);
    expect(settled).toBe(2);
    expect(charge.used).toBe(0);
    expect(service.bytesInUse).toBe(0);
  });
});
