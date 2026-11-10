/** R8 / spill contract: interleaving must not create uncharged resident extent state. */
import { afterAll, expect, it } from "vitest";
import { createReach, SCRATCH_METADATA_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";
import { Governor } from "../memory/governor.js";
import { countingMemory } from "../storage/space-test-support.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimBackend, SimDisk, SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import {
  METADATA_PAGE_BYTES,
  OwnershipLostError,
  ReleaseUnsettledError,
} from "../storage/contract.js";
import type { IdbDatabaseLike } from "../storage/idb-like.js";
import type { DataRange } from "../storage/contract.js";
import type { SpillPageRef } from "../spill/contract.js";
import { SCRATCH_CONTROL_BYTES } from "../spill/scratch-ranges.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";

const reach = createReach("scratch metadata", SCRATCH_METADATA_REACH);
function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new Error("Missing generated value");
  return value;
}

const fresh = () => new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null });

for (const seed of seedsFor("scratch-metadata", [881, 882, 883, 884]))
  it(`accounts interleaved ownership and release under pressure (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      governor = new Governor(64 * 1024 * 1024);
    const control = await governor.admit({
      label: "spill control",
      minimumBytes: 32 * 1024,
      spillable: true,
    });
    const pressure = await governor.admit({
      label: "other owners",
      minimumBytes: governor.capBytes - governor.reserveBytes - control.held,
      spillable: false,
    });
    const data = fresh(),
      metadata = fresh();
    const backing = new FileScratch(data, () => undefined);
    const scratch = Object.assign(backing, {
      metadata: () => ({
        async read(page: number, target: Uint8Array) {
          if (metadata.read(target, { at: page * 4096 }) !== target.length)
            throw Error("short metadata read");
        },
        async write(page: number, bytes: Uint8Array) {
          expect(control.allocated).toBe(SCRATCH_CONTROL_BYTES);
          metadata.write(bytes, { at: page * 4096 });
        },
        async reset() {
          metadata.truncate(0);
        },
      }),
    });
    const options = { memory: countingMemory(Infinity), budget: control };
    const service = new ScratchSpillService(scratch, options);
    const budget = { allocate: () => false, free: () => undefined };
    const files = [
      await service.create(1, budget),
      await service.create(2, budget),
      await service.create(3, budget),
    ];
    const refs: SpillPageRef[][] = [[], [], []];
    const page = new Uint8Array(4096),
      view = new DataView(page.buffer);
    const entries = () => {
      const tracked: { residentExtentEntries?: number } = service;
      return (
        tracked.residentExtentEntries ??
        files.reduce((n, file) => n + (file as typeof file & { extentRuns: number }).extentRuns, 0)
      );
    };
    try {
      for (let i = 0; i < 3072; i++) {
        const which = i < 2048 ? i % 2 : Math.floor(random() * 3);
        view.setUint32(0, i, true);
        view.setUint32(4, which, true);
        at(refs, which).push(await at(files, which).append(page));
        // Two scalar coordinates already cost 16 bytes, before object/array overhead.
        expect(entries() * 16).toBeLessThanOrEqual(control.allocated);
      }
      reach.reach("interleaved direct appends");
      reach.reach("metadata pressure fallback");
      for (let which = 0; which < files.length; which++)
        for (const ref of at(refs, which)) {
          await at(files, which).readInto(ref, page);
          expect(view.getUint32(4, true)).toBe(which);
          expect(entries() * 16).toBeLessThanOrEqual(control.allocated);
        }
      reach.reach("round trip");
      await at(files, 0).close();
      const reuse = await service.create(4, budget);
      const reused = await reuse.append(page);
      expect(reused.offset).toBeLessThan(data.getSize());
      await reuse.close();
      expect(control.allocated).toBe(SCRATCH_CONTROL_BYTES);
      reach.reach("fragmented reuse");
      await Promise.all(files.map((file) => file.close()));
      for (const file of files)
        expect((file as typeof file & { extentRuns: number }).extentRuns).toBe(0);
      const peak = metadata.getSize();
      for (let wave = 0; wave < 4; wave++) {
        const pair = [
          await service.create(10 + wave * 2, budget),
          await service.create(11 + wave * 2, budget),
        ];
        for (let i = 0; i < 512; i++) await at(pair, i % 2).append(page);
        for (const file of random() < 0.5 ? pair : pair.reverse()) await file.close();
        expect(metadata.getSize()).toBeLessThanOrEqual(peak + 2 * 4096);
        expect(control.allocated).toBe(SCRATCH_CONTROL_BYTES);
        expect(service.bytesInUse).toBe(0);
      }
      reach.reach("metadata pages reused");
      await service.close();
      expect(control.allocated).toBe(0);
      expect(service.bytesInUse).toBe(0);
      reach.reach("close returns charge");
    } finally {
      await service.close();
      control.close();
      pressure.close();
    }
    expect(governor.usedBytes).toBe(0);
  }, 60_000);

it("streams release descriptors without retaining an extent-sized array", async () => {
  reach.beginHistory();
  const data = fresh(),
    meta = fresh(),
    backing = new FileScratch(data, () => undefined);
  const control = {
    used: 0,
    allocate(n: number) {
      this.used += n;
      return true;
    },
    free(n: number) {
      this.used -= n;
    },
  };
  const scratch = {
    write: (offset: number, bytes: Uint8Array) => backing.write(offset, bytes),
    readInto: (offset: number, bytes: Uint8Array) => backing.readInto(offset, bytes),
    truncate: (size: number) => backing.truncate(size),
    metadata: () => ({
      async read(page: number, target: Uint8Array) {
        if (meta.read(target, { at: page * 4096 }) !== target.length) throw Error("short metadata");
      },
      async write(page: number, bytes: Uint8Array) {
        meta.write(bytes, { at: page * 4096 });
      },
      async reset() {
        meta.truncate(0);
      },
    }),
    async release(ranges: readonly DataRange[]) {
      expect(ranges.length).toBeLessThanOrEqual(16);
      expect(control.used).toBeGreaterThan(0);
      await backing.release(ranges);
    },
  };
  const options = { memory: countingMemory(Infinity), budget: control };
  const service = new ScratchSpillService(scratch, options),
    budget = { allocate: () => false, free: () => undefined };
  const a = await service.create(1, budget),
    b = await service.create(2, budget),
    page = new Uint8Array(4096);
  for (let i = 0; i < 1024; i++) {
    await a.append(page);
    await b.append(page);
  }
  await Promise.all([a.close(), b.close(), service.close()]);
  expect(control.used).toBe(0);
  reach.reach("bounded release descriptors");
});

for (const seed of seedsFor("scratch-free-fragments", [885, 886]))
  it(`reuses nonadjacent free runs across full metadata pages (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      data = fresh(),
      metadata = fresh();
    const control = {
      used: 0,
      allocate(n: number) {
        this.used += n;
        return true;
      },
      free(n: number) {
        this.used -= n;
      },
    };
    const service = new ScratchSpillService(new FileScratch(data, () => undefined, metadata), {
      budget: control,
    });
    const refused = { allocate: () => false, free: () => undefined };
    const files = [],
      refs: SpillPageRef[][] = [[], [], [], []];
    for (let i = 0; i < 4; i++) files.push(await service.create(i, refused));
    const page = new Uint8Array(4096),
      view = new DataView(page.buffer),
      rotation = Math.floor(random() * 4);
    try {
      for (let i = 0; i < 2080; i++) {
        const which = (i + rotation) % 4;
        view.setUint32(0, i, true);
        at(refs, which).push(await at(files, which).append(page));
      }
      // Neither closed stream borders the other's holes: >1,000 distinct free runs.
      await at(files, 0).close();
      await at(files, 2).close();
      reach.reach("nonadjacent frees beyond a metadata page");
      const reuse = await service.create(4, refused),
        reused: SpillPageRef[] = [];
      const size = data.getSize();
      for (let i = 0; i < 1000; i++) {
        view.setUint32(0, i, true);
        reused.push(await reuse.append(page));
      }
      expect(data.getSize()).toBeLessThanOrEqual(size);
      for (const [i, ref] of reused.entries()) {
        await reuse.readInto(ref, page);
        expect(view.getUint32(0, true)).toBe(i);
      }
      for (const which of [1, 3])
        for (const [i, ref] of at(refs, which).entries()) {
          await at(files, which).readInto(ref, page);
          expect(view.getUint32(0, true)).toBe(i * 4 + ((which - rotation + 4) % 4));
        }
      await reuse.close();
      await at(files, 1).close();
      await at(files, 3).close();
      expect(service.bytesInUse).toBe(0);
      expect(data.getSize()).toBe(0);
    } finally {
      await service.close();
    }
    expect(control.used).toBe(0);
  }, 60_000);

it("holds the control charge until an interrupted release settles and sweep can retry", async () => {
  reach.beginHistory();
  const data = fresh(),
    metadata = fresh();
  const backing = new FileScratch(data, () => undefined, metadata);
  const control = {
    used: 0,
    allocate(n: number) {
      this.used += n;
      return true;
    },
    free(n: number) {
      this.used -= n;
    },
  };
  let start!: () => void, unblock!: () => void;
  const started = new Promise<void>((resolve) => {
    start = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    unblock = resolve;
  });
  let fail = true;
  const scratch = {
    metadata: () => backing.metadata(),
    write: (offset: number, bytes: Uint8Array) => backing.write(offset, bytes),
    readInto: (offset: number, bytes: Uint8Array) => backing.readInto(offset, bytes),
    truncate: (size: number) => backing.truncate(size),
    async release(ranges: readonly DataRange[]) {
      start();
      await gate;
      expect(control.used).toBe(SCRATCH_CONTROL_BYTES);
      if (fail) {
        fail = false;
        throw Error("interrupted release");
      }
      await backing.release(ranges);
    },
  };
  const service = new ScratchSpillService(scratch, { budget: control });
  const refused = { allocate: () => false, free: () => undefined };
  const file = await service.create(1, refused);
  await file.append(new Uint8Array(4096));
  const closing = file.close();
  await started;
  const sweeping = service.sweep();
  expect(control.used).toBe(SCRATCH_CONTROL_BYTES);
  unblock();
  await expect(closing).rejects.toThrow("interrupted release");
  await sweeping;
  const next = await service.create(2, refused);
  expect((await next.append(new Uint8Array(4096))).offset).toBe(0);
  await service.close();
  expect(control.used).toBe(0);
  reach.reach("release failure and sweep retry");
});

it("refuses damaged ownership pages without returning their extents for reuse", async () => {
  reach.beginHistory();
  const data = fresh(),
    meta = fresh();
  const control = {
    used: 0,
    allocate(n: number) {
      this.used += n;
      return true;
    },
    free(n: number) {
      this.used -= n;
    },
  };
  const service = new ScratchSpillService(new FileScratch(data, () => undefined, meta), {
    budget: control,
  });
  const refused = { allocate: () => false, free: () => undefined };
  const a = await service.create(1, refused),
    b = await service.create(2, refused);
  await a.append(new Uint8Array(4096));
  await b.append(new Uint8Array(4096)); // evicts a's ownership page to scratch
  meta.write(new Uint8Array([0xff]), { at: 40 });
  await expect(a.close()).rejects.toThrow("Malformed scratch allocator metadata");
  await expect(service.create(3, refused)).rejects.toThrow("Malformed scratch allocator metadata");
  await service.close();
  expect(control.used).toBe(0);
  reach.reach("metadata corruption refusal");
});

for (const name of ["file", "IndexedDB"])
  it(`${name}: allocator pages are isolated and fenced alongside payload`, async () => {
    reach.beginHistory();
    const disk = new SimDisk(),
      backend =
        name === "file"
          ? new SimBackend(disk)
          : createIndexedDbBackend({
              database: () => simIndexedDb(disk),
              locks: new SimLocks(),
              name: "allocator",
            });
    const lease = await backend.acquire(),
      session = await backend.open(lease);
    const metadata = session.metadata?.(),
      scratch = backend.scratch(lease);
    if (metadata == null || scratch === null) throw Error("Missing native metadata");
    const allocator = scratch.metadata?.();
    if (allocator === undefined) throw Error("Missing allocator metadata");
    const control = {
      used: 0,
      allocate(n: number) {
        this.used += n;
        return true;
      },
      free(n: number) {
        this.used -= n;
      },
    };
    const service = new ScratchSpillService(scratch, { budget: control });
    const frame = new Uint8Array(METADATA_PAGE_BYTES).fill(37),
      target = new Uint8Array(METADATA_PAGE_BYTES);
    const refused = { allocate: () => false, free: () => undefined };
    let replacement;
    try {
      await metadata.write(0, frame);
      await service.sweep();
      await metadata.read(0, target);
      expect(target).toEqual(frame);
      const a = await service.create(1, refused),
        b = await service.create(2, refused);
      await a.append(new Uint8Array(4096));
      await b.append(new Uint8Array(4096));
      await metadata.reset();
      // Extent-space reset must preserve the allocator's first flushed ownership page.
      await a.close();
      await b.close();
      reach.reach("metadata namespace isolation");
      replacement = await backend.acquire({ steal: true, timeoutMs: 2_000 });
      const page = new Uint8Array(4096);
      for (const action of [
        () => allocator.read(0, page),
        () => allocator.write(0, page),
        () => allocator.reset(),
      ])
        await expect(action()).rejects.toThrow(OwnershipLostError);
      reach.reach("allocator fence");
    } finally {
      await service.close();
      await session.close();
      await lease.release();
      await replacement?.release();
    }
    expect(control.used).toBe(0);
  });

it("keeps allocator credit until native metadata ends after an unsettled lease release", async () => {
  reach.beginHistory();
  const inner = simIndexedDb(new SimDisk());
  let pause = false,
    began!: () => void,
    resume!: () => void,
    nativeEnd: Promise<void> = Promise.resolve();
  const started = new Promise<void>((resolve) => {
      began = resolve;
    }),
    gate = new Promise<void>((resolve) => {
      resume = resolve;
    });
  const database: IdbDatabaseLike = {
    close: () => inner.close(),
    transaction(stores, mode, durability, body) {
      if (
        pause &&
        stores.length === 1 &&
        stores[0] === "meta" &&
        mode === "readwrite" &&
        durability === "relaxed"
      ) {
        pause = false;
        nativeEnd = inner
          .transaction(stores, mode, durability, (tx) =>
            body({ store: (name) => tx.store(name), abort: () => undefined }),
          )
          .then(() => gate);
        began();
        return nativeEnd;
      }
      return inner.transaction(stores, mode, durability, body);
    },
  };
  const backend = createIndexedDbBackend({
    database: () => database,
    locks: new SimLocks(),
    name: "unsettled-scratch",
    releaseDrainMs: 1,
    releaseSettleMs: 1,
  });
  const lease = await backend.acquire(),
    scratch = backend.scratch(lease);
  if (scratch === null) throw Error("Missing scratch");
  const control = {
    used: 0,
    allocate(n: number) {
      this.used += n;
      return true;
    },
    free(n: number) {
      this.used -= n;
    },
  };
  const service = new ScratchSpillService(scratch, { budget: control }),
    refused = { allocate: () => false, free: () => undefined };
  const a = await service.create(1, refused),
    b = await service.create(2, refused),
    page = new Uint8Array(4096);
  let closing: Promise<void> | null = null;
  try {
    await a.append(page);
    expect(control.used).toBe(SCRATCH_CONTROL_BYTES);
    pause = true;
    const append = b.append(page);
    const rejected = expect(append).rejects.toBeInstanceOf(ReleaseUnsettledError);
    await started;
    await expect(lease.release()).rejects.toBeInstanceOf(ReleaseUnsettledError);
    await rejected;
    let closed = false;
    closing = service.close().then(() => {
      closed = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(closed).toBe(false);
    expect(control.used).toBe(SCRATCH_CONTROL_BYTES);
  } finally {
    resume();
    await nativeEnd;
    await closing;
    await service.close();
    await lease.release().catch(() => undefined);
  }
  expect(control.used).toBe(0);
  reach.reach("unsettled native metadata retains charge");
});

afterAll(() => reach.assertReached());
