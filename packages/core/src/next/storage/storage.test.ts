import { describe, expect, it } from "vitest";
import { xxHash32 } from "../hash/xxhash32.js";
import { storeConformanceCases } from "./conformance.js";
import {
  DATA_ALIGN,
  OwnerUnresponsiveError,
  OwnershipLostError,
  type StoreSession,
} from "./contract.js";
import { chooseRoot, decodeRootSlot, encodeRootSlot } from "./root-slot.js";
import { acquireFenced } from "./fence.js";
import { SimBackend, SimDisk, SimLocks, SimulatedCrash, type SimLease } from "./sim-disk.js";

const SEEDS = 12;

describe.each([
  ["handle fence (Chromium/Firefox busy name)", "handle", "NoModificationAllowedError"],
  ["handle fence (WebKit busy name)", "handle", "InvalidStateError"],
  ["epoch fence (IndexedDB model)", "epoch", "NoModificationAllowedError"],
] as const)("store conformance: %s", (_label, fence, busyName) => {
  for (const testCase of storeConformanceCases)
    it(testCase.name, () =>
      testCase.run(async () => new SimBackend(new SimDisk({ busyName }), fence)),
    );
});

describe("store conformance with asynchronous fence opens (createSyncAccessHandle is async)", () => {
  for (const fence of ["handle", "epoch"] as const)
    for (const testCase of storeConformanceCases)
      it(`${fence}: ${testCase.name}`, () =>
        testCase.run(
          async () =>
            new SimBackend(new SimDisk(), fence, {
              openDelay: () => new Promise((resolve) => setTimeout(resolve, 1)),
            }),
        ));
});

describe("fence acquisition", () => {
  const busy = () => Object.assign(new Error("busy"), { name: "NoModificationAllowedError" });
  const lockIsFree = async (locks: SimLocks) => {
    const lock = await Promise.race([
      locks.request("db", {}),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 50)),
    ]);
    lock?.release();
    return lock !== null;
  };

  it("retries an asynchronous busy rejection, then gives up and releases the lock (review probe)", async () => {
    const locks = new SimLocks();
    let tries = 0;
    await expect(
      acquireFenced(
        locks,
        "db",
        async () => {
          tries += 1;
          throw busy();
        },
        () => undefined,
        { timeoutMs: 30 },
      ),
    ).rejects.toThrow(OwnerUnresponsiveError);
    expect(tries).toBeGreaterThan(1);
    expect(await lockIsFree(locks)).toBe(true);
  });

  it("enforces the deadline during a slow open and closes the handle that arrives late", async () => {
    const locks = new SimLocks();
    let closed = 0;
    await expect(
      acquireFenced(
        locks,
        "db",
        () => new Promise<string>((resolve) => setTimeout(() => resolve("handle"), 60)),
        (fence) => {
          if (fence === "handle") closed += 1;
        },
        { timeoutMs: 20 },
      ),
    ).rejects.toThrow(OwnerUnresponsiveError);
    expect(await lockIsFree(locks)).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(closed).toBe(1);
  });

  it("rethrows a non-busy failure (sync or async) and releases the lock", async () => {
    const locks = new SimLocks();
    for (const openFence of [
      () => {
        throw new TypeError("quota");
      },
      () => new Promise<never>((_, reject) => setTimeout(() => reject(new TypeError("quota")), 5)),
    ]) {
      await expect(acquireFenced(locks, "db", openFence, () => undefined)).rejects.toThrow(
        TypeError,
      );
      expect(await lockIsFree(locks)).toBe(true);
    }
  });

  it("closes a fence that opens after its lock was stolen", async () => {
    const locks = new SimLocks();
    let closed = false;
    let finishOpen: (value: string) => void = () => undefined;
    const acquiring = acquireFenced(
      locks,
      "db",
      () => new Promise<string>((resolve) => (finishOpen = resolve)),
      () => (closed = true),
      { timeoutMs: 1_000 },
    );
    await new Promise((resolve) => setTimeout(resolve, 5));
    const thief = await locks.request("db", { steal: true });
    finishOpen("handle");
    await expect(acquiring).rejects.toThrow(OwnershipLostError);
    expect(closed).toBe(true);
    thief.release();
  });

  it("a delayed successful open within the deadline acquires", async () => {
    const locks = new SimLocks();
    const { lock, fence } = await acquireFenced(
      locks,
      "db",
      () => new Promise<string>((resolve) => setTimeout(() => resolve("handle"), 5)),
      () => undefined,
      { timeoutMs: 1_000 },
    );
    expect(fence).toBe("handle");
    lock.release();
  });
});

describe("stale owners", () => {
  it("handle fence: a frozen owner keeps the files; nobody else writes until it thaws", async () => {
    const backend = new SimBackend(new SimDisk(), "handle");
    const first = await backend.acquire();
    const session = await backend.open(first);
    await session.appendLog(new Uint8Array([1]));
    first.freeze();
    // The lock can be stolen, but the handles are the fence: the stealer cannot open them.
    await expect(backend.acquire({ steal: true, timeoutMs: 40 })).rejects.toThrow(
      OwnerUnresponsiveError,
    );
    // The frozen owner thaws: its lock-loss handling closes its handles before anything else.
    first.thaw();
    await expect(session.flushLog()).rejects.toThrow(OwnershipLostError);
    const second = await backend.acquire({ timeoutMs: 1_000 });
    expect(second.epoch).toBeGreaterThan(first.epoch);
    await second.release();
  });

  it("epoch fence: a frozen owner resumes after a takeover and every write is refused", async () => {
    const backend = new SimBackend(new SimDisk(), "epoch");
    const first = await backend.acquire();
    const session = await backend.open(first);
    await session.writeData(0, new Uint8Array([1]));
    await session.publish(new Uint8Array([1]));
    first.freeze();
    const second = await backend.acquire({ steal: true, timeoutMs: 1_000 });
    const current = await backend.open(second);
    first.thaw();
    await expect(session.writeData(0, new Uint8Array([9]))).rejects.toThrow(OwnershipLostError);
    await expect(session.publish(new Uint8Array([9]))).rejects.toThrow(OwnershipLostError);
    expect([...((await current.readRoot()) ?? [])]).toEqual([1]);
    expect([...((await current.readData([{ offset: 0, length: 1 }]))[0] ?? [])]).toEqual([1]);
    await second.release();
  });

  it("a fenced owner's publish never reaches the disk, at any crash (review probe)", async () => {
    for (let seed = 1; seed <= 30; seed++)
      for (const fence of ["handle", "epoch"] as const) {
        const disk = new SimDisk({ seed });
        const backend = new SimBackend(disk, fence);
        const old = await backend.acquire();
        const stale = await backend.open(old);
        await stale.publish(new Uint8Array([1]));
        await backend.acquire({ steal: true, timeoutMs: 1_000 });
        const stepsBefore = disk.steps;
        await expect(stale.publish(new Uint8Array([99]))).rejects.toThrow(OwnershipLostError);
        expect(disk.steps).toBe(stepsBefore);
        disk.powerLoss();
        backend.restart();
        const fresh = await backend.open(await backend.acquire());
        expect([...((await fresh.readRoot()) ?? [])]).toEqual([1]);
      }
  });
});

describe("root publication under every physical crash point", () => {
  const extent = (id: number) =>
    Uint8Array.from({ length: 700 + id * 90 }, (_, i) => (id * 13 + i) & 0xff);
  interface RootDoc {
    readonly extents: ReadonlyArray<{
      id: number;
      offset: number;
      length: number;
      checksum: number;
    }>;
  }
  const encode = (doc: RootDoc) => new TextEncoder().encode(JSON.stringify(doc));

  /** Rotates through three regions, so space freed two roots ago is reused (in place). */
  async function workload(
    session: StoreSession,
    attempted: RootDoc[],
    completed: RootDoc[],
  ): Promise<void> {
    let previous: RootDoc["extents"][number] | null = null;
    for (let id = 1; id <= 6; id++) {
      const bytes = extent(id);
      const offset = (id % 3) * 4 * DATA_ALIGN;
      await session.writeData(offset, bytes);
      const current = { id, offset, length: bytes.length, checksum: xxHash32(bytes) };
      const doc = { extents: previous === null ? [current] : [previous, current] };
      attempted.push(doc);
      await session.publish(encode(doc));
      completed.push(doc);
      previous = current;
    }
  }

  for (const flush of ["honest", "lying"] as const)
    it(`${flush} flush: opens to a published root whose extents verify${flush === "lying" ? ", or detects the loss" : ""}`, async () => {
      const clean = new SimDisk();
      const cleanBackend = new SimBackend(clean);
      await workload(await cleanBackend.open(await cleanBackend.acquire()), [], []);
      const totalSteps = clean.steps;
      let detected = 0;
      for (let step = 0; step < totalSteps; step++)
        for (let seed = 1; seed <= SEEDS; seed++) {
          const disk = new SimDisk({ crashAtStep: step, seed: seed * 7919 + step, flush });
          const backend = new SimBackend(disk);
          const attempted: RootDoc[] = [];
          const completed: RootDoc[] = [];
          const lease: SimLease = await backend.acquire();
          await expect(workload(await backend.open(lease), attempted, completed)).rejects.toThrow(
            SimulatedCrash,
          );
          backend.restart();
          const session = await backend.open(await backend.acquire());
          const root = await session.readRoot();
          const label = `step ${String(step)} seed ${String(seed)}`;
          const text = root === null ? null : new TextDecoder().decode(root);
          // Guarantee 1: the last completed root or the one in flight, nothing older or torn.
          // A lying flush may roll back further (the log header detects that; format tests).
          const allowed =
            flush === "honest"
              ? [completed.at(-1), attempted.at(-1)].map((d) =>
                  d === undefined ? null : JSON.stringify(d),
                )
              : [null, ...attempted.map((d) => JSON.stringify(d))];
          if (flush === "honest" && completed.length === 0) allowed.push(null);
          expect(allowed, label).toContain(text);
          if (text === null) continue;
          const doc = JSON.parse(text) as RootDoc;
          for (const ref of doc.extents) {
            const length = await session.dataLength();
            const intact =
              ref.offset + ref.length <= length &&
              xxHash32((await session.readData([ref]))[0] ?? new Uint8Array()) === ref.checksum;
            if (flush === "honest") expect(intact, `${label} extent ${String(ref.id)}`).toBe(true);
            else if (!intact) detected += 1;
          }
        }
      if (flush === "lying") expect(detected).toBeGreaterThan(0);
    });
});

describe("two-slot root publication", () => {
  const root = (text: string) => new TextEncoder().encode(text);

  it("round-trips a root and refuses a torn or foreign slot", () => {
    const slot = encodeRootSlot(2 ** 40 + 3, root("hello"));
    expect(decodeRootSlot(slot)).toEqual({ sequence: 2 ** 40 + 3, root: root("hello") });
    for (let cut = 0; cut < slot.length; cut++)
      expect(decodeRootSlot(slot.slice(0, cut)), `cut at ${String(cut)}`).toBeNull();
    for (let byte = 0; byte < slot.length; byte++) {
      const flipped = slot.slice();
      flipped[byte] = (flipped[byte] ?? 0) ^ 0x10;
      expect(decodeRootSlot(flipped), `flip at ${String(byte)}`).toBeNull();
    }
    expect(decodeRootSlot(null)).toBeNull();
  });

  it("chooses the newest valid slot and overwrites the other", () => {
    expect(chooseRoot(null, null)).toEqual({ current: null, nextSlot: 0, nextSequence: 1 });
    const one = encodeRootSlot(1, root("one"));
    const two = encodeRootSlot(2, root("two"));
    expect(chooseRoot(one, null)).toMatchObject({ nextSlot: 1, nextSequence: 2 });
    expect(chooseRoot(one, two).current?.root).toEqual(root("two"));
    expect(chooseRoot(one, two).nextSlot).toBe(0);
    // A torn write of the next root leaves the previous one current.
    const torn = encodeRootSlot(3, root("three")).slice(0, 10);
    expect(chooseRoot(torn, two).current?.sequence).toBe(2);
    expect(chooseRoot(torn, two).nextSlot).toBe(0);
  });
});
