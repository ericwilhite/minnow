/** Root and snapshot state has explicit ownership, including overlap and late release. */
import { afterAll, expect, it, vi } from "vitest";
import { createReach, ROOT_CREDIT_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import {
  RootMemory,
  ROOT_BANK_BYTES,
  SMALL_ROOT_BYTES,
  preparedRoot,
  type RootFrame,
} from "./root-memory.js";
import { RootPins, PIN_HEADER_BYTES } from "./root-pins.js";
import { countingBudget, countingMemory } from "./space-test-support.js";
import { MemoryBackend } from "./memory-store.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimDisk, SimLocks } from "./sim-disk.js";
import { simIndexedDb } from "./sim-idb.js";
import * as slotCodec from "./root-slot.js";
import { decodeRootSlot, encodeRootSlot } from "./root-slot.js";
import { ReadDeniedError } from "./extents.js";
const reach = createReach("root credit ownership", ROOT_CREDIT_REACH);
for (const store of ["files", "indexeddb"] as const)
  for (const seed of seedsFor("root-credit", [1631, 1632, 1633, 1634]))
    it(`${store}: accounts root overlap and snapshot descriptors (${String(seed)})`, async () => {
      reach.beginHistory();
      const random = mulberry32(seed),
        memory = countingMemory(4 << 20),
        budget = countingBudget(4 << 20);
      const roots = RootMemory.create(() => memory),
        frames: RootFrame[] = [];
      const backend =
          store === "files"
            ? new MemoryBackend()
            : createIndexedDbBackend({
                name: "root-credit",
                locks: new SimLocks(),
                database: () => simIndexedDb(new SimDisk()),
              }),
        lease = await backend.acquire(),
        session = await backend.open(lease);
      try {
        // Five simultaneous small frames include current/previous, preparation and captured audit.
        for (let i = 0; i < 5; i++) {
          const frame = await roots.copy(new Uint8Array(SMALL_ROOT_BYTES - i).fill(i + 1), budget);
          frames.push(frame);
          expect(memory.used).toBe(ROOT_BANK_BYTES);
          expect(frames.reduce((sum, value) => sum + value.slotBytes.length + 512, 0)).toBeLessThan(
            ROOT_BANK_BYTES,
          );
        }
        reach.reach("five overlapping root frames prepaid");
        const length = (256 << 10) + Math.floor(random() * 1024);
        const large = await roots.copy(new Uint8Array(length).fill(seed & 255), budget);
        frames.push(large);
        expect(memory.used).toBe(ROOT_BANK_BYTES + large.slotBytes.length + 512);
        expect(budget.used).toBe(0);
        const before = memory.used;
        large.retain();
        large.release();
        expect(memory.used).toBe(before);
        large.prepare(session, 0, 11);
        expect(preparedRoot(session, large.bytes)?.frame).toBe(large);
        expect(large.slotBytes).toEqual(encodeRootSlot(11, large.bytes));
        const encode = vi.spyOn(slotCodec, "encodeRootSlot"),
          choose = vi.spyOn(slotCodec, "chooseRoot");
        try {
          await session.publish(large.bytes);
          expect(encode).not.toHaveBeenCalled();
          expect(choose).not.toHaveBeenCalled();
        } finally {
          encode.mockRestore();
          choose.mockRestore();
        }
        expect(await session.readRoot()).toEqual(large.bytes);
        expect(decodeRootSlot(large.slotBytes, true)?.root.buffer).toBe(large.slotBytes.buffer);
        reach.reach("large frame credit and encoded compatibility");
        await expect(roots.copy(new Uint8Array(1 << 20), countingBudget(0))).rejects.toBeInstanceOf(
          ReadDeniedError,
        );
        expect(memory.used).toBe(before);
        reach.reach("refusal keeps owned roots and charge intact");
      } finally {
        for (const frame of frames) frame.release();
        roots.close();
        await session.close();
        await lease.release();
      }
      expect(memory.used).toBe(0);
      expect(budget.used).toBe(0);
      const cache = countingMemory(64 << 10),
        owner = countingBudget(64 << 10);
      let ticks = 0;
      const pins = new RootPins(
          cache,
          () => undefined,
          () => ticks,
        ),
        live: Array<{ pin: ReturnType<RootPins["pin"]>; caller: boolean; bytes: number }> = [];
      for (let i = 0; i < 160; i++) {
        ticks++;
        if (random() < 0.2) pins.advance();
        if (live.length > 0 && random() < 0.45) {
          const index = Math.floor(random() * live.length),
            value = live.splice(index, 1)[0];
          value?.pin.release();
          value?.pin.release();
        } else {
          const label = `snapshot-${String(i)}-${"x".repeat(i % 31)}`,
            caller = i % 2 === 0;
          live.push({
            pin: pins.pin(label, caller ? owner : undefined),
            caller,
            bytes: PIN_HEADER_BYTES + label.length * 2,
          });
        }
        expect(pins.pinCount).toBe(live.length);
        expect(pins.oldestPinned).toBe(
          live.length === 0 ? null : Math.min(...live.map((value) => value.pin.generation)),
        );
        expect(owner.used).toBe(
          live.filter((value) => value.caller).reduce((sum, value) => sum + value.bytes, 0),
        );
        expect(cache.used).toBe(
          live.filter((value) => !value.caller).reduce((sum, value) => sum + value.bytes, 0),
        );
      }
      reach.reach("independent descriptor accounting");
      expect(() => pins.pin("refused", countingBudget(0))).toThrow(ReadDeniedError);
      expect(pins.pinCount).toBe(live.length);
      pins.close();
      for (const value of live) value.pin.release();
      expect(cache.used).toBe(0);
      expect(owner.used).toBe(0);
      expect(pins.pinCount).toBe(0);
      reach.reach("late snapshot release refunds its owner");
    });
afterAll(() => reach.assertReached());

it("moves admitted wrapper credit from its native ordinary grant without overlap", async () => {
  const { Governor } = await import("../memory/governor.js");
  const governor = new Governor(8 << 20),
    grant = await governor.admit({ label: "root input", minimumBytes: 1 << 20, spillable: true });
  const roots = RootMemory.create((cache) => governor.registerCache(cache));
  const before = governor.usedBytes,
    held = grant.held;
  const budget = {
    allocate: (bytes: number) => grant.allocate(bytes),
    free: (bytes: number) => grant.free(bytes),
    ordinarySource: () => grant,
  };
  const frame = await roots.copy(new Uint8Array(256 << 10), budget);
  try {
    expect(governor.usedBytes).toBe(before);
    expect(grant.allocated).toBe(0);
    expect(held - grant.held).toBe(frame.slotBytes.length + 512);
  } finally {
    frame.release();
    roots.close();
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
});
