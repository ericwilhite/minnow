/** SimDisk's documented fault law is authority; reuse may change allocation cost only.
 * The cold reference copies its current image before each write, preventing append reuse.
 * Both disks must retain the same seeded crash images, physical steps and old visible views.
 */
import { afterAll, expect, it } from "vitest";
import { createReach, SIM_DISK_APPEND_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { SimDisk } from "./sim-disk.js";
import { bytesOf, expectBytes } from "./space-test-support.js";
import type { SyncFile } from "./contract.js";

const reach = createReach("simulated disk append reuse", SIM_DISK_APPEND_REACH);
for (const seed of seedsFor("sim-disk-append", [27921, 27922, 27923, 27924]))
  for (const flush of ["honest", "lying"] as const)
    it(`preserves images and amortizes appends (${String(seed)}, ${flush})`, () => {
      reach.beginHistory();
      const random = mulberry32(seed);
      const growing = new SimDisk({ seed, flush });
      const writer = growing.open("growth", false);
      const buffers = new Set<ArrayBufferLike>();
      const prefix: number[] = [];
      for (let at = 0; at < 256; at++) {
        const bytes = bytesOf(1 + Math.floor(random() * 2048), at);
        writer.write(bytes, { at: prefix.length });
        prefix.push(...bytes);
        const current = growing.files.get("growth")?.current;
        if (current === undefined) throw new Error("missing image");
        buffers.add(current.buffer);
        expectBytes(current, Uint8Array.from(prefix));
        expect(current.buffer.byteLength).toBeLessThanOrEqual(Math.max(4096, 2 * current.length));
        if (at % 7 === 0) writer.flush();
      }
      // A complexity bound, not a wall-time assertion: no full prefix copy per append.
      expect(buffers.size).toBeLessThanOrEqual(16);
      reach.reach("amortized append growth");
      const read = new Uint8Array(prefix.length + 17).fill(0xa5);
      expect(writer.read(read, { at: 0 })).toBe(prefix.length);
      expect(writer.getSize()).toBe(prefix.length);
      expectBytes(read.subarray(0, prefix.length), Uint8Array.from(prefix));
      expectBytes(read.subarray(prefix.length), new Uint8Array(17).fill(0xa5), "past EOF");
      writer.close();

      const options = { seed, flush, crashAtStep: 2 + Math.floor(random() * 6) };
      const disk = new SimDisk(options);
      const cold = new SimDisk(options);
      let file = disk.open("data", false);
      let reference = cold.open("data", false);
      const retained: Array<{ view: Uint8Array; expected: Uint8Array }> = [];
      const attempt = (handle: SyncFile, run: (handle: SyncFile) => void): string | null => {
        try {
          run(handle);
          return null;
        } catch (error) {
          return (error as Error).name;
        }
      };
      const compare = () => {
        const actual = disk.files.get("data");
        const expected = cold.files.get("data");
        if (actual === undefined || expected === undefined) throw new Error("missing image");
        expectBytes(actual.current, expected.current);
        expectBytes(actual.durable, expected.durable);
        expect(disk.steps).toBe(cold.steps);
        expect(disk.crashes).toBe(cold.crashes);
        for (const view of [actual.current, actual.durable])
          retained.push({ view, expected: view.slice() });
      };
      const reopen = () => {
        expect(() => file.getSize()).toThrow(/closed/);
        expect(() => reference.getSize()).toThrow(/closed/);
        file = disk.open("data", false);
        reference = cold.open("data", false);
      };
      const mutate = (run: (handle: SyncFile) => void) => {
        const crashes = disk.crashes;
        // A fresh image gives the reference no reusable spare backing. No physical step
        // or byte is changed, and durable/pending/queued state is left untouched.
        const state = cold.files.get("data");
        if (state === undefined) throw new Error("missing cold image");
        state.current = state.current.slice();
        const failure = attempt(file, run);
        expect(failure).toBe(attempt(reference, run));
        expect([null, "SimulatedCrash"]).toContain(failure);
        compare();
        if (disk.crashes !== crashes) {
          reach.reach("planned physical crash");
          reopen();
        }
      };
      for (let phase = 0; phase < 16; phase++) {
        for (let append = 0; append < 8; append++) {
          const bytes = bytesOf(1 + Math.floor(random() * 4096), phase * 8 + append);
          const at = file.getSize();
          mutate((handle) => handle.write(bytes, { at }));
        }
        mutate((handle) => handle.flush());
        const size = file.getSize();
        const offset = Math.floor(random() * Math.max(1, size));
        const patch = bytesOf(1 + Math.floor(random() * 512), phase);
        mutate((handle) => handle.write(patch, { at: offset }));
        mutate((handle) => handle.truncate(Math.floor(size / 2)));
        const gap = file.getSize() + 1 + Math.floor(random() * 4096);
        mutate((handle) => handle.write(patch, { at: gap }));
        reach.reach("overwrite truncate and gap");
        mutate((handle) => handle.flush());
        const before = file.getSize();
        const suffix = bytesOf(4096, phase);
        mutate((handle) => handle.write(suffix, { at: before }));
        disk.powerLoss();
        cold.powerLoss();
        compare();
        if ((disk.files.get("data")?.current.length ?? 0) < before + suffix.length)
          reach.reach("recovery forks before an old suffix");
        reopen();
        // The next iteration appends to a recovered prefix. It cannot overwrite any
        // visible byte in the longer pre-crash view, even if all unflushed writes lost.
        for (const { view, expected } of retained) expectBytes(view, expected, "retained image");
      }
      reach.reach("retained immutable views");
      reach.reach(flush === "honest" ? "honest flush" : "lying flush");
      file.close();
      reference.close();
    });
afterAll(() => reach.assertReached());
