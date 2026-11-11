/** Independent guards for checksummed allocator pages and retained ownership metadata. */
import { Session } from "node:inspector/promises";
import type { Runtime } from "node:inspector";
import { afterAll, expect, it } from "vitest";
import { createReach, SCRATCH_METADATA_GUARD_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { ScratchSpillService } from "../spill/scratch-spill.js";
import { FileScratch } from "../storage/file-store.js";
import { MemoryFile } from "../storage/memory-store.js";

const reach = createReach("scratch metadata guards", SCRATCH_METADATA_GUARD_REACH),
  fresh = () => new MemoryFile({ bytes: new Uint8Array(0), size: 0, holder: null }),
  refused = { allocate: () => false, free: () => undefined },
  workspaceBytes = 20 * 1024;
function controlBudget() {
  return {
    used: 0,
    allocate(bytes: number) {
      if (this.used + bytes > workspaceBytes) return false;
      this.used += bytes;
      return true;
    },
    free(bytes: number) {
      this.used -= bytes;
    },
  };
}

interface Inventory {
  objects: number;
  properties: number;
  arraySlots: number;
  bufferBytes: number;
}

/**
 * Inspect actual own/private references, not an engine-reported inventory. Budget and backing
 * storage are external boundaries; functions/prototypes and settled queue promises are excluded.
 * Array/Map/Set entries and views' backing buffers are followed, with identity deduplication.
 * This counts retained metadata structure, not V8 heap overhead or platform-owned memory.
 */
async function residentInventory(root: object, boundaries: readonly object[]): Promise<Inventory> {
  const key = "__minnow_scratch_residency",
    inspector = new Session(),
    inventory: Inventory = { objects: 0, properties: 0, arraySlots: 0, bufferBytes: 0 },
    pending: Runtime.RemoteObject[] = [];
  Reflect.set(globalThis, key, { root, seen: new WeakSet(boundaries) });
  inspector.connect();
  try {
    const first = await inspector.post("Runtime.evaluate", {
      expression: `globalThis[${JSON.stringify(key)}].root`,
      objectGroup: key,
    });
    pending.push(first.result);
    while (pending.length !== 0) {
      const object = pending.pop();
      if (
        object?.objectId === undefined ||
        object.type !== "object" ||
        object.subtype === "promise"
      )
        continue;
      const visited = await inspector.post("Runtime.callFunctionOn", {
        objectId: object.objectId,
        functionDeclaration: `function() {
          const seen = globalThis[${JSON.stringify(key)}].seen;
          if (seen.has(this)) return true;
          seen.add(this); return false;
        }`,
        returnByValue: true,
      });
      if (visited.result.value === true) continue;
      inventory.objects++;
      if (object.subtype === "arraybuffer") {
        const size = await inspector.post("Runtime.callFunctionOn", {
          objectId: object.objectId,
          functionDeclaration: "function() { return this.byteLength; }",
          returnByValue: true,
        });
        if (typeof size.result.value !== "number") throw Error("Missing inspected buffer size");
        inventory.bufferBytes += size.result.value;
        continue;
      }
      if (object.subtype === "typedarray" || object.subtype === "dataview") {
        const buffer = await inspector.post("Runtime.callFunctionOn", {
          objectId: object.objectId,
          functionDeclaration: "function() { return this.buffer; }",
          objectGroup: key,
        });
        pending.push(buffer.result);
        continue;
      }
      // Node exposes privateProperties at runtime; the installed CDP declarations predate it.
      const properties: Runtime.GetPropertiesReturnType & {
        privateProperties?: Array<{ name: string; value?: Runtime.RemoteObject }>;
      } = await inspector.post("Runtime.getProperties", {
        objectId: object.objectId,
        ownProperties: true,
      });
      for (const property of [...properties.result, ...(properties.privateProperties ?? [])]) {
        inventory.properties++;
        if (object.subtype === "array" && /^(0|[1-9]\d*)$/u.test(property.name))
          inventory.arraySlots++;
        if (property.value !== undefined) pending.push(property.value);
      }
      for (const property of properties.internalProperties ?? [])
        if (property.name === "[[Entries]]" && property.value !== undefined)
          pending.push(property.value);
    }
    return inventory;
  } finally {
    await inspector.post("Runtime.releaseObjectGroup", { objectGroup: key });
    inspector.disconnect();
    Reflect.deleteProperty(globalThis, key);
  }
}

for (const seed of seedsFor("scratch-resident-metadata", [887, 888, 889, 890]))
  it(`keeps inspected resident metadata fixed and prepaid as extents grow (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      data = fresh(),
      metadata = fresh(),
      backing = new FileScratch(data, () => undefined, metadata),
      control = controlBudget();
    let checkRelease = false,
      releasesChecked = 0;
    const scratch = {
      metadata: () => backing.metadata(),
      write: (offset: number, bytes: Uint8Array) => backing.write(offset, bytes),
      readInto: (offset: number, bytes: Uint8Array) => backing.readInto(offset, bytes),
      truncate: (size: number) => backing.truncate(size),
      async release(ranges: ReadonlyArray<{ readonly offset: number; readonly length: number }>) {
        if (checkRelease) {
          checkRelease = false;
          const duringRelease = await residentInventory(service, [scratch, control, refused]);
          // Only references into the prepaid 16-descriptor pool may be added during release.
          expect(duringRelease.objects).toBe(baseline.objects);
          expect(duringRelease.bufferBytes).toBe(baseline.bufferBytes);
          expect(duringRelease.arraySlots - baseline.arraySlots).toBeLessThanOrEqual(16);
          expect(duringRelease.properties - baseline.properties).toBeLessThanOrEqual(16);
          expect(control.used).toBe(workspaceBytes);
          releasesChecked++;
        }
        await backing.release(ranges);
      },
    };
    const service = new ScratchSpillService(scratch, { budget: control }),
      files = await Promise.all(Array.from({ length: 3 }, (_, i) => service.create(i, refused))),
      baseline = await residentInventory(service, [scratch, control, refused]),
      page = new Uint8Array(4096);
    // Independently observed three frames, plus prepaid native-response and control/release
    // allowances (4 KiB each). Their charge must already exist before the first append.
    expect(baseline.bufferBytes).toBe(3 * 4096);
    expect(baseline.arraySlots).toBe(16); // the actual pooled release descriptors
    expect(control.used).toBe(baseline.bufferBytes + 4096 + 4096);
    try {
      for (let i = 0; i < 3072; i++) {
        // First 2048 appends alternate, forcing >1000 separate runs per file. The remaining
        // appends use a generated rotation, keeping the number of live handles constant.
        const file = files[i < 2048 ? i % 2 : Math.floor(random() * files.length)];
        if (file === undefined) throw Error("Missing generated file");
        await file.append(page);
        if ([1, 15, 255, 1023, 2047, 3071].includes(i)) {
          const current = await residentInventory(service, [scratch, control, refused]);
          expect(current, `retained metadata grew after ${String(i + 1)} appends`).toEqual(
            baseline,
          );
          expect(control.used).toBe(current.bufferBytes + 4096 + 4096);
        }
      }
      reach.reach("inspected private ownership across growing extent counts");
      reach.reach("constant resident inventory matches prepaid workspace");
      checkRelease = true;
      const first = files[0];
      if (first === undefined) throw Error("Missing first file");
      await first.close();
      expect(releasesChecked).toBe(1);
      reach.reach("inspected bounded release bookkeeping");
      await service.close();
      expect((await residentInventory(service, [scratch, control, refused])).bufferBytes).toBe(0);
      expect(control.used).toBe(0);
      reach.reach("inspected buffers released with charge");
    } finally {
      await service.close();
    }
  }, 60_000);

for (const seed of seedsFor("scratch-checksum-grammar", [891, 892, 893, 894]))
  it(`refuses grammar-preserving scratch corruption before reuse (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed);
    for (const kind of ["offset", "length", "checksum", "padding"] as const)
      for (const position of ["first", "middle", "last"] as const) {
        const data = fresh(),
          metadata = fresh(),
          control = controlBudget(),
          backing = new FileScratch(data, () => undefined, metadata);
        let releases = 0;
        const scratch = {
          metadata: () => backing.metadata(),
          write: (offset: number, bytes: Uint8Array) => backing.write(offset, bytes),
          readInto: (offset: number, bytes: Uint8Array) => backing.readInto(offset, bytes),
          truncate: (size: number) => backing.truncate(size),
          async release(
            ranges: ReadonlyArray<{ readonly offset: number; readonly length: number }>,
          ) {
            releases++;
            await backing.release(ranges);
          },
        };
        const service = new ScratchSpillService(scratch, { budget: control }),
          a = await service.create(1, refused),
          b = await service.create(2, refused),
          page = new Uint8Array(4096),
          prefix = 1 + 2 * Math.floor(random() * 3),
          entries = 3 + Math.floor(random() * 4);
        try {
          for (let i = 0; i < prefix; i++) await b.append(page);
          for (let i = 0; i < entries; i++) {
            await a.append(page);
            await b.append(page); // flush A's page and make every A run nonadjacent
          }
          // A offsets are odd multiples of 4096. Doubling any of them addresses live B,
          // including the tail extension below, while preserving safe-integer/alignment grammar.
          while (b.length + a.length <= 2 * (prefix + 2 * entries) * 4096) await b.append(page);
          const frame = new Uint8Array(4096);
          // B owns metadata page zero; A owns page one, with all its generated entries.
          expect(metadata.read(frame, { at: 4096 })).toBe(4096);
          const view = new DataView(frame.buffer),
            index =
              position === "first"
                ? 0
                : position === "last"
                  ? entries - 1
                  : 1 + Math.floor(random() * (entries - 2));
          expect(view.getFloat64(8, true)).toBe(1);
          expect(view.getUint32(24, true)).toBe(entries);
          let at: number;
          if (kind === "offset" || kind === "length") {
            const field = 32 + index * 16 + (kind === "length" ? 8 : 0),
              original = frame.slice(field, field + 8);
            view.setFloat64(field, view.getFloat64(field, true) * 2, true);
            const changed = original.findIndex((byte, i) => byte !== frame[field + i]);
            expect(changed).toBeGreaterThanOrEqual(0);
            expect(original.filter((byte, i) => byte !== frame[field + i])).toHaveLength(1);
            at = field + changed;
            reach.reach(
              kind === "offset"
                ? "valid aligned offset redirected into live peer"
                : "valid aligned length changed",
            );
          } else {
            at =
              kind === "checksum"
                ? 28 + Math.floor(random() * 4)
                : 32 + entries * 16 + Math.floor(random() * (4096 - 32 - entries * 16));
            frame[at] = (frame[at] ?? 0) ^ (1 << Math.floor(random() * 8));
            reach.reach(
              kind === "checksum" ? "checksum byte flipped" : "unused padding byte flipped",
            );
          }
          // Change exactly one byte in scratch, preserving magic/owner/count/links and all
          // range integer/alignment checks. Only checksum verification can refuse this page.
          metadata.write(frame.subarray(at, at + 1), { at: 4096 + at });
          await expect(a.close()).rejects.toThrow("Malformed scratch allocator metadata");
          expect(releases).toBe(0);
          await expect(service.create(3, refused)).rejects.toThrow(
            "Malformed scratch allocator metadata",
          );
          reach.reach("corruption refused before release and reuse");
          reach.reach(`corrupted ${position} entry position`);
        } finally {
          await service.close();
        }
        expect(control.used).toBe(0);
      }
  }, 60_000);

afterAll(() => reach.assertReached());
