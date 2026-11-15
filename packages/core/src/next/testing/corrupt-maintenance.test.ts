/** PC2: maintenance consumes validated objects before it may publish derived roots. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  CORRUPT_CHECKPOINT_REACH,
  CORRUPT_MERGE_REACH,
  CORRUPT_MERGE_STRUCTURE_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import * as manifests from "../format/manifest-codec.js";
import { Governor } from "../memory/governor.js";
import { createOpfsBackend } from "../storage/opfs.js";
import { SimLocks } from "../storage/sim-disk.js";
import { openExtentSpace } from "../storage/space.js";
import type { StorageBackend } from "../storage/contract.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { WritePathImpl, type EngineInput } from "../write/engine.js";
import { ByteReader } from "../write/bytes.js";
import * as runs from "../write/runs.js";
import * as merging from "../write/merge.js";
import {
  autocommit,
  batch,
  column,
  INT,
  ints,
  tableInput,
  texts,
  rules,
} from "../write/test-support.js";

/** The ordinary R25 composition, with no debug full-walk audit. No storage implementation is
 * replaced. Defer initial maintenance only to make its explicit first consuming step observable. */
async function open(
  backend: StorageBackend,
  governor: Governor,
  tuning: NonNullable<EngineInput["tuning"]>,
  defer = false,
) {
  const startup = await governor.admit({
    label: "corruption startup",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  const lease = await backend.acquire(),
    session = await backend.open(lease);
  let space: Awaited<ReturnType<typeof openExtentSpace>> | undefined;
  try {
    space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      budget: startup,
      registerCache: (cache) => governor.registerCache(cache),
    });
    const opening = WritePathImpl.open({
      session,
      space,
      governor,
      options: { durability: "strict" },
      tuning,
      deferInitialMaintenance: defer,
    });
    startup.release(startup.held - startup.allocated - startup.loaned);
    const path = await opening,
      owned = space;
    return {
      path,
      space: owned,
      session,
      async close() {
        try {
          await path.close();
        } finally {
          try {
            await owned.close();
          } finally {
            startup.close();
            await session.close();
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    try {
      await space?.close();
    } finally {
      startup.close();
      await session.close();
      await lease.release();
    }
    throw error;
  }
}
function image(native: MemoryOpfs): string[] {
  return ["root-0", "root-1", "log"].map((name) =>
    Buffer.from(native.readFileBytes(name) ?? []).toString("base64"),
  );
}
function native() {
  const files = new MemoryOpfs();
  return {
    files,
    backend: createOpfsBackend({
      name: "corrupt-maintenance",
      directory: files.root,
      locks: new SimLocks(),
    }),
  };
}

for (const seed of seedsFor("next-corrupt-checkpoint", [0xb2a7, 0xb2a8]))
  it(`refuses a lazy staged root during checkpoint without publication seed ${String(seed)}`, async () => {
    const reach = createReach("corrupt checkpoint", CORRUPT_CHECKPOINT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 255 }),
        fc.integer({ min: 1, max: 100 }),
        async (unknown, salt) => {
          reach.beginHistory();
          const { files, backend } = native(),
            governor = new Governor(256 << 20);
          let release!: () => void;
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          const source = await open(backend, governor, {
            memtableRows: 4,
            merges: { mode: "off" },
            beforeFlushPublish: () => gate,
          });
          const encode = manifests.encodeGroupRoot;
          let damaged = 0;
          const mutation = vi.spyOn(manifests, "encodeGroupRoot").mockImplementation((root) => {
            const bytes = encode(root);
            bytes[4] = unknown;
            damaged++;
            return bytes;
          });
          let closed = false;
          try {
            await autocommit(source.path, (statement) =>
              statement.changeCatalog({
                kind: "createTable",
                table: tableInput("t", [column("v", INT)], { primaryKey: null }),
              }),
            );
            const tx = await source.path.begin(),
              statement = tx.statement({ rules });
            for (let call = 0; call < 6; call++)
              await statement
                .table(1)
                .insert(
                  batch(ints(Array.from({ length: 8 }, (_, row) => salt + call * 8 + row))),
                  null,
                );
            await statement.end();
            expect(source.path.maintenance.stages).toBeGreaterThan(0);
            await tx.commit(null);
            // Synchronous close fences the pending fold before releasing its publication barrier.
            const closing = source.close();
            closed = true;
            release();
            await closing;
            expect(damaged).toBeGreaterThan(0);
            expect(source.path.state().root.tables[0]?.staged.length).toBeGreaterThan(0);
          } finally {
            mutation.mockRestore();
            release();
            if (!closed) await source.close();
          }
          expect(governor.usedBytes).toBe(0);
          const owner = await open(
            backend,
            governor,
            { memtableRows: 4, merges: { mode: "off" } },
            true,
          );
          try {
            expect(owner.path.maintenance.publishedReplays).toBeGreaterThan(0);
            expect(owner.path.maintenanceState.initialStarted).toBe(false);
            const before = image(files);
            await owner.path.recycleLog();
            expect(owner.path.failure).toMatchObject({ code: "MINNOW_MANIFEST_CORRUPT" });
            expect(owner.path.failure?.message).not.toMatch(/checksum/iu);
            expect(owner.path.maintenance.stagedFolds).toBe(0);
            expect(image(files)).toEqual(before);
            reach.reach("checkpoint consumes staged root");
            reach.reach("typed corruption");
            reach.reach("root and WAL unchanged");
          } finally {
            await owner.close();
          }
          expect(governor.usedBytes).toBe(0);
          // The same physical files reopen; no partial fold replaced the staged root/log.
          const retry = await open(backend, governor, { merges: { mode: "off" } }, true);
          try {
            expect(retry.path.failure).toBeNull();
          } finally {
            await retry.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("reopenable");
          reach.reach("charges released");
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 120_000);

const flagReach = {
  1: "flag bit1",
  2: "flag bit2",
  3: "flag bit3",
  4: "flag bit4",
  5: "flag bit5",
  6: "flag bit6",
  7: "flag bit7",
} as const;

const MERGE_WITNESSES = [
  "merge consumes damaged block",
  "partial output discarded",
  "typed corruption",
  "root and WAL unchanged",
  "reopenable",
  "charges released",
] as const;

/** Shared ordinary native merge/publication history. No parser or storage path is replaced. */
async function corruptMerge(
  count: number,
  overflow: boolean,
  damage: (run: runs.EncodedRun, drained: number) => void,
  message?: string,
): Promise<void> {
  const { files, backend } = native(),
    governor = new Governor(256 << 20);
  const owner = await open(backend, governor, {
    memtableRows: 1024,
    merges: { mode: "manual", fanIn: 2, splitEntries: 16, sliceEntries: 32 },
  });
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Explicit .call(this) below.
  const finish = runs.RunBuilder.prototype.finish;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Explicit .call(this) below.
  const takeParts = runs.RunBuilder.prototype.takeParts;
  const drained = new WeakMap<runs.RunBuilder, number>();
  // Overflow payload parts may already have drained before finish. Track only the
  // scalar run-relative prefix, not their buffers or a synthetic storage path.
  const trackDrains = vi.spyOn(runs.RunBuilder.prototype, "takeParts").mockImplementation(function (
    this: runs.RunBuilder,
  ) {
    const parts = takeParts.call(this);
    drained.set(this, (drained.get(this) ?? 0) + parts.reduce((sum, part) => sum + part.length, 0));
    return parts;
  });
  let damaged = 0,
    built = 0;
  const mutate = vi.spyOn(runs.RunBuilder.prototype, "finish").mockImplementation(function (
    this: runs.RunBuilder,
  ) {
    const run = finish.call(this);
    if (run === null) return run;
    expect(run.encoding ?? 1).toBe(overflow ? 2 : 1);
    // Earlier inserts must not encounter corruption during their own uniqueness
    // checks. Only the final published input is damaged before its checksums.
    if (++built !== 3) return run;
    damage(run, drained.get(this) ?? 0);
    damaged++;
    return run;
  });
  const errors: unknown[] = [];
  let outputWrites = 0;
  const merge = merging.mergeRuns;
  const observe = vi.spyOn(merging, "mergeRuns").mockImplementation(async (input) => {
    try {
      return await merge(input);
    } catch (error) {
      errors.push(error);
      throw error;
    }
  });
  try {
    await autocommit(owner.path, (statement) =>
      statement.changeCatalog({
        kind: "createTable",
        table: tableInput("t", [column("v", overflow ? { kind: "text" } : INT, false)]),
      }),
    );
    for (let run = 0; run < 3; run++) {
      const values = Array.from({ length: count }, (_, row) => run * count + row);
      await autocommit(owner.path, (statement) =>
        statement
          .table(1)
          .insert(
            batch(
              overflow
                ? texts(values.map((value) => "x".repeat(3000) + String(value).padStart(8, "0")))
                : ints(values),
            ),
            null,
          ),
      );
      await owner.path.recycleLog();
      await owner.path.quiesce();
    }
    mutate.mockRestore();
    trackDrains.mockRestore();
    expect(built).toBe(3);
    expect(damaged).toBe(1);
    const write = owner.session.writeData.bind(owner.session);
    const writes = vi.spyOn(owner.session, "writeData").mockImplementation(async (...args) => {
      outputWrites++;
      await write(...args);
    });
    const before = image(files);
    try {
      await owner.path.runMaintenance();
    } finally {
      writes.mockRestore();
    }
    expect(errors.length).toBeGreaterThanOrEqual(2);
    for (const error of errors) {
      // Inline runs use the storage block grammar; the range parser's valid()
      // contract throws ManifestCorruptionError. Both are typed corruption.
      expect(error).toMatchObject({
        code: overflow ? "MINNOW_MANIFEST_CORRUPT" : "MINNOW_STORE_CORRUPT",
      });
      if (message !== undefined) expect((error as Error).message).toBe(message);
    }
    expect(owner.path.maintenance.merges).toBe(0);
    expect(outputWrites).toBeGreaterThan(0);
    expect(image(files)).toEqual(before);
    expect(owner.path.failure).toBeNull();
  } finally {
    mutate.mockRestore();
    trackDrains.mockRestore();
    observe.mockRestore();
    await owner.close();
  }
  expect(governor.usedBytes).toBe(0);
  const retry = await open(backend, governor, { merges: { mode: "manual" } }, true);
  try {
    expect(retry.path.failure).toBeNull();
  } finally {
    await retry.close();
  }
  expect(governor.usedBytes).toBe(0);
}

for (const seed of seedsFor("next-corrupt-run-merge", [0xb2a9, 0xb2aa]))
  it(`refuses checksummed run flags after partial merge output seed ${String(seed)}`, async () => {
    const reach = createReach("corrupt run merge", CORRUPT_MERGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 540, max: 600 }),
        fc.integer({ min: 1, max: 7 }),
        async (count, start) => {
          reach.beginHistory();
          for (const bit of ([1, 2, 3, 4, 5, 6, 7] as const)
            .slice(start)
            .concat(([1, 2, 3, 4, 5, 6, 7] as const).slice(0, start)))
            for (const overflow of [false, true]) {
              await corruptMerge(count, overflow, (run, drained) => {
                const index = run.parts[run.parts.length - 3];
                if (index === undefined) throw new Error("Missing native run index");
                const reader = new ByteReader(index),
                  blocks = reader.varuint();
                expect(blocks).toBeGreaterThan(1);
                const parts = new Map<number, Uint8Array>();
                let offset = drained;
                for (const part of run.parts) {
                  parts.set(offset, part);
                  offset += part.length;
                }
                for (let block = 0; block < blocks; block++) {
                  const at = reader.varuint(),
                    length = reader.varuint(),
                    checksumAt = reader.offset;
                  reader.u32();
                  if (block !== blocks - 1) continue;
                  const bytes = parts.get(at);
                  if (bytes?.length !== length) throw new Error("Missing last native block");
                  bytes[bytes.length - 1] = (bytes[bytes.length - 1] ?? 0) | (1 << bit);
                  new DataView(index.buffer, index.byteOffset).setUint32(
                    checksumAt,
                    xxHash32(bytes),
                    true,
                  );
                }
              });
              reach.reach(overflow ? "overflow run" : "inline run");
              reach.reach(flagReach[bit]);
              for (const state of MERGE_WITNESSES) reach.reach(state);
            }
        },
      ),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  }, 180_000);

const STRUCTURAL_DAMAGE = [
  ["short block", "Run block is truncated"],
  ["zero block count", "Run block offsets are invalid"],
  ["offset table bounds", "Run block offsets are invalid"],
  ["block byte cap", "Run block offsets are invalid"],
  ["offset tiling", "Run block entries do not tile its bytes"],
  ["offset outside block", "Run block entries do not tile its bytes"],
  ["truncated key length", "Run key length is truncated"],
  ["nonminimal key length", "Run key length is not canonical"],
  ["continued key length", "Run key length is not canonical"],
  ["key width cap", "Run block entry is truncated or too wide"],
  ["entry bounds", "Run block entry is truncated or too wide"],
  ["trailing block bytes", "Run block has trailing bytes"],
] as const;
type StructuralDamage = (typeof STRUCTURAL_DAMAGE)[number][0];

/** Damage before persistence/hashing. References with a shortened body retain the index
 * field's byte width (the run-index grammar accepts redundant varuint groups); the decoded
 * reference and its checksum still name exactly the damaged stored byte span. */
function damageStructure(run: runs.EncodedRun, kind: StructuralDamage): void {
  const index = run.parts[run.parts.length - 3];
  if (index === undefined) throw new Error("Missing structural run index");
  const reader = new ByteReader(index),
    count = reader.varuint(),
    refs = [];
  for (let i = 0; i < count; i++) {
    const offsetAt = reader.offset,
      offset = reader.varuint(),
      lengthAt = reader.offset,
      length = reader.varuint(),
      checksumAt = reader.offset;
    reader.u32();
    refs.push({ offsetAt, offset, lengthAt, length, checksumAt });
  }
  reader.end();
  expect(refs.length).toBeGreaterThan(2);
  const ref = refs.at(kind === "block byte cap" || kind === "key width cap" ? -2 : -1);
  if (ref === undefined) throw new Error("Missing target block");
  const parts = new Map<number, Uint8Array>();
  let total = 0;
  for (const part of run.parts) {
    parts.set(total, part);
    total += part.length;
  }
  const bytes = parts.get(ref.offset);
  if (bytes === undefined) throw new Error("Missing structural block bytes");
  expect(bytes.length).toBe(ref.length);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.length),
    entries = view.getUint16(0, true),
    last = view.getUint16(2 + (entries - 1) * 2, true),
    previous = view.getUint16(2 + (entries - 2) * 2, true),
    first = view.getUint16(2, true);
  expect(entries).toBeGreaterThan(2);
  expect(bytes[last]).toBeLessThan(128);
  let length = ref.length;
  switch (kind) {
    case "short block":
      length = 1;
      break;
    case "zero block count":
      view.setUint16(0, 0, true);
      break;
    case "offset table bounds":
      view.setUint16(0, bytes.length, true);
      break;
    case "block byte cap":
      length = runs.BLOCK_BYTES + 3;
      break;
    case "offset tiling":
      view.setUint16(2, first + 1, true);
      break;
    case "offset outside block":
    case "truncated key length": {
      // A valid preceding entry reaches this boundary, so the position/length guard is
      // exercised independently of the earlier tiling equality and entry-end checks.
      const end = bytes.length - (kind === "truncated key length" ? 1 : 0),
        keyLength = end - previous - 1 - 7;
      expect(keyLength).toBeGreaterThan(0);
      expect(keyLength).toBeLessThan(128);
      bytes[previous] = keyLength;
      view.setUint16(2 + (entries - 1) * 2, end, true);
      if (kind === "truncated key length") bytes[end] = 128;
      break;
    }
    case "nonminimal key length":
      // Two-byte encoding of K-1 has the SAME total entry length as one-byte K.
      // Bounds, offsets and the final end remain valid if canonicality is bypassed.
      bytes[last] = 128 | ((bytes[last] ?? 0) - 1);
      bytes[last + 1] = 0;
      break;
    case "continued key length":
      bytes[last] = 128;
      bytes[last + 1] = 128;
      break;
    case "key width cap":
      bytes[first] = 128 | ((runs.MAX_KEY_BYTES + 1) & 127);
      bytes[first + 1] = (runs.MAX_KEY_BYTES + 1) >>> 7;
      expect(first + 2 + runs.MAX_KEY_BYTES + 1 + 7).toBeLessThan(bytes.length);
      break;
    case "entry bounds":
      bytes[last] = 127;
      break;
    case "trailing block bytes":
      bytes[last] = (bytes[last] ?? 0) - 1;
      break;
  }
  // Build the exact underlying range, including an adjacent block when testing the cap.
  // Inline block refs may overlap; the space's union-accounted reads verify both hashes.
  const all = new Uint8Array(total);
  let at = 0;
  for (const part of run.parts) {
    all.set(part, at);
    at += part.length;
  }
  expect(ref.offset + length).toBeLessThanOrEqual(all.length);
  let value = length;
  for (let pos = ref.lengthAt; pos < ref.checksumAt; pos++) {
    index[pos] = (value & 127) | (pos + 1 < ref.checksumAt ? 128 : 0);
    value = Math.floor(value / 128);
  }
  expect(value).toBe(0);
  new DataView(index.buffer, index.byteOffset).setUint32(
    ref.checksumAt,
    xxHash32(all, 0, ref.offset, ref.offset + length),
    true,
  );
}

for (const seed of seedsFor("next-corrupt-run-structure", [0xb2b7, 0xb2b8]))
  it(`refuses each run structure before merge publication seed ${String(seed)}`, async () => {
    const reach = createReach("structural run merge", CORRUPT_MERGE_STRUCTURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 840, max: 900 }),
        fc.integer({ min: 0, max: STRUCTURAL_DAMAGE.length - 1 }),
        async (count, start) => {
          reach.beginHistory();
          for (const [kind, message] of STRUCTURAL_DAMAGE.slice(start).concat(
            STRUCTURAL_DAMAGE.slice(0, start),
          )) {
            await corruptMerge(
              count,
              false,
              (run, drained) => {
                expect(drained).toBe(0);
                damageStructure(run, kind);
              },
              message,
            );
            reach.reach(kind);
            reach.reach("checksums valid");
            for (const state of MERGE_WITNESSES) reach.reach(state);
          }
        },
      ),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  }, 180_000);
