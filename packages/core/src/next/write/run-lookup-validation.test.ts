/**
 * A run lookup or cursor validates every block it reads, once per page cache residency.
 *
 * Law (write/runs.ts "Stored form", storage/verify-cached.ts, storage/extents.ts `attach`): a run
 * block whose structure is invalid is refused with `StoreCorruptionError`, whatever its checksum.
 * Cached, verified bytes are immutable, so a validated block may carry a mark on its cache entry
 * (as a parsed page does) and a later read of the same entry's bytes need not walk it again. The
 * mark goes with the entry: a block read again after eviction is validated again. A block that
 * failed validation carries no mark, and a caller-owned buffer (`blockEntries`) is always
 * validated: nothing ties its contents to what was validated before.
 *
 * Runs live in a real `MemoryExtentSpace`, which verifies each blob's checksum when it reads it.
 * A block is damaged before its checksum is recorded in the run's block index, as
 * testing/corrupt-maintenance.test.ts does, so the checksum names the damaged bytes and only
 * structural validation can refuse them.
 */
import { describe, expect, it } from "vitest";
import type { BlobRef } from "../format/manifest.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { ByteReader, need, UNCHARGED } from "./bytes.js";
import { blockEntries, RunBuilder, RunReader, runRef, type RunEntry } from "./runs.js";

const KEYS = 2000;
const KEY_BYTES = 8;

function key(ordinal: number): Uint8Array {
  const bytes = new Uint8Array(KEY_BYTES);
  new DataView(bytes.buffer).setUint32(4, ordinal);
  return bytes;
}

const DAMAGE = {
  "zero entry count": (view: DataView) => {
    view.setUint16(0, 0, true);
  },
  "offset tiling": (view: DataView) => {
    view.setUint16(2, view.getUint16(2, true) + 1, true);
  },
  "entry flags": (view: DataView) => {
    // The first entry: one key-length byte, the key, a 6-byte row id, then its flags byte.
    view.setUint8(view.getUint16(2, true) + 1 + KEY_BYTES + 6, 0xfe);
  },
  "trailing bytes": (view: DataView) => {
    const count = view.getUint16(0, true),
      last = view.getUint16(2 + 2 * (count - 1), true);
    view.setUint8(last, view.getUint8(last) - 1);
  },
} as const;
type Damage = keyof typeof DAMAGE;

const MESSAGES: Record<Damage, RegExp> = {
  "zero entry count": /Run block offsets are invalid/,
  "offset tiling": /Run block entries do not tile its bytes/,
  "entry flags": /run entry flags/,
  "trailing bytes": /Run block has trailing bytes/,
};

const viewOf = (bytes: Uint8Array) => new DataView(bytes.buffer, bytes.byteOffset, bytes.length);

/** A run of `KEYS` keys stored in a real extent space, block 0 optionally damaged before hashing. */
async function storedRun(damage?: Damage) {
  const builder = new RunBuilder();
  for (let k = 0; k < KEYS; k++) builder.add(key(k), k, false);
  const run = need(builder.finish());
  // Blocks and Bloom partitions first, then index, fences and directory: part 0 is block 0.
  const block = need(run.parts[0]);
  if (damage !== undefined) {
    DAMAGE[damage](viewOf(block));
    // The block index records each block's checksum: record the damaged bytes'.
    const index = need(run.parts[run.parts.length - 3]);
    const reader = new ByteReader(index);
    expect(reader.varuint()).toBeGreaterThan(1);
    expect(reader.varuint(), "block 0 is the run's first part").toBe(0);
    expect(reader.varuint()).toBe(block.length);
    viewOf(index).setUint32(reader.offset, xxHash32(block), true);
  }
  const total = run.parts.reduce((n, part) => n + part.length, 0);
  const space = new MemoryExtentSpace();
  const extent = { offset: 0, length: total };
  let at = 0;
  const refs: BlobRef[] = run.parts.map((part) => {
    space.write(at, part);
    const ref = { extent, offset: at, length: part.length, checksum: xxHash32(part) };
    at += part.length;
    return ref;
  });
  const reader = await RunReader.open(space, runRef(run, refs), UNCHARGED, UNCHARGED);
  const lookup = async (k: number): Promise<RunEntry[]> => {
    const out: RunEntry[] = [];
    await reader.lookup(space, key(k), out, UNCHARGED);
    return out;
  };
  const firstEntries = async (count: number): Promise<number[]> => {
    const rows: number[] = [];
    await reader.cursor(space, UNCHARGED, (_, rowId) => {
      rows.push(rowId);
      return rows.length === count;
    });
    return rows;
  };
  return { space, lookup, firstEntries, block: block.slice() };
}

describe("a damaged block", () => {
  for (const damage of Object.keys(DAMAGE) as Damage[])
    it(`is refused by every read (${damage})`, async () => {
      const { space, lookup, firstEntries } = await storedRun(damage);
      const refused = {
        name: "StoreCorruptionError",
        message: expect.stringMatching(MESSAGES[damage]) as unknown,
      };
      // Read from storage (checksum verified), then served from the cache: refused both times.
      await expect(lookup(0), "first lookup").rejects.toMatchObject(refused);
      await expect(lookup(0), "the same cached block again").rejects.toBeInstanceOf(
        StoreCorruptionError,
      );
      await expect(lookup(1), "another key in it").rejects.toMatchObject(refused);
      await expect(firstEntries(1), "a cursor over it").rejects.toMatchObject(refused);
      expect(space.attached, "a refused block carries no validation mark").toBe(0);
      // A key in an undamaged block is still served.
      expect(await lookup(KEYS - 1)).toEqual([{ rowId: KEYS - 1, tombstone: false }]);
    });
});

it("validates a cached block once per residency, and again after eviction", async () => {
  const { space, lookup, firstEntries } = await storedRun();
  expect(await lookup(0)).toEqual([{ rowId: 0, tombstone: false }]);
  const marked = space.attached;
  expect(marked, "the validated block is marked on its cache entry").toBe(1);
  const hits = space.parsedHits;
  for (let round = 0; round < 3; round++)
    for (const k of [0, 1, 2])
      expect(await lookup(k), `key ${String(k)}, round ${String(round)}`).toEqual([
        { rowId: k, tombstone: false },
      ]);
  expect(await firstEntries(3)).toEqual([0, 1, 2]);
  expect(space.attached, "later reads of the same bytes add no mark").toBe(marked);
  expect(space.parsedHits - hits, "every later read found the mark").toBe(10);
  space.evict();
  expect(await lookup(0)).toEqual([{ rowId: 0, tombstone: false }]);
  expect(space.attached, "bytes read again after eviction are validated again").toBe(marked + 1);
  expect(await lookup(KEYS + 5)).toEqual([]);
});

it("always validates a caller-owned buffer in blockEntries, even one it decoded before", async () => {
  const { block } = await storedRun();
  const out: Parameters<typeof blockEntries>[1] = [];
  blockEntries(block, out);
  expect(out.length).toBeGreaterThan(1);
  expect(out[0]).toMatchObject({ rowId: 0, tombstone: false });
  for (const damage of Object.keys(DAMAGE) as Damage[]) {
    const buffer = block.slice();
    blockEntries(buffer, []);
    // The same array, rewritten in place after a valid decode.
    DAMAGE[damage](viewOf(buffer));
    expect(() => blockEntries(buffer, []), damage).toThrow(MESSAGES[damage]);
  }
});
