import fc from "fast-check";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, WRITE_KEY_HASH_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import * as canonicalHash from "../hash/xxhash32.js";
import * as hashing from "./key-hashes.js";
import { KeyDelta } from "./key-hash.js";
import { RunBuilder } from "./runs.js";

const reach = createReach("write-key-hash-reuse", WRITE_KEY_HASH_REACH);
afterAll(() => reach.assertReached());
const oracle = (bytes: Uint8Array, start: number, end: number) => ({
  first: xxHash32(bytes, 0, start, end),
  second: xxHash32(bytes, 0x9747b28c, start, end),
});

it("preserves both canonical seeds for all byte values and dispatch edges", () => {
  const out = { first: 0, second: 0 };
  for (const length of [...Array<number>(34).keys(), 255, 256, 257, 2048, 4096]) {
    for (let value = 0; value < 256; value++) {
      const backing = new Uint8Array(length + 8).fill(value);
      const view = backing.subarray(3);
      hashing.hashKey(view, 1, 1 + length, out);
      expect(out).toEqual(oracle(view, 1, 1 + length));
    }
  }
});

for (const seed of seedsFor("write-key-hash-reuse", [0x5eed, 0x91])) {
  it(`retains the hashes across growth, rollback, traversal and run encoding (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.oneof(
            fc.uint8Array({ maxLength: 17 }),
            fc.constantFrom(
              new Uint8Array([48, 91, 153, 195, 141]),
              new Uint8Array([185, 111, 69, 209, 253]),
            ),
            fc.uint8Array({ minLength: 18, maxLength: 2048 }),
            fc.uint8Array({ minLength: 257, maxLength: 2048 }),
          ),
          { minLength: 20, maxLength: 40 },
        ),
        fc.integer({ min: 0, max: 15 }),
        async (keys, lead) => {
          reach.beginHistory();
          let paid = 0;
          const delta = new KeyDelta({
            allocate: (n) => {
              paid += n;
              return true;
            },
            free: (n) => {
              paid -= n;
            },
          });
          const out = { first: 0, second: 0 };
          const canonical = new RunBuilder();
          const cached = new RunBuilder();
          const seenHashes = new Map<number, string>();
          for (let row = 0; row < keys.length; row++) {
            const key = keys[row] ?? new Uint8Array(0);
            const backing = new Uint8Array(lead + key.length + 8).fill(0xed);
            const view = backing.subarray(lead);
            view.set(key, 3);
            hashing.hashKey(view, 3, 3 + key.length, out);
            expect(out).toEqual(oracle(key, 0, key.length));
            const prior = seenHashes.get(out.first),
              value = key.toString();
            if (prior !== undefined && prior !== value) reach.reach("first hash collision");
            seenHashes.set(out.first, value);
            if (key.length === 0) reach.reach("empty");
            if (key.length === 5) reach.reach("five bytes");
            if (key.length >= 16) reach.reach("stripes");
            if (key.length > 256) reach.reach("long key");
            if (key.some((v) => v >= 128)) reach.reach("high bytes");
            if (lead > 0) reach.reach("shifted range");
            const entry = delta.add(view, 3, 3 + key.length, row, 1, false);
            const slot = delta.keyOf(entry);
            expect(delta.keyHash(slot)).toBe(out.first);
            expect(delta.keyBloomHash(slot)).toBe(out.second);
            expect(delta.find(view, 3, 3 + key.length, out.first)).toBe(slot);
            // A source buffer can be reused immediately after add, without invalidating metadata.
            view.fill(0);
          }
          if (delta.keyCount > 16) reach.reach("growth");
          // Popped entries leave immutable key slots and their hashes available for reinsertion.
          const last = delta.entryCount - 1;
          const slot = delta.keyOf(last);
          const key = delta.keyBytes(slot).slice();
          delta.pop(last);
          delta.add(key, 0, key.length, last, 1, false);
          reach.reach("rollback and reinsert");
          const spy = vi.spyOn(hashing, "hashKey");
          try {
            await delta.forEachSorted(1, (bytes, start, end, rowId, tombstone, hash, bloomHash) => {
              expect({ first: hash, second: bloomHash }).toEqual(oracle(bytes, start, end));
              cached.addRange(bytes, start, end, rowId, tombstone, hash, bloomHash);
              canonical.addRange(bytes, start, end, rowId, tombstone);
            });
            // Finish the cached run before the reference: supplying hashes must traverse no key.
            const before = spy.mock.calls.length;
            const actual = cached.finish();
            expect(spy.mock.calls.length).toBe(before);
            expect(actual).toEqual(canonical.finish());
            reach.reach("identical run bytes");
          } finally {
            spy.mockRestore();
            delta.releaseCharge();
          }
          expect(paid).toBe(0);
          reach.reach("released charge");
        },
      ),
      { seed, numRuns: 300 },
    );
  });
}

it("uses one pair per add and none for supplied-hash lookup or flush", async () => {
  const spy = vi.spyOn(hashing, "hashKey");
  const canonicalSpy = vi.spyOn(canonicalHash, "xxHash32");
  const delta = new KeyDelta();
  const builder = new RunBuilder();
  try {
    for (let i = 0; i < 100; i++) {
      const key = new Uint8Array([1, 0, 0, 0, i]);
      delta.add(key, 0, 5, i, 1, false);
    }
    expect(spy).toHaveBeenCalledTimes(100);
    for (let slot = 0; slot < delta.keyCount; slot++)
      expect(
        delta.find(delta.keyArena(), delta.keyStart(slot), delta.keyEnd(slot), delta.keyHash(slot)),
      ).toBe(slot);
    await delta.forEachSorted(1, (bytes, start, end, rowId, tombstone, hash, bloomHash) =>
      builder.addRange(bytes, start, end, rowId, tombstone, hash, bloomHash),
    );
    expect(builder.finish()?.entryCount).toBe(100);
    expect(spy).toHaveBeenCalledTimes(100);
    expect(
      canonicalSpy.mock.calls.filter(
        ([bytes, , start = 0, end = bytes.length]) => end - start === 5,
      ),
    ).toHaveLength(0);
  } finally {
    spy.mockRestore();
    canonicalSpy.mockRestore();
    delta.releaseCharge();
  }
});

it("reuses a repeated disk key across blocks and Bloom partitions, preserving cancellation", () => {
  const key = new Uint8Array(1900).fill(0xff);
  const builder = new RunBuilder();
  const reference = new RunBuilder();
  const hashes = oracle(key, 0, key.length);
  const spy = vi.spyOn(hashing, "hashKey");
  try {
    for (let row = 0; row < 600; row++) {
      builder.add(key, row, false);
      reference.addRange(key, 0, key.length, row, false, hashes.first, hashes.second);
      if (row % 13 === 0) {
        builder.add(key, row, true);
        reference.addRange(key, 0, key.length, row, true, hashes.first, hashes.second);
      }
    }
    expect(builder.finish()).toEqual(reference.finish());
    expect(spy).toHaveBeenCalledTimes(1);
  } finally {
    spy.mockRestore();
  }
});
