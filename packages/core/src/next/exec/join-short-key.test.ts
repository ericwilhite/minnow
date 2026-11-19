/** B9: byte equality and charged bounded work, independent of generator dispatch.
 * Authority: join-keys.ts SQL equality contract and KeyWork's spend-before-progress rule.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, JOIN_SHORT_KEY_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import * as hashing from "../hash/xxhash32.js";
import { XxHash32Stream } from "../hash/xxhash32.js";
import { ColumnStore } from "./column-store.js";
import { TEXT, batchesOf, vectorOf, type Row } from "./join-fixtures.js";
import {
  HASH_SEED,
  LONG_KEY_BYTES,
  KeyLayout,
  KeyScratch,
  KeyWork,
  extractKeys,
  extractKeysJob,
} from "./join-keys.js";
import { JoinTable } from "./join-table.js";
import { TaskClock } from "./pipeline.js";
import { Slice } from "./slices.js";
import type { Vector } from "../vector/vector.js";

const encoder = new TextEncoder();
const reach = createReach("b9-short-join-keys", JOIN_SHORT_KEY_REACH);
function ledger() {
  let bytes = 0;
  return {
    allocate(n: number) {
      bytes += n;
      return true;
    },
    free(n: number) {
      bytes -= n;
      expect(bytes).toBeGreaterThanOrEqual(0);
    },
    get bytes() {
      return bytes;
    },
  };
}
/** The same logical key bytes at nonzero buffer and offset bases. */
function shifted(vector: Vector, salt: number): Vector {
  if (vector.form === "constant") throw Error("Fixture expects flat/dictionary");
  const flat = vector.form === "dictionary" ? vector.dictionary : vector;
  if (flat.data.layout !== "varlen") throw Error("Fixture expects bytes");
  const byteOffset = (salt % 15) + 1,
    base = salt % 7;
  const backing = new Uint8Array(byteOffset + base + flat.data.bytes.length + 3);
  backing.set(flat.data.bytes, byteOffset + base);
  const changed = {
    ...flat,
    data: {
      ...flat.data,
      bytes: backing.subarray(byteOffset, byteOffset + base + flat.data.bytes.length),
      offsets: flat.data.offsets.map((value) => value + base),
    },
  };
  return vector.form === "dictionary" ? { ...vector, dictionary: changed } : changed;
}
function run<T>(job: Generator<Promise<void> | void, T, void>, work: KeyWork, slice: Slice) {
  let yields = 0,
    max = 0;
  for (;;) {
    const before = work.total;
    const step = job.next();
    max = Math.max(max, work.total - before);
    if (step.done === true) return { value: step.value, yields, max };
    yields++;
    slice.clock.begin();
  }
}

it("generated boundary keys keep exact hashes, row IDs and dictionary-local values", () => {
  const viewed = vi.spyOn(hashing, "xxHash32View");
  try {
    for (const seed of seedsFor("b9-short-join-extract", [0xb9f71, 0xb9f72]))
      fc.assert(
        fc.property(fc.boolean(), fc.integer({ min: 0, max: 255 }), (dictionary, salt) => {
          reach.beginHistory();
          const values = [
            "",
            "a",
            "x".repeat(63),
            "x".repeat(64),
            "x".repeat(65),
            "y".repeat(80),
            "é".repeat(32),
            `tail${String(salt)}`,
            null,
          ];
          const viewBefore = viewed.mock.calls.length;
          const vector = shifted(vectorOf(TEXT, values, { dictionary }), salt);
          const ids = Int32Array.from(values, (_, i) => values.length - i - 1);
          const layout = new KeyLayout([TEXT], [false]);
          const charged = ledger();
          const scratch = new KeyScratch(layout, values.length, charged);
          const sync = new KeyScratch(layout, values.length, charged);
          const slice = new Slice(0, new TaskClock(() => performance.now()));
          const work = new KeyWork(1);
          extractKeys([vector], ids, 0, values.length, sync);
          const out = run(
            extractKeysJob([vector], ids, 0, values.length, scratch, work, slice, new Map()),
            work,
            slice,
          );
          expect(out.value).toBe(values.length);
          let byteWork = 0;
          for (let k = 0; k < values.length; k++) {
            const v = values[ids[k] ?? 0];
            const bytes = encoder.encode(v ?? "");
            const expected = v === null ? 0 : hashing.xxHash32(bytes, HASH_SEED);
            expect(scratch.hashes[k]).toBe(expected);
            expect(scratch.hashes[k]).toBe(sync.hashes[k]);
            expect(scratch.hashes2[k]).toBe(sync.hashes2[k]);
            if (v !== null) byteWork += bytes.length * (bytes.length > LONG_KEY_BYTES ? 2 : 1);
          }
          // Each dictionary entry occurs once here; equal local codes in a later vector
          // are deliberately different values and must not reuse this vector's memo.
          const other = shifted(
            vectorOf(
              TEXT,
              values.map((v) => (v === null ? null : v + "z")),
              { dictionary },
            ),
            salt + 9,
          );
          const otherWork = new KeyWork(1);
          run(
            extractKeysJob([other], ids, 0, values.length, sync, otherWork, slice, new Map()),
            otherWork,
            slice,
          );
          expect(sync.hashes[values.length - 1]).not.toBe(scratch.hashes[values.length - 1]);
          expect(work.total).toBe(byteWork);
          expect(out.yields).toBeGreaterThan(0);
          for (const s of [
            "empty",
            "one byte",
            "63 bytes",
            "64 bytes",
            "65 bytes",
            "long fallback",
            "reordered selection",
            "due hash yield",
          ] as const)
            reach.reach(s);
          reach.reach(dictionary ? "dictionary" : "flat");
          expect(viewed.mock.calls.length).toBeGreaterThan(viewBefore);
          reach.reach("view hash");
          reach.reach("source byte offset");
          if (salt % 7 > 0) reach.reach("source offset base");
          charged.free(2 * KeyScratch.bytes(layout, values.length));
          expect(charged.bytes).toBe(0);
          reach.reach("zero charges");
        }),
        {
          seed,
          numRuns: 24,
          examples: [
            [false, 0],
            [true, 255],
          ],
        },
      );
  } finally {
    viewed.mockRestore();
  }
}, 60_000);

it("forced short-key collisions preserve duplicate heads and exact comparison work before progress", () => {
  const one = vi.spyOn(hashing, "xxHash32").mockReturnValue(42);
  const viewed = vi.spyOn(hashing, "xxHash32View").mockReturnValue(42);
  const digest = vi.spyOn(XxHash32Stream.prototype, "digest").mockReturnValue(42);
  try {
    for (const seed of seedsFor("b9-short-join-collisions", [0xb9f73, 0xb9f74]))
      fc.assert(
        fc.property(fc.constantFrom(1, 31, 63, 64, 65, 80), fc.boolean(), (length, dictionary) => {
          reach.beginHistory();
          const key = (last: string) => "x".repeat(length - 1) + last;
          // Same length and hash, differing only in their final byte. Two build rows
          // of each value; head is the last row, next is the preceding duplicate.
          const rows: Row[] = [[key("a")], [key("a")], [key("b")], [key("b")]];
          const charged = ledger();
          const store = new ColumnStore([TEXT], charged);
          for (const batch of batchesOf([TEXT], rows, { dictionary }))
            expect(store.append(batch.columns, null, 0, batch.length)).toBe(batch.length);
          const layout = new KeyLayout([TEXT], [false]);
          const scratch = new KeyScratch(layout, 2048, charged);
          const slice = new Slice(0, new TaskClock(() => performance.now()));
          const buildWork = new KeyWork(1);
          const built = run(
            JoinTable.buildJob(store, [0], layout, 0, charged, scratch, slice, {}, buildWork),
            buildWork,
            slice,
          );
          const table = built.value;
          if (table === null) throw Error("table was not funded");
          const probe = vectorOf(TEXT, [key("a"), key("b"), key("c"), key("a") + "z"], {
            dictionary,
          });
          const work = new KeyWork(1);
          extractKeys([probe], null, 0, 4, scratch);
          const heads = new Int32Array(4);
          const found = run(table.lookupJob(scratch, 4, heads, work, slice), work, slice);
          expect([...heads]).toEqual([2, 4, 0, 0]);
          expect(table.next?.[1]).toBe(1);
          expect(table.next?.[3]).toBe(3);
          expect(found.value).toBe(2);
          // Successful a compares one slot; successful b compares two; missing c
          // compares two. The fourth probe has a different length and spends no
          // compared bytes. Every rejected slot spends its 12-byte cursor work.
          expect(work.total).toBe(5 * length + 5 * 12);
          expect(found.yields).toBeGreaterThan(0);
          expect(found.max).toBeLessThanOrEqual(8192 + 64);
          reach.reach("collision mismatch");
          reach.reach("duplicate chain");
          reach.reach("due comparison yield");
          table.close();
          store.close();
          charged.free(KeyScratch.bytes(layout, 2048));
          expect(charged.bytes).toBe(0);
          reach.reach("zero charges");
        }),
        {
          seed,
          numRuns: 24,
          examples: [
            [64, false],
            [65, true],
          ],
        },
      );
  } finally {
    viewed.mockRestore();
    one.mockRestore();
    digest.mockRestore();
  }
  reach.assertReached();
}, 60_000);
