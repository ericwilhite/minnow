/**
 * The path-compressed registry directory gives exactly the uncompressed one's get/put results.
 *
 * Law (owner/registry-directory.ts): the directory is a map from (kind, owner, record ID) to a
 * 128-byte record; how the radix path is stored (29 fixed levels, or compressed skips that split
 * when a key leaves one) changes the spill work, never what a get returns. The reference is #55's
 * uncompressed directory, kept unchanged (registry-directory-uncompressed.test-support.ts).
 *
 * Keys are drawn to make compression work hard: a few namespaces and owners, and record IDs built
 * from a base that varies one digit at a random depth, so new keys leave existing paths deep
 * inside a skip and split it. More distinct blocks than resident leaves force flushes and reads.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, REGISTRY_COMPRESSION_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  RegistryDirectory,
  REGISTRY_DIRECTORY_BYTES,
  REGISTRY_DIRECTORY_WORK,
  REGISTRY_RECORD_BYTES,
} from "./registry-directory.js";
import {
  UncompressedRegistryDirectory,
  UNCOMPRESSED_DIRECTORY_BYTES,
} from "./registry-directory-uncompressed.test-support.js";

const budget = (limit: number) => {
  let used = 0;
  return {
    allocate: (n: number) => {
      if (used + n > limit) return false;
      used += n;
      return true;
    },
    free: (n: number) => {
      used -= n;
    },
  };
};

/** A record ID: a base block varied in one hex digit at `depth`, then a row within the block. */
const recordId = fc
  .record({
    base: fc.integer({ min: 0, max: 2 ** 20 }),
    depth: fc.integer({ min: 0, max: 12 }),
    digit: fc.integer({ min: 0, max: 15 }),
    row: fc.integer({ min: 0, max: 15 }),
  })
  .map(({ base, depth, digit, row }) => {
    const unit = 16 ** depth,
      block = base - (Math.floor(base / unit) % 16) * unit + digit * unit;
    return Math.min(block, Math.floor(Number.MAX_SAFE_INTEGER / 16) - 1) * 16 + row;
  });
const operation = fc.record({
  put: fc.boolean(),
  kind: fc.integer({ min: 0, max: 3 }),
  owner: fc.constantFrom(0, 0, 0, 7, 2 ** 40 + 3),
  id: recordId,
  fill: fc.integer({ min: 0, max: 255 }),
});

it.each(seedsFor("next-registry-compression", [0xc0de, 0xc0df]))(
  "returns exactly the uncompressed directory's records (seed %i)",
  async (seed) => {
    const reach = createReach("registry path compression", REGISTRY_COMPRESSION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.array(operation, { minLength: 20, maxLength: 160 }), async (ops) => {
        reach.beginHistory();
        const compressed = new RegistryDirectory(
            budget(REGISTRY_DIRECTORY_BYTES),
            new MemorySpillService(),
            1,
            new AbortController().signal,
          ),
          reference = new UncompressedRegistryDirectory(
            budget(UNCOMPRESSED_DIRECTORY_BYTES),
            new MemorySpillService(),
            1,
            new AbortController().signal,
          );
        const before = { ...REGISTRY_DIRECTORY_WORK };
        const record = new Uint8Array(REGISTRY_RECORD_BYTES),
          a = new Uint8Array(REGISTRY_RECORD_BYTES),
          b = new Uint8Array(REGISTRY_RECORD_BYTES);
        try {
          for (const op of ops) {
            if (op.put) {
              record.fill(op.fill);
              record[0] = op.kind;
              await compressed.put(op.kind, op.id, record, op.owner);
              await reference.put(op.kind, op.id, record, op.owner);
            } else {
              const found = await compressed.get(op.kind, op.id, a, op.owner);
              const expected = await reference.get(op.kind, op.id, b, op.owner);
              expect(found, `get ${String(op.kind)}/${String(op.owner)}/${String(op.id)}`).toBe(
                expected,
              );
              expect(a).toEqual(b);
              if (!expected) reach.reach("absent record");
            }
          }
          // Every key once more, after the last flushes and evictions.
          for (const op of ops) {
            expect(await compressed.get(op.kind, op.id, a, op.owner)).toBe(
              await reference.get(op.kind, op.id, b, op.owner),
            );
            expect(a).toEqual(b);
          }
        } finally {
          await compressed.close();
          await reference.close();
        }
        if (REGISTRY_DIRECTORY_WORK.splits > before.splits) reach.reach("split inside a skip");
        if (REGISTRY_DIRECTORY_WORK.pageReads > before.pageReads)
          reach.reach("evicted leaf read back");
        if (new Set(ops.map((op) => `${String(op.kind)}/${String(op.owner)}`)).size > 1)
          reach.reach("several namespaces");
        if (ops.some((op) => op.id >= 16 ** 6)) reach.reach("deep divergence");
      }),
      { seed, numRuns: 60 },
    );
    reach.assertReached();
  },
  120_000,
);
