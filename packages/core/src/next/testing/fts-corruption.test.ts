/** Bounded FTS references must refuse before IO; unique UTF16 keys constrain their fences. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  FTS_REFERENCE_GUARD_REACH,
  FTS_TERM_FENCE_REACH,
} from "../../testing/reach.js";
import { FTS_NATIVE_GRAMMAR } from "../durable-format.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import type { BlobRef, ExtentReader } from "../storage/extents.js";
import { ManifestCorruptionError } from "../format/bytes.js";
import { readFtsNode, walkFtsTree } from "../format/fts-storage.js";
import { decodeFts, encodeFts, type FtsFormatName } from "../format/fts-codec.js";

function budget() {
  return {
    held: 0,
    peak: 0,
    allocate(bytes: number) {
      this.held += bytes;
      this.peak = Math.max(this.peak, this.held);
      return true;
    },
    free(bytes: number) {
      this.held -= bytes;
      expect(this.held).toBeGreaterThanOrEqual(0);
    },
  };
}
const kinds: readonly FtsFormatName[] = [
  "ftsRoot",
  "ftsTerms",
  "ftsPostings",
  "ftsDocuments",
  "ftsDelta",
];
for (const seed of seedsFor("next-fts-reference-guard", [0xb821, 0xb822]))
  it(`refuses invalid FTS references before IO seed ${String(seed)}`, async () => {
    const reach = createReach("FTS reference guard", FTS_REFERENCE_GUARD_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 1024 }), async (extra) => {
        reach.beginHistory();
        const space = new MemoryExtentSpace(),
          writer = space.beginWrite("corrupt FTS root");
        try {
          // Real, checksummed oversized bytes: a late decoder refusal still allocates this read.
          const written = await writer.write([
            new Uint8Array(FTS_NATIVE_GRAMMAR.pageBytes + extra),
          ]);
          const original = written.blobs[0];
          if (!original) throw new Error("fixture has no root");
          const invalid: Array<[(typeof FTS_REFERENCE_GUARD_REACH)[number], BlobRef]> = [
            ["checksummed oversized root", original],
            [
              "beyond generic read cap",
              { ...original, length: 2 ** 24, extent: { offset: 0, length: 2 ** 24 } },
            ],
            ["outside extent", { ...original, offset: original.extent.length, length: 1 }],
            ["zero length", { ...original, length: 0 }],
            ["negative offset", { ...original, offset: -extra }],
            ["fractional length", { ...original, length: 1.5 }],
            [
              "unsafe extent end",
              { ...original, extent: { offset: Number.MAX_SAFE_INTEGER, length: extra } },
            ],
            ["invalid checksum", { ...original, checksum: 2 ** 32 }],
            ["nonfinite", { ...original, length: Infinity }],
          ];
          for (const [label, ref] of invalid) {
            const account = budget();
            let reads = 0;
            const reader: ExtentReader = {
              peek: () => null,
              read(refs, options) {
                reads++;
                return space.read(refs, options);
              },
            };
            for (const kind of kinds)
              await expect(readFtsNode(kind, ref, account, reader)).rejects.toMatchObject({
                code: "MINNOW_MANIFEST_CORRUPT",
              });
            await expect(async () => {
              for await (const event of walkFtsTree(ref, account, reader)) void event;
            }).rejects.toBeInstanceOf(ManifestCorruptionError);
            expect(reads).toBe(0);
            expect(account.peak).toBe(0);
            expect(account.held).toBe(0);
            expect(space.leased).toBe(0);
            reach.reach(label);
          }
          const entered = new Error("valid reference reached IO");
          for (const length of [
            1,
            FTS_NATIVE_GRAMMAR.pageBytes - 1,
            FTS_NATIVE_GRAMMAR.pageBytes,
          ]) {
            let reads = 0;
            const account = budget(),
              reader: ExtentReader = {
                peek: () => null,
                read() {
                  reads++;
                  return Promise.reject(entered);
                },
              };
            const ref = { ...original, length };
            await expect(readFtsNode("ftsRoot", ref, account, reader)).rejects.toBe(entered);
            expect(reads).toBe(1);
            expect(account.held).toBe(0);
          }
          reach.reach("legal boundary reaches IO");
          reach.reach("all five grammars");
          reach.reach("initial walker root");
          reach.reach("no allocation or IO");
        } finally {
          await writer.abort();
        }
      }),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });

// Independent v1 wire builder. No production encoder can sanitize these malformed descriptors.
function uint(value: number | bigint): number[] {
  let n = BigInt(value);
  const bytes: number[] = [];
  while (n >= 128n) {
    bytes.push(Number(n & 127n) | 128);
    n >>= 7n;
  }
  bytes.push(Number(n));
  return bytes;
}
function text(value: string): number[] {
  const bytes = uint(value.length);
  for (let i = 0; i < value.length; i++)
    bytes.push(value.charCodeAt(i) >>> 8, value.charCodeAt(i) & 255);
  return bytes;
}
const wireRef = [...uint(4096), ...uint(4096), 0, ...uint(128), 1, 0, 0, 0];
function fixture(
  kind: "ftsTerms" | "ftsRoot" | "ftsDelta",
  first: string,
  last: string,
  count: bigint,
) {
  const tokens = count || 1n;
  const termRef = [...wireRef, ...text(first), ...text(last), ...uint(count), ...uint(tokens)];
  const docRef = [...wireRef, 1, 1, 1, ...uint(tokens)];
  const magic = kind === "ftsTerms" ? 0x54 : kind === "ftsRoot" ? 0x52 : 0x4c;
  const header = [0x4d, 0x46, 0x54, magic, 1, 0, kind === "ftsTerms" ? 1 : 0, 0];
  return Uint8Array.from([
    ...header,
    ...(kind === "ftsTerms"
      ? [1, ...uint(count), ...uint(tokens), 1, ...termRef]
      : kind === "ftsDelta"
        ? [1, 1, 0, 1, 2, ...docRef, 1, ...termRef]
        : // table/epoch,state/semantics,id/column/schema/generation,owner/lease,coverage/corpus.
          [
            1,
            0,
            1,
            1,
            2,
            1,
            1,
            1,
            1,
            1,
            1,
            0,
            0,
            1,
            1,
            2,
            1,
            ...uint(tokens),
            1,
            ...termRef,
            1,
            ...docRef,
            0,
          ]),
  ]);
}
const contexts = ["ftsTerms", "ftsRoot", "ftsDelta"] as const;
for (const seed of seedsFor("next-fts-term-fences", [0xb823, 0xb824]))
  it(`checks unique term fence cardinality locally seed ${String(seed)}`, () => {
    const reach = createReach("FTS term fences", FTS_TERM_FENCE_REACH);
    fc.assert(
      fc.property(
        fc.constantFrom("a", "β", "東", "\ud801", "\udfff", "\u0000"),
        fc.integer({ min: 1, max: 31 }),
        fc.integer({ min: 1, max: 12 }),
        (unit, length, extra) => {
          reach.beginHistory();
          const prefix = unit.repeat(length),
            full = unit.repeat(31);
          const invalid: Array<[(typeof FTS_TERM_FENCE_REACH)[number], string, string, bigint]> = [
            ["equal fences multiple terms", prefix, prefix, BigInt(1 + extra)],
            ["singleton distinct fences", prefix, prefix + "\0", 1n],
            ["reversed fences", prefix + "\0", prefix, 2n],
            ["adjacent prefix capacity", prefix, prefix + "\0", BigInt(2 + extra)],
            ["maximum length interval capacity", full + "a", full + "c", BigInt(3 + extra)],
          ];
          for (const [label, first, last, count] of invalid) {
            for (const kind of contexts) {
              const account = budget();
              expect(() => decodeFts(kind, fixture(kind, first, last, count), account)).toThrow(
                ManifestCorruptionError,
              );
              expect(account.held).toBe(0);
              reach.reach(kind);
            }
            reach.reach(label);
          }
          // Valid boundary intervals must survive, including NUL and truncated surrogate keys.
          for (const [first, last, count] of [
            [prefix, prefix, 1n],
            [prefix, prefix + "\0", 2n],
            [full + "a", full + "c", 3n],
          ] as const)
            for (const kind of contexts) {
              const account = budget(),
                bytes = fixture(kind, first, last, count);
              const decoded = decodeFts(kind, bytes, account);
              try {
                const encoded = encodeFts(kind, decoded.value, account);
                try {
                  expect(encoded.bytes).toEqual(bytes);
                } finally {
                  encoded.close();
                }
              } finally {
                account.free(decoded.charge);
              }
              expect(account.held).toBe(0);
            }
          reach.reach("exact boundary accepted");
          if (unit.charCodeAt(0) >= 0xd800 && unit.charCodeAt(0) <= 0xdfff)
            reach.reach("raw surrogate");
          if (unit === "\0") reach.reach("NUL code unit");
          reach.reach("no child read needed");
          reach.reach("drained");
        },
      ),
      { seed, numRuns: 120 },
    );
    reach.assertReached();
  });
