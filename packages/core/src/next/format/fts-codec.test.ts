/** Independent wire fixtures and generated native values; no shipped postings representation. */
import { expect, it } from "vitest";
import fc from "fast-check";
import { createReach, FTS_CODEC_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ManifestCorruptionError } from "./bytes.js";
import { ParseBudgetError } from "./manifest-codec.js";
import { UnsupportedEncodingError } from "./encoding.js";
import type { BlobRef } from "../storage/extents.js";
import type { FtsRoot, FtsTermRef, FtsDocumentRef, FtsDeltaRef } from "./fts.js";
import {
  encodeFts,
  decodeFts,
  FTS_ENCODED_OVERHEAD,
  FTS_CODEC_SCRATCH_BYTES,
  type FtsFormatName,
  type FtsValues,
  type EncodedFts,
} from "./fts-codec.js";

function budget(cap = Infinity) {
  return {
    held: 0,
    peak: 0,
    limit: cap,
    allocate(n: number) {
      if (this.held + n > this.limit) return false;
      this.held += n;
      this.peak = Math.max(this.peak, this.held);
      return true;
    },
    free(n: number) {
      if (n > this.held) throw Error("FTS budget underflow");
      this.held -= n;
    },
  };
}
const ref = (slot: number): BlobRef => ({
  extent: { offset: slot * 4096, length: 4096 },
  offset: 0,
  length: 128,
  checksum: slot,
});
type Case = [FtsFormatName, FtsValues[FtsFormatName]];
function roundtrip([kind, value]: Case): void {
  const b = budget(),
    owned = encodeFts(kind, value, b);
  let decoded: ReturnType<typeof decodeFts> | undefined;
  try {
    expect(b.held).toBe(owned.bytes.length + FTS_ENCODED_OVERHEAD);
    decoded = decodeFts(kind, owned.bytes, b);
    expect(decoded.value).toEqual(value);
    expect(b.held).toBe(owned.charge + decoded.charge);
  } finally {
    if (decoded) {
      const charge = decoded.charge;
      decoded = undefined;
      b.free(charge);
    }
    owned.close();
    owned.close();
  }
  expect(decoded).toBeUndefined();
  expect(b.held).toBe(0);
}
const words = ["a", "β", "東京", "\ue000", "\ud801\udc28", "a".repeat(31) + "\ud801"];
function cases(
  ids: number[],
  frequencies: number[],
  word: string,
  state: "building" | "ready" | "invalid",
): Case[] {
  const n = ids.length,
    total = BigInt(frequencies.reduce((a, b) => a + b, 0));
  const doc: FtsDocumentRef = {
    ref: ref(1),
    first: ids[0] ?? 0,
    last: ids[n - 1] ?? 0,
    count: n,
    tokens: total,
  };
  const terms: FtsTermRef = {
    ref: ref(2),
    first: n ? word : "",
    last: n ? word : "",
    count: n ? 1n : 0n,
    tokens: total,
  };
  const root: FtsRoot = {
    kind: "column",
    ftsId: 1,
    tableId: 2,
    columnId: 3,
    schemaVersion: 1,
    generation: 2,
    mutationEpoch: 0,
    tokenizerVersion: 1,
    rendererVersion: 2,
    scorerVersion: 1,
    config: { version: 1 },
    state,
    owner: state === "building" ? "owner\ufeff" : null,
    leaseUntil: state === "building" ? 1000 : 0,
    baseLsn: 1,
    throughLsn: 2,
    nextRowId: (ids[n - 1] ?? 0) + 1,
    documentCount: n,
    tokenCount: total,
    documents: n ? doc : null,
    terms: n ? terms : null,
    deltas: null,
  };
  const input: Case[] = [
    ["ftsRoot", root],
    [
      "ftsRoot",
      {
        kind: "directory",
        tableId: 2,
        mutationEpoch: 0,
        depth: 0,
        count: 1,
        children: [{ ref: ref(3), first: 1, last: 1, count: 1 }],
      },
    ],
    ["ftsTerms", { kind: "leaf", entries: n ? [{ term: word, postings: doc }] : [] }],
    [
      "ftsPostings",
      { kind: "leaf", ids: Float64Array.from(ids), frequencies: Uint16Array.from(frequencies) },
    ],
    [
      "ftsDocuments",
      {
        kind: "leaf",
        ids: Float64Array.from(ids),
        lengths: Uint16Array.from(frequencies),
        renderable: new Uint8Array(n).fill(1),
      },
    ],
    [
      "ftsDelta",
      {
        kind: "leaf",
        ftsId: 1,
        generation: 2,
        mutationEpoch: 0,
        entries: n ? [{ lsn: 2, documents: doc, terms }] : [],
      },
    ],
  ];
  if (n) {
    const delta: FtsDeltaRef = {
      ref: ref(4),
      first: 2,
      last: 2,
      count: 1,
      firstDocument: doc.first,
      lastDocument: doc.last,
      documents: n,
      tokens: total,
    };
    input.push(
      ["ftsRoot", { ...root, baseLsn: 1, documents: null, terms: null, deltas: delta }],
      ["ftsTerms", { kind: "branch", depth: 1, count: 1n, tokens: total, children: [terms] }],
      ["ftsPostings", { kind: "branch", depth: 1, count: n, tokens: total, children: [doc] }],
      ["ftsDocuments", { kind: "branch", depth: 1, count: n, tokens: total, children: [doc] }],
      [
        "ftsDelta",
        {
          kind: "branch",
          ftsId: 1,
          generation: 2,
          mutationEpoch: 0,
          depth: 1,
          count: 1,
          documents: n,
          tokens: total,
          children: [delta],
        },
      ],
    );
  }
  return input;
}
const input = fc.record({
  base: fc.oneof(
    fc.integer({ min: 2, max: 1_000_000 }),
    fc.integer({ min: 0, max: 1024 }).map((n) => Number.MAX_SAFE_INTEGER - 2048 - n),
  ),
  offsets: fc.uniqueArray(fc.integer({ min: 0, max: 255 }), { maxLength: 12 }),
  frequency: fc.integer({ min: 1, max: 4096 }),
  word: fc.constantFrom(...words),
  state: fc.constantFrom("building" as const, "ready" as const, "invalid" as const),
});
for (const seed of seedsFor("next-fts-codec", [0xb819, 0xb81a]))
  it("roundtrips charged native FTS trees seed" + String(seed), () => {
    const reach = createReach("FTS codec", FTS_CODEC_REACH);
    fc.assert(
      fc.property(input, (value) => {
        reach.beginHistory();
        const ids = value.offsets.sort((a, b) => a - b).map((n) => value.base + n);
        for (const entry of cases(
          ids,
          ids.map(() => value.frequency),
          value.word,
          value.state,
        )) {
          roundtrip(entry);
          reach.reach(entry[0]);
          if ("kind" in entry[1]) {
            if (entry[1].kind === "branch") reach.reach("branch");
            if (entry[1].kind === "leaf") reach.reach("leaf");
          }
        }
        if (ids.some((id) => id > 2 ** 52)) reach.reach("wide IDs");
        reach.reach("raw UTF16");
        reach.reach("charged ownership");
        reach.reach("drained");
      }),
      { seed, numRuns: 120 },
    );
    reach.assertReached();
  });

it("pins independent little-endian lane fixtures and NULL versus empty text", () => {
  const posting = Uint8Array.from([
    0x4d, 0x46, 0x54, 0x50, 1, 0, 0, 0, 3, 1, 1, 0, 2, 3, 0, 4, 2, 0,
  ]);
  const documents = Uint8Array.from([
    0x4d, 0x46, 0x54, 0x44, 1, 0, 0, 0, 3, 1, 0, 0, 2, 1, 0, 4, 5, 0,
  ]);
  for (const [kind, bytes, expected] of [
    [
      "ftsPostings",
      posting,
      { kind: "leaf", ids: Float64Array.of(1, 3, 7), frequencies: Uint16Array.of(1, 3, 2) },
    ],
    [
      "ftsDocuments",
      documents,
      {
        kind: "leaf",
        ids: Float64Array.of(1, 3, 7),
        lengths: Uint16Array.of(0, 0, 2),
        renderable: Uint8Array.of(0, 1, 1),
      },
    ],
  ] as const) {
    const b = budget(),
      decoded = decodeFts(kind, bytes, b),
      encoded = encodeFts(kind, expected, b);
    try {
      expect(decoded.value).toEqual(expected);
      expect(encoded.bytes).toEqual(bytes);
    } finally {
      b.free(decoded.charge);
      encoded.close();
    }
    expect(b.held).toBe(0);
  }
});

it("keeps token totals beyond2^53 and UTF16 surrogate truncation exact", () => {
  const count = Number.MAX_SAFE_INTEGER - 1,
    tokens = BigInt(count) * 4096n - 1n;
  const child: FtsDocumentRef = { ref: ref(1), first: 1, last: count, count, tokens };
  roundtrip(["ftsDocuments", { kind: "branch", depth: 8, count, tokens, children: [child] }]);
  roundtrip(["ftsPostings", { kind: "branch", depth: 8, count, tokens, children: [child] }]);
  roundtrip([
    "ftsTerms",
    { kind: "leaf", entries: [{ term: "a".repeat(31) + "\ud801", postings: child }] },
  ]);
});

it("charges before constructors and drops the encoded owner before freeing it", () => {
  const source = { kind: "leaf" as const, ids: Float64Array.of(1), frequencies: Uint16Array.of(1) };
  const b = budget(),
    trace: Array<{ kind: string; bytes: number; held: number }> = [];
  const original = globalThis.Uint8Array;
  let encoded: EncodedFts;
  // Entire observer interval is synchronous and bounded; restore before any assertion/await.
  try {
    globalThis.Uint8Array = new Proxy(original, {
      construct(target, args, newTarget) {
        if (typeof args[0] === "number")
          trace.push({ kind: "bytes", bytes: args[0], held: b.held });
        return Reflect.construct(target, args, newTarget) as Uint8Array;
      },
    });
    encoded = encodeFts("ftsPostings", source, b);
  } finally {
    globalThis.Uint8Array = original;
  }
  expect(trace).toHaveLength(1);
  for (const allocation of trace)
    expect(allocation.held).toBeGreaterThanOrEqual(
      allocation.bytes + FTS_ENCODED_OVERHEAD + FTS_CODEC_SCRATCH_BYTES,
    );
  let wasReleased = false;
  const free = b.free.bind(b);
  b.free = (n) => {
    try {
      void encoded.bytes;
    } catch {
      wasReleased = true;
    }
    free(n);
  };
  encoded.close();
  expect(wasReleased).toBe(true);
  expect(b.held).toBe(0);
  const denied = budget(0);
  expect(() => encodeFts("ftsPostings", source, denied)).toThrow(ParseBudgetError);
  expect(denied.held).toBe(0);
});

it("refuses corrupt/future pages and releases every parse/encode charge", () => {
  const value = {
    kind: "leaf" as const,
    ids: Float64Array.of(1, 3),
    frequencies: Uint16Array.of(1, 2),
  };
  const b = budget(),
    encoded = encodeFts("ftsPostings", value, b);
  try {
    for (const at of [4, 5, 6, 7]) {
      const changed = encoded.bytes.slice();
      changed[at] = at === 4 ? 2 : 255;
      const before = b.held;
      expect(() => decodeFts("ftsPostings", changed, b)).toThrow(
        // format/encoding.ts owns version/tag refusals; damaged reserved bits are corruption.
        at === 4 || at === 6 ? UnsupportedEncodingError : ManifestCorruptionError,
      );
      expect(b.held).toBe(before);
    }
    for (let end = 0; end < encoded.bytes.length; end++) {
      const before = b.held;
      expect(() => decodeFts("ftsPostings", encoded.bytes.subarray(0, end), b)).toThrow();
      expect(b.held).toBe(before);
    }
    const duplicate = encoded.bytes.slice();
    duplicate[12] = 0; // second document delta after first id/u16frequency
    expect(() => decodeFts("ftsPostings", duplicate, b)).toThrow(ManifestCorruptionError);
    const before = b.held;
    expect(() => decodeFts("ftsPostings", encoded.bytes, budget(0))).toThrow(ParseBudgetError);
    expect(b.held).toBe(before);
  } finally {
    encoded.close();
  }
  expect(b.held).toBe(0);
  expect(() =>
    encodeFts("ftsPostings", { ...value, frequencies: Uint16Array.of(0, 1) }, b),
  ).toThrow(ManifestCorruptionError);
  expect(b.held).toBe(0);
});

it("a refusing parse grant sees no typed buffer or view construction", () => {
  const bytes = Uint8Array.from([0x4d, 0x46, 0x54, 0x50, 1, 0, 0, 0, 0]);
  const names = ["Uint8Array", "Uint16Array", "Float64Array", "DataView"] as const;
  const original = names.map((name) => [name, globalThis[name]] as const);
  let allocations = 0,
    failure: unknown;
  // No await or unbounded work under global observers; every restoration is in finally.
  try {
    for (const [name, ctor] of original)
      Reflect.set(
        globalThis,
        name,
        new Proxy(ctor, {
          construct(target, args, newTarget) {
            allocations++;
            return Reflect.construct(target, args, newTarget) as object;
          },
        }),
      );
    try {
      decodeFts("ftsPostings", bytes, budget(0));
    } catch (error) {
      failure = error;
    }
  } finally {
    for (const [name, ctor] of original) Reflect.set(globalThis, name, ctor);
  }
  expect(failure).toBeInstanceOf(ParseBudgetError);
  expect(allocations).toBe(0);
});
