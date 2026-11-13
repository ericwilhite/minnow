/** Summary prefixes are conservative; Bloom membership always hashes complete canonical keys. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, OVERFLOW_SUMMARY_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { inKeyRange, type KeyRange } from "../access/key-index.js";
import type { RunRef } from "../format/manifest.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { ByteReader, need, UNCHARGED } from "./bytes.js";
import { RunBuilder, RunReader, runRef } from "./runs.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { hashKey } from "./key-hashes.js";
import { Harness } from "./test-support.js";
import { runOutsideKeys, runOutsideRange } from "./run-bounds.js";
import { StreamingRangeCursor } from "./range-cursor.js";
import type { ViewIndex } from "./view.js";
const reach = createReach("bounded overflow summaries and Bloom", OVERFLOW_SUMMARY_STATES);
afterAll(() => reach.assertReached());
function key(width: number, ordinal: number, shared = false): Uint8Array {
  const out = new Uint8Array(width).fill(65);
  new DataView(out.buffer).setUint32(shared ? width - 4 : 0, ordinal, false);
  return out;
}
function bloomHas(bytes: Uint8Array, key: Uint8Array): boolean {
  const bits = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
  expect(bytes[4]).toBe(7);
  const first = xxHash32(key),
    second = (xxHash32(key, 0x9747b28c) | 1) >>> 0;
  for (let i = 0; i < 7; i++) {
    const bit = (first + i * second) % bits;
    if (((bytes[5 + Math.floor(bit / 8)] ?? 0) & (1 << (bit % 8))) === 0) return false;
  }
  return true;
}
function bloomParts(summary: NonNullable<ReturnType<RunBuilder["finish"]>>): Uint8Array[] {
  const parts = new Map<number, Uint8Array>();
  let at = 0;
  for (const p of summary.parts) {
    parts.set(at, p);
    at += p.length;
  }
  const r = new ByteReader(need(summary.parts.at(-1))),
    span = r.varuint(),
    count = r.varuint();
  expect(span).toBe(256);
  const out: Uint8Array[] = [];
  for (let i = 0; i < count; i++) {
    const offset = r.varuint(),
      length = r.varuint();
    r.u32();
    const data = need(parts.get(offset));
    expect(data.length).toBe(length);
    out.push(data);
  }
  r.end();
  return out;
}
for (const seed of seedsFor("next-overflow-summary-bloom", [0xb50a]))
  it(`probes complete keys across Bloom partitions (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2049, 49155, 65537),
        fc.integer({ min: 257, max: 273 }),
        fc.boolean(),
        async (width, count, supplied) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory");
          await h.open();
          const writer = h.space.beginWrite("overflow Bloom partitions"),
            builder = new RunBuilder(false, false, UNCHARGED, 2);
          let summary: ReturnType<RunBuilder["finish"]> | undefined;
          let grant: Awaited<ReturnType<Harness["governor"]["admit"]>> | undefined;
          let reader: RunReader | undefined;
          try {
            const keys = Array.from({ length: count }, (_, i) => key(i % 2 === 0 ? 12 : width, i));
            const hashes = { first: 0, second: 0 };
            for (let i = 0; i < keys.length; i++) {
              const bytes = need(keys[i]);
              if (supplied) {
                hashKey(bytes, 0, bytes.length, hashes);
                builder.addRange(bytes, 0, bytes.length, i, false, hashes.first, hashes.second);
              } else builder.add(bytes, i, false);
            }
            summary = need(builder.finish());
            const ref = runRef(summary, (await writer.write(summary.parts)).blobs);
            expect(
              summary.minKey.buffer.byteLength +
                summary.maxKey.buffer.byteLength +
                need(summary.keyStorage).minimum.buffer.byteLength +
                need(summary.keyStorage).maximum.buffer.byteLength,
            ).toBeLessThanOrEqual(2 * (2048 + 2112));
            reach.reach("bounded native summaries");
            grant = await h.governor.admit({
              label: "overflow Bloom reader",
              minimumBytes: 3 * MAX_BLOB_BYTES + RunReader.residentBound(ref),
              spillable: false,
            });
            reader = await RunReader.open(h.space, ref, grant, grant);
            expect(reader.blockCount).toBeGreaterThan(256);
            reach.reach("crossed partition boundary");
            const partitions = bloomParts(summary);
            expect(reader.blockCount).toBe(count); // alternating widths prove one block/entry
            for (let i = 0; i < keys.length; i++) {
              const bytes = need(keys[i]);
              expect(
                bloomHas(need(partitions[Math.floor(i / 256)]), bytes),
                "canonical partition contains complete-key probes",
              ).toBe(true);
              expect(reader.mayContain(bytes), "full-key Bloom has no false negative").toBe(true);
              const found: Array<{ rowId: number; tombstone: boolean }> = [];
              await reader.lookup(h.space, bytes, found, grant);
              expect(found).toEqual([{ rowId: i, tombstone: false }]);
            }
            reach.reach("full-key Bloom");
            reach.reach(supplied ? "supplied hashes" : "computed hashes");
            reach.reach(width > 65536 ? "multiple chunks" : "single chunk");
          } finally {
            if (reader && grant) grant.free(reader.bytes);
            summary?.release();
            builder.close();
            expect(grant?.allocated ?? 0).toBe(0);
            grant?.close();
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 120_000);

for (const seed of seedsFor("next-overflow-summary-ranges", [0xb50b]))
  it(`retains prefix ties and resolves pending heads in either direction (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2, 3, 8),
        fc.constantFrom(2049, 49155, 65537),
        fc.integer({ min: 9, max: 17 }),
        async (fan, width, perRun) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory");
          await h.open();
          const writer = h.space.beginWrite("overlapping summary prefixes");
          const summaries: Array<NonNullable<ReturnType<RunBuilder["finish"]>>> = [],
            refs: RunRef[] = [];
          const all: Array<{ key: Uint8Array; rowId: number }> = [];
          try {
            for (let run = 0; run < fan; run++) {
              const builder = new RunBuilder(false, false, UNCHARGED, 2);
              for (let i = 0; i < perRun; i++) {
                const rowId = i * fan + run,
                  bytes = key(width, rowId, true);
                builder.add(bytes, rowId, false);
                all.push({ key: bytes, rowId });
              }
              const summary = need(builder.finish());
              summaries.push(summary);
              const ref = runRef(summary, (await writer.write(summary.parts)).blobs);
              refs.push(ref);
              expect(ref.minKey.length).toBe(32);
              expect(ref.maxKey.length).toBe(32);
            }
            all.sort((a, b) => Buffer.compare(a.key, b.key) || a.rowId - b.rowId);
            const ranges: KeyRange[] = [
              { lower: null, upper: null },
              {
                lower: { kind: "atLeast", key: need(all[1]).key },
                upper: { kind: "through", key: need(all.at(-2)).key },
              },
              {
                lower: { kind: "after", key: need(all[1]).key },
                upper: { kind: "before", key: need(all.at(-2)).key },
              },
              {
                lower: { kind: "atLeast", key: need(all[0]).key.subarray(0, 32) },
                upper: { kind: "through", key: need(all[0]).key.subarray(0, 32) },
              },
              { lower: { kind: "after", key: need(all[0]).key.subarray(0, 32) }, upper: null },
            ];
            const fake = {
              def: { id: 1, role: "secondary", unique: false, kind: "ordered" },
              table: {
                tableView: {
                  runs: new Map([[1, refs]]),
                  memtables: [],
                  groups: { liveRowCount: all.length },
                },
                snapshot: {
                  bound: Infinity,
                  shared: { governor: h.governor, space: h.space },
                  check: () => undefined,
                  watchCursor() {
                    return () => undefined;
                  },
                },
                locator: (row: number) => row,
              },
            } as unknown as ViewIndex;
            for (const range of ranges) {
              for (let r = 0; r < refs.length; r++) {
                const actual = all.filter((e) => e.rowId % fan === r);
                if (actual.some((e) => inKeyRange(range, e.key)))
                  expect(runOutsideRange(need(refs[r]), range)).toBe(false);
                for (const e of actual) expect(runOutsideKeys(need(refs[r]), e.key)).toBe(false);
              }
              const expected = all.filter((e) => inKeyRange(range, e.key));
              for (const direction of ["asc", "desc"] as const) {
                const wanted = (direction === "asc" ? expected : [...expected].reverse()).map(
                  (e) => e.rowId,
                );
                for (const skipping of [false, true]) {
                  const cursor = new StreamingRangeCursor(fake, range, direction),
                    out = new Float64Array(7);
                  try {
                    const start = skipping ? Math.floor(wanted.length / 2) : 0;
                    if (skipping) expect(await cursor.skip(start)).toBe(start);
                    const actual: number[] = [];
                    for (;;) {
                      const n = await cursor.next(out);
                      if (n === 0) break;
                      for (let i = 0; i < n; i++) actual.push(need(out[i]));
                    }
                    expect(actual, "pending prefix heads preserve full-key order").toEqual(
                      wanted.slice(start),
                    );
                  } finally {
                    cursor.close();
                  }
                }
                reach.reach(direction === "asc" ? "ascending" : "descending");
              }
            }
            reach.reach("prefix ties retained");
            reach.reach("pending heads");
            reach.reach("range bounds and skip");
            reach.reach(fan === 8 ? "fan in eight" : "other fan in");
          } finally {
            for (const s of summaries) s.release();
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 12 },
    );
  }, 120_000);

for (const seed of seedsFor("next-overflow-hot-partitions", [0xb50f]))
  it(`retains full-key probes when a hot key crosses partitions (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(2049, 4096), fc.boolean(), async (width, supplied) => {
        reach.beginHistory();
        const keyBytes = key(width, 17, true),
          builder = new RunBuilder(false, true, UNCHARGED, 2);
        const hashes = { first: 0, second: 0 };
        hashKey(keyBytes, 0, keyBytes.length, hashes);
        let summary: ReturnType<RunBuilder["finish"]> | undefined;
        try {
          for (let row = 0; row < 24000; row++)
            builder.addRange(
              keyBytes,
              0,
              keyBytes.length,
              row,
              false,
              supplied ? hashes.first : undefined,
              supplied ? hashes.second : undefined,
            );
          summary = need(builder.finish());
          const parts = bloomParts(summary);
          expect(parts.length).toBeGreaterThan(1);
          for (const p of parts)
            expect(bloomHas(p, keyBytes), "hot full-key probes survive partition rollover").toBe(
              true,
            );
          reach.reach("hot overflow partitions");
        } finally {
          summary?.release();
          builder.close();
        }
      }),
      { seed, numRuns: 6 },
    );
  }, 120_000);
