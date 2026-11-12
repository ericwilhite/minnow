/** Exact canonical-key order, independent of short prefixes and lease lifetimes. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { createReach, OVERFLOW_READER_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { inKeyRange, type KeyRange } from "../access/key-index.js";
import type { ByteBudget, SpaceReader } from "../access/space.js";
import type { BlobRef, RunRef } from "../format/manifest.js";
import { ByteReader } from "../format/bytes.js";
import { ManifestCorruptionError } from "../format/bytes.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import { UnsupportedEncodingError } from "../format/encoding.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { Governor } from "../memory/governor.js";
import { need, UNCHARGED } from "./bytes.js";
import { RunBuilder, RunReader, runRef } from "./runs.js";
import { RunRangeInput, rangeRunBytes } from "./range-input.js";
import { OverflowKeys, overflowKeyBytes, pointerFrom } from "./overflow-keys.js";
import { Harness } from "./test-support.js";
import { mergeRuns, mergeWorkingBytes, DEFAULT_MERGE_POLICY } from "./merge.js";

const reach = createReach("overflow readers", OVERFLOW_READER_STATES);
afterAll(() => reach.assertReached());
const compare = (a: Uint8Array, b: Uint8Array) => Buffer.compare(a, b);
interface Entry {
  key: Uint8Array;
  rowId: number;
  tombstone: boolean;
}
class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  constructor(readonly cap = 8 * 1024 * 1024) {}
  allocate(bytes: number): boolean {
    if (this.used + bytes > this.cap) return false;
    this.used += bytes;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(bytes: number): void {
    this.used -= bytes;
    expect(this.used).toBeGreaterThanOrEqual(0);
  }
}
function fixture(entries: Entry[], hot: boolean) {
  const builder = new RunBuilder(false, false, UNCHARGED, 2);
  for (const e of entries) builder.add(e.key, e.rowId, e.tombstone);
  const run = need(builder.finish());
  let at = 0;
  const blobs = run.parts.map((part) => {
    const b = {
      extent: { offset: 0, length: 0 },
      offset: at,
      length: part.length,
      checksum: xxHash32(part),
    };
    at += part.length;
    return b;
  });
  const extent = { offset: 0, length: at };
  const refs = blobs.map((b) => ({ ...b, extent }));
  const ref = runRef(run, refs);
  const parts = new Map(refs.map((r, i) => [r.offset, need(run.parts[i])]));
  let leases = 0;
  let failAt = -1,
    reads = 0;
  let corrupt: ((blob: BlobRef, bytes: Uint8Array) => Uint8Array) | undefined;
  const source = (b: BlobRef) => {
    const bytes = need(parts.get(b.offset));
    expect(bytes.length).toBe(b.length);
    expect(xxHash32(bytes)).toBe(b.checksum);
    return corrupt?.(b, bytes) ?? bytes;
  };
  const space = {
    peek: (b: BlobRef) => (hot ? source(b) : null),
    async read(refs: readonly BlobRef[], options: { budget: ByteBudget; signal?: AbortSignal }) {
      expect(leases, "no input lease crosses another reader await").toBe(0);
      options.signal?.throwIfAborted();
      if (reads++ === failAt) throw new Error("injected read fault");
      const n = refs.reduce((n, b) => n + b.length, 0);
      if (!options.budget.allocate(n)) throw new Error("reader allowance exhausted");
      let bytes: Uint8Array[];
      try {
        bytes = refs.map((b) => source(b).slice());
      } catch (error) {
        options.budget.free(n);
        throw error;
      }
      leases++;
      await Promise.resolve();
      let released = false;
      return {
        bytes,
        release() {
          if (released) return;
          released = true;
          leases--;
          for (const b of bytes) b.fill(0xcc);
          options.budget.free(n);
        },
      };
    },
  } as unknown as SpaceReader;
  return {
    space,
    ref,
    parts,
    run,
    get reads() {
      return reads;
    },
    get leases() {
      return leases;
    },
    fail(n: number) {
      reads = 0;
      failAt = n;
    },
    corrupt(f: typeof corrupt) {
      corrupt = f;
    },
  };
}
function key(width: number, ordinal: number): Uint8Array {
  const b = new Uint8Array(width).fill(0x31);
  if (width >= 4) new DataView(b.buffer).setUint32(width - 4, ordinal, false);
  return b;
}
function expectEntry(
  bytes: Uint8Array,
  actual: { rowId: number; tombstone: boolean },
  expected: Entry,
) {
  expect(compare(bytes, expected.key)).toBe(0);
  expect(actual.rowId).toBe(expected.rowId);
  expect(actual.tombstone).toBe(expected.tombstone);
}
for (const seed of seedsFor("next-overflow-readers", [0xb503]))
  it(`point/range/cursor match complete bytes with poisoned leases (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2049, 4096, 49155, 65537),
        fc.boolean(),
        fc.integer({ min: 9, max: 180 }),
        async (width, hot, count) => {
          reach.beginHistory();
          const entries = Array.from({ length: count }, (_, i) => ({
            key: key(i % 5 === 0 ? 12 : width, Math.floor(i / 2)),
            rowId: 2 ** 40 + i,
            tombstone: i % 7 === 0,
          }))
            .concat([{ key: key(width, 0).slice(0, 32), rowId: 2 ** 41, tombstone: false }])
            .sort((a, b) => compare(a.key, b.key) || a.rowId - b.rowId);
          const f = fixture(entries, hot),
            b = new Budget();
          let reader: RunReader | undefined;
          try {
            const before = f.reads;
            await expect(
              RunReader.open(f.space, f.ref, b, { allocate: () => false, free: () => undefined }),
            ).rejects.toBeInstanceOf(ParseBudgetError);
            expect(f.reads, "metadata admission touches no I/O").toBe(before);
            reach.reach("metadata precharge");
            reader = await RunReader.open(f.space, f.ref, b, b);
            expect(reader.bytes).toBeLessThanOrEqual(RunReader.residentBound(f.ref));
            for (const e of entries) {
              const out: Array<{ rowId: number; tombstone: boolean }> = [];
              await reader.lookup(f.space, e.key, out, b);
              expect(out).toEqual(
                entries
                  .filter((x) => compare(x.key, e.key) === 0)
                  .map(({ rowId, tombstone }) => ({ rowId, tombstone })),
              );
            }
            reach.reach("point reads");
            const missing: Array<{ rowId: number; tombstone: boolean }> = [];
            await reader.lookup(f.space, key(width, count + 100), missing, b);
            expect(missing).toEqual([]);
            reach.reach("missing shared prefix");
            let n = 0;
            await reader.cursor(
              f.space,
              b,
              (bytes, rowId, tombstone) => {
                expectEntry(bytes, { rowId, tombstone }, need(entries[n++]));
                return false;
              },
              null,
              () => Promise.resolve(),
            );
            expect(n).toBe(entries.length);
            reach.reach("cursor drain");
            const ranges: KeyRange[] = [
              { lower: null, upper: null },
              {
                lower: { kind: "after", key: need(entries[2]).key },
                upper: { kind: "before", key: need(entries[count - 2]).key },
              },
              {
                lower: { kind: "atLeast", key: key(width, 0).slice(0, 32) },
                upper: { kind: "through", key: key(width, 0).slice(0, 32) },
              },
            ];
            for (const range of ranges)
              for (const direction of [1, -1]) {
                const bytes = rangeRunBytes(f.ref, direction);
                expect(b.allocate(bytes)).toBe(true);
                const Native = Uint8Array;
                let nativeBytes = 0;
                let input: RunRangeInput;
                try {
                  globalThis.Uint8Array = new Proxy(Native, {
                    construct(target, args) {
                      const size: unknown = args[0];
                      if (typeof size === "number") {
                        nativeBytes += size;
                        expect(
                          nativeBytes,
                          "workspace precharges native blocks and heads",
                        ).toBeLessThanOrEqual(bytes);
                      }
                      return Reflect.construct(target, args) as Uint8Array;
                    },
                  });
                  input = new RunRangeInput(
                    f.space,
                    b,
                    f.ref,
                    range,
                    direction,
                    Infinity,
                    new AbortController().signal,
                  );
                } finally {
                  globalThis.Uint8Array = Native;
                }
                reach.reach("native precharge");
                try {
                  await input.open();
                  input.pending = false;
                  const wanted = entries.filter((e) => inKeyRange(range, e.key));
                  if (direction < 0) wanted.reverse();
                  let n = 0;
                  while (await input.advance()) expectEntry(input.key(), input, need(wanted[n++]));
                  expect(n).toBe(wanted.length);
                  reach.reach(direction > 0 ? "ascending" : "descending");
                } finally {
                  input.close();
                  b.free(bytes);
                }
              }
            if (width === 49155) reach.reach("shipped key maximum");
            reach.reach("exact inline prefix");
            reach.reach("prefix seek");
            reach.reach("mixed inline blocks");
            reach.reach(hot ? "cached reads" : "cold reads");
            reach.reach(width > 65536 ? "multi chunk" : "single chunk");
            reach.reach("equal prefixes full comparison");
            reach.reach("wide row ids");
          } finally {
            if (reader) b.free(reader.bytes);
            f.run.release();
          }
          expect(f.leases).toBe(0);
          expect(b.used).toBe(0);
        },
      ),
      { seed, numRuns: 30 },
    );
  }, 120_000);

for (const seed of seedsFor("next-overflow-reader-faults", [0xb504]))
  it(`release errors, cancellation and structural corruption (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2049, 49155, 65537),
        fc.integer({ min: 0, max: 8 }),
        async (width, fault) => {
          reach.beginHistory();
          const entries = [0, 1, 2].map((i) => ({
            key: key(width, i),
            rowId: i + 1,
            tombstone: false,
          }));
          const f = fixture(entries, false),
            b = new Budget();
          let reader: RunReader | undefined;
          try {
            reader = await RunReader.open(f.space, f.ref, b, b);
            f.fail(fault);
            try {
              await reader.cursor(f.space, b, () => false);
            } catch (error) {
              expect(String(error)).toContain("injected read fault");
              reach.reach("read failure");
            }
            expect(f.leases).toBe(0);
            expect(b.used).toBe(reader.bytes);
            f.fail(-1);
            await expect(
              RunReader.open(f.space, { ...f.ref, entryCount: 0 }, b, b),
            ).rejects.toMatchObject({ code: "MINNOW_STORE_CORRUPT" });
            expect(b.used).toBe(reader.bytes);
            reach.reach("invalid entry count");
            let n = 0;
            await reader.cursor(f.space, b, () => {
              n++;
              return false;
            });
            expect(n).toBe(3);
            reach.reach("retry");
            const controller = new AbortController();
            const bytes = overflowKeyBytes(f.ref);
            expect(b.allocate(bytes)).toBe(true);
            try {
              const resolver = new OverflowKeys(f.ref, f.space, b, controller.signal);
              const ptr = pointerFrom(need(f.ref.keyStorage).maximum, f.ref.fences.offset);
              if (ptr.kind !== "overflow") throw new Error("missing long pointer");
              const raw = need(f.parts.get(ptr.directory.offset));
              for (const mode of ["magic", "version", "prefix"] as const) {
                f.corrupt((blob, original) => {
                  if (blob.offset !== ptr.directory.offset) return original;
                  const changed = raw.slice();
                  if (mode === "magic") changed[0] = (changed[0] ?? 0) ^ 1;
                  else if (mode === "version") changed[4] = 255;
                  else {
                    const r = new ByteReader(changed);
                    r.u32();
                    r.u8();
                    r.u8();
                    r.uint();
                    r.bytes();
                    // First prefix byte lies 32 bytes before the end of that blob value.
                    changed[r.at - 32] = (changed[r.at - 32] ?? 0) ^ 1;
                  }
                  return changed;
                });
                await expect(resolver.resolve(ptr)).rejects.toBeInstanceOf(
                  mode === "version" ? UnsupportedEncodingError : ManifestCorruptionError,
                );
                reach.reach(
                  mode === "version" ? "unknown directory version" : "corrupt descriptor",
                );
                expect(f.leases).toBe(0);
              }
              f.corrupt(undefined);
              const tooSmall = { ...ptr, length: need(f.ref.keyStorage).maxKeyBytes + 1 };
              await expect(resolver.resolve(tooSmall)).rejects.toBeInstanceOf(
                ManifestCorruptionError,
              );
              reach.reach("capacity refusal");
              controller.abort(new Error("reader cancelled"));
              await expect(resolver.resolve(ptr)).rejects.toThrow("reader cancelled");
              reach.reach("cancellation");
            } finally {
              b.free(bytes);
            }
          } finally {
            if (reader) b.free(reader.bytes);
            f.run.release();
          }
          expect(f.leases).toBe(0);
          expect(b.used).toBe(0);
        },
      ),
      { seed, numRuns: 18 },
    );
  }, 120_000);

for (const seed of seedsFor("next-overflow-reader-merge", [0xb505]))
  it(`mixed merges rebuild owned overflow references and split (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(1, 2, 8, 32),
        fc.constantFrom(2049, 49155, 65537),
        fc.boolean(),
        async (fan, width, bottom) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory");
          await h.open();
          const writer = h.space.beginWrite("overflow reader merge inputs");
          const readers: RunReader[] = [],
            refs: RunRef[] = [],
            summaries: Array<{ release(): void }> = [];
          const expected: Entry[] = [];
          let output: Awaited<ReturnType<typeof mergeRuns>> | undefined;
          let grant: Awaited<ReturnType<Governor["admit"]>> | undefined;
          const resultWriter = h.space.beginWrite("overflow merge output");
          try {
            for (let input = 0; input < fan; input++) {
              const builder = new RunBuilder(false, false, UNCHARGED, input % 2 === 0 ? 2 : 1);
              for (let i = 0; i < 7; i++) {
                const k = key(input % 2 === 0 ? width : 8, i),
                  rowId = input * 10 + i;
                builder.add(k, rowId, i === 3);
                if (!bottom || i !== 3) expected.push({ key: k, rowId, tombstone: i === 3 });
              }
              const summary = need(builder.finish());
              summaries.push(summary);
              const ref = runRef(summary, (await writer.write(summary.parts)).blobs);
              refs.push(ref);
              readers.push(await RunReader.open(h.space, ref, UNCHARGED, UNCHARGED));
            }
            grant = await h.governor.admit({
              label: "overflow merge",
              minimumBytes: mergeWorkingBytes(refs),
              spillable: false,
            });
            output = await mergeRuns({
              space: h.space,
              writer: resultWriter,
              grant,
              outputBudget: UNCHARGED,
              readers,
              level: 1,
              bottom,
              policy: { ...DEFAULT_MERGE_POLICY, splitEntries: 3, sliceEntries: 11 },
              stopped: () => false,
            });
            expected.sort((a, b) => compare(a.key, b.key) || a.rowId - b.rowId);
            let n = 0;
            for (const ref of output.runs) {
              expect(ref.encoding).toBe(2);
              const r = await RunReader.open(h.space, ref, UNCHARGED, UNCHARGED);
              await r.cursor(h.space, UNCHARGED, (key, rowId, tombstone) => {
                expectEntry(key, { rowId, tombstone }, need(expected[n++]));
                return false;
              });
            }
            expect(n).toBe(expected.length);
            expect(grant.allocated).toBe(0);
            reach.reach("split merge");
            reach.reach("owned merge heads");
            reach.reach(fan === 32 ? "fan in 32" : "other fan in");
          } finally {
            output?.release();
            for (const s of summaries) s.release();
            grant?.close();
            await resultWriter.abort();
            await writer.abort();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120_000);

for (const seed of seedsFor("next-overflow-reader-boundaries", [0xb507]))
  it(`hot fence ties and in-flight close/cancel use owned heads (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(2049, 49155, 65537),
        fc.integer({ min: 160, max: 200 }),
        async (width, count) => {
          reach.beginHistory();
          const entries = Array.from({ length: count }, (_, i) => ({
            key: key(width, 1),
            rowId: i,
            tombstone: i % 9 === 0,
          }));
          const f = fixture(entries, false),
            b = new Budget();
          let reader: RunReader | undefined;
          try {
            reader = await RunReader.open(f.space, f.ref, b, b);
            const found: Array<{ rowId: number; tombstone: boolean }> = [];
            await reader.lookup(f.space, need(entries[0]).key, found, b);
            expect(found).toEqual(entries.map(({ rowId, tombstone }) => ({ rowId, tombstone })));
            expect(reader.blockCount).toBeGreaterThan(1);
            reach.reach("hot key crosses fences");
            const resident = rangeRunBytes(f.ref, 1);
            expect(b.allocate(resident * 2)).toBe(true);
            const first = new RunRangeInput(
              f.space,
              b,
              f.ref,
              { lower: null, upper: null },
              1,
              Infinity,
              new AbortController().signal,
            );
            const second = new RunRangeInput(
              f.space,
              b,
              f.ref,
              { lower: null, upper: null },
              -1,
              Infinity,
              new AbortController().signal,
            );
            try {
              await first.open();
              await second.open();
              for (let i = 0; i < count; i++) {
                expect(await first.advance()).toBe(true);
                const retained = first.key();
                expect(await second.advance()).toBe(true);
                expectEntry(retained, first, need(entries[i]));
                expectEntry(second.key(), second, need(entries[count - 1 - i]));
              }
              reach.reach("interleaved readers");
            } finally {
              first.close();
              second.close();
              b.free(resident * 2);
            }
            for (const mode of ["close", "cancel"] as const) {
              const controller = new AbortController();
              expect(b.allocate(resident)).toBe(true);
              const input = new RunRangeInput(
                f.space,
                b,
                f.ref,
                { lower: null, upper: null },
                1,
                Infinity,
                controller.signal,
              );
              const original = f.space.read.bind(f.space);
              let release!: () => void, arrived!: () => void;
              const arrival = new Promise<void>((r) => {
                arrived = r;
              });
              const gate = new Promise<void>((r) => {
                release = r;
              });
              try {
                await input.open();
                f.space.read = async (refs, options) => {
                  const lease = await original(refs, options);
                  arrived();
                  await gate;
                  return lease;
                };
                const pending = input.advance();
                await arrival;
                if (mode === "close") input.close();
                else controller.abort(new Error("cancelled in flight"));
                release();
                if (mode === "close") expect(await pending).toBe(false);
                else {
                  // A cancelled read may observe its signal before or after copying the block;
                  // in either case no row is emitted and its lease has settled.
                  try {
                    expect(await pending).toBe(false);
                  } catch (error) {
                    expect(String(error)).toContain("cancelled in flight");
                  }
                }
                expect(f.leases).toBe(0);
                reach.reach(mode === "close" ? "close during read" : "cancel during read");
              } finally {
                release();
                f.space.read = original;
                input.close();
                b.free(resident);
              }
            }
          } finally {
            if (reader) b.free(reader.bytes);
            f.run.release();
          }
          expect(f.leases).toBe(0);
          expect(b.used).toBe(0);
        },
      ),
      { seed, numRuns: 8 },
    );
  }, 120_000);
