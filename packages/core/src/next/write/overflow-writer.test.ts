/** Approved overflow format: complete keys survive bounded storage and ownership transfer. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, OVERFLOW_WRITER_STATES } from "../../testing/reach.js";
import { UnsupportedEncodingError } from "../format/encoding.js";
import { encodeManifestRoot } from "../format/manifest-codec.js";
import { ByteReader as MetaReader } from "../format/bytes.js";
import {
  readKeyPointer,
  EXTENDED_KEY_BLOCK,
  KEY_DIRECTORY_MAGIC,
  KEY_DIRECTORY_VERSION,
  type KeyPointer,
} from "../format/key-pointer.js";
import { readRunKey } from "../format/run-key.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { Governor } from "../memory/governor.js";
import type { ByteBudget } from "../access/space.js";
import { OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import { KeyDelta, type DeltaVisitor } from "./key-hash.js";
import { ByteReader, MemtableMemoryError } from "./bytes.js";
import { RunEncoder, RunBuilder, runRef, RUN_BUFFER_OVERHEAD, type RunSummary } from "./runs.js";

const NativeBytes = Uint8Array;
const reach = createReach("overflow writer", OVERFLOW_WRITER_STATES);
function sameBytes(actual: Uint8Array | undefined, expected: Uint8Array | undefined): void {
  if (actual === undefined || expected === undefined) throw new Error("missing expected key");
  expect(Buffer.compare(actual, expected), "complete key byte equality").toBe(0);
}
afterAll(() => reach.assertReached());
interface Row {
  key: Uint8Array;
  id: number;
  tombstone: boolean;
}
interface Fault {
  kind: "credit" | "native" | "emit";
  at: number;
}
interface Trace {
  call: number;
  phase: string;
}
function encode(
  keys: Uint8Array[],
  repeat: number,
  bottom: boolean,
  borrow: boolean,
  detach: boolean,
  batch: number,
  fault?: Fault,
  backing?: ByteBudget,
  base = 0,
) {
  let paid = 0,
    peak = 0,
    call = 0,
    active = false,
    phase = "construct",
    partNo = 0,
    offset = 0;
  const nativeTokens: number[] = [],
    allocations: Trace[] = [],
    natives: Trace[] = [],
    pending: Uint8Array[] = [];
  const persisted: Array<{ offset: number; data: Uint8Array }> = [];
  const emits: Array<{ at: number; kind: string }> = [];
  const emittedFailure = new Error("injected emit failure"),
    nativeFailure = new Error("injected native construction failure");
  let encoder: RunEncoder | undefined,
    summary: RunSummary | null = null;
  const budget = {
    allocate(n: number) {
      call++;
      allocations.push({ call, phase });
      if (fault?.kind === "credit" && fault.at === call) return false;
      if (backing !== undefined && !backing.allocate(n)) return false;
      if (paid + n > 2 * 1024 * 1024) throw new Error("declared encoder minimum exceeded");
      paid += n;
      peak = Math.max(peak, paid);
      if (n >= RUN_BUFFER_OVERHEAD) nativeTokens.push(n - RUN_BUFFER_OVERHEAD);
      return true;
    },
    free(n: number) {
      backing?.free(n);
      paid -= n;
      if (paid < 0) throw new Error("double refund");
    },
  };
  const ack = () => {
    while (pending.length) {
      const part = pending.shift();
      if (!part) throw new Error("missing part");
      const before = paid,
        backing = part.buffer.byteLength + RUN_BUFFER_OVERHEAD;
      encoder?.releasePart(part);
      if (before - paid !== backing)
        throw new Error("emitted backing refunded before acknowledgement");
      encoder?.releasePart(part);
      if (before - paid !== backing) throw new Error("duplicate acknowledgement refunded twice");
      if (detach && part.buffer instanceof ArrayBuffer)
        structuredClone(part.buffer, { transfer: [part.buffer] });
    }
  };
  const ProxyBytes = new Proxy(NativeBytes, {
    construct(target, args) {
      const first: unknown = args[0];
      if (active && typeof first === "number") {
        const token = nativeTokens.shift();
        if (token !== first)
          throw new Error(`native allocation ${String(first)} preceded credit (${String(token)})`);
        natives.push({ call, phase });
        if (fault?.kind === "native" && fault.at === call) throw nativeFailure;
      }
      return Reflect.construct(target, args) as Uint8Array;
    },
  });
  const expected: Row[] = [];
  try {
    globalThis.Uint8Array = ProxyBytes;
    active = true;
    encoder = new RunEncoder(
      (part) => {
        active = false;
        try {
          const n = part.length;
          persisted.push({ offset, data: new NativeBytes(part) });
          offset += n;
          pending.push(part);
          const at = partNo++;
          const view = new DataView(part.buffer, part.byteOffset, part.byteLength);
          const count = n >= 4 ? view.getUint16(0, true) & ~EXTENDED_KEY_BLOCK : 0;
          const kind =
            n >= 5 && view.getUint32(0, true) === KEY_DIRECTORY_MAGIC
              ? "directory"
              : n >= 4 &&
                  n <= 4096 &&
                  count > 0 &&
                  2 + count * 2 <= n &&
                  view.getUint16(2, true) === 2 + count * 2
                ? "block"
                : "payload";
          emits.push({ at, kind });
          if (fault?.kind === "emit" && fault.at === at) throw emittedFailure;
          if (pending.length >= batch) ack();
        } finally {
          active = true;
        }
      },
      bottom,
      borrow,
      budget,
      2,
    );
    phase = "write";
    let id = base + 1;
    const add = (key: Uint8Array, tombstone: boolean) => {
      active = false;
      const source = borrow ? key : new NativeBytes(key);
      active = true;
      encoder?.add(source, 0, source.length, id, tombstone);
      active = false;
      if (!borrow) source.fill(238);
      active = true;
    };
    for (const key of keys)
      for (let r = 0; r < repeat; r++, id++) {
        if (r % 3 === 0) {
          add(key, false);
          add(key, true);
        } else if (r % 3 === 1) {
          add(key, true);
          if (!bottom) expected.push({ key, id, tombstone: true });
        } else {
          add(key, false);
          expected.push({ key, id, tombstone: false });
        }
      }
    phase = "finish";
    summary = encoder.finish();
    active = false;
    ack();
    active = true;
    encoder.close();
    active = false;
    expect(nativeTokens).toHaveLength(0);
    if (summary === null) {
      expect(expected).toHaveLength(0);
      expect(persisted).toHaveLength(0);
      expect(paid).toBe(0);
    } else {
      const storage = summary.keyStorage;
      if (!storage) throw new Error("missing overflow summary ownership");
      const arrays = [summary.minKey, summary.maxKey, storage.minimum, storage.maximum];
      expect(paid, "live summary buffers and holder stay funded").toBe(
        arrays.reduce(
          (n, a) => n + a.buffer.byteLength + RUN_BUFFER_OVERHEAD,
          OBJECT_OVERHEAD_BYTES,
        ),
      );
      expect(summary.encoding).toBe(2);
      expect(summary.entryCount).toBe(expected.length);
      const extent = { offset: 0, length: offset };
      const refs = persisted.map((p) => ({
        extent,
        offset: p.offset,
        length: p.data.length,
        checksum: xxHash32(p.data),
      }));
      const stored = runRef(summary, refs);
      expect(stored.encoding).toBe(2);
      expect(stored.keyStorage).toBe(storage);
      const blob = refs[0];
      if (!blob) throw new Error("missing fixture blob");
      expect(() =>
        encodeManifestRoot({
          formatVersion: 3,
          sequence: 1,
          epoch: 1,
          log: { salt: 1, firstLsn: 1, rootSequence: 1 },
          globalLsn: 0,
          catalogVersion: 1,
          catalog: blob,
          nextTableId: 2,
          sequences: [],
          tables: [
            {
              tableId: 1,
              schemaVersion: 1,
              nextRowId: id,
              flushedThroughLsn: 0,
              liveRowCount: expected.length,
              groups: blob,
              groupCount: 0,
              staged: [],
              keyIndex: { indexId: 1, columnIds: [1], unique: true, runs: [stored] },
              secondaryIndexes: [],
              statistics: null,
            },
          ],
          clients: [],
          extentList: [],
        }),
      ).toThrow(UnsupportedEncodingError);
      reach.reach("publication reader guards");
      sameBytes(summary.minKey, expected[0]?.key);
      sameBytes(summary.maxKey, expected.at(-1)?.key);
      const map = new Map(persisted.map((p) => [p.offset, p.data]));
      const get = (at: number, length: number, checksum: number) => {
        const b = map.get(at);
        if (!b) throw new Error("dangling reference");
        expect(b.length).toBe(length);
        expect(xxHash32(b)).toBe(checksum);
        return b;
      };
      const decoded = new Map<number, Uint8Array>();
      const resolve = (pointer: KeyPointer): Uint8Array => {
        if (pointer.kind === "inline") return pointer.bytes;
        const cached = decoded.get(pointer.directory.offset);
        if (cached) return cached;
        const ref = pointer.directory,
          dir = new MetaReader(get(ref.offset, ref.length, ref.checksum));
        expect(dir.u32()).toBe(KEY_DIRECTORY_MAGIC);
        expect(dir.u8()).toBe(KEY_DIRECTORY_VERSION);
        const key = readRunKey(dir, ref.offset);
        dir.end();
        if (key.kind !== "overflow") throw new Error("long pointer needs overflow directory");
        expect(key.length).toBe(pointer.length);
        sameBytes(key.prefix, pointer.prefix);
        const full = new NativeBytes(key.length);
        let at = 0;
        for (const chunk of key.chunks) {
          full.set(get(chunk.offset, chunk.length, chunk.checksum), at);
          at += chunk.length;
        }
        sameBytes(full.subarray(0, 32), pointer.prefix);
        decoded.set(ref.offset, full);
        return full;
      };
      const indexPart = persisted.at(-3),
        fencesPart = persisted.at(-2);
      if (!indexPart || !fencesPart) throw new Error("missing tails");
      const index = new ByteReader(indexPart.data),
        fences = new MetaReader(fencesPart.data),
        count = index.varuint();
      expect(fences.uint()).toBe(count);
      let seen = 0,
        extended = 0,
        plain = 0;
      for (let b = 0; b < count; b++) {
        const blockOffset = index.varuint(),
          length = index.varuint(),
          checksum = index.u32(),
          block = get(blockOffset, length, checksum);
        expect(block.length).toBeLessThanOrEqual(4096);
        const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
        const header = view.getUint16(0, true),
          isLong = (header & EXTENDED_KEY_BLOCK) !== 0,
          n = header & ~EXTENDED_KEY_BLOCK;
        if (isLong) extended++;
        else plain++;
        const fence = resolve(readKeyPointer(fences, blockOffset));
        for (let k = 0; k < n; k++) {
          const start = view.getUint16(2 + k * 2, true),
            end = k + 1 < n ? view.getUint16(4 + k * 2, true) : block.length;
          let key: Uint8Array;
          if (isLong) {
            const r = new MetaReader(block.subarray(start, end - 7));
            key = resolve(readKeyPointer(r, blockOffset));
            r.end();
          } else {
            const r = new ByteReader(block, start);
            key = r.blob();
            expect(r.offset).toBe(end - 7);
          }
          const wanted = expected[seen++];
          if (!wanted) throw new Error("extra output row");
          sameBytes(key, wanted.key);
          if (k === 0) sameBytes(fence, key);
          expect(view.getUint32(end - 7, true) + view.getUint16(end - 3, true) * 2 ** 32).toBe(
            wanted.id,
          );
          expect((block[end - 1] ?? 0) & 1).toBe(wanted.tombstone ? 1 : 0);
        }
      }
      index.end();
      fences.end();
      expect(seen).toBe(expected.length);
      const minimum = resolve(readKeyPointer(new MetaReader(storage.minimum), indexPart.offset));
      const maximum = resolve(readKeyPointer(new MetaReader(storage.maximum), indexPart.offset));
      sameBytes(minimum, expected[0]?.key);
      sameBytes(maximum, expected.at(-1)?.key);
      if (extended) reach.reach("extended blocks");
      if (plain) reach.reach("inline blocks");
      if (extended && plain) reach.reach("mixed blocks");
      expect(storage.maxKeyBytes).toBe(Math.max(...expected.map((x) => x.key.length)));
      summary.release();
      summary.release();
      expect(paid).toBe(0);
      reach.reach("funded until consumed");
    }
    return {
      allocations,
      natives,
      emits: emits.map((e) => ({ ...e, kind: e.at >= persisted.length - 3 ? "tail" : e.kind })),
      parts: persisted.length,
      peak,
    };
  } finally {
    active = false;
    pending.length = 0;
    try {
      summary?.release();
      encoder?.close();
    } finally {
      globalThis.Uint8Array = NativeBytes;
    }
    expect(paid, "encoder allocations must be settled").toBe(0);
  }
}
function keys(width: number, count: number, mixed: boolean): Uint8Array[] {
  return Array.from({ length: count }, (_, i) => {
    const n = mixed && width > 2048 && i % 2 ? 20 : width;
    const k = new NativeBytes(n).fill(129);
    if (n) {
      k[n - 1] = i;
      if (mixed && width > 2048) k[0] = i;
    }
    return k;
  }).sort((a, b) => Buffer.compare(a, b));
}
for (const seed of seedsFor("next-overflow-writer", [0xb511]))
  it(`funded bounded blocks and full keys (${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom(0, 2048, 2049, 4096, 49155, 65537),
        fc.constantFrom(4, 8, 80),
        fc.integer({ min: 1, max: 8 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom(1, 4, 8),
        fc.constantFrom(0, 2 ** 32, 2 ** 48 - 1024),
        (width, n, repeat, bottom, borrow, detach, batch, base) => {
          reach.beginHistory();
          encode(
            keys(width, n, borrow),
            repeat,
            bottom,
            borrow,
            detach,
            batch,
            undefined,
            undefined,
            base,
          );
          if (base > 0) reach.reach("wide row identifiers");
          reach.reach(borrow ? "borrowed input" : "copied input");
          reach.reach(bottom ? "bottom tombstones" : "retained tombstones");
          if (detach) reach.reach("acknowledged buffers detached");
          if (width === 49155) reach.reach("shipped maximum");
          if (width > 65536) reach.reach("multiple payload chunks");
          if (repeat < 3 && bottom) reach.reach("empty output");
          reach.reach("bounded budget");
        },
      ),
      { seed, numRuns: 60 },
    );
  }, 120000);
for (const seed of seedsFor("next-overflow-writer-failure", [0xb512]))
  it(`failure unwinds before identical retry (${String(seed)})`, () => {
    fc.assert(
      fc.property(
        fc.constantFrom(2049, 49155, 65537),
        fc.constantFrom("credit" as const, "native" as const, "emit" as const),
        fc.constantFrom("construct", "write", "finish"),
        fc.constantFrom("payload", "directory", "block", "tail"),
        fc.boolean(),
        (width, kind, phase, emission, detach) => {
          reach.beginHistory();
          const input = keys(width, 4, true),
            trace = encode(input, 4, false, false, detach, 4);
          const events = (kind === "native" ? trace.natives : trace.allocations).filter(
            (x) => x.phase === phase,
          );
          const at =
            kind === "emit" ? trace.emits.find((e) => e.kind === emission)?.at : events[0]?.call;
          if (at === undefined) throw new Error("missing failure phase");
          expect(() => encode(input, 4, false, false, detach, 4, { kind, at })).toThrow(
            kind === "credit"
              ? MemtableMemoryError
              : kind === "native"
                ? /injected native/
                : /injected emit/,
          );
          encode(input, 4, false, false, detach, 4);
          reach.reach("clean failure retry");
          if (kind === "emit") reach.reach(`${emission} failed`);
          else {
            reach.reach(kind === "credit" ? "credit failed" : "native failed");
            reach.reach(`${phase} failed`);
          }
        },
      ),
      { seed, numRuns: 40 },
    );
  }, 120000);

for (const seed of seedsFor("next-overflow-writer-admission", [0xb513]))
  it(`pressure waits cancellably before any encoder allocation (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (cancel) => {
        reach.beginHistory();
        const cap = 8 * 1024 * 1024,
          governor = new Governor(cap);
        const inputGrant = await governor.admit({
          label: "input fixture",
          minimumBytes: 320 * 1024,
          spillable: false,
        });
        expect(inputGrant.allocate(320 * 1024)).toBe(true);
        const input = keys(49155, 4, true);
        const pinned = await governor.admit({
          label: "other pinned owner",
          minimumBytes: 6 * 1024 * 1024 + 192,
          spillable: false,
        });
        expect(pinned.allocate(6 * 1024 * 1024 + 192)).toBe(true);
        const bytes = new NativeBytes(6 * 1024 * 1024).fill(7),
          stop = new AbortController(),
          reason = new Error("cancelled before encoder admission");
        let started = false;
        const work = governor
          .admit({
            label: "complete overflow minimum",
            minimumBytes: 2 * 1024 * 1024,
            spillable: false,
            signal: stop.signal,
          })
          .then((grant) => {
            started = true;
            try {
              encode(input, 4, false, false, true, 4, undefined, grant);
              expect(grant.allocated).toBe(0);
            } finally {
              grant.close();
            }
          });
        const settled = work.then(
          () => null,
          (error: unknown) => error,
        );
        try {
          await Promise.resolve();
          expect(started).toBe(false);
          expect(bytes[0]).toBe(7);
          expect(bytes.at(-1)).toBe(7);
          reach.reach("waiting before encoder");
          if (cancel) {
            stop.abort(reason);
            expect(await settled).toBe(reason);
            reach.reach("cancelled admission");
          } else {
            pinned.close();
            const error = await settled;
            if (error !== null)
              throw error instanceof Error
                ? error
                : new Error("admission failed", { cause: error });
            expect(started).toBe(true);
            reach.reach("resumed admission");
          }
        } finally {
          stop.abort(reason);
          await settled;
          pinned.close();
          inputGrant.close();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 12 },
    );
  }, 120000);

it("two overflow encoders retain private scratch when their operations interleave", () => {
  reach.beginHistory();
  const a = new NativeBytes(49155).fill(129),
    b = new NativeBytes(a);
  a[a.length - 1] = 11;
  b[b.length - 1] = 12;
  let paidA = 0,
    paidB = 0;
  const payloadA: Uint8Array[] = [],
    payloadB: Uint8Array[] = [];
  const budgetA = {
      allocate(n: number) {
        paidA += n;
        return true;
      },
      free(n: number) {
        paidA -= n;
      },
    },
    budgetB = {
      allocate(n: number) {
        paidB += n;
        return true;
      },
      free(n: number) {
        paidB -= n;
      },
    };
  const one: RunEncoder = new RunEncoder(
    (part) => {
      if (part.length === a.length) payloadA.push(new NativeBytes(part));
      one.releasePart(part);
      if (part.buffer instanceof ArrayBuffer)
        structuredClone(part.buffer, { transfer: [part.buffer] });
    },
    false,
    false,
    budgetA,
    2,
  );
  const two: RunEncoder = new RunEncoder(
    (part) => {
      if (part.length === b.length) payloadB.push(new NativeBytes(part));
      two.releasePart(part);
      if (part.buffer instanceof ArrayBuffer)
        structuredClone(part.buffer, { transfer: [part.buffer] });
    },
    false,
    true,
    budgetB,
    2,
  );
  let first: RunSummary | null = null,
    second: RunSummary | null = null;
  try {
    for (let id = 1; id <= 32; id++) {
      one.add(a, 0, a.length, id, false);
      two.add(b, 0, b.length, id, false);
    }
    first = one.finish();
    second = two.finish();
    if (!first || !second) throw new Error("missing interleaved summaries");
    sameBytes(first.minKey, a);
    sameBytes(first.maxKey, a);
    sameBytes(second.minKey, b);
    sameBytes(second.maxKey, b);
    expect(payloadA).toHaveLength(1);
    expect(payloadB).toHaveLength(1);
    sameBytes(payloadA[0], a);
    sameBytes(payloadB[0], b);
  } finally {
    first?.release();
    second?.release();
    one.close();
    two.close();
  }
  expect(paidA).toBe(0);
  expect(paidB).toBe(0);
  reach.reach("interleaved scratch");
});

for (const seed of seedsFor("next-overflow-bulk-writer", [0xb515]))
  it(`bulk input retains full key payloads and ownership (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(2049, 49155, 65537), fc.boolean(), async (width, borrow) => {
        reach.beginHistory();
        let paid = 0;
        const budget = {
          allocate(n: number) {
            paid += n;
            return true;
          },
          free(n: number) {
            paid -= n;
            if (paid < 0) throw new Error("negative bulk charge");
          },
        };
        const input = keys(width, 2, false),
          delta = new KeyDelta(budget),
          builder = new RunBuilder(false, borrow, budget, 2);
        let result: ReturnType<RunBuilder["finish"]> = null;
        try {
          for (let i = 0; i < input.length; i++) {
            const key = input[i];
            if (!key) throw new Error("missing key");
            delta.add(key, 0, key.length, i + 1, 1, false);
          }
          const visitor: DeltaVisitor = (bytes, from, to, id, tombstone, hash, bloomHash) =>
            builder.addRange(bytes, from, to, id, tombstone, hash, bloomHash);
          visitor.singles = (data, from, to, bound, keep) =>
            builder.addSingleBatch(data, from, to, bound, keep);
          await delta.forEachSorted(1, visitor);
          result = builder.finish();
          if (!result) throw new Error("missing bulk output");
          expect(result.entryCount).toBe(2);
          expect(result.encoding).toBe(2);
          expect(result.keyStorage?.maxKeyBytes).toBe(width);
          let offset = 0;
          const parts = new Map<number, Uint8Array>();
          for (const part of result.parts) {
            parts.set(offset, part);
            offset += part.length;
          }
          const get = (at: number, n: number, checksum: number) => {
            const part = parts.get(at);
            if (!part) throw new Error("missing bulk part");
            expect(part.length).toBe(n);
            expect(xxHash32(part)).toBe(checksum);
            return part;
          };
          const resolve = (encoded: Uint8Array) => {
            const r = new MetaReader(encoded),
              pointer = readKeyPointer(r, offset);
            r.end();
            if (pointer.kind !== "overflow") throw new Error("expected bulk long key");
            const ref = pointer.directory,
              d = new MetaReader(get(ref.offset, ref.length, ref.checksum));
            expect(d.u32()).toBe(KEY_DIRECTORY_MAGIC);
            expect(d.u8()).toBe(KEY_DIRECTORY_VERSION);
            const key = readRunKey(d, ref.offset);
            d.end();
            if (key.kind !== "overflow") throw new Error("expected bulk chunk directory");
            const full = new NativeBytes(key.length);
            let at = 0;
            for (const chunk of key.chunks) {
              expect(chunk.length).toBeLessThanOrEqual(65536);
              full.set(get(chunk.offset, chunk.length, chunk.checksum), at);
              at += chunk.length;
            }
            expect(at).toBe(width);
            return full;
          };
          if (!result.keyStorage) throw new Error("missing bulk descriptors");
          sameBytes(resolve(result.keyStorage.minimum), input[0]);
          sameBytes(resolve(result.keyStorage.maximum), input[1]);
          reach.reach("bulk supplied hashes");
        } finally {
          result?.release();
          builder.close();
          delta.releaseCharge();
        }
        expect(paid).toBe(0);
      }),
      { seed, numRuns: 20 },
    );
  }, 120000);
