/** BENCH adapter proof: transferred output equals source; cached/pool buffers never detach. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach } from "../../../packages/core/src/testing/reach.js";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import { PageCache } from "../../../packages/core/src/next/storage/page-cache.js";
import {
  countingBudget,
  countingMemory,
  expectBytes,
} from "../../../packages/core/src/next/storage/space-test-support.js";
import type { StoreSession } from "../../../packages/core/src/next/storage/contract.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import { GroupWriter } from "../../../packages/core/src/next/page/writer.js";
import { ChunkReader } from "../../../packages/core/src/next/page/reader.js";
import { TableScan } from "../../../packages/core/src/next/exec/table-scan.js";
import { chunkReads } from "../../../packages/core/src/next/exec/scan.js";
import { OutputArena } from "../../../packages/core/src/next/exec/kernels/vectors.js";
import { numbers, texts, I32 } from "../../../packages/core/src/next/exec/kernels/test-support.js";
import {
  ResultChannel,
  frameTransferList,
  type FramePayload,
  type ResultFrame,
} from "../../../packages/core/src/next/exec/result.js";
import { StreamDictionaries, transferFor } from "../../../packages/core/src/next/owner/frame.js";
import type { FlatVector, Batch } from "../../../packages/core/src/next/vector/vector.js";
import type { BlobRef } from "../../../packages/core/src/next/storage/extents.js";
import { benchTableOf } from "./scan-export.js";
import { BenchFrameAssembler } from "./owner-frames.js";

const little = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1;
for (const seed of seedsFor("export-owned-scan", [0xb2a5, 0xb2a6]))
  it(`owned scan transfer histories seed ${String(seed)}`, async () => {
    const reach = createReach("export-owned-scan", [
      "direct",
      "dictionary policy",
      "capped text fallback",
      "different result budget",
      "all column ownership",
      "mixed fallback",
      "nullable",
      "out of line",
      "credit wait",
      "abort retained",
      "error cleanup",
      "signaling NaN",
      "signed zero",
      "pool reuse",
      "cached canary",
      "real transfer",
    ] as const);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 512, max: 900 }),
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom("end", "abort", "error"),
        fc.constantFrom(true as const, "dictionary" as const),
        fc.boolean(),
        fc.boolean(),
        async (n, nullable, mixed, large, ending, ownership, capped, mismatchedBudget) => {
          reach.beginHistory();
          reach.reach(ownership === "dictionary" ? "dictionary policy" : "all column ownership");
          reach.reach(mixed ? "mixed fallback" : "direct");
          if (nullable) reach.reach("nullable");
          if (large) reach.reach("out of line");
          if (mismatchedBudget) reach.reach("different result budget");
          const mask = nullable ? new Uint32Array(Math.ceil(n / 32)).fill(0xeeeeeeee) : null;
          const f64raw = new Uint32Array(2 * n),
            f32raw = new Uint32Array(n);
          for (let k = 0; k < n; k++) {
            f64raw[2 * k + (little ? 0 : 1)] = k % 3 === 1 ? 0 : 0x12340000 + k;
            f64raw[2 * k + (little ? 1 : 0)] =
              k % 3 === 0 ? 0x7ff00000 : k % 3 === 1 ? 0x80000000 : 0x3ff00000;
            f32raw[k] = k % 3 === 0 ? 0x7f801234 : k % 3 === 1 ? 0x80000000 : 0x3f800000;
          }
          const strings = Array.from({ length: n }, (_, k) =>
            nullable && k % 4 === 0
              ? null
              : large && k % 100 === 1
                ? "水".repeat(300)
                : (["", "é", "😀"][k % 3] ?? ""),
          );
          const source: FlatVector[] = [
            numbers(
              I32,
              Array.from({ length: n }, (_, k) => k * 17),
            ),
            {
              form: "flat",
              type: { kind: "float64" },
              length: n,
              validity: mask,
              data: { layout: "f64", values: new Float64Array(f64raw.buffer) },
            },
            {
              form: "flat",
              type: { kind: "float32" },
              length: n,
              validity: mask,
              data: { layout: "f32", values: new Float32Array(f32raw.buffer) },
            },
            texts(strings),
          ];
          const space = new MemoryExtentSpace(),
            writer = new GroupWriter({
              writer: space.beginWrite("owned"),
              columns: source.map((v, k) => ({ columnId: k + 1, type: v.type })),
              inlineBytes: 512,
            });
          await writer.append(source, n);
          const group = await writer.finish();
          let failDecode = false;
          const cache = new PageCache({
            readDataInto(at: number, out: Uint8Array) {
              out.set(space.bytes.subarray(at, at + out.length));
              return Promise.resolve();
            },
          } as StoreSession);
          const memory = countingMemory(Infinity),
            budget = countingBudget(Infinity),
            results = countingBudget(Infinity);
          cache.attach(memory);
          const forbidden = new Set<ArrayBuffer>();
          const note = (bytes: Uint8Array) => {
            if (!(bytes.buffer instanceof ArrayBuffer)) throw new Error("Shared source");
            forbidden.add(bytes.buffer);
          };
          const pages = {
            read: async (...args: Parameters<typeof cache.read>) => {
              const lease = await cache.read(...args);
              for (const b of lease.bytes) note(b);
              return lease;
            },
            peek: (...args: Parameters<typeof cache.peek>) => {
              const b = cache.peek(...args);
              if (b) note(b);
              return b;
            },
            parsed: cache.parsed.bind(cache),
            attach: cache.attachParsed.bind(cache),
          };
          const first = group.columns[0],
            second = group.columns[1];
          if (!first || !second) throw new Error("Missing chunks");
          const reader = await ChunkReader.open(pages, first, { budget });
          const entry = reader.directory.pages[0];
          if (!entry) throw new Error("Missing page");
          const canaryRef: BlobRef = {
            extent: first.data.extent,
            offset: first.data.offset + entry.offset,
            length: entry.length,
            checksum: entry.checksum,
          };
          reader.close();
          const cached = await pages.read([canaryRef], { budget });
          const canary = cached.bytes[0];
          if (!canary) throw new Error("Missing canary");
          const before = canary.slice();
          cached.release();
          reach.reach("cached canary");
          const other = await ChunkReader.open(pages, second, { budget, once: true });
          const e = other.directory.pages[0];
          if (!e) throw new Error("Missing page");
          const poolRef: BlobRef = {
            extent: second.data.extent,
            offset: second.data.offset + e.offset,
            length: e.length,
            checksum: e.checksum,
          };
          other.close();
          const prime = await pages.read([poolRef], { budget, once: true, reuseBuffers: true });
          prime.release();
          expect(cache.stats.poolBytes).toBeGreaterThan(0);
          const arena = new OutputArena(budget, 2048),
            table = benchTableOf([{ rows: n, chunks: group.columns }]);
          const scan = new TableScan({
            table,
            pages,
            reads: {
              ...chunkReads,
              open(p, c, o) {
                return Promise.resolve(chunkReads.open(p, c, o)).then(
                  (r) =>
                    new Proxy(r, {
                      get(target, key) {
                        if (key === "decodeRange" || key === "gather")
                          return (...args: unknown[]) => {
                            if (failDecode && c.columnId === 2)
                              throw new Error("injected owned decode error");
                            return Reflect.apply(
                              Reflect.get(target, key) as (...xs: unknown[]) => unknown,
                              target,
                              args,
                            );
                          };
                        const value: unknown = Reflect.get(target, key);
                        return typeof value === "function" ? value.bind(target) : value;
                      },
                    }),
                );
              },
              loadTogether(loads) {
                return Promise.all(loads.map((l) => l.reader.load(l.pages))).then(() => undefined);
              },
            },
            readWindowBytes: 64 * 1048576,
            transferOutputs: ownership,
            outputBudget: results,
            budget,
            columns: [1, 2, 3, 4],
            filter: null,
            emitLocator: false,
            targetBytes: 2048,
            context: { arena, params: { slots: [] }, input: [] },
          });
          const dictionaries = new StreamDictionaries();
          const frameBudget = mismatchedBudget ? countingBudget(Infinity) : results;
          const assembler = new BenchFrameAssembler(frameBudget, capped ? 0 : undefined),
            channel = new ResultChannel([], { windowBytes: Infinity });
          const held: Array<{ payload: FramePayload; bytes: Uint8Array[]; release: () => void }> =
            [];
          let inProgress: ResultFrame | null = null;
          let row = 0,
            frames = 0;
          const check = (payload: FramePayload, start: number) => {
            for (let k = 0; k < payload.rowCount; k++) {
              const id = payload.columns[0],
                f = payload.columns[1],
                s = payload.columns[2],
                t = payload.columns[3];
              if (
                id?.form !== "flat" ||
                id.data.layout !== "i32" ||
                f?.form !== "flat" ||
                f.data.layout !== "f64" ||
                s?.form !== "flat" ||
                s.data.layout !== "f32" ||
                (t?.form !== "flat" && t?.form !== "dictionary")
              )
                throw new Error("Bad owned layouts");
              expect(id.data.values[k]).toBe((start + k) * 17);
              const valid = !nullable || (start + k) % 4 !== 0;
              expect(
                f.validity === null || ((f.validity[k >>> 5] ?? 0) & (1 << (k & 31))) !== 0,
              ).toBe(valid);
              if (valid) {
                expectBytes(
                  new Uint8Array(f.data.values.buffer, 8 * k, 8),
                  new Uint8Array(f64raw.buffer, 8 * (start + k), 8),
                );
                expectBytes(
                  new Uint8Array(s.data.values.buffer, 4 * k, 4),
                  new Uint8Array(f32raw.buffer, 4 * (start + k), 4),
                );
                if ((start + k) % 3 === 0) reach.reach("signaling NaN");
                if ((start + k) % 3 === 1) reach.reach("signed zero");
              }
              const textValid = strings[start + k] !== null;
              expect(
                t.validity === null || ((t.validity[k >>> 5] ?? 0) & (1 << (k & 31))) !== 0,
              ).toBe(textValid);
              if (textValid) {
                const located =
                  t.form === "dictionary"
                    ? dictionaries.locate(t.dictionary, t.codes[k] ?? 0)
                    : { entries: t, index: k };
                if (located.entries.data.layout !== "varlen") throw new Error("Not UTF-8");
                const data = located.entries.data,
                  index = located.index;
                expect(
                  new TextDecoder().decode(
                    data.bytes.subarray(data.offsets[index], data.offsets[index + 1]),
                  ),
                ).toBe(strings[start + k]);
              }
            }
          };
          try {
            let item = await scan.next();
            while (item !== null) {
              let prefetched: typeof item | undefined;
              expect(
                item.transfer?.detachableBuffers?.size ?? 0,
                "owned output must carry its buffer permission",
              ).toBeGreaterThan(0);
              expect(item.transfer?.detachableBudget).toBe(results);
              for (const buffer of item.transfer?.detachableBuffers ?? [])
                expect(forbidden.has(buffer), "source marked a cached or pooled buffer").toBe(
                  false,
                );
              let batch: Batch = item.batch;
              if (mixed) {
                const forbiddenBuffer = [...forbidden].find(
                  (b) => b.byteLength >= 4 * batch.length && b.byteLength % 4 === 0,
                );
                if (!forbiddenBuffer) throw new Error("No forbidden full view");
                batch = {
                  ...batch,
                  columns: [
                    ...batch.columns,
                    {
                      form: "flat",
                      type: I32,
                      length: batch.length,
                      validity: null,
                      data: { layout: "i32", values: new Int32Array(forbiddenBuffer) },
                    },
                  ],
                };
              }
              channel.offer(assembler.offer(batch, item.transfer));
              const frame = channel.poll();
              inProgress = frame ?? null;
              if (!frame) throw new Error("No frame");
              for (const b of frame.transfer)
                expect(forbidden.has(b), "cached or pooled buffer transferred").toBe(false);
              expect(
                frameBudget.used,
                "result grant owns the full offered frame",
              ).toBeGreaterThanOrEqual(frame.byteLength);
              const bytes = frameTransferList(frame.payload).map((b) => new Uint8Array(b).slice());
              const transfer = transferFor(frame.transfer);
              if (transfer.length > 0) reach.reach("real transfer");
              const payload = structuredClone(frame.payload, { transfer });
              frameTransferList(payload).forEach((b, k) =>
                expectBytes(new Uint8Array(b), bytes[k] ?? new Uint8Array()),
              );
              dictionaries.apply(payload);
              check(payload, row);
              row += payload.rowCount;
              frames++;
              held.push({ payload, bytes, release: frame.release });
              inProgress = null;
              if (frames === 4 && !mixed && !mismatchedBudget) {
                const readsBefore = cache.stats.platformReads;
                let settled = false;
                const pending = Promise.resolve(scan.next()).then((x) => {
                  settled = true;
                  return x;
                });
                await Promise.resolve();
                await Promise.resolve();
                expect(settled).toBe(false);
                expect(cache.stats.platformReads).toBe(readsBefore);
                reach.reach("credit wait");
                held.shift()?.release();
                if (ending === "abort") {
                  scan.abort();
                  expect(await pending).toBeNull();
                  reach.reach("abort retained");
                  break;
                }
                if (ending === "error") {
                  failDecode = true;
                  await expect(pending).rejects.toThrow("injected owned decode error");
                  reach.reach("error cleanup");
                  break;
                }
                const next = await pending;
                if (!next) throw new Error("Credit returned no output");
                prefetched = next;
              }
              if (held.length >= 4) held.shift()?.release();
              item = prefetched ?? (await scan.next());
            }
            if (ending === "end" || mixed) expect(row).toBe(n);
            if (assembler.flattenedColumns > 0) reach.reach("capped text fallback");
            const reuse = await pages.read([poolRef], { budget, once: true, reuseBuffers: true });
            reach.reach("pool reuse");
            reuse.release();
            for (const h of held) {
              frameTransferList(h.payload).forEach((b, k) =>
                expectBytes(new Uint8Array(b), h.bytes[k] ?? new Uint8Array()),
              );
              h.release();
            }
            held.length = 0;
            expectBytes(canary, before);
            for (const b of forbidden) expect(b.byteLength).toBeGreaterThan(0);
          } finally {
            inProgress?.release();
            for (const h of held) h.release();
            channel.cancel();
            scan.close();
            arena.close();
            assembler.close();
            cache.close();
          }
          expect(budget.used).toBe(0);
          expect(results.used).toBe(0);
          expect(frameBudget.used).toBe(0);
          expect(memory.used).toBe(0);
        },
      ),
      { seed, numRuns: 48 },
    );
    reach.assertReached();
    console.log(
      "OWNED_TRANSFER_REACH",
      JSON.stringify({ seed, states: Object.fromEntries(reach.counts()) }),
    );
  }, 120000);
