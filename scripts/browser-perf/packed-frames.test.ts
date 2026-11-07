import fc from "fast-check";
import type { VectorData } from "../../packages/core/src/next/vector/vector.js";
import { afterAll, expect, it } from "vitest";
import {
  createReach,
  PACKED_FRAME_REACH,
  RESULT_OWNED_LAYOUT_REACH,
} from "../../packages/core/src/testing/reach.js";
import { seedsFor } from "../../packages/core/src/testing/seeds.js";
import type { FramePayload } from "../../packages/core/src/next/exec/result.js";
import { packedTestRuntime } from "./packed-test-runtime.js";
const runtime = await packedTestRuntime();
const {
  FrameBufferError,
  frameTransferList,
  payloadArrays,
  packFrame,
  packedFrameBytes,
  StreamDictionaries,
  transferFor,
  Governor,
  ResultChannel,
  ResultSink,
  packedInput,
} = runtime;
const reach = createReach("packed frames", PACKED_FRAME_REACH);
afterAll(() => {
  try {
    reach.assertReached();
  } finally {
    runtime.close();
  }
});
const bytes = (frame: FramePayload) =>
  payloadArrays(frame).map((v) => new Uint8Array(v.buffer, v.byteOffset, v.byteLength).slice());
for (const kind of [...RESULT_OWNED_LAYOUT_REACH, "decimal-wide"] as const)
  for (const seed of seedsFor("packed-frame-bytes", [0xb2a0, 0xb2a1]))
    it(`packed ${kind} is byte exact (${String(seed)})`, () => {
      fc.assert(
        fc.property(
          fc.integer({ min: 33, max: 100 }),
          fc.integer({ min: 1, max: 31 }),
          fc.boolean(),
          fc.boolean(),
          (n, salt, sparse, large) => {
            reach.beginHistory();
            const column = packedInput(kind === "decimal-wide" ? "decimal-safe" : kind, n, salt);
            if (!sparse) column.validity = null;
            if (column.data.layout === "f64") {
              const words = new Uint32Array(
                column.data.values.buffer,
                column.data.values.byteOffset,
                column.data.values.length * 2,
              );
              words.set([0, 0x80000000, 0, 0x7ff00000, 5, 0x7ff00001, 1, 0, 7, 0xfff80000]);
            }
            if (kind === "decimal-wide" && column.data.layout === "decimal")
              (column as { data: VectorData }).data = {
                ...column.data,
                coefficients: {
                  repr: "wide",
                  values: Array.from({ length: n }, (_, r) => (1n << 5000n) + BigInt(r * salt)),
                },
              };
            if (kind === "text" && large) {
              const width = 65536 + salt;
              (column as { data: VectorData }).data = {
                layout: "varlen",
                bytes: Uint8Array.from({ length: n * width }, (_, i) => (i + salt) % 251),
                offsets: Int32Array.from({ length: n + 1 }, (_, r) => r * width),
              };
              reach.reach("large varlen");
            }
            const delta = {
              form: "flat",
              length: 2,
              validity: null,
              data: {
                layout: "varlen",
                bytes: new TextEncoder().encode("alphabeta"),
                offsets: new Int32Array([0, 5, 9]),
              },
            } as const;
            const original: FramePayload = {
              sequence: 0,
              rowCount: n,
              columns: [
                column,
                {
                  form: "constant",
                  length: n,
                  value: packedInput(kind === "decimal-wide" ? "decimal-safe" : kind, 1, salt),
                },
                {
                  form: "dictionary",
                  length: n,
                  dictionary: 7,
                  codes: Int32Array.from({ length: n }, (_, r) => r % 2),
                  validity: column.validity?.slice() ?? null,
                },
              ],
              dictionaries: [{ id: 7, from: 0, values: delta }],
            };
            const expected = bytes(original),
              sourceBuffers = new Set(payloadArrays(original).map((v) => v.buffer));
            const size = packedFrameBytes(original);
            let charged = 0;
            charged += size;
            const packed = packFrame(original, new ArrayBuffer(size));
            expect(charged).toBe(packed.buffer.byteLength);
            expect(frameTransferList(packed)).toEqual([packed.buffer]);
            expect(sourceBuffers.has(packed.buffer)).toBe(false);
            // Borrowed native input can be overwritten immediately; only the new frame is transferred.
            for (const array of payloadArrays(original))
              new Uint8Array(array.buffer, array.byteOffset, array.byteLength).fill(0xcc);
            const received = structuredClone(packed, {
              transfer: transferFor([packed.buffer], true),
            });
            expect(packed.buffer.byteLength).toBe(0);
            const actual = bytes(received);
            expect(actual.length).toBe(expected.length);
            for (let k = 0; k < actual.length; k++) {
              const a = actual[k],
                b = expected[k];
              if (!a || !b) throw new Error("missing byte lane");
              let same = a.length === b.length;
              for (let j = 0; same && j < a.length; j++) same = a[j] === b[j];
              if (!same) expect(a).toEqual(b);
            }
            const dictionaries = new StreamDictionaries();
            dictionaries.apply(received);
            const located = dictionaries.locate(7, 1);
            expect(located.entries.data.layout).toBe("varlen");
            for (const array of payloadArrays({
              ...received,
              columns: [],
              dictionaries: received.dictionaries,
            }))
              expect(array.buffer).toBe(received.buffer);
            const compact = dictionaries.parts(7)[0];
            if (compact?.data.layout !== "varlen") throw new Error("dictionary fixture");
            expect(compact.data.bytes.buffer).not.toBe(received.buffer);
            expect(new TextDecoder().decode(compact.data.bytes)).toBe("alphabeta");
            const appended: FramePayload = {
              sequence: 1,
              rowCount: 1,
              columns: [
                {
                  form: "dictionary",
                  length: 1,
                  dictionary: 7,
                  codes: new Int32Array([2]),
                  validity: null,
                },
              ],
              dictionaries: [
                {
                  id: 7,
                  from: 2,
                  values: {
                    form: "flat",
                    length: 1,
                    validity: null,
                    data: {
                      layout: "varlen",
                      bytes: new TextEncoder().encode("gamma"),
                      offsets: new Int32Array([0, 5]),
                    },
                  },
                },
              ],
            };
            const next = packFrame(appended, new ArrayBuffer(packedFrameBytes(appended)));
            dictionaries.apply(next);
            expect(dictionaries.size(7)).toBe(3);
            expect(dictionaries.locate(7, 2).index).toBe(0);
            reach.reach("dictionary append");
            reach.reach("dictionary delta");
            reach.reach("one fresh buffer");
            reach.reach(sparse ? "sparse NULLs" : "all valid");
            charged -= size;
            expect(charged).toBe(0);
          },
        ),
        { seed, numRuns: 8 },
      );
    });
for (const shape of [
  "foreign",
  "overlap",
  "misaligned",
  "out of bounds",
  "wrong type",
  "short lane",
] as const)
  for (const seed of seedsFor("packed-frame-corruption", [0xb2a2, 0xb2a3]))
    it(`both endpoints refuse ${shape} (${String(seed)})`, () => {
      fc.assert(
        fc.property(fc.integer({ min: 2, max: 64 }), (n) => {
          reach.beginHistory();
          const original: FramePayload = {
            sequence: 0,
            rowCount: n,
            columns: [0, 1].map((k) => ({
              form: "flat",
              length: n,
              validity: null,
              data: { layout: "i32", values: Int32Array.from({ length: n }, (_, r) => r + k) },
            })),
            dictionaries: [],
          };
          const packed = packFrame(original, new ArrayBuffer(packedFrameBytes(original))),
            first = packed.columns[0],
            second = packed.columns[1];
          if (first?.form !== "flat" || first.data.layout !== "i32" || second?.form !== "flat")
            throw new Error("fixture");
          let view = first.data.values;
          if (shape === "foreign") view = new Int32Array(n);
          else if (shape === "overlap") {
            (second as { data: VectorData }).data = {
              layout: "i32",
              values: new Int32Array(packed.buffer, 0, n),
            };
          } else if (shape === "wrong type") {
            (first as { data: VectorData }).data = {
              layout: "i32",
              values: new Uint8Array(packed.buffer, 0, n) as unknown as Int32Array,
            };
          } else if (shape === "short lane") view = new Int32Array(packed.buffer, 0, n - 1);
          else
            Object.defineProperty(view, shape === "misaligned" ? "byteOffset" : "byteLength", {
              value: shape === "misaligned" ? 1 : packed.buffer.byteLength + 8,
            });
          if (shape !== "wrong type")
            (first as { data: VectorData }).data = { layout: "i32", values: view };
          expect(() => frameTransferList(packed)).toThrow(FrameBufferError);
          expect(() => new StreamDictionaries().apply(packed)).toThrow(FrameBufferError);
          reach.reach(shape);
        }),
        { seed, numRuns: 16 },
      );
    });
for (const mode of ["complete", "abort", "cancel"] as const)
  for (const seed of seedsFor("packed-frame-cleanup", [0xb2a4, 0xb2a5]))
    it(`packed ownership returns on ${mode} (${String(seed)})`, async () => {
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 33, max: 100 }), async (n) => {
          reach.beginHistory();
          const vector = packedInput("i32", n, 7),
            governor = new Governor(8 * 1048576),
            grant = await governor.admit({ label: "packed", minimumBytes: 65536, spillable: true });
          const channel = new ResultChannel([{ name: "n", type: vector.type, nullable: true }], {
            windowBytes: 65536,
          });
          channel.delivered.catch(() => undefined);
          const sink = new ResultSink([vector.type], grant, channel, 0);
          expect(sink.push({ length: n, columns: [vector], selection: null })).toBe("ready");
          const frame = await channel.next();
          if (!frame) throw new Error("frame missing");
          expect(frame.transfer).toHaveLength(1);
          expect(frame.transfer[0]).toBe(
            (frame.payload as FramePayload & { buffer: ArrayBuffer }).buffer,
          );
          expect(grant.allocated).toBeGreaterThan(0);
          if (mode === "complete") await sink.finish();
          else if (mode === "abort") {
            sink.abort(new Error("abort"));
            channel.end();
            expect(grant.allocated).toBeGreaterThan(0);
          } else {
            sink.discard(new Error("cancel"));
            channel.cancel();
            expect(grant.allocated).toBe(0);
          }
          frame.release();
          frame.release();
          if (mode === "cancel") await expect(channel.delivered).rejects.toThrow();
          else await channel.delivered;
          expect(grant.allocated).toBe(0);
          grant.close();
          expect(governor.usedBytes).toBe(0);
          reach.reach(mode);
        }),
        { seed, numRuns: 16 },
      );
    });

// This is a rejection probe, not an accepted engine expectation: the unchanged engine must
// deliver this value, while the frozen copy-compaction design exposes its doubled loan need.
it("preserves the packed prototype's large-row rejection evidence", async () => {
  const legacy = await import("../../packages/core/src/next/exec/result-sink.js");
  const oldChannel = await import("../../packages/core/src/next/exec/result.js");
  const oldMemory = await import("../../packages/core/src/next/memory/governor.js");
  const type = { kind: "text" } as const,
    size = 550000;
  const source = {
    form: "flat" as const,
    type,
    length: 1,
    validity: null,
    data: {
      layout: "varlen" as const,
      bytes: new Uint8Array(size).fill(65),
      offsets: new Int32Array([0, size]),
    },
  };
  for (const packed of [false, true]) {
    const governor = new (packed ? Governor : oldMemory.Governor)(4 * 1048576);
    const hog = await governor.admit({
      label: "other",
      minimumBytes: 3 * 1048576 - 1024,
      spillable: false,
    });
    const grant = await governor.admit({ label: "result", minimumBytes: 1024, spillable: true });
    const channel = new (packed ? ResultChannel : oldChannel.ResultChannel)(
      [{ name: "v", type, nullable: false }],
      { windowBytes: 64 },
    );
    channel.delivered.catch(() => undefined);
    const sink = new (packed ? ResultSink : legacy.ResultSink)([type], grant, channel, 0, {
      now: () => 0,
    });
    const producer = (async () => {
      if (sink.push({ length: 1, columns: [source], selection: null }) === "blocked")
        await sink.ready();
      await sink.finish();
    })().catch((error: unknown) => {
      channel.fail(error);
      throw error;
    });
    const consumer = (async () => {
      let rows = 0;
      for (;;) {
        const frame = await channel.next();
        if (!frame) break;
        const column = frame.payload.columns[0];
        if (column?.form !== "flat" || column.data.layout !== "varlen")
          throw new Error("large row layout");
        expect(column.data.bytes.length).toBeGreaterThanOrEqual(size);
        for (let i = 0; i < size; i++)
          if (column.data.bytes[i] !== 65) throw new Error("large row value mismatch");
        rows += frame.payload.rowCount;
        frame.release();
      }
      await channel.delivered;
      return rows;
    })();
    try {
      const outcome = await Promise.allSettled([producer, consumer]);
      if (packed) {
        expect(outcome[0].status).toBe("rejected");
        if (outcome[0].status === "rejected")
          expect(String(outcome[0].reason)).toMatch(/memory reserve/u);
      } else
        expect(outcome).toEqual([
          { status: "fulfilled", value: undefined },
          { status: "fulfilled", value: 1 },
        ]);
    } finally {
      sink.discard(new Error("probe cleanup"));
      channel.cancel();
      grant.close();
      hog.close();
      expect(governor.usedBytes).toBe(0);
    }
  }
});
