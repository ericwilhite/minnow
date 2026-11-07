/** FramePayload ownership/bit laws (exec/result.ts, owner/frame.ts), for the bench stand-in. */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../../packages/core/src/testing/reach.js";
import { seedsFor } from "../../../packages/core/src/testing/seeds.js";
import { countingBudget } from "../../../packages/core/src/next/storage/space-test-support.js";
import {
  ResultChannel,
  frameByteLength,
  type FramePayload,
} from "../../../packages/core/src/next/exec/result.js";
import { StreamDictionaries, transferFor } from "../../../packages/core/src/next/owner/frame.js";
import type { FlatVector, VectorData } from "../../../packages/core/src/next/vector/vector.js";
import { MemoryExtentSpace } from "../../../packages/core/src/next/page/memory-extents.js";
import { writeGroup } from "../../../packages/core/src/next/page/test-support.js";
import type { PhysicalType } from "../../../packages/core/src/next/types.js";
import { BenchFrameAssembler } from "./owner-frames.js";
import { scanExport } from "./scan-export.js";

const reach = createReach(
  "export-bench-frame-ownership",
  [
    "f64",
    "f32",
    "flat",
    "constant",
    "nullable",
    "transferred",
    "tiny clone",
    "valid signaling NaN",
    "valid signed zero",
  ] as const,
  { minimum: 4 },
);
afterAll(() => {
  reach.assertReached();
  console.log("EXPORT_FRAME_REACH", JSON.stringify(Object.fromEntries(reach.counts())));
});

const littleEndian = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1;
const offsetReach = createReach(
  "export-bench-frame-offsets",
  ["zero bias", "nonzero bias", "NULL slots", "empty value"] as const,
  { minimum: 4 },
);
afterAll(() => offsetReach.assertReached());

describe("bench owner frames", () => {
  for (const seed of seedsFor("export-bench-frame-offsets", [0xb2f3, 0xb2f4]))
    it(`owns UTF-8 bytes and biased offsets seed ${String(seed)}`, () => {
      fc.assert(
        fc.property(
          fc.array(
            fc
              .array(fc.constantFrom("", "a", "é", "水", "😀"), { maxLength: 10 })
              .map((xs) => xs.join("")),
            { minLength: 1, maxLength: 80 },
          ),
          fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 64 })),
          fc.boolean(),
          (strings, bias, nullable) => {
            offsetReach.beginHistory();
            offsetReach.reach(bias === 0 ? "zero bias" : "nonzero bias");
            const encoded = strings.map((s) => new TextEncoder().encode(s));
            const bytes = new Uint8Array(bias + encoded.reduce((n, s) => n + s.length, 0) + 13);
            const offsets = new Int32Array(strings.length + 4);
            let at = bias;
            offsets[0] = at;
            for (let k = 0; k < encoded.length; k++) {
              const b = encoded[k];
              if (!b) throw new Error("Missing UTF-8");
              bytes.set(b, at);
              at += b.length;
              offsets[k + 1] = at;
            }
            const expectedBytes = bytes.slice(bias, at),
              expectedOffsets = Array.from(
                offsets.subarray(0, strings.length + 1),
                (x) => x - bias,
              );
            const validity = nullable
              ? new Uint32Array(Math.ceil(strings.length / 32)).fill(0xaaaaaaaa)
              : null;
            if (nullable) offsetReach.reach("NULL slots");
            if (strings.includes("")) offsetReach.reach("empty value");
            const source: FlatVector = {
              form: "flat",
              type: { kind: "text" },
              length: strings.length,
              validity,
              data: { layout: "varlen", bytes, offsets },
            };
            const budget = countingBudget(Infinity),
              assembler = new BenchFrameAssembler(budget);
            const offer = assembler.offer({
              length: strings.length,
              selection: null,
              columns: [source],
            });
            bytes.fill(0xff);
            offsets.fill(-1);
            validity?.fill(0);
            const owned = offer.payload.columns[0];
            if (owned?.form !== "flat" || owned.data.layout !== "varlen")
              throw new Error("Wrong owned layout");
            expect(owned.data.bytes).toEqual(expectedBytes);
            expect([...owned.data.offsets]).toEqual(expectedOffsets);
            expect(owned.validity).toEqual(
              nullable ? new Uint32Array(Math.ceil(strings.length / 32)).fill(0xaaaaaaaa) : null,
            );
            expect(frameByteLength(offer.payload)).toBe(budget.used);
            offer.free();
            assembler.close();
            expect(budget.used).toBe(0);
          },
        ),
        { seed, numRuns: 48 },
      );
    });

  for (const seed of seedsFor("export-bench-frame-ownership", [0xb2f1, 0xb2f2]))
    it(`borrowing and raw float bits seed ${String(seed)}`, () => {
      fc.assert(
        fc.property(
          fc.integer({ min: 1, max: 257 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          (n, small, constant, nullable) => {
            reach.beginHistory();
            reach.reach(small ? "f32" : "f64");
            reach.reach(constant ? "constant" : "flat");
            if (nullable) reach.reach("nullable");
            const count = constant ? 1 : n,
              raw = new Uint32Array((count + 4) * (small ? 1 : 2));
            for (let k = 0; k < count + 4; k++) {
              if (small) raw[k] = k % 3 === 0 ? 0x7f801234 : k % 3 === 1 ? 0x80000000 : 0xffc54321;
              else {
                raw[2 * k + (littleEndian ? 0 : 1)] = k % 3 === 1 ? 0 : 0x12345678 + k;
                raw[2 * k + (littleEndian ? 1 : 0)] =
                  k % 3 === 0 ? 0x7ff00000 : k % 3 === 1 ? 0x80000000 : 0xfff80000;
              }
            }
            const data = small
              ? { layout: "f32" as const, values: new Float32Array(raw.buffer, 4, count + 2) }
              : { layout: "f64" as const, values: new Float64Array(raw.buffer, 8, count + 2) };
            const flat: FlatVector = {
              form: "flat",
              type: { kind: small ? "float32" : "float64" },
              length: count,
              validity: nullable ? new Uint32Array(Math.ceil(count / 32)).fill(0xaaaaaaaa) : null,
              data,
            };
            const expected = new Uint8Array(
              data.values.buffer,
              data.values.byteOffset,
              count * data.values.BYTES_PER_ELEMENT,
            ).slice();
            const expectedValidity = flat.validity?.slice() ?? null;
            const budget = countingBudget(Infinity),
              assembler = new BenchFrameAssembler(budget),
              channel = new ResultChannel([]);
            channel.offer(
              assembler.offer({
                length: n,
                selection: null,
                columns: [
                  constant ? { form: "constant", type: flat.type, length: n, value: flat } : flat,
                ],
              }),
            );
            const frame = channel.poll();
            if (!frame) throw new Error("Missing frame");
            const transfer = transferFor(frame.transfer);
            reach.reach(transfer.length ? "transferred" : "tiny clone");
            const received = structuredClone(frame.payload, { transfer });
            raw.fill(0);
            flat.validity?.fill(0); // Recycle producer buffers after their borrow/query lifetime.
            const column = received.columns[0];
            if (!column) throw new Error("No column");
            const copy = column.form === "constant" ? column.value : column;
            if (copy.form !== "flat" || (copy.data.layout !== "f64" && copy.data.layout !== "f32"))
              throw new Error("Wrong wire layout");
            const actual = new Uint8Array(copy.data.values.buffer),
              words = new DataView(expected.buffer),
              stride = small ? 4 : 8;
            for (let k = 0; k < count; k++) {
              const valid =
                expectedValidity === null ||
                ((expectedValidity[k >>> 5] ?? 0) & (1 << (k & 31))) !== 0;
              expect(
                copy.validity === null || ((copy.validity[k >>> 5] ?? 0) & (1 << (k & 31))) !== 0,
              ).toBe(valid);
              if (!valid) continue; // Vector value rules leave bytes under NULL unspecified.
              expect(actual.subarray(k * stride, (k + 1) * stride)).toEqual(
                expected.subarray(k * stride, (k + 1) * stride),
              );
              const high = words.getUint32(
                  k * stride + (small ? 0 : littleEndian ? 4 : 0),
                  littleEndian,
                ),
                low = small
                  ? 0
                  : words.getUint32(k * stride + (littleEndian ? 0 : 4), littleEndian);
              if (
                small
                  ? (high & 0x7fc00000) === 0x7f800000 && (high & 0x003fffff) !== 0
                  : (high & 0x7ff80000) === 0x7ff00000 && ((high & 0x7ffff) !== 0 || low !== 0)
              )
                reach.reach("valid signaling NaN");
              if (high === 0x80000000 && low === 0) reach.reach("valid signed zero");
            }
            expect(frame.byteLength).toBe(frameByteLength(received));
            frame.release();
            channel.end();
            assembler.close();
            expect(budget.used).toBe(0);
          },
        ),
        { seed, numRuns: 96 },
      );
    });

  it("owns every fixed-width wire layout, including decimal limbs and wide coefficients", () => {
    const vectors: Array<{ type: PhysicalType; data: VectorData }> = [
      { type: { kind: "bool" }, data: { layout: "bool", values: new Uint8Array([1, 0]) } },
      {
        type: { kind: "int32", bits: 32 },
        data: { layout: "i32", values: new Int32Array([7, -8]) },
      },
      {
        type: { kind: "int64" },
        data: { layout: "int64", values: { repr: "safe", values: new Float64Array([1, 2]) } },
      },
      {
        type: { kind: "int64" },
        data: {
          layout: "int64",
          values: { repr: "int64", values: new BigInt64Array([2n ** 60n, -(2n ** 60n)]) },
        },
      },
      {
        type: { kind: "uuid" },
        data: { layout: "fixed16", bytes: Uint8Array.from({ length: 32 }, (_, k) => k) },
      },
      {
        type: { kind: "interval" },
        data: {
          layout: "interval",
          months: new Int32Array([1, 2]),
          days: new Int32Array([3, 4]),
          micros: { repr: "int64", values: new BigInt64Array([5n, 6n]) },
        },
      },
      ...(["safe", "int64", "int128", "wide"] as const).map((repr) => ({
        type: { kind: "decimal" as const, precision: 100, scale: 2 },
        data: {
          layout: "decimal" as const,
          scale: 2,
          scales: null,
          specials: new Uint8Array([0, 1]),
          coefficients:
            repr === "int128"
              ? { repr, limbs: Uint32Array.from({ length: 8 }, (_, k) => k + 1) }
              : repr === "wide"
                ? { repr, values: [2n ** 150n, -(2n ** 140n)] }
                : repr === "safe"
                  ? { repr, values: new Float64Array([12, 13]) }
                  : { repr, values: new BigInt64Array([12n, 13n]) },
        },
      })),
    ];
    const clear = (x: unknown): void => {
      if (ArrayBuffer.isView(x)) new Uint8Array(x.buffer).fill(0);
      else if (Array.isArray(x)) x.fill(0n);
      else if (x && typeof x === "object") for (const v of Object.values(x)) clear(v);
    };
    for (const { type, data } of vectors) {
      const expected = structuredClone(data),
        budget = countingBudget(Infinity),
        assembler = new BenchFrameAssembler(budget),
        channel = new ResultChannel([]);
      channel.offer(
        assembler.offer({
          length: 2,
          selection: null,
          columns: [{ form: "flat", length: 2, type, validity: null, data }],
        }),
      );
      const frame = channel.poll();
      if (!frame) throw new Error("Missing frame");
      const received = structuredClone(frame.payload, { transfer: transferFor(frame.transfer) });
      clear(data);
      const column = received.columns[0];
      if (column?.form !== "flat") throw new Error("Not flat");
      expect(column.data).toEqual(expected);
      frame.release();
      channel.end();
      assembler.close();
      expect(budget.used).toBe(0);
    }
  });

  it("sends dictionary deltas once, compacts varlen offsets and balances registry/frame charges", () => {
    const dictionary: FlatVector = {
      form: "flat",
      type: { kind: "text" },
      length: 2,
      validity: null,
      data: {
        layout: "varlen",
        bytes: new TextEncoder().encode("xxxalphaβtail"),
        offsets: new Int32Array([3, 8, 10]),
      },
    };
    const vector = {
      form: "dictionary" as const,
      type: dictionary.type,
      length: 3,
      validity: null,
      codes: new Int32Array([1, 0, 1, 99]),
      dictionary,
      dictionaryId: 1,
    };
    const budget = countingBudget(Infinity),
      assembler = new BenchFrameAssembler(budget),
      channel = new ResultChannel([]),
      client = new StreamDictionaries();
    for (let k = 0; k < 2; k++) {
      channel.offer(assembler.offer({ length: 3, selection: null, columns: [vector] }));
      const frame = channel.poll();
      if (!frame) throw new Error("No frame");
      const payload = structuredClone(frame.payload, { transfer: transferFor(frame.transfer) });
      client.apply(payload);
      expect(payload.dictionaries).toHaveLength(k === 0 ? 1 : 0);
      const column = payload.columns[0];
      if (column?.form !== "dictionary") throw new Error("Not dictionary");
      expect([...column.codes]).toEqual([1, 0, 1]);
      const entry = client.locate(column.dictionary, 1);
      if (entry.entries.data.layout !== "varlen") throw new Error("Not text");
      const d = entry.entries.data;
      expect(
        new TextDecoder().decode(
          d.bytes.subarray(d.offsets[entry.index], d.offsets[entry.index + 1]),
        ),
      ).toBe("β");
      frame.release();
    }
    expect(budget.used).toBe(64);
    channel.end();
    assembler.close();
    expect(budget.used).toBe(0);
  });

  it("flattens capped text dictionaries, including null slots with invalid codes", () => {
    const dictionary: FlatVector = {
      form: "flat",
      type: { kind: "text" },
      length: 2,
      validity: null,
      data: {
        layout: "varlen",
        bytes: new TextEncoder().encode("xxxalphaβtail"),
        offsets: new Int32Array([3, 8, 10]),
      },
    };
    const vector = {
      form: "dictionary" as const,
      type: dictionary.type,
      length: 3,
      validity: new Uint32Array([5]),
      codes: new Int32Array([0, 999, 1]),
      dictionary,
      dictionaryId: 1,
    };
    const budget = countingBudget(Infinity),
      assembler = new BenchFrameAssembler(budget, 0);
    const offer = assembler.offer({ length: 3, selection: null, columns: [vector] });
    expect(offer.payload.dictionaries).toHaveLength(0);
    const column = offer.payload.columns[0];
    if (column?.form !== "flat" || column.data.layout !== "varlen")
      throw new Error("Not flattened");
    expect([...column.data.offsets]).toEqual([0, 5, 5, 7]);
    expect(new TextDecoder().decode(column.data.bytes)).toBe("alphaβ");
    expect([...(column.validity ?? [])]).toEqual([5]);
    expect(assembler.dictionaryBytes).toBe(0);
    expect(assembler.flattenedColumns).toBe(1);
    offer.free();
    assembler.close();
    expect(budget.used).toBe(0);
  });

  it("refuses before allocation and does not publish dictionary ids on refusal", () => {
    const dictionary: FlatVector = {
      form: "flat",
      type: { kind: "int32", bits: 32 },
      length: 1,
      validity: null,
      data: { layout: "i32", values: new Int32Array([7]) },
    };
    const batch = {
      length: 1,
      selection: null,
      columns: [
        {
          form: "dictionary" as const,
          type: dictionary.type,
          length: 1,
          validity: null,
          codes: new Int32Array([0]),
          dictionary,
          dictionaryId: 1,
        },
      ],
    };
    let allow = false,
      used = 0;
    const assembler = new BenchFrameAssembler({
      allocate: (n) => {
        if (!allow) return false;
        used += n;
        return true;
      },
      free: (n) => {
        used -= n;
      },
    });
    expect(() => assembler.offer(batch)).toThrow("refused");
    expect(used).toBe(0);
    allow = true;
    const offer = assembler.offer(batch);
    expect(offer.payload.dictionaries[0]?.id).toBe(1);
    offer.free();
    assembler.close();
    expect(used).toBe(0);
  });

  it("drives the real scan through a real MessagePort, transfers frames and returns every charge", async () => {
    const space = new MemoryExtentSpace(),
      n = 257;
    const fixture = await writeGroup(
      [
        { type: { kind: "int32", bits: 32 }, values: Array.from({ length: n }, (_, k) => k + 1) },
        {
          type: { kind: "text" },
          values: Array.from({ length: n }, (_, k) =>
            new TextEncoder().encode(`v${String(k % 4)}`),
          ),
        },
      ],
      { space },
    );
    const budget = countingBudget(Infinity),
      results = countingBudget(Infinity),
      ports = new MessageChannel();
    let rows = 0;
    let failure: unknown;
    const dictionaries = new StreamDictionaries(),
      decoder = new TextDecoder();
    ports.port2.onmessage = (event: MessageEvent<{ frames: FramePayload[] }>) => {
      for (const frame of event.data.frames) {
        try {
          dictionaries.apply(frame);
          expect(frame.columns).toHaveLength(2);
          const ids = frame.columns[0],
            text = frame.columns[1];
          if (ids?.form !== "flat" || ids.data.layout !== "i32" || !text)
            throw new Error("Wrong scan frame layout");
          for (let k = 0; k < frame.rowCount; k++) {
            expect(ids.data.values[k]).toBe(rows + k + 1);
            const source =
              text.form === "dictionary"
                ? dictionaries.locate(text.dictionary, text.codes[k] ?? 0)
                : {
                    entries: text.form === "constant" ? text.value : text,
                    index: text.form === "constant" ? 0 : k,
                  };
            if (source.entries.data.layout !== "varlen") throw new Error("Wrong text layout");
            const d = source.entries.data;
            expect(
              decoder.decode(
                d.bytes.subarray(d.offsets[source.index], d.offsets[source.index + 1]),
              ),
            ).toBe(`v${String((rows + k) % 4)}`);
          }
        } catch (error) {
          failure = error;
        } finally {
          rows += frame.rowCount;
          ports.port2.postMessage({ t: "release", target: 1, through: frame.sequence });
        }
      }
    };
    ports.port2.start();
    try {
      const result = await scanExport(space, [{ rows: n, chunks: fixture.group.columns }], {
        budget,
        resultBudget: results,
        port: ports.port1,
      });
      if (failure) throw failure;
      expect(result.scanStats.transferredBuffers).toBeGreaterThan(0);
      expect(result.rows).toBe(n);
      expect(rows).toBe(n);
      expect(result.scanStats.scan.groupsRead).toBe(1);
      expect(budget.used).toBe(0);
      expect(results.used).toBe(0);
      expect(space.leased).toBe(0);
    } finally {
      ports.port2.close();
    }
  });
});
