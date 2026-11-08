import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, RESULT_OWNED_LAYOUT_REACH, OUTPUT_CLASS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, VectorData } from "../vector/vector.js";
import { frameTransferList, ResultChannel } from "../exec/result.js";
import { referenceCapacity, paddedCopy } from "../exec/output-class-test-support.js";
import { ResultSink } from "../exec/result-sink.js";

type Kind = (typeof RESULT_OWNED_LAYOUT_REACH)[number];
function input(kind: Kind, n: number, salt: number): FlatVector {
  const validity = new Uint32Array(Math.ceil(n / 32)).fill(0xffffffff);
  for (let row = 0; row < n; row++)
    if ((row + salt) % 7 === 0)
      validity[row >>> 5] = (validity[row >>> 5] ?? 0) & ~(1 << (row & 31));
  const i32 = () => Int32Array.from({ length: n }, (_, r) => r * salt - 19);
  const f64 = () => Float64Array.from({ length: n }, (_, r) => (r * salt) / 8);
  const i64 = () => BigInt64Array.from({ length: n }, (_, r) => (1n << 60n) + BigInt(r * salt));
  let type: PhysicalType = { kind: "float64" };
  let data: VectorData;
  switch (kind) {
    case "bool":
      type = { kind: "bool" };
      data = { layout: "bool", values: Uint8Array.from({ length: n }, (_, r) => r % 2) };
      break;
    case "i32":
      type = { kind: "int32", bits: 32 };
      data = { layout: "i32", values: i32() };
      break;
    case "f32": {
      type = { kind: "float32" };
      const words = Uint32Array.from({ length: n }, (_, r) =>
        r % 2 === 0 ? 0x7fc00001 + r : 0x80000000,
      );
      data = { layout: "f32", values: new Float32Array(words.buffer) };
      break;
    }
    case "f64": {
      const words = Uint32Array.from({ length: 2 * n }, (_, r) =>
        r % 2 === 0 ? (salt + r) >>> 0 : 0x7ff80000 + (r % 7),
      );
      data = { layout: "f64", values: new Float64Array(words.buffer) };
      break;
    }
    case "i64-safe":
      type = { kind: "int64" };
      data = { layout: "int64", values: { repr: "safe", values: f64().map((v) => Math.floor(v)) } };
      break;
    case "i64-wide":
      type = { kind: "int64" };
      data = { layout: "int64", values: { repr: "int64", values: i64() } };
      break;
    case "fixed16":
      type = { kind: "uuid" };
      data = {
        layout: "fixed16",
        bytes: Uint8Array.from({ length: 16 * n }, (_, r) => (r * salt) % 256),
      };
      break;
    case "interval":
      type = { kind: "interval" };
      data = {
        layout: "interval",
        months: i32(),
        days: i32(),
        micros: { repr: "int64", values: i64() },
      };
      break;
    case "text":
      type = { kind: "text" };
      data = {
        layout: "varlen",
        offsets: Int32Array.from({ length: n + 1 }, (_, r) => r * 5),
        bytes: Uint8Array.from({ length: 5 * n }, (_, r) => 65 + ((r + salt) % 26)),
      };
      break;
    default: {
      type = { kind: "decimal", precision: null, scale: null };
      data = {
        layout: "decimal",
        scale: kind === "decimal-mixed" ? null : 2,
        scales: kind === "decimal-mixed" ? Uint16Array.from({ length: n }, (_, r) => r % 5) : null,
        specials:
          kind === "decimal-mixed"
            ? Uint8Array.from({ length: n }, (_, r) => (r % 11 === 0 ? 1 : 0))
            : null,
        coefficients:
          kind === "decimal-i128"
            ? {
                repr: "int128",
                limbs: Uint32Array.from({ length: 4 * n }, (_, r) => (r % 4 === 0 ? r * salt : 0)),
              }
            : kind === "decimal-i64"
              ? { repr: "int64", values: i64() }
              : { repr: "safe", values: f64().map((v) => Math.floor(v)) },
      };
      break;
    }
  }
  return { form: "flat", type, length: n, validity, data };
}
for (const kind of RESULT_OWNED_LAYOUT_REACH)
  for (const seed of seedsFor("next-result-owned-bytes", [0xb2f7, 0xb2f8]))
    it(`dedicated ${kind} bytes survive real transfer seed ${String(seed)}`, async () => {
      const reach = createReach(`result bytes ${kind}`, [kind, ...OUTPUT_CLASS_REACH]);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 33, max: 100 }),
          fc.integer({ min: 1, max: 31 }),
          fc.boolean(),
          async (n, salt, bucketed) => {
            reach.beginHistory();
            reach.reach(bucketed ? "bucketed output" : "exact output");
            const source = input(kind, n, salt);
            const vector = bucketed ? paddedCopy(source) : source;
            const original = frameTransferList({
              rowCount: n,
              columns: [vector],
              dictionaries: [],
            });
            const sourceBytes = frameTransferList({
              rowCount: n,
              columns: [source],
              dictionaries: [],
            }).reduce((sum, b) => sum + b.byteLength, 0);
            if (original.reduce((sum, b) => sum + b.byteLength, 0) > sourceBytes)
              reach.reach("charged zero padding");
            const expected = original.map((b) => new Uint8Array(b).slice());
            const bytes = original.reduce((s, b) => s + b.byteLength, 0);
            const governor = new Governor(8 * 1024 * 1024),
              grant = await governor.admit({
                label: "result",
                minimumBytes: 65536,
                spillable: true,
              });
            expect(grant.allocate(bytes)).toBe(true);
            const channel = new ResultChannel([{ name: kind, type: vector.type, nullable: true }], {
              windowBytes: 65536,
            });
            const sink = new ResultSink([vector.type], grant, channel, 0, { now: () => 0 });
            let returned = false;
            const producer = (async () => {
              if (
                sink.push(
                  { length: n, columns: [vector], selection: null },
                  {
                    detachableBuffers: new Set(original),
                    detachableBudget: grant,
                    ...(bucketed ? { detachableCapacity: referenceCapacity } : {}),
                    release() {
                      if (!returned) {
                        returned = true;
                        grant.free(bytes);
                      }
                    },
                  },
                ) === "blocked"
              )
                await sink.ready();
              await sink.finish();
            })();
            const frame = await channel.next();
            if (!frame) throw new Error("missing frame");
            expect(frame.transfer.length).toBe(original.length);
            expect(frame.transfer.every((buffer, k) => buffer === original[k])).toBe(true);
            expect(returned).toBe(true);
            expect(grant.allocated).toBeGreaterThanOrEqual(bytes);
            const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
            const buffers = frameTransferList(received);
            expect(buffers.map((b) => new Uint8Array(b))).toEqual(expected);
            expect(original.every((b) => b.byteLength === 0)).toBe(true);
            frame.release();
            expect(await channel.next()).toBeNull();
            await producer;
            await channel.delivered;
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(governor.usedBytes).toBe(0);
            reach.reach(kind);
          },
        ),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    });
