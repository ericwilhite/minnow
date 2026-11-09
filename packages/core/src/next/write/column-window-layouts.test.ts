/** The bounded sink must preserve every stored column layout, including short NaN conversions. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_COLUMN_WINDOW_STATES } from "../../testing/reach.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, Int64Values, VectorData } from "../vector/vector.js";
import { encodeColumn, decodeColumn } from "./columns.js";
import { columnLength } from "./stream-body.js";
import { ByteWindow } from "./window.js";

const reach = createReach("all stored column windows", WRITE_COLUMN_WINDOW_STATES);
const kind = fc.constantFrom(
  "bool",
  "f32",
  "f64",
  "int64",
  "decimal64",
  "decimal128",
  "fixed16",
  "interval",
);
const scenario = fc.record({
  kind,
  words: fc.array(
    fc.oneof(
      fc.integer({ min: 0, max: 0xffffffff }),
      fc.constantFrom(0x7f800001, 0xff800001, 0x7ff00000, 0x7ff80000),
    ),
    { minLength: 1, maxLength: 80 },
  ),
  native: fc.boolean(),
  variableScale: fc.boolean(),
  nullable: fc.boolean(),
  specials: fc.boolean(),
  from: fc.nat(),
  count: fc.constantFrom(0, 1, 4, 8, 9, 31, 80),
});
for (const seed of seedsFor("next-write-column-window-layouts", [0xc011]))
  it(`preserves bytes in every layout (seed ${String(seed)})`, () => {
    fc.assert(
      fc.property(scenario, (s) => {
        reach.beginHistory();
        reach.reach(s.kind);
        const length = s.words.length;
        const word = (k: number) => s.words[k % length] ?? 0;
        const integers: Int64Values = s.native
          ? {
              repr: "int64",
              values: BigInt64Array.from(
                s.words,
                (_, k) => (BigInt(word(k)) << 32n) | BigInt(word(k + 1)),
              ),
            }
          : { repr: "safe", values: Float64Array.from(s.words, (v) => v | 0) };
        let type: PhysicalType;
        let data: VectorData;
        switch (s.kind) {
          case "bool":
            type = { kind: "bool" };
            data = { layout: "bool", values: Uint8Array.from(s.words, (v) => v & 1) };
            break;
          case "f32":
            type = { kind: "float32" };
            data = { layout: "f32", values: new Float32Array(Uint32Array.from(s.words).buffer) };
            break;
          case "f64":
            type = { kind: "float64" };
            data = {
              layout: "f64",
              values: new Float64Array(
                Uint32Array.from({ length: length * 2 }, (_, k) => word(k)).buffer,
              ),
            };
            break;
          case "int64":
            type = { kind: "int64" };
            data = { layout: "int64", values: integers };
            break;
          case "decimal64":
          case "decimal128":
            type = { kind: "decimal", precision: null, scale: s.variableScale ? null : 2 };
            data = {
              layout: "decimal",
              scale: s.variableScale ? null : 2,
              scales: s.variableScale ? Uint16Array.from(s.words, (v) => v & 0x3fff) : null,
              coefficients:
                s.kind === "decimal64"
                  ? integers
                  : {
                      repr: "int128",
                      limbs: Uint32Array.from({ length: length * 4 }, (_, k) => word(k)),
                    },
              specials: s.specials ? Uint8Array.from(s.words, (v) => v & 3) : null,
            };
            break;
          case "fixed16":
            type = { kind: "uuid" };
            data = {
              layout: "fixed16",
              bytes: Uint8Array.from({ length: length * 16 }, (_, k) => word(k) >>> ((k % 4) * 8)),
            };
            break;
          case "interval":
            type = { kind: "interval" };
            data = {
              layout: "interval",
              months: Int32Array.from(s.words),
              days: Int32Array.from(s.words, (v) => -v),
              micros: integers,
            };
            break;
        }
        const vector: FlatVector = {
          form: "flat",
          length,
          type,
          data,
          validity: s.nullable
            ? Uint32Array.from({ length: Math.ceil(length / 32) }, (_, k) => word(k))
            : null,
        };
        const from = s.from % length;
        const count = Math.min(length - from, s.count);
        reach.reach(count <= 8 ? "short range" : "long range");
        if (data.layout === "f32" && count > 0 && count <= 8) {
          const bits = new Uint32Array(data.values.buffer);
          if (
            Array.from({ length: count }, (_, k) => bits[from + k] ?? 0).some(
              (v) =>
                (v & 0x7f800000) === 0x7f800000 && (v & 0x7fffff) !== 0 && (v & 0x400000) === 0,
            )
          )
            reach.reach("signaling f32");
        }
        const expected = encodeColumn(vector, from, count);
        expect(columnLength(vector, from, count)).toBe(expected.length);
        const actual = new Uint8Array(expected.length);
        const scratch = new Uint8Array(16);
        for (let at = 0; at < actual.length; at += 7)
          encodeColumn(
            vector,
            from,
            count,
            new ByteWindow(actual.subarray(at, at + 7), at, scratch),
          );
        expect(
          Buffer.compare(actual, expected),
          `stored column window parity: ${s.kind}, count ${String(count)}`,
        ).toBe(0);
        const decoded = decodeColumn(actual, type);
        expect(decoded.length).toBe(count);
        expect(decoded.data.layout).toBe(data.layout);
      }),
      { seed, numRuns: 400 },
    );
  });
it("reaches every stored layout and short floating range", () => reach.assertReached());
