/** The page reader shares the native grammar and preserves native encoded bytes exactly. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_COLUMN_CODEC_REACH } from "../../testing/reach.js";
import {
  MemoryAdmissionError,
  bigintBytes,
  stringBytes,
  OBJECT_OVERHEAD_BYTES,
} from "../memory-values.js";
import * as nativeMemory from "../memory/governor.js";
import { decodeLiveColumn } from "../owner/live-column.js";
import { ProtocolError } from "../owner/protocol.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector, VectorData } from "../vector/vector.js";
import { decodeColumnBytes } from "../vector/column-codec.js";
import { ByteReader } from "../write/bytes.js";
import { decodeColumn, encodeColumn } from "../write/columns.js";

const kinds = [
  "bool",
  "i32",
  "f32",
  "f64",
  "i64-safe",
  "i64-wide",
  "decimal-safe",
  "decimal-i64",
  "decimal-i128",
  "decimal-mixed",
  "text",
  "enum",
  "fixed16",
  "interval",
  "date",
  "time",
  "timestamp",
] as const;
function input(
  kind: (typeof kinds)[number],
  n: number,
  salt: number,
  nullable: boolean,
): FlatVector {
  const validity = nullable ? new Uint32Array(Math.ceil(n / 32)).fill(0xffffffff) : null;
  if (validity !== null)
    for (let row = 0; row < n; row++)
      if (row % 7 === 0) validity[row >>> 5] = (validity[row >>> 5] ?? 0) & ~(1 << (row & 31));
  const i32 = () => Int32Array.from({ length: n }, (_, r) => r * salt - 19);
  const safe = () => ({
    repr: "safe" as const,
    values: Float64Array.from({ length: n }, (_, r) => r * salt - 19),
  });
  const wide = () => ({
    repr: "int64" as const,
    values: BigInt64Array.from({ length: n }, (_, r) => (1n << 60n) + BigInt(r * salt)),
  });
  let type: PhysicalType;
  let data: VectorData;
  switch (kind) {
    case "bool":
      type = { kind: "bool" };
      data = { layout: "bool", values: Uint8Array.from({ length: n }, (_, r) => r % 2) };
      break;
    case "i32":
    case "date":
      type = kind === "date" ? { kind: "date" } : { kind: "int32", bits: 32 };
      data = { layout: "i32", values: i32() };
      break;
    case "f32":
      type = { kind: "float32" };
      data = {
        layout: "f32",
        values: Float32Array.from({ length: n }, (_, r) => (r % 5 === 0 ? -0 : (r * salt) / 8)),
      };
      break;
    case "f64":
      type = { kind: "float64" };
      data = {
        layout: "f64",
        values: Float64Array.from({ length: n }, (_, r) => (r % 5 === 0 ? -0 : (r * salt) / 8)),
      };
      break;
    case "i64-safe":
    case "i64-wide":
    case "time":
    case "timestamp":
      type =
        kind === "time"
          ? { kind: "time" }
          : kind === "timestamp"
            ? { kind: "timestamp", zoned: true }
            : { kind: "int64" };
      data = {
        layout: "int64",
        values: kind === "i64-wide" || kind === "timestamp" ? wide() : safe(),
      };
      break;
    case "text":
    case "enum":
      type =
        kind === "text"
          ? { kind: "text" }
          : { kind: "enum", name: "e", labels: ["alpha", "bravo"] };
      data = {
        layout: "varlen",
        offsets: Int32Array.from({ length: n + 1 }, (_, r) => r * 5),
        bytes: new TextEncoder().encode(
          Array.from({ length: n }, (_, r) => (r % 2 === 0 ? "alpha" : "bravo")).join(""),
        ),
      };
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
      data = { layout: "interval", months: i32(), days: i32(), micros: wide() };
      break;
    default:
      type = { kind: "decimal", precision: null, scale: null };
      data = {
        layout: "decimal",
        scale: kind === "decimal-mixed" ? null : 2,
        scales: kind === "decimal-mixed" ? Uint16Array.from({ length: n }, (_, r) => r % 5) : null,
        specials: kind === "decimal-mixed" ? Uint8Array.from({ length: n }, (_, r) => r % 4) : null,
        coefficients:
          kind === "decimal-i128"
            ? {
                repr: "int128",
                limbs: Uint32Array.from({ length: 4 * n }, (_, r) => (r * salt) >>> 0),
              }
            : kind === "decimal-i64"
              ? wide()
              : safe(),
      };
      break;
  }
  return { form: "flat", type, length: n, validity, data };
}
it("validates the column header before constructing a reader", () => {
  let allocations = 0;
  class ProbeReader extends ByteReader {
    constructor(bytes: Uint8Array) {
      allocations++;
      super(bytes);
    }
  }
  expect(() =>
    decodeColumnBytes(
      Uint8Array.of(255),
      { kind: "int32", bits: 32 },
      ProbeReader,
      StoreCorruptionError,
    ),
  ).toThrow(StoreCorruptionError);
  expect(allocations).toBe(0);
});

for (const seed of seedsFor("next-live-column-codec", [8902, 8903])) {
  it(`preserves native column grammar and shared memory identities seed=${String(seed)}`, () => {
    const reach = createReach("page column codec", LIVE_COLUMN_CODEC_REACH);
    expect(nativeMemory.MemoryAdmissionError).toBe(MemoryAdmissionError);
    expect(nativeMemory.bigintBytes).toBe(bigintBytes);
    expect(nativeMemory.stringBytes).toBe(stringBytes);
    expect(nativeMemory.OBJECT_OVERHEAD_BYTES).toBe(OBJECT_OVERHEAD_BYTES);
    expect(new nativeMemory.MemoryAdmissionError("page", 2, 1)).toBeInstanceOf(
      MemoryAdmissionError,
    );
    fc.assert(
      fc.property(
        fc.integer({ min: 8, max: 48 }),
        fc.integer({ min: 1, max: 31 }),
        fc.boolean(),
        fc.nat(),
        (n, salt, nullable, offset) => {
          reach.beginHistory();
          reach.reach("shared definitions");
          for (const kind of kinds) {
            const vector = input(kind, n, salt, nullable),
              from = offset % n,
              count = n - from;
            const bytes = encodeColumn(vector, from, count);
            const backing = new Uint8Array(bytes.length + 7);
            backing.set(bytes, 3);
            const unaligned = backing.subarray(3, 3 + bytes.length);
            const decoded = decodeLiveColumn(unaligned, vector.type);
            expect(decoded).toEqual(decodeColumn(bytes, vector.type));
            expect(encodeColumn(decoded, 0, count)).toEqual(bytes);
            expect(unaligned).toEqual(bytes);
            reach.reach(kind);
            const malformed = [
              bytes.subarray(0, bytes.length - 1),
              Uint8Array.from([...bytes, 0]),
              Uint8Array.of(255),
              Uint8Array.from([bytes[0] ?? 0, ...Array.from({ length: 8 }, () => 0x80)]),
            ];
            for (const bad of malformed) {
              expect(() => decodeLiveColumn(bad, vector.type)).toThrow(ProtocolError);
              expect(() => decodeColumn(bad, vector.type)).toThrow(StoreCorruptionError);
            }
          }
          reach.reach(nullable ? "nullable" : "all valid");
          reach.reach("unaligned input");
          reach.reach("same encoded bytes");
          reach.reach("typed malformed refusal");
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [33, 2, true, 0],
          [8, 3, false, 1],
        ],
      },
    );
    reach.assertReached();
  });
}
