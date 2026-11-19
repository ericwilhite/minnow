/** A1 wire law: exact source-bound facts, never truncated coefficients or uncharged parsing. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, GROUP_AGGREGATE_CODEC_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { DURABLE_CODECS, DURABLE_FORMATS, durableFields } from "../durable-format.js";
import type { ByteBudget } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { ManifestCorruptionError } from "./bytes.js";
import { ParseBudgetError, parseCharge } from "./manifest-codec.js";
import type { ColumnChunk } from "./manifest.js";
import {
  decodeGroupAggregate,
  tryEncodeGroupAggregate,
  GROUP_AGGREGATE_HEADER_BYTES,
  GROUP_AGGREGATE_ENCODE_BYTES,
  MAX_GROUP_AGGREGATE_BYTES,
  type GroupAggregate,
} from "./group-aggregate.js";

const account = (cap = Number.MAX_SAFE_INTEGER): ByteBudget & { held: number; peak: number } => ({
  held: 0,
  peak: 0,
  allocate(bytes) {
    if (this.held + bytes > cap) return false;
    this.held += bytes;
    this.peak = Math.max(this.peak, this.held);
    return true;
  },
  free(bytes) {
    this.held -= bytes;
    expect(this.held).toBeGreaterThanOrEqual(0);
  },
});
function column(type: PhysicalType, rows: number, nonNull: number, identity: number): ColumnChunk {
  const extent = { offset: (identity + 1) * 4096, length: 8192 };
  return {
    columnId: identity,
    type,
    data: { extent, offset: 0, length: 4096 },
    directory: { extent, offset: 4096, length: 4096, checksum: (identity * 17) >>> 0 },
    statistics: { nullCount: rows - nonNull, min: null, max: null, dictionarySize: null },
    distinctSketch: null,
    aggregateSummary: null,
  };
}
const types: [PhysicalType, PhysicalType, PhysicalType, PhysicalType] = [
  { kind: "int32", bits: 16 },
  { kind: "int32", bits: 32 },
  { kind: "int64" },
  { kind: "decimal", precision: null, scale: null },
];
function encode(value: GroupAggregate, rows: number, source: ColumnChunk): Uint8Array {
  const funding = account();
  const encoded = tryEncodeGroupAggregate(value, rows, source, funding);
  expect(encoded).not.toBeNull();
  if (encoded === null) throw new Error("Missing generated summary");
  expect(funding.held).toBe(encoded.charge);
  funding.free(encoded.charge);
  return encoded.bytes;
}
function refused(bytes: Uint8Array, rows: number, source: ColumnChunk): void {
  const funding = account();
  expect(() => decodeGroupAggregate(bytes, rows, source, funding)).toThrow(ManifestCorruptionError);
  expect(funding.held).toBe(0);
}
const generated = fc.record({
  rows: fc.integer({ min: 1, max: 65536 }),
  id: fc.integer({ min: 0, max: 2 ** 40 }),
  count: fc.nat({ max: 65536 }),
  coefficient: fc.bigInt({ min: -(1n << 180n), max: 1n << 180n }),
  scale: fc.integer({ min: 0, max: 16383 }),
  special: fc.integer({ min: 0, max: 3 }),
});
for (const seed of seedsFor("next-group-aggregate-codec", [0xb2e1, 0xb2e2]))
  it(`keeps exact bounded coefficients and complete source identity seed ${String(seed)}`, () => {
    const reach = createReach("group aggregate codec", GROUP_AGGREGATE_CODEC_REACH);
    fc.assert(
      fc.property(generated, ({ rows, id, count, coefficient, scale, special }) => {
        reach.beginHistory();
        if (id > 0xffffffff) reach.reach("large source identity");
        for (const type of types) {
          const nonNull = Math.min(rows, count);
          const source = column(type, rows, nonNull, id);
          const kind =
            type.kind === "int32" ? (type.bits === 16 ? 1 : 2) : type.kind === "int64" ? 3 : 4;
          for (const hasSum of [false, true]) {
            const finite = kind === 4 && special !== 0 ? nonNull - Math.min(nonNull, 1) : nonNull;
            const bound = kind === 4 ? 1n << 190n : (1n << BigInt([0, 15, 31, 63][kind] ?? 0)) - 1n;
            const total = finite === 0 ? 0n : coefficient % (bound * BigInt(finite) + 1n);
            const specialCount = nonNull - finite;
            const value: GroupAggregate = {
              nonNullCount: nonNull,
              sum: hasSum
                ? {
                    coefficient: total,
                    coefficientScale: total === 0n || kind !== 4 ? 0 : scale,
                    resultScale: finite === 0 || kind !== 4 ? 0 : scale,
                    nanCount: special === 1 ? specialCount : 0,
                    positiveInfinityCount: special === 2 ? specialCount : 0,
                    negativeInfinityCount: special === 3 ? specialCount : 0,
                  }
                : null,
            };
            const bytes = encode(value, rows, source),
              funding = account();
            const parsed = decodeGroupAggregate(bytes, rows, source, funding);
            expect(parsed.value).toEqual(value);
            expect(encode(parsed.value, rows, source)).toEqual(bytes);
            expect(funding.held).toBe(parseCharge(bytes.length));
            funding.free(parsed.charge);
            expect(funding.held).toBe(0);
            const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
            expect(view.getUint32(0, true)).toBe(DURABLE_FORMATS.groupAggregate.magic);
            expect(view.getUint8(24)).toBe(kind);
            expect(view.getUint8(25)).toBe(hasSum ? 3 : 1);
            expect(view.getUint16(32, true)).toBe(hasSum && kind === 4 ? 12 : 0);
            reach.reach((["int16", "int32", "int64", "decimal"] as const)[kind - 1] ?? "decimal");
            reach.reach(hasSum ? "sum" : "count only");
            reach.reach(
              total < 0n
                ? "negative coefficient"
                : total > 0n
                  ? "positive coefficient"
                  : "zero coefficient",
            );
            if (specialCount > 0) reach.reach("specials");
            if (nonNull === 0) reach.reach("all null");
            reach.reach("funded decode");
            reach.reach("released");
            // Independently reconstruct the signed little-endian integer, not using the codec.
            if (hasSum) {
              const length = view.getUint16(30, true);
              let unsigned = 0n;
              for (let i = 0; i < length; i++)
                unsigned |= BigInt(bytes[GROUP_AGGREGATE_HEADER_BYTES + i] ?? 0) << BigInt(8 * i);
              expect(BigInt.asIntN(length * 8, unsigned)).toBe(total);
            }
            for (const changed of [
              ...(source.type.kind === "decimal"
                ? [
                    { ...source, type: { ...source.type, precision: 17 } },
                    { ...source, type: { ...source.type, scale: 2 } },
                  ]
                : []),
              { ...source, columnId: source.columnId + 1 },
              {
                ...source,
                directory: {
                  ...source.directory,
                  extent: {
                    ...source.directory.extent,
                    length: source.directory.extent.length + 1,
                  },
                },
              },
              {
                ...source,
                data: {
                  ...source.data,
                  extent: { ...source.data.extent, length: source.data.extent.length + 1 },
                },
              },
              // SAME flattened addresses/length/checksum, different containing-extent identity.
              {
                ...source,
                directory: {
                  ...source.directory,
                  offset: 0,
                  extent: { offset: source.directory.extent.offset + 4096, length: 4096 },
                },
              },
              {
                ...source,
                data: {
                  ...source.data,
                  offset: 4096,
                  extent: { offset: source.data.extent.offset - 4096, length: 12288 },
                },
              },
              {
                ...source,
                directory: { ...source.directory, checksum: (source.directory.checksum ^ 1) >>> 0 },
              },
              {
                ...source,
                directory: {
                  ...source.directory,
                  offset: source.directory.offset + 1,
                  length: 4095,
                },
              },
              { ...source, data: { ...source.data, length: 4095 } },
              {
                ...source,
                data: {
                  ...source.data,
                  extent: { ...source.data.extent, offset: source.data.extent.offset + 4096 },
                },
              },
            ])
              refused(bytes, rows, changed);
            reach.reach("source identity");
            for (const field of durableFields("groupAggregate")) {
              const changed = bytes.slice(),
                altered = new DataView(changed.buffer);
              const current =
                field.width === 1
                  ? altered.getUint8(field.offset)
                  : field.width === 2
                    ? altered.getUint16(field.offset, true)
                    : altered.getUint32(field.offset, true);
              let invalid =
                field.values === undefined
                  ? (current | (~(field.mask ?? 0) & 0x80000000) | (field.mask === 0 ? 1 : 0)) >>> 0
                  : 255;
              if (field.mask !== undefined && field.mask !== 0)
                invalid = (current | (field.mask + 1)) >>> 0;
              if (field.width === 1) altered.setUint8(field.offset, invalid);
              else if (field.width === 2) altered.setUint16(field.offset, invalid, true);
              else altered.setUint32(field.offset, invalid, true);
              refused(changed, rows, source);
            }
            reach.reach("header refusal");
            refused(bytes.subarray(0, bytes.length - 1), rows, source);
            const extended = new Uint8Array(bytes.length + 1);
            extended.set(bytes);
            refused(extended, rows, source);
            reach.reach("payload refusal");
          }
        }
        const source = column(types[3], rows, rows, id),
          funding = account();
        const tooWide: GroupAggregate = {
          nonNullCount: rows,
          sum: {
            coefficient: 1n << 32768n,
            coefficientScale: 0,
            resultScale: 0,
            nanCount: 0,
            positiveInfinityCount: 0,
            negativeInfinityCount: 0,
          },
        };
        expect(tryEncodeGroupAggregate(tooWide, rows, source, funding)).toBeNull();
        expect(funding.held).toBe(0);
        reach.reach("bounded omission");
      }),
      {
        seed,
        numRuns: 40,
        examples: [
          [{ rows: 1, id: 0, count: 0, coefficient: 0n, scale: 0, special: 0 }],
          [{ rows: 2, id: 2 ** 32 + 7, count: 2, coefficient: 128n, scale: 2, special: 0 }],
          [{ rows: 2, id: 1, count: 2, coefficient: -128n, scale: 2, special: 1 }],
          [{ rows: 2, id: 1, count: 2, coefficient: 128n, scale: 2, special: 2 }],
        ],
      },
    );
    reach.assertReached();
  });

it("funds encode and decode before backing allocations, and refuses nonminimal signed bytes", () => {
  const source = column(types[3], 1, 1, 1);
  const value: GroupAggregate = {
    nonNullCount: 1,
    sum: {
      coefficient: -1n,
      coefficientScale: -2,
      resultScale: 7,
      nanCount: 0,
      positiveInfinityCount: 0,
      negativeInfinityCount: 0,
    },
  };
  const funding = account(GROUP_AGGREGATE_ENCODE_BYTES - 1);
  expect(tryEncodeGroupAggregate(value, 1, source, funding)).toBeNull();
  expect(funding.held).toBe(0);
  const bytes = encode(value, 1, source),
    decodeFunding = account(parseCharge(bytes.length) - 1);
  expect(() => decodeGroupAggregate(bytes, 1, source, decodeFunding)).toThrow(ParseBudgetError);
  expect(decodeFunding.held).toBe(0);
  const changed = new Uint8Array(bytes.length + 1);
  changed.set(bytes.subarray(0, GROUP_AGGREGATE_HEADER_BYTES + 1));
  changed[GROUP_AGGREGATE_HEADER_BYTES + 1] = 255;
  changed.set(bytes.subarray(GROUP_AGGREGATE_HEADER_BYTES + 1), GROUP_AGGREGATE_HEADER_BYTES + 2);
  new DataView(changed.buffer).setUint16(30, 2, true);
  refused(changed, 1, source);
  expect(MAX_GROUP_AGGREGATE_BYTES).toBe(
    GROUP_AGGREGATE_HEADER_BYTES + DURABLE_CODECS.groupAggregate.payloadBytes,
  );
});

it("keeps signed integer endpoints and the full bounded NUMERIC payload exactly", () => {
  for (const [type, width] of [
    [types[0], 16],
    [types[1], 32],
    [types[2], 64],
  ] as const) {
    const rows = 65536,
      source = column(type, rows, rows, width);
    for (const coefficient of [
      -(1n << BigInt(width - 1)) * BigInt(rows),
      ((1n << BigInt(width - 1)) - 1n) * BigInt(rows),
    ]) {
      const value: GroupAggregate = {
        nonNullCount: rows,
        sum: {
          coefficient,
          coefficientScale: 0,
          resultScale: 0,
          nanCount: 0,
          positiveInfinityCount: 0,
          negativeInfinityCount: 0,
        },
      };
      const bytes = encode(value, rows, source),
        budget = account();
      const decoded = decodeGroupAggregate(bytes, rows, source, budget);
      expect(decoded.value).toEqual(value);
      budget.free(decoded.charge);
      expect(budget.held).toBe(0);
    }
  }
  const source = column(types[3], 1, 1, 4),
    bits = (4096 - 12) * 8;
  for (const coefficient of [-(1n << BigInt(bits - 1)), (1n << BigInt(bits - 1)) - 1n]) {
    const value: GroupAggregate = {
      nonNullCount: 1,
      sum: {
        coefficient,
        coefficientScale: -32768,
        resultScale: 16383,
        nanCount: 0,
        positiveInfinityCount: 0,
        negativeInfinityCount: 0,
      },
    };
    const bytes = encode(value, 1, source),
      budget = account();
    expect(bytes.length).toBe(MAX_GROUP_AGGREGATE_BYTES);
    const decoded = decodeGroupAggregate(bytes, 1, source, budget);
    expect(decoded.value).toEqual(value);
    budget.free(decoded.charge);
    expect(budget.held).toBe(0);
  }
});

it("binds fixed numeric result scale and omits unrepresentable optional source typmods", () => {
  const source = column({ kind: "decimal", precision: 10, scale: 2 }, 1, 1, 1);
  const value: GroupAggregate = {
    nonNullCount: 1,
    sum: {
      coefficient: 123n,
      coefficientScale: 2,
      resultScale: 2,
      nanCount: 0,
      positiveInfinityCount: 0,
      negativeInfinityCount: 0,
    },
  };
  const bytes = encode(value, 1, source),
    changed = bytes.slice();
  new DataView(changed.buffer).setUint16(28, 3, true);
  refused(changed, 1, source);
  for (const type of [
    { kind: "decimal" as const, precision: 65536, scale: 2 },
    { kind: "decimal" as const, precision: 10, scale: -32769 },
    { kind: "decimal" as const, precision: 0, scale: null },
  ]) {
    const budget = account();
    expect(tryEncodeGroupAggregate(value, 1, { ...source, type }, budget)).toBeNull();
    expect(budget.held).toBe(0);
  }
});
