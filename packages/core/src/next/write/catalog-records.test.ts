/** Logical MCAT records preserve exact field identity and reject ambiguous wire forms. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_RECORD_REACH } from "../../testing/reach.js";
import { CATALOG_RECORD_KINDS, CATALOG_NATIVE_GRAMMAR } from "../durable-format.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { StoreCorruptionError } from "../storage/contract.js";
import {
  encodeCatalogRecord,
  decodeCatalogRecord,
  UnsupportedCatalogError,
} from "./catalog-records.js";
function append(source: Uint8Array, id: number, payload: Uint8Array, flags = 0): Uint8Array {
  const r = new ByteReader(source),
    kind = r.varuint(),
    version = r.varuint(),
    body = r.blob();
  r.end();
  const b = new ByteWriter();
  b.raw(body);
  b.varuint(id);
  b.u8(flags);
  b.blob(payload);
  const out = new ByteWriter();
  out.varuint(kind);
  out.varuint(version);
  out.blob(b.finish());
  return out.finish();
}
it.each(seedsFor("next-catalog-records", [0xb750, 0xb751]))(
  "round trips typed schema records (seed %i)",
  async (seed) => {
    const reach = createReach("MCAT logical records", CATALOG_RECORD_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.string({ maxLength: 400 }),
        fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n }),
        fc.boolean(),
        fc.integer({ min: 100, max: Number.MAX_SAFE_INTEGER }),
        async (name, start, cycle, fieldId) => {
          reach.beginHistory();
          const sequence = {
            id: 1,
            name,
            increment: 1n,
            min: -(2n ** 63n),
            max: 2n ** 63n - 1n,
            start,
            cycle,
            ownedBy: { tableId: 3, columnId: 4 },
          };
          const encoded = encodeCatalogRecord("sequence", sequence);
          try {
            expect(encoded.bytes[0]).toBe(CATALOG_RECORD_KINDS.sequence);
            expect(decodeCatalogRecord("sequence", encoded.bytes)).toEqual(sequence);
            reach.reach("exact bigint");
            reach.reach("record round trip");
            let live = 0;
            const owned = encodeCatalogRecord("sequence", sequence, {
              create(n) {
                expect(
                  live + n,
                  "catalog output must be funded by one exact output buffer",
                ).toBeLessThanOrEqual(encoded.bytes.length);
                live += n;
                return new Uint8Array(n);
              },
              release(buffer) {
                live -= buffer.byteLength;
              },
            });
            try {
              expect(owned.bytes).toEqual(encoded.bytes);
            } finally {
              owned.close();
              owned.close();
            }
            expect(live).toBe(0);
            reach.reach("single charged output");
            const optional = append(encoded.bytes, fieldId, new Uint8Array([0xff, 0x00, 0x80]));
            const restored = encodeCatalogRecord(
              "sequence",
              decodeCatalogRecord("sequence", optional),
            );
            try {
              expect(restored.bytes).toEqual(optional);
            } finally {
              restored.close();
            }
            reach.reach("unknown optional retained");
            for (const flags of [2, 4, 8, 16, 32, 64, 128, 254])
              expect(() =>
                decodeCatalogRecord(
                  "sequence",
                  append(encoded.bytes, fieldId, new Uint8Array(), flags),
                ),
              ).toThrow(StoreCorruptionError);
            reach.reach("reserved field flags refused");
            reach.reach(fieldId <= 65535 ? "small field id" : "wide field id");
            if (fieldId === Number.MAX_SAFE_INTEGER) reach.reach("maximum field id");
            expect(() =>
              decodeCatalogRecord("sequence", append(encoded.bytes, fieldId, new Uint8Array(), 1)),
            ).toThrow(UnsupportedCatalogError);
            reach.reach("unknown required refused");
            expect(() =>
              decodeCatalogRecord("sequence", encoded.bytes.subarray(0, encoded.bytes.length - 1)),
            ).toThrow(StoreCorruptionError);
            reach.reach("truncated refused");
            expect(() =>
              decodeCatalogRecord("sequence", append(encoded.bytes, 1, new Uint8Array([1]), 1)),
            ).toThrow(StoreCorruptionError);
            reach.reach("duplicate/out-of-order refused");
          } finally {
            encoded.close();
          }
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          ["\ufeffCase.汉", -9223372036854775808n, false, 100],
          ["quoted.name", 9223372036854775807n, true, Number.MAX_SAFE_INTEGER],
        ],
      },
    );
    reach.assertReached();
  },
);
it("preserves vector raw float words and integer array representations", () => {
  for (const fields of [
    {
      type: { kind: "float64" },
      length: 2,
      data: {
        layout: "f64",
        values: new Float64Array(
          BigUint64Array.of(0x7ff800000000002an, 0x8000000000000000n).buffer,
        ),
      },
    },
    {
      type: { kind: "int64" },
      length: 1,
      data: {
        layout: "int64",
        values: { repr: "int64", values: BigInt64Array.of(-9223372036854775808n) },
      },
    },
    {
      type: { kind: "decimal", precision: null, scale: null },
      length: 1,
      data: { layout: "decimal", coefficients: { repr: "wide", values: [10n ** 100n] }, scale: 33 },
    },
  ]) {
    const vector = { form: "flat", validity: null, ...fields };
    const encoded = encodeCatalogRecord("vector", vector);
    try {
      const decoded = decodeCatalogRecord("vector", encoded.bytes);
      expect(decoded).toEqual(vector);
      const again = encodeCatalogRecord("vector", decoded);
      try {
        expect(again.bytes).toEqual(encoded.bytes);
      } finally {
        again.close();
      }
    } finally {
      encoded.close();
    }
  }
});
it("rejects noncanonical integers and unsupported record versions", () => {
  const valid = encodeCatalogRecord("view", { id: 1, name: "x", bodySql: "SELECT 1" });
  try {
    const extended = Uint8Array.of(CATALOG_RECORD_KINDS.view | 0x80, 0, ...valid.bytes.subarray(1));
    expect(() => decodeCatalogRecord("view", extended)).toThrow(StoreCorruptionError);
  } finally {
    valid.close();
  }
  expect(() =>
    decodeCatalogRecord(
      "sequence",
      Uint8Array.of(CATALOG_RECORD_KINDS.sequence, CATALOG_NATIVE_GRAMMAR.recordVersion + 1, 0),
    ),
  ).toThrow(UnsupportedCatalogError);
  expect(() => encodeCatalogRecord("view", { id: 1, name: "\ud800", bodySql: "SELECT 1" })).toThrow(
    StoreCorruptionError,
  );
});

/** Hand-authored approved grammar, independent of the encoder's sizing/tag helpers. */
it("matches the independent field-id/flags/length golden fixture", () => {
  const golden = Uint8Array.of(14, 1, 14, 1, 1, 1, 7, 2, 1, 2, 1, 97, 3, 1, 2, 1, 81);
  const view = { id: 7, name: "a", bodySql: "Q" };
  expect(decodeCatalogRecord("view", golden)).toEqual(view);
  // Exhaust the flags byte independently: this known required field admits exactly bit 0.
  for (let flags = 0; flags <= 255; flags++) {
    if (flags === 1) continue;
    const invalid = golden.slice();
    invalid[4] = flags;
    expect(() => decodeCatalogRecord("view", invalid)).toThrow(StoreCorruptionError);
  }
  const encoded = encodeCatalogRecord("view", view);
  try {
    expect(encoded.bytes).toEqual(golden);
  } finally {
    encoded.close();
  }
});
