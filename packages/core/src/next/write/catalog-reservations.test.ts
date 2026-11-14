/** Semantic reservations are never opaque display data, even with optional wire flags. */
import fc from "fast-check";
import {
  CATALOG_RECORD_KINDS,
  CATALOG_RECORD_SCHEMAS,
  CATALOG_RESERVED_FIELDS,
  CATALOG_RESERVED_RECORD_SCHEMAS,
} from "../durable-format.js";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_RESERVATION_REACH } from "../../testing/reach.js";
import { Catalog, decodeLogged, encodeLogged } from "./catalog.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { decodeCatalogRecord, encodeCatalogRecord } from "./catalog-records.js";

function changeRecord(
  source: Uint8Array,
  fieldId: number,
  payload: Uint8Array,
  flags: number,
  nested = false,
): Uint8Array {
  const reader = new ByteReader(source);
  const kind = reader.varuint(),
    version = reader.varuint();
  const fields = new ByteReader(reader.blob());
  reader.end();
  const body = new ByteWriter();
  while (!fields.done) {
    const id = fields.varuint(),
      flag = fields.u8(),
      value = fields.blob();
    body.varuint(id);
    body.u8(flag);
    body.blob(nested && id === 2 ? changeRecord(value, fieldId, payload, flags) : value);
  }
  if (!nested) {
    body.varuint(fieldId);
    body.u8(flags);
    body.blob(payload);
  }
  const out = new ByteWriter();
  out.varuint(kind);
  out.varuint(version);
  out.blob(body.finish());
  return out.finish();
}
function envelope(source: Uint8Array, changed: Uint8Array): Uint8Array {
  const out = new Uint8Array(16 + changed.length);
  out.set(source.subarray(0, 16));
  out.set(changed, 16);
  return out;
}
for (const seed of seedsFor("next-catalog-reservations", [0xb778, 0xb779]))
  it(`refuses reserved semantic fields without decoding their payload seed ${String(seed)}`, () => {
    const reach = createReach("catalog reservations", CATALOG_RESERVATION_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 2 }),
        fc.boolean(),
        fc.uint8Array({ maxLength: 100 }),
        (which, required, payload) => {
          reach.beginHistory();
          const source =
            which === 0
              ? Catalog.empty().encode()
              : encodeLogged({ op: "dropView", viewId: 1, expectedVersion: 0, version: 1 });
          const fieldId = which === 0 ? 8 : which === 1 ? 15 : 16;
          const altered = envelope(
            source,
            changeRecord(source.subarray(16), fieldId, payload, required ? 1 : 0, which === 0),
          );
          expect(() => (which === 0 ? Catalog.decode(altered) : decodeLogged(altered))).toThrow(
            expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }),
          );
          reach.reach(which === 0 ? "counter 8" : which === 1 ? "change 15" : "change 16");
          reach.reach(required ? "required" : "optional");
          // An unreserved optional field remains display-only, byte-preserved by the codec.
          const valid = encodeCatalogRecord("view", { id: 1, name: "v", bodySql: "SELECT 1" });
          try {
            const display = changeRecord(valid.bytes, 100, payload, 0);
            const again = encodeCatalogRecord("view", decodeCatalogRecord("view", display));
            try {
              expect(again.bytes).toEqual(display);
            } finally {
              again.close();
            }
          } finally {
            valid.close();
          }
          reach.reach("display retained");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [0, 1, 2].flatMap((which) =>
          [false, true].map(
            (required) => [which, required, Uint8Array.of(255)] as [number, boolean, Uint8Array],
          ),
        ),
      },
    );
    reach.assertReached();
  });

// Independent allocations freeze the seam and catch a new schema accidentally taking its ids.
it("registers FTS field/family reservations without making them supported schemas", () => {
  expect(CATALOG_RECORD_KINDS.fts).toBe(21);
  expect(CATALOG_RECORD_KINDS.ftsConfig).toBe(26);
  expect(CATALOG_RECORD_KINDS.ftsCoverage).toBe(27);
  expect(CATALOG_RECORD_KINDS.ftsState).toBe(28);
  expect(new Set(Object.values(CATALOG_RECORD_KINDS)).size).toBe(
    Object.keys(CATALOG_RECORD_KINDS).length,
  );
  expect(CATALOG_RESERVED_RECORD_SCHEMAS).toEqual({
    fts: [
      "id:u",
      "tableId:u",
      "columnId:u",
      "tokenizerVersion:u",
      "rendererVersion:u",
      "scorerVersion:u",
      "config:ftsConfig",
    ],
    ftsConfig: [],
    ftsCoverage: [],
    ftsState: [],
  });
  expect(CATALOG_RESERVED_FIELDS).toEqual({
    counters: [{ id: 8, name: "nextFtsId", type: "u" }],
    change: [
      { id: 15, name: "fts", type: "fts" },
      { id: 16, name: "ftsId", type: "u" },
    ],
  });
  for (const [name, fields] of Object.entries(CATALOG_RESERVED_FIELDS)) {
    const active = CATALOG_RECORD_SCHEMAS[name as keyof typeof CATALOG_RESERVED_FIELDS];
    expect(new Set(fields.map((field) => field.id)).size).toBe(fields.length);
    for (const field of fields) expect(field.id).toBeGreaterThan(active.length);
  }
  for (const name of Object.keys(CATALOG_RESERVED_RECORD_SCHEMAS)) {
    expect(Object.hasOwn(CATALOG_RECORD_KINDS, name)).toBe(true);
    expect(Object.hasOwn(CATALOG_RECORD_SCHEMAS, name)).toBe(false);
  }
});
