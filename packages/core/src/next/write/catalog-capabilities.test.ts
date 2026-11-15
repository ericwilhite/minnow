/** Lead's1.0 extension decision: optional reads, complete writes, and no opaque retention. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  CATALOG_CAPABILITIES_REACH,
  CATALOG_SEMANTIC_CAPABILITIES_REACH,
} from "../../testing/reach.js";
import { ByteWriter } from "./bytes.js";
import { Catalog } from "./catalog.js";
import { encodeCatalogStream, decodeCatalogParts, decodeCatalogStream } from "./catalog-stream.js";
import {
  decodeCatalogRecord,
  writeCatalogRecord,
  encodeCatalogRecord,
  type CatalogRecordKind,
} from "./catalog-records.js";
import {
  capabilityCases,
  capabilityCatalog,
  fields,
  futureCatalog,
  record,
  refusalCode,
} from "./catalog-capability.test-support.js";
const samples: ReadonlyArray<readonly [CatalogRecordKind, unknown]> = [
  ["physical", { kind: "int32", bits: 32 }],
  ["physical", { kind: "decimal", precision: 20, scale: 3 }],
  ["physical", { kind: "array", element: { kind: "json", binary: true } }],
  ["physical", { kind: "text", maxLength: 10, fixed: true }],
  ["physical", { kind: "timestamp", zoned: true }],
  ["declared", { type: "number", integer: true, declaredTypeName: "INTEGER" }],
  ["declared", { type: "string", sqlDomain: { kind: "numeric", precision: 20, scale: 3 } }],
  ["declared", { type: "string", declaredLength: { length: 10, fixed: true }, enumValues: ["a"] }],
  ["vectorData", { layout: "i32", values: Int32Array.of(0) }],
  ["integerValues", { repr: "safe", values: Float64Array.of(0) }],
  [
    "index",
    {
      id: 1,
      name: "i",
      tableId: 1,
      columnIds: [1],
      unique: false,
      kind: "ordered",
      role: "secondary",
      invalid: true,
      keyLimit: "scalar",
    },
  ],
  ["change", { op: "dropFts", ftsId: 1, expectedVersion: 0, version: 1 }],
];
for (const seed of seedsFor("next-catalog-capabilities", [0xb800, 0xb801]))
  it(`refuses unsupported or malformed capabilities in complete and fragmented MCAT seed ${String(seed)}`, async () => {
    const reach = createReach("catalog capability admission", CATALOG_CAPABILITIES_REACH),
      catalog = capabilityCatalog();
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: capabilityCases.length - 1 }),
        fc.integer({ min: 0, max: 2 }),
        fc.integer({ min: 100, max: Number.MAX_SAFE_INTEGER }),
        fc.uint8Array({ maxLength: 50 }),
        fc.integer({ min: 2, max: 255 }),
        fc.integer({ min: 1, max: 31 }),
        async (mode, level, tag, payload, flag, width) => {
          reach.beginHistory();
          const which = capabilityCases[mode];
          if (!which) throw new Error("missing capability case");
          const bytes = futureCatalog(catalog, which, level, tag, payload, flag),
            code = refusalCode(which);
          if (which === "optional field") {
            const inspection = decodeCatalogStream(bytes, "read");
            expect(inspection.tables[0]?.name).toBe("source");
            expect(() => new Catalog(inspection)).toThrow(expect.objectContaining({ code }));
            expect(() => Catalog.fromStored(inspection)).toThrow(expect.objectContaining({ code }));
            const output = new ByteWriter();
            output.u8(0x7f);
            expect(() => writeCatalogRecord(output, "catalog", inspection.wireMetadata)).toThrow(
              expect.objectContaining({ code }),
            );
            expect(output.finish()).toEqual(Uint8Array.of(0x7f));
            expect(() => encodeCatalogStream(inspection)).toThrow(
              expect.objectContaining({ code }),
            );
            reach.reach("inspection cannot rewrite");
            reach.reach("optional reader tolerated");
          } else
            expect(() => decodeCatalogStream(bytes, "read")).toThrow(
              expect.objectContaining({ code }),
            );
          expect(() => decodeCatalogStream(bytes)).toThrow(expect.objectContaining({ code }));
          let held = 0,
            closed = false;
          const allocator = {
            create(n: number) {
              held += n;
              return new Uint8Array(n);
            },
            release(b: Uint8Array) {
              held -= b.length;
            },
          };
          async function* parts() {
            try {
              for (let at = 0; at < bytes.length; at += width) yield bytes.subarray(at, at + width);
            } finally {
              closed = true;
            }
          }
          if (which === "optional field") {
            expect(
              (await decodeCatalogParts(parts(), allocator, undefined, "read")).tables[0]?.name,
            ).toBe("source");
            closed = false;
          }
          await expect(decodeCatalogParts(parts(), allocator)).rejects.toMatchObject({ code });
          expect(held).toBe(0);
          expect(closed).toBe(true);
          reach.reach(which);
          reach.reach(
            level === 0 ? "metadata" : level === 1 ? "nested counters" : "nested physical",
          );
          reach.reach("fragment cleanup");
        },
      ),
      {
        seed,
        numRuns: 36,
        examples: [
          ...capabilityCases.map(
            (_, i) =>
              [i, i % 3, 100, Uint8Array.of(255), 128, 1] as [
                number,
                number,
                number,
                Uint8Array,
                number,
                number,
              ],
          ),
          [2, 0, 100, Uint8Array.of(1), 3, 1],
        ],
      },
    );
    reach.assertReached();
  });
for (const seed of seedsFor("next-catalog-semantic-capabilities", [0xb802, 0xb803]))
  it(`requires every semantic field whenever present seed ${String(seed)}`, () => {
    const reach = createReach("semantic capability flags", CATALOG_SEMANTIC_CAPABILITIES_REACH);
    fc.assert(
      fc.property(fc.integer({ min: 0, max: samples.length - 1 }), (i) => {
        reach.beginHistory();
        const sample = samples[i];
        if (!sample) throw new Error("missing semantic sample");
        const [kind, value] = sample,
          encoded = encodeCatalogRecord(kind, value);
        try {
          const r = fields(encoded.bytes);
          for (const f of r.fields) {
            expect(f.flags, `${kind}.${String(f.id)} must require understanding`).toBe(1);
            expect(() =>
              decodeCatalogRecord(
                kind,
                record(
                  r.family,
                  r.version,
                  r.fields.map((item) => ({ ...item, flags: item === f ? 0 : item.flags })),
                ),
              ),
            ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
          }
        } finally {
          encoded.close();
        }
        const label = CATALOG_SEMANTIC_CAPABILITIES_REACH[i];
        if (!label) throw new Error("missing semantic reach");
        reach.reach(label);
      }),
      { seed, numRuns: 24, examples: samples.map((_, i) => [i] as [number]) },
    );
    reach.assertReached();
  });
