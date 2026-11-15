/** MCHG inspection cannot become a lossy catalog replay/serialization path. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_CAPABILITY_WAL_REACH } from "../../testing/reach.js";
import { Catalog, decodeLogged } from "./catalog.js";
import { decodeCatalogChange, encodeCatalogChange } from "./catalog-stream.js";
import { fields, futureRecord, record, refusalCode } from "./catalog-capability.test-support.js";
const cases = [
  "optional field",
  "required field",
  "unknown flags",
  "duplicate field",
  "out-of-order field",
] as const;
for (const seed of seedsFor("next-catalog-capability-wal", [0xb806, 0xb807]))
  it(`requires MCHG write capability before replay seed ${String(seed)}`, () => {
    const reach = createReach("catalog change write capability", CATALOG_CAPABILITY_WAL_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: cases.length - 1 }),
        fc.boolean(),
        fc.integer({ min: 100, max: Number.MAX_SAFE_INTEGER }),
        fc.uint8Array({ maxLength: 60 }),
        (mode, nested, tag, payload) => {
          reach.beginHistory();
          const which = cases[mode];
          if (!which) throw new Error("missing WAL capability case");
          const before = Catalog.empty(),
            change = before.change(
              { kind: "createView", view: { name: "Case.汉", bodySql: "SELECT 1" } },
              1,
            ).logged[0];
          if (!change) throw new Error("missing MCHG fixture");
          const encoded = encodeCatalogChange(change),
            logical = encoded.subarray(16);
          let altered: Uint8Array;
          if (nested) {
            const root = fields(logical);
            altered = record(
              root.family,
              root.version,
              root.fields.map((f) => ({
                ...f,
                bytes: f.id === 9 ? futureRecord(f.bytes, which, tag, payload, 3) : f.bytes,
              })),
            );
          } else altered = futureRecord(logical, which, tag, payload, 3);
          const bytes = new Uint8Array(16 + altered.length);
          bytes.set(encoded.subarray(0, 16));
          bytes.set(altered, 16);
          const code = refusalCode(which);
          expect(() => decodeLogged(bytes)).toThrow(expect.objectContaining({ code }));
          if (which === "optional field") {
            const inspection = decodeCatalogChange(bytes, "read");
            expect(inspection.op).toBe("createView");
            expect(() => before.apply(inspection, 1)).toThrow(expect.objectContaining({ code }));
            expect(() => encodeCatalogChange(inspection)).toThrow(
              expect.objectContaining({ code }),
            );
            reach.reach("inspection cannot replay");
            reach.reach("inspection cannot serialize");
          } else
            expect(() => decodeCatalogChange(bytes, "read")).toThrow(
              expect.objectContaining({ code }),
            );
          reach.reach(which);
          reach.reach(nested ? "nested view" : "top-level change");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: cases.flatMap((_, i) => [
          [i, false, 100, Uint8Array.of(255)] as [number, boolean, number, Uint8Array],
          [i, true, 2 ** 40, new Uint8Array()] as [number, boolean, number, Uint8Array],
        ]),
      },
    );
    reach.assertReached();
  });
