/** The lowering stand-in and native statement must reject inspection before mutating state. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_INSPECTION_ENTRY_REACH } from "../../testing/reach.js";
import type { CatalogChange } from "../access/catalog.js";
import { MemoryCatalog } from "../lower/memory-catalog.js";
import { decodeCatalogRecord, encodeCatalogRecord } from "../write/catalog-records.js";
import { futureRecord } from "../write/catalog-capability.test-support.js";
import { Harness } from "../write/test-support.js";

for (const seed of seedsFor("next-catalog-inspection-entry", [0xb80c, 0xb80d]))
  it(`rejects inspected mutations at native and stand-in entries seed ${String(seed)}`, async () => {
    const reach = createReach("inspection mutation entries", CATALOG_INSPECTION_ENTRY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 100, max: Number.MAX_SAFE_INTEGER }),
        fc.uint8Array({ maxLength: 40 }),
        async (tag, payload) => {
          reach.beginHistory();
          const encoded = encodeCatalogRecord("view", {
            id: 1,
            name: "v",
            bodySql: "SELECT 1 AS n",
          });
          let inspection: unknown;
          try {
            inspection = decodeCatalogRecord(
              "view",
              futureRecord(encoded.bytes, "optional field", tag, payload, 0),
              "read",
            );
          } finally {
            encoded.close();
          }
          const change = { kind: "createView", view: inspection } as CatalogChange,
            memory = new MemoryCatalog(),
            before = memory.view;
          expect(() => memory.apply(change)).toThrow(
            expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }),
          );
          expect(memory.view).toBe(before);
          reach.reach("stand-in");
          const harness = new Harness();
          await harness.open();
          try {
            const tx = await harness.path.begin(),
              statement = tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              });
            try {
              await expect(
                Promise.resolve().then(() => statement.changeCatalog(change)),
              ).rejects.toMatchObject({ code: "MINNOW_CATALOG_UNSUPPORTED" });
              expect(statement.catalog.views).toHaveLength(0);
              reach.reach("native");
            } finally {
              statement.rollback();
              tx.rollback();
            }
          } finally {
            await harness.close();
          }
          expect(harness.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 8, examples: [[100, Uint8Array.of(1)]] },
    );
    reach.assertReached();
  }, 120000);
