/** access/catalog.ts: a catalog and its logged changes retain the same resolved identities. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_NATIVE_REACH } from "../../testing/reach.js";
import { Harness, autocommit, tableInput, column, INT, BIG } from "./test-support.js";
import { Catalog } from "./catalog.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { decodeCatalogRoot } from "../format/catalog-pages.js";
import { CATALOG_NATIVE_GRAMMAR } from "../durable-format.js";
const FREE = { allocate: () => true, free: () => undefined };
for (const seed of seedsFor("next-catalog-native", [0xb75c, 0xb75d]))
  for (const store of ["sim", "memory"] as const)
    it(`retains logical catalog fields through WAL and native publication ${store} seed ${String(seed)}`, async () => {
      const reach = createReach("native catalog replay", CATALOG_NATIVE_REACH);
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 3 }),
          fc.integer({ min: 0, max: 150000 }),
          fc.boolean(),
          async (count, width, cycle) => {
            reach.beginHistory();
            const h = new Harness("strict", store);
            let p = await h.open();
            try {
              for (let i = 0; i < count; i++) {
                await autocommit(p, (s) =>
                  s.changeCatalog({
                    kind: "createTable",
                    table: tableInput(
                      `Case.${String(i)}汉`,
                      [
                        { ...column("a.b", INT, false), defaultSql: "1" },
                        { ...column("seq", BIG, false), identity: { start: 99n, increment: 2n } },
                      ],
                      { checks: [{ name: "positive", conditionSql: '"a.b" > 0', columns: [0] }] },
                    ),
                  }),
                );
                await autocommit(p, (s) =>
                  s.changeCatalog({
                    kind: "createView",
                    view: {
                      name: `View.${String(i)}`,
                      bodySql: `SELECT '${"x".repeat(width)}' AS "Case"`,
                    },
                  }),
                );
              }
              await autocommit(p, (s) =>
                s.changeCatalog({
                  kind: "createType",
                  type: { name: "type.Case", labels: ["A", "汉", "a.b"] },
                }),
              );
              await autocommit(p, (s) =>
                s.changeCatalog({
                  kind: "createSequence",
                  sequence: {
                    name: "seq.Case",
                    min: -(2n ** 63n),
                    max: 2n ** 63n - 1n,
                    start: -9999999999999n,
                    increment: 3n,
                    cycle,
                    ownedBy: null,
                  },
                }),
              );
              const expected = p.catalog.encode();
              expect(new DataView(expected.buffer, expected.byteOffset).getUint32(0, true)).toBe(
                CATALOG_NATIVE_GRAMMAR.streamMagic,
              );
              expect(Catalog.decode(expected).encode()).toEqual(expected);
              p = await h.reopen();
              expect(p.catalog.encode()).toEqual(expected);
              reach.reach("WAL replay exact");
              await p.recycleLog();
              await h.space.audit();
              const manifest = decodeManifestRoot(h.space.root ?? new Uint8Array(), FREE);
              const lease = await h.space.read([manifest.value.catalog], { budget: FREE });
              try {
                const tree = decodeCatalogRoot(lease.bytes[0] ?? new Uint8Array(), FREE);
                expect(tree.value.pages).toBeGreaterThanOrEqual(1);
                reach.reach(tree.value.pages > 1 ? "multiple data pages" : "single data page");
              } finally {
                lease.release();
              }
              p = await h.reopen();
              expect(p.catalog.encode()).toEqual(expected);
              reach.reach("native checkpoint exact");
              await autocommit(p, (s) =>
                s.changeCatalog({
                  kind: "createView",
                  view: { name: "continued", bodySql: "SELECT 1" },
                }),
              );
              const continued = p.catalog.encode();
              p = await h.reopen();
              expect(p.catalog.encode()).toEqual(continued);
              reach.reach("continued writes exact");
              reach.reach("quoted UTF8 identifiers");
              reach.reach("exact sequence config");
            } finally {
              await h.close();
            }
            expect(h.governor.usedBytes).toBe(0);
            reach.reach("zero resources");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [1, 0, false],
            [3, 150000, true],
          ],
        },
      );
      reach.assertReached();
    }, 120000);
