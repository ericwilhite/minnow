import { ByteWindow } from "./window.js";
import { createReach, CATALOG_STREAM_REACH, CATALOG_FRAGMENT_REACH } from "../../testing/reach.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { Catalog } from "./catalog.js";
import { column, tableInput, INT, TEXT } from "./test-support.js";
import {
  catalogStream,
  encodeCatalogStream,
  decodeCatalogStream,
  encodeCatalogChange,
  decodeCatalogChange,
  decodeCatalogParts,
  writeCatalogChange,
} from "./catalog-stream.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { UnsupportedCatalogError } from "./catalog-records.js";
it.each(seedsFor("next-catalog-stream", [0xb754, 0xb755]))(
  "streams logical catalogs and WAL without JSON (seed %i)",
  async (seed) => {
    const reach = createReach("catalog stream", CATALOG_STREAM_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 12 }),
        fc.string({ maxLength: 80 }),
        async (count, suffix) => {
          reach.beginHistory();
          if (count === 0) reach.reach("empty catalog");
          if (count > 1) reach.reach("multiple tables");
          let catalog = Catalog.empty();
          for (let i = 0; i < count; i++) {
            const next = catalog.change(
              {
                kind: "createTable",
                table: tableInput(`t${String(i)}.${suffix}`, [
                  column("n", INT, false),
                  column("v", TEXT, true),
                ]),
              },
              catalog.version + 1,
            );
            for (const log of next.logged) {
              const encoded = encodeCatalogChange(log);
              expect(Array.from(encoded.subarray(0, 4))).toEqual([0x4d, 0x43, 0x48, 0x47]);
              expect(decodeCatalogChange(encoded)).toEqual(log);
              const projected = new Uint8Array(encoded.length);
              for (let at = 0; at < encoded.length; at += 17) {
                const view = new ByteWindow(
                  projected.subarray(at, at + 17),
                  at,
                  new Uint8Array(17),
                );
                writeCatalogChange(view, log);
                expect(view.length).toBe(encoded.length);
              }
              expect(projected).toEqual(encoded);
              reach.reach("WAL exact round trip");
              let live = 0;
              const allocator = {
                create(n: number) {
                  expect(live).toBe(0);
                  live += n;
                  return new Uint8Array(n);
                },
                release(bytes: Uint8Array) {
                  live -= bytes.length;
                },
              };
              const owned = encodeCatalogChange(log, allocator);
              expect(owned).toEqual(encoded);
              allocator.release(owned);
              expect(live).toBe(0);
            }
            catalog = next.catalog;
          }
          const encoded = encodeCatalogStream(catalog),
            decoded = decodeCatalogStream(encoded);
          expect(Array.from(encoded.subarray(0, 4))).toEqual([0x4d, 0x43, 0x41, 0x54]);
          for (const key of [
            "version",
            "counters",
            "tables",
            "sequences",
            "views",
            "types",
          ] as const)
            expect(decoded[key]).toEqual(catalog[key]);
          expect(encodeCatalogStream(decoded)).toEqual(encoded);
          const bad = encoded.slice();
          bad[6] = 1;
          expect(() => decodeCatalogStream(bad)).toThrow(StoreCorruptionError);
          const future = encoded.slice();
          future[4] = 2;
          expect(() => decodeCatalogStream(future)).toThrow(UnsupportedCatalogError);
          expect(() => decodeCatalogStream(encoded.subarray(0, encoded.length - 1))).toThrow(
            StoreCorruptionError,
          );
          let live = 0,
            peak = 0,
            parts = 0;
          const allocator = {
            create(n: number) {
              expect(live).toBe(0);
              live += n;
              peak = Math.max(peak, live);
              return new Uint8Array(n).fill(0xaa);
            },
            release(bytes: Uint8Array) {
              live -= bytes.length;
            },
          };
          for (const part of catalogStream(catalog, allocator)) {
            parts++;
            expect(live).toBe(part.length);
          }
          reach.reach("one owned record");
          reach.reach("future version refused");
          reach.reach("flags refused");
          reach.reach("truncation refused");
          reach.reach("zero buffers");
          expect(parts).toBe(count + 2);
          expect(live).toBe(0);
          expect(peak).toBeLessThanOrEqual(encoded.length);
        },
      ),
      {
        seed,
        numRuns: 16,
        examples: [
          [0, ""],
          [2, ".汉"],
        ],
      },
    );
    reach.assertReached();
  },
);

it.each(seedsFor("next-catalog-fragments", [0xb75a, 0xb75b]))(
  "decodes arbitrary borrowed fragments and closes on cancellation (seed %i)",
  async (seed) => {
    const reach = createReach("catalog fragments", CATALOG_FRAGMENT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 73 }), async (width) => {
        reach.beginHistory();
        reach.reach(width === 1 ? "single-byte fragments" : "multiple-byte fragments");
        const catalog = Catalog.empty().change(
          { kind: "createView", view: { name: "Case.汉", bodySql: "SELECT 1" } },
          1,
        ).catalog;
        const source = encodeCatalogStream(catalog);
        let live = 0,
          closed = false;
        const allocator = {
          create(n: number) {
            live += n;
            return new Uint8Array(n);
          },
          release(b: Uint8Array) {
            live -= b.length;
          },
        };
        async function* parts() {
          try {
            yield new Uint8Array();
            for (let p = 0; p < source.length; p += width) yield source.subarray(p, p + width);
          } finally {
            closed = true;
          }
        }
        const decoded = await decodeCatalogParts(parts(), allocator);
        expect(decoded.views).toEqual(catalog.views);
        expect(live).toBe(0);
        expect(closed).toBe(true);
        closed = false;
        const stop = new AbortController();
        stop.abort();
        await expect(decodeCatalogParts(parts(), allocator, { signal: stop.signal })).rejects.toBe(
          stop.signal.reason,
        );
        expect(live).toBe(0);
        expect(closed).toBe(false); // pre-abort must not start the borrowed source
        for (const state of [
          "empty fragments",
          "exact descriptors",
          "iterator closed",
          "cancellation",
          "zero buffers",
        ] as const)
          reach.reach(state);
      }),
      { seed, numRuns: 16, examples: [[1], [73]] },
    );
    reach.assertReached();
  },
);
