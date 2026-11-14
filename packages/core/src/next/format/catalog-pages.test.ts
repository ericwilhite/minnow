import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, CATALOG_PAGE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_NATIVE_GRAMMAR } from "../durable-format.js";
import { ManifestCorruptionError } from "./bytes.js";
import { UnsupportedEncodingError } from "./encoding.js";
import {
  decodeCatalogPage,
  decodeCatalogRoot,
  encodeCatalogPage,
  encodeCatalogRoot,
} from "./catalog-pages.js";
it.each(seedsFor("next-catalog-pages", [0xb752, 0xb753]))(
  "round trips charged catalog pages and indexes (seed %i)",
  async (seed) => {
    const reach = createReach("catalog page envelopes", CATALOG_PAGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.uint8Array({ minLength: 1, maxLength: 4096 }), { minLength: 1, maxLength: 64 }),
        fc.nat(),
        fc.integer({ min: 0, max: 2 }),
        async (parts, cut, depth) => {
          reach.beginHistory();
          let held = 0;
          const budget = {
            allocate(n: number) {
              held += n;
              return true;
            },
            free(n: number) {
              held -= n;
            },
          };
          let offset = 0;
          const children = parts.map((data, i) => {
            const encoded = encodeCatalogPage({ offset, data }),
              parsed = decodeCatalogPage(encoded, budget);
            try {
              expect(parsed.value).toEqual({ offset, data });
              const last = encoded.length - 1,
                original = encoded[last] ?? 0;
              encoded[last] = original ^ 1;
              expect(parsed.value.data).toEqual(data);
              encoded[last] = original;
            } finally {
              budget.free(parsed.charge);
            }
            offset += data.length;
            return {
              ref: {
                extent: { offset: (i + 1) * 1048576, length: 1048576 },
                offset: 0,
                length: depth ? Math.max(128, encoded.length) : encoded.length,
                checksum: i,
              },
              bytes: data.length,
              pages: 1,
            };
          });
          const root = { depth, bytes: offset, pages: children.length, children },
            bytes = encodeCatalogRoot(root),
            parsed = decodeCatalogRoot(bytes, budget);
          try {
            expect(parsed.value).toEqual(root);
          } finally {
            budget.free(parsed.charge);
          }
          expect(() => decodeCatalogRoot(bytes.subarray(0, cut % bytes.length), budget)).toThrow(
            ManifestCorruptionError,
          );
          const unknown = bytes.slice();
          unknown[4] = 2;
          expect(() => decodeCatalogRoot(unknown, budget)).toThrow(UnsupportedEncodingError);
          const flags = bytes.slice();
          flags[5] = 1;
          expect(() => decodeCatalogRoot(flags, budget)).toThrow(ManifestCorruptionError);
          expect(() => encodeCatalogRoot({ ...root, bytes: root.bytes + 1 })).toThrow(
            ManifestCorruptionError,
          );
          const first = children[0];
          if (!first) throw new Error("Missing generated catalog child");
          if (children.length > 1)
            expect(() =>
              encodeCatalogRoot({
                ...root,
                children: [first, first, ...children.slice(2)],
              }),
            ).toThrow(ManifestCorruptionError);
          expect(held).toBe(0);
          reach.reach(parts.length === 1 ? "single page" : "multiple pages");
          reach.reach(depth === 0 ? "leaf index" : "internal index");
          for (const state of [
            "exact page bytes",
            "exact references",
            "truncation refused",
            "future version refused",
            "flags refused",
            "zero charged resources",
          ] as const)
            reach.reach(state);
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [[Uint8Array.of(1)], 0, 0],
          [Array.from({ length: 64 }, () => Uint8Array.of(1)), 0, 2],
        ],
      },
    );
    reach.assertReached();
  },
);
it("refuses impossible index depth and oversized data pages", () => {
  expect(() =>
    encodeCatalogRoot({
      depth: 9,
      bytes: 1,
      pages: 1,
      children: [
        {
          ref: { extent: { offset: 0, length: 256 }, offset: 0, length: 128, checksum: 0 },
          bytes: 1,
          pages: 1,
        },
      ],
    }),
  ).toThrow(ManifestCorruptionError);
  expect(() =>
    encodeCatalogPage({ offset: 0, data: new Uint8Array(CATALOG_NATIVE_GRAMMAR.pageBytes + 1) }),
  ).toThrow(ManifestCorruptionError);
});
