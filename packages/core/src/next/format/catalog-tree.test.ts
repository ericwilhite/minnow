/** Parent summaries and physical fragment positions must agree before reclaim can trust them. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, CATALOG_TREE_REACH } from "../../testing/reach.js";
import type { BlobRef } from "../storage/extents.js";
import {
  encodeCatalogRoot,
  encodeCatalogPage,
  decodeCatalogRoot,
  decodeCatalogPage,
} from "./catalog-pages.js";
import { walkCatalogTree, type CatalogTreeReader } from "./catalog-tree.js";
for (const seed of seedsFor("next-catalog-tree", [0xb762, 0xb763]))
  it(`cross-checks catalog roots and fragments with cleanup seed ${String(seed)}`, async () => {
    const reach = createReach("catalog reference graph", CATALOG_TREE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.uint8Array({ minLength: 1, maxLength: 128 }), { minLength: 1, maxLength: 8 }),
        async (parts) => {
          reach.beginHistory();
          const make = (extra: number, wrongOffset: boolean) => {
            const blobs = new Map<number, Uint8Array>();
            let held = 0,
              next = 4096,
              offset = 0;
            const budget = {
              allocate(n: number) {
                held += n;
                return true;
              },
              free(n: number) {
                held -= n;
              },
            };
            const put = (bytes: Uint8Array): BlobRef => {
              const ref = {
                extent: { offset: next, length: bytes.length },
                offset: 0,
                length: bytes.length,
                checksum: 0,
              };
              next += 4096;
              blobs.set(ref.extent.offset, bytes);
              return ref;
            };
            const children = parts.map((data, i) => {
              const ref = put(
                encodeCatalogPage({ offset: offset + (wrongOffset && i === 0 ? 1 : 0), data }),
              );
              offset += data.length;
              return { ref, bytes: data.length, pages: 1 };
            });
            const leaf = put(
              encodeCatalogRoot({ depth: 0, bytes: offset, pages: parts.length, children }),
            );
            const root = put(
              encodeCatalogRoot({
                depth: 1,
                bytes: offset + extra,
                pages: parts.length,
                children: [{ ref: leaf, bytes: offset + extra, pages: parts.length }],
              }),
            );
            const read = <T>(
              ref: BlobRef,
              decode: (
                bytes: Uint8Array,
                allocation: typeof budget,
              ) => { value: T; charge: number },
            ) => {
              const bytes = blobs.get(ref.extent.offset);
              if (!bytes) throw new Error("Missing generated catalog blob");
              const parsed = decode(bytes, budget);
              let closed = false;
              return {
                value: parsed.value,
                release() {
                  if (!closed) {
                    closed = true;
                    budget.free(parsed.charge);
                  }
                },
              };
            };
            const reader: CatalogTreeReader = {
              catalogRoot: async (ref) => read(ref, decodeCatalogRoot),
              catalogPage: async (ref) => read(ref, decodeCatalogPage),
            };
            return { root, reader, held: () => held };
          };
          const f = make(0, false),
            seen: Uint8Array[] = [];
          for await (const item of walkCatalogTree(f.root, f.reader))
            if (item.kind === "catalog-page") seen.push(item.data.slice());
          expect(seen).toEqual(parts);
          expect(f.held()).toBe(0);
          reach.reach("exact fragments");
          const bad = make(1, false);
          await expect(async () => {
            for await (const item of walkCatalogTree(bad.root, bad.reader)) void item;
          }).rejects.toThrow(/child root disagrees/u);
          expect(bad.held()).toBe(0);
          reach.reach("parent summary refused");
          const moved = make(0, true);
          await expect(async () => {
            for await (const item of walkCatalogTree(moved.root, moved.reader)) void item;
          }).rejects.toThrow(/data page disagrees/u);
          expect(moved.held()).toBe(0);
          reach.reach("page offset refused");
          for await (const item of walkCatalogTree(f.root, f.reader)) {
            void item;
            break;
          }
          expect(f.held()).toBe(0);
          reach.reach("early return cleanup");
          const stop = new AbortController(),
            reason = new Error("catalog walk cancelled");
          await expect(async () => {
            for await (const item of walkCatalogTree(f.root, f.reader, stop.signal)) {
              void item;
              stop.abort(reason);
            }
          }).rejects.toBe(reason);
          expect(f.held()).toBe(0);
          reach.reach("cancellation cleanup");
          reach.reach("zero resources");
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  });
