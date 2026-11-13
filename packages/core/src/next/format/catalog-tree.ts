/** Bounded catalog reference walk. The physical grammar is independent of logical records. */
import { pauseIfDue } from "../task.js";
import type { BlobRef } from "../storage/extents.js";
import type { CatalogRoot, CatalogPage } from "./catalog-pages.js";
import { ManifestCorruptionError } from "./bytes.js";
export interface CatalogTreeReader {
  catalogRoot(ref: BlobRef): Promise<{ readonly value: CatalogRoot; release(): void }>;
  catalogPage(ref: BlobRef): Promise<{ readonly value: CatalogPage; release(): void }>;
}
export type CatalogStorageEvent =
  | { readonly kind: "catalog-root"; readonly ref: BlobRef }
  | { readonly kind: "catalog-page"; readonly ref: BlobRef; readonly data: Uint8Array };
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new ManifestCorruptionError(message);
}
/** Borrowed page bytes live only through the yield; closing the iterator releases every hold. */
export async function* walkCatalogTree(
  ref: BlobRef,
  reader: CatalogTreeReader,
  signal?: AbortSignal,
): AsyncGenerator<CatalogStorageEvent> {
  const work = signal ? { signal } : undefined;
  async function* node(
    ref: BlobRef,
    offset: number,
    expected?: { depth: number; bytes: number; pages: number },
  ): AsyncGenerator<CatalogStorageEvent> {
    await pauseIfDue(work);
    const parsed = await reader.catalogRoot(ref);
    try {
      const root = parsed.value;
      if (expected)
        check(
          root.depth === expected.depth &&
            root.bytes === expected.bytes &&
            root.pages === expected.pages,
          "Catalog child root disagrees with parent",
        );
      yield { kind: "catalog-root", ref };
      for (const child of root.children) {
        await pauseIfDue(work);
        if (root.depth > 0)
          yield* node(child.ref, offset, {
            depth: root.depth - 1,
            bytes: child.bytes,
            pages: child.pages,
          });
        else {
          const page = await reader.catalogPage(child.ref);
          try {
            check(
              page.value.offset === offset && page.value.data.length === child.bytes,
              "Catalog data page disagrees with index",
            );
            yield { kind: "catalog-page", ref: child.ref, data: page.value.data };
          } finally {
            page.release();
          }
        }
        offset += child.bytes;
      }
    } finally {
      parsed.release();
    }
  }
  yield* node(ref, 0);
}
