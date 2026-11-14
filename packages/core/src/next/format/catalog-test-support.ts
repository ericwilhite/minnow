/** Physical format tests use opaque payloads. Actual CatalogView/WAL round trips live in write/. */
import type { ExtentWriter, BlobRef } from "../storage/extents.js";
import { encodeCatalogPage, encodeCatalogRoot } from "./catalog-pages.js";
export async function writeTestCatalog(writer: ExtentWriter): Promise<BlobRef> {
  const data = Uint8Array.of(0),
    page = (await writer.write([encodeCatalogPage({ offset: 0, data })])).blobs[0];
  if (!page) throw new Error("Missing physical catalog fixture page");
  const root = (
    await writer.write([
      encodeCatalogRoot({
        depth: 0,
        bytes: 1,
        pages: 1,
        children: [{ ref: page, bytes: 1, pages: 1 }],
      }),
    ])
  ).blobs[0];
  if (!root) throw new Error("Missing physical catalog fixture root");
  return root;
}
