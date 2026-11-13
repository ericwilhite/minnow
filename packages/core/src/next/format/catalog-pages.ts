/** Native catalog index/data pages. Logical MCAT records may cross any data-page boundary.
 * Only index nodes carry references; descending depth bounds traversal even for corrupt cycles.
 */
import { CATALOG_NATIVE_GRAMMAR as grammar } from "../durable-format.js";
import type { BlobRef, ByteBudget } from "../storage/extents.js";
import { ByteReader, ByteWriter, ManifestCorruptionError, valid } from "./bytes.js";
import { UnsupportedEncodingError } from "./encoding.js";
import { parseCharge, ParseBudgetError, type Parsed } from "./manifest-codec.js";
export interface CatalogChild {
  readonly ref: BlobRef;
  readonly bytes: number;
  readonly pages: number;
}
export interface CatalogRoot {
  /** Zero: children are MCPG data pages; otherwise children have depth-1. */
  readonly depth: number;
  readonly bytes: number;
  readonly pages: number;
  readonly children: readonly CatalogChild[];
}
export interface CatalogPage {
  readonly offset: number;
  readonly data: Uint8Array;
}
function header(out: ByteWriter, magic: number): void {
  out.u32(magic);
  out.u8(grammar.version);
  out.u8(0);
  out.u8(0);
  out.u8(0);
}
function readHeader(input: ByteReader, magic: number, name: string): void {
  valid(input.u32() === magic, `Not a ${name}`);
  const version = input.u8();
  if (version !== grammar.version)
    throw new UnsupportedEncodingError(name, version, [grammar.version]);
  valid((input.u8() | input.u8() | input.u8()) === 0, `Nonzero ${name} reserved flags`);
}
function whole(value: number, name: string, positive = false): void {
  valid(Number.isSafeInteger(value) && value >= (positive ? 1 : 0), `Invalid catalog ${name}`);
}
function checkRoot(root: CatalogRoot): void {
  whole(root.depth, "depth");
  valid(root.depth <= grammar.maximumDepth, "Catalog index depth exceeds cap");
  valid(
    root.children.length > 0 && root.children.length <= grammar.fanOut,
    "Invalid catalog fanout",
  );
  let bytes = 0,
    pages = 0;
  const seen = new Set<string>();
  for (const child of root.children) {
    whole(child.bytes, "child bytes", true);
    whole(child.pages, "child pages", true);
    valid(child.bytes >= child.pages, "Catalog pages cannot have empty payloads");
    const ref = child.ref;
    whole(ref.extent.offset, "extent offset");
    whole(ref.extent.length, "extent length", true);
    whole(ref.extent.offset + ref.extent.length, "extent end", true);
    whole(ref.offset, "blob offset");
    whole(ref.length, "blob length", true);
    whole(ref.checksum, "checksum");
    valid(ref.checksum <= 0xffffffff, "Catalog checksum out of range");
    valid(
      Number.isSafeInteger(ref.offset + ref.length) && ref.offset + ref.length <= ref.extent.length,
      "Catalog reference outside extent",
    );
    if (root.depth === 0)
      valid(child.pages === 1 && child.bytes <= ref.length, "Invalid catalog leaf summary");
    const id = `${String(ref.extent.offset)}/${String(ref.extent.length)}/${String(ref.offset)}/${String(ref.length)}`;
    valid(!seen.has(id), "Duplicate catalog child reference");
    seen.add(id);
    bytes += child.bytes;
    pages += child.pages;
  }
  whole(bytes, "byte total", true);
  whole(pages, "page total", true);
  valid(bytes === root.bytes && pages === root.pages, "Catalog summary disagrees with children");
}
export function encodeCatalogRoot(root: CatalogRoot): Uint8Array {
  checkRoot(root);
  const out = new ByteWriter();
  header(out, grammar.rootMagic);
  out.u8(root.depth);
  out.uint(root.bytes);
  out.uint(root.pages);
  out.uint(root.children.length);
  for (const { ref, bytes, pages } of root.children) {
    out.uint(ref.extent.offset);
    out.uint(ref.extent.length);
    out.uint(ref.offset);
    out.uint(ref.length);
    out.u32(ref.checksum);
    out.uint(bytes);
    out.uint(pages);
  }
  return out.finish();
}
function parsed<T>(
  bytes: Uint8Array,
  budget: ByteBudget,
  maximum: number,
  decode: (reader: ByteReader) => T,
  chargeBytes = parseCharge(bytes.length),
): Parsed<T> {
  valid(bytes.length <= maximum, "Catalog page exceeds its cap");
  const charge = chargeBytes;
  if (!budget.allocate(charge)) throw new ParseBudgetError(charge);
  try {
    const input = new ByteReader(bytes),
      value = decode(input);
    input.end();
    return { value, charge };
  } catch (error) {
    budget.free(charge);
    if (error instanceof ManifestCorruptionError) throw error;
    throw new ManifestCorruptionError(`Catalog page is malformed: ${String(error)}`);
  }
}
export function decodeCatalogRoot(bytes: Uint8Array, budget: ByteBudget): Parsed<CatalogRoot> {
  return parsed(bytes, budget, 8192, (input) => {
    readHeader(input, grammar.rootMagic, "catalogRoot");
    const depth = input.u8(),
      byteCount = input.uint(),
      pages = input.uint(),
      count = input.count(9);
    valid(count > 0 && count <= grammar.fanOut, "Invalid catalog fanout");
    const children: CatalogChild[] = [];
    for (let i = 0; i < count; i++)
      children.push({
        ref: {
          extent: { offset: input.uint(), length: input.uint() },
          offset: input.uint(),
          length: input.uint(),
          checksum: input.u32(),
        },
        bytes: input.uint(),
        pages: input.uint(),
      });
    const root = { depth, bytes: byteCount, pages, children };
    checkRoot(root);
    return root;
  });
}
export function encodeCatalogPage(page: CatalogPage): Uint8Array {
  whole(page.offset, "stream offset");
  valid(
    page.data.length > 0 && page.data.length <= grammar.pageBytes,
    "Invalid catalog data-page length",
  );
  whole(page.offset + page.data.length, "stream end", true);
  const out = new ByteWriter();
  header(out, grammar.pageMagic);
  out.uint(page.offset);
  out.bytes(page.data);
  return out.finish();
}
export function decodeCatalogPage(bytes: Uint8Array, budget: ByteBudget): Parsed<CatalogPage> {
  return parsed(
    bytes,
    budget,
    grammar.pageBytes + 32,
    (input) => {
      readHeader(input, grammar.pageMagic, "catalogPage");
      const offset = input.uint(),
        data = input.bytes();
      valid(
        data.length > 0 && data.length <= grammar.pageBytes,
        "Invalid catalog data-page length",
      );
      whole(offset + data.length, "stream end", true);
      return { offset, data };
    },
    128 + bytes.length,
  );
}
