/** Paged native catalog IO. A failure aborts the provisional publication writer; retry outside
 * that writer/lock. No physical tree or temporary IO buffer becomes catalog semantics.
 */
import { walkCatalogTree, type CatalogStorageEvent } from "../format/catalog-tree.js";
import { pauseIfDue } from "../task.js";
import { CATALOG_NATIVE_GRAMMAR as grammar } from "../durable-format.js";
import type { BlobRef, ByteBudget, ExtentReader, ExtentWriter } from "../storage/extents.js";
import type { BufferAllocator } from "./commit-buffers.js";
import {
  encodeCatalogRoot,
  decodeCatalogRoot,
  encodeCatalogPage,
  decodeCatalogPage,
  type CatalogChild,
} from "../format/catalog-pages.js";
import { ManifestCorruptionError } from "../format/bytes.js";
import { ParseBudgetError } from "../format/manifest-codec.js";
import {
  catalogStream,
  catalogStreamByteLength,
  type CatalogStreamData,
} from "./catalog-stream.js";
export class CatalogBuffers implements BufferAllocator {
  readonly #live = new Set<Uint8Array>();
  constructor(readonly budget: ByteBudget) {}
  create(n: number): Uint8Array {
    if (!this.budget.allocate(n)) throw new ParseBudgetError(n);
    try {
      const buffer = new Uint8Array(n);
      this.#live.add(buffer);
      return buffer;
    } catch (error) {
      this.budget.free(n);
      throw error;
    }
  }
  release(buffer: Uint8Array): void {
    if (!this.#live.delete(buffer)) throw new Error("Catalog buffer released without ownership");
    this.budget.free(buffer.length);
  }
  close(): void {
    for (const buffer of this.#live) this.budget.free(buffer.length);
    this.#live.clear();
  }
}
function check(ok: boolean, message: string): asserts ok {
  if (!ok) throw new ManifestCorruptionError(message);
}
function needRef(ref: BlobRef | undefined): BlobRef {
  if (!ref) throw new Error("Catalog writer omitted its blob");
  return ref;
}
export async function writeCatalogStorage(
  writer: ExtentWriter,
  data: CatalogStreamData,
  budget: ByteBudget,
  signal?: AbortSignal,
): Promise<BlobRef> {
  const work = signal ? { signal } : undefined;
  const total = catalogStreamByteLength(data),
    pageCount = Math.ceil(total / grammar.pageBytes);
  const pageSize = Math.min(total, grammar.pageBytes),
    levels = Math.max(1, Math.ceil(Math.log(pageCount) / Math.log(grammar.fanOut)));
  const workspace = 3 * pageSize + Math.min(pageCount, grammar.fanOut) * 256 * (levels + 1) + 1024;
  const buffers = new CatalogBuffers(budget);
  let scratch = false;
  const pending: CatalogChild[][] = [];
  let offset = 0,
    used = 0;
  try {
    signal?.throwIfAborted();
    if (!budget.allocate(workspace)) throw new ParseBudgetError(workspace);
    scratch = true;
    const page = buffers.create(pageSize);
    const node = async (
      depth: number,
      children: readonly CatalogChild[],
    ): Promise<CatalogChild> => {
      check(depth <= grammar.maximumDepth, "Catalog tree exceeds depth cap");
      signal?.throwIfAborted();
      const bytes = children.reduce((n, c) => n + c.bytes, 0),
        pages = children.reduce((n, c) => n + c.pages, 0);
      const encoded = encodeCatalogRoot({ depth, bytes, pages, children });
      const ref = needRef((await writer.write([encoded])).blobs[0]);
      await pauseIfDue(work);
      return { ref, bytes, pages };
    };
    const push = async (level: number, child: CatalogChild): Promise<void> => {
      check(level <= grammar.maximumDepth + 1, "Catalog tree exceeds depth cap");
      const list = pending[level] ?? (pending[level] = []);
      list.push(child);
      if (list.length === grammar.fanOut) {
        pending[level] = [];
        await push(level + 1, await node(level, list));
      }
    };
    const flush = async () => {
      signal?.throwIfAborted();
      const encoded = encodeCatalogPage({ offset, data: page.subarray(0, used) });
      const ref = needRef((await writer.write([encoded])).blobs[0]);
      await pauseIfDue(work);
      await push(0, { ref, bytes: used, pages: 1 });
      offset += used;
      used = 0;
    };
    for (const part of catalogStream(data, buffers)) {
      signal?.throwIfAborted();
      for (let from = 0; from < part.length;) {
        const n = Math.min(page.length - used, part.length - from);
        page.set(part.subarray(from, from + n), used);
        used += n;
        from += n;
        if (used === page.length) await flush();
      }
    }
    if (used) await flush();
    check(offset === total, "Catalog stream length changed during encoding");
    for (;;) {
      let first = -1,
        last = -1;
      for (let i = 0; i < pending.length; i++)
        if (pending[i]?.length) {
          if (first < 0) first = i;
          last = i;
        }
      check(first >= 0, "Catalog has no root");
      const children = pending[first];
      check(children !== undefined, "Missing catalog level");
      if (first === last) {
        if (first > 0 && children.length === 1) return needRef(children[0]?.ref);
        return (await node(first, children)).ref;
      }
      pending[first] = [];
      await push(first + 1, await node(first, children));
    }
  } catch (error) {
    await Promise.allSettled([writer.abort()]);
    throw error;
  } finally {
    buffers.close();
    if (scratch) budget.free(workspace);
  }
}
/** Caller must drain or close the iterator; borrowed page bytes live only through its yield. */
export async function* catalogStorageEvents(
  ref: BlobRef,
  reader: ExtentReader,
  budget: ByteBudget,
  signal?: AbortSignal,
): AsyncGenerator<CatalogStorageEvent> {
  const read = async <T>(
    ref: BlobRef,
    decode: (bytes: Uint8Array, budget: ByteBudget) => { value: T; charge: number },
  ) => {
    const lease = await reader.read([ref], { budget });
    let parsed;
    try {
      parsed = decode(lease.bytes[0] ?? new Uint8Array(), budget);
    } finally {
      lease.release();
    }
    let released = false;
    return {
      value: parsed.value,
      release() {
        if (!released) {
          released = true;
          budget.free(parsed.charge);
        }
      },
    };
  };
  yield* walkCatalogTree(
    ref,
    {
      catalogRoot: (ref) => read(ref, decodeCatalogRoot),
      catalogPage: (ref) => read(ref, decodeCatalogPage),
    },
    signal,
  );
}
export async function* catalogStorageParts(
  ref: BlobRef,
  reader: ExtentReader,
  budget: ByteBudget,
  signal?: AbortSignal,
): AsyncGenerator<Uint8Array> {
  for await (const event of catalogStorageEvents(ref, reader, budget, signal))
    if (event.kind === "catalog-page") yield event.data;
}
