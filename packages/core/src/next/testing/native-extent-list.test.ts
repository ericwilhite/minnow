import { afterAll, expect, it } from "vitest";
import { createReach, NATIVE_EXTENT_LIST_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { TaskClock } from "../task.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { NativeExtentList, NATIVE_LIST_WINDOW_BYTES } from "../storage/native-extent-list.js";
import { encodeListPage, LIST_PAGE_EXTENTS, type ExtentListPage } from "../storage/extent-list.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { PageCache } from "../storage/page-cache.js";
import { countingMemory } from "../storage/space-test-support.js";
import type { StoreSession } from "../storage/contract.js";

const BASE = 2 ** 40;
const reach = createReach("native extent-list cursor", NATIVE_EXTENT_LIST_REACH);
async function fixture(count: number, stride = 8192) {
  const backend = new MemoryBackend(),
    lease = await backend.acquire(),
    session = await backend.open(lease);
  const pages: ExtentListPage[] = [],
    images: Uint8Array[] = [];
  for (let at = 0; at < count; at += LIST_PAGE_EXTENTS) {
    const n = Math.min(LIST_PAGE_EXTENTS, count - at);
    const bytes = encodeListPage(
      Array.from({ length: n }, (_, i) => ({
        offset: BASE + (at + i) * stride,
        length: 4097 + (i % 31),
      })),
    );
    const offset = pages.length * 69632;
    await session.writeData(offset, bytes);
    pages.push({
      first: BASE + at * stride,
      count: n,
      blob: {
        extent: { offset, length: bytes.length },
        offset: 0,
        length: bytes.length,
        checksum: xxHash32(bytes),
      },
    });
    images.push(bytes);
  }
  let calls = 0;
  const instrumented = new Proxy(session, {
    get(target, key) {
      if (key === "readDataInto")
        return (...args: Parameters<StoreSession["readDataInto"]>) => {
          expect(args[2]?.serialRecords).toBe(true);
          calls++;
          return target.readDataInto(...args);
        };
      const item: unknown = Reflect.get(target, key);
      return typeof item === "function" ? (item.bind(target) as unknown) : item;
    },
  });
  const memory = countingMemory(Infinity),
    cache = new PageCache(instrumented);
  cache.attach(memory);
  return {
    session,
    pages,
    images,
    cache,
    memory,
    get calls() {
      return calls;
    },
    async close() {
      await cache.settled();
      cache.close();
      await session.close();
      await lease.release();
      expect(memory.used).toBe(0);
    },
  };
}
for (const seed of seedsFor("native-extent-list", [1571, 1572, 1573, 1574]))
  it(`decodes high coordinates through bounded slab views (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      count = 8193 + Math.floor(random() * 12000),
      stride = random() < 0.5 ? 8192 : 12288;
    const source = await fixture(count, stride);
    try {
      await source.cache.withReadSlab(async (slab) => {
        const cursor = new NativeExtentList(
          source.pages,
          source.cache,
          slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
          BASE + count * stride,
        );
        let at = 0,
          asynchronous = 0;
        try {
          for (;;) {
            let item = cursor.next();
            if (typeof item !== "boolean") {
              asynchronous++;
              item = await item;
            }
            if (!item) break;
            expect(cursor.offset).toBe(BASE + at * stride);
            expect(cursor.length).toBe(4097 + ((at % LIST_PAGE_EXTENTS) % 31));
            at++;
          }
          expect(at).toBe(count);
          expect(asynchronous).toBeLessThan(count / 64);
          expect(source.calls).toBe(Math.ceil(source.pages.length / 2));
          reach.reach("full safe integer offsets");
          reach.reach("coalesced page reads");
          reach.reach("synchronous scalar pairs");
        } finally {
          cursor.close();
        }
      });
    } finally {
      await source.close();
    }
  });

it("services a real timer and cancels during an immediately resolved list read", async () => {
  const source = await fixture(100_000),
    controller = new AbortController();
  let time = 0,
    turns = 0,
    read = 0;
  const clock = new TaskClock(() => (time += 5)),
    reason = new Error("cancel native list");
  try {
    await expect(
      source.cache.withReadSlab(async (slab) => {
        const cursor = new NativeExtentList(
          source.pages,
          source.cache,
          slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
          BASE + 100_000 * 8192,
          {
            clock,
            signal: controller.signal,
            yieldTask: async () => {
              await new Promise<void>((resolve) => setImmediate(resolve));
              if (++turns === 3) controller.abort(reason);
            },
          },
        );
        try {
          for (;;) {
            const next = cursor.next();
            if (!(typeof next === "boolean" ? next : await next)) break;
            read++;
          }
        } finally {
          cursor.close();
        }
      }),
    ).rejects.toBe(reason);
    expect(turns).toBe(3);
    expect(read).toBeGreaterThan(0);
    expect(read).toBeLessThan(512);
  } finally {
    await source.close();
  }
});

for (const kind of ["checksum", "header", "first", "overlap", "past end"] as const)
  it(`refuses ${kind} before consuming an invalid extent`, async () => {
    const source = await fixture(4097);
    try {
      let pages = source.pages,
        end = BASE + 4097 * 8192;
      const page = pages[0],
        image = source.images[0];
      if (page === undefined || image === undefined) throw new Error("missing fixture");
      if (kind === "checksum") image[image.length - 1] = (image[image.length - 1] ?? 0) ^ 1;
      if (kind === "header") new DataView(image.buffer).setUint32(12, 1, true);
      if (kind === "overlap") new DataView(image.buffer).setFloat64(32, BASE + 4096, true);
      if (kind === "past end") end = BASE + 4096;
      if (kind === "first") pages = [{ ...page, first: page.first + 4096 }, ...pages.slice(1)];
      else {
        await source.session.writeData(0, image);
        if (kind !== "checksum")
          pages = [
            { ...page, blob: { ...page.blob, checksum: xxHash32(image) } },
            ...pages.slice(1),
          ];
      }
      await expect(
        source.cache.withReadSlab(async (slab) => {
          const cursor = new NativeExtentList(
            pages,
            source.cache,
            slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
            end,
          );
          try {
            while (await cursor.next()) {
              /* exhaust to reach the bad coordinate */
            }
          } finally {
            cursor.close();
          }
        }),
      ).rejects.toThrow(/Extent list:/);
    } finally {
      await source.close();
    }
  });
afterAll(() => reach.assertReached());
