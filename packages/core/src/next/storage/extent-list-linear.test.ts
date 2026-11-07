/** R25's page traversal is linear even when each page names only one extent. */
import { expect, it } from "vitest";
import { encodeListPage, listedExtents, type ExtentList } from "./extent-list.js";
import type { ExtentReader } from "./extents.js";
import { at } from "./space-test-support.js";

it("visits page indices only O(pages) times across read-batch boundaries", async () => {
  const count = 1000;
  const bytes = Array.from({ length: count }, (_, index) =>
    encodeListPage([{ offset: index * 4096, length: 1 }]),
  );
  const pages: ExtentList = bytes.map((data, index) => ({
    first: index * 4096,
    count: 1,
    blob: {
      extent: { offset: 2 ** 30 + index * 4096, length: data.length },
      offset: 0,
      length: data.length,
      checksum: 0,
    },
  }));
  let lookups = 0;
  const tracked = new Proxy(pages, {
    get(target, key) {
      if (typeof key === "string" && /^\d+$/.test(key)) lookups++;
      return Reflect.get(target, key) as unknown;
    },
  });
  const reader: ExtentReader = {
    read: async (refs) => ({
      bytes: refs.map((ref) => at(bytes, (ref.extent.offset - 2 ** 30) / 4096)),
      release: () => undefined,
    }),
    peek: () => null,
  };
  let reached = 0;
  for await (const item of listedExtents(tracked, reader, {
    allocate: () => true,
    free: () => undefined,
  })) {
    void item;
    reached++;
  }
  expect(reached).toBe(count * 2);
  // Order check: 2(P-1); page traversal: P; next-page boundary checks: P.
  expect(lookups).toBeLessThanOrEqual(4 * count + 4);
});
