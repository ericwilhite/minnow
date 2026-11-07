import { expect, it } from "vitest";
import { buildListedReopenSpace, openListedReopen } from "./next-storage-listed-reopen.js";
import { MemoryBackend } from "../src/next/storage/memory-store.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeManifestRoot,
} from "../src/next/format/manifest-codec.js";
import { countingBudget } from "../src/next/storage/space-test-support.js";

function required<T>(value: T | null | undefined): T {
  if (value === null || value === undefined)
    throw new Error("Expected a stored reopen fixture value");
  return value;
}

it("builds real group directories and lists across a publication boundary, then reopens", async () => {
  const backend = new MemoryBackend();
  const built = await buildListedReopenSpace(backend, 4097, 4096);
  expect(built.groups).toBe(4097);
  const opened = await openListedReopen(backend);
  try {
    const budget = countingBudget(Infinity);
    const root = decodeManifestRoot(required(opened.space.root), budget);
    expect(root.value.sequence).toBe(2);
    expect(root.value.tables[0]?.groupCount).toBe(4097);
    expect(root.value.extentList.length).toBeGreaterThan(1);
    expect(root.value.tables[0]?.statistics).not.toBeNull();
    budget.free(root.charge);
    expect(opened.timing.bytesRead).toBeGreaterThan(4097 * 16);
    // Audit traverses the real catalog/statistics/directory/group/list references and compares
    // both kept roots with their lists; the timed open itself reads only list pages.
    await opened.space.audit();
  } finally {
    await opened.close();
  }
});

it("startup does not walk group roots; the explicit audit still refuses their corruption", async () => {
  const backend = new MemoryBackend();
  await buildListedReopenSpace(backend, 4, 4096);
  const lease = await backend.acquire();
  try {
    const session = await backend.open(lease);
    const rootBytes = required(await session.readRoot());
    const budget = countingBudget(Infinity);
    const root = decodeManifestRoot(rootBytes, budget);
    const read = async (ref: import("../src/next/storage/extents.js").BlobRef) =>
      required(
        (
          await session.readData([{ offset: ref.extent.offset + ref.offset, length: ref.length }])
        )[0],
      );
    const index = decodeGroupIndex(await read(required(root.value.tables[0]).groups), budget);
    const page = decodeGroupPage(await read(required(index.value[0]).ref), budget);
    const ref = required(page.value[0]).root;
    const damaged = (await read(ref)).slice();
    damaged[0] = (damaged[0] ?? 0) ^ 1;
    await session.writeData(ref.extent.offset + ref.offset, damaged);
    await session.publish(rootBytes);
    budget.free(root.charge + index.charge + page.charge);
    await session.close();
  } finally {
    await lease.release();
  }
  const opened = await openListedReopen(backend);
  try {
    await expect(opened.space.audit()).rejects.toThrow(/checksum/i);
  } finally {
    await opened.close();
  }
});
