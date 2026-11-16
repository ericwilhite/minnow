/** Native storage fixtures; the compact logical root never retains a per-data-extent array. */
import { expect } from "vitest";
import { Governor, type Grant } from "../memory/governor.js";
import { DATA_ALIGN, type StorageBackend, type StoreSession } from "../storage/contract.js";
import { ExtentListKeeper, listedExtents, type ExtentList } from "../storage/extent-list.js";
import type { BlobRef, ExtentRef, ExtentSpace, ReachableExtents } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";

export interface TestRoot {
  first: number;
  count: number;
  list: ExtentList;
  extra?: BlobRef;
}
const encoder = new TextEncoder(),
  decoder = new TextDecoder();
export const encodeRoot = (root: TestRoot): Uint8Array => encoder.encode(JSON.stringify(root));
export const decodeRoot = (bytes: Uint8Array): TestRoot =>
  JSON.parse(decoder.decode(bytes)) as TestRoot;
export const fromList: ReachableExtents = (root, reader, budget) =>
  listedExtents(decodeRoot(root).list, reader, budget, undefined, true, 1);
export const fullWalk: ReachableExtents = function* (bytes) {
  const root = decodeRoot(bytes);
  for (let i = 0; i < root.count; i++) yield { offset: root.first + i * DATA_ALIGN, length: 1 };
  if (root.extra !== undefined) yield root.extra;
  for (const page of root.list) yield page.blob;
};
export function* descriptors(first: number, count: number, reverse = false): Generator<ExtentRef> {
  for (let i = 0; i < count; i++)
    yield { offset: first + (reverse ? count - i - 1 : i) * DATA_ALIGN, length: 1 };
}
export async function openOwner(backend: StorageBackend = new MemoryBackend()) {
  const governor = new Governor(16 << 20);
  const startup = await governor.admit({
    label: "retirement fixture/root callbacks",
    minimumBytes: 6 << 20,
    spillable: false,
  });
  const lease = await backend.acquire(),
    session = await backend.open(lease);
  // Native settlement fence is a test-only implementation hook, not an added public API.
  const space = (await openExtentSpace(session, {
    reachable: fromList,
    walk: fullWalk,
    verifyAtOpen: true,
    budget: startup,
    registerCache: (cache) => governor.registerCache(cache),
  })) as ExtentSpace & { settled(): Promise<void> };
  let closed = false;
  let heldFixture: { root: TestRoot; keeper: ExtentListKeeper } | undefined;
  return {
    backend,
    governor,
    startup,
    lease,
    session,
    space,
    holdFixture(root: TestRoot, keeper: ExtentListKeeper) {
      if (heldFixture !== undefined) throw new Error("One admitted fixture list per owner");
      heldFixture = { root, keeper };
    },
    async close() {
      if (closed) return;
      closed = true;
      try {
        await space.close();
        await session.close();
        await lease.release();
      } finally {
        if (heldFixture !== undefined) {
          heldFixture.root.list = [];
          heldFixture.keeper.close();
          heldFixture = undefined;
        }
        startup.close();
        expect(governor.usedBytes).toBe(0);
      }
    },
  };
}
export type Owner = Awaited<ReturnType<typeof openOwner>>;
export async function fixture(
  owner: Owner,
  count: number,
  pageExtents = 64,
): Promise<{ root: TestRoot; anchor: BlobRef; firstBlob: BlobRef }> {
  const writer = owner.space.beginWrite("compact retirement fixture", { budget: owner.startup });
  const keeper = new ExtentListKeeper([], {
    reader: owner.space,
    budget: owner.startup,
    pageExtents,
  });
  let retained = false;
  let first = -1,
    anchor: BlobRef | undefined,
    firstBlob: BlobRef | undefined;
  try {
    for (let i = 0; i < count; i++) {
      const result = await writer.write([new Uint8Array([i & 255])]);
      if (first < 0) first = result.extent.offset;
      expect(result.extent.offset).toBe(first + i * DATA_ALIGN);
      anchor = result.blobs[0];
      if (i === 0) firstBlob = anchor;
    }
    const prepared = await keeper.prepare(1, { added: writer.extents, released: [], writer });
    const root = { first, count, list: prepared.list };
    await owner.space.publish(encodeRoot(root), {
      writers: [writer],
      retired: prepared.retire,
      list: root.list,
    });
    keeper.commit(1, 1);
    if (anchor === undefined || firstBlob === undefined) throw new Error("Missing fixture anchor");
    owner.holdFixture(root, keeper);
    retained = true;
    return { root, anchor, firstBlob };
  } finally {
    if (!retained) keeper.close();
    if (writer.state === "open") await writer.abort();
  }
}
export async function pressure(
  owner: Owner,
  bytes = 384 << 10,
): Promise<{ grant: Grant; close(): void }> {
  const grant = await owner.governor.admit({
    label: "fixed retirement window",
    minimumBytes: bytes,
    spillable: false,
  });
  const competitor = await owner.governor.admit({
    label: "all remaining ordinary credit",
    minimumBytes: owner.governor.capBytes - owner.governor.reserveBytes - owner.governor.usedBytes,
    spillable: false,
  });
  return {
    grant,
    close() {
      try {
        expect(grant.allocated).toBe(0);
      } finally {
        grant.close();
        competitor.close();
      }
    },
  };
}
export function releasedBytes(
  ranges: ReadonlyArray<{ offset: number; length: number }>,
  offset: number,
  length: number,
): number {
  return ranges.reduce(
    (sum, r) =>
      sum +
      Math.max(0, Math.min(offset + length, r.offset + r.length) - Math.max(offset, r.offset)),
    0,
  );
}
export function trackReleases(session: StoreSession): Array<{ offset: number; length: number }> {
  const ranges: Array<{ offset: number; length: number }> = [],
    release = session.releaseData.bind(session);
  session.releaseData = async (input) => {
    ranges.push(...input.map((r) => ({ ...r })));
    await release(input);
  };
  return ranges;
}
