/** Synthetic root handoff only: valid full metadata, opaque unvisited directory/data refs. */
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
import type { GroupRoot } from "../packages/core/dist/next/format/manifest.js";
export interface RootComponentConfig {
  runtimeRoot: string;
  cached: boolean;
  /** Full-parser comparison uses distinct group addresses, not repeated number/string keys. */
  decodeOnly?: boolean;
  /** Same query-local cache in both arms; only the candidate understands the owner binding. */
  validatedRoots?: boolean;
  warmups: number;
  repeats: number;
}
export default async function createRootComponent(
  context: CPUWorkerContext,
): Promise<CPUWorkerHarness> {
  const config = context.data as RootComponentConfig;
  const load = (name: string) =>
    import(pathToFileURL(join(config.runtimeRoot, "packages/core/dist/next", name + ".js")).href);
  const metadata = (await load(
    "format/metadata-reader",
  )) as typeof import("../packages/core/dist/next/format/metadata-reader.js");
  const codec = (await load(
    "format/manifest-codec",
  )) as typeof import("../packages/core/dist/next/format/manifest-codec.js");
  const memory = (await load(
    "memory/governor",
  )) as typeof import("../packages/core/dist/next/memory/governor.js");
  const extents = (await load(
    "page/memory-extents",
  )) as typeof import("../packages/core/dist/next/page/memory-extents.js");
  const cache = config.cached
    ? ((await load(
        "page/query-roots",
      )) as typeof import("../packages/core/dist/next/page/query-roots.js"))
    : null;
  const roots: GroupRoot[] = Array.from({ length: 163 }, (_, g) => ({
    rowCount: 6144,
    rowIds: { kind: "dense" },
    columns: Array.from({ length: 200 }, (_, c) => {
      const extent = {
        offset: config.decodeOnly ? (g * 200 + c + 1) * 65536 : (c + 1) * 4096,
        length: 1024,
      };
      return {
        columnId: c + 1,
        type: { kind: "int32", bits: 32 },
        data: { extent, offset: 0, length: 128 },
        directory: { extent, offset: 128, length: 0, checksum: 0 },
        statistics: { nullCount: 0, min: g, max: g + c, dictionarySize: null },
        distinctSketch: null,
      };
    }),
  }));
  const space = new extents.MemoryExtentSpace();
  const { blobs } = await space
    .beginWrite("independent root component")
    .write(roots.map(codec.encodeGroupRoot));
  const governor = new memory.Governor(128 << 20);
  const grant = await governor.admit({
    label: "component roots",
    minimumBytes: 1 << 20,
    spillable: false,
  });
  // A parser component must not silently become an owner-recipe hit after recipe integration.
  // Keep verified encoded reads, with optional parsed/attach hooks absent in both arms.
  const pages = config.decodeOnly
    ? { peek: space.peek.bind(space), read: space.read.bind(space) }
    : space;
  const blobCaches = config.validatedRoots
    ? ((await load(
        "format/blob-cache",
      )) as typeof import("../packages/core/dist/next/format/blob-cache.js"))
    : undefined;
  const groupRoots = blobCaches
    ? new blobCaches.BlobCache<GroupRoot>(
        (c) => governor.registerCache(c),
        (ref) =>
          `${ref.extent.offset}:${ref.extent.length}:${ref.offset}:${ref.length}:${ref.checksum}`,
      )
    : undefined;
  const shared = groupRoots ? { groupRoots, validatedRoots: true } : undefined;
  const selected = [2, 6, 10],
    expectedVisits = 2 * blobs.length * selected.length;
  const expected = roots.map((root) => ({
    rowCount: root.rowCount,
    columns: root.columns.filter((c) => selected.includes(c.columnId)),
  }));
  const expectedHash = createHash("sha256")
    .update(JSON.stringify(config.decodeOnly ? roots : expected))
    .digest("hex");
  async function once(verify = false) {
    const owner = cache
      ? new cache.QueryRoots(governor, [{ tableId: 1, columns: selected }], shared)
      : undefined;
    const reader = new metadata.MetadataReader(pages);
    let visits = 0;
    try {
      for (let pass = 0; pass < 2; pass++)
        for (let g = 0; g < blobs.length; g++) {
          const ref = blobs[g];
          if (!ref) throw new Error("Missing root");
          const held = owner
            ? await owner.read(1, ref, pages, grant, selected)
            : await reader.groupRoot(ref, grant);
          try {
            if (verify && config.decodeOnly && !isDeepStrictEqual(held.value, roots[g]))
              throw new Error("Complete root decode equality");
            const value = {
              rowCount: held.value.rowCount,
              columns: held.value.columns.filter((c) => selected.includes(c.columnId)),
            };
            if (verify && !isDeepStrictEqual(value, expected[g]))
              throw new Error("Root component equality");
            visits += value.columns.length;
          } finally {
            held.release();
          }
        }
    } finally {
      owner?.close();
    }
    if (visits !== expectedVisits || grant.allocated !== 0)
      throw new Error("Root visits / charge mismatch");
    return { rows: visits };
  }
  let warmed = false;
  return {
    revision: context.revision,
    identity: {
      runtimeRoot: config.runtimeRoot,
      cached: config.cached,
      decodeOnly: config.decodeOnly ?? false,
      validatedRoots: config.validatedRoots ?? false,
      groups: 163,
      storedColumns: 200,
      selected,
      repeats: config.repeats,
      warmups: config.warmups,
    },
    workloads: ["root_handoff", "root_decode"],
    boundaries: ["roots"],
    async verify() {
      await once(true);
      if (!warmed) {
        for (let w = 0; w < config.warmups; w++) await once();
        warmed = true;
      }
      return { fingerprint: expectedHash, rows: expectedVisits * config.repeats };
    },
    async run() {
      return await queryLifetimes(config.repeats, expectedVisits, () => once());
    },
    async close() {
      groupRoots?.clear();
      grant.close();
      if (governor.usedBytes !== 0) throw new Error("Root component charge leak");
    },
  };
}
