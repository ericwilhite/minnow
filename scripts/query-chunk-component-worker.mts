/** Isolated admission directory preparation -> execution open, without payload decoding.
 * All 163 x3 chunks are encoded independently. The control opens/closes twice; the candidate
 * consumes prepared facts for sizing and opens once from its charged query directory cache.
 * Includes cache registration/charges/disposal. Full canonical directory bytes checked outside
 * timing. Exact sizing reads and cached execution reads match the production facades.
 */
import { createHash } from "node:crypto";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import type { CPUWorkerContext, CPUWorkerHarness } from "./lib/independent-cpu.mts";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
import type { ColumnChunk } from "../packages/core/dist/next/format/manifest.js";
export interface ChunkComponentConfig {
  runtimeRoot: string;
  reuse: boolean;
  warmups: number;
  repeats: number;
  groups: number;
}
export default async function createChunkComponent(
  context: CPUWorkerContext,
): Promise<CPUWorkerHarness> {
  const config = context.data as ChunkComponentConfig;
  const load = (name: string) =>
    import(pathToFileURL(join(config.runtimeRoot, "packages/core/dist/next", name + ".js")).href);
  const { writeGroup, UNLIMITED } = (await load(
    "page/test-support",
  )) as typeof import("../packages/core/dist/next/page/test-support.js");
  const { MemoryExtentSpace } = (await load(
    "page/memory-extents",
  )) as typeof import("../packages/core/dist/next/page/memory-extents.js");
  const { ChunkReader, directoryCharge, DECODE_MEMO_BYTES } = (await load(
    "page/reader",
  )) as typeof import("../packages/core/dist/next/page/reader.js");
  const { encodeDirectory } = (await load(
    "page/directory",
  )) as typeof import("../packages/core/dist/next/page/directory.js");
  const { QueryRoots } = (await load(
    "page/query-roots",
  )) as typeof import("../packages/core/dist/next/page/query-roots.js");
  const { Governor } = (await load(
    "memory/governor",
  )) as typeof import("../packages/core/dist/next/memory/governor.js");
  const space = new MemoryExtentSpace(),
    chunks: ColumnChunk[] = [];
  for (let g = 0; g < config.groups; g++) {
    const made = await writeGroup(
      Array.from({ length: 3 }, (_, c) => ({
        type: { kind: "float64" as const },
        values: Array.from({ length: 6144 }, (_, r) => (g * 6144 + r) / 8 + c),
      })),
      { space },
    );
    chunks.push(...made.group.columns);
  }
  const expected = createHash("sha256");
  for (let pass = 0; pass < 2; pass++)
    for (const chunk of chunks) {
      const lease = await space.read([chunk.directory], { budget: UNLIMITED });
      try {
        expected.update(lease.bytes[0] ?? new Uint8Array(0));
      } finally {
        lease.release();
      }
    }
  const fingerprint = expected.digest("hex"),
    governor = new Governor(128 << 20);
  const grant = await governor.admit({
    label: "directory component",
    minimumBytes: 8 << 20,
    spillable: false,
  });
  const exact = {
    peek: () => null,
    read: (refs: Parameters<typeof space.read>[0], options: Parameters<typeof space.read>[1]) =>
      space.read(refs, { ...options, once: false, reuseBuffers: false }),
  };
  async function once(verify = false) {
    const cache = new QueryRoots(governor, []),
      sizePages = config.reuse ? cache.reader(exact) : exact,
      executionPages = config.reuse ? cache.reader(space) : space;
    const actual = verify ? createHash("sha256") : null;
    let visits = 0,
      nativeBytes = 0;
    try {
      for (let pass = 0; pass < 2; pass++)
        for (const chunk of chunks) {
          if (config.reuse && pass === 0) {
            const prepared = await ChunkReader.prepare(sizePages, chunk, {
              budget: grant,
              once: false,
            });
            try {
              actual?.update(encodeDirectory(prepared.directory));
              nativeBytes += prepared.nativeBytes;
              visits++;
            } finally {
              prepared.release();
            }
          } else {
            const reader = await ChunkReader.open(pass === 0 ? sizePages : executionPages, chunk, {
              budget: grant,
              once: pass === 0,
            });
            try {
              actual?.update(encodeDirectory(reader.directory));
              nativeBytes += directoryCharge(reader.directory) + DECODE_MEMO_BYTES;
              visits++;
            } finally {
              reader.close();
            }
          }
        }
      if (actual && actual.digest("hex") !== fingerprint)
        throw new Error("directory byte mismatch");
      if (
        verify &&
        config.reuse &&
        (cache.directories.hits !== chunks.length || cache.directories.misses !== chunks.length)
      )
        throw new Error("directory facts did not hand off");
      if (visits !== 2 * chunks.length || grant.allocated !== 0)
        throw new Error("directory visits/charge drift");
      return { rows: visits, nativeBytes };
    } finally {
      cache.close();
    }
  }
  let warmed = false;
  return {
    revision: context.revision,
    identity: {
      ...config,
      groups: config.groups,
      selectedColumns: 3,
      chunks: chunks.length,
      rowsPerChunk: 6144,
    },
    workloads: ["chunk_preparation"],
    boundaries: ["chunks"],
    async verify() {
      const first = await once(true);
      if (!warmed) {
        for (let k = 0; k < config.warmups; k++) await once();
        warmed = true;
      }
      return {
        fingerprint: createHash("sha256")
          .update(fingerprint + String(first.nativeBytes))
          .digest("hex"),
        rows: 2 * chunks.length * config.repeats,
      };
    },
    run: () => queryLifetimes(config.repeats, 2 * chunks.length, () => once()),
    async close() {
      grant.close();
      if (governor.usedBytes !== 0) throw new Error("directory component leak");
    },
  };
}
