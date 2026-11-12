/** Isolated sizing/scan root handoff over valid wide metadata, nine adjacent AB/BA CPU pairs.
 * Synthetic directories/data refs are not accessed. Full executor evidence is query-root-bench.
 */
import { mkdir, writeFile, mkdtemp } from "node:fs/promises";
import { tmpdir, loadavg } from "node:os";
import { join } from "node:path";
import { strict as assert } from "node:assert";
import { QueryRoots } from "../packages/core/dist/next/page/query-roots.js";
import { MetadataReader } from "../packages/core/dist/next/format/metadata-reader.js";
import { encodeGroupRoot } from "../packages/core/dist/next/format/manifest-codec.js";
import type { GroupRoot } from "../packages/core/dist/next/format/manifest.js";
import { MemoryExtentSpace } from "../packages/core/dist/next/page/memory-extents.js";
import { Governor } from "../packages/core/dist/next/memory/governor.js";
import { pairedCPU } from "./lib/paired-cpu.mts";
const at = process.argv.indexOf("--output");
const output =
  at < 0 ? await mkdtemp(join(tmpdir(), "minnow-root-component-")) : process.argv[at + 1];
if (!output) throw new Error("Missing output");
await mkdir(output, { recursive: true });
const space = new MemoryExtentSpace();
const roots: GroupRoot[] = Array.from({ length: 163 }, (_, g) => ({
  rowCount: 6144,
  rowIds: { kind: "dense" },
  columns: Array.from({ length: 200 }, (_, c) => {
    const extent = { offset: (c + 1) * 4096, length: 1024 };
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
const { blobs } = await space.beginWrite("component roots").write(roots.map(encodeGroupRoot));
const governor = new Governor(128 << 20);
const grant = await governor.admit({
  label: "component/root",
  minimumBytes: 1 << 20,
  spillable: false,
});
let blackhole = 0;
let calls = 0;
async function run(cached: boolean, verify = false) {
  calls++;
  const selected = [2, 6, 10];
  const cache = cached ? new QueryRoots(governor, [{ tableId: 1, columns: selected }]) : undefined;
  const reader = new MetadataReader(space);
  try {
    for (let pass = 0; pass < 2; pass++)
      for (let i = 0; i < blobs.length; i++) {
        const ref = blobs[i];
        if (!ref) throw new Error("Missing ref");
        const root = cache
          ? await cache.read(1, ref, space, grant, selected)
          : await reader.groupRoot(ref, grant);
        try {
          const columns = root.value.columns.filter((column) => selected.includes(column.columnId));
          if (verify)
            assert.deepEqual(
              columns,
              roots[i]?.columns.filter((column) => selected.includes(column.columnId)),
            );
          blackhole += columns.length;
        } finally {
          root.release();
        }
      }
  } finally {
    cache?.close();
  }
  assert.equal(grant.allocated, 0);
}
// Batch whole queries so natural/background GC is measured over a useful CPU interval.
const repeats = 10;
const a: number[] = [],
  b: number[] = [];
const samples = [];
try {
  await run(false, true);
  await run(true, true);
  await run(false);
  await run(true);
  for (let pair = 0; pair < 9; pair++)
    for (const cached of pair % 2 ? [true, false] : [false, true]) {
      const beforeCalls = calls;
      const load = loadavg(),
        wall = performance.now(),
        cpu = process.cpuUsage();
      for (let repeat = 0; repeat < repeats; repeat++) await run(cached);
      const used = process.cpuUsage(cpu),
        wallMs = performance.now() - wall;
      assert.equal(calls - beforeCalls, repeats);
      const cpuMs = (used.user + used.system) / 1000 / repeats;
      (cached ? b : a).push(cpuMs);
      samples.push({ pair, cached, cpuMs, wallMs: wallMs / repeats, repeats, load });
    }
  await run(false, true);
  await run(true, true);
} finally {
  grant.close();
}
assert.equal(governor.usedBytes, 0);
assert.equal(calls, 6 + 18 * repeats);
assert.equal(blackhole, calls * 2 * blobs.length * 3);
const result = {
  fixture: {
    groups: roots.length,
    columns: 200,
    selectedColumns: [2, 6, 10],
    encodedBytes: blobs.reduce((n, ref) => n + ref.length, 0),
  },
  samples,
  cpu: pairedCPU(a, b),
  blackhole,
  calls,
  governorUsedAfter: governor.usedBytes,
};
await writeFile(join(output, "component.json"), JSON.stringify(result, null, 2));
console.log(JSON.stringify(result.cpu));
