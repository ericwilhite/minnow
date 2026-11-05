/**
 * Benchmarks the manifest codecs, the metadata reader's caches and the open walk
 * (packages/core/src/next/format), against two targets:
 * - R6: an owner-side cached point read ≤ 10 µs, so a cached group root / directory must cost a
 *   small fraction of that;
 * - Gate "reopen ≤ 500 ms at W5M" (PROPOSAL.md Targets): the open walk over 5M rows × 200 columns
 *   (77 groups of 65,536 rows) reads one root, one directory index and page, and 77 group roots.
 *
 *   npx vite-node scripts/manifest-bench.mts [--repeats 7]
 *
 * Each figure is the median of `repeats` samples after a 200 ms warm-up; samples last ≥ 20 ms.
 */
import { performance } from "node:perf_hooks";
import { BlobCache } from "../packages/core/src/next/format/blob-cache.ts";
import {
  decodeGroupPage,
  decodeGroupRoot,
  decodeManifestRoot,
  encodeGroupPage,
  encodeGroupRoot,
  encodeManifestRoot,
  writeGroupDirectory,
} from "../packages/core/src/next/format/manifest-codec.ts";
import type {
  BlobRef,
  ColumnChunk,
  GroupRef,
  GroupRoot,
  ManifestRoot,
  TableRoot,
} from "../packages/core/src/next/format/manifest.ts";
import { MetadataReader } from "../packages/core/src/next/format/metadata-reader.ts";
import { reachableExtents } from "../packages/core/src/next/format/reachable.ts";
import { MemoryExtentSpace, UNLIMITED } from "../packages/core/src/next/page/memory-extents.ts";

const args = process.argv.slice(2);
const index = args.indexOf("--repeats");
const REPEATS = index >= 0 ? Number(args[index + 1]) : 7;

async function timed(run: () => unknown): Promise<number> {
  let calls = 0;
  const warm = performance.now();
  while (performance.now() - warm < 200 || calls < 3) {
    await run();
    calls++;
  }
  const perCall = (performance.now() - warm) / calls;
  const inner = Math.max(1, Math.ceil(20 / Math.max(perCall, 1e-4)));
  const samples: number[] = [];
  for (let k = 0; k < REPEATS; k++) {
    const start = performance.now();
    for (let i = 0; i < inner; i++) await run();
    samples.push((performance.now() - start) / inner);
  }
  samples.sort((a, b) => a - b);
  return samples[Math.floor(samples.length / 2)] ?? 0;
}

const extent = { offset: 1 << 24, length: 1 << 24 };
const blob = (offset: number, length: number): BlobRef => ({
  extent,
  offset,
  length,
  checksum: 0x1234_5678,
});

function groupRoot(columns: number): GroupRoot {
  const chunks: ColumnChunk[] = Array.from({ length: columns }, (_, columnId) => ({
    columnId,
    type:
      columnId % 3 === 0
        ? { kind: "text" }
        : columnId % 3 === 1
          ? { kind: "int32", bits: 32 }
          : { kind: "float64" },
    data: { extent, offset: columnId * 20_000, length: 19_000 },
    directory: blob(columnId * 20_000 + 19_000, 400),
    statistics: {
      nullCount: columnId,
      min: columnId % 3 === 0 ? new Uint8Array(24).fill(97) : columnId % 3 === 1 ? -columnId : 0.5,
      max:
        columnId % 3 === 0
          ? new Uint8Array(32).fill(122)
          : columnId % 3 === 1
            ? columnId * 1000
            : 99.5,
      dictionarySize: columnId % 3 === 0 ? 1000 : null,
    },
    distinctSketch: blob(columnId * 20_000 + 19_400, 300),
  }));
  return { rowCount: 65_536, rowIds: { kind: "dense" }, columns: chunks };
}

const groups: GroupRef[] = Array.from({ length: 4096 }, (_, k) => ({
  groupId: k + 1,
  firstRowId: k * 65_536 + 1,
  lastRowId: (k + 1) * 65_536,
  rowCount: 65_536,
  liveRowCount: 65_000,
  root: blob(k * 512, 500),
  deletions: k % 4 === 0 ? blob(k * 512 + 500, 12) : null,
}));

function manifestRoot(tables: number): ManifestRoot {
  const runs = Array.from({ length: 5 }, (_, level) => ({
    level,
    entryCount: 1_000_000,
    entries: blob(level * 100, 90),
    fences: blob(level * 100 + 90, 5),
    bloom: blob(level * 100 + 95, 5),
    minKey: new Uint8Array(9).fill(1),
    maxKey: new Uint8Array(9).fill(250),
  }));
  const tableRoots: TableRoot[] = Array.from({ length: tables }, (_, tableId) => ({
    tableId,
    schemaVersion: 3,
    nextRowId: 5_000_001,
    flushedThroughLsn: 123_456,
    liveRowCount: 5_000_000,
    groups: blob(0, 300),
    groupCount: 77,
    staged: [],
    keyIndex: { indexId: 1, columnIds: [1], unique: true, runs },
    secondaryIndexes: [{ indexId: 2, columnIds: [3, 4], unique: false, runs }],
    statistics: blob(500, 1000),
  }));
  return {
    formatVersion: 1,
    sequence: 99,
    epoch: 3,
    log: { salt: 77, firstLsn: 1, rootSequence: 90 },
    globalLsn: 123_456,
    catalogVersion: 12,
    catalog: blob(0, 4000),
    nextTableId: tables,
    sequences: Array.from({ length: 10 }, (_, sequenceId) => ({
      sequenceId,
      reservedThrough: 10_000n,
    })),
    tables: tableRoots,
    clients: Array.from({ length: 50 }, (_, k) => ({
      clientId: `client-${String(k)}`,
      resolvedThrough: 1000,
      outcomes: Array.from({ length: 8 }, (_, s) => ({
        sequence: s,
        lsn: s + 100,
        counts: { inserted: 1 },
        recordedAtMs: 1.7e12,
      })),
    })),
  };
}

const rows: string[] = [];
const report = (name: string, ms: number, bytes: number): void => {
  rows.push(`${name} | ${(ms * 1000).toFixed(1)} µs | ${String(bytes)} B`);
};

for (const columns of [200, 500]) {
  const root = groupRoot(columns);
  const bytes = encodeGroupRoot(root);
  report(
    `group root ${String(columns)} cols: encode`,
    await timed(() => encodeGroupRoot(root)),
    bytes.length,
  );
  report(
    `group root ${String(columns)} cols: decode`,
    await timed(() => decodeGroupRoot(bytes, UNLIMITED)),
    bytes.length,
  );
}
const pageBytes = encodeGroupPage(groups);
report(
  "directory page 4,096 groups: encode",
  await timed(() => encodeGroupPage(groups)),
  pageBytes.length,
);
report(
  "directory page 4,096 groups: decode",
  await timed(() => decodeGroupPage(pageBytes, UNLIMITED)),
  pageBytes.length,
);
const rootValue = manifestRoot(100);
const rootBytes = encodeManifestRoot(rootValue);
report(
  "root 100 tables, 2 indexes × 5 runs, 50 clients: encode",
  await timed(() => encodeManifestRoot(rootValue)),
  rootBytes.length,
);
report(
  "root: decode",
  await timed(() => decodeManifestRoot(rootBytes, UNLIMITED)),
  rootBytes.length,
);

// Cached hits through the metadata reader (the point-read path).
const space = new MemoryExtentSpace();
const writer = space.beginWrite("bench");
const rootBlob = (await writer.write([encodeGroupRoot(groupRoot(200))])).blobs[0] as BlobRef;
const caches = {
  groupRoots: new BlobCache<GroupRoot>(() => ({ charge: () => true, release: () => undefined })),
};
const reader = new MetadataReader(space, caches);
(await reader.groupRoot(rootBlob, UNLIMITED)).release();
report(
  "group root 200 cols: cached hit (pin + release)",
  await timed(() => {
    const held = reader.groupRoot(rootBlob, UNLIMITED);
    if (held instanceof Promise) throw new Error("expected a hit");
    held.release();
  }),
  0,
);
const uncached = new MetadataReader(space);
report(
  "group root 200 cols: resident blob, parse (no decoded cache)",
  await timed(() => {
    const held = uncached.groupRoot(rootBlob, UNLIMITED);
    if (held instanceof Promise) throw new Error("expected a peek");
    held.release();
  }),
  0,
);

// The open walk at W5M: 77 groups × 200 columns, real blobs in the memory space.
const w5m = new MemoryExtentSpace();
const metadata = w5m.beginWrite("w5m");
const roots = await metadata.write(
  Array.from({ length: 77 }, () => encodeGroupRoot(groupRoot(200))),
);
const refs: GroupRef[] = roots.blobs.map((root, k) => ({
  groupId: k + 1,
  firstRowId: k * 65_536 + 1,
  lastRowId: (k + 1) * 65_536,
  rowCount: 65_536,
  liveRowCount: 65_536,
  root,
  deletions: null,
}));
const directory = await writeGroupDirectory(metadata, refs);
const w5mRoot = encodeManifestRoot({
  ...manifestRoot(1),
  tables: [{ ...(manifestRoot(1).tables[0] as TableRoot), groups: directory.ref }],
});
const walk = reachableExtents();
const cold = w5m.view({ cache: false });
let reached = 0;
const walkMs = await timed(async () => {
  reached = 0;
  for await (const ref of walk(w5mRoot, cold, UNLIMITED)) {
    void ref;
    reached++;
  }
});
report(`open walk W5M (77 group roots × 200 cols, cold reads, ${String(reached)} refs)`, walkMs, 0);

console.log(`median of ${String(REPEATS)}`);
console.log("case | time | bytes");
for (const row of rows) console.log(row);
