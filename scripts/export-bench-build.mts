/**
 * Builds the wide table on disk for scripts/export-bench.mts: `--rows` rows × 200 columns of the
 * W1M generator (scripts/browser-perf/page/wide-data.ts), written through the real file store and
 * extent space into `--dir` (defaults under os.tmpdir()), then `--copies − 1` byte copies of every group's
 * extents at new addresses (W5M = W1M × 5: encoding throughput is not what the export measures,
 * and generating 1 billion values would take most of an hour). The groups' column chunks are
 * saved as `groups.json`.
 *
 *   npx vite-node scripts/export-bench-build.mts --dir ~/.cache/minnow-export --rows 1048576 --copies 5
 */
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";
import type { ExtentRef } from "../packages/core/src/next/storage/extents.ts";
import { FileStoreSession } from "../packages/core/src/next/storage/file-store.ts";
import { openExtentSpace } from "../packages/core/src/next/storage/space.ts";
import {
  countingBudget,
  countingMemory,
} from "../packages/core/src/next/storage/space-test-support.ts";
import { wideSpec } from "./browser-perf/page/wide-data.ts";
import { jsonReplacer, openFileSet } from "./node-sync-file.mts";
import { rebaseExtents } from "./browser-perf/page/export-stream.ts";
import { typeOf, vectorOf } from "./browser-perf/page/wide-vectors.ts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};
const DIR = option("dir", join(tmpdir(), "minnow-export"));
const ROWS = Number(option("rows", "1048576"));
const COPIES = Number(option("copies", "5"));
const GROUP = 65_536;
const MiB = 1024 * 1024;

const files = openFileSet(DIR, true);
const session = new FileStoreSession(files, 1, () => undefined);
const space = await openExtentSpace(session, {
  // An empty store: there is no root, so nothing to walk.
  reachable: async function* () {
    yield* [];
  },
  budget: countingBudget(Number.POSITIVE_INFINITY),
  registerCache: () => countingMemory(Number.POSITIVE_INFINITY),
});
const spec = wideSpec(200);
const types = spec.map((column) => typeOf(column));
interface Group {
  readonly rows: number;
  readonly chunks: readonly ColumnChunk[];
  readonly extents: readonly ExtentRef[];
}
const groups: Group[] = [];
const started = performance.now();
for (let first = 0; first < ROWS; first += GROUP) {
  const count = Math.min(GROUP, ROWS - first);
  const writer = new GroupWriter({
    columns: spec.map((_, k) => ({ columnId: k + 1, type: types[k] ?? { kind: "int64" } })),
    writer: space.beginWrite(`group ${String(first)}`),
  });
  await writer.append(
    spec.map((column, k) => vectorOf(column, types[k] ?? { kind: "int64" }, first, count, ROWS)),
    count,
  );
  const written = await writer.finish();
  groups.push({ rows: count, chunks: written.columns, extents: written.extents });
  console.log(
    `group ${String(groups.length)}: ${((performance.now() - started) / 1000).toFixed(0)} s`,
  );
}

const all: Array<{ rows: number; chunks: readonly ColumnChunk[] }> = groups.map(
  ({ rows, chunks }) => ({ rows, chunks }),
);
for (let copy = 1; copy < COPIES; copy++)
  for (const group of groups) {
    const writer = space.beginWrite(`copy ${String(copy)}`);
    const moved = new Map<number, ExtentRef>();
    for (const extent of group.extents) {
      const bytes = new Uint8Array(extent.length);
      await session.readDataInto(extent.offset, bytes);
      const parts: Uint8Array[] = [];
      for (let at = 0; at < bytes.length; at += MiB)
        parts.push(bytes.subarray(at, Math.min(bytes.length, at + MiB)));
      const written = await writer.write(parts);
      if (written.extent.length !== extent.length)
        throw new Error("a copied extent changed length");
      moved.set(extent.offset, written.extent);
    }
    all.push({ rows: group.rows, chunks: rebaseExtents(group.chunks, moved) });
  }
writeFileSync(join(DIR, "groups.json"), JSON.stringify(all, jsonReplacer));
console.log(
  `built ${String(all.length)} groups (${String(all.reduce((sum, group) => sum + group.rows, 0))} rows × 200 columns), ${(files.data.getSize() / MiB).toFixed(0)} MiB, in ${((performance.now() - started) / 1000).toFixed(0)} s`,
);
for (const file of [files.log, files.data, ...files.roots]) file.close();
