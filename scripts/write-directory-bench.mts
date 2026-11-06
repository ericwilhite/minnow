/**
 * Measures the write path's resident group directory (packages/core/src/next/write/groups.ts;
 * review B3-p2a-3, ruling on findings 1, 2 and 6) on the in-memory store, owner-side, in Node:
 *
 *   npx vite-node scripts/write-directory-bench.mts [--groups 4100,100000] [--appends 40]
 *
 * For each directory size G (one-row groups, built in batches of 1,000 groups per publish):
 * - resident metadata bytes per group (the write path's charged root and directory state);
 * - reopen time (wall and process CPU) with G groups;
 * - append-flush CPU: process CPU per flush that appends one group to a G-group directory, the
 *   median of `appends` flushes (each one a commit, its flush and its publish).
 * The machine is shared: compare runs made back to back.
 */
import { performance } from "node:perf_hooks";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
} from "../packages/core/src/next/write/test-support.ts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};
const SIZES = option("groups", "4100,100000").split(",").map(Number);
const APPENDS = Number(option("appends", "40"));

const cpu = (): number => {
  const usage = process.cpuUsage();
  return (usage.user + usage.system) / 1000;
};
const median = (values: number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
};

for (const groups of SIZES) {
  const harness = new Harness("relaxed", "memory", 1);
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          // No key: no index runs, so the root stays O(1) and the directory is what grows.
          table: tableInput("t", [column("id", INT, false)], { primaryKey: null }),
        })
      ).id ?? 0;
  });
  let next = 0;
  const built = performance.now();
  while (next < groups) {
    const count = Math.min(1000, groups - next);
    const ids = Array.from({ length: count }, (_, k) => next + k);
    await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
    next += count;
    await path.quiesce();
  }
  const buildMs = performance.now() - built;
  // One more row leaves the last memtable live; everything before it is published.
  await autocommit(path, (s) => s.table(t).insert(batch(ints([next++])), null));
  await path.quiesce();
  // `count` on the chunked directory, `length` on the array it replaced (before/after runs).
  const published = path.state().tables.get(t)?.groups as unknown as
    { readonly count?: number; readonly length?: number } | undefined;
  const groupCount = published?.count ?? published?.length ?? 0;
  const resident = path.residentMetadataBytes;
  const appendCpu: number[] = [];
  for (let k = 0; k < APPENDS; k++) {
    const before = cpu();
    await autocommit(path, (s) => s.table(t).insert(batch(ints([next++])), null));
    await path.quiesce();
    appendCpu.push(cpu() - before);
  }
  const openWall = performance.now();
  const openCpu = cpu();
  await harness.reopen();
  const reopenMs = performance.now() - openWall;
  const reopenCpu = cpu() - openCpu;
  console.log(
    JSON.stringify({
      groups: groupCount,
      buildSeconds: Number((buildMs / 1000).toFixed(1)),
      residentBytesPerGroup: Number((resident / Math.max(1, groupCount)).toFixed(1)),
      reopenMs: Math.round(reopenMs),
      reopenCpuMs: Math.round(reopenCpu),
      spaceOpenMs: Math.round(harness.lastOpen.spaceMs),
      writePathOpenMs: Math.round(harness.lastOpen.pathMs),
      appendFlushCpuMs: Number(median(appendCpu).toFixed(2)),
    }),
  );
  await harness.close();
}
