/**
 * Measures what the log costs a reopen (the single log scan and replay of `openWritePath`) at a
 * given log size, on the in-memory store, owner-side, in Node:
 *
 *   npx vite-node scripts/write-log-scan-bench.mts [--mib 32,8] [--runs 3]
 *
 * For each size: keyed 2,048-row commits (id INT PRIMARY KEY, v TEXT, n INT) until the log holds
 * that many MiB (recycling off for the fill), everything flushed but the live memtable, then the
 * median of `runs` reopens: the write path's open (`Harness.lastOpen.pathMs`, which is the log scan,
 * replay of the unflushed tail and the root/directory read) and the extent space's open apart.
 * The machine is shared: compare runs made back to back.
 */
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
  TEXT,
  texts,
} from "../packages/core/src/next/write/test-support.ts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? (args[index + 1] ?? fallback) : fallback;
};
const SIZES = option("mib", "32,8").split(",").map(Number);
const RUNS = Number(option("runs", "3"));
const median = (values: number[]): number => {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
};

for (const mib of SIZES) {
  const harness = new Harness("relaxed", "memory", undefined, undefined, {
    logRecycleBytes: 2 ** 40,
  });
  const path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", TEXT), column("n", INT)]),
        })
      ).id ?? 0;
  });
  let next = 0;
  while (path.stats().logBytes < mib * 1024 * 1024) {
    const ids = Array.from({ length: 2048 }, (_, k) => next + k);
    next += ids.length;
    await autocommit(path, (s) =>
      s
        .table(t)
        .insert(
          batch(
            ints(ids),
            texts(ids.map((id) => `value ${String(id)}`)),
            ints(ids.map((id) => id % 1000)),
          ),
          null,
        ),
    );
  }
  await path.quiesce();
  const logBytes = path.stats().logBytes;
  const pathMs: number[] = [];
  const spaceMs: number[] = [];
  for (let run = 0; run < RUNS; run++) {
    await harness.reopen();
    pathMs.push(harness.lastOpen.pathMs);
    spaceMs.push(harness.lastOpen.spaceMs);
  }
  console.log(
    JSON.stringify({
      logMiB: Number((logBytes / 1024 / 1024).toFixed(1)),
      rows: next,
      writePathOpenMs: Math.round(median(pathMs)),
      spaceOpenMs: Math.round(median(spaceMs)),
    }),
  );
  await harness.close();
}
