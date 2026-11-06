/**
 * Component benchmarks for the new executor's aggregation (packages/core/src/next/exec/agg):
 * the code one optimisation touches, isolated, in CPU time (process.cpuUsage: user + system, so
 * a loaded host's descheduling does not count). Each lever's commit names its component here.
 *
 *   npx tsc -b packages/core && node --expose-gc scripts/next-agg-components.mts \
 *     [--dist <a build of packages/core/dist>] [--only <substring>] [--repeats 7]
 *
 * A/B: copy a parent's `packages/core/dist` to a directory, then alternate
 * `--dist <parent copy>` and `--dist <this build's copy>` runs, five rounds or more (the rule for
 * accepting a lever: its median beats the spread). Reports min and median CPU ms per run.
 */
import path from "node:path";
import { pathToFileURL } from "node:url";

const args = process.argv.slice(2);
const option = (name: string): string | undefined => {
  const at = args.indexOf(name);
  return at >= 0 ? args[at + 1] : undefined;
};
const DIST = path.resolve(
  option("--dist") ?? new URL("../packages/core/dist", import.meta.url).pathname,
);
const ONLY = option("--only") ?? "";
const REPEATS = Number(option("--repeats") ?? 7);
const load = async <T,>(file: string): Promise<T> =>
  (await import(pathToFileURL(path.join(DIST, file)).href)) as T;

type GroupsModule = typeof import("../packages/core/dist/next/exec/agg/groups.js");
type SupportModule = typeof import("../packages/core/dist/next/exec/kernels/test-support.js");
const groups = await load<GroupsModule>("next/exec/agg/groups.js");
const support = await load<SupportModule>("next/exec/kernels/test-support.js");
const { GroupTable } = groups;
const { TEXT, UNCHARGED, texts } = support;

/** mulberry32: the same keys in every run and build. */
function rng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = Math.imul(state ^ (state >>> 15), state | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const results: Array<{ name: string; min: number; median: number; runs: number[] }> = [];

/** Two warm-ups, then `REPEATS` runs of `once`, in CPU ms. */
function measure(name: string, once: () => void): void {
  if (!name.includes(ONLY)) return;
  for (let w = 0; w < 2; w++) once();
  const runs: number[] = [];
  for (let r = 0; r < REPEATS; r++) {
    const before = process.cpuUsage();
    once();
    const spent = process.cpuUsage(before);
    runs.push((spent.user + spent.system) / 1000);
  }
  const sorted = [...runs].sort((a, b) => a - b);
  results.push({
    name,
    min: sorted[0] ?? 0,
    median: sorted[Math.floor(sorted.length / 2)] ?? 0,
    runs,
  });
}

const ROWS = 1 << 20;
const BATCH = 2048;

// Text keys (GroupTable over one text column: the COUNT(DISTINCT) harness shape's hot path).
// Levers 1 (word-at-a-time copy and compare) and 2 (the value table's growth).
{
  const random = rng(0x51);
  const keyed = (make: () => string) =>
    Array.from({ length: ROWS / BATCH }, () => texts(Array.from({ length: BATCH }, make)));
  const shapes: Array<[string, ReturnType<typeof keyed>]> = [
    [
      "text keys: emails ~22 B, ~585k distinct",
      keyed(() => `user${String(Math.floor(random() * 800000))}@example.com`),
    ],
    [
      "text keys: accounts ~11 B, ~200k distinct",
      keyed(() => `acct-${String(Math.floor(random() * 200000))}`),
    ],
  ];
  const gids = new Int32Array(BATCH);
  for (const [name, batches] of shapes)
    measure(name, () => {
      const table = new GroupTable([TEXT], UNCHARGED);
      for (const batch of batches) table.assign([batch], BATCH, null, gids);
      table.close();
    });
}

console.log(`components (CPU ms per run, ${String(REPEATS)} runs) — dist ${DIST}`);
for (const r of results)
  console.log(
    `${r.name}: min ${r.min.toFixed(1)} median ${r.median.toFixed(1)} runs ${r.runs.map((x) => x.toFixed(0)).join(" ")}`,
  );
