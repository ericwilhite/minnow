/**
 * Benchmarks read-side pushdown on encoded pages (packages/core/src/next/page/predicate.ts,
 * select.ts, `ChunkReader.select` / `refine` / `gather`) on the W1M wide table's real columns
 * (scripts/browser-perf/page/wide-data.ts), against decode-then-filter on the same pages.
 *
 *   npx vite-node scripts/pushdown-bench.mts [--rows 1048576] [--repeats 5]
 *
 * Queries (scripts/browser-perf/catalogue.ts):
 * - `scan_selective_30col`: the 30 columns of C30 where the filter column `i09` (uniform 0–999)
 *   passes at 0.1 % (`= 7`, the catalogue's query), 1 % (`< 10`), 10 % (`< 100`) and 100 %
 *   (`< 1000`); and `id` (clustered) at 0.1 %, where pruning skips groups and pages.
 * - `scan_proj3_agg`: COUNT, SUM(f03), AVG(i05) where `i07 < 500000` (≈ 50 %).
 * Each is run two ways over pages already in the memory space's cache (warm: the gates measure
 * CPU) and once cold:
 * - before: load every page, decode every row of the filter column, compare in JS, decode every
 *   row of the projected columns (dense), as a scan without pushdown does;
 * - after: prune groups and pages by statistics, `select` on encoded pages, then per projected
 *   column either `gather` the selected rows or decode the range densely (whichever measured
 *   faster, below), loading only the pages that hold selected rows.
 * Reported per stage: ms, µs per filter-column page, MB/s of output vector bytes; and process
 * CPU (user + system) per run. Medians of `repeats` back-to-back runs after a warm-up.
 */
import { performance } from "node:perf_hooks";
import type { ColumnChunk } from "../packages/core/src/next/format/manifest.ts";
import type { ExtentReader } from "../packages/core/src/next/storage/extents.ts";
import { BlobCache } from "../packages/core/src/next/format/blob-cache.ts";
import type { ChunkDirectory } from "../packages/core/src/next/page/directory.ts";
import { MemoryExtentSpace, UNLIMITED } from "../packages/core/src/next/page/memory-extents.ts";
import { chunkMayMatch, type ColumnPredicate } from "../packages/core/src/next/page/predicate.ts";
import { ChunkReader, prefersDense } from "../packages/core/src/next/page/reader.ts";
import { targetFor } from "../packages/core/src/next/page/test-support.ts";
import { GroupWriter } from "../packages/core/src/next/page/writer.ts";
import type { FlatVector } from "../packages/core/src/next/vector/vector.ts";
import { wideSpec } from "./browser-perf/page/wide-data.ts";
import { typeOf, vectorOf } from "./wide-table.mts";

const args = process.argv.slice(2);
const option = (name: string, fallback: number): number => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? Number(args[index + 1]) : fallback;
};
const ROWS = option("rows", 1_048_576);
/** Runs only the queries whose label contains this text, and skips the crossover. */
const ONLY = args.includes("--only") ? (args[args.indexOf("--only") + 1] ?? "") : null;
/** Runs the gather / dense crossover only. */
const CROSSOVER = args.includes("--crossover");
/** `--descriptors off`: readers see a space without parsed descriptors (reader.ts), as before. */
const DESCRIPTORS = !(
  args.includes("--descriptors") && args[args.indexOf("--descriptors") + 1] === "off"
);
/** `--columns gate`: only the selective scan's columns (C30 and i09), for W5M. */
const GATE_COLUMNS = args.includes("--columns") && args[args.indexOf("--columns") + 1] === "gate";
/** Runs one way only (profiling). */
const WAY = args.includes("--way") ? (args[args.indexOf("--way") + 1] ?? "") : null;
const REPEATS = option("repeats", 5);
const GROUP = 65_536;

const C30 =
  "id ts day d00 d01 d02 d03 d04 d05 h00 h01 h05 i00 i01 i02 i03 i04 f00 f01 f02 f03 n00 n01 n02 b00 b01 x00 x01 x02 x03".split(
    " ",
  );
const EXTRA = ["i09", "i07", "i05"];
const spec = new Map(wideSpec(200).map((column) => [column.name, column]));

// ── The table ────────────────────────────────────────────────────────────────────────────────

interface Group {
  readonly rows: number;
  readonly chunks: Map<string, ColumnChunk>;
}

const names = [...new Set([...C30, ...(GATE_COLUMNS ? ["i09"] : EXTRA)])];
const space = new MemoryExtentSpace();
const groups: Group[] = [];
const built = performance.now();
for (let first = 0; first < ROWS; first += GROUP) {
  const count = Math.min(GROUP, ROWS - first);
  const columns = names.map((name) => {
    const column = spec.get(name);
    if (column === undefined) throw new Error(`no column ${name}`);
    return { name, column, type: typeOf(column) };
  });
  const writer = new GroupWriter({
    columns: columns.map(({ type }, index) => ({ columnId: index + 1, type })),
    writer: space.beginWrite("bench"),
  });
  await writer.append(
    columns.map(({ column, type }) => vectorOf(column, type, first, count, ROWS)),
    count,
  );
  const written = await writer.finish();
  groups.push({
    rows: count,
    chunks: new Map(
      columns.map(({ name }, index) => [name, written.columns[index] as ColumnChunk]),
    ),
  });
}
console.log(
  `built ${String(ROWS)} rows × ${String(names.length)} columns in ${((performance.now() - built) / 1000).toFixed(1)} s, ${(space.allocator.end / 2 ** 20).toFixed(0)} MiB stored; descriptors ${DESCRIPTORS ? "on" : "off"}`,
);

/** The space a reader sees: with or without parsed descriptors on its cache entries. */
function readerView(view: MemoryExtentSpace): ExtentReader {
  if (DESCRIPTORS) return view;
  return { read: (refs, options) => view.read(refs, options), peek: (ref) => view.peek(ref) };
}

/** Parsed directories shared by every reader (ruling R15.7), as the owner keeps them. */
const directories = new BlobCache<ChunkDirectory>(() => ({
  charge: () => true,
  release: () => undefined,
}));
const options = { budget: UNLIMITED, directories };

// ── One scan ─────────────────────────────────────────────────────────────────────────────────

interface Stages {
  filterMs: number;
  materializeMs: number;
  filterPages: number;
  outputBytes: number;
  selected: number;
  cpuMs: number;
  /** Aggregates (proj3_agg): checks both ways agree. */
  check: number;
}

const allPages = (reader: ChunkReader): number[] => reader.directory.pages.map((_, page) => page);

function outputBytes(target: FlatVector, rows: number): number {
  const data = target.data;
  switch (data.layout) {
    case "varlen":
      return (data.offsets[rows] ?? 0) + rows * 4;
    case "bool":
      return rows;
    case "i32":
      return rows * 4;
    default:
      return rows * 8;
  }
}

/** Sums a numeric column's selected rows (proj3_agg), to check the two ways agree. */
function sumOf(target: FlatVector, rows: Uint16Array | null, count: number): number {
  const data = target.data;
  let sum = 0;
  for (let k = 0; k < count; k++) {
    const slot = rows === null ? k : (rows[k] ?? 0);
    if (data.layout === "i32") sum += data.values[slot] ?? 0;
    else if (data.layout === "f64") sum += data.values[slot] ?? 0;
  }
  return sum;
}

/**
 * Targets reused across groups, as the executor reuses its batch buffers: one per column, grown
 * when a group needs more (fixed-width data at the group's rows, varlen at its bytes).
 */
const targets = new Map<string, FlatVector>();
function reusedTarget(name: string, reader: ChunkReader, rows: number, bytes: number): FlatVector {
  const kept = targets.get(name);
  const fits =
    kept !== undefined &&
    (kept.validity?.length ?? 0) * 32 >= rows &&
    (kept.data.layout !== "varlen" ||
      (kept.data.bytes.length >= bytes && kept.data.offsets.length > rows)) &&
    capacityOf(kept) >= rows;
  if (fits) {
    kept.length = rows;
    return kept;
  }
  const target = targetFor(reader, rows, bytes);
  targets.set(name, target);
  return target;
}

function capacityOf(vector: FlatVector): number {
  const data = vector.data;
  switch (data.layout) {
    case "varlen":
      return data.offsets.length - 1;
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      return data.values.length;
    case "int64":
      return data.values.values.length;
    case "decimal":
      return data.coefficients.repr === "int128"
        ? data.coefficients.limbs.length / 4
        : data.coefficients.values.length;
    default:
      return 0;
  }
}

/** before: decode then filter; after: select + gather / dense; mask: selectMask + fused sums. */
type Way = "before" | "after" | "mask";

/** Fused, branch-free filter and sum over a dense decode and a 0/1 mask (B4's shape). */
function maskedSum(target: FlatVector, mask: Uint8Array, rows: number): number {
  const data = target.data;
  let sum = 0;
  if (data.layout === "i32") {
    const values = data.values;
    for (let row = 0; row < rows; row++) sum += (values[row] ?? 0) & -(mask[row] ?? 0);
  } else if (data.layout === "f64") {
    const values = data.values;
    for (let row = 0; row < rows; row++) sum += mask[row] === 1 ? (values[row] ?? 0) : 0;
  }
  return sum;
}

async function scan(
  view: ExtentReader,
  filter: string,
  predicate: ColumnPredicate,
  project: readonly string[],
  way: Way,
  aggregate: boolean,
): Promise<Stages> {
  const stages: Stages = {
    filterMs: 0,
    materializeMs: 0,
    filterPages: 0,
    outputBytes: 0,
    selected: 0,
    cpuMs: 0,
    check: 0,
  };
  const cpu = process.cpuUsage();
  const selection = new Uint16Array(GROUP);
  const mask = new Uint8Array(GROUP);
  for (const group of groups) {
    const filterChunk = group.chunks.get(filter) as ColumnChunk;
    let t = performance.now();
    const reader = await ChunkReader.open(view, filterChunk, options);
    let count = 0;
    if (way === "before") {
      await reader.load(allPages(reader));
      const target = reusedTarget(
        `filter:${filter}`,
        reader,
        group.rows,
        reader.varlenBytes(0, group.rows),
      );
      reader.decodeRange(0, group.rows, target, 0);
      const compiled = reader.compilePredicate(predicate);
      if (compiled === null) throw new Error("not pushed down");
      // Decode-then-filter: the JS comparison a scan without pushdown evaluates.
      const data = target.data;
      if (data.layout === "i32") {
        const values = data.values;
        const op = predicate.op;
        const c = Number((predicate as { value: number }).value);
        for (let row = 0; row < group.rows; row++) {
          const value = values[row] ?? 0;
          if (op === "=" ? value === c : value < c) selection[count++] = row;
        }
      } else if (data.layout === "int64") {
        const values = data.values.values as Float64Array;
        const c = Number((predicate as { value: bigint }).value);
        for (let row = 0; row < group.rows; row++)
          if ((values[row] ?? 0) < c) selection[count++] = row;
      }
      stages.filterPages += reader.directory.pages.length;
    } else {
      const compiled = reader.compilePredicate(predicate);
      if (compiled === null) throw new Error("not pushed down");
      if (chunkMayMatch(compiled, group.rows, filterChunk.statistics)) {
        const pages = reader.pagesMatching(compiled);
        await reader.load(pages);
        count =
          way === "mask"
            ? reader.selectMask(compiled, 0, group.rows, mask)
            : reader.select(compiled, 0, group.rows, selection);
        stages.filterPages += pages.length;
      }
    }
    reader.close();
    stages.filterMs += performance.now() - t;
    stages.selected += count;
    if (count === 0) continue;
    t = performance.now();
    const rows = selection.subarray(0, count);
    const dense = way === "before" || prefersDense(count, group.rows);
    for (const name of project) {
      const chunk = group.chunks.get(name) as ColumnChunk;
      const column = await ChunkReader.open(view, chunk, options);
      if (way === "mask") {
        await column.load(allPages(column));
        const target = reusedTarget(name, column, group.rows, column.varlenBytes(0, group.rows));
        column.decodeRange(0, group.rows, target, 0);
        stages.outputBytes += outputBytes(target, group.rows);
        stages.check += maskedSum(target, mask, group.rows);
      } else if (dense) {
        await column.load(allPages(column));
        const target = reusedTarget(name, column, group.rows, column.varlenBytes(0, group.rows));
        column.decodeRange(0, group.rows, target, 0);
        stages.outputBytes += outputBytes(target, group.rows);
        if (aggregate) stages.check += sumOf(target, way === "before" ? rows : rows, count);
      } else {
        await column.load(column.pagesForRows(rows));
        const target = reusedTarget(name, column, count, column.gatherBytes(rows));
        column.gather(rows, target, 0);
        stages.outputBytes += outputBytes(target, count);
        if (aggregate) stages.check += sumOf(target, null, count);
      }
      column.close();
    }
    stages.materializeMs += performance.now() - t;
  }
  const used = process.cpuUsage(cpu);
  stages.cpuMs = (used.user + used.system) / 1000;
  return stages;
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

async function measure(
  label: string,
  filter: string,
  predicate: ColumnPredicate,
  project: readonly string[],
  aggregate = false,
): Promise<void> {
  if (ONLY !== null && !label.includes(ONLY)) return;
  const warm = readerView(space.view({ cache: true }));
  for (const way of (aggregate ? ["before", "after", "mask"] : ["before", "after"]) as Way[]) {
    if (WAY !== null && way !== WAY) continue;
    await scan(warm, filter, predicate, project, way, aggregate); // warm-up: pages cached
    const runs: Stages[] = [];
    for (let k = 0; k < REPEATS; k++)
      runs.push(await scan(warm, filter, predicate, project, way, aggregate));
    const cold = await scan(
      space.view({ cache: false }),
      filter,
      predicate,
      project,
      way,
      aggregate,
    );
    const filterMs = median(runs.map((run) => run.filterMs));
    const materializeMs = median(runs.map((run) => run.materializeMs));
    const total = median(runs.map((run) => run.filterMs + run.materializeMs));
    const first = runs[0] as Stages;
    const mb = (bytes: number, ms: number): string =>
      ms > 0 ? (bytes / 1e6 / (ms / 1000)).toFixed(0) : "–";
    console.log(
      [
        label,
        way,
        String(first.selected),
        total.toFixed(1),
        `${filterMs.toFixed(1)} (${((filterMs * 1000) / Math.max(1, first.filterPages)).toFixed(1)} µs/page, ${String(first.filterPages)} pages)`,
        `${materializeMs.toFixed(1)} (${mb(first.outputBytes, materializeMs)} MB/s)`,
        median(runs.map((run) => run.cpuMs)).toFixed(0),
        (cold.filterMs + cold.materializeMs).toFixed(1),
        aggregate ? first.check.toFixed(0) : "",
      ].join(" | "),
    );
  }
}

// ── Gather or dense: the crossover, per projected column, measured on these pages ────────────

async function crossover(): Promise<void> {
  const warm = space.view({ cache: true });
  const group = groups[0] as Group;
  console.log("density | gather ms (30 columns, one group) | dense ms | gather / dense");
  for (const density of [0.001, 0.01, 0.1, 0.3, 0.5, 0.6, 0.7, 0.8, 0.9, 1]) {
    const step = Math.max(1, Math.round(1 / density));
    const rows = Uint16Array.from({ length: Math.floor(group.rows / step) }, (_, k) => k * step);
    const time = async (dense: boolean): Promise<number> => {
      const samples: number[] = [];
      for (let r = 0; r < REPEATS + 1; r++) {
        const t = performance.now();
        for (const name of C30) {
          const column = await ChunkReader.open(
            warm,
            group.chunks.get(name) as ColumnChunk,
            options,
          );
          if (dense) {
            await column.load(allPages(column));
            const target = targetFor(column, group.rows, column.varlenBytes(0, group.rows));
            column.decodeRange(0, group.rows, target, 0);
          } else {
            await column.load(column.pagesForRows(rows));
            const target = targetFor(column, rows.length, column.gatherBytes(rows));
            column.gather(rows, target, 0);
          }
          column.close();
        }
        if (r > 0) samples.push(performance.now() - t);
      }
      return median(samples);
    };
    const gather = await time(false);
    const dense = await time(true);
    console.log(
      `${String(density * 100)}% | ${gather.toFixed(2)} | ${dense.toFixed(2)} | ${(gather / dense).toFixed(2)}`,
    );
  }
}

/** Per projected column: µs to open, load the pages holding 0.1 % of the rows, and gather them. */
async function perColumn(): Promise<void> {
  const warm = space.view({ cache: true });
  const group = groups[0] as Group;
  const rows = Uint16Array.from({ length: Math.floor(group.rows / 1000) }, (_, k) => k * 1000 + 7);
  const parts = { open: 0, load: 0, gather: 0 };
  for (const name of C30) {
    const samples = {
      open: [] as number[],
      load: [] as number[],
      gather: [] as number[],
      pages: 0,
    };
    for (let r = 0; r < REPEATS + 3; r++) {
      let t = performance.now();
      const column = await ChunkReader.open(warm, group.chunks.get(name) as ColumnChunk, options);
      const opened = performance.now() - t;
      t = performance.now();
      const pages = column.pagesForRows(rows);
      await column.load(pages);
      const loaded = performance.now() - t;
      t = performance.now();
      const target = targetFor(column, rows.length, column.gatherBytes(rows));
      column.gather(rows, target, 0);
      const gathered = performance.now() - t;
      column.close();
      if (r >= 3) {
        samples.open.push(opened * 1000);
        samples.load.push(loaded * 1000);
        samples.gather.push(gathered * 1000);
        samples.pages = pages.length;
      }
    }
    const o = median(samples.open);
    const l = median(samples.load);
    const g = median(samples.gather);
    parts.open += o;
    parts.load += l;
    parts.gather += g;
    console.log(
      `${name} | ${String(samples.pages)} pages | open ${o.toFixed(0)} µs | load ${l.toFixed(0)} µs (${(l / Math.max(1, samples.pages)).toFixed(1)}/page) | gather ${g.toFixed(0)} µs`,
    );
  }
  console.log(
    `30 columns, one group: open ${parts.open.toFixed(0)} µs, load ${parts.load.toFixed(0)} µs, gather ${parts.gather.toFixed(0)} µs`,
  );
}

if (args.includes("--per-column")) {
  await perColumn();
  process.exit(0);
}
if (ONLY === null || CROSSOVER) await crossover();
if (CROSSOVER) process.exit(0);
console.log(
  `\nwarm medians of ${String(REPEATS)}, ${String(groups.length)} groups; dense at ≥ DENSE_SELECTION of a group`,
);
console.log(
  "query | way | rows | total ms | filter ms | materialize ms | CPU ms | cold total ms | check",
);
const project = C30;
await measure("selective_30col i09 = 7 (0.1%)", "i09", { op: "=", value: 7 }, project);
await measure("selective_30col i09 < 10 (1%)", "i09", { op: "<", value: 10 }, project);
await measure("selective_30col i09 < 100 (10%)", "i09", { op: "<", value: 100 }, project);
await measure("selective_30col i09 < 1000 (100%)", "i09", { op: "<", value: 1000 }, project);
await measure(
  "selective_30col id < 0.1% (clustered)",
  "id",
  { op: "<", value: BigInt(Math.ceil(ROWS / 1000)) },
  project,
);
await measure(
  "proj3_agg i07 < 500000 → SUM(f03), AVG(i05)",
  "i07",
  { op: "<", value: 500_000 },
  ["f03", "i05"],
  true,
);
