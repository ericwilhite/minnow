/** The speed-claim protocol for next/: the one standard for any "faster" or "not slower" claim.
 *
 *   node scripts/next-claim.mts manifest --control <sha> --candidate <sha> --datasets W1M,S1
 *        --shards 6 --pairs 15 --targets a,b [--only a,b,c] --output manifest.json
 *   node scripts/next-claim.mts run --manifest manifest.json --dataset W1M --shard 0/6
 *        --control <built checkout> --candidate <built checkout> --output <dir>
 *   node scripts/next-claim.mts report --manifest manifest.json <claim-*.json>... > table.md
 *
 * The manifest, written at dispatch, declares the run: both SHAs, the datasets, shards, cells,
 * targets and pairs. A table gives verdicts only when every declared shard and cell reported
 * every pair; Holm runs over the declared screened family; one void A/A voids the whole run.
 *
 * Protocol (also in the testing reference, "Speed claims for the rebuild engine"):
 * 1. Each arm builds its own fixture once, with its own writer, and captures it as a native
 *    storage image. The control arm also times each cell once to size its bracket.
 * 2. A bracket is a fixed number of complete queries per cell, ceil(1000 ms / one query),
 *    between 1 and 10, identical for both arms.
 * 3. Every sample is a fresh process: restore the arm's image, open, verify the full answer
 *    (lib/answers.mts), time one bracket of process CPU (all threads, natural GC), verify
 *    again. Pair order is a balanced AB/BA sequence shuffled from a recorded seed. Both arms
 *    must return the same answers, or the cell has no verdict.
 * 4. A/A first: fixed control cells run the control build against an independent control
 *    process. If any A/A cell meets the target rule below, the whole run is void.
 * 5. Target cells, declared before the run: a gain needs -median > IQR, at least 7 of 9 pairs
 *    (scaled) favouring the candidate and a per-query saving of at least MIN_EFFECT of the
 *    control median; a regression is the reverse. Every other cell is a regression screen:
 *    a flag needs the regression rule and a Holm-adjusted two-sided sign test p <= 0.05 across
 *    all screened cells of the run. Anything else is "no accepted difference", not equality.
 * Every sample records the runner's CPU model. Shards split the cells across jobs.
 * Without --manifest, `run` declares a one-dataset manifest from its own arguments.
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import {
  runIndependentCPU,
  startCPUWorker,
  type CPUArm,
  type Verification,
} from "./lib/independent-cpu.mts";
import { compareAnswers, type AnswerDigest } from "./lib/answers.mts";
import { gateBCells } from "./head-to-head.mts";
import type { HeadToHeadData } from "./head-to-head-worker.mts";

type Dataset = "W1M" | "S1";
type Role = "control" | "candidate";
/** A/A control cells, fixed per dataset, including one the single-process screen misread. */
export const CONTROL_CELLS: Record<Dataset, readonly string[]> = {
  W1M: ["scan_proj3_agg", "agg_3dims_20aggs"],
  S1: ["S_q3", "S_q8"],
};
/** The smallest relative per-query difference a verdict may report. */
export const MIN_EFFECT = 0.02;
const repository = resolve(import.meta.dirname, "..");
const worker = fileURLToPath(new URL("./head-to-head-worker.mts", import.meta.url));
const execArgv = ["--max-old-space-size=6144"] as const;

export const bracketRepeats = (oneQueryMs: number): number =>
  Math.min(10, Math.max(1, Math.ceil(1000 / Math.max(oneQueryMs, 1e-3))));

/** Exact two-sided sign test over nonzero paired deltas. */
export function signTestP(favoursCandidate: number, favoursControl: number): number {
  const n = favoursCandidate + favoursControl,
    k = Math.max(favoursCandidate, favoursControl);
  if (n === 0) return 1;
  let tail = 0,
    choose = 1;
  for (let i = 0; i <= n; i++) {
    if (i >= k) tail += choose;
    choose = (choose * (n - i)) / (i + 1);
  }
  return Math.min(1, (2 * tail) / 2 ** n);
}

/** Holm step-down adjustment of p-values. */
export function holm(ps: readonly number[]): number[] {
  const order = ps.map((p, index) => ({ p, index })).sort((x, y) => x.p - y.p);
  const adjusted = new Array<number>(ps.length).fill(1);
  let running = 0;
  order.forEach(({ p, index }, rank) => {
    running = Math.max(running, Math.min(1, (ps.length - rank) * p));
    adjusted[index] = running;
  });
  return adjusted;
}

/** A balanced AB/BA sequence for `pairs` pairs, shuffled from `seed` (xorshift32). */
export function balancedOrders(pairs: number, seed: number): Array<readonly [Role, Role]> {
  const orders: Array<readonly [Role, Role]> = [];
  for (let i = 0; i < pairs; i++)
    orders.push(i % 2 === 0 ? ["control", "candidate"] : ["candidate", "control"]);
  let state = seed >>> 0 || 1;
  for (let i = orders.length - 1; i > 0; i--) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    const j = state % (i + 1);
    const a = orders[i],
      b = orders[j];
    if (a !== undefined && b !== undefined) {
      orders[i] = b;
      orders[j] = a;
    }
  }
  return orders;
}

interface Acceptance {
  readonly medianDeltaMs: number;
  readonly iqrDeltaMs: number;
  readonly favoursCandidate: number;
  readonly favoursControl: number;
  readonly acceptedGain: boolean;
  readonly significantRegression: boolean;
}
export interface CellRecord {
  readonly cell: string;
  readonly target?: boolean;
  /** Image A/A only: which independent replicate capture the control ran against (1-based). */
  readonly replicate?: number;
  readonly repeats: number;
  /** Pairs with both arms timed. */
  readonly completedPairs?: number;
  readonly acceptance?: Acceptance;
  /** Exact two-sided sign test p over this cell's pairs (before any correction). */
  readonly signP?: number;
  readonly controlMedianMs?: number;
  readonly candidateMedianMs?: number;
  readonly cpus?: string[];
  readonly error?: string;
}
export interface ClaimResult {
  readonly dataset: Dataset;
  readonly shard: string;
  readonly started: string;
  readonly finished: string;
  readonly control: { root: string; revision: string };
  readonly candidate: { root: string; revision: string };
  readonly node: string;
  readonly pairs: number;
  readonly seed: number;
  readonly targets: string[];
  readonly repeats: Record<string, number>;
  readonly aa: CellRecord[];
  /**
   * Separate images only: the control build on its image vs on each independent replicate
   * capture, for the A/A controls, this shard's targets and every cell with a raw verdict.
   */
  readonly imageAa?: CellRecord[];
  /** Which fixture image each arm restored (digest of its files), and why. */
  readonly images?: ClaimImages;
  readonly ab: CellRecord[];
  readonly void: boolean;
  /** manifestDigest of the manifest this shard ran under: binds the artefact to its run. */
  readonly manifest: string;
}

/** The run declared at dispatch: what a complete run must report. */
/** A captured fixture image: SHA-256 over its files' names, sizes and hashes, and total bytes. */
export interface ImageFacts {
  readonly digest: string;
  readonly bytes: number;
}
/** What decides whether one image serves both builds (formatFacts), each null when unknown. */
export interface FormatFacts {
  /** The declared format registry (formatIdentity): the authority for compatibility. */
  readonly registry: string | null;
  /** The writer's and codecs' implementation (writerFingerprint): conservative. */
  readonly writer: string | null;
}
export interface ClaimImages {
  /** shared: both arms restored the control's image; separate: each arm its own capture. */
  readonly mode: "shared" | "separate";
  readonly formats: { readonly control: FormatFacts; readonly candidate: FormatFacts };
  readonly control: ImageFacts;
  readonly candidate: ImageFacts;
  /** Separate mode: the control build's further independent captures (IMAGE_REPLICATES). */
  readonly replicates?: readonly ImageFacts[];
  /** Shared mode: the candidate opened the control's image without writing its durable files. */
  readonly candidateOpened?: boolean;
}
/** Separate mode: independent control captures beside the control's own, for the image A/A. */
export const IMAGE_REPLICATES = 2;

export interface ClaimManifest {
  readonly control: string;
  readonly candidate: string;
  /**
   * auto: both arms restore one image when the builds' stored formats are identical (else
   * separate); separate: each arm its own image, with an image A/A (a claim about stored layout).
   */
  readonly images: "auto" | "separate";
  readonly pairs: number;
  readonly targets: readonly string[];
  readonly datasets: ReadonlyArray<{
    readonly dataset: Dataset;
    readonly shards: number;
    readonly controls: readonly string[];
    /** Every selected cell; shard i measures the cells whose index % shards === i. */
    readonly cells: readonly string[];
  }>;
}

export const shardCells = (cells: readonly string[], index: number, count: number): string[] =>
  cells.filter((_, i) => i % count === index);

const CELL = /^[A-Za-z0-9_]+$/u;
const SHA = /^[0-9a-f]{40}$/u;

/** The manifest's fields in a fixed order (its digest is over exactly this). */
function canonicalManifest(m: ClaimManifest): ClaimManifest {
  return {
    control: m.control,
    candidate: m.candidate,
    images: m.images,
    pairs: m.pairs,
    targets: [...m.targets],
    datasets: m.datasets.map((d) => ({
      dataset: d.dataset,
      shards: d.shards,
      controls: [...d.controls],
      cells: [...d.cells],
    })),
  };
}

/** SHA-256 of the canonical manifest: every result artefact carries it. */
export function manifestDigest(m: ClaimManifest): string {
  return createHash("sha256")
    .update(JSON.stringify(canonicalManifest(m)))
    .digest("hex");
}

/**
 * A build's stored-format identity: SHA-256 over every non-function export of its compiled
 * `next/durable-format.js` (reader and writer versions, the durable, private and conditional
 * format registries), or null when the build has none. Two builds with the same identity read and
 * write the same stored format, so one fixture image serves both.
 */
export async function formatIdentity(root: string): Promise<string | null> {
  let module: Record<string, unknown>;
  try {
    module = (await import(
      pathToFileURL(join(root, "packages/core/dist/next/durable-format.js")).href
    )) as Record<string, unknown>;
  } catch {
    return null;
  }
  const declared = Object.keys(module)
    .filter((name) => typeof module[name] !== "function")
    .sort()
    .map((name) => [name, module[name]]);
  if (declared.length === 0) return null;
  const text = JSON.stringify(declared, (_key, value: unknown) =>
    typeof value === "bigint" ? `${value.toString()}n` : value,
  );
  return createHash("sha256").update(text).digest("hex");
}

/** Compiled modules whose code decides a fixture image's bytes: the write path the fixture
 * loads through (test-support's Harness: writer, page encoders, format codecs, storage) and the
 * input builder that encodes its values. Their static and literal dynamic imports are followed.
 */
export const WRITER_ENTRIES = ["next/write/test-support.js", "next/testing/executor-input.js"];

/**
 * A conservative fingerprint of a build's writer: SHA-256 over the path and contents of every
 * compiled module reachable from WRITER_ENTRIES inside `packages/core/dist`, or null when the
 * build has none. Any change there (an encoder, a codec, a comment) changes it, even when the
 * declared format registry does not; two builds share an image only when both agree.
 */
export async function writerFingerprint(root: string): Promise<string | null> {
  const dist = join(root, "packages/core/dist");
  const seen = new Map<string, string>();
  const pending = WRITER_ENTRIES.map((entry) => join(dist, entry));
  const reference = /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s+)["'](\.{1,2}\/[^"']+)["']/gu;
  while (pending.length > 0) {
    const file = pending.pop() as string;
    if (seen.has(file)) continue;
    let text: string;
    try {
      text = await readFile(file, "utf8");
    } catch {
      return null;
    }
    seen.set(file, createHash("sha256").update(text).digest("hex"));
    for (const match of text.matchAll(reference))
      pending.push(resolve(dirname(file), match[1] as string));
  }
  const entries = [...seen]
    .map(([file, hash]) => [relative(dist, file), hash] as const)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return createHash("sha256").update(JSON.stringify(entries)).digest("hex");
}

export async function formatFacts(root: string): Promise<FormatFacts> {
  return { registry: await formatIdentity(root), writer: await writerFingerprint(root) };
}

/** One image serves both builds only when both facts are known and equal. */
export const sameFormat = (a: FormatFacts, b: FormatFacts): boolean =>
  a.registry !== null && a.writer !== null && a.registry === b.registry && a.writer === b.writer;

/** The image mode a run must use for its manifest and builds. */
export const imageMode = (
  images: ClaimManifest["images"],
  formats: ClaimImages["formats"],
): ClaimImages["mode"] =>
  images === "auto" && sameFormat(formats.control, formats.candidate) ? "shared" : "separate";

/** The cells a separate-mode shard's image A/A must cover: its A/A controls, its targets, and
 * every cell whose A/B met the target rule either way (a superset of the screen's flags). */
export function imageAaCells(
  controls: readonly string[],
  targets: readonly string[],
  ab: readonly CellRecord[],
): string[] {
  const cells = new Set(controls);
  for (const r of ab)
    if (targets.includes(r.cell) || (r.error === undefined && targetVerdict(r) !== "none"))
      cells.add(r.cell);
  return [...cells];
}

/** A captured image's digest (its manifest's files: name, size, SHA-256) and total bytes. */
export async function imageFacts(directory: string): Promise<ImageFacts> {
  const image = JSON.parse(await readFile(join(directory, "manifest.json"), "utf8")) as {
    files: Array<{ name: string; size: number; sha256: string }>;
  };
  const files = [...image.files]
    .map((file) => ({ name: file.name, size: file.size, sha256: file.sha256 }))
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  return {
    digest: createHash("sha256").update(JSON.stringify(files)).digest("hex"),
    bytes: files.reduce((sum, file) => sum + file.size, 0),
  };
}

/**
 * Checks a loaded manifest against the schema: both SHAs, 9 to 101 pairs, 1 to 32 shards per
 * dataset, the mandatory A/A controls of each dataset, unique Gate B cells and declared targets.
 */
export async function validateManifest(raw: unknown): Promise<ClaimManifest> {
  const fail = (message: string): never => {
    throw new Error(`Invalid claim manifest: ${message}`);
  };
  const record = (value: unknown, what: string): Record<string, unknown> =>
    value !== null && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : fail(`${what} is not an object`);
  const names = (value: unknown, what: string): string[] => {
    if (!Array.isArray(value) || !value.every((v) => typeof v === "string" && CELL.test(v)))
      return fail(`${what} must be a list of cell names`);
    const list = value as string[];
    if (new Set(list).size !== list.length) fail(`${what} repeats a cell`);
    return list;
  };
  const m = record(raw, "the manifest");
  const keys = ["candidate", "control", "datasets", "images", "pairs", "targets"];
  if (JSON.stringify(Object.keys(m).sort()) !== JSON.stringify(keys))
    fail(`fields must be exactly ${keys.join(", ")}`);
  const control = m.control,
    candidate = m.candidate,
    pairs = m.pairs;
  if (typeof control !== "string" || typeof candidate !== "string")
    return fail("control and candidate must be commit SHAs");
  if (!SHA.test(control) || !SHA.test(candidate) || control === candidate)
    fail("control and candidate must be two different full commit SHAs");
  if (typeof pairs !== "number" || !Number.isSafeInteger(pairs) || pairs < 9 || pairs > 101)
    fail("pairs must be an integer from 9 to 101");
  const images = m.images;
  if (images !== "auto" && images !== "separate") return fail("images must be auto or separate");
  const targets = names(m.targets, "targets");
  if (!Array.isArray(m.datasets) || m.datasets.length === 0) return fail("no datasets");
  const datasets: Array<ClaimManifest["datasets"][number]> = [];
  for (const raw of m.datasets as unknown[]) {
    const d = record(raw, "a dataset");
    const dataset = d.dataset;
    if (dataset !== "W1M" && dataset !== "S1") return fail(`unknown dataset ${String(dataset)}`);
    if (datasets.some((e) => e.dataset === dataset)) fail(`${dataset} declared twice`);
    const shards = d.shards;
    if (typeof shards !== "number" || !Number.isSafeInteger(shards) || shards < 1 || shards > 32)
      fail(`${dataset} shards must be an integer from 1 to 32`);
    const controls = names(d.controls, `${dataset} controls`);
    if (JSON.stringify(controls) !== JSON.stringify(CONTROL_CELLS[dataset]))
      fail(`${dataset} must run its A/A controls ${CONTROL_CELLS[dataset].join(", ")}`);
    const cells = names(d.cells, `${dataset} cells`);
    if (cells.length === 0) fail(`${dataset} has no cells`);
    const known = new Set(await gateBCells(dataset));
    for (const cell of cells) if (!known.has(cell)) fail(`${cell} is not a ${dataset} Gate B cell`);
    datasets.push({ dataset, shards: shards as number, controls, cells });
  }
  for (const target of targets)
    if (!datasets.some((d) => d.cells.includes(target))) fail(`target ${target} is not a cell`);
  return canonicalManifest({
    control: control as string,
    candidate: candidate as string,
    images,
    pairs: pairs as number,
    targets,
    datasets,
  });
}

/** Validates the dispatch inputs and declares the run. */
export async function claimManifest(options: {
  control: string;
  candidate: string;
  datasets: readonly string[];
  shards: number;
  pairs: number;
  targets: readonly string[];
  only: readonly string[];
  images?: string;
}): Promise<ClaimManifest> {
  const images = options.images ?? "auto";
  if (images !== "auto" && images !== "separate")
    throw new Error("images must be auto or separate");
  if (!SHA.test(options.control) || !SHA.test(options.candidate))
    throw new Error("control and candidate must be full commit SHAs");
  if (options.control === options.candidate)
    throw new Error("A claim compares two different builds; an A/A run uses one build");
  if (!Number.isSafeInteger(options.pairs) || options.pairs < 9 || options.pairs > 101)
    throw new Error("pairs must be an integer from 9 to 101");
  if (!Number.isSafeInteger(options.shards) || options.shards < 1 || options.shards > 32)
    throw new Error("shards must be an integer from 1 to 32");
  for (const name of [...options.targets, ...options.only])
    if (!CELL.test(name)) throw new Error(`Invalid cell name ${JSON.stringify(name)}`);
  if (options.datasets.length === 0) throw new Error("no datasets");
  const only = new Set(options.only);
  const datasets: Array<ClaimManifest["datasets"][number]> = [];
  const known = new Set<string>();
  for (const name of options.datasets) {
    if (name !== "W1M" && name !== "S1") throw new Error(`Unknown dataset ${name}`);
    const all = await gateBCells(name);
    for (const cell of all) known.add(cell);
    const cells = all.filter((id) => only.size === 0 || only.has(id));
    if (cells.length === 0) throw new Error(`No ${name} cells selected`);
    datasets.push({ dataset: name, shards: options.shards, controls: CONTROL_CELLS[name], cells });
  }
  for (const cell of only) if (!known.has(cell)) throw new Error(`Unknown cell ${cell}`);
  for (const target of options.targets)
    if (!datasets.some((entry) => entry.cells.includes(target)))
      throw new Error(`Target ${target} is not a selected cell`);
  return validateManifest({
    control: options.control,
    candidate: options.candidate,
    images,
    pairs: options.pairs,
    targets: [...options.targets],
    datasets,
  });
}

/** Target rule: the paired gain/regression rule plus the minimum per-query effect. */
export function targetVerdict(r: CellRecord): "gain" | "regression" | "none" {
  const a = r.acceptance;
  if (a === undefined || r.controlMedianMs === undefined) return "none";
  const big = Math.abs(a.medianDeltaMs) / r.repeats >= MIN_EFFECT * r.controlMedianMs;
  if (a.acceptedGain && big) return "gain";
  if (a.significantRegression && big) return "regression";
  return "none";
}

const brief = (error: unknown): string =>
  (error instanceof Error ? error.message : String(error)).split("\n", 1).join("").slice(0, 300);
const medianOf = (values: readonly number[]): number => {
  const sorted = [...values].sort((a, b) => a - b),
    middle = sorted.length >>> 1;
  return sorted.length % 2 === 1
    ? (sorted[middle] ?? NaN)
    : ((sorted[middle - 1] ?? NaN) + (sorted[middle] ?? NaN)) / 2;
};

async function run(): Promise<void> {
  const { values } = parseArgs({
    args: process.argv.slice(3),
    options: {
      dataset: { type: "string", default: "W1M" },
      control: { type: "string" },
      candidate: { type: "string" },
      output: { type: "string" },
      shard: { type: "string", default: "0/1" },
      manifest: { type: "string" },
      pairs: { type: "string", default: "15" },
      targets: { type: "string", default: "" },
      seed: { type: "string", default: "" },
      "verify-only": { type: "boolean", default: false },
      only: { type: "string", default: "" },
      rows: { type: "string", default: "1000000" },
      scale: { type: "string", default: "333" },
      images: { type: "string", default: "auto" },
    },
  });
  const dataset = values.dataset as Dataset;
  if (dataset !== "W1M" && dataset !== "S1") throw new Error("--dataset must be W1M or S1");
  if (values.control === undefined || values.candidate === undefined || values.output === undefined)
    throw new Error("--control, --candidate and --output are required");
  const [shardIndex, shardCount] = values.shard.split("/").map(Number);
  if (
    shardIndex === undefined ||
    shardCount === undefined ||
    !Number.isSafeInteger(shardIndex) ||
    !Number.isSafeInteger(shardCount) ||
    shardIndex < 0 ||
    shardIndex >= shardCount
  )
    throw new Error("--shard must be i/n with 0 <= i < n");
  const rows = Number(values.rows),
    scale = Number(values.scale),
    seed = values.seed === "" ? Math.floor(Math.random() * 2 ** 31) : Number(values.seed);
  if (!Number.isSafeInteger(seed)) throw new Error("--seed must be an integer");
  const roots = { control: resolve(values.control), candidate: resolve(values.candidate) };
  const revisionOf = (root: string) =>
    execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  const revisions = { control: revisionOf(roots.control), candidate: revisionOf(roots.candidate) };
  const manifest: ClaimManifest =
    values.manifest === undefined
      ? await claimManifest({
          control: revisions.control,
          candidate: revisions.candidate,
          datasets: [dataset],
          shards: shardCount,
          pairs: Number(values.pairs),
          targets: values.targets.split(",").filter(Boolean),
          only: values.only.split(",").filter(Boolean),
          images: values.images,
        })
      : await validateManifest(JSON.parse(await readFile(resolve(values.manifest), "utf8")));
  if (manifest.control !== revisions.control || manifest.candidate !== revisions.candidate)
    throw new Error("The built checkouts are not the manifest's control and candidate");
  const entry = manifest.datasets.find((d) => d.dataset === dataset);
  if (entry === undefined) throw new Error(`${dataset} is not in the manifest`);
  if (entry.shards !== shardCount) throw new Error("--shard count differs from the manifest");
  const pairs = manifest.pairs,
    targets = [...manifest.targets];
  const cells = shardCells(entry.cells, shardIndex, shardCount);
  const controls = entry.controls;
  const needed = [...new Set([...controls, ...cells])];
  const catalogue = (await import(
    pathToFileURL(join(repository, "scripts/next-executor-workload/queries.mjs")).href
  )) as {
    W_QUERIES: Array<{ id: string; ordered?: boolean }>;
    S_QUERIES: Array<{ id: string; ordered?: boolean }>;
  };
  const ordered = new Set(
    [...catalogue.W_QUERIES, ...catalogue.S_QUERIES]
      .filter((q) => q.ordered === true)
      .map((q) => q.id),
  );
  const output = resolve(values.output);
  await mkdir(join(output, "images"), { recursive: true });
  const started = new Date().toISOString();
  const log = (event: object) => console.error(JSON.stringify(event));
  const base = { dataset, rows, scale, capBytes: 256 * 1024 * 1024, deadlineMs: 600_000 };

  // 1. Fixture images. Builds with the same stored format and writer restore ONE image (the
  // control's): two captures of the same rows differ in physical layout (the writer's flush and
  // merge timing), which would bias every cell, and an A/A on one image cannot see it. A build
  // that changes either (or a manifest that asks for separate images) restores its own capture,
  // and the control's independent replicate captures run an image A/A beside it.
  const formats: ClaimImages["formats"] = {
    control: await formatFacts(roots.control),
    candidate: await formatFacts(roots.candidate),
  };
  const mode = imageMode(manifest.images, formats);
  log({ phase: "images", mode, formats });
  type ImageRole = "control" | "candidate" | `replicate${string}`;
  const replicateRoles = Array.from(
    { length: IMAGE_REPLICATES },
    (_, i): ImageRole => `replicate${String(i + 1)}`,
  );
  const imageDirectory = (image: ImageRole) => join(output, "images", image);
  // The build that captures each image, and the identity it is captured (and restored) under.
  const captures: ImageRole[] =
    mode === "shared" ? ["control"] : ["control", "candidate", ...replicateRoles];
  const builtBy = (image: ImageRole): Role => (image === "candidate" ? "candidate" : "control");
  const identityOf = (image: ImageRole) => `${image}:${revisions[builtBy(image)]}`;
  let times: Record<string, number> = {};
  for (const image of captures) {
    const arm = builtBy(image);
    const began = performance.now();
    const data: HeadToHeadData = { ...base, engine: "next", root: roots[arm], workloads: needed };
    const handle = await startCPUWorker(
      { module: worker, cwd: repository, revision: revisions[arm], data, execArgv },
      { arm, cohort: -1, workload: "image", boundary: "frames" },
    );
    try {
      await handle.ask({
        op: "command",
        name: "capture-image",
        data: { directory: imageDirectory(image), identity: identityOf(image) },
      });
      if (image === "control")
        times = await handle.ask<Record<string, number>>({ op: "command", name: "time-cells" });
    } finally {
      await handle.close();
    }
    log({ phase: "image", image, arm, seconds: (performance.now() - began) / 1000 });
  }
  const restored: Record<Role, ImageRole> = {
    control: "control",
    candidate: mode === "shared" ? "control" : "candidate",
  };
  const replicates: ImageFacts[] = [];
  if (mode === "separate")
    for (const image of replicateRoles) replicates.push(await imageFacts(imageDirectory(image)));
  const facts = {
    mode,
    formats,
    control: await imageFacts(imageDirectory(restored.control)),
    candidate: await imageFacts(imageDirectory(restored.candidate)),
    ...(mode === "separate" ? { replicates } : {}),
  };
  const repeats = Object.fromEntries(needed.map((id) => [id, bracketRepeats(times[id] ?? 0)]));
  log({ phase: "brackets", repeats, seed });
  const specOn = (arm: Role, image: ImageRole): CPUArm => ({
    module: worker,
    cwd: repository,
    revision: revisions[arm],
    data: {
      ...base,
      engine: "next",
      root: roots[arm],
      workloads: needed,
      image: imageDirectory(image),
      imageIdentity: identityOf(image),
      repeats,
    } satisfies HeadToHeadData,
    execArgv,
  });
  const spec = (arm: Role): CPUArm => specOn(arm, restored[arm]);
  // Shared mode: the candidate must open the control's image as it is. Every restored process
  // refuses durable writes during open; this checks it once, before anything is measured.
  let candidateOpened = false;
  if (mode === "shared") {
    const handle = await startCPUWorker(spec("candidate"), {
      arm: "candidate",
      cohort: -1,
      workload: "open-check",
      boundary: "frames",
    });
    try {
      const opened = await handle.ask<{ durableWrites: number | null }>({
        op: "command",
        name: "opened",
      });
      if (opened.durableWrites !== 0)
        throw new Error(
          `The candidate wrote the shared image's durable files on open (${String(opened.durableWrites)})`,
        );
      candidateOpened = true;
    } finally {
      await handle.close();
    }
  }
  const imageRecord: ClaimImages = { ...facts, ...(mode === "shared" ? { candidateOpened } : {}) };
  log({ phase: "image-facts", ...imageRecord });
  if (values["verify-only"]) {
    // Answers only: one fresh process per arm and cell, compared in full, no timing.
    const lines = [
      `## Answer check: control \`${revisions.control.slice(0, 10)}\` vs candidate \`${revisions.candidate.slice(0, 10)}\` (${dataset} shard ${values.shard})`,
      "",
      "| Cell | rows (control/candidate) | method | answers |",
      "|---|---:|---|---|",
    ];
    let different = 0;
    for (const cell of needed) {
      const digests: Partial<Record<Role, AnswerDigest>> = {};
      let failure = "";
      for (const arm of ["control", "candidate"] as const) {
        const handle = await startCPUWorker(spec(arm), {
          arm,
          cohort: -1,
          workload: cell,
          boundary: "frames",
        });
        try {
          const checked = await handle.ask<Verification & { answer: AnswerDigest }>({
            op: "verify",
            workload: cell,
          });
          digests[arm] = checked.answer;
        } catch (error) {
          failure = `${arm}: ${brief(error)}`;
        } finally {
          await handle.close();
        }
      }
      const a = digests.control,
        b = digests.candidate;
      const comparison =
        a !== undefined && b !== undefined ? compareAnswers(a, b, ordered.has(cell)) : undefined;
      if (comparison?.same !== true) different++;
      lines.push(
        `| ${dataset}/${cell} | ${String(a?.rows ?? "—")}/${String(b?.rows ?? "—")} | ${comparison?.method ?? "—"} | ${comparison === undefined ? `error: ${failure}` : comparison.same ? "same" : `**different** (${comparison.detail ?? ""})`} |`,
      );
      log({ phase: "verified", cell, comparison, failure });
    }
    lines.push("", `${String(different)} of ${String(needed.length)} cells differ or failed.`, "");
    process.stdout.write(lines.join("\n"));
    if (different > 0) process.exitCode = 1;
    return;
  }
  const measure = async (
    cell: string,
    candidate: CPUArm,
    purpose: "true-parent" | "identical-build-control",
  ): Promise<CellRecord> => {
    const began = performance.now();
    try {
      const result = await runIndependentCPU({
        control: spec("control"),
        candidate,
        workloads: [cell],
        boundaries: ["frames"],
        primaryBoundary: "frames",
        pairs,
        purpose,
        orders: balancedOrders(pairs, seed ^ (needed.indexOf(cell) + 1)),
        // Full answers, floats within the stated tolerance (lib/answers.mts).
        equivalent: (a, b) => {
          const left = (a as Verification & { answer?: AnswerDigest }).answer,
            right = (b as Verification & { answer?: AnswerDigest }).answer;
          return left !== undefined && right !== undefined
            ? compareAnswers(left, right, ordered.has(cell))
            : a.fingerprint === b.fingerprint && a.rows === b.rows;
        },
        onSample: (sample) => log({ cell, purpose, sample }),
      });
      const record = result.records[0];
      if (record === undefined) throw new Error("No paired record");
      const samples = record.samples as Array<{
        arm: string;
        cpuMs: number;
        identity: { identity: { cpu?: string } };
      }>;
      const per = (arm: string) =>
        medianOf(samples.filter((s) => s.arm === arm).map((s) => s.cpuMs)) / (repeats[cell] ?? 1);
      log({ phase: "cell", cell, purpose, seconds: (performance.now() - began) / 1000 });
      const timed = (arm: string) => samples.filter((s) => s.arm === arm).length;
      return {
        cell,
        target: targets.includes(cell),
        repeats: repeats[cell] ?? 1,
        completedPairs: Math.min(timed("control"), timed("candidate")),
        acceptance: record.acceptance,
        signP: signTestP(record.acceptance.favoursCandidate, record.acceptance.favoursControl),
        controlMedianMs: per("control"),
        candidateMedianMs: per("candidate"),
        cpus: [...new Set(samples.map((s) => s.identity.identity.cpu ?? "unknown"))],
      };
    } catch (error) {
      return {
        cell,
        target: targets.includes(cell),
        repeats: repeats[cell] ?? 1,
        error: brief(error),
      };
    }
  };

  // 2. A/A first: the same control build in independent processes.
  const aa: CellRecord[] = [];
  for (const cell of controls)
    aa.push(await measure(cell, spec("control"), "identical-build-control"));
  // 3. A/B on this shard's cells, measured even when void so the evidence is kept.
  const ab: CellRecord[] = [];
  for (const cell of cells) ab.push(await measure(cell, spec("candidate"), "true-parent"));
  // 4. Separate images only: the control build on its own capture vs each independent replicate,
  // on every cell whose verdict the images could bias, so capture-to-capture variation shows.
  const imageAa: CellRecord[] = [];
  if (mode === "separate")
    for (const cell of imageAaCells(controls, targets, ab))
      for (const [index, image] of replicateRoles.entries())
        imageAa.push({
          ...(await measure(cell, specOn("control", image), "identical-build-control")),
          replicate: index + 1,
        });
  const flagged = [...aa, ...imageAa].some(
    (r) => r.error !== undefined || targetVerdict(r) !== "none",
  );
  const result: ClaimResult = {
    dataset,
    shard: values.shard,
    started,
    finished: new Date().toISOString(),
    control: { root: roots.control, revision: revisions.control },
    candidate: { root: roots.candidate, revision: revisions.candidate },
    node: process.version,
    pairs,
    seed,
    targets,
    repeats,
    aa,
    ...(mode === "separate" ? { imageAa } : {}),
    images: imageRecord,
    ab,
    void: flagged,
    manifest: manifestDigest(manifest),
  };
  await writeFile(
    join(output, `claim-${dataset}-${String(shardIndex)}.json`),
    JSON.stringify(result, null, 2),
  );
  process.stdout.write(report([result], manifest));
}

const fmt = (value: number | undefined, digits = 1) =>
  value === undefined || !Number.isFinite(value)
    ? "—"
    : value.toLocaleString("en-US", {
        maximumFractionDigits: digits,
        minimumFractionDigits: digits,
      });

const HEX64 = /^[0-9a-f]{64}$/u;
/** The recorded image facts' shape: a complete ClaimImages, or what is wrong with it. */
export function imageSchemaProblems(images: unknown): string[] {
  if (images === undefined) return ["reported no images"];
  const problems: string[] = [];
  const isRecord = (value: unknown): value is Record<string, unknown> =>
    value !== null && typeof value === "object" && !Array.isArray(value);
  if (!isRecord(images)) return ["reported images that are not an object"];
  if (images.mode !== "shared" && images.mode !== "separate")
    problems.push("reported no image mode");
  const formats = images.formats;
  for (const arm of ["control", "candidate"] as const) {
    const facts = isRecord(formats) ? formats[arm] : undefined;
    if (!isRecord(facts)) problems.push(`reported no format facts for the ${arm}`);
    else
      for (const field of ["registry", "writer"] as const) {
        const value = facts[field];
        if (value !== null && (typeof value !== "string" || !HEX64.test(value)))
          problems.push(`reported an invalid ${arm} ${field} identity`);
        else if (value === null && images.mode === "shared")
          problems.push(`shared mode without the ${arm} ${field} identity`);
      }
  }
  const image = (value: unknown, what: string) => {
    if (!isRecord(value)) problems.push(`reported no ${what} image`);
    else {
      if (typeof value.digest !== "string" || !HEX64.test(value.digest))
        problems.push(`reported no ${what} image digest`);
      if (typeof value.bytes !== "number" || !Number.isSafeInteger(value.bytes) || value.bytes <= 0)
        problems.push(`reported no ${what} image size`);
    }
  };
  image(images.control, "control");
  image(images.candidate, "candidate");
  if (images.mode === "separate") {
    const replicates = images.replicates;
    if (replicates !== undefined && !Array.isArray(replicates))
      problems.push("reported replicates that are not a list");
    else (replicates ?? []).forEach((r: unknown, i) => image(r, `replicate ${String(i + 1)}`));
  } else if (images.replicates !== undefined)
    problems.push("reported replicates on a shared image");
  if (
    images.mode === "shared" &&
    isRecord(formats) &&
    isRecord(formats.control) &&
    isRecord(formats.candidate) &&
    !sameFormat(
      formats.control as unknown as FormatFacts,
      formats.candidate as unknown as FormatFacts,
    ) &&
    problems.length === 0
  )
    problems.push("shared mode with different format facts");
  return problems;
}

/** Everything a complete run must report, checked against the manifest. */
export function claimProblems(results: readonly ClaimResult[], manifest: ClaimManifest): string[] {
  const problems: string[] = [];
  const declared = new Set<string>();
  const shardImages = new Map<string, string>();
  const digest = manifestDigest(manifest);
  for (const result of results)
    if (
      result.manifest !== digest ||
      JSON.stringify(result.targets) !== JSON.stringify(manifest.targets)
    )
      problems.push(`${result.dataset} shard ${result.shard} ran under a different manifest`);
  for (const entry of manifest.datasets)
    for (let index = 0; index < entry.shards; index++) {
      const shard = `${String(index)}/${String(entry.shards)}`;
      declared.add(`${entry.dataset} ${shard}`);
      const found = results.filter((r) => r.dataset === entry.dataset && r.shard === shard);
      if (found.length !== 1) {
        problems.push(
          `${entry.dataset} shard ${shard} ${found.length === 0 ? "missing" : "reported twice"}`,
        );
        continue;
      }
      const result = found[0] as ClaimResult;
      if (
        result.control.revision !== manifest.control ||
        result.candidate.revision !== manifest.candidate
      )
        problems.push(`${entry.dataset} shard ${shard} measured other revisions`);
      if (result.pairs !== manifest.pairs)
        problems.push(`${entry.dataset} shard ${shard} ran ${String(result.pairs)} pairs`);
      const check = (records: readonly CellRecord[], wanted: readonly string[], role: string) => {
        for (const cell of wanted) {
          const record = records.find((r) => r.cell === cell);
          if (record === undefined) problems.push(`${entry.dataset}/${cell} ${role} missing`);
          else if (record.error !== undefined)
            problems.push(`${entry.dataset}/${cell} ${role} failed`);
          else if (record.completedPairs !== manifest.pairs)
            problems.push(
              `${entry.dataset}/${cell} ${role} completed ${String(record.completedPairs ?? 0)}/${String(manifest.pairs)} pairs`,
            );
        }
        for (const record of records)
          if (!wanted.includes(record.cell))
            problems.push(`${entry.dataset}/${record.cell} ${role} was not declared`);
      };
      check(result.aa, entry.controls, "A/A");
      check(result.ab, shardCells(entry.cells, index, entry.shards), "A/B");
      // Both arms restored one image, or the image A/A ran beside them on every cell it covers.
      const where = `${entry.dataset} shard ${shard}`;
      const wrong = imageSchemaProblems(result.images);
      if (wrong.length > 0) {
        problems.push(...wrong.map((problem) => `${where} ${problem}`));
        continue;
      }
      const images = result.images as ClaimImages;
      const modes = JSON.stringify([images.mode, images.formats]);
      shardImages.set(where, modes);
      const same = images.control.digest === images.candidate.digest;
      if (images.mode !== imageMode(manifest.images, images.formats))
        problems.push(
          manifest.images === "separate" && images.mode !== "separate"
            ? `${where} did not use separate images`
            : `${where} used ${images.mode} images, which its format facts do not allow`,
        );
      if (images.mode === "shared") {
        if (!same) problems.push(`${where} shared mode restored two images`);
        if (images.candidateOpened !== true)
          problems.push(`${where} did not check that the candidate opened the shared image as is`);
        if ((result.imageAa ?? []).length > 0)
          problems.push(`${where} ran an image A/A on a shared image`);
      } else {
        const replicates = images.replicates ?? [];
        if (replicates.length < IMAGE_REPLICATES)
          problems.push(
            `${where} arms restored different images without an image A/A (${String(replicates.length)} of ${String(IMAGE_REPLICATES)} replicate captures)`,
          );
        const wanted = imageAaCells(entry.controls, manifest.targets, result.ab);
        for (let replicate = 1; replicate <= IMAGE_REPLICATES; replicate++)
          check(
            (result.imageAa ?? []).filter((r) => r.replicate === replicate),
            wanted,
            `image A/A (replicate ${String(replicate)})`,
          );
        for (const r of result.imageAa ?? [])
          if (r.replicate === undefined || r.replicate < 1 || r.replicate > IMAGE_REPLICATES)
            problems.push(`${entry.dataset}/${r.cell} image A/A names no replicate capture`);
      }
    }
  // Every shard built the same two revisions: one mode and the same format facts throughout.
  if (new Set(shardImages.values()).size > 1)
    problems.push(
      `shards disagree on the image mode or format facts (${[...shardImages.keys()].join(", ")})`,
    );
  for (const result of results)
    if (!declared.has(`${result.dataset} ${result.shard}`))
      problems.push(`${result.dataset} shard ${result.shard} was not declared`);
  return problems;
}

/** How each shard's arms got their fixture image, distinct descriptions joined. */
function imageSummary(results: readonly ClaimResult[]): string {
  const describe = (r: ClaimResult): string => {
    const images = r.images;
    if (images === undefined) return "unreported";
    if (images.mode === "shared")
      return `one image restored in both arms (${images.control.digest.slice(0, 12)}, ${String(images.control.bytes)} B), opened by the candidate without durable writes`;
    const replicates = (images.replicates ?? []).map((r) => r.digest.slice(0, 12)).join(", ");
    return `separate images (control ${images.control.digest.slice(0, 12)}, candidate ${images.candidate.digest.slice(0, 12)}, replicates ${replicates || "missing"}) with an image A/A on the controls, targets and every cell with a verdict`;
  };
  return [...new Set(results.map(describe))].join("; ") || "none reported";
}

/** One table across shards: verdicts only for a complete run, never for a void one. */
export function report(results: readonly ClaimResult[], manifest: ClaimManifest): string {
  const problems = claimProblems(results, manifest);
  const complete = problems.length === 0;
  // One A/A control meeting the target rule (or failing) in any shard voids every verdict.
  const voided = results.filter(
    (r) =>
      r.void ||
      [...r.aa, ...(r.imageAa ?? [])].some(
        (a) => a.error !== undefined || targetVerdict(a) !== "none",
      ),
  );
  // Holm runs over the declared screened family, not only the cells that reported.
  const screened = manifest.datasets.flatMap((entry) =>
    entry.cells
      .filter((cell) => !manifest.targets.includes(cell))
      .map((cell) => `${entry.dataset}/${cell}`),
  );
  const pOf = new Map<string, number>();
  for (const result of results)
    for (const r of result.ab)
      if (r.signP !== undefined) pOf.set(`${result.dataset}/${r.cell}`, r.signP);
  const adjusted = holm(screened.map((key) => pOf.get(key) ?? 1));
  const holmOf = new Map(screened.map((key, index) => [key, adjusted[index] ?? 1]));
  const first = results[0];
  const lines = [
    `## Speed claim: candidate vs control (next/, frames boundary)`,
    "",
    '_Internal measurement protocol (testing reference, "Speed claims"), not a published benchmark._',
    "",
    `control \`${manifest.control.slice(0, 10)}\` · candidate \`${manifest.candidate.slice(0, 10)}\` · Node ${first?.node ?? "—"} · ${String(manifest.pairs)} fresh-process pairs per cell · seed ${first === undefined ? "—" : String(first.seed)} · targets ${manifest.targets.join(", ") || "none"}`,
    "",
    voided.length > 0
      ? `**VOID:** an A/A or image A/A control met the target rule or failed in shard(s) ${voided.map((r) => `${r.dataset} ${r.shard}`).join(", ")}; no verdict from this run may be cited.`
      : "A/A controls neutral in every reported shard.",
    "",
    `Fixture images: ${imageSummary(results)}.`,
    ...(complete
      ? []
      : [
          "",
          `**INCOMPLETE:** no verdicts. ${problems.slice(0, 12).join("; ")}${problems.length > 12 ? `; and ${String(problems.length - 12)} more` : ""}.`,
        ]),
    "",
    `Per-query process CPU medians in ms; Δ is per bracket (candidate − control). Targets: a gain or regression needs |median Δ| > IQR, 7 of 9 pairs (scaled) one way and at least ${String(MIN_EFFECT * 100)}% of the control median. Other cells: a regression flag also needs a Holm-adjusted sign-test p ≤ 0.05 across all ${String(screened.length)} declared screened cells. Anything else is no accepted difference, not equality.`,
    "",
    "| Cell | role | shard | control | candidate | Δ median | IQR | candidate faster | p (Holm for screen) | verdict | CPU |",
    "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
  ];
  for (const result of results) {
    const rows: Array<[string, CellRecord]> = [
      ...result.aa.map((r): [string, CellRecord] => ["A/A", r]),
      ...(result.imageAa ?? []).map((r): [string, CellRecord] => [
        `image A/A r${String(r.replicate ?? "?")}`,
        r,
      ]),
      ...result.ab.map((r): [string, CellRecord] => [
        manifest.targets.includes(r.cell) ? "target" : "screen",
        r,
      ]),
    ];
    for (const [role, r] of rows) {
      const key = `${result.dataset}/${r.cell}`;
      const tv = targetVerdict(r);
      const p = role === "screen" ? holmOf.get(key) : r.signP;
      const verdict =
        r.error !== undefined
          ? `error: ${r.error.replaceAll("|", "\\|").slice(0, 80)}`
          : role === "A/A" || role.startsWith("image A/A")
            ? tv === "none"
              ? "neutral"
              : `**${tv}** (voids the run)`
            : !complete
              ? "no verdict (incomplete run)"
              : voided.length > 0
                ? "void"
                : role === "screen"
                  ? tv === "regression" && (p ?? 1) <= 0.05
                    ? "**regression flag**"
                    : "no accepted difference"
                  : tv === "none"
                    ? "no accepted difference"
                    : `**${tv}**`;
      const a = r.acceptance;
      lines.push(
        `| ${key} | ${role} | ${result.shard} | ${fmt(r.controlMedianMs, 2)} | ${fmt(r.candidateMedianMs, 2)} | ${fmt(a?.medianDeltaMs, 2)} | ${fmt(a?.iqrDeltaMs, 2)} | ${a === undefined ? "—" : `${String(a.favoursCandidate)}/${String(a.favoursCandidate + a.favoursControl)}`} | ${fmt(p, 4)} | ${verdict} | ${(r.cpus ?? []).join(", ")} |`,
      );
    }
  }
  lines.push("");
  return lines.join("\n");
}

const mode = process.argv[2];
if (process.argv[1] !== undefined && resolve(process.argv[1]) !== fileURLToPath(import.meta.url)) {
  // Imported: expose the report and protocol functions only.
} else if (mode === "run") await run();
else if (mode === "manifest") {
  const { values } = parseArgs({
    args: process.argv.slice(3),
    options: {
      control: { type: "string", default: "" },
      candidate: { type: "string", default: "" },
      datasets: { type: "string", default: "W1M,S1" },
      shards: { type: "string", default: "6" },
      pairs: { type: "string", default: "15" },
      targets: { type: "string", default: "" },
      only: { type: "string", default: "" },
      images: { type: "string", default: "auto" },
      output: { type: "string", default: "manifest.json" },
    },
  });
  const manifest = await claimManifest({
    control: values.control,
    candidate: values.candidate,
    datasets: values.datasets.split(",").filter(Boolean),
    shards: Number(values.shards),
    pairs: Number(values.pairs),
    targets: values.targets.split(",").filter(Boolean),
    only: values.only.split(",").filter(Boolean),
    images: values.images,
  });
  await writeFile(resolve(values.output), JSON.stringify(manifest, null, 2));
  process.stdout.write(
    `${JSON.stringify({ datasets: manifest.datasets.map((d) => d.dataset), shards: Array.from({ length: Number(values.shards) }, (_, i) => i) })}\n`,
  );
} else if (mode === "report") {
  const { values, positionals } = parseArgs({
    args: process.argv.slice(3),
    options: { manifest: { type: "string" } },
    allowPositionals: true,
  });
  if (values.manifest === undefined) throw new Error("report needs --manifest");
  const manifest = await validateManifest(
    JSON.parse(await readFile(resolve(values.manifest), "utf8")),
  );
  const results = await Promise.all(
    positionals.map(
      async (file) => JSON.parse(await readFile(resolve(file), "utf8")) as ClaimResult,
    ),
  );
  process.stdout.write(report(results, manifest));
} else throw new Error("usage: next-claim.mts manifest|run|report ...");
