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
import { join, resolve } from "node:path";
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
  readonly ab: CellRecord[];
  readonly void: boolean;
  /** manifestDigest of the manifest this shard ran under: binds the artefact to its run. */
  readonly manifest: string;
}

/** The run declared at dispatch: what a complete run must report. */
export interface ClaimManifest {
  readonly control: string;
  readonly candidate: string;
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
  const keys = ["candidate", "control", "datasets", "pairs", "targets"];
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
}): Promise<ClaimManifest> {
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

  // 1. One fixture per arm, captured; the control arm also sizes the brackets.
  const images: Record<Role, string> = {
    control: join(output, "images", "control"),
    candidate: join(output, "images", "candidate"),
  };
  let times: Record<string, number> = {};
  for (const arm of ["control", "candidate"] as const) {
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
        data: { directory: images[arm], identity: `${arm}:${revisions[arm]}` },
      });
      if (arm === "control")
        times = await handle.ask<Record<string, number>>({ op: "command", name: "time-cells" });
    } finally {
      await handle.close();
    }
    log({ phase: "image", arm, seconds: (performance.now() - began) / 1000 });
  }
  const repeats = Object.fromEntries(needed.map((id) => [id, bracketRepeats(times[id] ?? 0)]));
  log({ phase: "brackets", repeats, seed });
  const spec = (arm: Role): CPUArm => ({
    module: worker,
    cwd: repository,
    revision: revisions[arm],
    data: {
      ...base,
      engine: "next",
      root: roots[arm],
      workloads: needed,
      image: images[arm],
      imageIdentity: `${arm}:${revisions[arm]}`,
      repeats,
    } satisfies HeadToHeadData,
    execArgv,
  });
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
            ? compareAnswers(left, right, ordered.has(cell)).same
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
  const flagged = aa.some((r) => r.error !== undefined || targetVerdict(r) !== "none");
  // 3. A/B on this shard's cells, measured even when void so the evidence is kept.
  const ab: CellRecord[] = [];
  for (const cell of cells) ab.push(await measure(cell, spec("candidate"), "true-parent"));
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

/** Everything a complete run must report, checked against the manifest. */
export function claimProblems(results: readonly ClaimResult[], manifest: ClaimManifest): string[] {
  const problems: string[] = [];
  const declared = new Set<string>();
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
    }
  for (const result of results)
    if (!declared.has(`${result.dataset} ${result.shard}`))
      problems.push(`${result.dataset} shard ${result.shard} was not declared`);
  return problems;
}

/** One table across shards: verdicts only for a complete run, never for a void one. */
export function report(results: readonly ClaimResult[], manifest: ClaimManifest): string {
  const problems = claimProblems(results, manifest);
  const complete = problems.length === 0;
  // One A/A control meeting the target rule (or failing) in any shard voids every verdict.
  const voided = results.filter(
    (r) => r.void || r.aa.some((a) => a.error !== undefined || targetVerdict(a) !== "none"),
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
      ? `**VOID:** an A/A control met the target rule or failed in shard(s) ${voided.map((r) => `${r.dataset} ${r.shard}`).join(", ")}; no verdict from this run may be cited.`
      : "A/A controls neutral in every reported shard.",
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
          : role === "A/A"
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
