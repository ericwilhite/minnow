/** The speed-claim protocol for next/: the one standard for any "faster" or "not slower" claim.
 *
 *   node scripts/next-claim.mts run --dataset W1M --control <built checkout>
 *        --candidate <built checkout> --targets a,b --output <dir> [--shard 0/6] [--pairs 15]
 *   node scripts/next-claim.mts report <claim-*.json>... > table.md
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
 */
import { execFileSync } from "node:child_process";
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
      pairs: { type: "string", default: "15" },
      targets: { type: "string", default: "" },
      seed: { type: "string", default: "" },
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
  const pairs = Number(values.pairs),
    rows = Number(values.rows),
    scale = Number(values.scale),
    seed = values.seed === "" ? Math.floor(Math.random() * 2 ** 31) : Number(values.seed);
  if (!Number.isSafeInteger(pairs) || pairs < 9) throw new Error("--pairs must be at least 9");
  const targets = values.targets.split(",").filter(Boolean);
  const roots = { control: resolve(values.control), candidate: resolve(values.candidate) };
  const revisionOf = (root: string) =>
    execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  const revisions = { control: revisionOf(roots.control), candidate: revisionOf(roots.candidate) };
  if (revisions.control === revisions.candidate)
    throw new Error("A claim compares two different builds; an A/A run uses one build");
  const only = new Set(values.only.split(",").filter(Boolean));
  const all = (await gateBCells(dataset)).filter((id) => only.size === 0 || only.has(id));
  for (const target of targets)
    if (!all.includes(target)) throw new Error(`Target ${target} is not a selected cell`);
  const cells = all.filter((_, index) => index % shardCount === shardIndex);
  const controls = CONTROL_CELLS[dataset];
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
      return {
        cell,
        target: targets.includes(cell),
        repeats: repeats[cell] ?? 1,
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
  };
  await writeFile(
    join(output, `claim-${dataset}-${String(shardIndex)}.json`),
    JSON.stringify(result, null, 2),
  );
  process.stdout.write(report([result]));
}

const fmt = (value: number | undefined, digits = 1) =>
  value === undefined || !Number.isFinite(value)
    ? "—"
    : value.toLocaleString("en-US", {
        maximumFractionDigits: digits,
        minimumFractionDigits: digits,
      });

/** One table across shards: Holm over every screened cell; a void shard voids the run. */
export function report(results: readonly ClaimResult[]): string {
  const first = results[0];
  if (first === undefined) throw new Error("no claim results");
  const voided = results.filter((r) => r.void);
  // Every dataset must report every shard; a crashed shard leaves its cells unmeasured.
  const missing: string[] = [];
  for (const dataset of new Set(results.map((r) => r.dataset))) {
    const shards = results.filter((r) => r.dataset === dataset);
    const count = Number(shards[0]?.shard.split("/")[1] ?? 1);
    const present = new Set(shards.map((r) => r.shard));
    for (let i = 0; i < count; i++)
      if (!present.has(`${String(i)}/${String(count)}`))
        missing.push(`${dataset} ${String(i)}/${String(count)}`);
  }
  const screened = results.flatMap((result) =>
    result.ab
      .filter((r) => r.target !== true && r.signP !== undefined)
      .map((r) => `${result.dataset}/${r.cell}`),
  );
  const pOf = new Map<string, number>();
  results.forEach((result) =>
    result.ab.forEach((r) => {
      if (r.target !== true && r.signP !== undefined)
        pOf.set(`${result.dataset}/${r.cell}`, r.signP);
    }),
  );
  const adjusted = holm(screened.map((key) => pOf.get(key) ?? 1));
  const holmOf = new Map(screened.map((key, index) => [key, adjusted[index] ?? 1]));
  const lines = [
    `## Speed claim: candidate vs control (next/, frames boundary)`,
    "",
    '_Internal measurement protocol (testing reference, "Speed claims"), not a published benchmark._',
    "",
    `control \`${first.control.revision.slice(0, 10)}\` · candidate \`${first.candidate.revision.slice(0, 10)}\` · Node ${first.node} · ${String(first.pairs)} fresh-process pairs per cell · seed ${String(first.seed)} · targets ${first.targets.join(", ") || "none"}`,
    "",
    voided.length > 0
      ? `**VOID:** an A/A control met the target rule in shard(s) ${voided.map((r) => r.shard).join(", ")}; no verdict from this run may be cited.`
      : "A/A controls neutral in every shard.",
    ...(missing.length > 0
      ? [
          "",
          `**INCOMPLETE:** missing shard(s) ${missing.join(", ")}; their cells were not measured.`,
        ]
      : []),
    "",
    `Per-query process CPU medians in ms; Δ is per bracket (candidate − control). Targets: a gain or regression needs |median Δ| > IQR, 7 of 9 pairs (scaled) one way and at least ${String(MIN_EFFECT * 100)}% of the control median. Other cells: a regression flag also needs a Holm-adjusted sign-test p ≤ 0.05 across all ${String(screened.length)} screened cells. Anything else is no accepted difference, not equality.`,
    "",
    "| Cell | role | shard | control | candidate | Δ median | IQR | candidate faster | p (Holm for screen) | verdict | CPU |",
    "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
  ];
  for (const result of results) {
    const rows: Array<[string, CellRecord]> = [
      ...result.aa.map((r): [string, CellRecord] => ["A/A", r]),
      ...result.ab.map((r): [string, CellRecord] => [r.target === true ? "target" : "screen", r]),
    ];
    for (const [role, r] of rows) {
      const key = `${result.dataset}/${r.cell}`;
      const tv = targetVerdict(r);
      const p = role === "screen" ? holmOf.get(key) : r.signP;
      const verdict =
        r.error !== undefined
          ? `error: ${r.error.replaceAll("|", "\\|").slice(0, 80)}`
          : result.void && role !== "A/A"
            ? "void"
            : role === "screen"
              ? tv === "regression" && (p ?? 1) <= 0.05
                ? "**regression flag**"
                : "no accepted difference"
              : tv === "none"
                ? role === "A/A"
                  ? "neutral"
                  : "no accepted difference"
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
else if (mode === "report") {
  const files = process.argv.slice(3);
  if (files.length === 0) throw new Error("report needs claim result files");
  const results = await Promise.all(
    files.map(async (file) => JSON.parse(await readFile(resolve(file), "utf8")) as ClaimResult),
  );
  process.stdout.write(report(results));
} else throw new Error("usage: next-claim.mts run|report ...");
