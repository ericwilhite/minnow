/** Head-to-head progress lane: this checkout's 1.0 rebuild (next/) against a published engine.
 *
 *   node scripts/head-to-head.mts run --dataset W1M --shipped <unpacked package> --output <dir>
 *   node scripts/head-to-head.mts run --dataset W1M --candidate <built checkout> --output <dir>
 *   node scripts/head-to-head.mts report <result.json>... > table.md
 *
 * `run` starts one long-lived worker per engine (`head-to-head-worker.mts` through
 * `lib/independent-cpu.mts`), builds each engine's fixture once, verifies every Gate B cell on
 * both engines, then times `--rounds` alternating rounds (AB, BA, ...). It records process CPU
 * (all threads, natural GC) and wall time per run; setup, verification and IPC are excluded.
 * A cell that fails or times out on one engine is recorded and the lane moves on.
 *
 * This is a visibility lane, not an acceptance protocol: one shared runner, a few rounds, no A/A
 * calibration. Paired acceptance stays with the dedicated probes. Nothing here fails a build
 * because of a timing.
 */
import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { cpus, totalmem } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { startCPUWorker, type CPUValue, type Verification } from "./lib/independent-cpu.mts";
import type { HeadToHeadData } from "./head-to-head-worker.mts";

type Dataset = "W1M" | "S1";
type Arm = "next" | "shipped";
interface Sample {
  readonly arm: Arm;
  readonly boundary: string;
  readonly round: number;
  readonly cpuMs: number;
  readonly wallMs: number;
}
interface Cell {
  readonly id: string;
  readonly verification: Partial<Record<Arm, Verification | { error: string }>>;
  readonly samples: Sample[];
  readonly errors: Partial<Record<Arm, string>>;
}
export interface HeadToHeadResult {
  readonly dataset: Dataset;
  readonly started: string;
  readonly finished: string;
  readonly revision: string;
  readonly shipped: { version: string; root: string; integrity: string | null };
  /** Set in candidate mode: the second arm is this checkout's next/ engine, not a package. */
  readonly candidate?: { root: string; revision: string };
  readonly rows: number;
  readonly scale: number;
  readonly rounds: number;
  readonly capBytes: number;
  readonly node: string;
  readonly cpu: string | undefined;
  readonly memoryBytes: number;
  readonly loadMs: Partial<Record<Arm, number>>;
  readonly cells: Cell[];
}

const repository = resolve(import.meta.dirname, "..");
const ERROR_TEXT = 300;
const brief = (error: unknown): string =>
  (error instanceof Error ? error.message : String(error))
    .split("\n", 1)
    .join("")
    .slice(0, ERROR_TEXT);

/** Gate B cells, from the committed browser-lane baseline so the two lanes name the same set. */
async function gateBCells(dataset: Dataset): Promise<string[]> {
  const baseline = JSON.parse(
    await readFile(join(repository, "packages/core/browser-perf-baseline.json"), "utf8"),
  ) as { targets: Record<string, { gate?: string }> };
  return Object.entries(baseline.targets)
    .filter(([key, target]) => target.gate === "B" && key.startsWith(`${dataset}/`))
    .map(([key]) => key.slice(dataset.length + 1));
}

const median = (values: readonly number[]): number | null => {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = sorted.length >>> 1;
  const upper = sorted[middle];
  if (upper === undefined) return null;
  return sorted.length % 2 === 1 ? upper : ((sorted[middle - 1] ?? upper) + upper) / 2;
};

async function run(): Promise<void> {
  const { values } = parseArgs({
    args: process.argv.slice(3),
    options: {
      dataset: { type: "string", default: "W1M" },
      shipped: { type: "string" },
      candidate: { type: "string" },
      output: { type: "string" },
      rounds: { type: "string", default: "5" },
      rows: { type: "string", default: "1000000" },
      scale: { type: "string", default: "333" },
      only: { type: "string", default: "" },
      "deadline-ms": { type: "string", default: "180000" },
      "heap-mb": { type: "string", default: "6144" },
    },
  });
  const dataset = values.dataset as Dataset;
  if (dataset !== "W1M" && dataset !== "S1") throw new Error("--dataset must be W1M or S1");
  if (
    (values.shipped === undefined) === (values.candidate === undefined) ||
    values.output === undefined
  )
    throw new Error(
      "one of --shipped <unpacked package root> or --candidate <built checkout>, and --output <dir>, are required",
    );
  const candidateRoot = values.candidate === undefined ? null : resolve(values.candidate);
  const rounds = Number(values.rounds),
    rows = Number(values.rows),
    scale = Number(values.scale),
    deadlineMs = Number(values["deadline-ms"]);
  for (const [name, value] of Object.entries({ rounds, rows, scale, deadlineMs }))
    if (!Number.isFinite(value) || value <= 0) throw new RangeError(`positive ${name} required`);
  const shippedRoot = candidateRoot ?? resolve(values.shipped ?? ""),
    output = resolve(values.output);
  const candidateRevision =
    candidateRoot === null
      ? null
      : execFileSync("git", ["rev-parse", "HEAD"], { cwd: candidateRoot, encoding: "utf8" }).trim();
  const shippedPackage =
    candidateRevision === null
      ? (JSON.parse(await readFile(join(shippedRoot, "package.json"), "utf8")) as {
          version: string;
        })
      : { version: `candidate ${candidateRevision.slice(0, 10)}` };
  const only = new Set(values.only.split(",").filter(Boolean));
  const cells = (await gateBCells(dataset)).filter((id) => only.size === 0 || only.has(id));
  if (cells.length === 0) throw new Error("no cells selected");
  await mkdir(output, { recursive: true });
  const revision = execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: repository,
    encoding: "utf8",
  }).trim();
  let integrity: string | null = null;
  try {
    integrity = (await readFile(join(shippedRoot, "..", "integrity.txt"), "utf8")).trim();
  } catch {
    integrity = null;
  }
  const capBytes = 256 * 1024 * 1024;
  const started = new Date().toISOString();
  const base = { dataset, rows, scale, capBytes, deadlineMs, workloads: cells } as const;
  const arms: Record<Arm, HeadToHeadData> = {
    next: { ...base, engine: "next", root: repository },
    shipped: { ...base, engine: candidateRoot === null ? "shipped" : "next", root: shippedRoot },
  };
  const workers: Partial<Record<Arm, Awaited<ReturnType<typeof startCPUWorker>>>> = {};
  const loadMs: Partial<Record<Arm, number>> = {};
  const startupErrors: Partial<Record<Arm, string>> = {};
  const log = (event: object) => console.error(JSON.stringify(event));
  // Start serially so the two fixture loads never compete for the runner.
  for (const arm of ["next", "shipped"] as const) {
    log({ arm, phase: "load", dataset });
    try {
      const worker = await startCPUWorker(
        {
          module: fileURLToPath(new URL("./head-to-head-worker.mts", import.meta.url)),
          cwd: repository,
          revision:
            arm === "next"
              ? revision
              : (candidateRevision ?? `@minnowdb/core@${shippedPackage.version}`),
          data: arms[arm],
          execArgv: [`--max-old-space-size=${values["heap-mb"]}`],
        },
        {
          arm: arm === "next" ? "candidate" : "control",
          cohort: 0,
          workload: dataset,
          boundary: "rows",
        },
      );
      workers[arm] = worker;
      loadMs[arm] = (worker.hello.identity as { loadMs: number }).loadMs;
      log({ arm, phase: "loaded", loadMs: loadMs[arm] });
    } catch (error) {
      startupErrors[arm] = `fixture: ${brief(error)}`;
      log({ arm, phase: "load-failed", error: startupErrors[arm] });
    }
  }
  const results: Cell[] = [];
  const save = async (finished: string) => {
    const result: HeadToHeadResult = {
      dataset,
      started,
      finished,
      revision,
      shipped: { version: shippedPackage.version, root: shippedRoot, integrity },
      ...(candidateRevision === null || candidateRoot === null
        ? {}
        : { candidate: { root: candidateRoot, revision: candidateRevision } }),
      rows,
      scale,
      rounds,
      capBytes,
      node: process.version,
      cpu: cpus()[0]?.model,
      memoryBytes: totalmem(),
      loadMs,
      cells: results,
    };
    await writeFile(join(output, `head-to-head-${dataset}.json`), JSON.stringify(result, null, 2));
    return result;
  };
  try {
    for (const id of cells) {
      const cell: Cell = { id, verification: {}, samples: [], errors: { ...startupErrors } };
      results.push(cell);
      for (const arm of ["next", "shipped"] as const) {
        const worker = workers[arm];
        if (worker === undefined) continue;
        try {
          cell.verification[arm] = await worker.ask<Verification>({ op: "verify", workload: id });
        } catch (error) {
          cell.errors[arm] = brief(error);
          cell.verification[arm] = { error: cell.errors[arm] };
        }
      }
      log({ id, phase: "verified", verification: cell.verification });
      for (let round = 0; round < rounds; round++) {
        const order: Arm[] = round % 2 === 0 ? ["next", "shipped"] : ["shipped", "next"];
        for (const arm of order) {
          const worker = workers[arm];
          if (worker === undefined || cell.errors[arm] !== undefined) continue;
          // Candidate mode compares two next/ builds at the native frames boundary only.
          const boundaries =
            candidateRoot !== null ? ["frames"] : arm === "next" ? ["rows", "frames"] : ["rows"];
          for (const boundary of boundaries) {
            try {
              const value = await worker.ask<CPUValue>({ op: "run", workload: id, boundary });
              const sample = { arm, boundary, round, cpuMs: value.cpuMs, wallMs: value.wallMs };
              cell.samples.push(sample);
              log({ id, ...sample });
            } catch (error) {
              cell.errors[arm] = brief(error);
              log({ id, arm, boundary, round, error: cell.errors[arm] });
              break;
            }
          }
        }
      }
      await save("running");
    }
  } finally {
    for (const worker of Object.values(workers)) await worker.close().catch(() => undefined);
  }
  const result = await save(new Date().toISOString());
  process.stdout.write(await report([result]));
  // Timings never fail the lane; an engine that cannot load its fixture does.
  if (Object.keys(startupErrors).length > 0) process.exitCode = 1;
}

const format = (value: number | null, digits = 1): string =>
  value === null
    ? "—"
    : value.toLocaleString("en-US", {
        maximumFractionDigits: digits,
        minimumFractionDigits: digits,
      });
const ratio = (a: number | null, b: number | null): number | null =>
  a === null || b === null || b <= 0 ? null : a / b;
const geomean = (values: readonly number[]): number | null =>
  values.length === 0
    ? null
    : Math.exp(values.reduce((sum, value) => sum + Math.log(value), 0) / values.length);

/** One markdown table across every dataset in the given results. */
/** Type-7 quantile, as the paired CPU probes use. */
function quantile(sorted: readonly number[], p: number): number {
  const index = (sorted.length - 1) * p,
    low = Math.floor(index),
    high = Math.min(low + 1, sorted.length - 1);
  return (sorted[low] ?? NaN) + ((sorted[high] ?? NaN) - (sorted[low] ?? NaN)) * (index - low);
}

/** Candidate mode: per-cell paired frames-CPU deltas (candidate - main) by round. */
function reportCandidate(results: readonly HeadToHeadResult[]): string {
  const first = results[0];
  if (first?.candidate === undefined) throw new Error("not a candidate run");
  const lines = [
    `## Candidate vs main (next/ engine, frames boundary)`,
    "",
    `main \`${first.revision.slice(0, 10)}\` · candidate \`${first.candidate.revision.slice(0, 10)}\` · Node ${first.node} · ${first.cpu ?? "unknown CPU"} · ${first.rounds} alternating rounds · 256 MiB on both`,
    "",
    "Process CPU in ms. Δ = candidate − main per round; a gain or regression needs |median Δ| above its IQR and at least 7 of 9 rounds the same way (the paired-CPU rule), otherwise it is inconclusive. One shared runner, no A/A: a screen, not acceptance.",
    "",
    "| Cell | main | candidate | Δ median | IQR | candidate faster | verdict | answers |",
    "|---|---:|---:|---:|---:|---:|---|---|",
  ];
  const regressions: string[] = [],
    gains: string[] = [];
  for (const result of results)
    for (const cell of result.cells) {
      const by = (arm: Arm) =>
        new Map(
          cell.samples
            .filter((sample) => sample.arm === arm && sample.boundary === "frames")
            .map((sample) => [sample.round, sample.cpuMs]),
        );
      const main = by("next"),
        candidate = by("shipped");
      const deltas = [...main.keys()]
        .filter((round) => candidate.has(round))
        .map((round) => (candidate.get(round) ?? NaN) - (main.get(round) ?? NaN))
        .sort((a, b) => a - b);
      const name = `${result.dataset}/${cell.id}`;
      const a = cell.verification.next,
        b = cell.verification.shipped;
      const answers =
        a === undefined || b === undefined || "error" in a || "error" in b
          ? "—"
          : a.fingerprint === b.fingerprint && a.rows === b.rows
            ? "same"
            : "**differ**";
      if (deltas.length === 0) {
        lines.push(`| ${name} | — | — | — | — | — | error | ${answers} |`);
        continue;
      }
      const middle = quantile(deltas, 0.5),
        iqr = quantile(deltas, 0.75) - quantile(deltas, 0.25),
        faster = deltas.filter((delta) => delta < 0).length,
        need = Math.ceil((7 / 9) * deltas.length);
      const verdict =
        Math.abs(middle) > iqr && middle < 0 && faster >= need
          ? "gain"
          : Math.abs(middle) > iqr && middle > 0 && deltas.length - faster >= need
            ? "**regression**"
            : "inconclusive";
      if (verdict === "gain") gains.push(name);
      if (verdict !== "gain" && verdict !== "inconclusive") regressions.push(name);
      lines.push(
        `| ${name} | ${format(median([...main.values()]))} | ${format(median([...candidate.values()]))} | ${format(middle, 2)} | ${format(iqr, 2)} | ${String(faster)}/${String(deltas.length)} | ${verdict} | ${answers} |`,
      );
    }
  lines.push(
    "",
    `**Gains (${String(gains.length)}):** ${gains.join(", ") || "none"}`,
    "",
    `**Regressions (${String(regressions.length)}):** ${regressions.join(", ") || "none"}`,
    "",
  );
  return lines.join("\n");
}

export async function report(results: readonly HeadToHeadResult[]): Promise<string> {
  if (results[0]?.candidate !== undefined) return reportCandidate(results);
  const targets = JSON.parse(
    await readFile(join(repository, "scripts/head-to-head-targets.json"), "utf8"),
  ) as Record<string, Record<string, number> | string>;
  const first = results[0];
  if (first === undefined) throw new Error("no results");
  const v = first.shipped.version.split(".").slice(0, 2).join(".");
  const lines = [
    `## Head-to-head: main vs published ${first.shipped.version}`,
    "",
    `main \`${first.revision.slice(0, 10)}\` · \`@minnowdb/core@${first.shipped.version}\` · Node ${first.node} · ${first.cpu ?? "unknown CPU"} · ${Math.round(first.memoryBytes / 2 ** 30)} GiB · ${first.rounds} alternating rounds · 256 MiB execution budget on both`,
    "",
    "Medians in ms. **Rows** materializes every result row on both engines (matched boundary); **frames** stops at main's columnar result frames. CPU is process user+system; target is the fixed DuckDB-Wasm warm wall time from the 1.0 proposal, compared here with main's frames wall time. Ratios above 1 mean main is slower. Shared hosted runner: read trends, not single cells.",
    "",
    `Fixtures: ${results.map((r) => (r.dataset === "W1M" ? `W1M ${r.rows.toLocaleString("en-US")} rows × 200 columns` : `S1 star scale ${r.scale}`)).join(", ")}.`,
    "",
    `| Cell | main rows CPU | ${v} rows CPU | main/${v} | main frames wall | target | frames/target | answers | note |`,
    "|---|---:|---:|---:|---:|---:|---:|---|---|",
  ];
  const versus: number[] = [],
    toTarget: number[] = [],
    slower: string[] = [],
    perDataset: string[] = [];
  for (const result of results) {
    const table = targets[result.dataset];
    const datasetVersus: number[] = [],
      datasetTarget: number[] = [];
    for (const cell of result.cells) {
      const pick = (arm: Arm, boundary: string, field: "cpuMs" | "wallMs") =>
        median(
          cell.samples
            .filter((sample) => sample.arm === arm && sample.boundary === boundary)
            .map((sample) => sample[field]),
        );
      const mainRows = pick("next", "rows", "cpuMs"),
        shippedRows = pick("shipped", "rows", "cpuMs"),
        mainFrames = pick("next", "frames", "wallMs");
      const target = typeof table === "object" ? (table[cell.id] ?? null) : null;
      const vs = ratio(mainRows, shippedRows),
        tt = ratio(mainFrames, target);
      if (vs !== null) {
        versus.push(vs);
        datasetVersus.push(vs);
      }
      if (tt !== null) {
        toTarget.push(tt);
        datasetTarget.push(tt);
      }
      if (vs !== null && vs > 1) slower.push(`${result.dataset}/${cell.id}`);
      const a = cell.verification.next,
        b = cell.verification.shipped;
      const answers =
        a === undefined || b === undefined || "error" in a || "error" in b
          ? "—"
          : a.fingerprint === b.fingerprint && a.rows === b.rows
            ? "same"
            : `**differ** (${a.rows ?? "?"}/${b.rows ?? "?"} rows)`;
      const note = (["next", "shipped"] as const)
        .flatMap((arm) => {
          const error = cell.errors[arm];
          return error === undefined
            ? []
            : [`${arm === "next" ? "main" : v}: ${error.replaceAll("|", "\\|").slice(0, 80)}`];
        })
        .join("; ");
      lines.push(
        `| ${result.dataset}/${cell.id} | ${format(mainRows)} | ${format(shippedRows)} | ${vs === null ? "—" : `${format(vs, 2)}×`} | ${format(mainFrames)} | ${format(target)} | ${tt === null ? "—" : `${format(tt, 2)}×`} | ${answers} | ${note} |`,
      );
    }
    perDataset.push(
      `${result.dataset}: main/${v} ${datasetVersus.length === 0 ? "—" : `${format(geomean(datasetVersus), 2)}×`}, frames/target ${datasetTarget.length === 0 ? "—" : `${format(geomean(datasetTarget), 2)}×`}`,
    );
  }
  lines.push(
    "",
    `**Geomean** (${perDataset.join("; ")}) · all: main/${v} (rows CPU): ${versus.length === 0 ? "—" : `${format(geomean(versus), 2)}× over ${versus.length} cells`} · main frames/target (wall): ${toTarget.length === 0 ? "—" : `${format(geomean(toTarget), 2)}× over ${toTarget.length} cells`}`,
    "",
    `**Slower than ${v} (${slower.length}):** ${slower.length === 0 ? "none" : slower.join(", ")}`,
    "",
    `Fixture load: ${results.map((r) => `${r.dataset} main ${format(r.loadMs.next ?? null, 0)} ms, ${v} ${format(r.loadMs.shipped ?? null, 0)} ms`).join("; ")}.`,
    "",
  );
  return lines.join("\n");
}

const mode = process.argv[2];
if (process.argv[1] !== undefined && resolve(process.argv[1]) !== fileURLToPath(import.meta.url)) {
  // Imported: expose report() only.
} else if (mode === "run") await run();
else if (mode === "report") {
  const files = process.argv.slice(3);
  if (files.length === 0) throw new Error("report needs result files");
  const results = await Promise.all(
    files.map(
      async (file) => JSON.parse(await readFile(resolve(file), "utf8")) as HeadToHeadResult,
    ),
  );
  process.stdout.write(await report(results));
} else throw new Error("usage: head-to-head.mts run|report ...");
