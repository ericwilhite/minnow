/** Branch-only hosted B9 diagnostics. All setup is excluded from CPU pairs.
 * One fresh process per arm/cell/round, serial execution, nine AB/BA rounds.
 */
import { createHash } from "node:crypto";
import type { Profiler } from "node:inspector";
import { execFileSync } from "node:child_process";
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { cpus, hostname, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { runIndependentCPU, startCPUWorker, type CPUArm } from "./lib/independent-cpu.mts";
import type { B9WorkerData } from "./b9-perf-worker.mts";
import { compareFloatResults, samePlanBits, sameResultBits } from "./b9-float-law.mts";
import type { Verification } from "./lib/independent-cpu.mts";
import { planFixtures, runtimeFormat, type ProbeFixture } from "./b9-perf-fixtures.mts";

const args = process.argv.slice(2);
const option = (name: string, fallback: string) => {
  const i = args.indexOf(name);
  return i < 0 ? fallback : (args[i + 1] ?? fallback);
};
const root = resolve(dirname(fileURLToPath(import.meta.url)), ".."),
  output = resolve(option("--output", join(process.env.HOME ?? ".", ".cache/b9-perf")));
const candidatePolicy = JSON.parse(
  await readFile(join(root, "scripts/b9-candidate.json"), "utf8"),
) as { readonly exactMainResults?: boolean };
const exactMainResults = candidatePolicy.exactMainResults === true;
const candidate = resolve(option("--candidate", root)),
  parent = resolve(option("--parent", root)),
  frozen = option("--frozen", "");
const dataset = option("--dataset", "wide");
if (dataset !== "wide" && dataset !== "star") throw new Error("dataset must be wide or star");
const scale = Number(option("--scale", "333"));
if (!Number.isFinite(scale) || scale <= 0) throw new Error("positive commerce scale required");
const rows =
  dataset === "star" ? Math.max(1, Math.ceil(3000 * scale)) : Number(option("--rows", "1000000"));
const commerceSource = join(root, "apps/site/bench/benchmark.ts");
const diagnosticsOnly = args.includes("--diagnostics-only"),
  pairsOnly = args.includes("--pairs-only");
if (diagnosticsOnly && pairsOnly) throw new Error("Choose diagnostics or pairs, not both");
if (!Number.isSafeInteger(rows) || (dataset === "wide" && rows < 1000))
  throw new Error("rows must be an integer >=1000");
const wideDefaults = [
  "agg_highcard_text",
  "agg_highcard_int",
  "agg_count_distinct",
  "agg_3dims_20aggs",
  "join_fact_fact_int",
  "join_fact_fact_text",
  "join_star_3dims",
  "join_semi_exists",
  "join_anti_not_exists",
];
const catalogue = (await import(
  pathToFileURL(join(root, "scripts/next-executor-workload/queries.mjs")).href
)) as { S_QUERIES: Array<{ id: string }> };
const defaults = dataset === "wide" ? wideDefaults : catalogue.S_QUERIES.map((q) => q.id);
const cells = option("--cells", defaults.join(",")).split(",");
if (
  cells.length === 0 ||
  new Set(cells).size !== cells.length ||
  cells.some((c) => !defaults.includes(c))
)
  throw new Error("Unknown B9 cell");
await mkdir(output, { recursive: true });
const git = (path: string, ...command: string[]) =>
  execFileSync("git", ["-C", path, ...command], { encoding: "utf8" }).trim();
// A provisional candidate can be a reviewed patch on a fresh-main checkout.
// Include staged additions so equal base SHAs never classify a patched arm as A/A.
const sourceDiff = (runtimeRoot: string) =>
  createHash("sha256")
    .update(git(runtimeRoot, "diff", "HEAD", "--binary"))
    .digest("hex");
const identity = (runtimeRoot: string) =>
  git(runtimeRoot, "rev-parse", "HEAD") + ":" + sourceDiff(runtimeRoot);
const module = fileURLToPath(new URL("./b9-perf-worker.mts", import.meta.url));
const fixtureArms = await Promise.all(
  [
    { label: "main", root: parent },
    { label: "candidate", root: candidate },
    ...(frozen ? [{ label: "frozen", root: resolve(frozen) }] : []),
  ].map(async (arm) => ({ ...arm, format: await runtimeFormat(arm.root) })),
);
const fixtures = planFixtures(
  fixtureArms,
  output,
  dataset === "star" ? `S-${String(scale)}` : `W-${String(rows)}`,
);
const controller = new AbortController();
for (const signal of ["SIGTERM", "SIGINT"] as const)
  process.once(signal, () => controller.abort(new Error(signal)));
const imageFor = (runtimeRoot: string) => {
  const fixture = fixtures.find((fixture) => fixture.readers.includes(resolve(runtimeRoot)));
  if (!fixture) throw new Error("Missing runtime's ordinary-writer fixture");
  return fixture;
};
const data = (runtimeRoot: string, restore: boolean): B9WorkerData => ({
  runtimeRoot,
  workloadRoot: join(root, "scripts/next-executor-workload"),
  rows,
  dataset,
  scale,
  commerceSource,
  imageIdentity: imageFor(runtimeRoot).imageIdentity,
  capBytes: 256 * 2 ** 20,
  ...(restore ? { image: imageFor(runtimeRoot).image } : {}),
});
const arm = (runtimeRoot: string): CPUArm => ({
  module,
  cwd: runtimeRoot,
  revision: identity(runtimeRoot),
  data: data(runtimeRoot, true),
  execArgv: ["--max-old-space-size=3072", "--v8-pool-size=1"],
  env: { UV_THREADPOOL_SIZE: "1" },
});
const provenance = async (runtimeRoot: string) => ({
  root: runtimeRoot,
  revision: git(runtimeRoot, "rev-parse", "HEAD"),
  sourceDiffSha256: sourceDiff(runtimeRoot),
  dirty: git(runtimeRoot, "status", "--porcelain"),
  files: Object.fromEntries(
    await Promise.all(
      [
        "next/exec/create.js",
        "next/exec/join.js",
        "next/exec/join-table.js",
        "next/exec/agg/groups.js",
        "next/exec/agg/aggregate.js",
        "next/exec/plan.js",
      ].map(async (path) => [
        path,
        createHash("sha256")
          .update(await readFile(join(runtimeRoot, "packages/core/dist", path)))
          .digest("hex"),
      ]),
    ),
  ),
});
await writeFile(
  join(output, "provenance.json"),
  JSON.stringify(
    {
      hosted: process.env.GITHUB_ACTIONS === "true",
      measurementClass: diagnosticsOnly
        ? (dataset === "wide" && rows === 1000000) || (dataset === "star" && scale === 333)
          ? `${dataset === "wide" ? "W1M" : "S1"} path diagnostics; no timing pairs`
          : "small path diagnostics; no timing pairs"
        : args.includes("--smoke")
          ? "harness smoke, no timing pairs"
          : (dataset === "wide" && rows === 1000000) || (dataset === "star" && scale === 333)
            ? dataset === "wide"
              ? "W1M"
              : "S1"
            : "small harness check",
      runner: process.env.RUNNER_NAME,
      hostname: hostname(),
      node: process.version,
      cpu: cpus()[0]?.model,
      totalMemory: totalmem(),
      rows,
      dataset,
      scale: dataset === "star" ? scale : null,
      commerceSourceSha256:
        dataset === "star"
          ? createHash("sha256")
              .update(await readFile(commerceSource))
              .digest("hex")
          : null,
      sqlCorrections:
        dataset === "star" ? ["S_q7 ROUND(revenue::numeric,4), matching existing benchmark"] : [],
      cells,
      capBytes: 256 * 2 ** 20,
      comparisonLaw: {
        exactCandidateVersusMain: exactMainResults,
        authority: "https://www.postgresql.org/docs/18/sql-expressions.html#SYNTAX-AGGREGATES",
        finite:
          "Only unordered float4/float8 SUM/AVG/variance/stddev output lineage; abs(a-b)<=max(abs(a),abs(b),1)*1e-9",
        exact:
          "All keys, counts, non-float/NUMERIC/integer/ordered aggregates, row positions and specials; raw results and IEEE bits retained",
        determinism:
          "Three runs of the same prepared object, and exact before/after bits around every CPU sample",
        observed: "Actual65536 W star:18 SUMs, maximum2.64e-16 relative after J3 orientation",
      },
      fixturePolicy:
        "one in-job ordinary-writer image per durable vocabulary; compatible arms share it, incompatible arms use separate images from the identical logical generator",
      fixtureArms,
      fixtures,
      workload: Object.fromEntries(
        await Promise.all(
          ["gen.mjs", "queries.mjs"].map(async (file) => [
            file,
            createHash("sha256")
              .update(await readFile(join(root, "scripts/next-executor-workload", file)))
              .digest("hex"),
          ]),
        ),
      ),
      candidate: await provenance(candidate),
      parent: await provenance(parent),
      frozen: frozen ? await provenance(resolve(frozen)) : null,
    },
    null,
    2,
  ),
);
// Unreleased development formats are not compatible predecessors. Every distinct
// vocabulary writes once in this job; no arm opens another vocabulary's bytes.
async function prepareFixture(descriptor: ProbeFixture) {
  const runtimeRoot = descriptor.writer;
  const sourcePath = join(output, descriptor.label + "-fixture-source.json");
  const source = {
    writer: git(runtimeRoot, "rev-parse", "HEAD"),
    format: descriptor.format,
    rows,
    dataset,
    scale: dataset === "star" ? scale : null,
    commerceSourceSha256:
      dataset === "star"
        ? createHash("sha256")
            .update(await readFile(commerceSource))
            .digest("hex")
        : null,
    columns: dataset === "wide" ? 200 : null,
    generator: createHash("sha256")
      .update(await readFile(join(root, "scripts/next-executor-workload/gen.mjs")))
      .digest("hex"),
    worker: createHash("sha256")
      .update(await readFile(module))
      .digest("hex"),
  };
  let known: string | null = null;
  try {
    known = await readFile(sourcePath, "utf8");
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
  }
  if (known !== null) {
    if (JSON.stringify(JSON.parse(known)) !== JSON.stringify(source))
      throw new Error("Fixture source changed across probe phases");
    return;
  }
  const preparation = await startCPUWorker(
    { ...arm(runtimeRoot), data: data(runtimeRoot, false) },
    { arm: "control", cohort: -1, workload: cells[0] ?? "agg_highcard_text", boundary: "frames" },
    undefined,
    controller.signal,
  );
  try {
    const manifest = await preparation.ask({
      op: "command",
      name: "capture",
      data: { directory: descriptor.image },
    });
    await writeFile(
      join(output, descriptor.label + "-fixture-manifest.json"),
      JSON.stringify(manifest, null, 2),
    );
    await writeFile(sourcePath, JSON.stringify(source));
  } finally {
    await preparation.close();
  }
}
for (const fixture of fixtures)
  if (!diagnosticsOnly || fixture.readers.includes(parent) || fixture.readers.includes(candidate))
    await prepareFixture(fixture);
// Publish diagnostic profiles before spending time on any timing comparison.
if (!pairsOnly) {
  for (const [label, runtimeRoot] of [
    ["parent", parent],
    ["candidate", candidate],
  ] as const) {
    for (const id of cells) {
      const worker = await startCPUWorker(
        arm(runtimeRoot),
        {
          arm: label === "parent" ? "control" : "candidate",
          cohort: -1,
          workload: id,
          boundary: "frames",
        },
        undefined,
        controller.signal,
      );
      try {
        const deterministic = await worker.ask({
          op: "command",
          name: "determinism",
          data: { id },
        });
        await writeFile(
          join(output, `${label}-${id}-determinism.json`),
          JSON.stringify(deterministic, null, 2),
        );
        const before = await worker.ask<Verification>({ op: "verify", workload: id });
        await worker.ask({
          op: "command",
          name: "profile",
          data: { id, path: join(output, `${label}-${id}.cpuprofile`) },
        });
        const cpuProfile = JSON.parse(
          await readFile(join(output, `${label}-${id}.cpuprofile`), "utf8"),
        ) as Profiler.Profile;
        const nodes = new Map(cpuProfile.nodes.map((node) => [node.id, node]));
        const samples = new Map<number, number>();
        for (const sample of cpuProfile.samples ?? []) {
          if (nodes.get(sample)?.callFrame.functionName === "(idle)") continue;
          samples.set(sample, (samples.get(sample) ?? 0) + 1);
        }
        const total = [...samples.values()].reduce((sum, count) => sum + count, 0);
        const functions = [...samples]
          .map(([node, count]) => ({
            frame: nodes.get(node)?.callFrame,
            samples: count,
            share: total === 0 ? 0 : count / total,
          }))
          .sort((a, b) => b.samples - a.samples);
        await writeFile(
          join(output, `${label}-${id}-profile-summary.json`),
          JSON.stringify(
            {
              basis: "non-idle main-isolate self samples; separate from paired CPU",
              total,
              functions,
            },
            null,
            2,
          ),
        );
        const trace = await worker.ask({ op: "command", name: "trace", data: { id } });
        const after = await worker.ask<Verification>({ op: "verify", workload: id });
        if (!samePlanBits(before, after)) throw new Error("Trace verification changed output bits");
        await writeFile(
          join(output, `${label}-${id}-trace.json`),
          JSON.stringify({ id, before, trace, after }, null, 2),
        );
      } finally {
        await worker.close();
      }
    }
  }
}
if (!diagnosticsOnly) {
  const controls: Array<readonly [string, string]> = [];
  if (!args.includes("--smoke")) controls.push(["main", parent]);
  if (frozen) controls.push(["frozen", resolve(frozen)]);
  for (const [label, runtimeRoot] of controls) {
    let recording = Promise.resolve();
    let result: Awaited<ReturnType<typeof runIndependentCPU>>;
    try {
      result = await runIndependentCPU({
        control: arm(runtimeRoot),
        candidate: arm(candidate),
        workloads: cells,
        boundaries: ["frames"],
        primaryBoundary: "frames",
        pairs: 9,
        purpose:
          identity(runtimeRoot) === identity(candidate) ? "identical-build-control" : "true-parent",
        signal: controller.signal,
        compareVerification:
          exactMainResults && label === "main" ? sameResultBits : compareFloatResults,
        onVerification: (value) => {
          recording = recording.then(() =>
            appendFile(join(output, `${label}-raw-results.jsonl`), JSON.stringify(value) + "\n"),
          );
          return recording;
        },
        onEvent: (event) => {
          recording = recording.then(() =>
            appendFile(join(output, `${label}-events.jsonl`), JSON.stringify(event) + "\n"),
          );
        },
        onSample: (sample) => {
          recording = recording.then(() =>
            appendFile(join(output, `${label}-pairs.jsonl`), JSON.stringify(sample) + "\n"),
          );
        },
      });
    } finally {
      await recording;
    }
    await writeFile(join(output, `${label}-summary.json`), JSON.stringify(result, null, 2));
  }
}
console.log(JSON.stringify({ output, rows, dataset, scale, cells, complete: true }));
