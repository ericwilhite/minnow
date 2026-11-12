/** Independent-VM root benchmark. A/A is required before A/B; never reuse a timed worker.
 * B7's shared runner owns every CPU clock and balanced process start/execution order.
 */
import { createHash } from "node:crypto";
import { appendFileSync } from "node:fs";
import { access, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { manifest } from "./next-executor-quiet.mts";
import { runIndependentCPU, startCPUWorker, type CPUArm } from "./lib/independent-cpu.mts";
import { pairedCPU } from "./lib/paired-cpu.mts";
import type { RootRunConfig } from "./query-root-runtime-worker.mts";
import type { RootComponentConfig } from "./query-root-component-worker.mts";

const option = (key: string, fallback = "") => {
  const at = process.argv.indexOf(key);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const sha = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
type Result = Awaited<ReturnType<typeof runIndependentCPU>>;
interface Bundle {
  rows: number;
  image: string;
  imageIdentity: string;
  expected: Record<string, string>;
}
interface AAReport {
  purpose: string;
  smoke: boolean;
  mode: string;
  rows: number;
  warmups: number;
  repeats: number;
  candidateRevision: string;
  sources: Record<string, string>;
  imageHash: string | null;
  result: Result;
}
function normalized(result: Result, repeats: number) {
  return result.records.map((record) => ({
    workload: record.workload,
    boundary: record.boundary,
    cpu: pairedCPU(
      record.samples.filter((s) => s.arm === "control").map((s) => s.cpuMs / repeats),
      record.samples.filter((s) => s.arm === "candidate").map((s) => s.cpuMs / repeats),
    ),
    wall: pairedCPU(
      record.samples.filter((s) => s.arm === "control").map((s) => s.wallMs / repeats),
      record.samples.filter((s) => s.arm === "candidate").map((s) => s.wallMs / repeats),
    ),
  }));
}
/** Independent lifecycle audit is also applied to smoke reports; timing claims are forbidden there. */
export function auditRootWorkers(result: Result, repeats: number): void {
  if (result.pairs !== 9 || result.records.length !== 1)
    throw new Error("Missing independent workload");
  const pids = new Set<number>();
  for (const record of result.records) {
    const arithmetic = pairedCPU(
      record.samples.filter((sample) => sample.arm === "control").map((sample) => sample.cpuMs),
      record.samples.filter((sample) => sample.arm === "candidate").map((sample) => sample.cpuMs),
    );
    if (!isDeepStrictEqual(arithmetic, record.acceptance))
      throw new Error("Independent paired arithmetic mismatch");
    if (record.samples.length !== 18) throw new Error("Exactly nine independent pairs required");
    for (let pair = 0; pair < 9; pair++) {
      const samples = record.samples.filter((s) => s.cohort === pair);
      if (
        !isDeepStrictEqual(
          samples.map((s) => s.arm),
          pair % 2 ? ["candidate", "control"] : ["control", "candidate"],
        )
      )
        throw new Error("Build start order was not balanced");
      for (const sample of samples) {
        if (
          !Number.isSafeInteger(sample.identity.pid) ||
          sample.identity.pid <= 0 ||
          pids.has(sample.identity.pid)
        )
          throw new Error("Timed worker was reused");
        pids.add(sample.identity.pid);
        const detail = sample.detail as { completedLifetimes?: number; rowsPerLifetime?: number };
        if (
          detail.completedLifetimes !== repeats ||
          detail.rowsPerLifetime === undefined ||
          sample.rows !== repeats * detail.rowsPerLifetime
        )
          throw new Error("Incomplete independent query volume");
      }
    }
  }
}
export async function runQueryRootBench(mode: "whole" | "component" = "whole") {
  const aa = process.argv.includes("--aa"),
    smoke = process.argv.includes("--smoke") || process.argv.includes("--check");
  const rows = Number(option("--rows", process.argv.includes("--check") ? "7" : "1000000")),
    repeats = Number(option("--repeats", "10")),
    warmups = Number(option("--warmups", "10"));
  if (
    ![rows, repeats, warmups].every((n) => Number.isSafeInteger(n) && n > 0) ||
    rows > 1000000 ||
    repeats > 1000 ||
    warmups > 1000
  )
    throw new Error("Invalid query observation policy");
  const selectedWorkload = mode === "whole" ? "scan_plain3" : "root_handoff";
  if (option("--only", selectedWorkload) !== selectedWorkload)
    throw new Error("This runner measures only its declared root workload");
  const parentRoot = resolve(option("--baseline")),
    candidateRoot = resolve(option("--candidate"));
  if (!option("--baseline") || !option("--candidate"))
    throw new Error("Complete frozen --baseline and --candidate required");
  const parent = await manifest(parentRoot),
    candidate = await manifest(candidateRoot);
  const output =
    option("--output") || (await mkdtemp(join(tmpdir(), "minnow-query-root-independent-")));
  await mkdir(output, { recursive: true });
  if (
    await access(join(output, "samples.jsonl")).then(
      () => true,
      () => false,
    )
  )
    throw new Error("Refusing to overwrite independent evidence");
  const files = [
    "query-root-bench.mts",
    "query-root-component.mts",
    "next-executor-quiet.mts",
    "next-percentile-component.mts",
    "query-root-runtime-worker.mts",
    "query-root-component-worker.mts",
    "next-sort-runtime-worker.mts",
    "lib/independent-cpu.mts",
    "lib/native-storage-image.mts",
    "lib/query-root-lifetimes.mts",
    "lib/paired-cpu.mts",
    "next-executor-workload/gen.mjs",
    "next-executor-workload/queries.mjs",
  ];
  const sources = Object.fromEntries(
    await Promise.all(
      files.map(async (file) => [file, sha(await readFile(new URL(file, import.meta.url)))]),
    ),
  );
  const abort = new AbortController();
  const stop = () => abort.abort(new Error("Root campaign cancelled"));
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
  const workloadRoot = fileURLToPath(new URL("./next-executor-workload", import.meta.url));
  const event = (value: unknown) =>
    appendFileSync(join(output, "events.jsonl"), JSON.stringify(value) + "\n");
  const makeSQL = (root: string, revision: string, data: Partial<RootRunConfig> = {}): CPUArm => ({
    module: fileURLToPath(new URL("./query-root-runtime-worker.mts", import.meta.url)),
    cwd: root,
    revision,
    execArgv: ["--max-old-space-size=2048"],
    data: { runtimeRoot: root, workloadRoot, rows, repeats, warmups, ...data },
  });
  try {
    let bundle: Bundle | undefined;
    if (mode === "whole") {
      if (option("--bundle"))
        bundle = JSON.parse(await readFile(resolve(option("--bundle")), "utf8")) as Bundle;
      else {
        const worker = await startCPUWorker(
          makeSQL(parentRoot, parent.revision),
          { arm: "control", cohort: -1, workload: "scan_plain3", boundary: "frames" },
          event,
          abort.signal,
        );
        try {
          const imageIdentity = `B2-root-${parent.revision}-${String(rows)}-${sources["next-executor-workload/gen.mjs"] ?? ""}`;
          const capture = await worker.ask<{
            image: unknown;
            fingerprints: Record<string, string>;
            identity: unknown;
          }>({
            op: "command",
            name: "capture-image",
            data: {
              directory: join(output, "image"),
              identity: imageIdentity,
              workloads: ["scan_plain3"],
            },
          });
          bundle = {
            rows,
            image: resolve(join(output, "image")),
            imageIdentity,
            expected: capture.fingerprints,
          };
          await writeFile(
            join(output, "input.json"),
            JSON.stringify({ ...bundle, capture }, null, 2) + "\n",
          );
        } finally {
          await worker.close();
        }
      }
      if (bundle.rows !== rows || !bundle.expected.scan_plain3)
        throw new Error("Native image workload mismatch");
      if (process.argv.includes("--capture")) {
        console.log(JSON.stringify({ bundle: join(output, "input.json") }));
        return;
      }
    }
    const imageHash = bundle ? sha(await readFile(join(bundle.image, "manifest.json"))) : null;
    if (!aa && !smoke) {
      const path = option("--aa-report");
      if (!path) throw new Error("A verified identical-build --aa-report is required before A/B");
      const checked = JSON.parse(await readFile(path, "utf8")) as AAReport;
      if (
        checked.purpose !== "identical-build-control" ||
        checked.smoke ||
        checked.mode !== mode ||
        checked.rows !== rows ||
        checked.repeats !== repeats ||
        checked.warmups !== warmups ||
        checked.candidateRevision !== candidate.revision ||
        checked.imageHash !== imageHash ||
        !isDeepStrictEqual(checked.sources, sources)
      )
        throw new Error("A/A control policy or source mismatch");
      auditRootWorkers(checked.result, repeats);
      if (
        checked.result.purpose !== "identical-build-control" ||
        checked.result.records.some((r) =>
          r.samples.some((s) => s.identity.revision !== candidate.revision),
        )
      )
        throw new Error("A/A build identity mismatch");
      if (
        checked.result.records.some(
          (r) => r.acceptance.acceptedGain || r.acceptance.significantRegression,
        )
      )
        throw new Error("Identical-build A/A control showed a false signal");
    }
    const spec = (root: string, revision: string, cached: boolean): CPUArm => {
      if (mode === "whole") {
        if (bundle === undefined) throw new Error("Missing native image");
        return makeSQL(root, revision, {
          image: bundle.image,
          imageIdentity: bundle.imageIdentity,
          expected: bundle.expected,
        });
      }
      return {
        module: fileURLToPath(new URL("./query-root-component-worker.mts", import.meta.url)),
        cwd: root,
        revision,
        execArgv: ["--max-old-space-size=2048"],
        data: { runtimeRoot: root, cached, warmups, repeats } satisfies RootComponentConfig,
      };
    };
    const result = await runIndependentCPU({
      control: spec(aa ? candidateRoot : parentRoot, aa ? candidate.revision : parent.revision, aa),
      candidate: spec(candidateRoot, candidate.revision, true),
      purpose: aa ? "identical-build-control" : "true-parent",
      workloads: [mode === "whole" ? "scan_plain3" : "root_handoff"],
      boundaries: [mode === "whole" ? "frames" : "roots"],
      primaryBoundary: mode === "whole" ? "frames" : "roots",
      pairs: 9,
      settleMs: smoke ? 0 : 1000,
      signal: abort.signal,
      onEvent: event,
      onSample: (sample) => {
        appendFileSync(join(output, "samples.jsonl"), JSON.stringify(sample) + "\n");
        console.error(JSON.stringify(sample));
      },
    });
    auditRootWorkers(result, repeats);
    const report = {
      purpose: result.purpose,
      smoke,
      mode,
      rows,
      warmups,
      repeats,
      candidateRevision: candidate.revision,
      parentRevision: parent.revision,
      sources,
      imageHash,
      result,
      normalized: normalized(result, repeats),
    };
    await writeFile(join(output, "results.json"), JSON.stringify(report, null, 2) + "\n");
    console.log(JSON.stringify(report.normalized));
    if (
      aa &&
      !smoke &&
      result.records.some((r) => r.acceptance.acceptedGain || r.acceptance.significantRegression)
    )
      throw new Error(
        "Identical-build A/A control showed a false signal; no A/B acceptance allowed",
      );
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
  }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  await runQueryRootBench();
