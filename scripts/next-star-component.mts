/** Fresh-process star dictionary policy; B7 owns the sole CPU clock and VM lifecycle. */
import { appendFileSync } from "node:fs";
import { mkdir, readFile, writeFile, access } from "node:fs/promises";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { manifest } from "./next-executor-quiet.mts";
import { runIndependentCPU, type CPUArm } from "./lib/independent-cpu.mts";
import { pairedCPU } from "./lib/paired-cpu.mts";
import { auditRootWorkers } from "./query-root-bench.mts";
const option = (name: string, fallback = "") => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const aa = process.argv.includes("--aa"),
  plain = process.argv.includes("--plain");
const mode = option("--mode", "component"),
  rows = Number(option("--rows", "1000000"));
if (
  !["component", "whole"].includes(mode) ||
  !Number.isSafeInteger(rows) ||
  rows < 1 ||
  rows > 1000000 ||
  (plain && (!aa || mode !== "component"))
)
  throw new Error("Invalid fixed star dictionary observation policy");
const root = resolve(option("--control")),
  candidateRoot = resolve(option("--candidate", root));
const control = await manifest(root),
  candidate = await manifest(candidateRoot);
const output = resolve(option("--output", join(tmpdir(), "star-independent")));
await mkdir(output, { recursive: true });
if (
  await access(join(output, "samples.jsonl")).then(
    () => true,
    () => false,
  )
)
  throw new Error("Refusing existing evidence");
const files = [
  "next-star-component.mts",
  "next-star-worker.mts",
  "next-aggregation-runtime-worker.mts",
  "lib/star-fixture.mts",
  "lib/star-light.mts",
  "lib/star-native.mts",
  "lib/independent-cpu.mts",
  "lib/query-root-lifetimes.mts",
  "lib/paired-cpu.mts",
  "next-sort-runtime-worker.mts",
  "next-executor-workload/gen.mjs",
  "next-executor-workload/queries.mjs",
  "next-star-workload/gen.mjs",
  "next-star-workload/queries.mjs",
];
const sha = (v: Uint8Array) => createHash("sha256").update(v).digest("hex");
const sources = Object.fromEntries(
  await Promise.all(files.map(async (f) => [f, sha(await readFile(new URL(f, import.meta.url)))])),
);
const workload = option("--only", "join_star_3dims"),
  boundary = mode === "whole" ? "frames" : "native-component";
let bundle:
  | { rows: number; image: string; imageIdentity: string; expected: Record<string, string> }
  | undefined;
let imageHash: string | null = null;
if (mode === "whole") {
  const bytes = await readFile(resolve(option("--bundle")));
  imageHash = sha(bytes);
  bundle = JSON.parse(bytes.toString()) as typeof bundle;
  if (bundle?.rows !== rows || bundle.expected[workload] === undefined)
    throw new Error("Unverified native input");
}
if (!aa) {
  const negative = JSON.parse(await readFile(resolve(option("--aa-report")), "utf8")) as {
    mode: string;
    plain: boolean;
    rows: number;
    controlRevision: string;
    candidateRevision: string;
    imageHash: string | null;
    sources: unknown;
    result: Awaited<ReturnType<typeof runIndependentCPU>>;
  };
  if (
    negative.mode !== mode ||
    negative.plain ||
    negative.rows !== rows ||
    negative.controlRevision !== control.revision ||
    negative.candidateRevision !== control.revision ||
    negative.imageHash !== imageHash ||
    !isDeepStrictEqual(negative.sources, sources) ||
    negative.result.records.some(
      (r) => r.acceptance.acceptedGain || r.acceptance.significantRegression,
    ) ||
    negative.result.records[0]?.workload !== workload
  )
    throw new Error("Matching neutral identical-build A/A required before A/B");
  auditRootWorkers(negative.result, 10);
}
const abort = new AbortController();
const stop = () => abort.abort(new Error("star dictionary campaign cancelled"));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
try {
  const arm = (runtimeRoot: string, revision: string, candidateArm: boolean): CPUArm => ({
    module: fileURLToPath(
      new URL(
        mode === "whole" ? "./next-aggregation-runtime-worker.mts" : "./next-star-worker.mts",
        import.meta.url,
      ),
    ),
    cwd: runtimeRoot,
    revision,
    execArgv: ["--max-old-space-size=2048"],
    data:
      mode === "whole"
        ? {
            runtimeRoot,
            workloadRoot: resolve(
              option(
                "--workload-root",
                fileURLToPath(new URL("./next-executor-workload", import.meta.url)),
              ),
            ),
            rows,
            image: bundle?.image,
            imageIdentity: bundle?.imageIdentity,
            expected: bundle?.expected,
          }
        : { runtimeRoot, rows, plain, encoded: !aa && candidateArm },
  });
  const result = await runIndependentCPU({
    control: arm(root, control.revision, false),
    candidate: arm(candidateRoot, candidate.revision, true),
    workloads: [workload],
    boundaries: [boundary],
    primaryBoundary: boundary,
    pairs: 9,
    purpose: aa ? "identical-build-control" : "true-parent",
    signal: abort.signal,
    onEvent: (value) => appendFileSync(join(output, "events.jsonl"), JSON.stringify(value) + "\n"),
    onSample: (sample) => {
      appendFileSync(join(output, "samples.jsonl"), JSON.stringify(sample) + "\n");
      const value = sample as { arm: string; cohort: number; cpuMs: number };
      console.error(
        JSON.stringify({ arm: value.arm, cohort: value.cohort, cpuMs: value.cpuMs / 10 }),
      );
    },
  });
  auditRootWorkers(result, 10);
  const normalized = result.records.map((r) => ({
    workload: r.workload,
    boundary: r.boundary,
    cpu: pairedCPU(
      r.acceptance.pairs.map((p) => p.control / 10),
      r.acceptance.pairs.map((p) => p.candidate / 10),
    ),
  }));
  await writeFile(
    join(output, "result.json"),
    JSON.stringify(
      {
        mode,
        plain,
        rows,
        controlRevision: control.revision,
        candidateRevision: candidate.revision,
        sources,
        imageHash,
        warmups: 10,
        lifetimes: 10,
        scope:
          mode === "whole"
            ? "SQL lowering/prepare/admission/scan/operator/delivered frames; native restored input"
            : "already-decoded typed arrays; private fixed-grant admission plus fresh operator allocation/transition/final output+ordering; no SQL/scan/ResultSink; certified plain bound is diagnostic",
        result,
        normalized,
      },
      null,
      2,
    ) + "\n",
  );
} finally {
  process.off("SIGINT", stop);
  process.off("SIGTERM", stop);
}
