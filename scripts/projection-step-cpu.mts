/** node scripts/projection-step-cpu.mts MAIN_ROOT CANDIDATE_ROOT OUTPUT_JSON */
import { writeFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runIndependentCPU, type CPUArm } from "./lib/independent-cpu.mts";
const [before, after, output, controlRevision, candidateRevision] = process.argv.slice(2);
if (!before || !after || !output)
  throw new Error("Expected main root, candidate root, output JSON");
const arm = (root: string, revision?: string): CPUArm => ({
  cwd: resolve(root),
  module: fileURLToPath(new URL("./projection-step-worker.mts", import.meta.url)),
  revision:
    revision ?? execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
  data: { runtimeRoot: resolve(root), pushes: 10000, warmups: 3000 },
});
const control = arm(before, controlRevision),
  candidate = arm(after, candidateRevision);
const options = {
  workloads: ["case1024"],
  boundaries: ["operator"],
  primaryBoundary: "operator",
  pairs: 9,
  settleMs: 0,
};
const aa = await runIndependentCPU({
  ...options,
  control: candidate,
  candidate,
  purpose: "identical-build-control",
});
await writeFile(resolve(output), JSON.stringify({ aa }, null, 2) + "\n");
if (aa.records.some((r) => r.acceptance.acceptedGain || r.acceptance.significantRegression))
  throw new Error("A/A showed a false signal; retain evidence and repeat independently");
const ab = await runIndependentCPU({ ...options, control, candidate });
await writeFile(resolve(output), JSON.stringify({ aa, ab }, null, 2) + "\n");
console.log(
  JSON.stringify(
    { aa: aa.records.map((r) => r.acceptance), ab: ab.records.map((r) => r.acceptance) },
    null,
    2,
  ),
);
