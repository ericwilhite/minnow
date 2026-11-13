/** node scripts/hash-transition-cpu.mts CONTROL_ROOT CANDIDATE_ROOT OUTPUT CONTROL_SHA CANDIDATE_SHA */
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runIndependentCPU, type CPUArm } from "./lib/independent-cpu.mts";
const [controlRoot, candidateRoot, output, controlSHA, candidateSHA] = process.argv.slice(2);
if (!controlRoot || !candidateRoot || !output || !controlSHA || !candidateSHA)
  throw new Error("Expected compiled control/candidate roots, output and both revisions");
const arm = (root: string, revision: string): CPUArm => ({
  module: fileURLToPath(new URL("./hash-transition-worker.mts", import.meta.url)),
  cwd: resolve(root),
  revision,
  data: { runtimeRoot: resolve(root) },
});
const control = arm(controlRoot, controlSHA),
  candidate = arm(candidateRoot, candidateSHA);
const config = {
  workloads: ["short-text-hash"],
  boundaries: ["operator", "query"],
  primaryBoundary: "query",
  pairs: 9,
  settleMs: 0,
};
const aa = await runIndependentCPU({
  ...config,
  control: candidate,
  candidate,
  purpose: "identical-build-control",
});
await writeFile(resolve(output), JSON.stringify({ aa }, null, 2) + "\n");
if (aa.records.some((r) => r.acceptance.acceptedGain || r.acceptance.significantRegression))
  throw new Error("A/A signal; preserve raw data and independently repeat");
const ab = await runIndependentCPU({ ...config, control, candidate });
await writeFile(resolve(output), JSON.stringify({ aa, ab }, null, 2) + "\n");
console.log(
  JSON.stringify(
    { aa: aa.records.map((r) => r.acceptance), ab: ab.records.map((r) => r.acceptance) },
    null,
    2,
  ),
);
