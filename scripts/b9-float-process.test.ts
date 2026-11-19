/** Branch-only probe law: tolerant cross-plan comparison never permits within-plan drift. */
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { runIndependentCPU } from "./lib/independent-cpu.mts";
import { compareFloatResults, sameResultBits } from "./b9-float-law.mts";

it("keeps raw verification, rejects a 1e-6 SUM and exact-bit drift in real workers", async () => {
  const directory = await mkdtemp(join(tmpdir(), "minnow-b9-float-")),
    module = join(directory, "worker.mjs"),
    helper = new URL("./b9-float-law.mts", import.meta.url).href;
  await writeFile(
    module,
    `
import {createHash} from 'node:crypto';
import {rawRow} from ${JSON.stringify(helper)};
export default async function(context) {
  let ran=false;
  const flags=context.data;
  return {revision:context.revision, workloads:[context.workload], boundaries:[context.boundary],
    async verify() {
      const value=1000*(1+(flags.delta??0))+(flags.drift&&ran?Number.EPSILON*1024:0);
      const rows=[rawRow([value],['float64'])];
      const digest=createHash('sha256').update(rows.join('\\n')).digest('hex');
      return {fingerprint:flags.constantHash?'legacy-stable':digest,determinismFingerprint:digest,rows:1,
        detail:{raw:{version:1,schema:[{name:'sum',kind:'float64',type:'float64'}],eligible:[true],rows,planIdentity:context.revision}}};
    },
    async run(){ran=true;return {rows:1};},async close(){}
  };
}
`,
  );
  const arm = (revision: string, data: Record<string, unknown>) => ({
      module,
      cwd: directory,
      revision,
      data,
    }),
    options = {
      control: arm("parent", {}),
      candidate: arm("candidate", { delta: 1e-12 }),
      workloads: ["sum"],
      boundaries: ["query"],
      primaryBoundary: "query",
      settleMs: 0,
      compareVerification: compareFloatResults,
    };
  try {
    const records: unknown[] = [];
    const report = await runIndependentCPU({
      ...options,
      onVerification: async (value) => {
        await Promise.resolve();
        records.push(value);
      },
    });
    expect(report.records[0]?.samples).toHaveLength(18);
    expect(records).toHaveLength(36);
    await expect(
      runIndependentCPU({ ...options, candidate: arm("candidate", { delta: 1e-6 }) }),
    ).rejects.toThrow("CPU workload differs");
    await expect(
      runIndependentCPU({
        ...options,
        control: arm("parent", { constantHash: true }),
        candidate: arm("candidate", { constantHash: true, drift: true }),
      }),
    ).rejects.toThrow("CPU output changed after measurement");
    await expect(
      runIndependentCPU({ ...options, compareVerification: sameResultBits }),
    ).rejects.toThrow("CPU workload differs");
    // The shared harness default remains exact when no probe-specific law is supplied.
    const { compareVerification, ...strict } = options;
    expect(compareVerification).toBe(compareFloatResults);
    await expect(runIndependentCPU(strict)).rejects.toThrow("CPU workload differs");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 60000);
