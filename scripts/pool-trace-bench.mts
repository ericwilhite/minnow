/** Replay a captured production export pool prefix: five interleaved fresh-process CPU rounds.
 * Whole component: requests, allocation, synthetic writes, real transfer detachment, issue/return
 * validation and pool eviction/clearing. Excludes fixture load, warmup/GC, IO, decode and framing.
 * Uses os.tmpdir(); callers choose TMPDIR. --compare=REV --trace=/path/to/trace.json.gz
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "esbuild";
import { executorSnapshot } from "./browser-perf/executor-comparison.js";
const reference = process.argv.find((a) => a.startsWith("--compare="))?.slice(10);
const trace = process.argv.find((a) => a.startsWith("--trace="))?.slice(8);
if (!reference || !trace) throw new Error("Use --compare=REV --trace=FILE.json.gz");
const capMiB = Number(process.argv.find((a) => a.startsWith("--cap-mib="))?.slice(10) ?? 4);
if (!Number.isSafeInteger(capMiB) || capMiB < 1 || capMiB > 128)
  throw new Error("Invalid bounded candidate cap");
const bucketed = process.argv.includes("--bucketed");
const scratch = mkdtempSync(join(tmpdir(), "minnow-pool-replay-"));
try {
  const worker = `import {readFileSync}from"node:fs";import{gunzipSync}from"node:zlib";
 import{replayPoolTrace}from"./scripts/browser-perf/page/pool-trace-replay.ts";
 const trace=JSON.parse(gunzipSync(readFileSync(process.argv[2])).toString()),variant=process.argv[3];
 const bucketed=process.argv[5]==="true";
 const cap=variant==="baseline"?trace.capBytes:Number(process.argv[4]);
 await replayPoolTrace(trace,variant==="baseline",cap,bucketed);if(typeof global.gc!=="function")throw new Error("expose GC");global.gc();
 console.log(JSON.stringify({variant,...await replayPoolTrace(trace,variant==="baseline",cap,bucketed)}));`;
  for (const variant of ["baseline", "candidate"]) {
    await build({
      stdin: { contents: worker, resolveDir: process.cwd(), loader: "ts" },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: join(scratch, variant + ".mjs"),
      ...(variant === "baseline" ? { plugins: [executorSnapshot(reference).esbuild] } : {}),
    });
  }
  const samples: Array<{
    variant: string;
    cpuMs: number;
    threadCpuMs: number;
    wallMs: number;
    measuredRssPeakMiB: number;
    measuredRssGrowthMiB: number;
  }> = [];
  for (let round = 0; round < 5; round++)
    for (const variant of round % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"]) {
      const row = JSON.parse(
        execFileSync(
          process.execPath,
          [
            "--expose-gc",
            join(scratch, variant + ".mjs"),
            resolve(trace),
            variant,
            String(capMiB * 1048576),
            String(variant === "candidate" && bucketed),
          ],
          { encoding: "utf8", timeout: 120000 },
        ),
      ) as (typeof samples)[number];
      samples.push(row);
      console.log(JSON.stringify({ round, hostLoad: loadavg(), ...row }));
    }
  for (const variant of ["baseline", "candidate"]) {
    const rows = samples.filter((s) => s.variant === variant);
    console.log(
      JSON.stringify({
        variant,
        ...Object.fromEntries(
          (
            [
              "cpuMs",
              "threadCpuMs",
              "wallMs",
              "measuredRssPeakMiB",
              "measuredRssGrowthMiB",
            ] as const
          ).map((key) => {
            const v = rows.map((r) => r[key]).sort((a, b) => a - b);
            return [key, { median: v[2], min: v[0], max: v[4] }];
          }),
        ),
      }),
    );
  }
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
