/** External task watchdog: a synchronous join loop cannot hide behind Vitest's own timer. */
import { build } from "esbuild";
import { Worker } from "node:worker_threads";
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, expect, it } from "vitest";
let scratch: string, bundle: string;
beforeAll(async () => {
  const base = join(homedir(), ".cache", "minnow-key-only");
  await mkdir(base, { recursive: true });
  scratch = await mkdtemp(join(base, "case-"));
  bundle = join(scratch, "watchdog.mjs");
  await build({
    stdin: {
      contents: `import {parentPort} from 'node:worker_threads';import {runJoin,INT} from ${JSON.stringify(join(process.cwd(), "packages/core/src/next/exec/join-fixtures.ts"))};const r=await runJoin({kind:'inner',probeTypes:[INT],buildTypes:[INT],probeKeys:[0],buildKeys:[0],nullSafe:[false],residual:null,publishes:[]},[[1]],[[1]],{cap:8<<20});await new Promise(r=>setTimeout(r,0));parentPort.postMessage(r.rows);parentPort.close();`,
      resolveDir: process.cwd(),
    },
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: bundle,
  });
}, 60000);
afterAll(async () => {
  await rm(scratch, { recursive: true, force: true });
});
it("one-row key-only join completes under an external worker watchdog", async () => {
  const worker = new Worker(bundle);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const rows = await new Promise<unknown>((resolve, reject) => {
      timer = setTimeout(
        () => reject(new Error("key-only join blocked worker task/cancellation watchdog")),
        5000,
      );
      worker.once("message", resolve);
      worker.once("error", reject);
    });
    expect(rows).toEqual([[1, 1]]);
  } finally {
    clearTimeout(timer);
    await worker.terminate();
  }
}, 10000);
