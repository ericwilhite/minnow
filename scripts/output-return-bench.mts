/** Dedicated output allocation/transfer/return component: five interleaved fresh processes.
 * Each child warms its selected path, collects, then times 512 frames of 64 × 8 KiB outputs.
 * Includes charging, fill, real structured-clone detachment, return validation and zeroing.
 * Excludes process startup, warmup, explicit GC, IO, scan, framing and row construction.
 * npx vite-node scripts/output-return-bench.mts (scratch/TMPDIR under ~/.cache).
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { loadavg, tmpdir } from "node:os";
import { join } from "node:path";
import { Governor } from "../packages/core/src/next/memory/governor.js";
import { ReturnPool } from "../packages/core/src/next/exec/return-pool.js";
async function run(reuse: boolean) {
  const governor = new Governor(64 * 1048576),
    grant = await governor.admit({ label: "frames", minimumBytes: 1048576, spillable: true }),
    pool = new ReturnPool(governor, 2 * 1048576);
  const before = process.cpuUsage(),
    threadBefore = process.threadCpuUsage(),
    start = performance.now();
  for (let frame = 0; frame < 512; frame++) {
    const buffers = Array.from({ length: 64 }, () => {
      if (!grant.allocate(8192)) throw new Error("frame charge");
      const b = reuse ? (pool.take(8192) ?? new ArrayBuffer(8192)) : new ArrayBuffer(8192);
      new Uint8Array(b).fill(frame & 255);
      return b;
    });
    const ids = reuse ? pool.issue(frame, buffers) : [];
    const client = structuredClone(buffers, { transfer: buffers });
    if (reuse) {
      const back = structuredClone(client, { transfer: client });
      pool.release(
        frame,
        back.map((buffer, i) => ({ id: ids[i] ?? -1, buffer })),
      );
    }
    grant.free(64 * 8192);
  }
  const cpu = process.cpuUsage(before),
    thread = process.threadCpuUsage(threadBefore);
  const row = {
    variant: reuse ? "recycle" : "allocate",
    cpuMs: (cpu.user + cpu.system) / 1000,
    threadCpuMs: (thread.user + thread.system) / 1000,
    wallMs: performance.now() - start,
    hits: pool.hits,
  };
  pool.close();
  grant.close();
  if (governor.usedBytes !== 0) throw new Error("charge leak");
  return row;
}
const rows: Array<Awaited<ReturnType<typeof run>>> = [];
if (process.argv.includes("--child")) {
  const reuse = process.argv.includes("--recycle");
  await run(reuse);
  globalThis.gc?.();
  console.log(JSON.stringify(await run(reuse)));
} else {
  const scratch = mkdtempSync(join(tmpdir(), "minnow-output-return-"));
  try {
    const { build } = await import("esbuild");
    const outfile = join(scratch, "component.mjs");
    await build({
      entryPoints: [import.meta.filename],
      outfile,
      bundle: true,
      platform: "node",
      format: "esm",
      packages: "external",
    });
    for (let round = 0; round < 5; round++)
      for (const reuse of round % 2 === 0 ? [false, true] : [true, false]) {
        const row = JSON.parse(
          execFileSync(
            process.execPath,
            ["--expose-gc", outfile, "--child", ...(reuse ? ["--recycle"] : [])],
            { encoding: "utf8" },
          ),
        ) as Awaited<ReturnType<typeof run>>;
        rows.push(row);
        console.log(JSON.stringify({ round, ...row, hostLoad: loadavg() }));
      }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
  for (const variant of ["allocate", "recycle"]) {
    const data = rows.filter((r) => r.variant === variant);
    console.log(
      JSON.stringify({
        variant,
        ...Object.fromEntries(
          ["cpuMs", "threadCpuMs", "wallMs"].map((key) => {
            const v = data
              .map((r) => r[key as "cpuMs" | "threadCpuMs" | "wallMs"])
              .sort((a, b) => a - b);
            return [key, { median: v[2], min: v[0], max: v[4] }];
          }),
        ),
      }),
    );
  }
}
