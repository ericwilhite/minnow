/** Synthetic runner control checks only: no actual point workload or performance claim. */
import { it, expect } from "vitest";
import { mkdtempSync, writeFileSync, readFileSync, copyFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
it("validates frozen setup without executing an arm, and preserves nine synthetic pairs", () => {
  const dir = mkdtempSync(join(tmpdir(), "point-queue-test-"));
  try {
    copyFileSync(
      fileURLToPath(new URL("./point-quiet-runner.mjs", import.meta.url)),
      join(dir, "run.mjs"),
    );
    for (const arm of ["baseline", "candidate"]) {
      writeFileSync(
        join(dir, arm + ".mjs"),
        `import{appendFileSync,mkdirSync,writeFileSync}from'node:fs';appendFileSync(${JSON.stringify(join(dir, "called"))},'${arm}\\n');if(process.env.MINNOW_POINT_PROFILE_DIR){mkdirSync(process.env.MINNOW_POINT_PROFILE_DIR,{recursive:true});for(const n of ['point.cpuprofile','gc.json'])writeFileSync(process.env.MINNOW_POINT_PROFILE_DIR+'/'+n,'{}');}console.log(JSON.stringify({warmPoint:{cpuMs:${arm === "baseline" ? 10 : 30},wallMs:1,backendReads:0}}));`,
      );
    }
    const manifest = {
      parent: "97192fcd",
      candidate: "a".repeat(40),
      files: Object.fromEntries(
        ["baseline.mjs", "candidate.mjs", "run.mjs"].map((n) => [
          n,
          createHash("sha256")
            .update(readFileSync(join(dir, n)))
            .digest("hex"),
        ]),
      ),
    };
    writeFileSync(join(dir, "manifest.json"), JSON.stringify(manifest));
    execFileSync(process.execPath, [join(dir, "run.mjs"), "--check"]);
    expect(() => readFileSync(join(dir, "called"))).toThrow();
    writeFileSync(
      join(dir, "load.mjs"),
      "import os from 'node:os';import{syncBuiltinESMExports}from'node:module';os.loadavg=()=>[0,0,0];syncBuiltinESMExports();",
    );
    execFileSync(process.execPath, ["--import", join(dir, "load.mjs"), join(dir, "run.mjs")], {
      env: { ...process.env, MINNOW_QUIET_ITEM_OUT: join(dir, "out") },
    });
    const rows = JSON.parse(readFileSync(join(dir, "out", "pairs.json"), "utf8")) as Array<{
      round: number;
      variant: string;
    }>;
    expect(rows.map((r) => r.variant)).toEqual(
      Array.from({ length: 9 }, (_, i) =>
        i % 2 ? ["candidate", "baseline"] : ["baseline", "candidate"],
      ).flat(),
    );
    expect(
      JSON.parse(readFileSync(join(dir, "out", "comparison.json"), "utf8")).metrics.cpuMs,
    ).toMatchObject({ median: 20, iqr: 0, positive: 9, decision: "REGRESSION" });
    expect(readFileSync(join(dir, "called"), "utf8").trim().split("\n")).toHaveLength(20);
    writeFileSync(join(dir, "candidate.mjs"), "throw Error('changed');");
    expect(() =>
      execFileSync(process.execPath, [join(dir, "run.mjs"), "--check"], { stdio: "pipe" }),
    ).toThrow();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
