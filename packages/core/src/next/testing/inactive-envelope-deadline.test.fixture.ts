/** A parent-process deadline also catches synchronous front-end stalls. */
import { spawn } from "node:child_process";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export async function inactiveEnvelopeChild(seeds: readonly number[]): Promise<void> {
  const root = fileURLToPath(new URL("../../../../../", import.meta.url));
  const directory = await mkdtemp(join(tmpdir(), "minnow-inactive-envelope-"));
  const config = join(directory, "vitest.config.mjs"),
    report = join(directory, "result.json");
  try {
    await writeFile(
      config,
      `export default ${JSON.stringify({ test: { pool: "threads", maxWorkers: 1, env: { MINNOW_VERIFY_CACHED_BYTES: "1" }, setupFiles: process.env.MINNOW_FORCE_PACKING === "1" ? [join(root, "scripts/forced-packing.setup.ts")] : [] } })};\n`,
    );
    await new Promise<void>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        [
          join(root, "node_modules/vitest/vitest.mjs"),
          "run",
          "packages/core/src/next/testing/executor-inactive-envelope.test.ts",
          "--config",
          config,
          "--maxWorkers=1",
          "--reporter=json",
          `--outputFile=${report}`,
        ],
        {
          cwd: root,
          env: { ...process.env, MINNOW_INACTIVE_ENVELOPE_CHILD: "1" },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      let output = "",
        expired = false;
      const collect = (data: Buffer) => {
        output = (output + data.toString()).slice(-65536);
      };
      child.stdout.on("data", collect);
      child.stderr.on("data", collect);
      // The child uses worker threads, so terminating this retained PID also ends the
      // blocked test. No process lookup, name/pattern kill, or shared process group.
      const stop = () => {
        child.kill("SIGKILL");
      };
      process.once("exit", stop);
      const timer = setTimeout(() => {
        expired = true;
        stop();
      }, 90000);
      child.once("error", reject);
      child.once("close", (code, signal) => {
        clearTimeout(timer);
        process.removeListener("exit", stop);
        if (expired)
          reject(
            new Error(
              `Inactive envelope seeds ${seeds.join(", ")} exceeded their 90 s child deadline; child closed (${signal ?? String(code)}).\n${output}`,
            ),
          );
        else if (code !== 0)
          reject(
            new Error(
              `Inactive envelope seeds ${seeds.join(", ")} failed (${signal ?? String(code)}).\n${output}`,
            ),
          );
        else resolve();
      });
    });
    const result = JSON.parse(await readFile(report, "utf8")) as {
      success: boolean;
      numTotalTests: number;
      numPassedTests: number;
    };
    if (
      !result.success ||
      result.numTotalTests !== seeds.length ||
      result.numPassedTests !== seeds.length
    )
      throw new Error("The bounded child must run every original campaign seed");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
