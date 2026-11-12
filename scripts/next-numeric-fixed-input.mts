/** Create one quiescent real-write 200-column input; no timings and no private data imports. */
import { createHash } from "node:crypto";
import { access, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { startCPUWorker } from "./lib/independent-cpu.mts";
import { manifest } from "./next-executor-quiet.mts";
const option = (key: string, fallback = "") => {
  const at = process.argv.indexOf(key);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
if (!option("--runtime")) throw new Error("A complete frozen --runtime is required");
const runtimeRoot = resolve(option("--runtime"));
const rows = Number(option("--rows", "1000000"));
if (!Number.isSafeInteger(rows) || rows < 1 || rows > 1000000) throw new Error("Invalid row count");
const output = option("--output") || (await mkdtemp(join(tmpdir(), "minnow-numeric-input-")));
await mkdir(output, { recursive: true });
if (
  await access(join(output, "input.json")).then(
    () => true,
    () => false,
  )
)
  throw new Error("Refusing existing input evidence");
const pinned = await manifest(runtimeRoot);
const sources = Object.fromEntries(
  await Promise.all(
    [
      "next-numeric-fixed-input.mts",
      "next-sort-runtime-worker.mts",
      "lib/native-storage-image.mts",
      "next-executor-workload/gen.mjs",
      "next-executor-workload/queries.mjs",
    ].map(async (file) => [
      file,
      createHash("sha256")
        .update(await readFile(new URL(file, import.meta.url)))
        .digest("hex"),
    ]),
  ),
);
const abort = new AbortController();
const stop = () => abort.abort(new Error("Input capture cancelled"));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
try {
  const worker = await startCPUWorker(
    {
      module: fileURLToPath(new URL("./next-sort-runtime-worker.mts", import.meta.url)),
      cwd: runtimeRoot,
      revision: pinned.revision,
      execArgv: ["--max-old-space-size=2048"],
      data: {
        runtimeRoot,
        rows,
        workloadRoot: fileURLToPath(new URL("./next-executor-workload", import.meta.url)),
      },
    },
    { arm: "control", cohort: -1, workload: "agg_numeric_grouped", boundary: "frames" },
    undefined,
    abort.signal,
  );
  try {
    const image = resolve(output, "image"),
      imageIdentity = `NUMERIC-${pinned.revision}-${String(rows)}`;
    const capture = await worker.ask<{
      fingerprints: Record<string, string>;
      image: unknown;
      identity: unknown;
    }>({
      op: "command",
      name: "capture-image",
      data: {
        directory: image,
        identity: imageIdentity,
        workloads: ["agg_numeric_grouped", "agg_numeric_sum"],
      },
    });
    await writeFile(
      join(output, "input.json"),
      JSON.stringify(
        {
          rows,
          image,
          imageIdentity,
          expected: capture.fingerprints,
          capture,
          runtime: pinned,
          sources,
          scope:
            "Quiescent native real-write input; file hashes/logical lengths/retained capacities preserved before close. Not a backup/upgrade API.",
        },
        null,
        2,
      ) + "\n",
    );
    console.log(join(output, "input.json"));
  } finally {
    await worker.close();
  }
} finally {
  process.off("SIGINT", stop);
  process.off("SIGTERM", stop);
}
