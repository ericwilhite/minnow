/** Five fresh-process interleaved parent/candidate rounds; os.tmpdir honors shell TMPDIR. */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, mkdirSync, copyFileSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir, loadavg } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { build } from "esbuild";
import { executorSnapshot } from "./browser-perf/executor-comparison.js";
const ref = process.argv.find((a) => a.startsWith("--compare="))?.slice(10);
if (!ref) throw Error("--compare=REV required");
const prepared = process.argv.find((a) => a.startsWith("--prepare-point="))?.slice(16);
const dir = prepared ?? mkdtempSync(join(tmpdir(), "read-retention-"));
mkdirSync(dir, { recursive: true });
try {
  for (const variant of ["baseline", "candidate"]) {
    await build({
      stdin: {
        contents: prepared
          ? 'import{readRetentionComponent}from"./scripts/browser-perf/page/read-retention.ts";console.log(JSON.stringify(await readRetentionComponent()));'
          : 'import{readRetentionComponent,ordinaryAndExport,storedWarmQueries}from"./scripts/browser-perf/page/read-retention.ts";console.log(JSON.stringify({component:await readRetentionComponent(),api:await ordinaryAndExport(),stored:await storedWarmQueries()}));',
        resolveDir: process.cwd(),
        loader: "ts",
      },
      platform: "node",
      format: "esm",
      bundle: true,
      outfile: join(dir, variant + ".mjs"),
      ...(variant === "baseline" ? { plugins: [executorSnapshot(ref).esbuild] } : {}),
    });
  }
  if (prepared) {
    copyFileSync("scripts/browser-perf/page/point-quiet-runner.mjs", join(dir, "run.mjs"));
    const files = Object.fromEntries(
      ["baseline.mjs", "candidate.mjs", "run.mjs"].map((name) => [
        name,
        createHash("sha256")
          .update(readFileSync(join(dir, name)))
          .digest("hex"),
      ]),
    );
    writeFileSync(
      join(dir, "manifest.json"),
      JSON.stringify(
        { parent: ref, candidate: process.env.MINNOW_POINT_TIP ?? "UNSPECIFIED", files },
        null,
        2,
      ),
    );
    console.log("Prepared frozen point arms; no measured workload executed");
  } else {
    const rows = [];
    for (let round = 0; round < 5; round++)
      for (const variant of round % 2 ? ["candidate", "baseline"] : ["baseline", "candidate"]) {
        const result = JSON.parse(
          execFileSync(process.execPath, [join(dir, variant + ".mjs")], {
            encoding: "utf8",
            timeout: 300000,
          }),
        ) as unknown;
        const row = { round, variant, load: loadavg(), result };
        rows.push(row);
        console.log(JSON.stringify(row));
      }
    // Payload physical bytes, validity and padding, dictionary deltas and headers vs frozen parent.
    const first = rows[0] as
      | {
          result: {
            api: Array<{
              hashes: string[];
              afterExportResident: number;
              ordinary: Array<{ resident: number; backendReads: number }>;
            }>;
          };
        }
      | undefined;
    if (!first) throw Error("no rounds");
    const expected = JSON.stringify(first.result.api.map((c) => c.hashes));
    for (const row of rows) {
      const r = row.result as typeof first.result;
      if (row.variant === "candidate" && r.api.some((c) => c.afterExportResident !== 0))
        throw Error("Export admitted misses");
      if (r.api.some((c) => (c.ordinary[1]?.resident ?? 0) <= 0))
        throw Error("Ordinary query lost cache retention");
      if (JSON.stringify(r.api.map((c) => c.hashes)) !== expected)
        throw Error("Parent/candidate export bytes differ");
    }
  }
} finally {
  if (!prepared) rmSync(dir, { recursive: true, force: true });
}
