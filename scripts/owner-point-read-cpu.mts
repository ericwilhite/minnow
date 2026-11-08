/** Five fresh-process paired rounds; CPU-only timings, profile kept separate from measured blocks. */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "esbuild";
const args = process.argv.slice(2);
if (args.length !== 0 && (args.length !== 2 || args[0] !== "--baseline" || !args[1]))
  throw new Error("Usage: npm run benchmark:owner:cpu -- [--baseline frozen-frame-buffers.ts]");
const out = mkdtempSync(join(tmpdir(), "minnow-owner-cpu-"));
// --baseline is an explicitly supplied frozen source file, never an inferred Git dependency.
const baselineAt = args.indexOf("--baseline");
const variants = baselineAt < 0 ? ["candidate"] : ["baseline", "candidate"];
for (const variant of variants) {
  await build({
    entryPoints: ["scripts/owner-point-read-cpu-worker.mts"],
    bundle: true,
    platform: "node",
    format: "esm",
    outfile: join(out, variant + ".mjs"),
    plugins:
      variant === "baseline"
        ? [
            {
              name: "frozen-frame-validator",
              setup(b) {
                b.onLoad({ filter: /\/next\/exec\/frame-buffers\.ts$/ }, () => ({
                  contents: readFileSync(resolve(args[baselineAt + 1] ?? ""), "utf8"),
                  loader: "ts",
                }));
              },
            },
          ]
        : [],
  });
}
const rows = [];
for (let round = 0; round < 5; round++)
  for (const mode of ["frame", "frame-wide", "full"])
    for (const variant of round % 2 === 0 ? variants : [...variants].reverse()) {
      const row = {
        round,
        variant,
        ...(JSON.parse(
          execFileSync(process.execPath, [join(out, variant + ".mjs"), mode], { encoding: "utf8" }),
        ) as object),
      };
      rows.push(row);
      console.log(JSON.stringify(row));
    }
const profile = JSON.parse(
  execFileSync(
    process.execPath,
    [join(out, "candidate.mjs"), "full", join(out, "prepared.cpuprofile")],
    { encoding: "utf8" },
  ),
) as unknown;
writeFileSync(join(out, "report.json"), JSON.stringify({ rows, profile }, null, 2) + "\n");
console.log(`CPU report and sampled component profile: ${out}`);
