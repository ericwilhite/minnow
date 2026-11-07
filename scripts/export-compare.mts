/**
 * Compare the complete file-backed export with p2i on identical stored data, five interleaved
 * rounds in fresh processes, after one untimed priming pass per variant. OS cache is not
 * dropped (host pressure can evict it); this is not the OPFS cold Gate A check.
 * Bundling substitutes the baseline's next/ modules without touching the checkout.
 *   npx vite-node scripts/export-compare.mts --dir ~/.cache/minnow-export [--baseline a01c4f86]
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { build } from "esbuild";

const args = process.argv.slice(2);
const option = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index < 0 ? fallback : (args[index + 1] ?? fallback);
};
const baseline = option("baseline", "a01c4f86");
const rounds = Number(option("rounds", "5"));
if (!Number.isInteger(rounds) || rounds < 5) throw new Error("Need at least five rounds");
const scratch = mkdtempSync(join(tmpdir(), "minnow-export-compare-"));
const variants = ["baseline", "current"] as const;
interface Sample {
  cpuSeconds: number;
  rowsPerCpuSecond: number;
  rowsPerSecond: number;
  rssDeltaMiB: number;
}
const samples: Record<(typeof variants)[number], Sample[]> = { baseline: [], current: [] };
try {
  for (const variant of variants)
    await build({
      entryPoints: ["scripts/export-bench.mts"],
      outfile: join(scratch, `${variant}.mjs`),
      bundle: true,
      platform: "node",
      format: "esm",
      target: "node24",
      plugins:
        variant === "current"
          ? []
          : [
              {
                name: "baseline",
                setup(builder) {
                  builder.onLoad({ filter: /[/\\]next[/\\].*\.ts$/ }, ({ path }) => ({
                    contents: execFileSync(
                      "git",
                      ["show", `${baseline}:${relative(process.cwd(), path)}`],
                      { encoding: "utf8" },
                    ),
                    loader: "ts",
                  }));
                },
              },
            ],
    });
  for (let round = 0; round <= rounds; round++)
    for (let offset = 0; offset < 2; offset++) {
      const variant = defined(variants[(round + offset) % 2]);
      const out = execFileSync(
        process.execPath,
        [
          "--expose-gc",
          join(scratch, `${variant}.mjs`),
          "--dir",
          option("dir", join(tmpdir(), "minnow-export")),
          "--groups",
          option("groups", "16"),
          "--window-mib",
          "64",
        ],
        { encoding: "utf8", timeout: 120_000 },
      );
      const line = out.split("\n").find((line) => line.startsWith("EXPORT_SAMPLE "));
      if (line === undefined) throw new Error(out);
      const sample = JSON.parse(line.slice("EXPORT_SAMPLE ".length)) as Sample;
      if (round === 0) continue; // untimed priming, one complete pass per variant
      samples[variant].push(sample);
      console.log(JSON.stringify({ round: round - 1, variant, sample }));
    }
  const summary = variants.map((variant) => ({
    variant,
    metrics: Object.fromEntries(
      (["cpuSeconds", "rowsPerCpuSecond", "rowsPerSecond", "rssDeltaMiB"] as const).map((key) => {
        const values = samples[variant].map((sample) => sample[key]).sort((a, b) => a - b);
        return [
          key,
          { median: values[Math.floor(values.length / 2)], min: values[0], max: values.at(-1) },
        ];
      }),
    ),
  }));
  console.log(JSON.stringify({ baseline, rounds, summary }));
} finally {
  rmSync(scratch, { recursive: true, force: true });
}

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Missing benchmark sample");
  return value;
}
