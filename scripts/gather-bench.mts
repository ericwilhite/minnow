/**
 * Five rotating rounds of actual ChunkReader.gather, in fresh processes per variant.
 * Bundles production code with one narrowly substituted non-NaN gather loop; no source edits.
 * JSON includes every CPU/wall sample and median/min/max across rounds (ns/gathered row).
 * Number-only is a finite-input comparator, never a permissible NaN implementation.
 *   npx vite-node scripts/gather-bench.mts [--rounds 5]
 */
import { execFileSync } from "node:child_process";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { build } from "esbuild";

const roundArg = process.argv.indexOf("--rounds");
const rounds = Number(roundArg < 0 ? 5 : process.argv[roundArg + 1]);
if (!Number.isInteger(rounds) || rounds < 5) throw new Error("Need at least five rounds");
const scratch = mkdtempSync(join(homedir(), ".cache", "minnow-gather-"));
const variants = ["raw", "number", "production"] as const;
interface Sample {
  density: number;
  wallNsPerRow: number;
  cpuNsPerRow: number;
}
const samples: Record<(typeof variants)[number], Sample[][]> = {
  raw: [],
  number: [],
  production: [],
};
try {
  for (const variant of variants) {
    const outfile = join(scratch, `${variant}.mjs`);
    await build({
      entryPoints: ["scripts/gather-bench-worker.mts"],
      outfile,
      bundle: true,
      platform: "node",
      format: "esm",
      target: "node24",
      plugins: [
        {
          name: "gather-comparator",
          setup(builder) {
            builder.onLoad({ filter: /[/\\]page[/\\]decode\.ts$/ }, ({ path }) => {
              let contents = readFileSync(path, "utf8");
              if (variant !== "production") {
                const start = contents.indexOf("        // A non-NaN float keeps its bits");
                const end = contents.indexOf("\n      }\n      return;", start);
                if (start < 0 || end < 0) throw new Error("Gather comparator anchor moved");
                const loop =
                  variant === "number"
                    ? `
        for (let k = 0; k < count; k++) out[at + k] = values[(rows[k] ?? 0) - base0] ?? 0;`
                    : `
        const words = values.BYTES_PER_ELEMENT / 4;
        const source = new Uint32Array(values.buffer, values.byteOffset, values.length * words);
        const target = new Uint32Array(out.buffer, out.byteOffset, out.length * words);
        for (let k = 0; k < count; k++) {
          const from = ((rows[k] ?? 0) - base0) * words;
          const slot = (at + k) * words;
          target[slot] = source[from] ?? 0;
          if (words === 2) target[slot + 1] = source[from + 1] ?? 0;
        }`;
                contents = contents.slice(0, start) + loop + contents.slice(end);
              }
              return { contents, loader: "ts" };
            });
          },
        },
      ],
    });
    samples[variant] = [];
  }
  for (let round = 0; round < rounds; round++)
    for (let offset = 0; offset < variants.length; offset++) {
      const variant = defined(variants[(round + offset) % variants.length]);
      const result = JSON.parse(
        execFileSync(process.execPath, [join(scratch, `${variant}.mjs`)], {
          encoding: "utf8",
          timeout: 120_000,
        }),
      ) as Sample[];
      samples[variant].push(result);
      console.log(JSON.stringify({ round, variant, result }));
    }
  const summary = variants.flatMap((variant) =>
    [0.01, 0.1, 0.5].map((density) => {
      const rows = samples[variant].map((round) =>
        defined(round.find((row) => row.density === density)),
      );
      const stats = (key: "cpuNsPerRow" | "wallNsPerRow") => {
        const values = rows.map((row) => row[key]).sort((a, b) => a - b);
        return {
          median: values[Math.floor(values.length / 2)],
          min: values[0],
          max: values.at(-1),
        };
      };
      return { variant, density, cpu: stats("cpuNsPerRow"), wall: stats("wallNsPerRow") };
    }),
  );
  console.log(JSON.stringify({ rounds, summary }));
} finally {
  rmSync(scratch, { recursive: true, force: true });
}

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Missing benchmark sample");
  return value;
}
